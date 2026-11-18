"""PostgreSQL schema definition and initialisation.

Durability model
----------------
* ``batches`` carries every persisted fact about a submission and its
  decision.  Wafers are stored in a child table ``wafer_measurements``.
* A batch is processed inside a *single* transaction that inserts the
  decision and either appends the batch id to ``device_state.ref_batch_ids``
  (released only) or leaves it untouched.  Hence the decision, the reference
  history and data-lake visibility become visible atomically -- there is no
  observable intermediate ("half batch") state and the reference list can
  never be advanced twice.
* Row-level locking (``SELECT ... FOR UPDATE`` on ``device_state`` plus a
  transaction-scoped advisory lock on the device) serialises workers for one
  device while different devices proceed independently.
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS batches (
    id              BIGGENERATED_PLACEHOLDER,
    batch_no        TEXT        NOT NULL,
    device_id       TEXT        NOT NULL,
    sha256          TEXT        NOT NULL,
    size_bytes      BIGINT      NOT NULL,
    storage_path    TEXT        NOT NULL,
    accept_seq      BIGINT      NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('PENDING','RELEASED','QUARANTINED')),
    median_nm       DOUBLE PRECISION,
    ref_mean_nm     DOUBLE PRECISION,
    deviation_nm    DOUBLE PRECISION,
    reference_ids   JSONB,
    decided_at      TIMESTAMPTZ,
    decided_by      TEXT,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT batches_pkey PRIMARY KEY (id),
    CONSTRAINT batches_batch_no_uniq UNIQUE (batch_no),
    CONSTRAINT batches_device_accept_uniq UNIQUE (device_id, accept_seq)
);

CREATE TABLE IF NOT EXISTS wafer_measurements (
    batch_id    BIGINT  NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    wafer_no    TEXT    NOT NULL,
    thickness_nm INTEGER NOT NULL CHECK (thickness_nm > 0),
    CONSTRAINT wafer_measurements_pkey PRIMARY KEY (batch_id, wafer_no)
);

CREATE INDEX IF NOT EXISTS idx_batches_device_seq
    ON batches (device_id, accept_seq);
CREATE INDEX IF NOT EXISTS idx_batches_status
    ON batches (status) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_wafers_batch ON wafer_measurements (batch_id);

-- One row per device: the durable processing cursor (last accepted sequence
-- that has been decided) and the FIFO list of released batch ids that form
-- the rolling reference window (most recent last).
CREATE TABLE IF NOT EXISTS device_state (
    device_id        TEXT PRIMARY KEY,
    last_decided_seq BIGINT NOT NULL DEFAULT 0,
    ref_batch_ids    JSONB  NOT NULL DEFAULT '[]'::jsonb,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Sealed snapshots
--
-- A snapshot copies everything needed to recompute a device's supply state at
-- one committed point in time: per-batch status, and for released batches the
-- wafer rows plus the reference batch numbers/medians/mean that were used.
-- PENDING and QUARANTINED batches store their status only.
--
-- Deliberately no foreign key from snapshot_* to batches: a snapshot must stay
-- readable unchanged even after later decisions (or archival) touch batches.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS snapshots (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY,
    snapshot_no         TEXT        NOT NULL,
    request_no          TEXT        NOT NULL,
    device_id           TEXT        NOT NULL,
    start_seq           BIGINT      NOT NULL,
    end_seq             BIGINT      NOT NULL,
    batch_count         INTEGER     NOT NULL,
    total_released      INTEGER     NOT NULL DEFAULT 0,
    total_quarantined   INTEGER     NOT NULL DEFAULT 0,
    total_pending       INTEGER     NOT NULL DEFAULT 0,
    total_wafers        BIGINT      NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by          TEXT,
    CONSTRAINT snapshots_pkey PRIMARY KEY (id),
    CONSTRAINT snapshots_no_uniq UNIQUE (snapshot_no),
    CONSTRAINT snapshots_request_no_uniq UNIQUE (request_no),
    CONSTRAINT snapshots_range_ordered CHECK (end_seq >= start_seq),
    CONSTRAINT snapshots_batch_count CHECK (batch_count BETWEEN 1 AND 20)
);

CREATE TABLE IF NOT EXISTS snapshot_batches (
    snapshot_id          BIGINT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    accept_seq           BIGINT NOT NULL,
    batch_id             BIGINT NOT NULL,
    batch_no             TEXT   NOT NULL,
    status               TEXT   NOT NULL
                         CHECK (status IN ('PENDING','RELEASED','QUARANTINED')),
    median_nm            DOUBLE PRECISION,
    ref_mean_nm          DOUBLE PRECISION,
    deviation_nm         DOUBLE PRECISION,
    reference_batch_ids  JSONB,
    reference_batch_nos  JSONB,
    reference_medians_nm JSONB,
    wafer_count          INTEGER,
    CONSTRAINT snapshot_batches_pkey PRIMARY KEY (snapshot_id, accept_seq)
);

CREATE TABLE IF NOT EXISTS snapshot_wafers (
    snapshot_id  BIGINT  NOT NULL,
    accept_seq   BIGINT  NOT NULL,
    wafer_no     TEXT    NOT NULL,
    thickness_nm INTEGER NOT NULL CHECK (thickness_nm > 0),
    CONSTRAINT snapshot_wafers_pkey
        PRIMARY KEY (snapshot_id, accept_seq, wafer_no),
    CONSTRAINT snapshot_wafers_batch_fk
        FOREIGN KEY (snapshot_id, accept_seq)
        REFERENCES snapshot_batches(snapshot_id, accept_seq) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_snapshot_batches_snap
    ON snapshot_batches (snapshot_id, accept_seq);
"""

# One fixed lock id makes concurrent schema initialisation from the two API
# instances safe; DDL is transactional, so the lock is released on commit.
SCHEMA_INIT_LOCK_ID = 78219002


async def init_schema(conn) -> None:
    # BIGSERIAL spelled out so the file stays readable on its own.
    sql = SCHEMA_SQL.replace("BIGGENERATED_PLACEHOLDER",
                             "BIGINT GENERATED ALWAYS AS IDENTITY")
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)",
                           SCHEMA_INIT_LOCK_ID)
        await conn.execute(sql)
