"""Submission/acceptance service and read models."""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from .csvutil import ParsedCsv
from .storage import StoredArtifact

# Unique constraint backing per-device accept_seq (see schema.py).  Concurrent
# inserts of the same next sequence collide here and are retried, which keeps
# every intake transaction short -- no connection is ever held while blocked
# on a lock, so the pool cannot be starved by same-device contention.
DEVICE_SEQ_CONSTRAINT = "batches_device_accept_uniq"
BATCH_NO_CONSTRAINT = "batches_batch_no_uniq"
SNAPSHOT_REQUEST_CONSTRAINT = "snapshots_request_no_uniq"
SNAPSHOT_NO_CONSTRAINT = "snapshots_no_uniq"

MAX_SNAPSHOT_BATCHES = 20


class ConflictError(Exception):
    """HTTP 409: duplicate batch number / request number with other params."""

    def __init__(self, message: str, existing: Dict[str, Any]):
        super().__init__(message)
        self.existing = existing


class SnapshotValidationError(ValueError):
    """HTTP 400: snapshot range is malformed or contains unaccepted seqs."""


@dataclass
class Acceptance:
    batch_id: int
    batch_no: str
    device_id: str
    accept_seq: int
    status: str
    reused: bool


async def accept_submission(
    pool: asyncpg.pool.Pool,
    *,
    batch_no: str,
    device_id: str,
    raw: bytes,
    parsed: ParsedCsv,
    artifact: StoredArtifact,
) -> Acceptance:
    """Persist a new batch or resolve an idempotent retry.

    The original file is already on disk (see :mod:`app.storage`) before this
    runs.  Same ``batch_no`` + same device + identical bytes reuses the
    existing row; any other combination of an existing number is rejected.
    """
    for attempt in range(50):
        try:
            return await _accept_once(
                pool, batch_no=batch_no, device_id=device_id,
                parsed=parsed, artifact=artifact)
        except asyncpg.UniqueViolationError as exc:
            if exc.constraint_name == DEVICE_SEQ_CONSTRAINT:
                # Another same-device submission won this sequence number;
                # re-read the max and try again. Tiny bounded backoff.
                await asyncio.sleep(min(0.05, 0.005 * (attempt + 1)))
                continue
            if exc.constraint_name == BATCH_NO_CONSTRAINT:
                # Race: two brand-new submissions with the same batch_no.
                # Re-read and turn it into the normal 409 decision.
                async with pool.acquire() as conn:
                    existing = await conn.fetchrow(
                        "SELECT id, batch_no, device_id, sha256, accept_seq, "
                        "status FROM batches WHERE batch_no = $1", batch_no)
                if existing is not None:
                    return _reuse_or_conflict(batch_no, device_id, artifact, existing)
            raise
    raise RuntimeError(f"could not allocate accept_seq for {batch_no} after retries")


async def _accept_once(pool, *, batch_no, device_id, parsed, artifact) -> Acceptance:
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT id, batch_no, device_id, sha256, accept_seq, status "
                "FROM batches WHERE batch_no = $1",
                batch_no,
            )
            if existing is not None:
                return _reuse_or_conflict(batch_no, device_id, artifact, existing)

            next_seq = await conn.fetchval(
                "SELECT COALESCE(MAX(accept_seq), 0) + 1 FROM batches "
                "WHERE device_id = $1",
                device_id,
            )
            batch_id = await conn.fetchval(
                """
                INSERT INTO batches
                    (batch_no, device_id, sha256, size_bytes, storage_path,
                     accept_seq, status)
                VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
                RETURNING id
                """,
                batch_no, device_id, artifact.sha256, artifact.size_bytes,
                artifact.storage_path, next_seq,
            )
            await conn.executemany(
                "INSERT INTO wafer_measurements(batch_id, wafer_no, thickness_nm) "
                "VALUES ($1, $2, $3)",
                [(batch_id, w, t) for w, t in parsed.rows],
            )
            await conn.execute(
                "INSERT INTO device_state(device_id) VALUES ($1) "
                "ON CONFLICT (device_id) DO NOTHING",
                device_id,
            )
            return Acceptance(
                batch_id=batch_id, batch_no=batch_no, device_id=device_id,
                accept_seq=next_seq, status="PENDING", reused=False,
            )


def _reuse_or_conflict(batch_no: str, device_id: str,
                       artifact: StoredArtifact,
                       existing: asyncpg.Record) -> Acceptance:
    """Idempotent reuse on identical re-submission, else HTTP 409."""
    if existing["device_id"] != device_id:
        raise ConflictError(
            f"批号 {batch_no} 已被其他设备占用 / batch number already "
            "registered for another device", _brief(existing))
    if existing["sha256"] != artifact.sha256:
        raise ConflictError(
            f"批号 {batch_no} 已存在但 CSV 内容不同（sha256 不一致），"
            "拒绝重复提交 / same batch number with different content",
            _brief(existing))
    # Identical retry: re-use the original acceptance.
    return Acceptance(
        batch_id=existing["id"], batch_no=batch_no, device_id=device_id,
        accept_seq=existing["accept_seq"], status=existing["status"],
        reused=True,
    )


def _brief(row: asyncpg.Record) -> Dict[str, Any]:
    return {
        "batch_no": row["batch_no"],
        "device_id": row["device_id"],
        "accept_seq": row["accept_seq"],
        "status": row["status"],
    }


async def get_batch_detail(pool: asyncpg.pool.Pool,
                           batch_no: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM batches WHERE batch_no = $1", batch_no)
        if row is None:
            return None
        ref_ids: List[int] = list(row["reference_ids"] or [])
        ref_batch_nos: List[str] = []
        if ref_ids:
            ref_rows = await conn.fetch(
                "SELECT id, batch_no FROM batches WHERE id = ANY($1::bigint[]) "
                "ORDER BY array_position($2::bigint[], id)",
                ref_ids, ref_ids,
            )
            ref_batch_nos = [r["batch_no"] for r in ref_rows]
        return {
            "batch_no": row["batch_no"],
            "device_id": row["device_id"],
            "status": row["status"],
            "accept_seq": row["accept_seq"],
            "wafer_count": await conn.fetchval(
                "SELECT count(*) FROM wafer_measurements WHERE batch_id=$1",
                row["id"]),
            "median_nm": row["median_nm"],
            "reference_mean_nm": row["ref_mean_nm"],
            "deviation_nm": row["deviation_nm"],
            "reference_batch_ids": ref_ids,
            "reference_batch_nos": ref_batch_nos,
            "sha256": row["sha256"],
            "original_storage_path": row["storage_path"],
            "attempts": row["attempts"],
            "decided_by": row["decided_by"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "decided_at": row["decided_at"].isoformat() if row["decided_at"] else None,
        }


async def list_device_batches(pool: asyncpg.pool.Pool,
                              device_id: str) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT batch_no, accept_seq, status, median_nm, deviation_nm, "
            "decided_at, attempts FROM batches WHERE device_id=$1 "
            "ORDER BY accept_seq",
            device_id,
        )
        return [
            {
                "batch_no": r["batch_no"],
                "accept_seq": r["accept_seq"],
                "status": r["status"],
                "median_nm": r["median_nm"],
                "deviation_nm": r["deviation_nm"],
                "attempts": r["attempts"],
                "decided_at": r["decided_at"].isoformat() if r["decided_at"] else None,
            }
            for r in rows
        ]


async def get_released_data(pool: asyncpg.pool.Pool, *,
                            device_id: Optional[str],
                            batch_no: Optional[str],
                            limit: int,
                            offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    """Read model for the data lake: *only* released batches are served."""
    where = ["status = 'RELEASED'"]
    params: List[Any] = []
    if device_id:
        params.append(device_id)
        where.append(f"device_id = ${len(params)}")
    if batch_no:
        params.append(batch_no)
        where.append(f"batch_no = ${len(params)}")
    where_sql = " WHERE " + " AND ".join(where)

    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM batches{where_sql}", *params)
        params.append(limit)
        params.append(offset)
        rows = await conn.fetch(
            f"SELECT id, batch_no, device_id, accept_seq, median_nm, decided_at "
            f"FROM batches{where_sql} ORDER BY id "
            f"LIMIT ${len(params) - 1} OFFSET ${len(params)}",
            *params,
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            wafers = await conn.fetch(
                "SELECT wafer_no, thickness_nm FROM wafer_measurements "
                "WHERE batch_id=$1 ORDER BY wafer_no", r["id"])
            out.append({
                "batch_no": r["batch_no"],
                "device_id": r["device_id"],
                "accept_seq": r["accept_seq"],
                "median_nm": r["median_nm"],
                "decided_at": r["decided_at"].isoformat() if r["decided_at"] else None,
                "wafers": [
                    {"wafer_no": w["wafer_no"], "thickness_nm": w["thickness_nm"]}
                    for w in wafers
                ],
            })
        return total, out


# ---------------------------------------------------------------------------
# Sealed snapshots
# ---------------------------------------------------------------------------

@dataclass
class SnapshotCreation:
    snapshot_no: str
    replayed: bool
    payload: Dict[str, Any]


def _snapshot_header(row: asyncpg.Record) -> Dict[str, Any]:
    return {
        "snapshot_no": row["snapshot_no"],
        "request_no": row["request_no"],
        "device_id": row["device_id"],
        "start_accept_seq": row["start_seq"],
        "end_accept_seq": row["end_seq"],
        "batch_count": row["batch_count"],
        "totals": {
            "batches": row["batch_count"],
            "released": row["total_released"],
            "quarantined": row["total_quarantined"],
            "pending": row["total_pending"],
            "wafers": row["total_wafers"],
        },
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "created_by": row["created_by"],
    }


def _same_request(row: asyncpg.Record, device_id: str,
                  start_seq: int, end_seq: int) -> bool:
    return (row["device_id"] == device_id
            and row["start_seq"] == start_seq
            and row["end_seq"] == end_seq)


async def _fetch_snapshot_by_request(conn, request_no: str):
    return await conn.fetchrow(
        "SELECT * FROM snapshots WHERE request_no = $1", request_no)


async def _read_snapshot(conn, header: asyncpg.Record) -> Dict[str, Any]:
    """Assemble the sealed payload from ``snapshot_*`` tables only.

    Nothing here touches ``batches``/``wafer_measurements``, so a snapshot's
    content is frozen: later decisions cannot change it.
    """
    rows = await conn.fetch(
        "SELECT * FROM snapshot_batches WHERE snapshot_id = $1 "
        "ORDER BY accept_seq", header["id"])
    batches: List[Dict[str, Any]] = []
    for r in rows:
        wafers: List[Dict[str, Any]] = []
        if r["status"] == "RELEASED":
            wafer_rows = await conn.fetch(
                "SELECT wafer_no, thickness_nm FROM snapshot_wafers "
                "WHERE snapshot_id = $1 AND accept_seq = $2 ORDER BY wafer_no",
                header["id"], r["accept_seq"])
            wafers = [
                {"wafer_no": w["wafer_no"], "thickness_nm": w["thickness_nm"]}
                for w in wafer_rows
            ]
        released = r["status"] == "RELEASED"
        batches.append({
            "accept_seq": r["accept_seq"],
            "batch_no": r["batch_no"],
            "status": r["status"],
            # Pending / quarantined batches freeze the status only.
            "median_nm": r["median_nm"] if released else None,
            "reference_mean_nm": r["ref_mean_nm"] if released else None,
            "deviation_nm": r["deviation_nm"] if released else None,
            "reference_batch_nos":
                list(r["reference_batch_nos"] or []) if released else [],
            "reference_medians_nm":
                list(r["reference_medians_nm"] or []) if released else [],
            "wafer_count": r["wafer_count"] if released else None,
            "wafers": wafers if released else [],
        })
    payload = _snapshot_header(header)
    payload["batches"] = batches
    return payload


async def create_snapshot(pool: asyncpg.pool.Pool, *,
                          device_id: str,
                          start_seq: int,
                          end_seq: int,
                          request_no: str,
                          created_by: str) -> SnapshotCreation:
    """Seal a device's accepted-sequence range at one committed state.

    Idempotency is keyed on ``request_no``: concurrent duplicate calls and
    retries after a lost response return the *same* snapshot; a request number
    reused with different parameters is a 409.  All writes happen in a single
    transaction, so a failure never leaves a readable partial snapshot.
    """
    device_id = device_id.strip()
    request_no = request_no.strip()
    if not device_id or not request_no:
        raise SnapshotValidationError(
            "device_id 与 request_no 不能为空 / fields required")
    if start_seq < 1 or end_seq < start_seq:
        raise SnapshotValidationError(
            "受理序号区间无效 / invalid accept sequence range")
    if end_seq - start_seq + 1 > MAX_SNAPSHOT_BATCHES:
        raise SnapshotValidationError(
            f"单次快照最多 {MAX_SNAPSHOT_BATCHES} 批 / too many batches")

    for attempt in range(10):
        try:
            return await _create_snapshot_once(
                pool, device_id=device_id, start_seq=start_seq,
                end_seq=end_seq, request_no=request_no, created_by=created_by)
        except asyncpg.SerializationError:
            # REPEATABLE READ aborted (e.g. the database chose to fail one of
            # two racing transactions).  A fresh transaction either sees the
            # other request's committed snapshot or creates this one.
            await asyncio.sleep(0.01 * (attempt + 1))
            continue
        except asyncpg.UniqueViolationError as exc:
            if exc.constraint_name in (SNAPSHOT_REQUEST_CONSTRAINT,
                                       SNAPSHOT_NO_CONSTRAINT):
                # Lost the concurrent race: resolve against the committed row.
                async with pool.acquire() as conn:
                    existing = await _fetch_snapshot_by_request(conn, request_no)
                    if existing is not None:
                        return await _resolve_existing(
                            conn, existing, device_id, start_seq, end_seq)
                await asyncio.sleep(0.01 * (attempt + 1))
                continue
            raise
    raise RuntimeError(
        f"could not create snapshot for request {request_no} after retries")


async def _resolve_existing(conn, existing: asyncpg.Record, device_id: str,
                            start_seq: int, end_seq: int) -> SnapshotCreation:
    if not _same_request(existing, device_id, start_seq, end_seq):
        raise ConflictError(
            f"请求号 {existing['request_no']} 已封存其他参数 / request number "
            "already used with different parameters",
            _brief_snapshot(existing))
    payload = await _read_snapshot(conn, existing)
    return SnapshotCreation(snapshot_no=existing["snapshot_no"],
                            replayed=True, payload=payload)


def _brief_snapshot(row: asyncpg.Record) -> Dict[str, Any]:
    return {
        "snapshot_no": row["snapshot_no"],
        "request_no": row["request_no"],
        "device_id": row["device_id"],
        "start_accept_seq": row["start_seq"],
        "end_accept_seq": row["end_seq"],
    }


async def _create_snapshot_once(pool, *, device_id: str, start_seq: int,
                                end_seq: int, request_no: str,
                                created_by: str) -> SnapshotCreation:
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read"):
            existing = await _fetch_snapshot_by_request(conn, request_no)
            if existing is not None:
                return await _resolve_existing(
                    conn, existing, device_id, start_seq, end_seq)

            # Reject ranges that contain sequence numbers never accepted for
            # this device (a gap means the requested interval is not a
            # contiguous accepted prefix).
            batch_rows = await conn.fetch(
                "SELECT id, batch_no, accept_seq, status, median_nm, "
                "ref_mean_nm, deviation_nm, reference_ids "
                "FROM batches WHERE device_id = $1 AND accept_seq "
                "BETWEEN $2 AND $3 ORDER BY accept_seq",
                device_id, start_seq, end_seq)
            got_seqs = [r["accept_seq"] for r in batch_rows]
            expected_seqs = list(range(start_seq, end_seq + 1))
            if got_seqs != expected_seqs:
                missing = sorted(set(expected_seqs) - set(got_seqs))
                raise SnapshotValidationError(
                    f"设备 {device_id} 的受理序号 {missing} 未受理，拒绝封存 / "
                    "range contains unaccepted sequence(s)")

            snapshot_no = f"SNP-{uuid.uuid4().hex}"
            snapshot_id = await conn.fetchval(
                """
                INSERT INTO snapshots
                    (snapshot_no, request_no, device_id, start_seq, end_seq,
                     batch_count, created_by)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
                """,
                snapshot_no, request_no, device_id, start_seq, end_seq,
                len(batch_rows), created_by,
            )

            total_released = total_quarantined = total_pending = 0
            total_wafers = 0
            for b in batch_rows:
                status = b["status"]
                if status == "RELEASED":
                    total_released += 1
                elif status == "QUARANTINED":
                    total_quarantined += 1
                else:
                    total_pending += 1

                # Only released batches carry detail; pending/quarantined
                # freeze their status alone.
                if status != "RELEASED":
                    await conn.execute(
                        "INSERT INTO snapshot_batches"
                        "(snapshot_id, accept_seq, batch_id, batch_no, status) "
                        "VALUES ($1,$2,$3,$4,$5)",
                        snapshot_id, b["accept_seq"], b["id"], b["batch_no"],
                        status)
                    continue

                ref_ids: List[int] = list(b["reference_ids"] or [])
                ref_nos: List[str] = []
                ref_medians: List[float] = []
                if ref_ids:
                    ref_rows = await conn.fetch(
                        "SELECT id, batch_no, median_nm FROM batches "
                        "WHERE id = ANY($1::bigint[])", ref_ids)
                    by_id = {r["id"]: r for r in ref_rows}
                    # Never seal a silently truncated reference set.
                    missing = [rid for rid in ref_ids if rid not in by_id]
                    if missing:
                        raise RuntimeError(
                            f"batch {b['batch_no']} references missing "
                            f"batch ids {missing}; refusing partial snapshot")
                    for rid in ref_ids:
                        ref = by_id[rid]
                        ref_nos.append(ref["batch_no"])
                        ref_medians.append(float(ref["median_nm"]))
                wafer_rows = await conn.fetch(
                    "SELECT wafer_no, thickness_nm FROM wafer_measurements "
                    "WHERE batch_id = $1 ORDER BY wafer_no", b["id"])
                total_wafers += len(wafer_rows)

                await conn.execute(
                    """
                    INSERT INTO snapshot_batches
                        (snapshot_id, accept_seq, batch_id, batch_no, status,
                         median_nm, ref_mean_nm, deviation_nm,
                         reference_batch_ids, reference_batch_nos,
                         reference_medians_nm, wafer_count)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,
                            $11::jsonb,$12)
                    """,
                    snapshot_id, b["accept_seq"], b["id"], b["batch_no"],
                    status, b["median_nm"], b["ref_mean_nm"],
                    b["deviation_nm"], ref_ids, ref_nos, ref_medians,
                    len(wafer_rows),
                )
                if wafer_rows:
                    await conn.executemany(
                        "INSERT INTO snapshot_wafers"
                        "(snapshot_id, accept_seq, wafer_no, thickness_nm) "
                        "VALUES ($1,$2,$3,$4)",
                        [(snapshot_id, b["accept_seq"], w["wafer_no"],
                          w["thickness_nm"]) for w in wafer_rows])

            await conn.execute(
                "UPDATE snapshots SET total_released=$2, total_quarantined=$3, "
                "total_pending=$4, total_wafers=$5 WHERE id=$1",
                snapshot_id, total_released, total_quarantined,
                total_pending, total_wafers)

            header = await conn.fetchrow(
                "SELECT * FROM snapshots WHERE id = $1", snapshot_id)
            payload = await _read_snapshot(conn, header)
            return SnapshotCreation(snapshot_no=snapshot_no, replayed=False,
                                    payload=payload)


async def get_snapshot(pool: asyncpg.pool.Pool,
                       snapshot_no: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        header = await conn.fetchrow(
            "SELECT * FROM snapshots WHERE snapshot_no = $1", snapshot_no)
        if header is None:
            return None
        payload = await _read_snapshot(conn, header)
        return payload
