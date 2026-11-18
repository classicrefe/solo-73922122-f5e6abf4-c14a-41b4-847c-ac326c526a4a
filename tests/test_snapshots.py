"""Sealed snapshot API tests.

Covers the hand-off contract:
* a contiguous accepted-sequence range is sealed from one committed state;
* released batches carry wafer detail + reference numbers/medians/mean,
  pending and quarantined batches carry status only;
* reading by snapshot number is frozen even after later decisions;
* request numbers are idempotent across retries and two concurrent
  "instances" (independent pools), but conflicting parameters are rejected;
* a failed seal leaves no readable partial snapshot and does not consume the
  request number.
"""
import asyncio
import time

import asyncpg
import pytest

from app.config import settings
from app.db import _init_connection, get_pool
from app.worker import DecisionWorker
from tests.conftest import submit, uniform_csv


async def _stop_app_worker():
    from app.main import app
    worker = getattr(app.state, "worker", None)
    if worker is not None:
        await worker.stop()


async def _drain(timeout=15.0):
    pool = get_pool()
    deadline = time.time() + timeout
    pending = -1
    while time.time() < deadline:
        async with pool.acquire() as conn:
            pending = await conn.fetchval(
                "SELECT count(*) FROM batches WHERE status = 'PENDING'")
        if pending == 0:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"batches still pending: {pending}")


async def _seal(ac, device, start, end, request_no):
    return await ac.post("/snapshots", json={
        "device_id": device,
        "start_accept_seq": start,
        "end_accept_seq": end,
        "request_no": request_no,
    })


@pytest.mark.asyncio
async def test_snapshot_seals_from_one_committed_state_and_is_frozen(client):
    ac = client
    await _stop_app_worker()          # decide the story explicitly below

    # 3 baselines at 500, a drifting 900, and a returning 502.
    for i in range(1, 4):
        r = await submit(ac, f"S{i}", "EQ-SNAP", uniform_csv(f"s{i}", 5, 500))
        assert r.status_code == 202, r.text
    await submit(ac, "S4", "EQ-SNAP", uniform_csv("s4", 5, 900))
    await submit(ac, "S5", "EQ-SNAP", uniform_csv("s5", 5, 502))

    # Seal while everything is still PENDING: status-only records, zero wafers.
    r = await _seal(ac, "EQ-SNAP", 1, 5, "REQ-EARLY")
    assert r.status_code == 201, r.text
    early = r.json()
    snapshot_no = early["snapshot_no"]
    assert early["batch_count"] == 5
    assert early["totals"] == {"batches": 5, "released": 0, "quarantined": 0,
                               "pending": 5, "wafers": 0}
    assert [b["status"] for b in early["batches"]] == ["PENDING"] * 5
    assert all(b["wafers"] == [] for b in early["batches"])
    assert all(b["median_nm"] is None for b in early["batches"])

    # Now let decisions happen.
    worker = DecisionWorker(get_pool(), "snap-test")
    worker.start()
    try:
        await _drain()
    finally:
        await worker.stop()

    statuses = {r["batch_no"]: r["status"] for r in [
        (await ac.get(f"/batches/S{i}")).json() for i in range(1, 6)]}
    assert statuses == {"S1": "RELEASED", "S2": "RELEASED", "S3": "RELEASED",
                        "S4": "QUARANTINED", "S5": "RELEASED"}

    # The early snapshot is byte-for-byte unchanged, including totals.
    later = await ac.get(f"/snapshots/{snapshot_no}")
    assert later.status_code == 200
    assert later.json() == early
    assert later.json()["totals"]["pending"] == 5
    assert later.json()["totals"]["released"] == 0

    # A new snapshot of the same range reflects the now-committed decisions.
    r = await _seal(ac, "EQ-SNAP", 1, 5, "REQ-LATE")
    assert r.status_code == 201, r.text
    late = r.json()
    assert late["snapshot_no"] != snapshot_no
    assert late["totals"] == {"batches": 5, "released": 4, "quarantined": 1,
                              "pending": 0, "wafers": 20}
    by_no = {b["batch_no"]: b for b in late["batches"]}
    # Baseline: released with wafers, no reference set.
    assert by_no["S1"]["status"] == "RELEASED"
    assert by_no["S1"]["reference_batch_nos"] == []
    assert len(by_no["S1"]["wafers"]) == 5
    assert by_no["S1"]["wafers"][0] == {"wafer_no": "s1-W001",
                                        "thickness_nm": 500}
    # Quarantined: status only, no detail.
    assert by_no["S4"]["status"] == "QUARANTINED"
    assert by_no["S4"]["wafers"] == []
    assert by_no["S4"]["median_nm"] is None
    assert by_no["S4"]["reference_batch_nos"] == []
    # Post-baseline release: reference batch numbers and medians are frozen.
    s5 = by_no["S5"]
    assert s5["reference_batch_nos"] == ["S1", "S2", "S3"]
    assert s5["reference_medians_nm"] == [500.0, 500.0, 500.0]
    assert s5["reference_mean_nm"] == pytest.approx(500.0)
    assert s5["deviation_nm"] == pytest.approx(2.0)
    assert s5["wafer_count"] == 5
    assert len(s5["wafers"]) == 5

    # The late snapshot is also frozen, even if the source rows disappear.
    again = await ac.get(f"/snapshots/{late['snapshot_no']}")
    assert again.json() == late


@pytest.mark.asyncio
async def test_snapshot_rejects_gaps_unaccepted_and_oversized_ranges(client):
    ac = client
    await _stop_app_worker()
    for i in range(1, 4):
        await submit(ac, f"G{i}", "EQ-GAP", uniform_csv(f"g{i}", 2, 500))

    # End sequence not accepted yet -> 400, nothing sealed.
    r = await _seal(ac, "EQ-GAP", 1, 4, "REQ-GAP1")
    assert r.status_code == 400, r.text
    assert "未受理" in r.json()["error"]

    # Start sequence not accepted (starts after the last one) -> 400.
    r = await _seal(ac, "EQ-GAP", 4, 4, "REQ-GAP2")
    assert r.status_code == 400

    # Unknown device -> 400.
    r = await _seal(ac, "EQ-NOPE", 1, 1, "REQ-GAP3")
    assert r.status_code == 400

    # More than 20 batches -> 400.
    r = await _seal(ac, "EQ-GAP", 1, 21, "REQ-GAP4")
    assert r.status_code == 400
    assert "20" in r.json()["error"]

    # start > end -> 400; start < 1 -> 422 (schema validation).
    assert (await _seal(ac, "EQ-GAP", 3, 2, "REQ-GAP5")).status_code == 400
    assert (await _seal(ac, "EQ-GAP", 0, 2, "REQ-GAP6")).status_code == 422

    # A contiguous sub-range is fine.
    r = await _seal(ac, "EQ-GAP", 2, 3, "REQ-GAP-OK")
    assert r.status_code == 201, r.text
    assert [b["accept_seq"] for b in r.json()["batches"]] == [2, 3]

    # None of the rejected requests left a readable snapshot behind.
    pool = get_pool()
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM snapshots") == 1
        assert await conn.fetchval("SELECT count(*) FROM snapshot_batches") == 2
        # No partial rows for rejected request numbers.
        assert await conn.fetchval(
            "SELECT count(*) FROM snapshots WHERE request_no = ANY($1::text[])",
            ["REQ-GAP1", "REQ-GAP2", "REQ-GAP3", "REQ-GAP4", "REQ-GAP5",
             "REQ-GAP6"]) == 0


@pytest.mark.asyncio
async def test_snapshot_request_number_is_idempotent_and_conflicts(client):
    ac = client
    await _stop_app_worker()
    for i in range(1, 4):
        await submit(ac, f"I{i}", "EQ-IDEM", uniform_csv(f"i{i}", 3, 500))

    first = await _seal(ac, "EQ-IDEM", 1, 3, "REQ-IDEM")
    assert first.status_code == 201
    snapshot_no = first.json()["snapshot_no"]

    # Retry after a "lost response": identical parameters, same snapshot.
    retry = await _seal(ac, "EQ-IDEM", 1, 3, "REQ-IDEM")
    assert retry.status_code == 200
    assert retry.json() == first.json()
    assert retry.json()["snapshot_no"] == snapshot_no

    # Same request number with different parameters -> 409 + existing identity.
    for params in [
        dict(device_id="EQ-OTHER", start_accept_seq=1, end_accept_seq=3),
        dict(device_id="EQ-IDEM", start_accept_seq=1, end_accept_seq=2),
        dict(device_id="EQ-IDEM", start_accept_seq=2, end_accept_seq=3),
    ]:
        r = await ac.post("/snapshots", json={**params,
                                              "request_no": "REQ-IDEM"})
        assert r.status_code == 409, (params, r.text)
        assert r.json()["existing"]["snapshot_no"] == snapshot_no
    # The conflict path did not seal anything extra.
    pool = get_pool()
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM snapshots") == 1


@pytest.mark.asyncio
async def test_concurrent_same_request_single_snapshot_two_instances(client):
    ac = client
    await _stop_app_worker()
    for i in range(1, 4):
        await submit(ac, f"C{i}", "EQ-CONC", uniform_csv(f"c{i}", 3, 500))
    assert (await _seal(ac, "EQ-CONC", 1, 3, "REQ-SETUP")).status_code == 201
    # /snapshots itself doesn't decide; batches stay PENDING, that's fine.

    # Two independent pools emulate two API instances sharing one database.
    dsn = settings.database_dsn
    pool_b = await asyncpg.create_pool(dsn, min_size=1, max_size=4,
                                       init=_init_connection)
    try:
        from app.service import create_snapshot
        tasks = [
            create_snapshot(get_pool(), device_id="EQ-CONC", start_seq=1,
                            end_seq=3, request_no="REQ-RACE", created_by="api1"),
            create_snapshot(pool_b, device_id="EQ-CONC", start_seq=1,
                            end_seq=3, request_no="REQ-RACE", created_by="api2"),
        ]
        results = await asyncio.gather(*tasks)
    finally:
        await pool_b.close()

    assert len({r.snapshot_no for r in results}) == 1
    assert sorted(r.replayed for r in results) == [False, True]
    assert results[0].payload == results[1].payload

    # And over HTTP: many concurrent identical submissions all return it.
    responses = await asyncio.gather(
        *(_seal(ac, "EQ-CONC", 1, 3, "REQ-RACE2") for _ in range(8)))
    assert {r.status_code for r in responses} == {200, 201}
    assert sum(r.status_code == 201 for r in responses) == 1
    bodies = [r.json() for r in responses]
    assert len({b["snapshot_no"] for b in bodies}) == 1
    assert all(b == bodies[0] for b in bodies)

    pool = get_pool()
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT count(*) FROM snapshots WHERE request_no='REQ-RACE'") == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM snapshot_batches sb JOIN snapshots s "
            "ON s.id = sb.snapshot_id WHERE s.request_no='REQ-RACE'") == 3
        # Concurrent duplicates did not create orphan snapshot headers.
        assert await conn.fetchval("SELECT count(*) FROM snapshots") == 3


@pytest.mark.asyncio
async def test_failed_seal_leaves_no_readable_partial_result(client, monkeypatch):
    ac = client
    await _stop_app_worker()
    for i in range(1, 4):
        await submit(ac, f"F{i}", "EQ-FAIL", uniform_csv(f"f{i}", 2, 500))

    import app.service as svc
    real_read = svc._read_snapshot

    async def boom(*args, **kwargs):
        raise RuntimeError("simulated failure after partial writes")

    monkeypatch.setattr(svc, "_read_snapshot", boom)
    with pytest.raises(RuntimeError, match="simulated failure"):
        await _seal(ac, "EQ-FAIL", 1, 3, "REQ-FAIL")

    pool = get_pool()
    async with pool.acquire() as conn:
        # Header and child rows were rolled back together.
        assert await conn.fetchval("SELECT count(*) FROM snapshots") == 0
        assert await conn.fetchval("SELECT count(*) FROM snapshot_batches") == 0
        assert await conn.fetchval("SELECT count(*) FROM snapshot_wafers") == 0

    # The request number was not consumed: retrying succeeds normally.
    monkeypatch.setattr(svc, "_read_snapshot", real_read)
    r = await _seal(ac, "EQ-FAIL", 1, 3, "REQ-FAIL")
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["totals"]["batches"] == 3
    got = await ac.get(f"/snapshots/{body['snapshot_no']}")
    assert got.status_code == 200 and got.json() == body


@pytest.mark.asyncio
async def test_seal_is_one_committed_state_and_does_not_block_work(client,
                                                                   monkeypatch):
    """Pause the seal between its reads and commit; decisions/uploads proceed.

    The seal runs in one REPEATABLE READ transaction, so even though batches
    are decided while it is still open, the sealed payload is exactly the
    single committed state the transaction started from -- never a mix of old
    and new decisions.
    """
    ac = client
    await _stop_app_worker()
    for i in range(1, 4):
        await submit(ac, f"T{i}", "EQ-TORN", uniform_csv(f"t{i}", 2, 500))

    import app.service as svc
    real_read = svc._read_snapshot
    in_flight = asyncio.Event()
    release = asyncio.Event()

    async def pausing_read(conn, header):
        in_flight.set()
        await asyncio.wait_for(release.wait(), timeout=10)
        return await real_read(conn, header)

    monkeypatch.setattr(svc, "_read_snapshot", pausing_read)

    seal_task = asyncio.create_task(_seal(ac, "EQ-TORN", 1, 3, "REQ-TORN"))
    await asyncio.wait_for(in_flight.wait(), timeout=5)

    # While the seal transaction is still open: the worker decides the queue
    # and intake keeps accepting.  Neither may block on the snapshot.
    worker = DecisionWorker(get_pool(), "snap-torn")
    worker.start()
    try:
        await _drain(timeout=10)
        extra = await submit(ac, "T4", "EQ-TORN", uniform_csv("t4", 2, 500))
        assert extra.status_code == 202, extra.text
        await _drain(timeout=10)
        assert (await ac.get("/health")).json()["database"] is True
    finally:
        await worker.stop()

    release.set()
    resp = await asyncio.wait_for(seal_task, timeout=10)
    assert resp.status_code == 201, resp.text
    sealed = resp.json()
    # Sealed from the pre-decision state, even though decisions committed
    # while the transaction was open.
    assert sealed["totals"] == {"batches": 3, "released": 0, "quarantined": 0,
                                "pending": 3, "wafers": 0}
    assert [b["status"] for b in sealed["batches"]] == ["PENDING"] * 3
    assert all(b["wafers"] == [] for b in sealed["batches"])

    # The live read models show the later decisions; the snapshot does not.
    for i in range(1, 5):
        detail = (await ac.get(f"/batches/T{i}")).json()
        assert detail["status"] == "RELEASED"
    frozen = await ac.get(f"/snapshots/{sealed['snapshot_no']}")
    assert frozen.json() == sealed


@pytest.mark.asyncio
async def test_seal_never_observes_an_uncommitted_decision(client):
    """Deterministic interleaving: a decision transaction is open mid-commit.

    While a worker-like transaction holds an uncommitted RELEASE for W1, a
    seal must read only the last committed state (W1 still PENDING there) --
    never the uncommitted row.  After that decision commits, the sealed
    snapshot is unchanged; a *new* snapshot sees the new state.
    """
    ac = client
    await _stop_app_worker()
    for i in range(1, 4):
        await submit(ac, f"W{i}", "EQ-WAIT", uniform_csv(f"w{i}", 2, 500))

    pool = get_pool()
    deciding = asyncio.Event()
    allow_commit = asyncio.Event()

    async def decide_hold():
        async with pool.acquire() as conn:
            async with conn.transaction():
                batch = await conn.fetchrow(
                    "SELECT * FROM batches WHERE device_id='EQ-WAIT' "
                    "ORDER BY accept_seq LIMIT 1 FOR UPDATE")
                await conn.execute(
                    "UPDATE batches SET status='RELEASED', median_nm=500.0, "
                    "decided_at=now(), decided_by='test' WHERE id=$1",
                    batch["id"])
                await conn.execute(
                    "INSERT INTO device_state(device_id, last_decided_seq) "
                    "VALUES('EQ-WAIT', 1) ON CONFLICT (device_id) DO UPDATE "
                    "SET last_decided_seq=1")
                deciding.set()
                await asyncio.wait_for(allow_commit.wait(), timeout=10)

    decide_task = asyncio.create_task(decide_hold())
    await asyncio.wait_for(deciding.wait(), timeout=5)

    # Seal while the release is uncommitted: it reads the committed PENDING
    # state for W1 and must not block (MVCC read, no device lock).
    resp = await asyncio.wait_for(
        _seal(ac, "EQ-WAIT", 1, 3, "REQ-WAIT"), timeout=10)
    assert resp.status_code == 201, resp.text
    sealed = resp.json()
    assert sealed["totals"] == {"batches": 3, "released": 0, "quarantined": 0,
                                "pending": 3, "wafers": 0}
    assert all(b["status"] == "PENDING" for b in sealed["batches"])
    assert all(b["wafers"] == [] for b in sealed["batches"])

    # Let the decision commit; the sealed snapshot must not change.
    allow_commit.set()
    await asyncio.wait_for(decide_task, timeout=10)
    frozen = await ac.get(f"/snapshots/{sealed['snapshot_no']}")
    assert frozen.json() == sealed

    # A fresh seal of the same range now sees exactly one committed state.
    fresh = await _seal(ac, "EQ-WAIT", 1, 3, "REQ-WAIT-2")
    assert fresh.status_code == 201
    by_no = {b["batch_no"]: b for b in fresh.json()["batches"]}
    assert by_no["W1"]["status"] == "RELEASED"
    assert len(by_no["W1"]["wafers"]) == 2
    assert by_no["W2"]["status"] == "PENDING" and by_no["W2"]["wafers"] == []
    assert fresh.json()["totals"] == {"batches": 3, "released": 1,
                                      "quarantined": 0, "pending": 2,
                                      "wafers": 2}


@pytest.mark.asyncio
async def test_snapshot_read_is_independent_of_source_rows(client):
    """Even deleting the source batches must not alter the sealed payload."""
    ac = client
    await _stop_app_worker()
    for i in range(1, 4):
        await submit(ac, f"Z{i}", "EQ-ZS", uniform_csv(f"z{i}", 2, 500))
    worker = DecisionWorker(get_pool(), "snap-test")
    worker.start()
    try:
        await _drain()
    finally:
        await worker.stop()

    r = await _seal(ac, "EQ-ZS", 1, 3, "REQ-FREEZE")
    assert r.status_code == 201
    sealed = r.json()
    snapshot_no = sealed["snapshot_no"]

    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM wafer_measurements WHERE batch_id IN "
                           "(SELECT id FROM batches WHERE device_id='EQ-ZS')")
        await conn.execute("DELETE FROM batches WHERE device_id='EQ-ZS'")
        await conn.execute("DELETE FROM device_state WHERE device_id='EQ-ZS'")

    got = await ac.get(f"/snapshots/{snapshot_no}")
    assert got.status_code == 200
    assert got.json() == sealed
    # Snapshot number is not secret but unknown ones are 404.
    assert (await ac.get("/snapshots/SNP-nope")).status_code == 404
