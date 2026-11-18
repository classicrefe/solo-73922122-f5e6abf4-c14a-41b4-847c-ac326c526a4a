"""End-to-end demonstration of the sealed snapshot API on two API instances.

Run (no Docker needed)::

    python -m scripts.demo_snapshot

It starts an embedded PostgreSQL, two uvicorn instances (api1/api2) sharing
it, and demonstrates:

1. sealing a contiguous accepted range while one batch is still PENDING --
   the snapshot freezes the status only and stays unchanged after the batch
   later releases (and after a later, differently-sealed snapshot);
2. a released-range snapshot carries wafer detail, reference batch numbers,
   reference medians/mean, and totals;
3. the same request number from both instances concurrently (and a later
   retry simulating a lost response) returns the *same* snapshot; different
   parameters for that request number return 409;
4. a range containing an unaccepted sequence is rejected with 400, and no
   snapshot is left behind;
5. uploads and decisions keep flowing on another device while snapshots are
   being sealed and read.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import uuid

from scripts.demo_local import ApiInstance, ROOT, http, upload


def seal(base: str, device: str, start: int, end: int, request_no: str):
    payload = json.dumps({
        "device_id": device, "start_accept_seq": start,
        "end_accept_seq": end, "request_no": request_no,
    }).encode()
    return http("POST", f"{base}/snapshots", payload, "application/json")


def get_snapshot(base: str, snapshot_no: str):
    return http("GET", f"{base}/snapshots/{snapshot_no}")


def wait_released(port: str, batch_no: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        code, last = http("GET", f"http://127.0.0.1:{port}/batches/{batch_no}")
        if code == 200 and last.get("status") == "RELEASED":
            return last
        time.sleep(0.2)
    raise RuntimeError(f"{batch_no} not released in time: {last}")


class DeviceLock:
    """Hold one device's worker advisory lock from a dedicated event loop.

    The demo uses this to keep a device's batches PENDING while a snapshot is
    sealed, then releases the lock so the workers can decide them.  Both calls
    must share one event loop, hence the persistent loop object.
    """

    def __init__(self, dsn: str, device: str):
        self.dsn = dsn
        self.device = device
        self.loop = asyncio.new_event_loop()
        self.conn = None

    def acquire(self) -> None:
        async def _do():
            import asyncpg
            self.conn = await asyncpg.connect(self.dsn)
            got = await self.conn.fetchval(
                "SELECT pg_try_advisory_lock(78219001, hashtext($1))",
                self.device)
            assert got, "could not take the device advisory lock"
        self.loop.run_until_complete(_do())

    def release(self) -> None:
        async def _do():
            await self.conn.execute("SELECT pg_advisory_unlock_all()")
            await self.conn.close()
        self.loop.run_until_complete(_do())
        self.loop.close()


def main() -> int:
    import pgserver

    workdir = tempfile.mkdtemp(prefix="fabsnap_")
    raw_dir = os.path.join(workdir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    pg = pgserver.get_server(os.path.join(workdir, "pg"), cleanup_mode=None)
    dsn = pg.get_uri()
    print(f"[setup] embedded PostgreSQL: {dsn}")

    common_env = {**os.environ, "DATABASE_DSN": dsn, "STORAGE_DIR": raw_dir,
                  "SLOW_MARKER_FILE": os.path.join(raw_dir, "slow.txt"),
                  "SHUTDOWN_TOKEN": "demo-token", "PYTHONPATH": ROOT}
    port1 = os.environ.get("DEMO_PORT1", "8111")
    port2 = os.environ.get("DEMO_PORT2", "8112")
    base1, base2 = f"http://127.0.0.1:{port1}", f"http://127.0.0.1:{port2}"
    api1 = ApiInstance("api1", port1, {**common_env, "INSTANCE_ID": "api1"},
                       os.path.join(workdir, "api1.log"))
    api2 = ApiInstance("api2", port2, {**common_env, "INSTANCE_ID": "api2"},
                       os.path.join(workdir, "api2.log"))
    run = uuid.uuid4().hex[:6]
    dev = f"SNAP-{run}"
    other = f"OTHER-{run}"

    api1.start(); api2.start()
    try:
        print("\n=== 1. seed a device: 3 released baselines + 1 drift batch ===")
        codes = []
        for i in range(1, 4):
            code, _ = upload(port1 if i % 2 else port2, f"{run}-S{i}", dev, 500)
            codes.append(code)
        assert codes == [202, 202, 202], codes
        for i in range(1, 4):
            wait_released(port2, f"{run}-S{i}")
        code, _ = upload(port1, f"{run}-S4", dev, 900)
        assert code == 202
        deadline = time.time() + 20
        while time.time() < deadline:
            _, s4 = http("GET", f"{base2}/batches/{run}-S4")
            if s4.get("status") == "QUARANTINED":
                break
            time.sleep(0.2)
        assert s4["status"] == "QUARANTINED", s4
        print(f"  S1..S3 RELEASED, S4 {s4['status']} (drift)")

        print("\n=== 2. released-range snapshot carries detail + references ===")
        req = f"REQ-REL-{run}"
        code, sealed = seal(base1, dev, 1, 4, req)
        assert code == 201, sealed
        print(f"  snapshot {sealed['snapshot_no']} totals={sealed['totals']}")
        assert sealed["totals"] == {"batches": 4, "released": 3,
                                    "quarantined": 1, "pending": 0,
                                    "wafers": 15}
        by_no = {b["batch_no"]: b for b in sealed["batches"]}
        assert len(by_no[f"{run}-S1"]["wafers"]) == 5
        assert by_no[f"{run}-S4"]["status"] == "QUARANTINED"
        assert by_no[f"{run}-S4"]["wafers"] == []
        assert by_no[f"{run}-S4"]["median_nm"] is None
        # S4 is not a reference (quarantined); baseline batches carry none.
        assert by_no[f"{run}-S1"]["reference_batch_nos"] == []
        print("  S1 wafers:", by_no[f"{run}-S1"]["wafers"][:2], "...")
        print("  S4 (quarantined) status only:",
              {k: by_no[f"{run}-S4"][k]
               for k in ("status", "wafers", "median_nm")})
        # Read from the *other* instance: same sealed payload.
        code2, again = get_snapshot(base2, sealed["snapshot_no"])
        assert code2 == 200 and again == sealed
        print("  read back from the other instance: identical")

        print("\n=== 3. dynamic release: baseline's reference info is frozen ===")
        # S4 quarantined, so a 5th batch is compared against S1..S3 only.
        code, _ = upload(port2, f"{run}-S5", dev, 502)
        assert code == 202
        wait_released(port1, f"{run}-S5")
        code, sealed5 = seal(base2, dev, 5, 5, f"REQ-S5-{run}")
        assert code == 201, sealed5
        s5 = sealed5["batches"][0]
        print(f"  S5 refs={s5['reference_batch_nos']} "
              f"medians={s5['reference_medians_nm']} "
              f"mean={s5['reference_mean_nm']} dev={s5['deviation_nm']}")
        assert s5["reference_batch_nos"] == [
            f"{run}-S1", f"{run}-S2", f"{run}-S3"]
        assert s5["reference_medians_nm"] == [500.0, 500.0, 500.0]

        print("\n=== 4. pending batch frozen: seal PENDING, release later ===")
        pdev = f"PEND-{run}"
        lock = DeviceLock(dsn, pdev)
        lock.acquire()
        try:
            code, p1 = upload(port1, f"{run}-P1", pdev, 500)
            assert code == 202, p1
            code, _ = upload(port2, f"{run}-P2", pdev, 500)
            assert code == 202
            # Workers cannot decide this device while the demo holds its lock.
            code, early = seal(base1, pdev, 1, 2, f"REQ-PEND-{run}")
            assert code == 201, early
            print(f"  sealed while PENDING: totals={early['totals']}")
            assert early["totals"] == {"batches": 2, "released": 0,
                                       "quarantined": 0, "pending": 2,
                                       "wafers": 0}
            assert all(b["wafers"] == [] for b in early["batches"])
            # Uploads keep working during the seal.
            code, _ = upload(port1, f"{run}-P3", pdev, 500)
            assert code == 202
        finally:
            lock.release()

        for i in range(1, 4):
            wait_released(port1, f"{run}-P{i}")
        frozen = get_snapshot(base2, early["snapshot_no"])[1]
        assert frozen == early, "snapshot changed after pending batches released"
        print("  after P1..P3 released, the early snapshot is unchanged "
              f"(pending={frozen['totals']['pending']}, wafers="
              f"{frozen['totals']['wafers']})")

        print("\n=== 5. same request_no: concurrent + retry + conflict ===")
        req = f"REQ-IDEM-{run}"
        results = {}

        def call(name, base):
            try:
                results[name] = seal(base, dev, 1, 3, req)
            except Exception as exc:  # pragma: no cover
                results[name] = (0, {"error": str(exc)})

        import threading
        threads = [threading.Thread(target=call, args=(f"i{i}", b))
                   for i, b in enumerate((base1, base2))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        outcomes = list(results.values())
        codes = sorted(c for c, _ in outcomes)
        bodies = [b for _, b in outcomes]
        print(f"  concurrent from api1+api2 -> HTTP {codes}")
        assert codes == [200, 201], codes
        assert bodies[0] == bodies[1], "same request_no returned two snapshots"
        assert bodies[0]["request_no"] == req
        snap_no = bodies[0]["snapshot_no"]

        code, retry = seal(base2, dev, 1, 3, req)
        assert code == 200 and retry == bodies[0]
        print(f"  retry after lost response -> 200, same {snap_no}")

        code, conflict = seal(base1, dev, 1, 2, req)
        assert code == 409, (code, conflict)
        assert conflict["existing"]["snapshot_no"] == snap_no
        print(f"  different parameters -> 409 referencing {snap_no}")

        print("\n=== 6. range with unaccepted seq rejected, nothing left ===")
        code, err = seal(base1, dev, 1, 6, f"REQ-GAP-{run}")
        assert code == 400, (code, err)
        assert "未受理" in err["error"]
        code, missing = get_snapshot(base2, "SNP-does-not-exist")
        assert code == 404
        print(f"  gap -> HTTP 400: {err['error'][:70]}...")
        print(f"  unknown snapshot -> HTTP {code}")

        print("\n=== 7. uploads/decisions continue during snapshot traffic ===")
        for i in range(1, 4):
            code, _ = upload(port2, f"{run}-O{i}", other, 700)
            assert code == 202
        for i in range(1, 4):
            wait_released(port1, f"{run}-O{i}")
        code, sealed_other = seal(base2, other, 1, 3, f"REQ-OTHER-{run}")
        assert code == 201 and sealed_other["totals"]["released"] == 3
        code, final = seal(base1, dev, 1, 5, f"REQ-FINAL-{run}")
        assert code == 201, final
        print(f"  other device drained and sealed; final dev totals="
              f"{final['totals']}")

        print("\nALL SNAPSHOT DEMO CHECKS PASSED")
        print(f"[done] logs and raw files kept under {workdir}")
        return 0
    finally:
        api1.stop(); api2.stop()


if __name__ == "__main__":
    raise SystemExit(main())
