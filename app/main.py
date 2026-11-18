"""FastAPI application: intake, decision query and data-lake read API.

Run two instances behind the load balancer defined in docker-compose.yml;
each instance serves HTTP and runs one background decision worker.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, Response, UploadFile, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import settings
from .csvutil import CsvValidationError, parse_thickness_csv
from .db import close_pool, create_pool
from .service import (
    MAX_SNAPSHOT_BATCHES,
    ConflictError,
    SnapshotValidationError,
    accept_submission,
    create_snapshot,
    get_batch_detail,
    get_released_data,
    get_snapshot,
    list_device_batches,
)
from .storage import persist_original
from .worker import DecisionWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("fab.api")
MAX_UPLOAD_BYTES = 8 * 1024 * 1024


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await create_pool()
    worker = DecisionWorker(pool, settings.instance_id)
    worker.start()
    app.state.worker = worker
    try:
        yield
    finally:
        await worker.stop()
        await close_pool()


app = FastAPI(
    title="晶圆厚度批次放行 API / Wafer Thickness Batch Release API",
    version="1.0.0",
    description=(
        "厚度明细入湖前的漂移批次隔离服务。\n\n"
        "同设备按持久化受理顺序判定：前 3 批放行；其后批中位数与最近 3 个"
        "已放行批中位数均值相差超过 5nm 则隔离，隔离批不进入参考窗口。"
        "双实例后台处理，同设备严格 FIFO、不阻塞其他设备。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(CsvValidationError)
async def _csv_error_handler(_request, exc: CsvValidationError):
    return JSONResponse(status_code=400, content={"error": str(exc)})


@app.exception_handler(ConflictError)
async def _conflict_handler(_request, exc: ConflictError):
    return JSONResponse(
        status_code=409,
        content={"error": str(exc), "existing": exc.existing},
    )


@app.exception_handler(SnapshotValidationError)
async def _snapshot_validation_handler(_request, exc: SnapshotValidationError):
    return JSONResponse(status_code=400, content={"error": str(exc)})


@app.get("/health")
async def health():
    from .db import get_pool
    try:
        async with get_pool().acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ok = True
    except Exception:  # pragma: no cover
        db_ok = False
    return {"status": "ok" if db_ok else "degraded",
            "instance_id": settings.instance_id, "database": db_ok}


@app.post("/batches", status_code=status.HTTP_202_ACCEPTED,
          summary="提交一批晶圆厚度 CSV（原件持久化后受理）")
async def submit_batch(
    batch_no: str = Form(..., description="全局唯一批号"),
    device_id: str = Form(..., description="设备号"),
    file: UploadFile = File(..., description="CSV：晶圆号,厚度nm"),
):
    batch_no = batch_no.strip()
    device_id = device_id.strip()
    if not batch_no or not device_id:
        raise HTTPException(400, "batch_no 与 device_id 不能为空 / fields required")

    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "CSV 超过 8MB 上限 / file too large")

    parsed = parse_thickness_csv(raw)          # 400 on any structural violation
    artifact = persist_original(raw)           # durable original before acceptance
    acceptance = await accept_submission(
        get_pool_safe(),
        batch_no=batch_no, device_id=device_id,
        raw=raw, parsed=parsed, artifact=artifact,
    )
    return {
        "batch_no": acceptance.batch_no,
        "device_id": acceptance.device_id,
        "accept_seq": acceptance.accept_seq,
        "batch_id": acceptance.batch_id,
        "status": acceptance.status,
        "reused_retry": acceptance.reused,
        "wafer_count": parsed.count,
        "sha256": artifact.sha256,
        "message": ("同原件重试，复用既有受理 / identical retry reused"
                    if acceptance.reused
                    else "已受理，等待后台判定 / accepted, pending decision"),
    }


@app.get("/batches/{batch_no}", summary="查询判定结果及参考批号")
async def query_batch(batch_no: str):
    detail = await get_batch_detail(get_pool_safe(), batch_no)
    if detail is None:
        raise HTTPException(404, f"批号不存在 / batch not found: {batch_no}")
    return detail


@app.get("/devices/{device_id}/batches", summary="按设备列出受理顺序与判定")
async def device_batches(device_id: str):
    rows = await list_device_batches(get_pool_safe(), device_id)
    return {"device_id": device_id, "count": len(rows), "batches": rows}


@app.get("/data/released", summary="数据湖供数接口（仅放行明细，隔离批不可见）")
async def released_data(
    device_id: Optional[str] = Query(None),
    batch_no: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    total, items = await get_released_data(
        get_pool_safe(), device_id=device_id, batch_no=batch_no,
        limit=limit, offset=offset)
    return {"total_released": total, "limit": limit, "offset": offset, "items": items}


class SnapshotRequest(BaseModel):
    """Seal request: a contiguous accepted-sequence range of one device."""

    device_id: str = Field(..., min_length=1, max_length=200,
                           description="设备号")
    start_accept_seq: int = Field(..., ge=1,
                                  description="区间起始受理序号（含）")
    end_accept_seq: int = Field(..., ge=1,
                                description="区间结束受理序号（含，最多20批）")
    request_no: str = Field(..., min_length=1, max_length=200,
                            description="请求号（幂等键；同号异参返回 409）")


class SnapshotTotals(BaseModel):
    batches: int = Field(..., description="区间内批数")
    released: int = Field(..., description="放行批数")
    quarantined: int = Field(..., description="隔离批数")
    pending: int = Field(..., description="封存时待判定批数")
    wafers: int = Field(..., description="放行批晶圆明细总片数")


class SnapshotBatch(BaseModel):
    accept_seq: int
    batch_no: str
    status: str = Field(..., description="RELEASED / QUARANTINED / PENDING")
    median_nm: Optional[float] = Field(
        None, description="当时中位数；仅放行批")
    reference_mean_nm: Optional[float] = Field(
        None, description="当时参考中位数均值；仅放行批且非基线批")
    deviation_nm: Optional[float] = Field(
        None, description="当时偏差；仅放行批且非基线批")
    reference_batch_nos: list = Field(
        [], description="当时参考批号（按判定时顺序）；仅放行批")
    reference_medians_nm: list = Field(
        [], description="当时参考批各自的中位数；仅放行批")
    wafer_count: Optional[int] = Field(
        None, description="放行批片数；待判定/隔离批为 null")
    wafers: list = Field([], description="放行批晶圆明细；其他状态为空")


class SnapshotSealed(BaseModel):
    snapshot_no: str = Field(..., description="快照号，用于后续按号读取")
    request_no: str
    device_id: str
    start_accept_seq: int
    end_accept_seq: int
    batch_count: int
    totals: SnapshotTotals
    created_at: Optional[str] = None
    created_by: Optional[str] = Field(None, description="封存实例 ID")
    batches: list[SnapshotBatch]


@app.post(
    "/snapshots", status_code=status.HTTP_201_CREATED,
    response_model=SnapshotSealed,
    summary="封存设备一段批次的供数快照（同请求号幂等）",
    description=(
        "从同一已提交状态封存设备连续受理序号区间（1–20 批）。"
        "区间含未受理序号返回 400。放行批记录当时结论、晶圆明细、"
        "参考批号与参考中位数；待判定/隔离批只记录状态。\n\n"
        "同一 `request_no` 重复提交（含并发与响应丢失后的重试）返回同一"
        "快照（200）；参数不同返回 409。失败时不留下任何可读的半份结果。"
    ),
)
async def seal_snapshot(body: SnapshotRequest, response: Response):
    if body.end_accept_seq < body.start_accept_seq:
        raise SnapshotValidationError(
            "结束序号必须不小于起始序号 / end_accept_seq must be >= start")
    if body.end_accept_seq - body.start_accept_seq + 1 > MAX_SNAPSHOT_BATCHES:
        raise SnapshotValidationError(
            f"单次快照最多 {MAX_SNAPSHOT_BATCHES} 批 / too many batches")
    created = await create_snapshot(
        get_pool_safe(),
        device_id=body.device_id,
        start_seq=body.start_accept_seq,
        end_seq=body.end_accept_seq,
        request_no=body.request_no,
        created_by=settings.instance_id,
    )
    # A replayed request (retry / concurrent duplicate) yields the sealed
    # snapshot unchanged and 200, so callers can tell it apart from a new seal.
    response.status_code = (status.HTTP_200_OK if created.replayed
                            else status.HTTP_201_CREATED)
    return created.payload


@app.get("/snapshots/{snapshot_no}", response_model=SnapshotSealed,
         summary="按快照号读取封存内容（与创建后不再变化）")
async def read_snapshot(snapshot_no: str):
    payload = await get_snapshot(get_pool_safe(), snapshot_no)
    if payload is None:
        raise HTTPException(404, f"快照不存在 / snapshot not found: {snapshot_no}")
    return payload


def get_pool_safe():
    from .db import get_pool
    return get_pool()


@app.post("/internal/shutdown", include_in_schema=False)
async def internal_shutdown(token: str = Query(...)):
    """Graceful shutdown hook for the crash-takeover demonstration.

    Disabled unless ``SHUTDOWN_TOKEN`` is configured; the caller must present
    the same token.  In production simply leave the variable unset.
    """
    import os
    import signal
    expected = os.environ.get("SHUTDOWN_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(404, "not found")
    worker = getattr(app.state, "worker", None)
    if worker is not None:
        await worker.stop()
    await close_pool()
    try:
        os.kill(os.getpid(), signal.SIGTERM)
    except PermissionError:  # pragma: no cover - sandboxed demo environments
        os._exit(0)
    return {"status": "shutting down"}
