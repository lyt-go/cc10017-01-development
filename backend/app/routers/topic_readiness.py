"""主题上线就绪数据包接口：生成、续做、裁决与按业务场景查询。

约定：主题注册页发起上线检查，消息接入页查看同一版本的就绪结果。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, PageResult
from app.services.topic_readiness import SOURCE_FIRST, topic_readiness_service

router = APIRouter(prefix="/api/topic_readiness", tags=["主题上线就绪"])

STATUSES = ["就绪", "校验失败", "检查中断", "冲突待裁决"]
SORT_OPTIONS = ["version_desc", "version_asc"]


@router.get("", response_model=PageResult[dict])
def list_packages(
    scenario: str | None = Query(default=None, description="按业务场景筛选"),
    source: str | None = Query(default=None, description="首次上线、补报"),
    status: str | None = Query(default=None, description="就绪、校验失败、检查中断、冲突待裁决"),
    keyword: str | None = Query(default=None, description="按就绪包编号、主题编号、主题名称或消费组检索"),
    sort: str = Query(default="version_desc", description="version_desc 或 version_asc"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按筛选条件返回就绪包，附带业务场景分组，可按版本排列。"""
    items, total, _ = topic_readiness_service.list_packages(
        scenario=scenario, source=source, status=status, keyword=keyword,
        sort=sort if sort in SORT_OPTIONS else "version_desc", page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/scenarios", response_model=dict)
def list_scenarios() -> dict:
    """业务场景分组，供视图并列展示来源、版本与概览。"""
    _, total, scenarios = topic_readiness_service.list_packages(page=1, size=1)
    return {"items": scenarios, "total": total}


@router.get("/by-topic/{topic_code}", response_model=dict)
def get_by_topic(topic_code: str, package_version: str | None = Query(default=None)) -> dict:
    """按主题编号（可带就绪包版本）取最近一份就绪包。"""
    package = topic_readiness_service.latest_for_topic(topic_code, package_version)
    if package is None:
        raise HTTPException(status_code=404, detail=f"主题 {topic_code} 尚无上线就绪数据包")
    return topic_readiness_service.decorate(package)


@router.get("/by-connector/{connector_id}", response_model=dict)
def get_by_connector(connector_id: int) -> dict:
    """消息接入页按接入配置读取同一版本的就绪结果。"""
    package = topic_readiness_service.latest_for_connector(connector_id)
    if package is None:
        raise HTTPException(status_code=404, detail=f"接入配置 {connector_id} 尚无上线就绪数据包")
    return topic_readiness_service.decorate(package)


@router.get("/{entry_id}", response_model=dict)
def get_package(entry_id: int) -> dict:
    """读取单份就绪包：主题版本与接收端版本可逐项回查。"""
    package = topic_readiness_service.get_package(entry_id)
    if package is None:
        raise HTTPException(status_code=404, detail=f"就绪包 {entry_id} 不存在")
    return topic_readiness_service.decorate(package)


@router.post("/check", response_model=ActionResult)
def run_check(payload: dict) -> ActionResult:
    """主题注册页发起上线检查：写入主题配置与接收端版本同一发布边界的就绪包。"""
    topic_code = str(payload.get("topic_code") or "").strip()
    if not topic_code:
        return ActionResult(ok=False, message="缺少 topic_code")
    batch_limit = payload.get("batch_limit")
    if batch_limit is not None:
        try:
            batch_limit = int(batch_limit)
        except (TypeError, ValueError):
            return ActionResult(ok=False, message="batch_limit 必须是整数")
    package, message = topic_readiness_service.check(
        topic_code,
        source=str(payload.get("source") or SOURCE_FIRST),
        scenario=payload.get("scenario"),
        conflict_policy=payload.get("conflict_policy"),
        batch_limit=batch_limit,
        topic_version=payload.get("topic_version"),
        receiver_version=payload.get("receiver_version"),
    )
    if package is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=package)


@router.post("/{entry_id}/resume", response_model=ActionResult)
def resume_check(entry_id: int, payload: dict | None = None) -> ActionResult:
    """处理中断后续做：只补齐未完成批次，已完成检查项保持原样。"""
    body = payload or {}
    package, message = topic_readiness_service.resume(
        entry_id,
        batch_limit=body.get("batch_limit"),
        conflict_policy=body.get("conflict_policy"),
    )
    if package is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=package)


@router.post("/{entry_id}/resolve", response_model=ActionResult)
def resolve_conflict(entry_id: int, payload: dict | None = None) -> ActionResult:
    """补报冲突裁决：确认主题版本与接收端版本先后，仍只保留一份可消费就绪包。"""
    body = payload or {}
    package, message = topic_readiness_service.resolve_conflict(
        entry_id, body.get("conflict_policy"))
    if package is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=package)
