"""RPA流程自动化中心 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.store import store
from app.services import runtime_session, callback_hook

app = FastAPI(title="RPA流程自动化中心", version="1.0.0")


@app.on_event("startup")
def seed_config_snapshots() -> None:
    """启动时为已使用的页面模型补登配置版本，保证工作台开箱有数据。"""
    from app.services.page_model import ensure_seed_snapshots

    ensure_seed_snapshots()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# ---- 跨模块数据流 API ----

@app.get("/api/nodestats")
def nodestats(module: str | None = Query(default=None)) -> dict[str, object]:
    """节点统计：某一模块或全平台的领域快照。

    用于跨模块数据消费方查看各领域数据质量状态。
    """
    from app.services import SERVICES, service_for

    if module:
        svc = service_for(module)
        if svc is None:
            return {"error": f"未知模块: {module}"}
        return svc.domain_snapshot()

    # 全平台聚合快照
    snapshots = {}
    for key, svc in SERVICES.items():
        snapshots[key] = svc.domain_snapshot()

    # 附加非模块服务
    snapshots["runtime_session"] = runtime_session.domain_snapshot()
    snapshots["callback_hook"] = callback_hook.domain_snapshot()

    # 存储基础设施概览
    infra = {
        "channel": {
            "total_messages": store.channel.total_messages,
            "unconfirmed": store.channel.uncomfirmed_count(),
            "topics": list(set(
                m["topic"] for m in store.channel.snapshot()
            )),
        },
        "dirty_vault": store.dirty_vault.snapshot(),
        "collect_records": store.collect_records.snapshot(),
        "recovery": store.recovery.snapshot(),
    }

    return {"modules": snapshots, "infrastructure": infra}


@app.get("/api/channel/{topic}")
def channel_snapshot(topic: str) -> dict[str, object]:
    """获取指定 topic 的通道消息快照。"""
    msgs = store.channel.snapshot(topic=topic)
    latest = store.channel.latest(topic)
    return {"topic": topic, "count": len(msgs), "messages": msgs, "latest": latest}


@app.get("/api/dirty-vault")
def dirty_vault(source: str | None = Query(default=None)) -> dict[str, object]:
    """脏数据隔离区查询。"""
    if source:
        records = store.dirty_vault.list_by_source(source)
    else:
        return store.dirty_vault.snapshot()
    return {
        "source": source,
        "count": len(records),
        "records": [
            {"id": r.record_id, "batch": r.batch_ref, "reason": r.reason,
             "raw": r.raw_data, "detected_at": r.detected_at}
            for r in records
        ],
    }


@app.get("/api/collect-records")
def collect_records(batch_ref: str | None = Query(default=None),
                     source: str | None = Query(default=None)) -> dict[str, object]:
    """采集记录查询。"""
    if batch_ref:
        records = store.collect_records.list_by_batch(batch_ref)
    elif source:
        records = store.collect_records.list_by_source(source)
    else:
        return store.collect_records.snapshot()
    return {
        "count": len(records),
        "records": [
            {"id": e.entry_id, "batch": e.batch_ref, "fields": e.fields,
             "validated": e.validated, "score": e.quality_score}
            for e in records
        ],
    }


@app.get("/api/recovery")
def recovery_stats() -> dict[str, object]:
    """失败恢复状态查询。"""
    return store.recovery.snapshot()


@app.get("/api/runtime-sessions")
def runtime_sessions() -> dict[str, object]:
    """运行时会话列表。"""
    sessions = runtime_session.list_sessions()
    return {"count": len(sessions), "sessions": sessions}


# ---- 源端配置版本化 API（页面模型工作台） ----

@app.get("/api/source-configs")
def list_source_configs(
    site: str | None = Query(default=None),
    version: str | None = Query(default=None),
    model_id: int | None = Query(default=None),
    status: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    sort_by: str = Query(default="snapshot_id"),
    sort_order: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, object]:
    """页面模型工作台：按站点名称和版本查看配置版本快照。"""
    from app.services import PageModelService

    items, total = PageModelService().list_config_snapshots(
        site=site, version=version, model_id=model_id, status=status,
        keyword=keyword, sort_by=sort_by, sort_order=sort_order,
        page=page, size=size,
    )
    return {"items": items, "total": total, "page": page, "size": size}


@app.get("/api/source-configs/bindings")
def list_source_config_bindings(
    site: str | None = Query(default=None),
    version: str | None = Query(default=None),
    model_id: int | None = Query(default=None),
    batch_ref: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, object]:
    """核对每个采集批次绑定的字段映射和配置快照。"""
    from app.services import PageModelService

    items, total = PageModelService().snapshot_batch_bindings(
        site=site, version=version, model_id=model_id, batch_ref=batch_ref,
        page=page, size=size,
    )
    return {"items": items, "total": total, "page": page, "size": size}


@app.get("/api/source-configs/conflicts")
def list_source_config_conflicts(
    batch_ref: str | None = Query(default=None),
) -> dict[str, object]:
    """配置版本冲突记录（统一落到页面源配置的 7 号处理终态）。"""
    rows = store.source_configs.list_conflicts(batch_ref)
    return {"count": len(rows), "items": rows}


@app.get("/api/source-configs/batch/{batch_ref}")
def batch_source_config(batch_ref: str) -> dict[str, object]:
    """按批次回溯实际使用的配置快照、字段映射明细和结果清单。"""
    from app.services import BatchTrackerService

    tracker = BatchTrackerService()
    return {
        "batch_ref": batch_ref,
        "binding": tracker.locate_snapshot(batch_ref),
        "ledger_check": tracker.cross_check_ledger(batch_ref),
        "mapping_details": store.source_configs.list_details(batch_ref=batch_ref),
    }


@app.post("/api/source-configs/conflicts/{batch_ref}/resolve")
def resolve_source_config_conflict(batch_ref: str, payload: dict) -> dict[str, object]:
    """处理采集启动后模型再次发布导致的版本冲突。

    策略 continue（沿用旧快照）或 abort（中止并按新配置重启），
    两种方式都只保留一个配置版本绑定，重复提交只保留一条结果。
    """
    from app.services import BatchTrackerService, DataExtractService

    strategy = str(payload.get("strategy") or "").strip()
    result = DataExtractService().resolve_version_conflict(batch_ref, strategy)
    if result.get("ok"):
        tracker = BatchTrackerService()
        label = {
            "continue": "沿用旧配置快照，待继续采集",
            "abort": "已按新配置重启，待核对",
        }.get(strategy)
        if label:
            tracker.update_todo(batch_ref, label)
        result["ledger_check"] = tracker.cross_check_ledger(batch_ref)
    return result


@app.get("/api/source-configs/{snapshot_id}")
def get_source_config(snapshot_id: int) -> dict[str, object]:
    """单个配置版本快照详情（含字段映射与批次绑定）。"""
    snapshot = store.source_configs.get(snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"配置快照 {snapshot_id} 不存在")
    payload = snapshot.as_dict()
    payload["mapping_details"] = store.source_configs.list_details(
        snapshot_id=snapshot_id)
    return payload


# ---- 跨模块数据流编排入口 ----

@app.post("/api/pipeline/rpa-lifecycle")
def pipeline_rpa_lifecycle() -> dict[str, object]:
    """执行 RPA 流程全生命周期数据流。

    模拟完整的端到端数据流：bot_definition → step_editor → task_queue → page_model → data_extract → result_verify → callback_hook
    """
    from app.services import (
        BotDefinitionService, StepEditorService, TaskQueueService,
        PageModelService, DataExtractService, ResultVerifyService,
    )

    results: dict[str, Any] = {}
    errors: list[str] = []

    # 1. 发布流程
    bot_svc = BotDefinitionService()
    active_bots = bot_svc.query_active_bots()
    publish_results = []
    for bot in active_bots:
        r = bot_svc.publish_bot(bot["id"])
        if r:
            publish_results.append(r)
    results["bot_published"] = len(publish_results)

    # 2. 编排步骤
    step_svc = StepEditorService()
    enrich = step_svc.enrich_steps_from_channel()
    results["steps_enriched"] = enrich["enriched"]

    # 3. 提交到队列
    task_svc = TaskQueueService()
    queue_result = task_svc.consume_steps_to_queue()
    results["tasks_queued"] = queue_result["generated"]

    # 4. 发布源端配置
    page_svc = PageModelService()
    active_models = page_svc.list_active_models()
    config_published = 0
    for model in active_models:
        r = page_svc.publish_source_config(model["model_id"])
        if r:
            config_published += 1
    # 如果没有active模型，尝试发布已验证模型
    if config_published == 0:
        models = store.rows("page_model")
        for m in models:
            if m.get("status") in ("草稿", "已验证"):
                r = page_svc.publish_source_config(m["id"])
                if r:
                    config_published += 1
                    break
    results["source_configs_published"] = config_published

    # 5. 执行数据采集（批次与配置版本同时绑定）
    extract_svc = DataExtractService()
    extract_result = extract_svc.consume_source_and_extract()
    if not extract_result["batches"]:
        # 幂等发布不会重复广播，直接按当前有效快照启动一个采集批次
        for model in page_svc.list_active_models():
            snapshot_id = extract_svc.active_snapshot_id(model["model_id"])
            if snapshot_id is None:
                continue
            batch_ref = f"BATC-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-P{snapshot_id}"
            direct = extract_svc.extract_batch(snapshot_id, batch_ref)
            if direct.get("ok"):
                extract_result["extracted"] += direct["extracted"]
                extract_result["dirty"] += direct["dirty"]
                extract_result["batches"].append(direct)
            break
    results["data_extracted"] = extract_result
    results["config_versions"] = len(store.source_configs.all())
    results["mapping_details"] = len(store.source_configs.list_details())

    # 6. 校验采集结果
    verify_svc = ResultVerifyService()
    verify_result = verify_svc.verify_collected_data()
    results["validation"] = verify_result

    # 7. 触发回调
    cb_result = callback_hook.consume_validation_and_callback()
    results["callbacks"] = cb_result

    # 8. 标记任务完成
    scheduled = task_svc.schedule_next_batch(max_tasks=3)
    results["tasks_scheduled"] = scheduled["scheduled"]

    return {"ok": True, "results": results, "errors": errors if errors else None}


@app.post("/api/pipeline/crawl-schedule")
def pipeline_crawl_schedule() -> dict[str, object]:
    """触发调度采集流程。"""
    from app.services import SchedulerService
    sched_svc = SchedulerService()
    result = sched_svc.schedule_crawl_jobs()
    return {"ok": True, "scheduled": result}


@app.post("/api/pipeline/element-execution")
def pipeline_element_execution() -> dict[str, object]:
    """触发元素定位与自动化执行流程。"""
    result = runtime_session.consume_strategies_and_execute()
    return {"ok": True, "result": result}


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)