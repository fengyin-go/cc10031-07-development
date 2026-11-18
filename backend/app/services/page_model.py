"""页面建模领域服务 — 源端结构定义、发布与配置版本快照。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    page_model (定义源端结构) → 发布时冻结配置版本快照
    → channel("rpa.source_configured") → data_extract 消费
    → channel("rpa.data_collected")

版本化约束:
    - 每次发布按「模型 + 站点 + 版本 + 结构 + 推断字段」登记一个快照，重复发布幂等；
    - 新版本发布只冻结旧快照承接新批次的能力，不覆盖已开始批次的字段口径；
    - 批次与配置版本一一绑定，采集明细按快照回溯字段映射。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import normalize_source_version, store

# 页面结构关键词 → 可提取字段及字段映射口径
STRUCTURE_FIELD_RULES: dict[str, list[dict[str, str]]] = {
    "表格": [
        {"field": "序号", "source_path": "table > tbody > tr > td[1]", "data_type": "整数"},
        {"field": "名称", "source_path": "table > tbody > tr > td[2]", "data_type": "文本"},
        {"field": "状态", "source_path": "table > tbody > tr > td[3]", "data_type": "枚举"},
        {"field": "金额", "source_path": "table > tbody > tr > td[4]", "data_type": "数值"},
        {"field": "日期", "source_path": "table > tbody > tr > td[5]", "data_type": "日期"},
    ],
    "列表": [
        {"field": "标题", "source_path": "ul > li > .title", "data_type": "文本"},
        {"field": "链接", "source_path": "ul > li > a[href]", "data_type": "链接"},
        {"field": "摘要", "source_path": "ul > li > .summary", "data_type": "文本"},
        {"field": "时间", "source_path": "ul > li > .time", "data_type": "日期时间"},
    ],
    "表单": [
        {"field": "字段名", "source_path": "form label", "data_type": "文本"},
        {"field": "字段值", "source_path": "form input[name]", "data_type": "文本"},
        {"field": "类型", "source_path": "form select[name=type]", "data_type": "枚举"},
        {"field": "必填", "source_path": "form [required]", "data_type": "布尔"},
    ],
    "详情": [
        {"field": "标题", "source_path": "article > h1", "data_type": "文本"},
        {"field": "内容", "source_path": "article > .content", "data_type": "富文本"},
        {"field": "标签", "source_path": "article > .tags > span", "data_type": "列表"},
        {"field": "发布日期", "source_path": "article > .publish-date", "data_type": "日期"},
    ],
}

DEFAULT_FIELD_RULES: list[dict[str, str]] = [
    {"field": "数据项1", "source_path": "main > .item[1]", "data_type": "文本"},
    {"field": "数据项2", "source_path": "main > .item[2]", "data_type": "文本"},
    {"field": "数据项3", "source_path": "main > .item[3]", "data_type": "文本"},
]


class PageModelService(BaseModuleService):
    MODULE = "page_model"
    ENTITY = "页面模型"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["模型名称", "目标站点", "页面结构", "动态元素", "分页模式", "加载策略", "版本号"]
    REQUIRED_FIELDS = ["模型名称", "目标站点", "页面结构"]
    STATUS_ORDER = ["草稿", "已验证", "已使用", "结构失效"]
    ACTION_RULES = {"验证模型": "草稿", "标记失效": "已验证"}
    NEGATIVE_ACTIONS = []
    # 升级模型时保留的版本回溯字段，不允许被动作直接抹掉
    VERSIONED_FIELDS = ("版本号",)

    # ---- 跨模块业务方法 (source_config 角色) ----

    def publish_source_config(self, model_id: int) -> dict[str, Any] | None:
        """发布页面模型作为源端配置，并冻结一个可回溯的配置版本快照。

        读取页面模型字段（目标站点、页面结构、动态元素、分页模式、加载策略、
        版本号），解析字段推断结果并生成源端配置快照。发布只影响后续批次：
        已开始批次继续使用其绑定的旧快照。

        Args:
            model_id: 页面模型 ID

        Returns:
            channel 消息概要或 None
        """
        entry = store.find(self.MODULE, model_id)
        if entry is None:
            return None

        current = entry.get("status")
        if current in ("结构失效", "已停用"):
            return None

        field_mapping = self._infer_field_mapping(entry)
        snapshot, reused = store.source_configs.publish(
            model_id=model_id,
            model_name=entry.get("模型名称", ""),
            target_site=entry.get("目标站点", ""),
            model_version=entry.get("版本号"),
            page_structure=entry.get("页面结构", ""),
            dynamic_elements=entry.get("动态元素", ""),
            pagination_mode=entry.get("分页模式", "none"),
            load_strategy=entry.get("加载策略", "lazy"),
            inferred_fields=[item["field"] for item in field_mapping],
            field_mapping=field_mapping,
        )

        entry["status"] = "已使用"
        entry["pending"] = False
        entry["配置版本"] = snapshot.snapshot_version
        entry["当前快照"] = snapshot.snapshot_ref

        if reused:
            # 幂等发布：不重复生成快照，也不重复广播
            return {
                "model_id": model_id,
                "status": "已使用",
                "reused": True,
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_ref": snapshot.snapshot_ref,
                "snapshot_version": snapshot.snapshot_version,
                "target_site": snapshot.target_site,
                "inferred_fields": len(snapshot.inferred_fields),
            }

        source_config = {
            "model_id": model_id,
            "model_name": snapshot.model_name,
            "target_site": snapshot.target_site,
            "page_structure": snapshot.page_structure,
            "dynamic_elements": snapshot.dynamic_elements,
            "pagination_mode": snapshot.pagination_mode,
            "load_strategy": snapshot.load_strategy,
            "version": snapshot.model_version,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_ref": snapshot.snapshot_ref,
            "snapshot_version": snapshot.snapshot_version,
            "processing_item": snapshot.processing_item,
            "fields_to_extract": list(snapshot.inferred_fields),
            "field_mapping": [dict(item) for item in snapshot.field_mapping],
        }

        msg = store.channel.publish(
            topic="rpa.source_configured",
            source=self.MODULE,
            payload=source_config,
        )
        return {
            "model_id": model_id,
            "status": "已使用",
            "reused": False,
            "msg_id": msg.msg_id,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_ref": snapshot.snapshot_ref,
            "snapshot_version": snapshot.snapshot_version,
            "target_site": snapshot.target_site,
            "inferred_fields": len(snapshot.inferred_fields),
        }

    def _infer_extractable_fields(self, entry: dict[str, Any]) -> list[str]:
        """从页面结构推断可提取字段（基于结构描述关键词）。"""
        return [item["field"] for item in self._infer_field_mapping(entry)]

    def _infer_field_mapping(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """解析页面结构 + 动态元素，产出字段推断结果（字段映射口径）。

        推断结果同时写入配置快照，后续批次的采集明细按同一口径生成。
        """
        structure = f"{entry.get('页面结构', '')}{entry.get('动态元素', '')}"
        rules = DEFAULT_FIELD_RULES
        matched_keyword = ""
        for keyword, candidates in STRUCTURE_FIELD_RULES.items():
            if keyword in structure:
                rules = candidates
                matched_keyword = keyword
                break

        pagination = entry.get("分页模式", "none")
        load_strategy = entry.get("加载策略", "lazy")
        mapping: list[dict[str, Any]] = []
        for rule in rules:
            mapping.append({
                "field": rule["field"],
                "source_path": rule["source_path"],
                "target_field": rule["field"],
                "data_type": rule["data_type"],
                "inferred": True,
                "inference_basis": matched_keyword or "默认结构",
                "pagination": pagination,
                "load_strategy": load_strategy,
            })
        return mapping

    def list_active_models(self) -> list[dict[str, Any]]:
        """查询所有已使用状态的页面模型。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "model_id": r.get("id"), "name": r.get("模型名称"),
                "site": r.get("目标站点"), "structure": r.get("页面结构"),
                "pagination": r.get("分页模式"), "version": r.get("版本号"),
                "snapshot_ref": r.get("当前快照"),
            }
            for r in rows if r.get("status") == "已使用"
        ]

    def validate_model_structure(self, model_id: int) -> dict[str, Any]:
        """验证页面模型结构完整性，标记潜在问题。

        Returns:
            {"model_id": int, "valid": bool, "issues": [...]}
        """
        entry = store.find(self.MODULE, model_id)
        if entry is None:
            return {"model_id": model_id, "valid": False, "issues": ["模型不存在"]}

        issues = []
        if not entry.get("目标站点", "").strip():
            issues.append("目标站点为空")
        if not entry.get("页面结构", "").strip():
            issues.append("页面结构未定义")
        if entry.get("分页模式", "none") == "none":
            issues.append("分页模式未配置")

        return {
            "model_id": model_id,
            "model_name": entry.get("模型名称"),
            "valid": len(issues) == 0,
            "issues": issues,
        }

    # ---- 页面模型工作台：按站点名称与版本核对批次绑定 ----

    def list_config_snapshots(
        self,
        *,
        site: str | None = None,
        version: str | None = None,
        model_id: int | None = None,
        status: str | None = None,
        keyword: str | None = None,
        sort_by: str = "snapshot_id",
        sort_order: str = "desc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """按站点名称、版本筛选配置快照台账（含字段映射与批次绑定）。"""
        return store.source_configs.list_snapshots(
            site=site, version=version, model_id=model_id, status=status,
            keyword=keyword, sort_by=sort_by, sort_order=sort_order,
            page=page, size=size,
        )

    def snapshot_batch_bindings(
        self,
        *,
        site: str | None = None,
        version: str | None = None,
        model_id: int | None = None,
        batch_ref: str | None = None,
        sort_by: str = "snapshot_id",
        sort_order: str = "desc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """核对每个采集批次绑定的字段映射和配置快照。

        一个批次只会出现在一个配置版本下；模型再次发布不会改变已开始批次。
        """
        snapshots, _ = store.source_configs.list_snapshots(
            site=site, version=version, model_id=model_id,
            sort_by=sort_by, sort_order=sort_order, page=1, size=1_000_000,
        )
        rows: list[dict[str, Any]] = []
        for snap in snapshots:
            details = store.source_configs.list_details(snapshot_id=snap["snapshot_id"])
            details_by_batch: dict[str, list[dict[str, Any]]] = {}
            for detail in details:
                details_by_batch.setdefault(detail["batch_ref"], []).append(detail)
            for ref, bound_details in details_by_batch.items():
                rows.append({
                    "batch_ref": ref,
                    "snapshot_id": snap["snapshot_id"],
                    "snapshot_ref": snap["snapshot_ref"],
                    "snapshot_version": snap["snapshot_version"],
                    "model_id": snap["model_id"],
                    "model_version": snap["model_version"],
                    "site": snap["target_site"],
                    "processing_item": snap["processing_item"],
                    "field_mapping": bound_details,
                    "collect_records": len(store.collect_records.list_by_batch(ref)),
                    "dirty_records": len(store.dirty_vault.list_by_batch(ref)),
                })
        # 未产生字段映射明细的已绑定批次也要可见
        for snap in snapshots:
            for ref in snap["bound_batches"]:
                if any(row["batch_ref"] == ref for row in rows):
                    continue
                rows.append({
                    "batch_ref": ref,
                    "snapshot_id": snap["snapshot_id"],
                    "snapshot_ref": snap["snapshot_ref"],
                    "snapshot_version": snap["snapshot_version"],
                    "model_id": snap["model_id"],
                    "model_version": snap["model_version"],
                    "site": snap["target_site"],
                    "processing_item": snap["processing_item"],
                    "field_mapping": [],
                    "collect_records": len(store.collect_records.list_by_batch(ref)),
                    "dirty_records": len(store.dirty_vault.list_by_batch(ref)),
                })
        if batch_ref:
            rows = [row for row in rows if batch_ref in row["batch_ref"]]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        active = self.list_active_models()
        channel_events = store.channel.snapshot(topic="rpa.source_configured")
        snapshots = store.source_configs.all()
        return {
            **base,
            "active_models": len(active),
            "source_config_events": len(channel_events),
            "config_snapshots": len(snapshots),
            "snapshots_in_use": sum(
                1 for snap in snapshots if snap.terminal_state in (3, 4)),
            "snapshot_conflicts": sum(
                1 for snap in snapshots if snap.terminal_state == 7),
        }


def ensure_seed_snapshots() -> int:
    """演示数据：为已使用的页面模型补登配置版本快照（幂等）。

    只写快照台账，不改模型状态、不广播 channel，避免影响既有数据流。
    """
    service = PageModelService()
    created = 0
    for entry in store.rows("page_model"):
        if entry.get("status") != "已使用":
            continue
        mapping = service._infer_field_mapping(entry)
        _, reused = store.source_configs.publish(
            model_id=int(entry.get("id", 0)),
            model_name=entry.get("模型名称", ""),
            target_site=entry.get("目标站点", ""),
            model_version=entry.get("版本号"),
            page_structure=entry.get("页面结构", ""),
            dynamic_elements=entry.get("动态元素", ""),
            pagination_mode=entry.get("分页模式", "none"),
            load_strategy=entry.get("加载策略", "lazy"),
            inferred_fields=[item["field"] for item in mapping],
            field_mapping=mapping,
        )
        if not reused:
            created += 1
    return created

