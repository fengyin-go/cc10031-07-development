"""批次追踪领域服务 — 追踪采集批次状态，输出 domain_snapshot。

数据工程核心：数据入湖存储与分区管理 — 跨模块数据产物

版本化约束:
    采集批次与源端配置版本同时绑定；模型再次发布不会覆盖已开始批次。
    批次进度、待办、台账与结果清单都按绑定的 snapshot 回溯。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import SOURCE_CONFIG_TERMINAL_STATES, store


class BatchTrackerService(BaseModuleService):
    MODULE = "batch_tracker"
    ENTITY = "采集批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "触发流程", "采集时间", "目标条数", "成功条数", "遗漏条数", "校验状态"]
    REQUIRED_FIELDS = ["批次编号", "触发流程", "采集时间"]
    STATUS_ORDER = ["收集中", "已收集", "校验中", "已完成", "有遗漏"]
    ACTION_RULES = {"发起校验": "已收集", "确认完成": "有遗漏"}
    NEGATIVE_ACTIONS = []

    def track_batch_progress(self, batch_ref: str) -> dict[str, Any]:
        """追踪指定批次的进度（跨模块聚合，附带配置版本快照）。"""
        # 采集记录统计
        collect_records = store.collect_records.list_by_batch(batch_ref)
        # 脏数据统计
        dirty_records = store.dirty_vault.list_by_batch(batch_ref)
        # 校验事件
        validation_events = [
            e for e in store.channel.snapshot(topic="rpa.validation_complete")
            if e.get("payload", {}).get("batch_ref") == batch_ref
        ]
        snapshot = self.snapshot_for_batch(batch_ref)
        return {
            "batch_ref": batch_ref,
            "collected": len(collect_records),
            "dirty": len(dirty_records),
            "validated_events": len(validation_events),
            "completion_rate": round(
                len(collect_records) / max(len(collect_records) + len(dirty_records), 1), 2),
            "snapshot_id": snapshot["snapshot_id"] if snapshot else None,
            "snapshot_version": snapshot["snapshot_version"] if snapshot else None,
            "field_mapping": snapshot["field_mapping"] if snapshot else [],
        }

    # ---- 采集批次与源端配置版本的双向绑定 ----

    def locate_snapshot(
        self,
        batch_ref: str,
    ) -> dict[str, Any] | None:
        """按批次定位实际使用的源端配置版本。"""
        for snapshot in store.source_configs.all():
            if batch_ref in snapshot.bound_batches:
                return snapshot.as_dict()
        return None

    def snapshot_for_batch(self, batch_ref: str) -> dict[str, Any] | None:
        """返回批次绑定的快照与字段映射明细（台账与结果清单联动）。"""
        snapshot = self.locate_snapshot(batch_ref)
        if snapshot is None:
            return None
        return {
            **snapshot,
            "field_mapping": store.source_configs.list_details(
                batch_ref=batch_ref, snapshot_id=snapshot["snapshot_id"]),
        }

    def start_batch(
        self,
        *,
        batch_no: str,
        flow_name: str,
        model_id: int,
        collect_time: str | None = None,
    ) -> dict[str, Any]:
        """发起采集批次：同时绑定当前有效的源端配置版本。

        如果模型已经再次发布，新批次绑定新版本；已开始的批次保持旧版本。
        """
        existing = self.locate_snapshot(batch_no)
        if existing is not None:
            # 已开始的批次继续使用原配置版本，不因模型再次发布而改绑
            self._ensure_tracker_row(batch_no, flow_name, collect_time)
            return {
                "ok": True,
                "reused": True,
                "batch_ref": batch_no,
                "snapshot_id": existing["snapshot_id"],
                "snapshot_ref": existing["snapshot_ref"],
                "snapshot_version": existing["snapshot_version"],
                "message": "批次已绑定配置版本，沿用原快照",
            }

        snapshot = store.source_configs.active_for_model(model_id)
        if snapshot is None:
            return {"ok": False, "message": f"页面模型 {model_id} 没有可用配置版本"}
        bound, message, snapshot = store.source_configs.bind_batch(
            snapshot.snapshot_id, batch_no)
        if not bound:
            return {"ok": False, "message": message}

        entry = self._ensure_tracker_row(batch_no, flow_name, collect_time)
        entry["配置快照"] = snapshot.snapshot_ref
        entry["配置版本"] = snapshot.snapshot_version
        entry["模型ID"] = model_id
        entry["站点"] = snapshot.target_site
        entry["待办"] = "待校验"
        return {
            "ok": True,
            "batch_ref": batch_no,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_ref": snapshot.snapshot_ref,
            "snapshot_version": snapshot.snapshot_version,
            "message": message,
        }

    def _ensure_tracker_row(
        self,
        batch_no: str,
        flow_name: str,
        collect_time: str | None,
    ) -> dict[str, Any]:
        """确保采集批次台账里有一行，重复提交不新增。"""
        rows = store.rows(self.MODULE)
        entry = next(
            (row for row in rows if row.get("批次编号") == batch_no), None)
        if entry is not None:
            return entry
        entry = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "批次编号": batch_no,
            "触发流程": flow_name,
            "采集时间": collect_time or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "目标条数": 0,
            "成功条数": 0,
            "遗漏条数": 0,
            "校验状态": "收集中",
            "status": "收集中",
            "pending": True,
            "abnormal": False,
        }
        rows.append(entry)
        return entry

    def update_todo(self, batch_ref: str, todo: str) -> dict[str, Any] | None:
        """配置版本变化时同步更新另一个业务入口的待办。"""
        entry = next(
            (row for row in store.rows(self.MODULE)
             if row.get("批次编号") == batch_ref), None)
        if entry is None:
            return None
        entry["待办"] = todo
        entry["pending"] = todo not in ("", "已完成")
        return entry

    def cross_check_ledger(self, batch_ref: str) -> dict[str, Any]:
        """处理台账与结果清单联动核对。"""
        snapshot = self.locate_snapshot(batch_ref)
        details = store.source_configs.list_details(batch_ref=batch_ref)
        collect_records = store.collect_records.list_by_batch(batch_ref)
        dirty_records = store.dirty_vault.list_by_batch(batch_ref)
        detail_fields = {item["field"] for item in details}
        record_fields = set()
        for entry in collect_records:
            record_fields.update(entry.fields.keys())
        record_fields.discard("_seq")
        conflicts = store.source_configs.list_conflicts(batch_ref)
        return {
            "batch_ref": batch_ref,
            "snapshot_ref": snapshot["snapshot_ref"] if snapshot else None,
            "snapshot_version": snapshot["snapshot_version"] if snapshot else None,
            "detail_count": len(details),
            "collect_count": len(collect_records),
            "dirty_count": len(dirty_records),
            "detail_fields": sorted(detail_fields),
            "record_fields": sorted(record_fields),
            "fields_missing_in_records": sorted(detail_fields - record_fields),
            "fields_not_in_details": sorted(record_fields - detail_fields),
            "consistent": not (detail_fields - record_fields),
            "conflicts": len(conflicts),
            "source_config_terminal_state": (
                conflicts[0]["source_config_terminal_state"] if conflicts
                else (snapshot["terminal_state"] if snapshot else None)
            ),
            "source_config_terminal_label": (
                conflicts[0]["source_config_terminal_label"] if conflicts
                else (SOURCE_CONFIG_TERMINAL_STATES.get(snapshot["terminal_state"])
                      if snapshot else None)
            ),
        }

    def list_batch_bindings(
        self,
        *,
        site: str | None = None,
        version: str | None = None,
        model_id: int | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """采集批次按站点和模型版本定位实际使用的配置。"""
        snapshots = store.source_configs.all()
        rows: list[dict[str, Any]] = []
        for snapshot in snapshots:
            if site and site.casefold() not in snapshot.target_site.casefold():
                continue
            if version and version.casefold() not in snapshot.snapshot_version.casefold():
                continue
            if model_id is not None and snapshot.model_id != model_id:
                continue
            for batch_ref in snapshot.bound_batches:
                tracker = next(
                    (row for row in store.rows(self.MODULE)
                     if row.get("批次编号") == batch_ref), None)
                rows.append({
                    "batch_ref": batch_ref,
                    "site": snapshot.target_site,
                    "model_id": snapshot.model_id,
                    "model_version": snapshot.model_version,
                    "snapshot_id": snapshot.snapshot_id,
                    "snapshot_ref": snapshot.snapshot_ref,
                    "snapshot_version": snapshot.snapshot_version,
                    "processing_item": snapshot.processing_item,
                    "terminal_state": snapshot.terminal_state,
                    "terminal_state_label": SOURCE_CONFIG_TERMINAL_STATES.get(
                        snapshot.terminal_state),
                    "batch_status": tracker.get("status") if tracker else None,
                    "todo": tracker.get("待办") if tracker else None,
                    "field_mapping": store.source_configs.list_details(
                        batch_ref=batch_ref, snapshot_id=snapshot.snapshot_id),
                })
        rows.sort(key=lambda row: (row["snapshot_id"], row["batch_ref"]), reverse=True)
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        bound = sum(1 for snapshot in store.source_configs.all() if snapshot.bound_batches)
        return {
            **base,
            "completed_batches": sum(1 for r in rows if r.get("status") == "已完成"),
            "missed_batches": sum(1 for r in rows if r.get("status") == "有遗漏"),
            "in_progress": sum(1 for r in rows if r.get("status") in ("收集中", "已收集", "校验中")),
            "config_versions_bound": bound,
            "version_conflicts": len(store.source_configs.list_conflicts()),
            "batches_with_todo": sum(1 for r in rows if r.get("待办")),
        }
