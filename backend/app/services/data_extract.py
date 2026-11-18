"""数据提取领域服务 — 消费源端配置 + 字段映射，执行数据采集。

数据工程核心：数据归档存储与快照管理 — 跨模块数据产物

数据流:
    channel("rpa.source_configured") ← page_model
    → data_extract 消费 → 执行采集 → CollectRecord + DirtyDataVault
    → channel("rpa.data_collected") → result_verify
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.page_model import PageModelService
from app.store import store


class DataExtractService(BaseModuleService):
    MODULE = "data_extract"
    ENTITY = "提取规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "源页面结构", "提取字段", "提取路径", "数据类型", "格式化", "去空策略"]
    REQUIRED_FIELDS = ["规则名称", "源页面结构", "提取字段"]
    STATUS_ORDER = ["草稿", "已测试", "已生效", "已过期"]
    ACTION_RULES = {"测试提取": "草稿", "启用规则": "已测试"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 (field_mapping + data_extractor 角色) ----

    def consume_source_and_extract(self, batch_ref: str | None = None) -> dict[str, Any]:
        """消费 page_model 发布的源端配置，按配置快照执行字段映射采集。

        从 channel("rpa.source_configured") 获取源端配置（含 snapshot_id），
        将快照版本与字段推断结果写入字段映射采集明细，并让采集批次
        与源端配置版本一一绑定。模型再次发布不会改变已绑定批次。

        Args:
            batch_ref: 可选的批次号（不传则按时间生成）。

        Returns:
            {"extracted": int, "dirty": int, "batches": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.source_configured",
        )
        if not msgs:
            return {"extracted": 0, "dirty": 0, "batches": []}

        batches = []
        for msg in msgs:
            source_config = msg.payload
            snapshot_id = source_config.get("snapshot_id")
            snapshot = (
                store.source_configs.get(int(snapshot_id))
                if snapshot_id is not None else None
            )
            if snapshot is None:
                # 兼容历史消息：按当前模型或载荷补登快照
                snapshot = self._snapshot_from_payload(source_config)
            if snapshot is None:
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue

            batch_ref = batch_ref or (
                f"BATC-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{msg.msg_id}")
            batch_result = self._run_batch(source_config, snapshot, batch_ref)
            store.channel.confirm(msg.msg_id, self.MODULE)
            if batch_result is not None:
                batches.append(batch_result)

        return {
            "extracted": sum(b["extracted"] for b in batches),
            "dirty": sum(b["dirty"] for b in batches),
            "batches": batches,
        }

    def extract_batch(self, snapshot_id: int, batch_ref: str) -> dict[str, Any]:
        """按指定配置快照直接启动一个采集批次（调度器重复采集场景）。

        与 channel 消费路径共用同一套绑定、字段映射明细和采集逻辑，
        保证批次与配置版本仍然一一绑定。
        """
        snapshot = store.source_configs.get(snapshot_id)
        if snapshot is None:
            return {"ok": False, "message": f"配置快照 {snapshot_id} 不存在"}
        source_config = {
            "model_id": snapshot.model_id,
            "target_site": snapshot.target_site,
            "fields_to_extract": list(snapshot.inferred_fields),
        }
        batch_result = self._run_batch(source_config, snapshot, batch_ref)
        if batch_result is None:
            return {
                "ok": False,
                "message": "批次已绑定其他配置版本，不能跨版本采集",
                "batch_ref": batch_ref,
            }
        return {"ok": True, **batch_result}

    def active_snapshot_id(self, model_id: int) -> int | None:
        """模型当前可承接新批次的配置版本。"""
        snapshot = store.source_configs.active_for_model(model_id)
        return snapshot.snapshot_id if snapshot else None

    def _run_batch(
        self,
        source_config: dict[str, Any],
        snapshot: Any,
        batch_ref: str,
    ) -> dict[str, Any] | None:
        """绑定快照 → 写字段映射明细 → 采集 → 广播采集结果。"""
        bound, bind_msg, bound_snapshot = store.source_configs.bind_batch(
            snapshot.snapshot_id, batch_ref)
        if not bound:
            store.channel.publish(
                topic="rpa.source_config_conflict",
                source=self.MODULE,
                payload={"batch_ref": batch_ref, "reason": bind_msg},
            )
            return None
        snapshot = bound_snapshot or snapshot

        # 1. 字段映射口径全部来自快照，而不是当前页面模型
        extract_rules = self._rules_for_snapshot(snapshot)

        # 2. 写入字段映射采集明细（快照版本 + 字段推断结果）
        detail_result = store.source_configs.add_mapping_details(
            batch_ref=batch_ref,
            snapshot=snapshot,
            fields=self._mapping_details(snapshot, extract_rules),
        )

        # 3. 模拟采集数据
        extracted, dirty = self._simulate_collection(
            source_config, extract_rules, batch_ref, snapshot=snapshot)

        # 4. 写 channel
        result_payload = {
            "batch_ref": batch_ref,
            "model_id": snapshot.model_id,
            "site": snapshot.target_site,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_ref": snapshot.snapshot_ref,
            "snapshot_version": snapshot.snapshot_version,
            "processing_item": snapshot.processing_item,
            "extracted": len(extracted),
            "dirty": len(dirty),
            "extract_rules_applied": len(extract_rules),
            "mapping_details_written": detail_result["written"],
            "sample_records": extracted[:3],
        }
        store.channel.publish(
            topic="rpa.data_collected",
            source=self.MODULE,
            payload=result_payload,
        )
        return {
            "batch_ref": batch_ref,
            "site": snapshot.target_site,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_version": snapshot.snapshot_version,
            "mapping_details": detail_result["written"],
            "extracted": len(extracted),
            "dirty": len(dirty),
        }

    def _snapshot_from_payload(self, source_config: dict[str, Any]):
        """为缺少 snapshot_id 的历史消息补登快照（保持向后兼容）。"""
        model_id = int(source_config.get("model_id") or 0)
        if not model_id:
            return None
        entry = store.find("page_model", model_id)
        page_service = PageModelService()
        if entry is not None:
            mapping = page_service._infer_field_mapping(entry)
            snapshot, _ = store.source_configs.publish(
                model_id=model_id,
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
            return snapshot
        inferred = list(source_config.get("fields_to_extract") or [])
        snapshot, _ = store.source_configs.publish(
            model_id=model_id,
            model_name=source_config.get("model_name", ""),
            target_site=source_config.get("target_site", ""),
            model_version=source_config.get("version"),
            page_structure=source_config.get("page_structure", ""),
            dynamic_elements=source_config.get("dynamic_elements", ""),
            pagination_mode=source_config.get("pagination_mode", "none"),
            load_strategy=source_config.get("load_strategy", "lazy"),
            inferred_fields=inferred,
            field_mapping=[
                {"field": field, "target_field": field, "inferred": True}
                for field in inferred
            ],
        )
        return snapshot

    def _rules_for_snapshot(self, snapshot) -> list[dict[str, Any]]:
        """按快照口径获取提取规则：优先快照字段映射，再补充站点已生效规则。"""
        rules: list[dict[str, Any]] = []
        for item in snapshot.field_mapping:
            rules.append({
                "field": item.get("field"),
                "source_path": item.get("source_path", ""),
                "target_field": item.get("target_field", item.get("field")),
                "data_type": item.get("data_type", ""),
                "inferred": True,
            })
        active = self._get_active_rules_for_site(snapshot.target_site)
        known = {rule["field"] for rule in rules}
        for rule in active:
            field = rule.get("提取字段", "")
            if not field:
                continue
            rule_field = field.split("、")[0]
            if rule_field in known:
                continue
            rules.append({
                "field": rule_field,
                "source_path": rule.get("提取路径", ""),
                "target_field": rule_field,
                "data_type": rule.get("数据类型", ""),
                "inferred": False,
                "rule_id": rule.get("id"),
                "rule_name": rule.get("规则名称", ""),
            })
        return rules

    @staticmethod
    def _mapping_details(snapshot, rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """把快照字段推断结果整理为字段映射采集明细。"""
        details: list[dict[str, Any]] = []
        for rule in rules:
            details.append({
                "field": rule.get("field"),
                "source_path": rule.get("source_path", ""),
                "target_field": rule.get("target_field", rule.get("field")),
                "data_type": rule.get("data_type", ""),
                "inferred": rule.get("inferred", True),
                "rule_id": rule.get("rule_id"),
                "rule_name": rule.get("rule_name", ""),
            })
        return details

    def get_batch_snapshot(self, batch_ref: str) -> dict[str, Any] | None:
        """按批次回溯其绑定的源端配置快照（历史明细口径）。"""
        for snapshot in store.source_configs.all():
            if batch_ref in snapshot.bound_batches:
                return {
                    "batch_ref": batch_ref,
                    "snapshot": snapshot.as_dict(),
                    "field_mapping": store.source_configs.list_details(
                        batch_ref=batch_ref, snapshot_id=snapshot.snapshot_id),
                    "collect_records": [
                        {"id": e.entry_id, "fields": e.fields}
                        for e in store.collect_records.list_by_batch(batch_ref)
                    ],
                    "dirty_records": [
                        {"id": r.record_id, "reason": r.reason}
                        for r in store.dirty_vault.list_by_batch(batch_ref)
                    ],
                }
        return None

    def detect_version_conflict(self, batch_ref: str) -> dict[str, Any]:
        """检测批次启动后模型是否再次发布（采集启动与首批写入之间）。

        Returns:
            {"conflict": bool, "batch_ref": ..., "bound_snapshot": ..., "current_snapshot": ...}
        """
        bound = None
        for snapshot in store.source_configs.all():
            if batch_ref in snapshot.bound_batches:
                bound = snapshot
                break
        if bound is None:
            return {"conflict": False, "batch_ref": batch_ref, "reason": "批次未绑定配置快照"}
        current = store.source_configs.active_for_model(bound.model_id)
        if current is None or current.snapshot_id == bound.snapshot_id:
            return {
                "conflict": False,
                "batch_ref": batch_ref,
                "bound_snapshot": bound.as_dict(),
                "current_snapshot": current.as_dict() if current else None,
            }
        return {
            "conflict": True,
            "batch_ref": batch_ref,
            "bound_snapshot": bound.as_dict(),
            "current_snapshot": current.as_dict(),
            "options": ["continue", "abort"],
        }

    def resolve_version_conflict(self, batch_ref: str, strategy: str) -> dict[str, Any]:
        """处理「采集启动后模型再次发布」的版本冲突。

        - continue: 已启动批次继续使用旧配置快照，只保留一个版本绑定；
        - abort: 中止该批次，按新配置重启，新批次绑定新快照。

        两种方式都不允许一个批次跨两个配置版本；重复提交只保留一条结果。
        冲突统一记录到页面源配置的 7 号处理终态。
        """
        if strategy not in ("continue", "abort"):
            return {"ok": False, "message": f"未知处理方式：{strategy}"}
        detection = self.detect_version_conflict(batch_ref)
        if not detection.get("conflict"):
            existing = store.source_configs.list_conflicts(batch_ref)
            if existing:
                return {"ok": True, "reused": True, "conflict": existing[0]}
            return {"ok": False, "message": "批次不存在配置版本冲突", "batch_ref": batch_ref}

        bound = store.source_configs.get(detection["bound_snapshot"]["snapshot_id"])
        current = store.source_configs.get(detection["current_snapshot"]["snapshot_id"])
        if bound is None or current is None:
            return {"ok": False, "message": "配置快照已不存在", "batch_ref": batch_ref}

        conflict, reused = store.source_configs.record_conflict(
            batch_ref=batch_ref,
            model_id=bound.model_id,
            site=bound.target_site,
            bound_snapshot=bound,
            current_snapshot=current,
            strategy=strategy,
        )
        if reused:
            return {"ok": True, "reused": True, "conflict": conflict, "batch_ref": batch_ref}

        restart_batch_ref = None
        if strategy == "abort":
            restart_batch_ref = self._restart_batch_with_snapshot(batch_ref, current)
        return {
            "ok": True,
            "reused": False,
            "conflict": conflict,
            "batch_ref": batch_ref,
            "restart_batch_ref": restart_batch_ref,
            "source_config_terminal_state": conflict["source_config_terminal_state"],
        }

    def _restart_batch_with_snapshot(self, origin_batch_ref: str, snapshot) -> str | None:
        """按新配置快照重启采集，新批次单独绑定新版本。"""
        restart_ref = f"{origin_batch_ref}-R{snapshot.snapshot_ref}"
        bound, _, _ = store.source_configs.bind_batch(snapshot.snapshot_id, restart_ref)
        if not bound:
            return None
        rules = self._rules_for_snapshot(snapshot)
        store.source_configs.add_mapping_details(
            batch_ref=restart_ref,
            snapshot=snapshot,
            fields=self._mapping_details(snapshot, rules),
        )
        source_config = {
            "model_id": snapshot.model_id,
            "target_site": snapshot.target_site,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_ref": snapshot.snapshot_ref,
            "snapshot_version": snapshot.snapshot_version,
            "processing_item": snapshot.processing_item,
            "fields_to_extract": list(snapshot.inferred_fields),
        }
        self._simulate_collection(source_config, rules, restart_ref, snapshot=snapshot)
        store.channel.publish(
            topic="rpa.source_config_restarted",
            source=self.MODULE,
            payload={
                "origin_batch_ref": origin_batch_ref,
                "batch_ref": restart_ref,
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_version": snapshot.snapshot_version,
            },
        )
        return restart_ref

    def _get_active_rules_for_site(self, site: str) -> list[dict[str, Any]]:
        """获取目标站点关联的已生效提取规则（field_mapping 语义）。"""
        rows = store.rows(self.MODULE)
        active = [
            r for r in rows if r.get("status") == "已生效"
        ]
        if site:
            active = [
                r for r in active
                if site[:2] in (r.get("源页面结构") or "")
                or r.get("源页面结构", "") == site
            ]
        # 如果没有关联规则，用默认规则
        if not active and rows:
            active = [r for r in rows if r.get("status") != "已过期"][:3]
        return active

    def _simulate_collection(
        self,
        source_config: dict[str, Any],
        rules: list[dict[str, Any]],
        batch_ref: str,
        snapshot: Any | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """模拟从目标站点采集数据。

        字段口径来自绑定的配置快照；每条采集记录与脏数据都带上
        snapshot_id / snapshot_version，便于历史明细回溯。
        """
        fields_to_extract = list(
            source_config.get("fields_to_extract")
            or (snapshot.inferred_fields if snapshot is not None else [])
            or ["字段1", "字段2", "字段3"]
        )
        for rule in rules:
            rule_field = rule.get("field") or rule.get("提取字段", "")
            if rule_field and rule_field not in fields_to_extract:
                fields_to_extract.append(rule_field)

        snapshot_id = snapshot.snapshot_id if snapshot is not None else None
        snapshot_version = snapshot.snapshot_version if snapshot is not None else None

        # 模拟采集 8-15 条记录
        record_count = random.randint(8, 15)
        valid_records = []
        dirty_records = []

        for i in range(record_count):
            record = self._generate_mock_record(source_config, fields_to_extract, i)

            # 质量检查：模拟约 15% 的脏数据
            if random.random() < 0.15:
                # 随机缺失字段或格式异常
                reasons = ["字段缺失", "格式非法", "解析超时", "编码错误"]
                reason = random.choice(reasons)
                if reason == "字段缺失":
                    del record[random.choice(list(record.keys()))]

                store.dirty_vault.quarantine(
                    source=self.MODULE,
                    batch_ref=batch_ref,
                    raw_data=dict(record),
                    reason=reason,
                    snapshot_id=snapshot_id,
                    snapshot_version=snapshot_version,
                )
                dirty_records.append(record)
            else:
                # 正常数据：写入采集记录
                store.collect_records.add(
                    batch_ref=batch_ref,
                    source=self.MODULE,
                    fields=record,
                    snapshot_id=snapshot_id,
                    snapshot_version=snapshot_version,
                )
                valid_records.append(record)

        return valid_records, dirty_records

    def _generate_mock_record(
        self,
        config: dict[str, Any],
        fields: list[str],
        index: int,
    ) -> dict[str, Any]:
        """生成模拟采集记录（基于真实的数据工程场景）。"""
        site = config.get("target_site", "unknown")
        record: dict[str, Any] = {"_seq": index + 1}

        sample_templates = {
            "序号": lambda i: i + 1,
            "名称": lambda i: f"{site}-产品-{1000 + i}",
            "标题": lambda i: f"关于{site}的第{i + 1}期报告",
            "状态": lambda i: random.choice(["正常", "异常", "待审核"]),
            "金额": lambda i: round(random.uniform(100, 99999), 2),
            "日期": lambda i: f"2026-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            "链接": lambda i: f"https://{site}.example.com/item/{i + 1}",
            "摘要": lambda i: f"{site}数据采集摘要 #{i + 1}",
            "时间": lambda i: datetime.now(timezone.utc).isoformat(),
            "字段名": lambda i: f"field_{i + 1}",
            "字段值": lambda i: f"value_{random.randint(1, 100)}",
            "内容": lambda i: f"{site}采集内容行 {i + 1}",
            "标签": lambda i: random.choice(["采集", "RPA", "自动化"]),
            "发布日期": lambda i: f"2026-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "类型": lambda i: random.choice(["文本", "数值", "日期"]),
            "必填": lambda i: random.choice(["是", "否"]),
            "数据项1": lambda i: f"data1_{i}",
            "数据项2": lambda i: f"data2_{i}",
            "数据项3": lambda i: f"data3_{i}",
        }

        for field in fields:
            if field in sample_templates:
                record[field] = sample_templates[field](index)
            else:
                record[field] = f"{field}-{index + 1}"

        return record

    def execute_mapping_and_extract(
        self,
        model_id: int,
        target_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """直接执行字段映射 + 数据提取（不通过 channel 的简化版）。

        用于直接调用场景：已知 page_model 和规则，立即执行采集。

        Args:
            model_id: 页面模型 ID
            target_fields: 额外目标字段（可选）

        Returns:
            采集结果概要
        """
        model_entry = store.find("page_model", model_id)
        if model_entry is None:
            return {"error": f"页面模型 {model_id} 不存在"}

        # 直接执行同样要先定位/冻结配置快照，保证预算口径可回溯
        snapshot = store.source_configs.active_for_model(model_id)
        if snapshot is None:
            page_service = PageModelService()
            field_mapping = page_service._infer_field_mapping(model_entry)
            snapshot, _ = store.source_configs.publish(
                model_id=model_id,
                model_name=model_entry.get("模型名称", ""),
                target_site=model_entry.get("目标站点", "unknown"),
                model_version=model_entry.get("版本号"),
                page_structure=model_entry.get("页面结构", ""),
                dynamic_elements=model_entry.get("动态元素", ""),
                pagination_mode=model_entry.get("分页模式", "none"),
                load_strategy=model_entry.get("加载策略", "lazy"),
                inferred_fields=[item["field"] for item in field_mapping],
                field_mapping=field_mapping,
            )
        batch_ref = f"DIRECT-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        bound, _, _ = store.source_configs.bind_batch(snapshot.snapshot_id, batch_ref)

        source_config = {
            "model_id": model_id,
            "target_site": snapshot.target_site,
            "fields_to_extract": target_fields or snapshot.inferred_fields,
        }
        rules = self._rules_for_snapshot(snapshot)
        if bound:
            store.source_configs.add_mapping_details(
                batch_ref=batch_ref,
                snapshot=snapshot,
                fields=self._mapping_details(snapshot, rules),
            )
        valid, dirty = self._simulate_collection(
            source_config, rules, batch_ref, snapshot=snapshot)

        return {
            "batch_ref": batch_ref,
            "model_id": model_id,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_version": snapshot.snapshot_version,
            "extracted": len(valid),
            "dirty": len(dirty),
            "sample": valid[:3],
        }

    def get_collection_stats(self) -> dict[str, Any]:
        """获取采集统计概览。"""
        collect_snapshot = store.collect_records.snapshot()
        dirty_snapshot = store.dirty_vault.snapshot()
        return {
            "collect_records": collect_snapshot,
            "dirty_vault": dirty_snapshot,
            "collection_events": len(
                store.channel.snapshot(topic="rpa.data_collected")),
            "config_snapshots": len(store.source_configs.all()),
            "mapping_details": len(store.source_configs.list_details()),
            "version_conflicts": len(store.source_configs.list_conflicts()),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_collection_stats()
        return {**base, **stats}