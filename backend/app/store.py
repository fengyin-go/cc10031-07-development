"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None
    # 记录产生脏数据时使用的源端配置快照，避免模型改版后无法回溯
    snapshot_id: int | None = None
    snapshot_version: str | None = None


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str,
                   snapshot_id: int | None = None,
                   snapshot_version: str | None = None) -> DirtyRecord:
        with self._lock:
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
                snapshot_id=snapshot_id, snapshot_version=snapshot_version,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def list_by_snapshot(self, snapshot_id: int) -> list[DirtyRecord]:
        """按源端配置快照回溯脏数据，模型改版后仍可定位当时字段口径。"""
        with self._lock:
            return [r for r in self._records if r.snapshot_id == snapshot_id]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "by_reason": by_reason,
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float = 1.0
    # 采集当时绑定的源端配置版本，模型改版后仍可判断字段口径
    snapshot_id: int | None = None
    snapshot_version: str | None = None


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any],
            snapshot_id: int | None = None,
            snapshot_version: str | None = None) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
                snapshot_id=snapshot_id, snapshot_version=snapshot_version,
            )
            self._entries.append(entry)
            return entry

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_snapshot(self, snapshot_id: int) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.snapshot_id == snapshot_id]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            by_snapshot: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
                if e.snapshot_id is not None:
                    key = str(e.snapshot_id)
                    by_snapshot[key] = by_snapshot.get(key, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
                "by_snapshot": by_snapshot,
            }


# ---------------------------------------------------------------------------
# 源端配置版本快照 (SourceConfigRegistry) — 页面模型发布后的可追溯配置
# ---------------------------------------------------------------------------

SOURCE_CONFIG_PROCESSING_ITEM = "处理项7"

# 页面源配置的 7 个处理终态；冲突固定落到 7 号「冲突终止」
SOURCE_CONFIG_TERMINAL_STATES: dict[int, str] = {
    1: "草稿快照",
    2: "已发布",
    3: "已绑定批次",
    4: "采集中",
    5: "已冻结",
    6: "已完成",
    7: "冲突终止",
}
SOURCE_CONFIG_CONFLICT_TERMINAL_STATE = 7


def normalize_source_version(raw: Any) -> str:
    """沿用既有版本口径：直接取模型「版本号」，空值才回退默认版本。"""
    text = str(raw or "").strip()
    return text or "1.0"


@dataclass
class SourceConfigSnapshot:
    """页面模型发布时冻结的源端配置快照。

    一旦有采集批次绑定，快照内容不再随页面模型改版变化；
    批次只能通过 `snapshot_id` / `snapshot_version` 回溯当时字段口径。
    """
    snapshot_id: int
    snapshot_ref: str
    processing_item: str
    model_id: int
    model_name: str
    target_site: str
    model_version: str
    snapshot_version: str
    page_structure: str
    dynamic_elements: str
    pagination_mode: str
    load_strategy: str
    inferred_fields: list[str]
    field_mapping: list[dict[str, Any]]
    status: str = "已发布"
    terminal_state: int = 2
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    bound_batches: list[str] = field(default_factory=list)
    conflict_batches: list[str] = field(default_factory=list)
    accepting_new_batches: bool = True
    superseded_by: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_ref": self.snapshot_ref,
            "processing_item": self.processing_item,
            "model_id": self.model_id,
            "model_name": self.model_name,
            "target_site": self.target_site,
            "model_version": self.model_version,
            "snapshot_version": self.snapshot_version,
            "page_structure": self.page_structure,
            "dynamic_elements": self.dynamic_elements,
            "pagination_mode": self.pagination_mode,
            "load_strategy": self.load_strategy,
            "inferred_fields": list(self.inferred_fields),
            "field_mapping": [dict(item) for item in self.field_mapping],
            "status": self.status,
            "terminal_state": self.terminal_state,
            "terminal_state_label": SOURCE_CONFIG_TERMINAL_STATES.get(
                self.terminal_state, "未知"),
            "created_at": self.created_at,
            "bound_batches": list(self.bound_batches),
            "conflict_batches": list(self.conflict_batches),
            "accepting_new_batches": self.accepting_new_batches,
            "superseded_by": self.superseded_by,
        }


class SourceConfigRegistry:
    """源端配置版本台账：发布快照、绑定批次、记录字段映射明细与冲突。

    关键约束：
    - 发布新版本只冻结旧快照的「承接新批次」能力，不覆盖已开始批次；
    - 一个批次只能绑定一个配置版本（bind_batch 拒绝二次绑定到别的快照）；
    - 重复发布/重复提交按幂等键去重，只保留一条结果；
    - 冲突处理固定落到页面源配置的 7 号处理终态。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._snapshots: list[SourceConfigSnapshot] = []
        self._detail_seq = 0
        self._details: list[dict[str, Any]] = []
        self._conflict_seq = 0
        self._conflicts: list[dict[str, Any]] = []

    # ---- 快照发布 ----

    @staticmethod
    def _dedupe_key(**values: Any) -> tuple[Any, ...]:
        return (
            values.get("model_id"),
            values.get("target_site"),
            values.get("snapshot_version"),
            values.get("page_structure"),
            tuple(values.get("inferred_fields") or []),
            json.dumps(values.get("field_mapping") or [], ensure_ascii=False, sort_keys=True),
        )

    def publish(
        self,
        *,
        model_id: int,
        model_name: str,
        target_site: str,
        model_version: Any,
        page_structure: str,
        dynamic_elements: str = "",
        pagination_mode: str = "none",
        load_strategy: str = "lazy",
        inferred_fields: list[str] | None = None,
        field_mapping: list[dict[str, Any]] | None = None,
    ) -> tuple[SourceConfigSnapshot, bool]:
        """登记一个源端配置快照。

        Returns:
            (snapshot, reused) — reused=True 表示命中幂等键，未新建版本。
        """
        version = normalize_source_version(model_version)
        inferred = list(inferred_fields or [])
        mapping = [dict(item) for item in (field_mapping or [])]
        key = self._dedupe_key(
            model_id=model_id, target_site=target_site, snapshot_version=version,
            page_structure=page_structure, inferred_fields=inferred,
            field_mapping=mapping,
        )
        with self._lock:
            for snap in self._snapshots:
                if self._dedupe_key(**snap.as_dict()) == key:
                    return snap, True

            # 同一模型发布新版本：旧快照不再承接新批次，但在途批次继续可用
            for snap in self._snapshots:
                if snap.model_id == model_id and snap.accepting_new_batches:
                    snap.accepting_new_batches = False
                    snap.superseded_by = self._seq + 1
                    if snap.terminal_state in (1, 2):
                        snap.terminal_state = 5
                        snap.status = "已冻结"

            self._seq += 1
            snapshot = SourceConfigSnapshot(
                snapshot_id=self._seq,
                snapshot_ref=f"CFG-{self._seq:04d}",
                processing_item=SOURCE_CONFIG_PROCESSING_ITEM,
                model_id=model_id,
                model_name=model_name,
                target_site=target_site,
                model_version=version,
                snapshot_version=version,
                page_structure=page_structure,
                dynamic_elements=dynamic_elements,
                pagination_mode=pagination_mode,
                load_strategy=load_strategy,
                inferred_fields=inferred,
                field_mapping=mapping,
            )
            self._snapshots.append(snapshot)
            return snapshot, False

    def get(self, snapshot_id: int) -> SourceConfigSnapshot | None:
        with self._lock:
            for snap in self._snapshots:
                if snap.snapshot_id == snapshot_id:
                    return snap
            return None

    def get_by_ref(self, snapshot_ref: str) -> SourceConfigSnapshot | None:
        with self._lock:
            for snap in self._snapshots:
                if snap.snapshot_ref == snapshot_ref:
                    return snap
            return None

    def active_for_model(self, model_id: int) -> SourceConfigSnapshot | None:
        with self._lock:
            active = [
                snap for snap in self._snapshots
                if snap.model_id == model_id and snap.accepting_new_batches
            ]
            return active[-1] if active else None

    def all(self) -> list[SourceConfigSnapshot]:
        with self._lock:
            return list(self._snapshots)

    def mark_state(self, snapshot_id: int, terminal_state: int) -> SourceConfigSnapshot | None:
        with self._lock:
            for snap in self._snapshots:
                if snap.snapshot_id == snapshot_id:
                    snap.terminal_state = terminal_state
                    snap.status = SOURCE_CONFIG_TERMINAL_STATES.get(
                        terminal_state, snap.status)
                    if terminal_state in (5, 6, 7):
                        snap.accepting_new_batches = False
                    return snap
            return None

    # ---- 批次绑定（只能绑定一个配置版本） ----

    def bind_batch(self, snapshot_id: int, batch_ref: str) -> tuple[bool, str, SourceConfigSnapshot | None]:
        with self._lock:
            target = next(
                (snap for snap in self._snapshots if snap.snapshot_id == snapshot_id),
                None,
            )
            if target is None:
                return False, f"配置快照 {snapshot_id} 不存在", None
            for snap in self._snapshots:
                if batch_ref in snap.bound_batches and snap.snapshot_id != snapshot_id:
                    return False, (
                        f"批次 {batch_ref} 已绑定配置快照 {snap.snapshot_ref}，"
                        "不能同时绑定多个版本"), target
            if batch_ref in target.bound_batches:
                return True, "批次已绑定该配置快照（幂等）", target
            if not target.accepting_new_batches:
                return False, f"配置快照 {target.snapshot_ref} 已冻结，不能承接新批次", target
            target.bound_batches.append(batch_ref)
            if target.terminal_state in (1, 2):
                target.terminal_state = 3
                target.status = "已绑定批次"
            return True, "批次已绑定配置快照", target

    # ---- 字段映射采集明细 ----

    def add_mapping_details(
        self,
        *,
        batch_ref: str,
        snapshot: SourceConfigSnapshot,
        fields: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """写入字段映射采集明细；同一批次同一字段只保留一条结果。"""
        created: list[dict[str, Any]] = []
        duplicates = 0
        with self._lock:
            existing = {
                (item["batch_ref"], item["snapshot_id"], item["field"])
                for item in self._details
            }
            for field in fields:
                key = (batch_ref, snapshot.snapshot_id, field.get("field"))
                if key in existing:
                    duplicates += 1
                    continue
                self._detail_seq += 1
                detail = {
                    "detail_id": self._detail_seq,
                    "batch_ref": batch_ref,
                    "snapshot_id": snapshot.snapshot_id,
                    "snapshot_ref": snapshot.snapshot_ref,
                    "snapshot_version": snapshot.snapshot_version,
                    "model_id": snapshot.model_id,
                    "model_version": snapshot.model_version,
                    "site": snapshot.target_site,
                    "processing_item": snapshot.processing_item,
                    "field": field.get("field"),
                    "source_path": field.get("source_path", ""),
                    "target_field": field.get("target_field", field.get("field")),
                    "data_type": field.get("data_type", ""),
                    "inferred": bool(field.get("inferred")),
                    "rule_id": field.get("rule_id"),
                    "rule_name": field.get("rule_name", ""),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                self._details.append(detail)
                existing.add(key)
                created.append(detail)
            if created and snapshot.terminal_state in (2, 3):
                snapshot.terminal_state = 4
                snapshot.status = "采集中"
        return {
            "written": len(created),
            "duplicates": duplicates,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_version": snapshot.snapshot_version,
            "details": created,
        }

    def list_details(
        self,
        *,
        batch_ref: str | None = None,
        snapshot_id: int | None = None,
        site: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._details)
        if batch_ref is not None:
            rows = [row for row in rows if row["batch_ref"] == batch_ref]
        if snapshot_id is not None:
            rows = [row for row in rows if row["snapshot_id"] == snapshot_id]
        if site:
            rows = [row for row in rows if row["site"] == site]
        return rows

    # ---- 冲突记录（7 号处理终态） ----

    def record_conflict(
        self,
        *,
        batch_ref: str,
        model_id: int,
        site: str,
        bound_snapshot: SourceConfigSnapshot,
        current_snapshot: SourceConfigSnapshot,
        strategy: str,
    ) -> tuple[dict[str, Any], bool]:
        """记录一次配置冲突处理；同一批次重复提交只保留一条结果。"""
        labels = {"continue": "沿用旧配置快照", "abort": "中止并按新配置重启"}
        with self._lock:
            for conflict in self._conflicts:
                if conflict["batch_ref"] == batch_ref:
                    return conflict, True
            self._conflict_seq += 1
            conflict = {
                "conflict_id": self._conflict_seq,
                "batch_ref": batch_ref,
                "model_id": model_id,
                "site": site,
                "bound_snapshot_id": bound_snapshot.snapshot_id,
                "bound_snapshot_version": bound_snapshot.snapshot_version,
                "current_snapshot_id": current_snapshot.snapshot_id,
                "current_snapshot_version": current_snapshot.snapshot_version,
                "strategy": strategy,
                "strategy_label": labels.get(strategy, strategy),
                "source_config_terminal_state": SOURCE_CONFIG_CONFLICT_TERMINAL_STATE,
                "source_config_terminal_label": SOURCE_CONFIG_TERMINAL_STATES[
                    SOURCE_CONFIG_CONFLICT_TERMINAL_STATE],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self._conflicts.append(conflict)
            if batch_ref not in bound_snapshot.conflict_batches:
                bound_snapshot.conflict_batches.append(batch_ref)
            # 两种处理方式都只能绑定一个配置版本，冲突统一落到 7 号处理终态
            bound_snapshot.terminal_state = SOURCE_CONFIG_CONFLICT_TERMINAL_STATE
            bound_snapshot.status = SOURCE_CONFIG_TERMINAL_STATES[
                SOURCE_CONFIG_CONFLICT_TERMINAL_STATE]
            bound_snapshot.accepting_new_batches = False
            return conflict, False

    def list_conflicts(self, batch_ref: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._conflicts)
        if batch_ref:
            rows = [row for row in rows if row["batch_ref"] == batch_ref]
        return rows

    # ---- 台账查询：筛选 / 排序 / 分页 ----

    def list_snapshots(
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
        rows = [snap.as_dict() for snap in self.all()]
        if site:
            rows = [row for row in rows if site.casefold() in row["target_site"].casefold()]
        if version:
            rows = [
                row for row in rows
                if version.casefold() in row["snapshot_version"].casefold()
                or version.casefold() in row["model_version"].casefold()
            ]
        if model_id is not None:
            rows = [row for row in rows if row["model_id"] == model_id]
        if status:
            rows = [row for row in rows if row["status"] == status]
        if keyword:
            needle = keyword.casefold()
            rows = [
                row for row in rows
                if needle in row["snapshot_ref"].casefold()
                or needle in row["model_name"].casefold()
                or needle in row["target_site"].casefold()
                or needle in row["processing_item"].casefold()
            ]

        sort_fields = {
            "snapshot_id": lambda row: row["snapshot_id"],
            "created_at": lambda row: row["created_at"],
            "site": lambda row: row["target_site"],
            "version": lambda row: row["snapshot_version"],
            "batch_count": lambda row: len(row["bound_batches"]),
        }
        key = sort_fields.get(sort_by, sort_fields["snapshot_id"])
        rows.sort(key=key, reverse=(sort_order == "desc"))
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()
        self.source_configs = SourceConfigRegistry()

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()