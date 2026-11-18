"""源端配置版本化测试 — 快照冻结、批次绑定、字段映射明细与冲突处理。

覆盖需求：
1. 页面模型发布后生成可筛选的配置版本快照（含字段推断结果）；
2. 数据提取按处理项庚口径写入字段映射采集明细；
3. 批次与配置版本同时绑定，模型再次发布不覆盖已开始批次；
4. 采集启动与首批写入之间的版本冲突：沿用旧快照 / 中止并重启，且只绑定一个版本；
5. 重复发布、重复提交幂等，只保留一条结果；
6. 台账筛选、排序、分页；
7. 台账与结果清单联动核对。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from app.services.batch_tracker import BatchTrackerService
from app.services.data_extract import DataExtractService
from app.services.page_model import PageModelService
from app.store import SOURCE_CONFIG_PROCESSING_ITEM, store


def _reset_source_configs() -> None:
    """每个用例使用独立的模型与快照，避免共享状态互相影响。"""
    store._tables["page_model"] = [
        {
            "id": 901,
            "模型名称": "招聘岗位表格页",
            "目标站点": "jobs.example.com",
            "页面结构": "表格列表页",
            "动态元素": "表格行懒加载",
            "分页模式": "page-number",
            "加载策略": "lazy",
            "版本号": "v1",
            "status": "已验证",
            "pending": True,
            "abnormal": False,
        },
        {
            "id": 902,
            "模型名称": "新闻列表页",
            "目标站点": "news.example.com",
            "页面结构": "列表卡片",
            "动态元素": "无限滚动",
            "分页模式": "scroll",
            "加载策略": "eager",
            "版本号": "v1",
            "status": "已验证",
            "pending": True,
            "abnormal": False,
        },
    ]
    store.source_configs = type(store.source_configs)()
    store.collect_records._entries.clear()
    store.dirty_vault._records.clear()
    store.rows("batch_tracker").clear()
    store.channel._messages.clear()
    store.channel._seq = 0


@pytest.fixture()
def services():
    originals = {
        "page_model": [dict(row) for row in store.rows("page_model")],
        "batch_tracker": [dict(row) for row in store.rows("batch_tracker")],
        "source_configs": store.source_configs,
        "collect_records": store.collect_records,
        "dirty_vault": store.dirty_vault,
    }
    _reset_source_configs()
    yield PageModelService(), DataExtractService(), BatchTrackerService()
    # 还原全局仓库，避免影响其它用例
    store._tables["page_model"] = originals["page_model"]
    store._tables["batch_tracker"] = originals["batch_tracker"]
    store.source_configs = originals["source_configs"]
    store.collect_records = originals["collect_records"]
    store.dirty_vault = originals["dirty_vault"]


def test_publish_creates_snapshot_with_inferred_mapping(services):
    page_svc, _, _ = services

    result = page_svc.publish_source_config(901)

    assert result is not None
    assert result["snapshot_version"] == "v1"
    snapshot = store.source_configs.get(result["snapshot_id"])
    assert snapshot is not None
    assert snapshot.target_site == "jobs.example.com"
    assert snapshot.processing_item == SOURCE_CONFIG_PROCESSING_ITEM
    # 表格结构应推断出表格字段，并带字段映射口径
    assert snapshot.inferred_fields == ["序号", "名称", "状态", "金额", "日期"]
    assert snapshot.field_mapping[0]["source_path"].startswith("table")
    # 模型上的版本回溯标记
    model = store.find("page_model", 901)
    assert model["配置版本"] == "v1"


def test_publish_is_idempotent(services):
    page_svc, _, _ = services

    first = page_svc.publish_source_config(901)
    second = page_svc.publish_source_config(901)

    assert second["reused"] is True
    assert second["snapshot_id"] == first["snapshot_id"]
    assert len(store.source_configs.all()) == 1
    assert len(store.channel.snapshot(topic="rpa.source_configured")) == 1


def test_batch_writes_mapping_details_with_snapshot_version(services):
    page_svc, extract_svc, _ = services
    publish = page_svc.publish_source_config(901)
    snapshot_id = publish["snapshot_id"]

    result = extract_svc.consume_source_and_extract(batch_ref="BATCH-001")

    assert result["extracted"] > 0
    batch = result["batches"][0]
    assert batch["snapshot_id"] == snapshot_id
    assert batch["snapshot_version"] == "v1"

    details = store.source_configs.list_details(batch_ref="BATCH-001")
    assert details
    assert all(item["snapshot_id"] == snapshot_id for item in details)
    assert all(item["snapshot_version"] == "v1" for item in details)
    assert all(item["processing_item"] == SOURCE_CONFIG_PROCESSING_ITEM for item in details)
    assert {"序号", "名称", "状态", "金额", "日期"}.issubset(
        {item["field"] for item in details})

    # 采集记录与脏数据都能回溯到快照
    records = store.collect_records.list_by_batch("BATCH-001")
    assert records
    assert all(record.snapshot_id == snapshot_id for record in records)


def test_batch_binding_is_exclusive(services):
    page_svc, extract_svc, _ = services
    first = page_svc.publish_source_config(901)["snapshot_id"]
    extract_svc.consume_source_and_extract(batch_ref="BATCH-001")

    # 发布新版本后再尝试把同一批次绑到新快照
    store.find("page_model", 901)["版本号"] = "v2"
    second = page_svc.publish_source_config(901)["snapshot_id"]
    assert second != first

    bound, message, _ = store.source_configs.bind_batch(second, "BATCH-001")
    assert bound is False
    assert "不能同时绑定多个版本" in message

    # 原批次仍留在旧版本下
    snapshot = store.source_configs.get(first)
    assert "BATCH-001" in snapshot.bound_batches
    assert "BATCH-001" not in store.source_configs.get(second).bound_batches


def test_republish_does_not_override_started_batch(services):
    page_svc, extract_svc, _ = services
    page_svc.publish_source_config(901)
    extract_svc.consume_source_and_extract(batch_ref="BATCH-OLD")

    store.find("page_model", 901)["版本号"] = "v2"
    store.find("page_model", 901)["页面结构"] = "详情页"
    page_svc.publish_source_config(901)

    # 历史明细仍是旧口径
    details = store.source_configs.list_details(batch_ref="BATCH-OLD")
    assert all(item["snapshot_version"] == "v1" for item in details)
    assert {item["field"] for item in details} >= {"序号", "名称", "状态", "金额", "日期"}

    # 新批次走新版本
    result = extract_svc.consume_source_and_extract(batch_ref="BATCH-NEW")
    assert result["batches"][0]["snapshot_version"] == "v2"


def test_conflict_detection_and_continue(services):
    page_svc, extract_svc, tracker = services
    page_svc.publish_source_config(901)
    extract_svc.consume_source_and_extract(batch_ref="BATCH-C1")

    store.find("page_model", 901)["版本号"] = "v2"
    page_svc.publish_source_config(901)

    detection = extract_svc.detect_version_conflict("BATCH-C1")
    assert detection["conflict"] is True
    assert detection["options"] == ["continue", "abort"]

    resolved = extract_svc.resolve_version_conflict("BATCH-C1", "continue")
    assert resolved["ok"] is True
    assert resolved["restart_batch_ref"] is None
    # 冲突统一落到 7 号处理终态
    assert resolved["conflict"]["source_config_terminal_state"] == 7
    assert resolved["conflict"]["source_config_terminal_label"] == "冲突终止"

    # 已启动批次仍只有一个版本绑定
    bindings = [
        snapshot.snapshot_id for snapshot in store.source_configs.all()
        if "BATCH-C1" in snapshot.bound_batches
    ]
    assert bindings == [resolved["conflict"]["bound_snapshot_id"]]

    # 另一个业务入口的待办同步更新
    tracker.start_batch(batch_no="BATCH-C1", flow_name="招聘采集", model_id=901)
    todo = tracker.update_todo("BATCH-C1", "冲突已按旧快照继续")
    assert todo["待办"] == "冲突已按旧快照继续"


def test_conflict_abort_restarts_with_new_snapshot(services):
    page_svc, extract_svc, _ = services
    page_svc.publish_source_config(901)
    extract_svc.consume_source_and_extract(batch_ref="BATCH-C2")

    store.find("page_model", 901)["版本号"] = "v2"
    new_snapshot_id = page_svc.publish_source_config(901)["snapshot_id"]

    resolved = extract_svc.resolve_version_conflict("BATCH-C2", "abort")
    assert resolved["ok"] is True
    restart_ref = resolved["restart_batch_ref"]
    assert restart_ref
    assert resolved["conflict"]["source_config_terminal_state"] == 7

    # 原批次停在旧快照，新批次单独绑定新快照，两者不共享
    new_snapshot = store.source_configs.get(new_snapshot_id)
    assert restart_ref in new_snapshot.bound_batches
    assert "BATCH-C2" not in new_snapshot.bound_batches
    assert store.source_configs.list_details(batch_ref=restart_ref)


def test_repeated_conflict_submission_keeps_single_result(services):
    page_svc, extract_svc, _ = services
    page_svc.publish_source_config(901)
    extract_svc.consume_source_and_extract(batch_ref="BATCH-C3")
    store.find("page_model", 901)["版本号"] = "v2"
    page_svc.publish_source_config(901)

    first = extract_svc.resolve_version_conflict("BATCH-C3", "continue")
    second = extract_svc.resolve_version_conflict("BATCH-C3", "abort")

    assert second["reused"] is True
    assert second["conflict"]["conflict_id"] == first["conflict"]["conflict_id"]
    assert len(store.source_configs.list_conflicts("BATCH-C3")) == 1
    # 中止重复提交不会额外生成重启批次
    assert second.get("restart_batch_ref") is None


def test_ledger_filters_sort_and_pagination(services):
    page_svc, _, tracker = services
    page_svc.publish_source_config(901)
    page_svc.publish_source_config(902)
    store.find("page_model", 901)["版本号"] = "v2"
    page_svc.publish_source_config(901)

    rows, total = page_svc.list_config_snapshots(site="jobs")
    assert total == 2
    assert all("jobs" in row["target_site"] for row in rows)

    rows, total = page_svc.list_config_snapshots(version="v2")
    assert total == 1
    assert rows[0]["snapshot_version"] == "v2"

    rows, total = page_svc.list_config_snapshots(sort_by="site", sort_order="asc")
    assert total == 3
    assert [row["target_site"] for row in rows] == sorted(
        row["target_site"] for row in rows)

    rows, total = page_svc.list_config_snapshots(page=1, size=2)
    assert total == 3 and len(rows) == 2
    rows, total = page_svc.list_config_snapshots(page=2, size=2)
    assert total == 3 and len(rows) == 1

    bindings, binding_total = tracker.list_batch_bindings(site="jobs")
    assert binding_total == 0  # 尚未采集


def test_ledger_and_result_list_cross_check(services):
    page_svc, extract_svc, tracker = services
    page_svc.publish_source_config(901)
    extract_svc.consume_source_and_extract(batch_ref="BATCH-D1")

    check = tracker.cross_check_ledger("BATCH-D1")
    assert check["snapshot_version"] == "v1"
    assert check["consistent"] is True
    assert check["detail_count"] >= 5
    assert check["collect_count"] > 0
    assert check["fields_missing_in_records"] == []

    progress = tracker.track_batch_progress("BATCH-D1")
    assert progress["snapshot_id"] is not None
    assert progress["field_mapping"]


def test_batch_locates_snapshot_by_site_and_version(services):
    page_svc, extract_svc, tracker = services
    page_svc.publish_source_config(901)
    snapshot_id = store.source_configs.active_for_model(901).snapshot_id
    extract_svc.consume_source_and_extract(batch_ref="BATCH-E1")
    extract_svc.extract_batch(snapshot_id, "BATCH-E2")
    tracker.start_batch(batch_no="BATCH-E2", flow_name="招聘采集", model_id=901)

    rows, total = tracker.list_batch_bindings(site="jobs", version="v1")
    assert total == 2
    assert {row["batch_ref"] for row in rows} == {"BATCH-E1", "BATCH-E2"}
    assert all(row["snapshot_version"] == "v1" for row in rows)
    assert all(row["field_mapping"] for row in rows)
