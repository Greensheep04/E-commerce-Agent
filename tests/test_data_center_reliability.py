import csv
import json
import multiprocessing
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest

from scripts import verify_data_center_reliability as verify
from scripts.verify_data_center_reliability import (
    ROOT,
    create_data_center_engine,
    database_task,
    replay_sync_from_process,
)


@pytest.mark.skipif(os.environ.get("DC06_LIVE_ACCEPTANCE") != "1",
                    reason="需要独立数据服务、真实Shop服务和已批准来源")
def test_concurrent_processes_reuse_real_published_sync_request():
    identities = ROOT / ".scratch/evidence/dc02-host-20261004-r2/identities.json"
    state = json.loads((ROOT / ".scratch/evidence/dc05-integration-20261004-r2/state.json")
                       .read_text(encoding="utf-8"))
    expected = json.loads((ROOT / ".scratch/evidence/dc05-integration-20261004-r2/sync-result.json")
                          .read_text(encoding="utf-8"))
    expected_snapshot = expected["data"]["snapshot"]["snapshot_id"]
    arguments = [str(identities), "http://127.0.0.1:8002", "http://127.0.0.1:8101",
                 state["request_id"]]

    with ProcessPoolExecutor(max_workers=3,
                             mp_context=multiprocessing.get_context("spawn")) as pool:
        results = list(pool.map(replay_sync_from_process, [arguments] * 3))

    assert {item["status"] for item in results} == {"completed"}
    assert {item["task_id"] for item in results} == {state["task_id"]}
    assert {item["snapshot_id"] for item in results} == {expected_snapshot}
    assert {item["snapshot_state"] for item in results} == {"published"}
    assert all(item["usage"]["requests"] == [] for item in results)


def test_persisted_task_lookup_uses_data_center_database():
    engine = create_data_center_engine()
    try:
        assert database_task(engine, str(uuid4()), str(uuid4())) is None
    finally:
        engine.dispose()


def test_schedule_round_selection_uses_live_r9_created_at_and_coalescing_records():
    evidence_dir = ROOT / ".scratch/evidence/dc06-reliability-20261004-r9"
    order_report = json.loads((evidence_dir / "schedule-order.json").read_text(encoding="utf-8"))
    assert order_report["api_matches_database_order"] is True
    assert order_report["api_database_rows_match"] is True
    with (evidence_dir / "schedule-runs-audit.tsv").open(encoding="utf-8", newline="") as source:
        status_rows = list(csv.reader(source, delimiter="|"))
    status_by_run_id = {row[0]: row[1] for row in status_rows}
    live_runs = [
        {**run, "status": status_by_run_id[run["run_id"]]}
        for run in order_report["schedule_runs_api_order"]
        if run["run_id"] in status_by_run_id
    ]
    assert [run["status"] for run in live_runs[:3]] == ["waiting", "waiting", "waiting"]

    selector = getattr(verify, "select_post_baseline_schedule_rounds", None)
    assert callable(selector), "验证器没有按真实created_at选取baseline后的日程运行"
    validator = getattr(verify, "validate_schedule_timeline", None)
    assert callable(validator), "验证器没有核查真实trigger与coalesced_periods记录"
    trigger_prefix = "schedule:data-center-interval-f9b45629b0f4877383568019ae660f23:"
    baseline, rounds = selector(live_runs, trigger_prefix)
    assert baseline["run_id"] == "67db428f-4999-4df8-b5cd-d7e0b8ff68b5"
    assert baseline["status"] == "completed"
    assert [run["run_id"] for run in rounds] == [
        "4f9bd91a-f994-4c78-9ff9-0932668a71eb",
        "27518007-13a9-4adf-b5c3-30689a6a599e",
        "eca0f197-1119-451f-977c-d81c32a4453d",
    ]
    assert all(run["status"] == "waiting" for run in rounds)

    events = {event["run_id"]: event for event in order_report["schedule_event_records"]}
    schedule_id = trigger_prefix[len("schedule:"):-1]
    timeline = validator([baseline, *rounds], events, schedule_id, interval_seconds=5)
    assert timeline["trigger_times_in_order"] == [
        "2026-10-04T08:46:22.504005+08:00",
        "2026-10-04T08:46:32.504005+08:00",
        "2026-10-04T08:46:37.504005+08:00",
        "2026-10-04T08:46:42.504005+08:00",
    ]
    assert timeline["coalesced_periods"] == [0, 1, 0, 0]
    assert timeline["trigger_gaps_seconds"] == [10, 5, 5]


@pytest.mark.skipif(not os.environ.get("DC06_CANCELLED_GOAL_ID"),
                    reason="需要原始可靠性运行中的已取消目标及真实服务")
def test_cancelled_goal_supplement_preserves_run_and_request_records():
    goal_id = os.environ["DC06_CANCELLED_GOAL_ID"]
    identities = Path(os.environ.get(
        "DC06_IDENTITIES",
        ROOT / ".scratch/evidence/dc02-host-20261004-r3/identities.json",
    ))
    output_dir = Path(os.environ.get(
        "DC06_CANCELLED_GOAL_OUTPUT_DIR",
        ROOT / ".scratch/evidence/dc06-cancellation-supplement-20261004-r1",
    ))
    base_report = ROOT / ".scratch/evidence/dc06-reliability-20261004T011619Z-5abb75ca/report.json"
    result = subprocess.run([
        sys.executable,
        str(ROOT / "scripts/verify_data_center_reliability.py"),
        "--identities", str(identities),
        "--output-dir", str(output_dir),
        "--cancelled-goal-id", goal_id,
        "--base-report", str(base_report),
    ], cwd=ROOT, capture_output=True, text=True, timeout=45, check=False)
    assert result.returncode == 0, result.stderr or result.stdout

    report_path = output_dir / "report.json"
    cancellation_path = output_dir / "cancellation.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    cancellation = json.loads(cancellation_path.read_text(encoding="utf-8"))
    check = next(item for item in report["checks"]
                 if item["id"] == "cancel_stops_new_scheduled_and_source_runs")

    assert report["status"] == "passed"
    assert cancellation == report["cancellation"] == check["facts"]
    assert cancellation["goal_id"] == goal_id
    assert cancellation["state"] == "cancelled"
    assert cancellation["state_after_schedule_wait"] == "cancelled"
    assert cancellation["state_after_source_event"] == "cancelled"
    assert cancellation["source_active_after_event"] is True
    assert cancellation["runs_before"] == cancellation["runs_after_schedule_wait"]
    assert cancellation["runs_before"] == cancellation["runs_after_source_event"]
    assert cancellation["runs_before_snapshot"] == cancellation["runs_after_schedule_wait_snapshot"]
    assert cancellation["runs_before_snapshot"] == cancellation["runs_after_source_event_snapshot"]
    assert cancellation["provider_request_ids_before"] == cancellation[
        "provider_request_ids_after_schedule_wait"]
    assert cancellation["provider_request_ids_before"] == cancellation["provider_request_ids_after"]
    assert cancellation["provider_request_records_before"] == cancellation[
        "provider_request_records_after_schedule_wait"]
    assert cancellation["provider_request_records_before"] == cancellation[
        "provider_request_records_after"]
    assert cancellation["schedule_wait_seconds"] >= 6
    assert cancellation["source_event_wait_seconds"] >= 6
    assert [item["target_elapsed_seconds"] for item in cancellation["schedule_observations"]] == [0, 2, 4, 6]
    assert [item["target_elapsed_seconds"] for item in cancellation["source_event_observations"]] == [0, 2, 4, 6]
    assert all(item["goal_id"] == goal_id for item in (
        *cancellation["schedule_observations"],
        *cancellation["source_event_observations"],
    ))
    assert cancellation["cancel_http_response"]["status_code"] == 200
    assert cancellation["cancel_http_response"]["body"]["state"] == "cancelled"
    assert cancellation["registry_http_response"]["status_code"] == 200
    assert cancellation["registry_http_response"]["body"]["active"] is True
    assert report["source_manifest"]["before"] == report["source_manifest"]["after"]
    assert set(check["source_files"]) <= set(report["source_manifest"]["before"])
    original = report["original_r10"]
    assert original["run_id"] == "dc06-20261004T011619Z-8e87c41d"
    assert original["report_sha256"]
    assert original["source_sha256_before"] == json.loads(base_report.read_text(encoding="utf-8"))[
        "source_sha256_before"]
    assert original["source_sha256_after"] == json.loads(base_report.read_text(encoding="utf-8"))[
        "source_sha256_after"]
    changed_paths = {item["path"] for item in report["source_applicability"]["changed_since_r10"]}
    assert "data-center/src/data_center/runtime.py" in changed_paths
    assert "packages/data-center-protocol/src/data_center_protocol/__init__.py" in changed_paths
    assert all(item["path"].startswith(".scratch/evidence/")
               for item in report["evidence_files"])
    assert {item["path"] for item in report["evidence_files"]} >= set(check["evidence_files"])
    assert all(len(values) == len(set(values)) for values in (
        cancellation["provider_request_ids_before"],
        cancellation["provider_request_ids_after"],
        [item["run_id"] for item in cancellation["runs_before_snapshot"]],
        [item["run_id"] for item in cancellation["runs_after_schedule_wait_snapshot"]],
        [item["run_id"] for item in cancellation["runs_after_source_event_snapshot"]],
    ))
