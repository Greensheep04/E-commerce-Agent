import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4

from scripts.verify_data_center_reliability import validate_schedule_timeline

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUTS = ROOT / ".scratch/evidence/dc01-inputs-20261004-r4/report.json"
DEFAULT_QUALITY = ROOT / ".scratch/evidence/dc-quality-checker-r1/formal-90-report-r3.json"
DEFAULT_INSTALL = ROOT / ".scratch/evidence/dc01-independent-install-20261004-r1/run-r5/report.json"
DEFAULT_IDENTITIES = ROOT / ".scratch/evidence/dc02-host-20261004-r3/identities.json"
DEFAULT_DC06 = ROOT / ".scratch/evidence/dc06-reliability/report.json"
DEFAULT_SOURCE_APPLICABILITY = None
FIXTURE = ROOT / "tests/fixtures/data_center_cases.json"
PACKAGE_PATHS = {
    "integration-protocol": ROOT / "packages/integration-protocol",
    "data-center-protocol": ROOT / "packages/data-center-protocol",
    "data-center": ROOT / "data-center",
}
SOURCE_ROOTS = {
    "integration-protocol": ROOT / "packages/integration-protocol/src",
    "data-center-protocol": ROOT / "packages/data-center-protocol/src",
    "data-center": ROOT / "data-center/src",
}
SOURCE_DIRECTORIES = (
    "scripts",
    "src",
    "tests",
    "agent-cluster",
    "data-center",
    "packages/data-center-protocol",
    "packages/integration-protocol",
    "frontend/src",
    "alembic",
)
SOURCE_FILES = (
    "pyproject.toml",
    "uv.lock",
    "alembic.ini",
    "frontend/package.json",
    "frontend/package-lock.json",
    "frontend/tsconfig.json",
    "frontend/vite.config.ts",
)
IGNORED_PARTS = {
    ".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache", "node_modules",
    ".scratch", ".local",
}
FORMAL_STATUSES = {
    "DATA_BLOCKED", "completed", "denied", "duplicate", "failed", "forbidden",
    "missing", "partial", "running", "timeout", "unknown",
}
DC06_BASE_SOURCE_PATHS = (
    "scripts/verify_data_center_reliability.py",
    "scripts/install_data_center_watch.py",
    "tests/test_data_center_reliability.py",
    "tests/test_data_center_host.py",
    "src/shop/data_center_automation.py",
    "src/shop/data_center_api.py",
    "src/shop/data_center_adapter.py",
    "src/shop/data_center_host.py",
    "src/shop/data_center_gateway.py",
    "src/shop/budget.py",
    "src/shop/integration_models.py",
    "src/shop/runtime_models.py",
    "agent-cluster/src/agent_cluster/runtime.py",
    "agent-cluster/src/agent_cluster/store.py",
    "data-center/src/data_center/api.py",
    "data-center/src/data_center/runtime.py",
    "data-center/src/data_center/store.py",
    "data-center/src/data_center/documents/engine.py",
    "data-center/src/data_center/documents/gateway.py",
    "data-center/src/data_center/graph/engine.py",
    "data-center/src/data_center/analytics/engine.py",
    "data-center/src/data_center/exporting.py",
    "data-center/tests/test_documents_live.py",
)
DC06_SOURCE_PATHS = (*DC06_BASE_SOURCE_PATHS,
    "src/shop/data_center_registry.py",
    "src/shop/data_center_models.py",
    "alembic/versions/0016_data_center_source_version.py",
    "agent-cluster/src/agent_cluster/api.py",
    "packages/data-center-protocol/src/data_center_protocol/__init__.py",
)
DC06_SOURCE_PATHS = tuple(sorted(set(DC06_SOURCE_PATHS) | {
    path.relative_to(ROOT).as_posix()
    for directory in (
        "src/shop", "agent-cluster/src/agent_cluster", "data-center/src/data_center",
        "packages/data-center-protocol/src", "packages/integration-protocol/src",
        "packages/integration-host/src",
    )
    for path in (ROOT / directory).rglob("*")
    if path.is_file() and path.suffix in {".py", ".sql", ".yml", ".yaml"}
} | {
    "pyproject.toml", "uv.lock", "data-center/pyproject.toml", "data-center/uv.lock",
    "agent-cluster/pyproject.toml", "agent-cluster/uv.lock",
    "packages/data-center-protocol/pyproject.toml",
    "packages/integration-protocol/pyproject.toml", "packages/integration-host/pyproject.toml",
    "tests/test_data_center_reliability_sources.py",
}))
GRAPH_CACHE_SOURCE = "data-center/src/data_center/graph/engine.py"
AUTOMATION_INSTALL_SOURCE = "src/shop/data_center_automation.py"
SOURCE_APPLICABILITY_PATHS = sorted({GRAPH_CACHE_SOURCE, AUTOMATION_INSTALL_SOURCE})
GRAPH_CACHE_CHECKS = {
    "order_accepts",
    "missing_denied",
    "extra_denied",
    "duplicate_denied",
    "content_change_denied",
}
SOURCE_APPLICABILITY_CATEGORIES = {"document", "analytics", "refusal"}
DC06_REQUIRED_CHECKS = {
    "shared_shop_readonly_health",
    "published_task_snapshot_request_reuse",
    "multiprocess_duplicate_request_id",
    "full_input_note_and_persisted_schedules",
    "three_real_apscheduler_rounds_and_unchanged_waits",
    "duplicate_source_event_deduplication",
    "cancel_stops_new_scheduled_and_source_runs",
    "sigterm_during_persisted_running_task",
    "new_process_health_after_sigterm",
    "new_process_resumes_same_task_request_and_snapshot",
    "early_input_note_preserved_after_recovery",
    "permission_revoke_denies_queries_and_reference_reads",
    "budget_goal_rejects_before_provider_request",
    "budget_active_window_unchanged",
}


def digest_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def decimal_is(value, expected):
    try:
        actual = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return False
    return actual.is_finite() and actual == Decimal(expected)


def current_source_manifest():
    manifest = {}
    for package, source_root in SOURCE_ROOTS.items():
        paths = sorted(path for path in source_root.rglob("*")
                       if path.is_file() and path.suffix in {".py", ".sql", ".yml", ".yaml"})
        paths.append(PACKAGE_PATHS[package] / "pyproject.toml")
        if package == "data-center":
            paths.append(PACKAGE_PATHS[package] / "uv.lock")
        package_files = {
            str(path.relative_to(ROOT)): digest_file(path)
            for path in paths
        }
        package_hash = hashlib.sha256()
        for relative_path, file_hash in package_files.items():
            package_hash.update(relative_path.encode())
            package_hash.update(b"\0")
            package_hash.update(file_hash.encode())
            package_hash.update(b"\n")
        manifest[package] = {"sha256": package_hash.hexdigest(), "files": package_files}
    lock_digests = {
        "python311": digest_file(ROOT / "uv.lock"),
        "python314": digest_file(ROOT / "data-center/uv.lock"),
    }
    return manifest, lock_digests


def release_source_digests():
    files = {ROOT / relative for relative in SOURCE_FILES}
    for relative in SOURCE_DIRECTORIES:
        directory = ROOT / relative
        if directory.exists():
            files.update(
                path for path in directory.rglob("*")
                if path.is_file() and not any(part in IGNORED_PARTS for part in path.parts)
                and path.suffix in {".py", ".pyi", ".toml", ".json", ".ts", ".tsx", ".js", ".css"}
            )
    return {
        str(path.relative_to(ROOT)): digest_file(path)
        for path in sorted(files)
        if path.is_file()
    }


def verify_fixed_inputs(inputs_path, fixture_path=FIXTURE):
    inputs = read_json(inputs_path)
    fixture = read_json(fixture_path)
    registry_path = ROOT / inputs["source_registry"]
    if inputs.get("status") != "frozen_inputs_ready":
        raise ValueError("固定输入报告未达到 frozen_inputs_ready")
    if inputs.get("source_fixture_sha256") != digest_file(fixture_path):
        raise ValueError("固定输入报告与当前用例夹具摘要不一致")
    if inputs.get("source_registry_sha256") != digest_file(registry_path):
        raise ValueError("固定输入报告与当前来源登记摘要不一致")
    if fixture.get("fixture_id") != "data-center-cases-v1":
        raise ValueError("当前用例夹具编号不符合正式评价范围")
    source_files = inputs.get("source_files")
    if not isinstance(source_files, list) or len(source_files) != inputs.get("source_file_count"):
        raise ValueError("固定输入报告的来源文件数量不一致")
    source_root = (ROOT / ".local/data-center/sources").resolve()
    for item in source_files:
        path = (source_root / item["path"]).resolve()
        if not path.is_relative_to(source_root):
            raise ValueError("固定输入报告包含越界来源路径")
        if digest_file(path) != item["sha256"]:
            raise ValueError(f"固定来源文件摘要不一致: {item['path']}")
    return inputs, fixture


def dc06_cancellation_is_valid(facts):
    if not isinstance(facts, dict) or not isinstance(facts.get("goal_id"), str) or not facts["goal_id"]:
        return False
    if any(facts.get(key) != "cancelled" for key in (
        "state", "state_after_schedule_wait", "state_after_source_event",
    )) or facts.get("source_active_after_event") is not True:
        return False
    phase_fields = (
        ("runs_before", "runs_before_snapshot"),
        ("runs_after_schedule_wait", "runs_after_schedule_wait_snapshot"),
        ("runs_after_source_event", "runs_after_source_event_snapshot"),
    )
    identities = []
    for count_field, snapshot_field in phase_fields:
        count, snapshot = facts.get(count_field), facts.get(snapshot_field)
        if type(count) is not int or count < 0 or not isinstance(snapshot, list) or len(snapshot) != count:
            return False
        if any(not isinstance(item, dict)
               or not isinstance(item.get("run_id"), str) or not item["run_id"]
               or not isinstance(item.get("status"), str) or not item["status"]
               for item in snapshot):
            return False
        run_ids = {item["run_id"] for item in snapshot}
        if len(run_ids) != count:
            return False
        identities.append(run_ids)
    if identities[0] != identities[1] or identities[0] != identities[2]:
        return False
    requests = []
    for field in ("provider_request_ids_before", "provider_request_ids_after"):
        values = facts.get(field)
        if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
            return False
        if len(set(values)) != len(values):
            return False
        requests.append(set(values))
    if requests[0] != requests[1]:
        return False
    schedule_wait = facts.get("schedule_wait_seconds")
    event_wait = facts.get("source_event_wait_seconds")
    return (type(schedule_wait) in {int, float} and schedule_wait >= 6
            and type(event_wait) in {int, float} and event_wait >= 0)


def audit_dc06_cancellation_supplement(report_path, identity_path):
    report = read_json(report_path)
    errors = []
    if (report.get("milestone") != "DC-06" or report.get("status") != "passed"
            or report.get("report_type") != "cancellation_supplement"):
        errors.append("supplement_identity_invalid")
    if report.get("identities_sha256") != digest_file(identity_path):
        errors.append("identity_digest_mismatch")
    if (report.get("shop_url") != "http://127.0.0.1:8002"
            or report.get("database") != {"host": "127.0.0.1", "port": 55432, "name": "shop"}):
        errors.append("cancellation_environment_mismatch")
    manifest = report.get("source_manifest", {})
    before, after = manifest.get("before", {}), manifest.get("after", {})
    if not before or before != after:
        errors.append("source_changed_during_observation")
    required_sources = (
        "agent-cluster/src/agent_cluster/api.py", "agent-cluster/src/agent_cluster/store.py",
        "src/shop/runtime_models.py", "src/shop/data_center_api.py",
        "src/shop/data_center_automation.py", "src/shop/data_center_registry.py",
    )
    if any(before.get(path) != digest_file(ROOT / path) for path in required_sources):
        errors.append("cancellation_source_manifest_not_applicable")
    evidence = {}
    for item in report.get("evidence_files", []):
        relative = item.get("path")
        if not isinstance(relative, str) or Path(relative).is_absolute():
            errors.append("evidence_path_invalid")
            continue
        path = (ROOT / relative).resolve()
        if not path.is_relative_to((ROOT / ".scratch/evidence").resolve()) or not path.is_file():
            errors.append("evidence_file_missing_or_outside_scope")
            continue
        evidence[relative] = digest_file(path)
        if evidence[relative] != item.get("sha256"):
            errors.append("evidence_digest_mismatch")
    origin = report.get("original_r10", {})
    origin_path = origin.get("path")
    if origin_path not in evidence or evidence.get(origin_path) != origin.get("report_sha256"):
        errors.append("original_report_not_bound")
    elif read_json(ROOT / origin_path).get("cancellation", {}).get("goal_id") != report.get("goal_id"):
        errors.append("original_goal_identity_mismatch")
    facts = report.get("cancellation", {})
    if report.get("goal_id") != facts.get("goal_id") or not dc06_cancellation_is_valid(facts):
        errors.append("cancellation_facts_failed")
    check_id = "cancel_stops_new_scheduled_and_source_runs"
    checks = report.get("checks", [])
    if len(checks) != 1 or checks[0].get("id") != check_id or checks[0].get("passed") is not True:
        errors.append("supplement_check_invalid")
    elif checks[0].get("facts") != facts or any(path not in evidence for path in checks[0].get("evidence_files", [])):
        errors.append("supplement_check_not_bound")
    files = {Path(path).name: ROOT / path for path in evidence}
    for name, expected in (
        ("cancellation.json", facts),
        ("cancel-response.json", facts.get("cancel_http_response")),
        ("registry-response.json", facts.get("registry_http_response")),
    ):
        if name not in files or read_json(files[name]) != expected:
            errors.append("cancellation_response_not_bound:" + name)
    cancel_response, registry_response = facts.get("cancel_http_response", {}), facts.get("registry_http_response", {})
    if (cancel_response.get("status_code") != 200
            or cancel_response.get("body") != {"goal_id": facts.get("goal_id"), "state": "cancelled"}
            or registry_response.get("status_code") != 200
            or registry_response.get("body", {}).get("active") is not True):
        errors.append("cancellation_http_response_invalid")
    run_ids = {item["run_id"] for item in facts.get("runs_before_snapshot", [])}
    request_records = facts.get("provider_request_records_before", [])
    for filename, field in (
        ("schedule-observations.json", "schedule_observations"),
        ("source-event-observations.json", "source_event_observations"),
    ):
        observed = read_json(files[filename]) if filename in files else {}
        observations = observed.get("observations", [])
        if (observations != facts.get(field) or len(observations) != 4
                or [item.get("target_elapsed_seconds") for item in observations] != [0, 2, 4, 6]
                or observed.get("duration_seconds") != 6 or observed.get("interval_seconds") != 2
                or observed.get("elapsed_seconds", 0) < 6
                or any(item.get("goal_id") != facts.get("goal_id") or item.get("state") != "cancelled"
                       or {run["run_id"] for run in item.get("runs", [])} != run_ids
                       or item.get("provider_request_records") != request_records
                       for item in observations)):
            errors.append("cancellation_observations_not_bound:" + filename)
    return {
        "status": "passed" if not errors else "rejected",
        "run_id": report.get("run_id"), "report_sha256": digest_file(report_path),
        "goal_id": facts.get("goal_id"), "accepted_checks": [check_id] if not errors else [],
        "evidence_files": evidence,
        "source_sha256": {path: before.get(path) for path in required_sources},
        "errors": sorted(set(errors)),
    }


def audit_dc06_schedule(report, evidence_by_path):
    checks = [item for item in report.get("checks", []) if isinstance(item, dict)
              and item.get("id") == "three_real_apscheduler_rounds_and_unchanged_waits"]
    if len(checks) != 1:
        return ["schedule_check_not_unique"]
    check = checks[0]
    documents = {}
    for name in ("schedule-baseline.json", "schedule-rounds.json",
                 "schedule-events.json", "schedule-timeline.json"):
        paths = [path for path in check.get("evidence_files", [])
                 if path in evidence_by_path and Path(path).name == name]
        if len(paths) != 1:
            return ["schedule_evidence_not_bound:" + name]
        documents[name] = read_json(ROOT / paths[0])
    baseline = documents["schedule-baseline.json"]
    rounds = documents["schedule-rounds.json"]
    events = documents["schedule-events.json"]
    if (baseline != report.get("schedule_baseline") or rounds != report.get("schedule_rounds")
            or not isinstance(rounds, list) or len(rounds) != 3
            or not isinstance(events, list) or len(events) != 4):
        return ["schedule_records_not_bound"]
    try:
        runs = [baseline, *rounds]
        run_ids = {run["run_id"] for run in runs}
        events_by_run = {event["run_id"]: event for event in events}
        if len(run_ids) != 4 or len(events_by_run) != 4 or run_ids != set(events_by_run):
            return ["schedule_event_run_identity_invalid"]
        setup = report["schedule_setup"]
        timeline = validate_schedule_timeline(
            runs, events_by_run, setup["interval_schedule_id"], setup["interval_seconds"],
        )
        if any(run["scheduled_at"] != events_by_run[run["run_id"]]["scheduled_at"]
               or run["coalesced_periods"] != events_by_run[run["run_id"]]["coalesced_periods"]
               for run in runs):
            return ["schedule_run_event_fields_inconsistent"]
        expected_facts = {
            "goal_id": setup["goal_id"], "round_count": 3,
            "baseline_run_id": baseline["run_id"], "baseline_status": baseline["status"],
            "baseline_created_at": baseline["created_at"],
            "statuses": [run["status"] for run in rounds],
            "waiting_task_count": sum(run["task_count"] for run in rounds if run["status"] == "waiting"),
            "completed_run_count": sum(run["status"] == "completed" for run in rounds),
            "trigger_times_in_order": timeline["trigger_times_in_order"][1:],
            "trigger_gaps_seconds": timeline["trigger_gaps_seconds"],
            "coalesced_periods": timeline["coalesced_periods"][1:],
            "coalesced_gaps_accounted": timeline["coalesced_gaps_accounted"],
        }
        if (baseline["status"] not in {"completed", "waiting"}
                or expected_facts["statuses"] != ["waiting"] * 3
                or expected_facts["waiting_task_count"] != 0
                or timeline != documents["schedule-timeline.json"]
                or timeline != report.get("schedule_timeline")
                or check.get("facts") != expected_facts):
            return ["schedule_timeline_or_summary_inconsistent"]
    except (AssertionError, KeyError, TypeError, ValueError):
        return ["schedule_timeline_invalid"]
    return []


def audit_dc06_report(report_path, identity_path):
    report = read_json(report_path)
    errors = []
    if report.get("milestone") != "DC-06":
        errors.append("milestone_mismatch")
    if report.get("status") != "passed" or report.get("failed_at") or report.get("failure"):
        errors.append("run_not_passed")
    if not isinstance(report.get("run_id"), str) or not report["run_id"].startswith("dc06-"):
        errors.append("run_identity_missing")
    try:
        datetime.fromisoformat(report["started_at"])
    except (KeyError, TypeError, ValueError):
        errors.append("started_at_invalid")
    if report.get("identities_sha256") != digest_file(identity_path):
        errors.append("identity_digest_mismatch")
    if report.get("shop_url") != "http://127.0.0.1:8002":
        errors.append("shop_environment_mismatch")
    if report.get("data_center_url") != "http://127.0.0.1:8101":
        errors.append("data_center_environment_mismatch")
    if report.get("database") != {"host": "127.0.0.1", "port": 55432, "name": "shop_data_center"}:
        errors.append("database_environment_mismatch")
    source_before = report.get("source_sha256_before")
    source_after = report.get("source_sha256_after")
    expected_sources = {
        path: digest_file(ROOT / path)
        for path in DC06_SOURCE_PATHS
        if (ROOT / path).is_file()
    }
    if source_before != expected_sources or source_after != expected_sources:
        errors.append("source_manifest_not_applicable")
    if report.get("source_unchanged") is not True:
        errors.append("source_changes_not_ruled_out")
    service_start = report.get("service_start", {})
    if service_start.get("pid") != report.get("service_health", {}).get("pid"):
        errors.append("service_start_identity_mismatch")
    if report.get("service_health", {}).get("status") != "ok":
        errors.append("service_health_missing")
    checks = report.get("checks", [])
    check_ids = [item.get("id") if isinstance(item, dict) else item for item in checks]
    duplicate_checks = sorted({item for item in check_ids if check_ids.count(item) > 1})
    missing_checks = sorted(DC06_REQUIRED_CHECKS - set(check_ids))
    unexpected_checks = sorted(set(check_ids) - DC06_REQUIRED_CHECKS)
    failed_checks = sorted(
        item.get("id") for item in checks
        if isinstance(item, dict) and item.get("passed") is not True
    )
    if duplicate_checks:
        errors.append("duplicate_check_ids")
    if missing_checks:
        errors.append("required_checks_missing")
    if unexpected_checks:
        errors.append("unexpected_check_ids")
    if failed_checks:
        errors.append("check_result_failed")
    if any(not isinstance(item, dict) for item in checks):
        errors.append("check_results_not_individually_audited")
    evidence = report.get("evidence_files", [])
    evidence_by_path = {}
    for item in evidence:
        path_value = item.get("path")
        if not isinstance(path_value, str) or Path(path_value).is_absolute():
            errors.append("evidence_path_invalid")
            continue
        path = (ROOT / path_value).resolve()
        if not path.is_relative_to(ROOT / ".scratch/evidence") or not path.is_file():
            errors.append("evidence_file_missing_or_outside_scope")
            continue
        actual_digest = digest_file(path)
        if item.get("sha256") != actual_digest:
            errors.append("evidence_digest_mismatch")
        evidence_by_path[path_value] = actual_digest
    for item in checks:
        if not isinstance(item, dict):
            continue
        check_evidence = item.get("evidence_files")
        if not isinstance(check_evidence, list) or not check_evidence:
            errors.append("check_evidence_missing")
            continue
        if any(path not in evidence_by_path for path in check_evidence):
            errors.append("check_evidence_not_bound")
        if not isinstance(item.get("facts"), dict) or not item["facts"]:
            errors.append("check_facts_missing")
    facts = {
        "request_reuse": report.get("request_reuse", {}),
        "concurrent_request_reuse": report.get("concurrent_request_reuse", []),
        "schedule_rounds": report.get("schedule_rounds", []),
        "duplicate_source_event": report.get("duplicate_source_event", {}),
        "cancellation": report.get("cancellation", {}),
        "process_termination": report.get("process_termination", {}),
        "process_restart": report.get("process_restart", {}),
        "process_recovery": report.get("process_recovery", {}),
    }
    reuse = facts["request_reuse"]
    if (not reuse or reuse.get("prior_task_id") != reuse.get("replayed_task_id")
            or not reuse.get("run_id") or not reuse.get("snapshot_id")
            or reuse.get("model_requests") != []):
        errors.append("request_reuse_facts_failed")
    concurrent = facts["concurrent_request_reuse"]
    if (len(concurrent) != 2 or len({item.get("task_id") for item in concurrent}) != 1
            or len({item.get("run_id") for item in concurrent}) != 1
            or len({item.get("snapshot_id") for item in concurrent}) != 1
            or any(item.get("status") != "completed" or item.get("snapshot_state") != "published"
                   or item.get("usage", {}).get("requests") != [] for item in concurrent)):
        errors.append("multiprocess_reuse_facts_failed")
    rounds = facts["schedule_rounds"]
    if (len(rounds) != 3 or len({item.get("trigger_id") for item in rounds}) != 3
            or any(item.get("status") != "waiting" or item.get("task_count") != 0
                   or item.get("spent_cny") != "0" or item.get("reserved_cny") != "0"
                   for item in rounds)):
        errors.append("schedule_round_facts_failed")
    errors.extend(audit_dc06_schedule(report, evidence_by_path))
    event = facts["duplicate_source_event"]
    if (event.get("event_run_count") != 1 or event.get("registry_calls") != 2
            or not event.get("run_id") or not event.get("trigger_id") or not event.get("note_id")):
        errors.append("duplicate_event_facts_failed")
    cancellation = facts["cancellation"]
    if not dc06_cancellation_is_valid(cancellation):
        errors.append("cancellation_facts_failed")
    termination = facts["process_termination"]
    restart = facts["process_restart"]
    recovery = facts["process_recovery"]
    if (termination.get("signal") != "SIGTERM" or termination.get("remaining_listeners") != []
            or termination.get("status_at_termination") != "running"
            or not termination.get("task_id") or not termination.get("run_id")):
        errors.append("process_termination_facts_failed")
    if (not restart.get("pid") or restart.get("pid") == termination.get("pid")
            or restart.get("health_status") != "ok"):
        errors.append("process_restart_facts_failed")
    identities = read_json(identity_path)
    sync_actor_id = identities["actors"]["sync"]["actor_id"]
    if (not recovery or recovery.get("actor_id") != sync_actor_id
            or recovery.get("task_id") != termination.get("task_id")
            or recovery.get("run_id") != termination.get("run_id")
            or recovery.get("request_id") != termination.get("request_id")
            or not recovery.get("request_id")
            or recovery.get("status") != "completed"
            or not recovery.get("snapshot_id")
            or recovery.get("model_requests") != []):
        errors.append("process_recovery_facts_failed")
    check_facts = {
        item["id"]: item.get("facts", {}) if isinstance(item.get("facts"), dict) else {}
        for item in checks if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    cancel_check = next((item for item in checks if isinstance(item, dict)
                         and item.get("id") == "cancel_stops_new_scheduled_and_source_runs"), {})
    if cancel_check.get("facts") != cancellation:
        errors.append("cancellation_summary_mismatch")
    cancellation_files = cancel_check.get("evidence_files", [])
    if not any(path in evidence_by_path and Path(path).name == "cancellation.json"
               and read_json(ROOT / path) == cancellation for path in cancellation_files):
        errors.append("cancellation_evidence_not_bound")
    permission = check_facts.get("permission_revoke_denies_queries_and_reference_reads", {})
    try:
        permission_valid = (
            isinstance(permission.get("source_id"), str) and bool(permission["source_id"])
            and isinstance(permission.get("original_snapshot"), str)
            and isinstance(permission.get("current_snapshot"), str)
            and permission["original_snapshot"] != permission["current_snapshot"]
            and type(permission.get("graph_reference_count")) is int
            and permission["graph_reference_count"] == 44
            and permission.get("permission_reduced") is True
            and permission.get("source_withdrawn") is True
            and permission.get("usage_unchanged_after_denial") is True
        )
    except (KeyError, TypeError):
        permission_valid = False
    if not permission_valid:
        errors.append("permission_revoke_facts_failed")
    if report.get("permission_reuse") != permission:
        errors.append("permission_reuse_summary_mismatch")
    budget = check_facts.get("budget_goal_rejects_before_provider_request", {})
    try:
        budget_valid = (
            budget.get("database") == "shop_test"
            and budget.get("scenario") == "test_data_task_budget_obeys_parent_goal_before_integration_mapping"
            and budget.get("rejection_type") == "BudgetExceeded"
            and type(budget.get("provider_request_count_before")) is int
            and budget["provider_request_count_before"] == 0
            and type(budget.get("provider_request_count_after")) is int
            and budget["provider_request_count_after"] == 0
            and isinstance(budget.get("goal_budget_cny"), str)
            and decimal_is(budget.get("goal_budget_cny"), "0.001")
        )
    except (InvalidOperation, TypeError, ValueError):
        budget_valid = False
    if not budget_valid:
        errors.append("budget_rejection_facts_failed")
    active_budget = check_facts.get("budget_active_window_unchanged", {})
    expected_window = {
        "id": "dc-live-20261004",
        "active": True,
        "limit_cny": "50.00000000",
        "include_open_reservations": True,
    }
    if (active_budget.get("database") != "shop_test"
            or active_budget.get("scenario") != "test_validation_window_includes_existing_unsent_reservation"
            or active_budget.get("rejection_type") != "BudgetExceeded"
            or type(active_budget.get("provider_request_count_before")) is not int
            or active_budget.get("provider_request_count_before") != 1
            or type(active_budget.get("provider_request_count_after")) is not int
            or active_budget.get("provider_request_count_after") != 1
            or active_budget.get("prior_reservation_status") != "reserved"
            or not isinstance(active_budget.get("prior_reservation_cny"), str)
            or not decimal_is(active_budget.get("prior_reservation_cny"), "0.00480000")
            or not isinstance(active_budget.get("window_limit_cny"), str)
            or not decimal_is(active_budget.get("window_limit_cny"), "0.006")
            or active_budget.get("window_id") != "dc-live-20261004"
            or active_budget.get("window_before") != expected_window
            or active_budget.get("window_after") != expected_window
            or active_budget.get("unchanged") is not True):
        errors.append("budget_active_window_facts_failed")
    budget_validation = report.get("budget_validation", {})
    if (not isinstance(budget_validation, dict)
            or budget_validation.get("goal_rejection") != budget
            or budget_validation.get("active_window_unchanged") != active_budget):
        errors.append("budget_validation_summary_mismatch")
    return {
        "status": "passed" if not errors else "rejected",
        "source_status": report.get("status"),
        "report_sha256": digest_file(report_path),
        "run_id": report.get("run_id"),
        "failed_at": report.get("failed_at"),
        "last_check": report.get("failure", {}).get("last_check"),
        "failure": report.get("failure"),
        "identity_sha256": report.get("identities_sha256"),
        "source_sha256": source_before,
        "evidence_files": evidence_by_path,
        "required_checks": sorted(DC06_REQUIRED_CHECKS),
        "observed_checks": check_ids,
        "missing_checks": missing_checks,
        "unexpected_checks": unexpected_checks,
        "failed_checks": failed_checks,
        "errors": sorted(set(errors)),
    }


def audit_dc06_partial_report(report_path, identity_path):
    report = read_json(report_path)
    report_digest = digest_file(report_path)
    checks = report.get("checks", [])
    check_ids = [item.get("id") for item in checks if isinstance(item, dict)]
    errors = []
    if report.get("milestone") != "DC-06":
        errors.append("milestone_mismatch")
    if report.get("status") != "failed":
        errors.append("source_status_not_failed")
    if report.get("identities_sha256") != digest_file(identity_path):
        errors.append("identity_digest_mismatch")
    if len(check_ids) != 10 or len(set(check_ids)) != len(check_ids):
        errors.append("observed_check_set_invalid")
    source_before = report.get("source_sha256_before", {})
    expected_paths = set(DC06_BASE_SOURCE_PATHS)
    if set(source_before) != expected_paths:
        errors.append("source_manifest_path_set_invalid")
    source_differences = []
    source_manifest_current = {}
    for relative_path in DC06_BASE_SOURCE_PATHS:
        path = ROOT / relative_path
        old_digest = source_before.get(relative_path)
        if not path.is_file():
            current_digest = None
            errors.append("current_source_missing")
        else:
            current_digest = digest_file(path)
            source_manifest_current[relative_path] = current_digest
        if not isinstance(old_digest, str) or len(old_digest) != 64:
            errors.append("historical_source_digest_invalid")
        category = (
            "test" if "/tests/" in f"/{relative_path}/"
            else "script" if relative_path.startswith("scripts/")
            else "production"
        )
        source_differences.append({
            "path": relative_path,
            "category": category,
            "sha256_at_r8": old_digest,
            "sha256_current": current_digest,
            "state": "unchanged" if old_digest == current_digest else "changed_unverified",
        })

    evidence_by_path = {}
    check_results = []
    for item in checks:
        if not isinstance(item, dict):
            continue
        check_id = item.get("id")
        evidence_items = []
        valid_evidence = True
        for evidence_path in item.get("evidence_files", []):
            if not isinstance(evidence_path, str) or Path(evidence_path).is_absolute():
                valid_evidence = False
                errors.append("evidence_path_invalid")
                continue
            resolved = (ROOT / evidence_path).resolve()
            if not resolved.is_relative_to((ROOT / ".scratch/evidence").resolve()) and evidence_path not in DC06_SOURCE_PATHS:
                valid_evidence = False
                errors.append("evidence_outside_allowed_scope")
                continue
            if not resolved.is_file():
                valid_evidence = False
                errors.append("evidence_file_missing")
                continue
            evidence_digest = digest_file(resolved)
            evidence_by_path[evidence_path] = evidence_digest
            evidence_items.append({
                "path": evidence_path,
                "sha256_current": evidence_digest,
                "digest_bound_by_r8": False,
            })
        facts = item.get("facts", {})
        audit_status = "reported_pass_unverified"
        findings = []
        if item.get("passed") is not True:
            audit_status = "reported_failure"
            findings.append("r8_check_not_passed")
        elif not valid_evidence or not evidence_items:
            audit_status = "evidence_missing"
            findings.append("evidence_missing_or_invalid")
        elif not isinstance(facts, dict) or not facts:
            audit_status = "facts_missing"
            findings.append("check_facts_missing")
        else:
            evidence_json = {}
            for evidence in evidence_items:
                if evidence["path"].endswith(".json"):
                    evidence_json[evidence["path"]] = read_json(ROOT / evidence["path"])
            if check_id == "shared_shop_readonly_health":
                health = evidence_json.get(".scratch/evidence/dc06-reliability-20261004-r8/shared-shop-health.json", {})
                if health.get("http_status") != 200 or health.get("body") != {"status": "ok"}:
                    findings.append("health_evidence_mismatch")
            elif check_id == "published_task_snapshot_request_reuse":
                reuse = evidence_json.get(".scratch/evidence/dc06-reliability-20261004-r8/request-reuse.json", {})
                if (reuse.get("prior_task_id") != reuse.get("replayed_task_id")
                        or reuse.get("run_id") != facts.get("run_id")
                        or reuse.get("snapshot_id") != facts.get("snapshot_id")
                        or reuse.get("model_requests") != []
                        or facts.get("task_id_reused") is not True):
                    findings.append("request_reuse_evidence_mismatch")
            elif check_id == "multiprocess_duplicate_request_id":
                results = evidence_json.get(".scratch/evidence/dc06-reliability-20261004-r8/multiprocess-request-reuse.json", [])
                if (len(results) != 2
                        or len({result.get("task_id") for result in results}) != 1
                        or len({result.get("run_id") for result in results}) != 1
                        or len({result.get("snapshot_id") for result in results}) != 1
                        or any(result.get("status") != "completed"
                               or result.get("snapshot_state") != "published"
                               or result.get("usage", {}).get("requests") != [] for result in results)):
                    findings.append("multiprocess_evidence_mismatch")
            elif check_id == "full_input_note_and_persisted_schedules":
                setup = evidence_json.get(".scratch/evidence/dc06-reliability-20261004-r8/schedule-setup.json", {})
                watch = evidence_json.get(".scratch/evidence/dc06-reliability-20261004-r8/active-watch-install.json", {})
                note = watch.get("note", {})
                if (setup.get("goal_id") != facts.get("goal_id")
                        or setup.get("watch_id") != facts.get("watch_id")
                        or setup.get("input_note_id") != note.get("note_id")
                        or not note.get("sha256") or not isinstance(note.get("summary"), str)):
                    findings.append("watch_setup_evidence_mismatch")
                findings.append("full_note_content_not_present")
            elif check_id == "three_real_apscheduler_rounds_and_unchanged_waits":
                rounds = evidence_json.get(".scratch/evidence/dc06-reliability-20261004-r8/schedule-rounds.json", [])
                if (len(rounds) != 3 or facts.get("round_count") != 3
                        or any(result.get("status") != "waiting" or result.get("task_count") != 0
                               or result.get("spent_cny") != "0" or result.get("reserved_cny") != "0"
                               for result in rounds)):
                    findings.append("schedule_round_evidence_contradicts_unchanged_waits")
            elif check_id == "duplicate_source_event_deduplication":
                event = evidence_json.get(".scratch/evidence/dc06-reliability-20261004-r8/duplicate-source-event.json", {})
                if (event.get("event_run_count") != 1 or event.get("registry_calls") != 2
                        or event.get("run_id") != facts.get("run_id")
                        or event.get("trigger_id") != facts.get("trigger_id")
                        or event.get("note_id") != facts.get("note_id")):
                    findings.append("duplicate_event_evidence_mismatch")
            elif check_id == "cancel_stops_new_scheduled_and_source_runs":
                cancellation = evidence_json.get(".scratch/evidence/dc06-reliability-20261004-r8/cancellation.json", {})
                if not dc06_cancellation_is_valid(cancellation):
                    findings.append("cancellation_evidence_contradicts_no_new_runs")
            elif check_id == "budget_goal_rejects_before_provider_request":
                if (facts.get("rejection_type") != "BudgetExceeded"
                        or facts.get("provider_request_count_before") != 0
                        or facts.get("provider_request_count_after") != 0
                        or facts.get("goal_budget_cny") != "0.001"):
                    findings.append("budget_goal_facts_invalid")
                findings.append("database_state_not_independently_bound")
            elif check_id == "budget_active_window_unchanged":
                if (facts.get("rejection_type") != "BudgetExceeded"
                        or facts.get("window_before") != facts.get("window_after")
                        or facts.get("unchanged") is not True
                        or facts.get("prior_reservation_status") != "reserved"):
                    findings.append("budget_window_facts_invalid")
                findings.append("database_state_not_independently_bound")
            elif check_id == "permission_revoke_denies_queries_and_reference_reads":
                if (facts.get("permission_reduced") is not True
                        or facts.get("source_withdrawn") is not True
                        or facts.get("usage_unchanged_after_denial") is not True):
                    findings.append("permission_facts_invalid")
                findings.append("evidence_digest_not_bound_by_r8")
            if findings:
                audit_status = "contradicted_or_incomplete"
            else:
                audit_status = "evidence_consistent_but_digest_unbound"
        check_results.append({
            "id": check_id,
            "reported_pass": item.get("passed") is True,
            "audit_status": audit_status,
            "findings": findings,
            "facts": facts,
            "evidence_files": evidence_items,
        })
    missing_checks = sorted(DC06_REQUIRED_CHECKS - set(check_ids))
    changed_sources = [item for item in source_differences if item["state"] != "unchanged"]
    if missing_checks:
        errors.append("required_checks_missing")
    if report.get("source_sha256_after") is None:
        errors.append("source_after_missing")
    if not isinstance(report.get("evidence_files"), list):
        errors.append("top_level_evidence_digest_manifest_missing")
    return {
        "status": "partial",
        "source_status": report.get("status"),
        "report_sha256": report_digest,
        "run_id": report.get("run_id"),
        "failed_at": report.get("failed_at"),
        "failure": report.get("failure"),
        "identity_sha256": report.get("identities_sha256"),
        "source_sha256_at_r8": source_before,
        "source_sha256_current": source_manifest_current,
        "source_differences": source_differences,
        "changed_source_paths": [item["path"] for item in changed_sources],
        "observed_check_ids": check_ids,
        "missing_checks": missing_checks,
        "checks": check_results,
        "evidence_files_sha256_current": evidence_by_path,
        "errors": sorted(set(errors)),
        "proof_limitations": [
            "r8_has_no_source_sha256_after",
            "r8_has_no_top_level_evidence_digest_manifest",
            "r8_evidence_digests_are_captured_during_this_audit_only",
            "database_query_failure_did_not_return_rows",
            "8101_access_log_has_no_request_id",
        ],
    }


def quality_source_digests():
    directories = (
        ROOT / "data-center/src", ROOT / "packages/data-center-protocol/src",
        ROOT / "src/shop", ROOT / "agent-cluster/src", ROOT / "agent-hub/src",
    )
    paths = {path for directory in directories for path in directory.rglob("*")
             if path.is_file() and path.suffix in {".py", ".sql", ".yml", ".yaml"}}
    paths.update(ROOT / name for name in (
        "pyproject.toml", "uv.lock", "data-center/pyproject.toml", "data-center/uv.lock",
        "packages/data-center-protocol/pyproject.toml",
    ))
    return {
        str(path.relative_to(ROOT)): digest_file(path)
        for path in sorted(paths)
    }


PERSISTENCE_APPLICABILITY_PATHS = sorted([
    "data-center/src/data_center/database.py",
    "data-center/src/data_center/documents/engine.py",
    "data-center/src/data_center/documents/gateway.py",
    "data-center/src/data_center/documents/models.py",
    "data-center/src/data_center/graph/engine.py",
    "data-center/src/data_center/graph/receipts.py",
    "data-center/src/data_center/ports.py",
    "packages/data-center-protocol/src/data_center_protocol/__init__.py",
    "src/shop/data_center_api.py",
    "src/shop/data_center_gateway.py",
    "src/shop/data_center_models.py",
])


def audit_persistence_source_applicability(path, applicability, current_sources):
    categories = ["analytics", "cross_document", "document", "refusal"]
    if (applicability.get("changed_paths") != PERSISTENCE_APPLICABILITY_PATHS
            or applicability.get("allowed_categories") != categories
            or applicability.get("source_sha256_after") != current_sources):
        raise ValueError("保存恢复适用性记录的源码或类别范围不一致")
    if not isinstance(applicability.get("reason"), str) or not applicability["reason"].strip():
        raise ValueError("保存恢复适用性记录缺少依据说明")
    originals = applicability.get("source_manifests_before")
    added = "data-center/src/data_center/graph/receipts.py"
    if not isinstance(originals, list) or not originals:
        raise ValueError("保存恢复适用性记录缺少原源码清单")
    for original in originals:
        if not isinstance(original, dict) or set(original) != set(current_sources) - {added}:
            raise ValueError("保存恢复适用性记录的原源码清单不完整")
        differences = sorted(name for name in current_sources if original.get(name) != current_sources[name])
        if differences != PERSISTENCE_APPLICABILITY_PATHS:
            raise ValueError("保存恢复适用性记录包含未登记的源码变化")

    def evidence(name):
        entry = applicability["evidence"][name]
        target = (ROOT / entry["path"]).resolve()
        if (Path(entry["path"]).is_absolute()
                or not target.is_relative_to((ROOT / ".scratch/evidence").resolve())
                or not target.is_file() or digest_file(target) != entry["sha256"]):
            raise ValueError("保存恢复检查的路径或文件摘要不一致")
        return target

    baseline = read_json(evidence("recovery_baseline"))
    final = read_json(evidence("recovery_final"))
    stopped = read_json(evidence("recovery_terminated"))
    resumed = read_json(evidence("recovery_resumed"))
    complete = read_json(evidence("recovery_complete"))
    vectors = read_json(evidence("vector_proof"))
    if (final.get("exit_code") != 0 or stopped.get("exit_code") != -15
            or stopped["task"]["status"] != "running" or stopped["task"]["result"] is not None
            or resumed["same_identity"] != final["identity"]
            or complete["identity"] != final["identity"]
            or complete.get("duplicate_build_and_run_new_requests") != 0
            or vectors["identity"] != final["identity"]
            or vectors.get("source_count") != 7 or vectors.get("reference_count") != 247
            or vectors.get("entity_vector_count") != 1734 or vectors.get("relation_vector_count") != 2662
            or len(vectors["vectors"]["entity"]) != 1734 or len(vectors["vectors"]["relation"]) != 2662
            or vectors.get("model_response_vectors_exact_float32_match") is not True
            or final["snapshot"]["provider_sha256"] != baseline["snapshot"]["provider_sha256"]
            or any(final["snapshot"]["cache_sha256"].get(key) != value
                   for key, value in baseline["snapshot"]["cache_sha256"].items())):
        raise ValueError("Graph完整恢复、向量或重复执行检查未通过")
    for report in (final, stopped):
        if (report.get("old_provider_preserved") is not True
                or report.get("old_sdk_cache_preserved") is not True):
            raise ValueError("Graph恢复没有保留历史请求及缓存")
    for name in PERSISTENCE_APPLICABILITY_PATHS:
        if final["source_sha256"].get(name) != current_sources[name]:
            raise ValueError("Graph恢复记录没有绑定当前保存源码")
    migration = "alembic/versions/0017_data_center_model_reuse.py"
    if final["source_sha256"].get(migration) != digest_file(ROOT / migration):
        raise ValueError("Graph恢复记录没有绑定当前迁移")
    review = read_json(evidence("integration_review"))
    if review.get("status") != "no_confirmed_remaining_findings" or review.get("findings") != []:
        raise ValueError("保存恢复集成审查尚有未完成项目")
    for name in PERSISTENCE_APPLICABILITY_PATHS:
        if review["source_sha256"].get(str(ROOT / name)) != current_sources[name]:
            raise ValueError("保存恢复集成审查的源码摘要不一致")
    http = read_json(evidence("http_reuse"))
    if (http.get("status") != "passed" or http.get("provider_records_unchanged") is not True
            or http.get("fees_unchanged") is not True or len(http.get("checks", {})) != 7
            or http["provider_before"] != http["provider_after"]
            or http["checks"]["parallel_runs"]["http_statuses"] != [200, 200]
            or http["checks"]["independent_process_readback"]["process"]["returncode"] != 0
            or http["checks"]["cross_actor_rejection"]["http_status"] != 403
            or http["checks"]["cancelled_task_rejections"]["reuse_after_cancel_http_status"] != 403):
        raise ValueError("保存恢复的真实HTTP检查未通过")
    checks = http["checks"]
    expected_rejections = {
        "request_id_rejections": {
            "missing_id_http_status", "malformed_id_http_status",
            "wrong_completed_id_http_status", "unknown_id_http_status",
        },
        "reserved_id_rejection": {"http_status"},
        "scope_and_request_identity_rejections": {
            "changed_source_set_http_status", "changed_request_body_http_status", "changed_step_key_http_status",
        },
    }
    for name, fields in expected_rejections.items():
        if set(checks[name]) != fields or any(checks[name][field] != 422 for field in fields):
            raise ValueError("保存恢复的请求身份或范围拒绝检查未通过")
    if checks["cancelled_task_rejections"] != {
        "reuse_before_cancel_http_status": 200, "cancel_http_status": 200,
        "lookup_after_cancel_http_status": 403, "reuse_after_cancel_http_status": 403,
    }:
        raise ValueError("保存恢复的取消检查未通过")
    parallel = checks["parallel_runs"]
    original_request = http["original"]["request_id"]
    if (parallel["reply_request_ids"] != [original_request, original_request]
            or len(parallel["bindings"]) != 2
            or len({item["task_id"] for item in parallel["bindings"]}) != 2
            or len({item["run_id"] for item in parallel["bindings"]}) != 2
            or len(parallel["usage"]) != 2):
        raise ValueError("保存恢复并发记录的任务身份不一致")
    for binding, usage in zip(parallel["bindings"], parallel["usage"], strict=True):
        if (usage["task_id"] != binding["task_id"] or usage["run_id"] != binding["run_id"]
                or usage["requests"] != [] or not decimal_is(usage["spent_cny"], 0)
                or not decimal_is(usage["reserved_cny"], 0) or len(usage["reused_requests"]) != 1
                or usage["reused_requests"][0]["request_id"] != original_request
                or usage["reused_requests"][0]["original_run_id"] != http["original"]["run_id"]):
            raise ValueError("保存恢复并发记录的原请求或费用不一致")
    readback = checks["independent_process_readback"]["result"]
    if (readback["task_id"] != parallel["bindings"][0]["task_id"]
            or readback["run_id"] != parallel["bindings"][0]["run_id"]
            or readback["lookup_http_status"] != 200 or readback["usage_http_status"] != 200
            or readback["reply"]["request_id"] != original_request
            or readback["reply"]["status"] != "completed"
            or readback["reply"]["response_sha256"] != http["original"]["response_body_sha256"]
            or readback["usage"] != parallel["usage"][0]
            or checks["cross_actor_rejection"]["bound_actor_id"] != http["actor_id"]
            or checks["cross_actor_rejection"]["request_actor_id"] == http["actor_id"]):
        raise ValueError("保存恢复的独立进程或用户身份检查未通过")
    host_cases = {
        ("tests.test_integration_migrations", f"test_if03_merge_migration_preserves_each_existing_branch[{previous}]")
        for previous in ("base", "0008_customer_tool_attempts", "0005_assessment", "0016_data_center_source_version")
    } | {
        ("tests.test_data_center_model_reuse", name) for name in (
            "test_completed_graph_embedding_reuses_original_request_in_new_run",
            "test_reused_graph_embedding_rejects_changed_input_and_withdrawn_source",
            "test_explicit_saved_request_reuse_never_creates_paid_request",
        )
    } | {
        ("tests.test_data_center_service_identity", f"test_user_credentials_cannot_access_internal_task_and_model_endpoints[{identity}]")
        for identity in ("missing", "unrelated", "user_credential")
    } | {("tests.test_data_center_service_identity", "test_service_identity_preserves_user_authorization_and_cancellation")}
    protocol_names = {
        "test_required_document_sources_are_serialized_only_when_requested",
        "test_required_sources_validate_the_real_document_candidate_set",
        "test_required_sources_reject_duplicate_and_unselected_sources[DataQuestion]",
        "test_required_sources_reject_duplicate_and_unselected_sources[DataQuery]",
        "test_business_query_rejects_required_document_sources",
        "test_data_query_rejects_actor_and_sql_fields",
        "test_metric_query_requires_valid_half_open_period_and_registered_names",
        "test_document_reference_rejects_unbound_or_invalid_locations",
        "test_completed_result_requires_bound_sources_and_evidence",
        "test_final_answer_accepts_16384_output_tokens",
        "test_other_chat_steps_keep_8192_output_limit[query-plan]",
        "test_other_chat_steps_keep_8192_output_limit[graph-extract]",
        "test_embedding_keeps_8192_output_limit_even_with_answer_step_key",
    }
    protocol_cases = {("tests.test_data_center_protocol", name) for name in protocol_names}
    for name, expected in (("host_tests", host_cases), ("protocol_tests_314", protocol_cases)):
        cases = ET.parse(evidence(name)).getroot().findall(".//testcase")
        observed = {(case.get("classname"), case.get("name")) for case in cases}
        if (len(cases) != len(expected) or observed != expected or any(case.find(item) is not None
                for case in cases for item in ("failure", "error", "skipped"))):
            raise ValueError("保存恢复的迁移、权限或协议回归未通过")
    return {**applicability, "status": "reusable", "path": str(Path(path).resolve()),
            "sha256": digest_file(path)}


def audit_source_applicability(path, current_sources):
    if path is None:
        return None
    applicability = read_json(path)
    if applicability.get("schema") == "dc07.persistence_source_applicability.v1":
        return audit_persistence_source_applicability(path, applicability, current_sources)
    if applicability.get("schema") != "dc07.source_applicability.v1":
        raise ValueError("源码适用记录的schema不受支持")
    if applicability.get("changed_paths") != SOURCE_APPLICABILITY_PATHS:
        raise ValueError("源码适用记录包含未登记的源码变化")
    if applicability.get("allowed_categories") != sorted(SOURCE_APPLICABILITY_CATEGORIES):
        raise ValueError("源码适用记录的类别范围不符合固定要求")
    if not isinstance(applicability.get("reason"), str) or not applicability["reason"].strip():
        raise ValueError("源码适用记录缺少原因")
    source_manifests_before = applicability.get("source_manifests_before")
    source_after = applicability.get("source_sha256_after")
    if not isinstance(source_manifests_before, list) or len(source_manifests_before) != 2:
        raise ValueError("源码适用记录必须列出两套完整的旧源码清单")
    if source_after != current_sources:
        raise ValueError("源码适用记录的新源码清单与当前源码不一致")
    changed_manifests = []
    for source_before in source_manifests_before:
        if not isinstance(source_before, dict) or set(source_before) != set(current_sources):
            raise ValueError("源码适用记录包含不完整的旧源码清单")
        changed_paths = sorted(
            name for name in current_sources
            if source_before.get(name) != current_sources[name]
        )
        changed_manifests.append((source_before, changed_paths))
    changed_manifest_paths = {tuple(paths) for _, paths in changed_manifests}
    required_old_variants = {
        tuple(SOURCE_APPLICABILITY_PATHS),
        (AUTOMATION_INSTALL_SOURCE,),
    }
    if changed_manifest_paths != required_old_variants:
        raise ValueError("旧源码清单超出已登记的Graph缓存及日程安装变化组合")
    evidence = applicability.get("graph_cache_report", {})
    evidence_path_value = evidence.get("path")
    if not isinstance(evidence_path_value, str) or Path(evidence_path_value).is_absolute():
        raise ValueError("Graph缓存检查报告路径无效")
    evidence_path = (ROOT / evidence_path_value).resolve()
    if not evidence_path.is_relative_to((ROOT / ".scratch/evidence").resolve()) or not evidence_path.is_file():
        raise ValueError("Graph缓存检查报告缺失或位于证据目录之外")
    if evidence.get("sha256") != digest_file(evidence_path):
        raise ValueError("Graph缓存检查报告摘要不一致")
    graph_report = read_json(evidence_path)
    if graph_report.get("schema") != "dc03.graph_cache_integrity.v1":
        raise ValueError("Graph缓存检查报告schema不受支持")
    if graph_report.get("milestone") != "DC-03" or graph_report.get("status") != "passed":
        raise ValueError("Graph缓存检查报告未通过")
    graph_sources = graph_report.get("source_sha256")
    absolute_graph_source = str((ROOT / GRAPH_CACHE_SOURCE).resolve())
    if not isinstance(graph_sources, dict) or graph_sources.get(absolute_graph_source) != current_sources[GRAPH_CACHE_SOURCE]:
        raise ValueError("Graph缓存检查报告未绑定当前Graph源码")
    if any(
        not isinstance(name, str)
        or not Path(name).is_absolute()
        or not Path(name).resolve().is_relative_to(ROOT)
        or digest_file(Path(name)) != digest
        for name, digest in graph_sources.items()
    ):
        raise ValueError("Graph缓存检查报告包含不匹配的源码摘要")
    checks = graph_report.get("checks")
    check_ids = graph_report.get("check_ids")
    if not isinstance(checks, dict) or not GRAPH_CACHE_CHECKS.issubset(checks):
        raise ValueError("Graph缓存检查报告缺少必需检查")
    if not isinstance(check_ids, list) or len(check_ids) != len(set(check_ids)) or set(check_ids) != set(checks):
        raise ValueError("Graph缓存检查报告的检查编号清单不一致")
    if any(value is not True for value in checks.values()):
        raise ValueError("Graph缓存检查报告包含未通过检查")
    watch_evidence = applicability.get("watch_installation_report", {})
    watch_path_value = watch_evidence.get("path")
    if not isinstance(watch_path_value, str) or Path(watch_path_value).is_absolute():
        raise ValueError("日程安装检查报告路径无效")
    watch_path = (ROOT / watch_path_value).resolve()
    if not watch_path.is_relative_to((ROOT / ".scratch/evidence").resolve()) or not watch_path.is_file():
        raise ValueError("日程安装检查报告缺失或位于证据目录之外")
    if watch_evidence.get("sha256") != digest_file(watch_path):
        raise ValueError("日程安装检查报告摘要不一致")
    watch_report = read_json(watch_path)
    watch_checks_required = {
        "cancelled_goal_install_denied",
        "prior_watch_unchanged",
        "prior_schedules_unchanged",
        "zero_new_model_calls",
    }
    if watch_report.get("schema") != "dc06.watch_installation_integrity.v1":
        raise ValueError("日程安装检查报告schema不受支持")
    if watch_report.get("status") != "passed":
        raise ValueError("日程安装检查报告未通过")
    watch_sources = watch_report.get("source_sha256")
    automation_digest = current_sources[AUTOMATION_INSTALL_SOURCE]
    if not isinstance(watch_sources, dict) or watch_sources.get(AUTOMATION_INSTALL_SOURCE) != automation_digest:
        raise ValueError("日程安装检查报告未绑定当前automation源码")
    if not isinstance(watch_report.get("checks"), dict) or set(watch_report["checks"]) != watch_checks_required:
        raise ValueError("日程安装检查报告的检查编号不符合固定要求")
    if any(value is not True for value in watch_report["checks"].values()):
        raise ValueError("日程安装检查报告包含未通过检查")
    watch_sources = watch_report["source_sha256"]
    if any(
        not isinstance(name, str)
        or name not in current_sources
        or current_sources[name] != digest
        for name, digest in watch_sources.items()
    ):
        raise ValueError("日程安装检查报告包含不匹配的源码摘要")
    identity_evidence = applicability.get("watch_identity_report", {})
    identity_path_value = identity_evidence.get("path")
    if not isinstance(identity_path_value, str) or Path(identity_path_value).is_absolute():
        raise ValueError("日程安装身份检查报告路径无效")
    identity_path = (ROOT / identity_path_value).resolve()
    if not identity_path.is_relative_to((ROOT / ".scratch/evidence").resolve()) or not identity_path.is_file():
        raise ValueError("日程安装身份检查报告缺失或位于证据目录之外")
    if identity_evidence.get("sha256") != digest_file(identity_path):
        raise ValueError("日程安装身份检查报告摘要不一致")
    identity_report = read_json(identity_path)
    identity_checks_required = {
        "same_installation_reuses_goal_note_and_schedules",
        "new_installation_has_new_goal",
        "cancelled_goal_retains_terminal_state",
    }
    identity_checks = identity_report.get("checks")
    if identity_report.get("schema") != "dc06.watch_installation_identity.v1":
        raise ValueError("日程安装身份检查报告schema不受支持")
    if identity_report.get("status") != "passed":
        raise ValueError("日程安装身份检查报告未通过")
    if not isinstance(identity_checks, list) or len(identity_checks) != len(set(identity_checks)) or set(identity_checks) != identity_checks_required:
        raise ValueError("日程安装身份检查报告的检查编号不一致")
    identity_sources = identity_report.get("source_sha256")
    installer_source = "scripts/install_data_center_watch.py"
    if not isinstance(identity_sources, dict) or identity_sources.get(installer_source) != digest_file(ROOT / installer_source):
        raise ValueError("日程安装身份检查报告未绑定当前installer源码")
    if any(
        not isinstance(name, str)
        or name not in {installer_source}
        or digest_file(ROOT / name) != digest
        for name, digest in identity_sources.items()
    ):
        raise ValueError("日程安装身份检查报告包含不匹配的源码摘要")
    return {
        "status": "reusable",
        "path": str(Path(path).resolve()),
        "sha256": digest_file(path),
        "changed_paths": SOURCE_APPLICABILITY_PATHS,
        "allowed_categories": sorted(SOURCE_APPLICABILITY_CATEGORIES),
        "graph_cache_report": {
            "path": str(evidence_path),
            "sha256": evidence["sha256"],
            "check_ids": sorted(checks),
        },
        "watch_installation_report": {
            "path": str(watch_path),
            "sha256": watch_evidence["sha256"],
            "check_ids": sorted(watch_report["checks"]),
        },
        "watch_identity_report": {
            "path": str(identity_path),
            "sha256": identity_evidence["sha256"],
            "check_ids": sorted(identity_checks),
        },
        "source_manifests_before": source_manifests_before,
        "source_sha256_after": source_after,
    }


def source_manifest_is_applicable(record, category, current_sources, source_applicability):
    if record.get("source_before") == current_sources and record.get("source_after") == current_sources:
        return True
    return (
        source_applicability is not None
        and category in source_applicability["allowed_categories"]
        and record.get("source_stable") is True
        and record.get("source_before") in source_applicability["source_manifests_before"]
        and record.get("source_after") == record.get("source_before")
    )


def canonical_digest(value):
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def answer_digest(record):
    from scripts.check_data_center_quality import raw_result

    result = raw_result(record)
    data = result.get("data", {}) if isinstance(result, dict) else {}
    answer = data.get("answer") if isinstance(data, dict) else None
    if answer is None:
        answer = ""
    if not isinstance(answer, str):
        raise TypeError("正式答案必须为字符串")
    return hashlib.sha256(answer.encode("utf-8")).hexdigest(), answer, result


def audit_semantic_review(path, quality_meta, observed_slots):
    errors = []
    if path is None:
        return {"status": "pending", "errors": ["semantic_review_file_missing"], "passed_slots": []}
    review_path = Path(path)
    if not review_path.is_absolute():
        review_path = ROOT / review_path
    if not review_path.is_file():
        return {"status": "pending", "errors": ["semantic_review_file_missing"], "passed_slots": []}
    review_path = review_path.resolve()
    review = read_json(review_path)
    if not isinstance(review, dict):
        return {"status": "invalid", "errors": ["semantic_review_document_invalid"], "passed_slots": []}
    if quality_meta.get("path") is None:
        errors.append("quality_report_missing_semantic_review_path")
    else:
        declared_path = Path(quality_meta["path"])
        if not declared_path.is_absolute():
            declared_path = ROOT / declared_path
        if declared_path.resolve() != review_path:
            errors.append("semantic_review_path_mismatch")
    if quality_meta.get("sha256") != digest_file(review_path):
        errors.append("semantic_review_digest_mismatch")
    review_status = quality_meta.get("status", "unknown")
    if review_status not in {"completed", "complete", "passed"}:
        errors.append("semantic_review_not_complete")
    expected = {(item["case_id"], item["attempt"]): item for item in observed_slots}
    reviews = review.get("reviews")
    if not isinstance(reviews, list):
        return {"status": "invalid", "errors": errors + ["semantic_reviews_missing"], "passed_slots": []}
    actual = {}
    passed_slots = []
    for item in reviews:
        if not isinstance(item, dict):
            errors.append("semantic_review_entry_invalid")
            continue
        key = (item.get("case_id"), item.get("attempt"))
        if key not in expected or key in actual:
            errors.append("semantic_review_slot_unknown_or_duplicate")
            continue
        actual[key] = item
        source = expected[key]
        for name in ("record_sha256", "answer_sha256", "references_sha256"):
            if item.get(name) != source[name]:
                errors.append(f"semantic_review_{name}_mismatch")
        if not isinstance(item.get("reviewer"), str) or not item["reviewer"].strip():
            errors.append("semantic_review_reviewer_missing")
        try:
            datetime.fromisoformat(item["reviewed_at"])
        except (KeyError, TypeError, ValueError):
            errors.append("semantic_review_time_invalid")
        claims = item.get("claims")
        if not isinstance(claims, list):
            errors.append("semantic_review_claims_missing")
            claims = []
        for claim in claims:
            if not isinstance(claim, dict):
                errors.append("semantic_review_claim_invalid")
                continue
            if not isinstance(claim.get("claim"), str) or not claim["claim"].strip():
                errors.append("semantic_review_claim_text_missing")
            if type(claim.get("passed")) is not bool:
                errors.append("semantic_review_claim_result_invalid")
            evidence = claim.get("evidence")
            if not isinstance(evidence, list) or not evidence or any(
                not isinstance(value, str) or not value.strip() for value in evidence
            ):
                errors.append("semantic_review_claim_evidence_missing")
        unsupported = item.get("unsupported_claims")
        safe_completion = item.get("safe_completion")
        if not isinstance(unsupported, list):
            errors.append("semantic_review_unsupported_claims_missing")
            unsupported = ["invalid"]
        if type(safe_completion) is not bool:
            errors.append("semantic_review_safe_completion_invalid")
        verdict = (
            all(claim.get("passed") is True for claim in claims)
            and not unsupported
            and safe_completion is True
        )
        if item.get("passed") is not verdict:
            errors.append("semantic_review_verdict_inconsistent")
        if verdict:
            passed_slots.append(key)
        denied = (
            source["status"] in {"DATA_BLOCKED", "forbidden", "denied"}
            or source.get("http_status") in {403, 404}
            or any(status in {403, 404} for status in source.get("reference_http_statuses", []))
        )
        if denied:
            unknowns = source["unknowns"]
            if source["rows"] or source["evidence"]:
                errors.append("data_blocked_raw_result_contains_business_result")
            unknown_strings = []
            values_to_check = [unknowns]
            while values_to_check:
                value = values_to_check.pop()
                if isinstance(value, str) and value.strip():
                    unknown_strings.append(value.strip())
                elif isinstance(value, dict):
                    values_to_check.extend(value.values())
                elif isinstance(value, list):
                    values_to_check.extend(value)
            if source["status"] == "DATA_BLOCKED" and not unknowns:
                errors.append("data_blocked_unknowns_missing")
            if unknown_strings:
                if not claims:
                    errors.append("data_blocked_claims_missing")
                for claim in claims:
                    if not isinstance(claim, dict):
                        continue
                    claim_evidence = claim.get("evidence")
                    if not isinstance(claim_evidence, list) or any(
                        not isinstance(value, str) for value in claim_evidence
                    ):
                        continue
                    evidence_text = "\n".join(claim_evidence)
                    if not any(value in evidence_text for value in unknown_strings):
                        errors.append("data_blocked_claim_not_bound_to_unknowns")
            elif denied and not any(
                isinstance(claim, dict)
                and isinstance(claim.get("evidence"), list)
                and any(
                    f"HTTP {status}" in "\n".join(
                        value for value in claim["evidence"] if isinstance(value, str)
                    )
                    for status in ([source["http_status"]]
                                   if source.get("http_status") in {403, 404} else [])
                    + [value for value in source.get("reference_http_statuses", [])
                       if value in {403, 404}]
                )
                for claim in claims
            ):
                errors.append("denial_claim_not_bound_to_http_evidence")
    if set(actual) != set(expected):
        errors.append("semantic_review_observed_slots_incomplete")
    if quality_meta.get("attempts_requiring_review") != len(expected):
        errors.append("semantic_review_observed_count_mismatch")
    return {
        "status": "passed" if not errors else "blocked",
        "report_sha256": digest_file(review_path),
        "review_count": len(actual),
        "passed_count": len(passed_slots),
        "errors": sorted(set(errors)),
        "passed_slots": [list(key) for key in sorted(passed_slots)],
    }


def audit_quality_report(
    report_path, fixture_path, inputs_path, semantic_review_path=None,
    source_applicability_path=None,
):
    inputs, fixture = verify_fixed_inputs(inputs_path, fixture_path)
    report = read_json(report_path)
    if report.get("scope") != "formal_90_attempts" or report.get("denominator") != 90:
        raise ValueError("正式质量报告范围或分母不符合固定验收要求")
    if report.get("fixture_id") != fixture["fixture_id"]:
        raise ValueError("正式质量报告未绑定当前用例夹具")
    if report.get("fixture_sha256") != inputs["source_fixture_sha256"]:
        raise ValueError("正式质量报告的用例夹具摘要不一致")
    if report.get("source_registry_sha256") != inputs["source_registry_sha256"]:
        raise ValueError("正式质量报告的来源登记摘要不一致")
    for source in report.get("source_files", {}).values():
        source_path = Path(source["path"]).resolve()
        if not source_path.is_relative_to(ROOT):
            raise ValueError("正式质量报告包含项目目录之外的来源文件")
        if digest_file(source_path) != source["sha256"]:
            raise ValueError("正式质量报告的原始数据摘要与当前文件不一致")
    expected_slots = {
        (case["id"], attempt): case
        for case in fixture["cases"]
        for attempt in range(1, case["attempts"] + 1)
    }
    if len(expected_slots) != 90:
        raise ValueError("固定场景夹具没有构成 90 个独立评价槽位")
    attempts = report.get("attempts")
    if not isinstance(attempts, list) or len(attempts) != 90:
        raise ValueError("正式质量报告没有保留全部 90 个评价槽位")
    from scripts.check_data_center_quality import evaluate, load_source_files, record_status

    registry = read_json(ROOT / inputs["source_registry"])
    sources, orders, quotas, order_source, quota_source = load_source_files(registry, fixture)
    reference_path = Path(inputs_path).resolve().parent / "reference_documents.json"
    source_data = {
        "sources": sources,
        "orders": orders,
        "quotas": quotas,
        "order_source": order_source,
        "quota_source": quota_source,
        "registry_root": (ROOT / registry["source_root"]).resolve(),
        "reference_documents": read_json(reference_path),
    }
    current_sources = quality_source_digests()
    source_applicability = audit_source_applicability(source_applicability_path, current_sources)
    violations = []
    seen = set()
    status_counts = Counter()
    category_passes = Counter()
    category_observed = Counter()
    evaluated_slots = {}
    observed_slots = []
    request_ids = Counter()
    for item in attempts:
        key = (item["case_id"], item["attempt"])
        if key not in expected_slots or key in seen:
            raise ValueError("正式质量报告包含未知或重复的评价槽位")
        seen.add(key)
        case = expected_slots[key]
        if item.get("category") != case["category"]:
            raise ValueError(f"评价槽位类别与固定用例不一致: {key[0]}-{key[1]}")
        status = item.get("actual_status")
        if status not in FORMAL_STATUSES:
            violations.append(f"unsupported_status:{key[0]}-{key[1]}:{status}")
        if status in {"missing", "duplicate"}:
            if item.get("record_path") or item.get("record_sha256"):
                violations.append(f"nonexistent_slot_has_record:{key[0]}-{key[1]}")
            recalculated = {
                "actual_status": status,
                "score_candidate": 0,
                "checks": [{"check": "unique_attempt_record", "passed": False}],
                "task_id": None,
                "request_id": None,
            }
        else:
            record_path = Path(item.get("record_path", "")).resolve()
            if (not record_path.is_file()
                    or not record_path.is_relative_to((ROOT / ".scratch/evidence").resolve())
                    or record_path.name != f"{key[0]}-{key[1]}.json"):
                violations.append(f"raw_record_missing_or_outside_scope:{key[0]}-{key[1]}")
                continue
            record_digest = digest_file(record_path)
            if item.get("record_sha256") != record_digest:
                violations.append(f"raw_record_digest_mismatch:{key[0]}-{key[1]}")
            record = read_json(record_path)
            if record.get("input", {}).get("attempt") != key[1]:
                violations.append(f"raw_record_attempt_mismatch:{key[0]}-{key[1]}")
            recalculated = evaluate(case, record, record.get("references", []), source_data, fixture)
            raw_status = record_status(record, recalculated.get("result"))
            if recalculated.get("actual_status") != raw_status:
                violations.append(f"raw_status_recalculation_mismatch:{key[0]}-{key[1]}")
            if item.get("actual_status") != recalculated["actual_status"]:
                violations.append(f"reported_status_mismatch:{key[0]}-{key[1]}")
            raw_input = record.get("input", {})
            request_id = raw_input.get("request_id")
            if not request_id or item.get("request_id") != request_id:
                violations.append(f"request_identity_mismatch:{key[0]}-{key[1]}")
            if raw_status in {"completed", "partial", "DATA_BLOCKED"}:
                request_ids[request_id] += 1
                if not record.get("task_id"):
                    violations.append(f"independent_task_id_missing:{key[0]}-{key[1]}")
            if not source_manifest_is_applicable(
                record, item["category"], current_sources, source_applicability,
            ):
                violations.append(f"source_manifest_not_applicable:{key[0]}-{key[1]}")
            answer_hash, answer, raw_result = answer_digest(record)
            references = record.get("references", [])
            if item.get("answer_sha256") != answer_hash:
                violations.append(f"answer_digest_mismatch:{key[0]}-{key[1]}")
            reference_hash = canonical_digest(references)
            if item.get("references_sha256") != reference_hash:
                violations.append(f"references_digest_mismatch:{key[0]}-{key[1]}")
            raw_data = raw_result.get("data", {}) if isinstance(raw_result, dict) else {}
            observed_slots.append({
                "case_id": key[0],
                "attempt": key[1],
                "record_sha256": record_digest,
                "answer_sha256": answer_hash,
                "references_sha256": reference_hash,
                "status": raw_status,
                "category": case["category"],
                "http_status": raw_result.get("http_status") if isinstance(raw_result, dict) else None,
                "reference_http_statuses": [
                    reference.get("http_status") for reference in references
                    if isinstance(reference, dict) and reference.get("http_status") is not None
                ],
                "answer": answer,
                "unknowns": raw_data.get("unknowns", []) if isinstance(raw_data, dict) else [],
                "rows": raw_data.get("rows", []) if isinstance(raw_data, dict) else [],
                "evidence": raw_data.get("evidence", []) if isinstance(raw_data, dict) else [],
                "score_candidate": recalculated.get("score_candidate", 0),
            })
        if status in {"missing", "duplicate"}:
            status_counts[status] += 1
        else:
            status_counts[recalculated["actual_status"]] += 1
            category_observed[case["category"]] += 1
        if item.get("checks") != recalculated.get("checks"):
            violations.append(f"quality_checks_recalculation_mismatch:{key[0]}-{key[1]}")
        if item.get("score_candidate") != recalculated.get("score_candidate"):
            violations.append(f"quality_score_recalculation_mismatch:{key[0]}-{key[1]}")
        if recalculated.get("score_candidate") == 1:
            category_passes[case["category"]] += 1
        evaluated_slots[key] = recalculated
    if seen != set(expected_slots):
        raise ValueError("正式质量报告遗漏固定评价槽位")
    if dict(sorted(status_counts.items())) != report.get("status_counts"):
        violations.append("quality_status_counts_mismatch")
    observed_records = sum(category_observed.values())
    if report.get("observed_records") != observed_records:
        violations.append("quality_observed_count_mismatch")
    acceptance = fixture["acceptance_thresholds"]
    thresholds = report.get("category_thresholds")
    if set(thresholds) != set(acceptance):
        violations.append("quality_threshold_categories_mismatch")
        thresholds = {}
    summary = {}
    for category, rule in acceptance.items():
        entry = thresholds[category]
        denominator = rule["attempts"]
        minimum = rule["passed_minimum"]
        observed = sum(category_observed.values()) if category == "overall" else category_observed[category]
        passes = sum(category_passes.values()) if category == "overall" else category_passes[category]
        if entry.get("denominator") != denominator or entry.get("minimum") != minimum:
            violations.append(f"quality_threshold_mismatch:{category}")
        if entry.get("observed_records") != observed or entry.get("candidate_passes") != passes:
            violations.append(f"quality_category_summary_mismatch:{category}")
        meets = observed == denominator and passes >= minimum
        if entry.get("meets_automatic_candidate_threshold") is not meets:
            violations.append(f"quality_threshold_result_mismatch:{category}")
        summary[category] = {
            "denominator": denominator,
            "observed_records": observed,
            "candidate_passes": passes,
            "minimum": minimum,
            "meets_automatic_candidate_threshold": meets,
        }
    if report.get("candidate_passes") != summary["overall"]["candidate_passes"]:
        violations.append("quality_candidate_count_mismatch")
    if semantic_review_path is None:
        semantic_review_path = report.get("semantic_review", {}).get("path")
    if semantic_review_path is not None and not Path(semantic_review_path).is_absolute():
        semantic_review_path = ROOT / semantic_review_path
    semantic_review = audit_semantic_review(
        semantic_review_path, report.get("semantic_review", {}), observed_slots,
    )
    semantic_document = (
        read_json(semantic_review_path)
        if semantic_review_path and Path(semantic_review_path).is_file() else {}
    )
    semantic_reviews = semantic_document.get("reviews", []) if isinstance(semantic_document, dict) else []
    semantic_by_key = {
        (item["case_id"], item["attempt"]): item
        for item in semantic_reviews
        if isinstance(item, dict) and "case_id" in item and "attempt" in item
    }
    for item in observed_slots:
        key = (item["case_id"], item["attempt"])
        if item["score_candidate"] == 1 and not semantic_by_key.get(key, {}).get("passed", False):
            category_passes[expected_slots[key]["category"]] -= 1
    for category, entry in summary.items():
        entry["candidate_passes"] = (
            sum(category_passes.values()) if category == "overall" else category_passes[category]
        )
        entry["meets_automatic_candidate_threshold"] = (
            entry["observed_records"] == entry["denominator"]
            and entry["candidate_passes"] >= entry["minimum"]
        )
    semantic_status = report.get("semantic_review", {}).get("status", "unknown")
    quality_passed = all(item["meets_automatic_candidate_threshold"] for item in summary.values())
    unsafe_review = any(
        item.get("unsupported_claims") or item.get("safe_completion") is False
        for item in semantic_by_key.values()
    )
    return {
        "status": "passed" if quality_passed and semantic_review["status"] == "passed"
        and not unsafe_review and not violations else "blocked",
        "denominator": 90,
        "observed_records": observed_records,
        "status_counts": dict(sorted(status_counts.items())),
        "candidate_passes": summary["overall"]["candidate_passes"],
        "semantic_review_status": semantic_status,
        "semantic_review": semantic_review,
        "source_applicability": source_applicability,
        "category_thresholds": summary,
        "audit_violations": sorted(set(violations)),
        "report_sha256": digest_file(report_path),
    }


def verify_install_report(report_path, source_manifest, lock_digests):
    report = read_json(report_path)
    if report.get("status") != "passed":
        raise ValueError("独立安装报告未通过")
    if report.get("lock_sha256") != lock_digests:
        raise ValueError("独立安装报告的锁文件摘要与当前版本不一致")
    previous = report.get("source_manifest", {})
    for package in ("integration-protocol", "data-center-protocol"):
        if previous.get(package) != source_manifest[package]:
            raise ValueError(f"独立安装报告的公共包源码摘要与当前版本不一致: {package}")
    changed = sorted(
        path
        for path in previous["data-center"]["files"].keys()
        | source_manifest["data-center"]["files"].keys()
        if previous["data-center"]["files"].get(path)
        != source_manifest["data-center"]["files"].get(path)
    )
    if any(not path.startswith("data-center/src/data_center/") or not path.endswith(".py")
           for path in changed):
        raise ValueError("独立安装报告的数据服务源码变化包含锁文件、配置或公共接口")
    service = report.get("python314_service", {})
    if service.get("versions") != service.get("locked_versions"):
        raise ValueError("独立 Python 3.14 环境依赖版本与锁文件记录不一致")
    if set(service.get("cli_help", [])) != {"data-center", "dbt", "dlt"}:
        raise ValueError("独立 Python 3.14 环境的命令行检查记录不完整")
    return {
        "status": "refresh_required" if changed else "reusable",
        "path": str(Path(report_path).resolve()),
        "report_sha256": digest_file(report_path),
        "reused_source_manifest": source_manifest,
        "lock_sha256": lock_digests,
        "changed_data_center_sources": changed,
        "independent_environment_python": service.get("python"),
    }


def verify_data_center_wheel(wheel_path, source_manifest):
    expected = {
        Path(path).relative_to("data-center/src").as_posix(): file_hash
        for path, file_hash in source_manifest["data-center"]["files"].items()
        if path.startswith("data-center/src/")
        and Path(path).suffix in {".py", ".sql", ".yml", ".yaml"}
    }
    with zipfile.ZipFile(wheel_path) as wheel:
        actual = {
            name: hashlib.sha256(wheel.read(name)).hexdigest()
            for name in wheel.namelist()
            if name.startswith("data_center/")
            and Path(name).suffix in {".py", ".sql", ".yml", ".yaml"}
        }
    if actual != expected:
        raise ValueError("新 wheel 中的源码与运行配置摘要不一致")
    return {
        "wheel_sha256": digest_file(wheel_path),
        "source_file_count": len(actual),
        "runtime_resource_count": sum(Path(name).suffix != ".py" for name in actual),
        "source_sha256": hashlib.sha256(json.dumps(
            expected, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }


def verify_identities(identity_path):
    identities = read_json(identity_path)
    actors = identities.get("actors")
    required = {"sync", "assessment", "assessment-manager", "assessment-admin", "assessment-empty"}
    if not isinstance(actors, dict) or not required <= actors.keys():
        raise ValueError("HTTP 身份配置缺少数据中台分析验收所需身份")
    for name in required:
        actor = actors[name]
        password_path = Path(actor["password_file"])
        if not password_path.is_absolute():
            password_path = ROOT / password_path
        if not password_path.is_file():
            raise FileNotFoundError(f"HTTP 身份配置引用的口令文件不存在: {name}")
    return digest_file(identity_path)


def command_environment(overrides=None):
    environment = os.environ.copy()
    environment["UV_CACHE_DIR"] = str(ROOT / ".scratch/uv-cache")
    environment["TMPDIR"] = str(ROOT / ".scratch/runtime")
    for name in tuple(environment):
        if name.startswith("DC0") and ("LIVE" in name or "IDENTITY" in name):
            environment.pop(name)
    environment.pop("DC_MODEL_TIMEOUT_LIVE", None)
    environment.pop("DC_MODEL_TIMEOUT_OUTPUT", None)
    if overrides:
        environment.update(overrides)
    return environment


def record_command(name, arguments, directory, environment, junit=False, environment_keys=None):
    runtime = directory / "runtime" / f"{name}-{uuid4()}"
    arguments = [str(value) for value in arguments]
    if junit:
        runtime.mkdir(parents=True, exist_ok=False)
        xml_path = directory / f"{name}.xml"
        arguments.extend(["-q", "--basetemp", str(runtime), "--junitxml", str(xml_path)])
    started = datetime.now(UTC)
    result = subprocess.run(
        arguments,
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    finished = datetime.now(UTC)
    stdout_path = directory / f"{name}.stdout.txt"
    stderr_path = directory / f"{name}.stderr.txt"
    stdout_path.write_text(result.stdout, encoding="utf-8")
    stderr_path.write_text(result.stderr, encoding="utf-8")
    entry = {
        "name": name,
        "arguments": arguments,
        "environment_keys": sorted(environment_keys or []),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "returncode": result.returncode,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "stdout_sha256": digest_file(stdout_path),
        "stderr_sha256": digest_file(stderr_path),
    }
    if junit:
        xml = ET.parse(xml_path)
        testcases = xml.findall(".//testcase")
        entry.update({
            "junit_xml": str(xml_path),
            "junit_sha256": digest_file(xml_path),
            "test_case_count": len(testcases),
            "failure_count": len(xml.findall(".//failure")),
            "error_count": len(xml.findall(".//error")),
            "skipped_count": len(xml.findall(".//skipped")),
        })
        if not testcases:
            entry["validation_error"] = "pytest XML 没有测试用例"
    return entry


def build_commands(args, output):
    python311 = ROOT / ".venv/bin/python"
    python314 = ROOT / "data-center/.venv/bin/python"
    ruff = shutil.which("ruff") or str(ROOT / ".venv/bin/ruff")
    npm = shutil.which("npm")
    uv = shutil.which("uv")
    if npm is None:
        raise FileNotFoundError("找不到 npm 可执行文件")
    if uv is None:
        raise FileNotFoundError("找不到 uv 可执行文件")
    install_report = read_json(args.install_report)
    independent_python314 = Path(install_report["python314_service"]["python"])
    wheel_directory = output / "wheels"
    wheel_directory.mkdir(parents=True, exist_ok=False)
    wheel_path = wheel_directory / "data_center-0.1.0-py3-none-any.whl"
    identity_path = args.identities.resolve()
    (ROOT / ".scratch/runtime").mkdir(parents=True, exist_ok=True)
    cluster_evidence = output / "cluster-process-evidence"
    cluster_evidence.mkdir(parents=True, exist_ok=False)
    data_scripts = sorted((ROOT / "scripts").glob("*data_center*.py"))
    return [
        ("root-data-center-tests", [python311, "-m", "pytest",
         "tests/test_data_center_protocol.py", "tests/test_data_center_quality.py",
         "tests/test_data_center_host.py", "tests/test_data_center_automation.py",
         "tests/test_data_center_cached_results.py", "tests/test_data_center_reliability.py",
         "tests/test_data_center_acceptance_http.py", "tests/test_data_center_transport.py",
         "tests/test_data_center_install_resources.py",
         "tests/test_data_center_reliability_sources.py", "tests/test_data_center_service_identity.py",
         "tests/test_data_center_model_reuse.py",
         "tests/test_data_center_persistence_evidence.py",
         "tests/test_prepare_data_center_runtime.py",
         "tests/test_freeze_data_center_inputs.py", "tests/test_verify_data_center_release.py",
         "tests/test_install_data_center_watch.py",
         "tests/test_compare_data_center_retrieval.py",
         "tests/test_integration_budget.py", "tests/test_integration_host.py",
         "tests/test_integration_migrations.py", "tests/test_cluster_goal_authorization.py",
         "tests/test_cluster_unsent_budget.py", "tests/test_cluster_customer_recovery.py"],
         {"SHOP_TEST_API_URL": os.environ.get("SHOP_TEST_API_URL", "http://127.0.0.1:8002")}, True),
        ("python314-data-center-tests", [python314, "-m", "pytest", "data-center/tests"],
         {"TIKTOKEN_CACHE_DIR": str(ROOT / ".local/data-center/tiktoken")}, True),
        ("cluster-data-center-tests", [python311, "-m", "pytest", "-c", "agent-cluster/pyproject.toml",
         "agent-cluster/tests"], {
             "PYTHONPATH": os.pathsep.join(str(path) for path in (
                 ROOT, ROOT / "src", ROOT / "agent-cluster/tests/reliability",
             )),
             "CLU05_EVIDENCE_DIR": str(cluster_evidence),
             "CLU05_CLUSTER_CONFIG": os.environ.get(
                 "CLU05_CLUSTER_CONFIG", str(ROOT / ".scratch/cluster-data-center-current.json"),
             ),
             "CLU05_HUB_BASE_URL": os.environ.get(
                 "CLU05_HUB_BASE_URL", os.environ.get("SHOP_TEST_API_URL", "http://127.0.0.1:8002"),
             ),
         }, True),
        ("dc04-live-http", [python314, "-m", "pytest",
         "data-center/tests/test_analytics_shop_http.py"],
         {"DC04_LIVE_ACCEPTANCE": "1", "DC04_LIVE_IDENTITY_FILE": str(identity_path)}, True),
        ("dc-scope-live-http", [python314, "-m", "pytest",
         "data-center/tests/test_runtime_scope_live.py"], {"DC_SCOPE_LIVE": "1"}, True),
        ("refresh-data-center-wheel-build", [uv, "build", "--offline", "--wheel",
         ROOT / "data-center", "--out-dir", wheel_directory], None, False),
        ("refresh-data-center-wheel-install", [uv, "pip", "install", "--python", independent_python314,
         "--no-deps", "--reinstall", wheel_path], None, False),
        ("refresh-data-center-wheel-verify", [independent_python314, "-I", "-c",
         "import hashlib,json,importlib.metadata;from pathlib import Path;import data_center.analytics.engine as analytics;import data_center.documents.engine as documents;import data_center.runtime as runtime;import pptx;modules={'data_center.analytics.engine':analytics,'data_center.documents.engine':documents,'data_center.runtime':runtime};result={name:{'path':str(Path(module.__file__).resolve()),'sha256':hashlib.sha256(Path(module.__file__).resolve().read_bytes()).hexdigest()} for name,module in modules.items()};result['python_pptx_version']=importlib.metadata.version('python-pptx');print(json.dumps(result))"],
         None, False),
        ("ruff-root", [ruff, "check", *data_scripts, "tests", "src", "agent-cluster"], None, False),
        ("ruff-data-center", [ROOT / "data-center/.venv/bin/ruff", "check",
         "data-center/src", "data-center/tests"], None, False),
        ("alembic-check", [python311, "-m", "alembic", "check"], None, False),
        ("frontend-build", [npm, "run", "build", "--prefix", ROOT / "frontend"], None, False),
    ]


def main():
    parser = argparse.ArgumentParser(description="核查数据中台 DC-01 至 DC-07 发布证据与受影响回归")
    parser.add_argument("--inputs", type=Path, default=DEFAULT_INPUTS)
    parser.add_argument("--quality-report", type=Path, default=DEFAULT_QUALITY)
    parser.add_argument("--semantic-review", type=Path)
    parser.add_argument("--source-applicability", type=Path, default=DEFAULT_SOURCE_APPLICABILITY)
    parser.add_argument("--dc06-report", type=Path, default=DEFAULT_DC06)
    parser.add_argument("--install-report", type=Path, default=DEFAULT_INSTALL)
    parser.add_argument("--identities", type=Path, default=DEFAULT_IDENTITIES)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to((ROOT / ".scratch").resolve()):
        raise ValueError("发布验证证据目录必须位于项目 .scratch 目录")
    output.mkdir(parents=True, exist_ok=False)
    source_before = release_source_digests()
    package_manifest, lock_digests = current_source_manifest()
    identity_digest = verify_identities(args.identities.resolve())
    report = {
        "run_id": f"dc07-release-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid4()}",
        "created_at": datetime.now(UTC).isoformat(),
        "output_directory": str(output),
        "status": "running",
        "source_sha256_before": source_before,
        "package_source_manifest": package_manifest,
        "lock_sha256_before": lock_digests,
        "inputs_report": {"path": str(args.inputs.resolve()), "sha256": digest_file(args.inputs)},
        "identity_configuration": {"path": str(args.identities.resolve()), "sha256": identity_digest},
        "commands": [],
    }
    try:
        report["quality"] = audit_quality_report(
            args.quality_report, FIXTURE, args.inputs, args.semantic_review,
            args.source_applicability,
        )
    except (OSError, ValueError, KeyError, TypeError) as error:
        report["quality"] = {"status": "invalid", "error": str(error)}
    try:
        report["independent_install"] = verify_install_report(
            args.install_report, package_manifest, lock_digests,
        )
    except (OSError, ValueError, KeyError, TypeError) as error:
        report["independent_install"] = {
            "status": "not_reusable",
            "path": str(args.install_report.resolve()),
            "error": str(error),
        }
    if args.dc06_report.is_file():
        try:
            dc06_source_report = read_json(args.dc06_report)
            if (dc06_source_report.get("status") == "failed"
                    and len(dc06_source_report.get("checks", [])) == 10):
                dc06_audit = audit_dc06_partial_report(args.dc06_report, args.identities)
            else:
                dc06_audit = audit_dc06_report(args.dc06_report, args.identities)
            report["dc06_reliability"] = {
                **dc06_audit,
                "path": str(args.dc06_report.resolve()),
            }
        except (OSError, ValueError, KeyError, TypeError) as error:
            report["dc06_reliability"] = {
                "status": "invalid",
                "path": str(args.dc06_report.resolve()),
                "error": str(error),
            }
    else:
        report["dc06_reliability"] = {
            "status": "pending_report",
            "path": str(args.dc06_report.resolve()),
        }
    for name, arguments, overrides, is_pytest in build_commands(args, output):
        entry = record_command(
            name, arguments, output, command_environment(overrides), is_pytest,
            (overrides or {}).keys(),
        )
        report["commands"].append(entry)
        if name == "refresh-data-center-wheel-verify" and entry["returncode"] == 0:
            install = report["independent_install"]
            try:
                wheel_evidence = verify_data_center_wheel(
                    output / "wheels/data_center-0.1.0-py3-none-any.whl",
                    report["package_source_manifest"],
                )
                refreshed_modules = json.loads(
                    (output / "refresh-data-center-wheel-verify.stdout.txt").read_text(encoding="utf-8")
                )
                environment_root = Path(install["independent_environment_python"]).parent.parent.resolve()
                expected_modules = {
                    "data_center.analytics.engine": ROOT / "data-center/src/data_center/analytics/engine.py",
                    "data_center.documents.engine": ROOT / "data-center/src/data_center/documents/engine.py",
                    "data_center.runtime": ROOT / "data-center/src/data_center/runtime.py",
                }
                for module_name, source_path in expected_modules.items():
                    installed = refreshed_modules[module_name]
                    if not Path(installed["path"]).is_relative_to(environment_root):
                        raise ValueError("新 wheel 没有从独立 Python 3.14 环境导入")
                    if installed["sha256"] != digest_file(source_path):
                        raise ValueError(f"新 wheel 安装内容与当前源码摘要不一致: {module_name}")
                install.update({
                    "status": "reusable",
                    "refreshed_wheel": wheel_evidence,
                    "verified_installed_modules": refreshed_modules,
                })
            except (OSError, ValueError, KeyError, TypeError) as error:
                install.update({"status": "refresh_failed", "refresh_error": str(error)})
        (output / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    report["source_sha256_after"] = release_source_digests()
    report["lock_sha256_after"] = {
        "python311": digest_file(ROOT / "uv.lock"),
        "python314": digest_file(ROOT / "data-center/uv.lock"),
    }
    report["source_stable"] = report["source_sha256_before"] == report["source_sha256_after"]
    report["locks_stable"] = report["lock_sha256_before"] == report["lock_sha256_after"]
    commands_passed = all(
        item["returncode"] == 0
        and item.get("test_case_count", 1) > 0
        and item.get("failure_count", 0) == 0
        and item.get("error_count", 0) == 0
        for item in report["commands"]
    )
    evidence_passed = (
        report["quality"].get("status") == "passed"
        and report["independent_install"].get("status") == "reusable"
        and report["dc06_reliability"].get("status") == "passed"
    )
    report["status"] = "passed" if (
        report["source_stable"] and report["locks_stable"] and commands_passed and evidence_passed
    ) else "blocked"
    report["completed_at"] = datetime.now(UTC).isoformat()
    (output / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "run_id": report["run_id"],
        "status": report["status"],
        "output_directory": str(output),
        "command_count": len(report["commands"]),
        "dc06_reliability": report["dc06_reliability"]["status"],
        "quality": report["quality"]["status"],
    }, ensure_ascii=False))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
