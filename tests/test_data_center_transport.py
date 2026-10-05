import json
import os
from hashlib import sha256
from pathlib import Path

import pytest

from scripts.verify_data_center_transport import digest

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / ".scratch/evidence/dc05-transport-green-20261004-r1/report.json"
ATTEMPT = ROOT / ".scratch/evidence/dc-formal-20261004-r6/attempts/DOC-01-1.json"


def transport_report():
    report_path = Path(os.environ.get("DC_TRANSPORT_REPORT", DEFAULT_REPORT))
    if not report_path.exists():
        pytest.skip("需要先运行真实数据中台传输检查并保存报告")
    return json.loads(report_path.read_text())


def test_transport_observations_match_the_fixed_real_task():
    report = transport_report()
    attempt = json.loads(ATTEMPT.read_text())

    assert report["task_id"] == attempt["task_id"]
    assert report["run_id"] == attempt["result"]["run_id"]
    assert report["result_sha256"] == digest(attempt["result"])
    assert len(report["observations"]) == 4
    assert all(item["status"] == "completed" for item in report["observations"])
    assert all(item["task_id"] == attempt["task_id"] for item in report["observations"])
    assert all(item["run_id"] == attempt["result"]["run_id"] for item in report["observations"])
    assert all(item["result_sha256"] == digest(attempt["result"])
               for item in report["observations"])
    assert len(report["provider_requests_before"]) == 3
    assert report["provider_requests_before"] == report["provider_requests_after"]
    assert report["model_requests"] == 0
    assert report["status"] == "passed"
    assert report["failure"] is None


def test_transport_report_contains_only_connection_trace_events_and_stable_sources():
    report = transport_report()
    observed = report["transport_events"]

    assert observed
    assert all(event["event"].rsplit(".", 2)[-2] in {"connect_tcp", "close"}
               for event in observed)
    assert all(event["at"] for event in observed)
    assert all(event["address"] is None or set(event["address"]) == {"host", "port"}
               for event in observed)
    assert report["source_stable"] is True
    assert report["source_sha256_before"] == report["source_sha256_after"]
    assert report["attempt_sha256"] == sha256(ATTEMPT.read_bytes()).hexdigest()
    for relative_path, source_digest in report["source_sha256_after"].items():
        assert sha256((ROOT / relative_path).read_bytes()).hexdigest() == source_digest

    observations = {item["label"]: item["trace_events"] for item in report["observations"]}
    assert any(event["event"].endswith(".connect_tcp.started") for event in observations["initial"])
    assert not any(event["event"].endswith(".connect_tcp.started")
                   for event in observations["short_interval"])
    assert any(event["event"].endswith(".connect_tcp.started")
               for event in observations["after_2_2_seconds"])
    assert report["service_expiry_5_2_seconds_executed"] is True
    assert any(event["event"].endswith(".connect_tcp.started")
               for event in observations["after_5_2_seconds"])
