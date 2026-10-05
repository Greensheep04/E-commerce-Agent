import json
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest

from scripts.verify_data_center_release import (
    audit_source_applicability,
    quality_source_digests,
    source_manifest_is_applicable,
)

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / ".scratch/evidence/dc-persistence-applicability-20261004-r1/source-applicability.json"


def saved_evidence():
    if not REPORT.is_file():
        pytest.skip("需要当前真实Graph完整恢复及保存验收报告")
    return json.loads(REPORT.read_text())


def test_real_persistence_evidence_reuses_original_quality_records():
    saved_evidence()
    current = quality_source_digests()
    audit = audit_source_applicability(REPORT, current)
    assert audit["status"] == "reusable"
    for name, category in (("DOC-01-1", "document"), ("REL-01-1", "cross_document"),
                           ("MET-01-3", "analytics"), ("REF-01-1", "refusal")):
        path = ROOT / ".scratch/evidence/dc-formal-20261004-round-review-r1/attempts" / (name + ".json")
        record = json.loads(path.read_text())
        assert source_manifest_is_applicable(record, category, current, audit)
    failed = json.loads((ROOT / ".scratch/evidence/dc-formal-20261004-round-review-r1/attempts/MET-01-3.json").read_text())
    assert failed["result"]["status"] == "failed"


@pytest.mark.parametrize("change", ["unregistered_source", "category", "missing_receipt", "evidence_digest"])
def test_saved_persistence_evidence_rejects_incomplete_or_changed_proof(tmp_path, change):
    report = saved_evidence()
    if change == "unregistered_source":
        report["changed_paths"].append("src/shop/paid_models.py")
    elif change == "category":
        report["allowed_categories"].remove("refusal")
    elif change == "missing_receipt":
        report["source_sha256_after"].pop("data-center/src/data_center/graph/receipts.py")
    else:
        report["evidence"]["vector_proof"]["sha256"] = "0" * 64
    path = tmp_path / "source-applicability.json"
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError):
        audit_source_applicability(path, quality_source_digests())


def test_saved_persistence_evidence_does_not_allow_unregistered_record_changes():
    saved_evidence()
    current = quality_source_digests()
    audit = audit_source_applicability(REPORT, current)
    path = ROOT / ".scratch/evidence/dc-formal-20261004-round-review-r1/attempts/DOC-01-1.json"
    record = json.loads(path.read_text())
    record["source_before"]["src/shop/paid_models.py"] = "0" * 64
    record["source_after"] = dict(record["source_before"])
    assert not source_manifest_is_applicable(record, "document", current, audit)


@pytest.mark.parametrize("change", ["request_rejection", "cancelled_lookup", "readback_identity", "usage_cost"])
def test_real_http_evidence_rejects_changed_observations(tmp_path, change):
    report = saved_evidence()
    original = ROOT / report["evidence"]["http_reuse"]["path"]
    http = json.loads(original.read_text())
    if change == "request_rejection":
        http["checks"]["request_id_rejections"]["unknown_id_http_status"] = 200
    elif change == "cancelled_lookup":
        http["checks"]["cancelled_task_rejections"]["lookup_after_cancel_http_status"] = 200
    elif change == "readback_identity":
        http["checks"]["independent_process_readback"]["result"]["run_id"] = http["original"]["run_id"]
    else:
        http["checks"]["parallel_runs"]["usage"][0]["spent_cny"] = "0.01"
    evidence_dir = ROOT / ".scratch/evidence" / ("dc-persistence-negative-" + uuid4().hex)
    evidence_dir.mkdir()
    altered = evidence_dir / "http.json"
    altered.write_text(json.dumps(http))
    report["evidence"]["http_reuse"] = {
        "path": str(altered.relative_to(ROOT)), "sha256": sha256(altered.read_bytes()).hexdigest(),
    }
    path = tmp_path / "source-applicability.json"
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="请求身份|取消检查|并发记录|独立进程"):
        audit_source_applicability(path, quality_source_digests())


def test_protocol_junit_cannot_replace_real_migration_and_host_checks(tmp_path):
    report = saved_evidence()
    report["evidence"]["host_tests"] = dict(report["evidence"]["protocol_tests_314"])
    path = tmp_path / "source-applicability.json"
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="迁移、权限或协议"):
        audit_source_applicability(path, quality_source_digests())
