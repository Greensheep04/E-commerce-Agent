import json
from pathlib import Path

import pytest

import scripts.verify_data_center_release as release
from scripts.check_data_center_quality import record_answer_digest
from scripts.verify_data_center_release import (
    answer_digest,
    audit_dc06_partial_report,
    audit_dc06_report,
    audit_quality_report,
    audit_source_applicability,
    current_source_manifest,
    digest_file,
    quality_source_digests,
    release_source_digests,
    source_manifest_is_applicable,
    verify_install_report,
)

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / ".scratch/evidence"


def test_real_cancellation_count_summary_requires_phase_and_request_evidence():
    path = EVIDENCE / "dc06-reliability-20261004T011619Z-5abb75ca/cancellation.json"
    facts = json.loads(path.read_text(encoding="utf-8"))
    assert facts["state"] == "cancelled"
    assert facts["runs_before"] == facts["runs_after"] == 7
    assert facts["post_cancel_registry_active"] is True
    assert not release.dc06_cancellation_is_valid(facts)


def test_real_cancellation_observations_require_applicable_current_api_source():
    path = EVIDENCE / "dc06-cancellation-supplement-20261004-r1/cancellation.json"
    facts = json.loads(path.read_text(encoding="utf-8"))
    assert release.dc06_cancellation_is_valid(facts)
    assert facts["runs_before"] == facts["runs_after_schedule_wait"] == facts["runs_after_source_event"] == 7
    assert facts["source_active_after_event"] is True
    assert facts["provider_request_ids_before"] == facts["provider_request_ids_after"]
    report = release.audit_dc06_cancellation_supplement(
        path.with_name("report.json"), EVIDENCE / "dc02-host-20261004-r3/identities.json",
    )
    assert report["status"] == "rejected"
    assert "cancellation_source_manifest_not_applicable" in report["errors"]
    assert report["accepted_checks"] == []


def test_release_source_paths_include_all_sources_in_the_real_cancellation_report():
    path = EVIDENCE / "dc06-cancellation-supplement-20261004-r1/report.json"
    report = json.loads(path.read_text(encoding="utf-8"))
    producer_sources = set(report["source_manifest"]["before"])
    assert len(producer_sources) == 28
    assert producer_sources <= set(release.DC06_SOURCE_PATHS)
    assert "agent-cluster/src/agent_cluster/api.py" in release.DC06_SOURCE_PATHS
    assert "packages/data-center-protocol/src/data_center_protocol/__init__.py" in release.DC06_SOURCE_PATHS


@pytest.mark.parametrize("change", ["missing_run", "different_run", "duplicate_run", "new_request", "short_wait"])
def test_real_cancellation_evidence_rejects_inconsistent_observations(change):
    path = EVIDENCE / "dc06-cancellation-supplement-20261004-r1/cancellation.json"
    facts = json.loads(path.read_text(encoding="utf-8"))
    if change == "missing_run":
        facts["runs_after_source_event_snapshot"].pop()
    elif change == "different_run":
        facts["runs_after_source_event_snapshot"][0]["run_id"] = facts["provider_request_ids_before"][0]
    elif change == "duplicate_run":
        facts["runs_after_source_event_snapshot"][0] = facts["runs_after_source_event_snapshot"][1]
    elif change == "new_request":
        facts["provider_request_ids_after"].append(facts["runs_before_snapshot"][0]["run_id"])
    else:
        facts["schedule_wait_seconds"] = 5
    assert not release.dc06_cancellation_is_valid(facts)


def test_real_quality_report_retains_fixed_denominator_and_rejects_stale_evidence():
    result = audit_quality_report(
        EVIDENCE / "dc-quality-20261004-r3/quality-r4.json",
        ROOT / "tests/fixtures/data_center_cases.json",
        EVIDENCE / "dc01-inputs-20261004-r4/report.json",
    )

    assert result["denominator"] == 90
    assert result["observed_records"] == 71
    assert result["status_counts"] == {
        "completed": 27,
        "failed": 20,
        "missing": 19,
        "partial": 23,
        "unknown": 1,
    }
    assert result["semantic_review_status"] == "pending_lead_review"
    assert "raw_record_digest_mismatch:DOC-10-2" in result["audit_violations"]
    assert "source_manifest_not_applicable:DOC-01-1" in result["audit_violations"]
    assert result["status"] == "blocked"


def test_real_refusal_record_null_answer_matches_quality_checker_empty_text_digest():
    record_path = EVIDENCE / "dc-formal-20261004-r4/attempts/REF-01-1.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))

    answer_hash, answer, result = answer_digest(record)

    assert result["data"]["answer"] is None
    assert answer == ""
    assert answer_hash == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert answer_hash == record_answer_digest(record)


def test_answer_digest_rejects_non_string_non_null_answers():
    record = json.loads(
        (EVIDENCE / "dc-formal-20261004-r4/attempts/REF-01-1.json").read_text(encoding="utf-8")
    )
    record["result"]["data"]["answer"] = 7

    with pytest.raises(TypeError, match="必须为字符串"):
        answer_digest(record)


def test_real_install_report_is_not_reused_after_current_lock_changes():
    report_path = EVIDENCE / "dc01-independent-install-20261004-r1/run-r5/report.json"
    source_manifest, lock_digests = current_source_manifest()

    with pytest.raises(ValueError, match="锁文件摘要"):
        verify_install_report(report_path, source_manifest, lock_digests)


def test_earlier_install_report_is_rejected_after_lock_and_source_changes():
    report_path = EVIDENCE / "dc01-independent-install-20261004-r2/report.json"
    source_manifest, lock_digests = current_source_manifest()

    with pytest.raises(ValueError, match="锁文件摘要"):
        verify_install_report(report_path, source_manifest, lock_digests)


def test_real_failed_dc06_report_is_rejected_with_missing_checks_retained():
    report_path = EVIDENCE / "dc06-reliability-20261004-r6/report.json"
    identity_path = EVIDENCE / "dc02-host-20261004-r3/identities.json"

    result = audit_dc06_report(report_path, identity_path)

    assert result["status"] == "rejected"
    assert result["source_status"] == "failed"
    assert result["last_check"] == "three_real_apscheduler_rounds_and_unchanged_waits"
    assert "duplicate_source_event_deduplication" in result["missing_checks"]
    assert "permission_revoke_denies_queries_and_reference_reads" in result["missing_checks"]
    assert "budget_goal_rejects_before_provider_request" in result["missing_checks"]
    assert "budget_active_window_unchanged" in result["missing_checks"]
    assert "permission_revoke_facts_failed" in result["errors"]
    assert "budget_rejection_facts_failed" in result["errors"]
    assert "budget_active_window_facts_failed" in result["errors"]


def real_schedule_report():
    path = EVIDENCE / "dc06-reliability-20261004T011619Z-5abb75ca/report.json"
    report = json.loads(path.read_text())
    evidence = {item["path"]: item["sha256"] for item in report["evidence_files"]}
    return report, evidence


def test_real_schedule_audit_binds_events_baseline_and_timeline():
    assert callable(getattr(release, "audit_dc06_schedule", None))
    report, evidence = real_schedule_report()
    assert release.audit_dc06_schedule(report, evidence) == []


@pytest.mark.parametrize("change", ["missing_event", "different_event_run", "wrong_coalesced_periods"])
def test_schedule_audit_rejects_changed_real_event_records(tmp_path, change):
    assert callable(getattr(release, "audit_dc06_schedule", None))
    report, evidence = real_schedule_report()
    check = next(item for item in report["checks"]
                 if item["id"] == "three_real_apscheduler_rounds_and_unchanged_waits")
    old_path = next(path for path in check["evidence_files"] if Path(path).name == "schedule-events.json")
    events = json.loads((ROOT / old_path).read_text())
    if change == "missing_event":
        events.pop()
    elif change == "different_event_run":
        events[1]["run_id"] = events[0]["run_id"]
    else:
        events[1]["coalesced_periods"] = 0
    new_path = tmp_path / "schedule-events.json"
    new_path.write_text(json.dumps(events))
    relative = new_path.relative_to(ROOT).as_posix()
    check["evidence_files"] = [relative if path == old_path else path for path in check["evidence_files"]]
    evidence[relative] = digest_file(new_path)
    assert release.audit_dc06_schedule(report, evidence)


def test_schedule_audit_requires_manifest_bound_event_file():
    assert callable(getattr(release, "audit_dc06_schedule", None))
    report, evidence = real_schedule_report()
    events_path = next(path for path in evidence if Path(path).name == "schedule-events.json")
    evidence.pop(events_path)
    assert release.audit_dc06_schedule(report, evidence)


def test_real_r8_dc06_report_is_audited_as_partial_with_itemized_source_changes():
    report_path = EVIDENCE / "dc06-reliability-20261004-r8/report.json"
    identity_path = EVIDENCE / "dc02-host-20261004-r3/identities.json"

    result = audit_dc06_partial_report(report_path, identity_path)

    assert result["report_sha256"] == "f35943c5af43f93984da2cd98f8d388fd063334a6672b53ea544c27aa9507cff"
    assert result["status"] == "partial"
    assert result["source_status"] == "failed"
    assert len(result["source_differences"]) == 23
    assert all(
        item["state"] in {"unchanged", "changed_unverified"}
        and item["sha256_at_r8"]
        and item["sha256_current"]
        for item in result["source_differences"]
    )
    checks = {item["id"]: item for item in result["checks"]}
    assert checks["shared_shop_readonly_health"]["audit_status"] == "evidence_consistent_but_digest_unbound"
    assert checks["three_real_apscheduler_rounds_and_unchanged_waits"]["audit_status"] == "contradicted_or_incomplete"
    assert "schedule_round_evidence_contradicts_unchanged_waits" in checks[
        "three_real_apscheduler_rounds_and_unchanged_waits"
    ]["findings"]
    assert "cancellation_evidence_contradicts_no_new_runs" in checks[
        "cancel_stops_new_scheduled_and_source_runs"
    ]["findings"]
    assert "database_query_failure_did_not_return_rows" in result["proof_limitations"]
    assert "8101_access_log_has_no_request_id" in result["proof_limitations"]


def graph_source_applicability_document():
    record_path = EVIDENCE / "dc-formal-20261004-r4/attempts/DOC-01-1.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    relation_record_path = EVIDENCE / "dc-formal-20261004-r4/attempts/REF-01-1.json"
    relation_record = json.loads(relation_record_path.read_text(encoding="utf-8"))
    graph_report = EVIDENCE / "dc03-cache-mapping-20261004-r1/graph-cache-integrity-20261003T225427573175Z.json"
    watch_report = EVIDENCE / "dc-watch-cancel-20261004-r1/report.json"
    identity_report = EVIDENCE / "dc-watch-identity-20261004-r2/report.json"
    return {
        "schema": "dc07.source_applicability.v1",
        "changed_paths": [
            "data-center/src/data_center/graph/engine.py",
            "src/shop/data_center_automation.py",
        ],
        "allowed_categories": ["analytics", "document", "refusal"],
        "reason": "Graph retrieval cache validation and cancelled-goal watch installation have independent real checks.",
        "source_manifests_before": [record["source_before"], relation_record["source_before"]],
        "source_sha256_after": quality_source_digests(),
        "graph_cache_report": {
            "path": graph_report.relative_to(ROOT).as_posix(),
            "sha256": digest_file(graph_report),
        },
        "watch_installation_report": {
            "path": watch_report.relative_to(ROOT).as_posix(),
            "sha256": digest_file(watch_report),
        },
        "watch_identity_report": {
            "path": identity_report.relative_to(ROOT).as_posix(),
            "sha256": digest_file(identity_report),
        },
    }


def test_source_applicability_rejects_incomplete_runtime_config_identity(tmp_path):
    current_sources = quality_source_digests()
    assert audit_source_applicability(None, current_sources) is None
    document = graph_source_applicability_document()
    path = tmp_path / "source-applicability.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="不完整的旧源码清单"):
        audit_source_applicability(path, current_sources)

    document["changed_paths"].append("src/shop/app.py")
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="未登记"):
        audit_source_applicability(path, current_sources)


def test_graph_source_applicability_does_not_reuse_cross_document_records():
    current_sources = quality_source_digests()
    document = graph_source_applicability_document()
    old_sources = document["source_manifests_before"]
    document["source_sha256_after"] = current_sources

    doc_record = json.loads(
        (EVIDENCE / "dc-formal-20261004-r4/attempts/DOC-01-1.json").read_text()
    )
    relation_record = json.loads(
        (EVIDENCE / "dc-formal-20261004-r4/attempts/REF-01-1.json").read_text()
    )
    assert source_manifest_is_applicable(doc_record, "document", current_sources, document)
    assert not source_manifest_is_applicable(
        relation_record, "cross_document", current_sources, document,
    )
    assert doc_record["source_before"] in old_sources
    assert relation_record["source_before"] in old_sources


def test_release_source_manifest_excludes_local_data_and_evidence():
    source_manifest = release_source_digests()

    assert source_manifest
    assert all(
        ".scratch" not in Path(path).parts and ".local" not in Path(path).parts
        for path in source_manifest
    )
