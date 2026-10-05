import json
from hashlib import sha256
from pathlib import Path

import pytest

from scripts import check_data_center_quality
from scripts.check_data_center_quality import (
    ROOT,
    analytics_from_csv,
    evaluate,
    load_source_files,
    validate_fixture_metrics,
)


def test_fixed_metrics_are_recomputed_at_the_registered_decimal_precision():
    fixture = json.loads((ROOT / "tests/fixtures/data_center_cases.json").read_text())
    registry = json.loads((ROOT / ".local/data-center/sources/r3/sources.json").read_text())
    _, orders, quotas, _, _ = load_source_files(registry, fixture)
    cases = [case for case in fixture["cases"] if case["category"] == "analytics"]
    assert len(cases) == 8
    for case in cases:
        computed = analytics_from_csv(case, orders, quotas)
        checks = validate_fixture_metrics(case, computed)
        assert checks and all(item["passed"] for item in checks), (case["id"], checks)


def test_existing_ratio_observation_preserves_the_registered_display_values():
    path = Path(".scratch/evidence/dc-question-20261004-r3/result.json")
    result = json.loads(path.read_text())
    row = result["data"]["rows"][0]
    assert row["sales_amount"] == "1142965.428500"
    assert row["order_count"] == 42
    assert "27213.46" in result["data"]["answer"]
    assert "86.33%" in result["data"]["answer"]
    assert result["status"] == "partial" and result["data"]["unknowns"]


def test_real_metricflow_ratios_and_exact_amounts_are_checked_against_csv():
    fixture = json.loads((ROOT / "tests/fixtures/data_center_cases.json").read_text())
    registry = json.loads((ROOT / ".local/data-center/sources/r3/sources.json").read_text())
    sources, orders, quotas, order_source, quota_source = load_source_files(registry, fixture)
    record = json.loads(Path(
        ".scratch/evidence/dc-formal-20261004-r2/attempts/MET-01-1.json"
    ).read_text())
    case = next(case for case in fixture["cases"] if case["id"] == "MET-01")
    checked = evaluate(case, record, record["references"], {
        "sources": sources, "orders": orders, "quotas": quotas,
        "order_source": order_source, "quota_source": quota_source,
    }, fixture)
    assert checked["score_candidate"] == 1, checked["checks"]
    assert checked["actual_status"] == "partial" and checked["unknowns"]


def test_docling_references_bind_original_bytes_and_conversion_positions():
    fixture = json.loads((ROOT / "tests/fixtures/data_center_cases.json").read_text())
    registry_path = ROOT / ".local/data-center/sources/r3/sources.json"
    registry = json.loads(registry_path.read_text())
    sources, orders, quotas, order_source, quota_source = load_source_files(registry, fixture)
    record = json.loads((ROOT / (
        ".scratch/evidence/dc-formal-20261004-r3/attempts/DOC-01-1.json"
    )).read_text())
    case = next(case for case in fixture["cases"] if case["id"] == "DOC-01")
    checked = evaluate(case, record, record["references"], {
        "sources": sources, "orders": orders, "quotas": quotas,
        "order_source": order_source, "quota_source": quota_source,
        "registry_root": ROOT / registry["source_root"],
        "reference_documents": json.loads((ROOT / (
            ".scratch/evidence/dc01-inputs-20261004-r2/reference_documents.json"
        )).read_text()),
    }, fixture)
    assert checked["score_candidate"] == 1, checked["checks"]
    assert checked["manual_review_required"]


def test_semantic_review_is_bound_to_the_saved_answer_and_references(tmp_path):
    record_path = ROOT / ".scratch/evidence/dc-formal-20261004-r3/attempts/DOC-01-1.json"
    record = json.loads(record_path.read_text())
    slot = {"case_id": "DOC-01", "attempt": 1, "actual_status": record["result"]["status"],
            "record_sha256": sha256(record_path.read_bytes()).hexdigest(),
            "answer_sha256": sha256(record["result"]["data"]["answer"].encode()).hexdigest(),
            "references_sha256": sha256(json.dumps(record["references"], ensure_ascii=False,
                sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    path = tmp_path / "semantic-review.json"
    review = {"status": "completed", "reviews": [{
        **{name: value for name, value in slot.items() if name != "actual_status"},
        "reviewer": "root", "passed": True,
    }]}
    path.write_text(json.dumps(review))
    metadata = check_data_center_quality.semantic_review_metadata(path, [slot])
    assert metadata["status"] == "completed"
    assert metadata["sha256"] == sha256(path.read_bytes()).hexdigest()
    assert metadata["attempts_requiring_review"] == 1
    review["reviews"][0]["answer_sha256"] = "0" * 64
    path.write_text(json.dumps(review))
    with pytest.raises(ValueError, match="摘要"):
        check_data_center_quality.semantic_review_metadata(path, [slot])


def test_real_chinese_answer_keeps_adjacent_numeric_values_visible():
    record = json.loads((ROOT / (
        ".scratch/evidence/dc-formal-20261004-r4/attempts/MET-07-2.json"
    )).read_text())
    answer = record["result"]["data"]["answer"]
    expected = {"order_count": 105,
                "average_order_value": "24919.048897142857142857142857142857143",
                "average_order_value_display": "24919.05",
                "quota_completion": "0.87071551886855241264559068219633943428",
                "quota_completion_display": "87.07%"}
    for name, value in expected.items():
        assert check_data_center_quality.answer_contains(answer, value), name
    assert not check_data_center_quality.answer_contains(answer, 106)
    assert not check_data_center_quality.answer_contains(answer, "24919.06")
    assert not check_data_center_quality.answer_contains("order_count105", 105)


def test_real_answer_without_fixed_source_line_keeps_the_check_failed():
    record = json.loads((ROOT / (
        ".scratch/evidence/dc-formal-20261004-r4/attempts/MET-01-1.json"
    )).read_text())
    assert not check_data_center_quality.answer_contains(record["result"]["data"]["answer"], 18)


def test_actual_cached_refusal_has_an_empty_text_answer_digest():
    record = json.loads((ROOT / (
        ".scratch/evidence/dc-formal-20261004-r4/attempts/REF-01-1.json"
    )).read_text())
    assert record["result"]["data"]["answer"] is None
    assert check_data_center_quality.record_answer_digest(record) == sha256(b"").hexdigest()
