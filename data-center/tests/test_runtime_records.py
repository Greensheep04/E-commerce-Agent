import json
from datetime import date
from pathlib import Path

from data_center_protocol import DataQuestion

from data_center.runtime import DataRuntime, QueryPlan


def test_answer_inputs_include_exact_metric_numerators_and_denominators():
    root = Path(__file__).resolve().parents[2]
    record = json.loads((root / (
        ".scratch/evidence/dc-formal-20261004-r2/attempts/MET-01-1.json"
    )).read_text())
    detail = record["references"][0]["data"]
    values = DataRuntime._metric_inputs({"rows": detail["rows"],
                                        "original_quotas": detail["original_quotas"]})
    assert values["sales_amount"] == "1142965.428500"
    assert values["order_count"] == 42
    assert values["quota_amount"] == "1324000.0000"
    assert values["average_order_value"] == "27213.462583333333333333333333333333333"
    assert values["quota_completion"] == "0.86326693995468277945619335347432024169"


def test_query_selection_preserves_the_real_original_question():
    root = Path(__file__).resolve().parents[2]
    record = json.loads((root / (
        ".scratch/evidence/dc-formal-20261004-r3/attempts/DOC-04-1.json"
    )).read_text())
    provider = json.loads((root / (
        ".scratch/evidence/dc-planning-repro-20261004-r1/doc04.json"
    )).read_text())
    selection = json.loads(provider["requests"][0]["response"]["choices"][0]["message"]["content"])
    original = DataQuestion.model_validate(record["input"]["payload"])
    assert selection["queries"][0]["question"] != original.question
    for item in selection["queries"]:
        for name in ("question", "snapshot_id", "retrieval_mode"):
            item.pop(name, None)
    plan = QueryPlan.model_validate(selection)
    queries = plan.to_queries(original)
    assert len(queries) == 1
    assert queries[0].question == original.question
    assert queries[0].source_ids == original.source_ids
    assert queries[0].retrieval_mode == original.retrieval_mode
    assert queries[0].snapshot_id == original.snapshot_id


def test_query_selection_keeps_validated_metric_dates():
    root = Path(__file__).resolve().parents[2]
    record = json.loads((root / (
        ".scratch/evidence/dc-formal-20261004-r3/attempts/MET-04-3.json"
    )).read_text())
    provider = json.loads((root / (
        ".scratch/evidence/dc-planning-repro-20261004-r1/met04.json"
    )).read_text())
    selection = json.loads(provider["requests"][0]["response"]["choices"][0]["message"]["content"])
    for item in selection["queries"]:
        for name in ("question", "snapshot_id", "retrieval_mode"):
            item.pop(name, None)
    plan = QueryPlan.model_validate_json(json.dumps(selection))
    queries = plan.to_queries(DataQuestion.model_validate(record["input"]["payload"]))
    assert queries[0].start_date == date(2023, 7, 1)
    assert queries[0].end_date == date(2023, 10, 1)
