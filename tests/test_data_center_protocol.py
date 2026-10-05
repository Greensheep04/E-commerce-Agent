import hashlib
import json
from datetime import date
from importlib import import_module
from pathlib import Path

import pytest
from pydantic import ValidationError

ROOT = Path(__file__).parents[1]


def source_coverage_question():
    record = json.loads((ROOT / (
        ".scratch/evidence/dc-formal-20261004-r6/attempts/REL-03-1.json"
    )).read_text(encoding="utf-8"))
    return record["input"]["payload"]


def test_required_document_sources_are_serialized_only_when_requested():
    protocol = import_module("data_center_protocol")
    original = source_coverage_question()
    question = protocol.DataQuestion.model_validate(original)
    legacy = {**original, "snapshot_id": None}
    assert question.required_source_ids == []
    assert question.model_dump(mode="json") == legacy
    assert protocol.DataQuestion.model_validate({
        **original, "required_source_ids": [],
    }).model_dump(mode="json") == legacy
    required = original["source_ids"]
    covered = protocol.DataQuestion.model_validate({
        **original, "required_source_ids": required,
    })
    assert covered.model_dump(mode="json")["required_source_ids"] == required
    assert covered.model_dump_json() != question.model_dump_json()


def test_required_sources_validate_the_real_document_candidate_set():
    protocol = import_module("data_center_protocol")
    original = source_coverage_question()
    payload = {**original, "intent": "documents", "limit": 5,
               "required_source_ids": original["source_ids"]}
    query = protocol.DataQuery.model_validate(payload)
    assert query.required_source_ids == original["source_ids"]
    assert query.model_dump(mode="json")["required_source_ids"] == original["source_ids"]
    with pytest.raises(ValidationError, match="数量.*limit"):
        protocol.DataQuery.model_validate({**payload, "limit": 4})
    legacy = protocol.DataQuery.model_validate({
        key: value for key, value in payload.items() if key != "required_source_ids"
    })
    assert "required_source_ids" not in legacy.model_dump(mode="json")
    assert protocol.DataQuery.model_validate({
        **payload, "required_source_ids": [],
    }).model_dump(mode="json") == legacy.model_dump(mode="json")


@pytest.mark.parametrize("model_name", ["DataQuestion", "DataQuery"])
def test_required_sources_reject_duplicate_and_unselected_sources(model_name):
    protocol = import_module("data_center_protocol")
    original = source_coverage_question()
    if model_name == "DataQuery":
        original = {**original, "intent": "documents"}
    model = getattr(protocol, model_name)
    source = original["source_ids"][0]
    with pytest.raises(ValidationError, match="必需来源重复"):
        model.model_validate({**original, "required_source_ids": [source, source]})
    with pytest.raises(ValidationError, match="必需来源.*候选来源"):
        model.model_validate({**original, "source_ids": original["source_ids"][1:],
                              "required_source_ids": [source]})


def test_business_query_rejects_required_document_sources():
    protocol = import_module("data_center_protocol")
    with pytest.raises(ValidationError, match="必需来源只适用于文档"):
        protocol.DataQuery.model_validate({
            "question": "读取来源", "intent": "business",
            "source_ids": ["microsoft/adventureworks"],
            "required_source_ids": ["microsoft/adventureworks"],
        })


def test_data_query_rejects_actor_and_sql_fields():
    protocol = import_module("data_center_protocol")
    valid = protocol.DataQuery.model_validate({"question": "查询订单", "intent": "business",
                                                "source_ids": ["microsoft/adventureworks"]})
    assert valid.source_ids == ["microsoft/adventureworks"]
    for extra in ({"actor_id": "unauthorized"}, {"sql": "SELECT * FROM orders"}):
        with pytest.raises(ValidationError):
            protocol.DataQuery.model_validate({"question": "查询订单", "intent": "business",
                                              "source_ids": ["adventureworks"], **extra})


def test_metric_query_requires_valid_half_open_period_and_registered_names():
    protocol = import_module("data_center_protocol")
    valid = {"question": "查询季度销售额", "intent": "metrics",
             "source_ids": ["adventureworks"], "metric_ids": ["sales_amount"],
             "start_date": date(2023, 7, 1), "end_date": date(2023, 10, 1)}
    assert protocol.DataQuery.model_validate(valid).end_date == date(2023, 10, 1)
    for changes in ({"end_date": date(2023, 7, 1)}, {"metric_ids": []},
                    {"metric_ids": ["sales; DROP TABLE orders"]}, {"start_date": None}):
        with pytest.raises(ValidationError):
            protocol.DataQuery.model_validate({**valid, **changes})


def test_document_reference_rejects_unbound_or_invalid_locations():
    protocol = import_module("data_center_protocol")
    source = Path(__file__).parents[1] / "AGENTS.md"
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    reference = {"kind": "document", "source_id": "project_shop",
                 "snapshot_id": digest, "record_id": "AGENTS.md", "source_revision": digest,
                 "source_sha256": digest, "content_sha256": digest,
                 "chunk_id": "project_instructions", "start_line": 1, "end_line": 3}
    assert protocol.DataEvidenceRef.model_validate(reference).record_id == "AGENTS.md"
    for changes in ({"source_sha256": "missing"}, {"end_line": 0}, {"start_line": 4},
                    {"snapshot_id": None}, {"kind": "record", "start_line": 1}):
        with pytest.raises(ValidationError):
            protocol.DataEvidenceRef.model_validate({**reference, **changes})


def test_completed_result_requires_bound_sources_and_evidence():
    protocol = import_module("data_center_protocol")
    with pytest.raises(ValidationError):
        protocol.DataResult.model_validate({"result_id": "result", "status": "completed",
                                            "answer": "销售额是100元"})
    blocked = protocol.DataResult.model_validate({"result_id": "inventory-check",
                                                  "status": "DATA_BLOCKED",
                                                  "unknowns": ["库存来源尚未登记"]})
    assert blocked.answer is None
    with pytest.raises(ValidationError):
        protocol.DataResult.model_validate({"result_id": "result", "status": "DATA_BLOCKED",
                                            "answer": "当前库存是10件"})


def output_limit_request():
    record = json.loads((ROOT / (
        ".scratch/evidence/dc-formal-20261004-r6/attempts/REL-03-1.json"
    )).read_text())
    return {"task_id": record["task_id"], "run_id": record["result"]["run_id"],
            "step_key": "answer", "kind": "chat", "model": "deepseek-flash",
            "source_ids": record["input"]["payload"]["source_ids"],
            "messages": [{"role": "user", "content": record["input"]["payload"]["question"]}],
            "reasoning_effort": "high", "max_output_tokens": 16384}


def test_final_answer_accepts_16384_output_tokens():
    protocol = import_module("data_center_protocol")
    request = protocol.ModelInvocation.model_validate(output_limit_request())
    assert request.max_output_tokens == 16384
    with pytest.raises(ValidationError):
        protocol.ModelInvocation.model_validate({**output_limit_request(), "max_output_tokens": 16385})


@pytest.mark.parametrize("step_key", ["query-plan", "graph-extract"])
def test_other_chat_steps_keep_8192_output_limit(step_key):
    protocol = import_module("data_center_protocol")
    request = {**output_limit_request(), "step_key": step_key}
    assert protocol.ModelInvocation.model_validate({
        **request, "max_output_tokens": 8192,
    }).max_output_tokens == 8192
    with pytest.raises(ValidationError):
        protocol.ModelInvocation.model_validate({**request, "max_output_tokens": 8193})


def test_embedding_keeps_8192_output_limit_even_with_answer_step_key():
    protocol = import_module("data_center_protocol")
    request = {**output_limit_request(), "kind": "embedding", "model": "qwen3.7-text-embedding",
               "messages": [], "texts": [source_coverage_question()["question"]]}
    assert protocol.ModelInvocation.model_validate({
        **request, "max_output_tokens": 8192,
    }).max_output_tokens == 8192
    with pytest.raises(ValidationError):
        protocol.ModelInvocation.model_validate({**request, "max_output_tokens": 8193})
