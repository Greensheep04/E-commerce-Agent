import json
from pathlib import Path

import pytest
from data_center_protocol import DataQuery, DataQuestion, DataResult

from data_center.runtime import DataRuntime, GroundedAnswer, QueryPlan

ROOT = Path(__file__).resolve().parents[2]
INPUTS = ROOT / (
    ".scratch/evidence/dc03-source-coverage-20261004-r1/"
    "inputs-20261004T012249869096Z.json"
)
ATTEMPT = ROOT / ".scratch/evidence/dc-formal-20261004-r6/attempts/REL-03-1.json"


def coverage_query():
    return DataQuery.model_validate(json.loads(INPUTS.read_text())["query_inputs"]["coverage_graph"])


def original_plan(query):
    selection = query.model_dump(mode="json", exclude={
        "question", "snapshot_id", "retrieval_mode", "required_source_ids",
    })
    return {"queries": [selection], "reason": "读取登记的跨文档来源"}


def test_query_plan_preserves_explicit_required_sources_from_original_question():
    query = coverage_query()
    question = DataQuestion(
        question=query.question, source_ids=query.source_ids,
        required_source_ids=query.required_source_ids,
        retrieval_mode=query.retrieval_mode, snapshot_id=query.snapshot_id,
    )
    queries = QueryPlan.model_validate(original_plan(query)).to_queries(question)
    assert len(queries) == 1
    assert queries[0].required_source_ids == query.required_source_ids
    assert queries[0].source_ids == question.source_ids
    assert queries[0].snapshot_id == question.snapshot_id
    assert queries[0].question == question.question


def test_query_plan_cannot_remove_an_explicit_required_source():
    query = coverage_query()
    question = DataQuestion(
        question=query.question, source_ids=query.source_ids,
        required_source_ids=query.required_source_ids,
    )
    plan = original_plan(query)
    plan["queries"][0]["source_ids"] = query.source_ids[1:]
    with pytest.raises(ValueError, match="必需来源"):
        QueryPlan.model_validate(plan).to_queries(question)


def test_model_can_require_a_subset_within_the_original_candidate_sources():
    query = coverage_query()
    question = DataQuestion(question=query.question, source_ids=query.source_ids)
    plan = original_plan(query)
    plan["queries"][0]["required_source_ids"] = query.source_ids[:2]
    queries = QueryPlan.model_validate(plan).to_queries(question)
    assert queries[0].required_source_ids == query.source_ids[:2]


def test_original_answer_with_only_readme_citations_fails_five_source_requirement():
    query = coverage_query()
    record = json.loads(ATTEMPT.read_text())
    result = DataResult.model_validate_json(json.dumps(record["result"]["data"]))
    answer = GroundedAnswer(
        answer=result.answer, citations=result.metadata["answer_citations"],
    )
    assert len({ref.source_id for ref in result.evidence}) == 1
    DataRuntime._validate_answer_citations(answer, result, [])
    with pytest.raises(ValueError, match="回答引用缺少必需来源"):
        DataRuntime._validate_answer_citations(answer, result, query.required_source_ids)


def test_original_readme_answer_satisfies_its_single_required_source():
    record = json.loads(ATTEMPT.read_text())
    result = DataResult.model_validate_json(json.dumps(record["result"]["data"]))
    answer = GroundedAnswer(
        answer=result.answer, citations=result.metadata["answer_citations"],
    )
    required = [result.evidence[answer.citations[0] - 1].source_id]
    DataRuntime._validate_answer_citations(answer, result, required)
    answer = answer.model_copy(update={"citations": [len(result.evidence) + 1]})
    with pytest.raises(ValueError, match="没有提供的证据编号"):
        DataRuntime._validate_answer_citations(answer, result, required)
