import hashlib
import json
import os
from pathlib import Path

import pytest

from data_center.retrieval import ranking


@pytest.fixture
def coverage_input():
    configured = os.environ.get("DC03_COVERAGE_INPUTS")
    if configured is None:
        pytest.skip("来源覆盖检查需要指定已核查的真实PG片段与请求输入")
    value = json.loads(Path(configured).read_text())
    assert value["model_calls"] == 0
    for chunk in value["actual_chunks"]:
        assert hashlib.sha256(chunk["text"].encode()).hexdigest() == chunk["text_sha256"]
        assert len(chunk["embedding"]) == chunk["embedding_dimensions"] == 1024
    return value


def ranked_inputs(value):
    chunks = value["actual_chunks"]
    source_by_key = {f"{item['snapshot_id']}:{item['chunk_id']}": item["source_id"] for item in chunks}
    graph = [{"key": f"{hit['chunk']['ref']['snapshot_id']}:{hit['chunk']['ref']['chunk_id']}",
              "origin": "graph"} for hit in value["original_envelope"]["result"]["hits"]]
    required = value["query_inputs"]["coverage_graph"]["required_source_ids"]
    graph_sources = {source_by_key[item["key"]] for item in graph}
    supplements = [{"key": f"{value['snapshot_id']}:{value['lexical_ranked_chunk_ids_by_source'][source_id][0]}",
                   "origin": "hybrid"} for source_id in required if source_id not in graph_sources]
    assert len(graph) == 10 and len(graph_sources) == 1 and len(supplements) == 4
    return graph, supplements, source_by_key, required


def selector():
    function = getattr(ranking, "select_source_coverage", None)
    assert function is not None, "检索选择器需要保留每个必需来源的真实引用"
    return function


def test_required_sources_survive_global_graph_ranking(coverage_input):
    graph, supplements, source_by_key, required = ranked_inputs(coverage_input)
    selected, missing = selector()([*graph, *supplements], source_by_key, required, limit=10)
    assert missing == []
    assert len(selected) == 10
    assert {source_by_key[item["key"]] for item in selected} == set(required)
    assert selected[:6] == graph[:6]
    assert selected[6:] == supplements


def test_limit_equal_to_required_count_keeps_each_source(coverage_input):
    graph, supplements, source_by_key, required = ranked_inputs(coverage_input)
    selected, missing = selector()([*graph, *supplements], source_by_key, required, limit=5)
    assert selected == [graph[0], *supplements]
    assert missing == []


def test_empty_required_sources_preserve_original_ranking(coverage_input):
    graph, supplements, source_by_key, _ = ranked_inputs(coverage_input)
    selected, missing = selector()([*graph, *supplements], source_by_key, [], limit=10)
    assert selected == graph
    assert missing == []


def test_missing_required_sources_are_explicit(coverage_input):
    graph, _, source_by_key, required = ranked_inputs(coverage_input)
    selected, missing = selector()(graph, source_by_key, required, limit=10)
    assert selected == []
    assert set(missing) == set(required) - {source_by_key[graph[0]["key"]]}
    assert len(missing) == 4


def test_repeated_candidates_preserve_unique_chunk_identities(coverage_input):
    graph, supplements, source_by_key, required = ranked_inputs(coverage_input)
    selected, missing = selector()([*graph, graph[0], *supplements, supplements[0]],
                                   source_by_key, required, limit=10)
    assert missing == []
    assert len(selected) == len({item["key"] for item in selected}) == 10
    assert selected[:6] == graph[:6] and selected[6:] == supplements


def test_identical_text_from_two_sources_keeps_both_references(coverage_input):
    chunks = coverage_input["equal_content_chunks"]
    assert len({item["text_sha256"] for item in chunks}) == 1
    assert len({item["source_id"] for item in chunks}) == 2
    candidates = [{"key": f"{item['snapshot_id']}:{item['chunk_id']}"} for item in chunks]
    source_by_key = {item["key"]: chunk["source_id"] for item, chunk in zip(candidates, chunks, strict=True)}
    selected, missing = selector()(candidates, source_by_key, [item["source_id"] for item in chunks], limit=2)
    assert selected == candidates and missing == []
