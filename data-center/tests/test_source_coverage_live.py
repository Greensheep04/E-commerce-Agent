import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from data_center_protocol import DataEvidenceRef, DataQuery, DocumentSearchHit, DocumentSearchResult
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from data_center.documents.engine import DocumentEngine
from data_center.documents.models import DocumentRequestRow
from data_center.graph.engine import GraphConfig, GraphEngine
from data_center.ports import ShopPorts
from data_center.retrieval.ranking import cosine_distance, lexical_score, rank_rrf

pytest_plugins = ["test_source_coverage"]


@pytest_asyncio.fixture
async def coverage_live(coverage_input):
    configured = os.environ.get("DC03_COVERAGE_LIVE_SPEC")
    if configured is None:
        pytest.skip("来源覆盖检查需要真实HTTP与PG配置")
    spec_path = Path(configured)
    spec = json.loads(spec_path.read_text())
    assert hashlib.sha256(spec_path.read_bytes()).hexdigest() == coverage_input["spec_sha256"]
    identities = json.loads(Path(spec["identities_path"]).read_text())
    actor = identities["actors"]["admin"]
    ports = ShopPorts(spec["host_url"])
    database = create_engine(spec["database_url"])
    original = coverage_input["original_identity"]
    context = await ports.login(actor["username"], Path(actor["password_file"]).read_text().strip(),
                                original["request_id"])
    assert context.actor_id == original["actor_id"]
    documents = DocumentEngine(database, Path(spec["artifact_root"]), ports, ports, ports,
                               graph_config=GraphConfig(enabled=True))
    attempt = json.loads(Path(".scratch/evidence/dc-formal-20261004-r6/attempts/REL-03-1.json").read_text())
    state = {"input": coverage_input, "documents": documents, "context": context,
             "database": database, "ports": ports, "identity": original,
             "run_id": attempt["result"]["run_id"]}
    state["usage_before"] = await ports.usage(original["task_id"], context)
    try:
        yield state
        usage_after = await ports.usage(original["task_id"], context)
        assert usage_after == state["usage_before"]
        observe(state, "model-usage-unchanged", {"usage_before": state["usage_before"],
                                                  "usage_after": usage_after})
    finally:
        await ports.close()
        database.dispose()


def observe(state, name, values):
    source_paths = [Path("data-center/src/data_center/documents/engine.py"),
                    Path("data-center/src/data_center/retrieval/ranking.py"),
                    Path("data-center/src/data_center/graph/engine.py"),
                    Path("packages/data-center-protocol/src/data_center_protocol/__init__.py")]
    input_path = Path(os.environ["DC03_COVERAGE_INPUTS"])
    report = {"milestone": "DC-03", "check": name, "captured_at": datetime.now(UTC).isoformat(),
              "snapshot_id": state["input"]["snapshot_id"], "identity": state["identity"],
              "input_sha256": hashlib.sha256(input_path.read_bytes()).hexdigest(),
              "source_sha256": {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in source_paths},
              "test_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "model_calls": 0, "usage": state["usage_before"], **values}
    output = Path(".scratch/evidence/dc03-source-coverage-20261004-r1")
    target = output / f"{name}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


async def rank_probe(state, name):
    documents = state["documents"]
    assert hasattr(documents, "_rank_hybrid"), "来源分组排序方法尚未完成"
    query = DataQuery.model_validate(state["input"]["query_inputs"][name])
    scope, snapshots, rows = await documents._visible(query, state["context"])
    expected_rows = [item for item in [*state["input"]["actual_chunks"], *state["input"]["equal_content_chunks"]]
                     if item["source_id"] in query.source_ids]
    assert {(row.snapshot_id, row.chunk_id, row.text_sha256) for row in rows} == {
        (item["snapshot_id"], item["chunk_id"], item["text_sha256"]) for item in expected_rows}
    vector_origin = state["input"]["actual_chunks"][0]
    vector = vector_origin["embedding"]
    ranked, missing = documents._rank_hybrid(query, scope, rows, vector)
    assert missing == []
    indexed = {f"{row.snapshot_id}:{row.chunk_id}": row for row in rows}
    hits = []
    for rank, item in enumerate(ranked, 1):
        chunk = await documents.read_evidence(DataEvidenceRef.model_validate(indexed[item["key"]].evidence),
                                               state["context"])
        hits.append(DocumentSearchHit(rank=rank, chunk=chunk, lexical_score=item["lexical_score"],
                                      vector_distance=item["vector_distance"], rrf_score=item["rrf_score"]))
    result = DocumentSearchResult(status="completed", snapshots=snapshots, hits=hits)
    return query, scope, rows, vector_origin, ranked, result


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["required_subset", "limit_exact", "same_content_distinct_sources"])
async def test_real_pg_required_subset_limit_and_equal_content(coverage_live, name):
    state = coverage_live
    query, _, _, origin, ranked, result = await rank_probe(state, name)
    assert set(query.required_source_ids) <= {hit.chunk.ref.source_id for hit in result.hits}
    assert len(result.hits) <= query.limit
    if name == "limit_exact":
        assert len(result.hits) == len(query.required_source_ids) == 5
        assert any(item.get("retrieval_scope") == "source" for item in ranked)
    if name == "same_content_distinct_sources":
        assert len(result.hits) == 2
        assert len({hit.chunk.text_sha256 for hit in result.hits}) == 1
        assert len({(hit.chunk.ref.snapshot_id, hit.chunk.ref.chunk_id) for hit in result.hits}) == 2
    observe(state, "ranking-" + name, {"query": query.model_dump(mode="json"), "ranked": ranked,
        "result": result.model_dump(mode="json"), "probe_vector_origin": origin["evidence"],
        "quality_verified": False})


@pytest.mark.asyncio
async def test_real_pg_hybrid_coverage_and_original_ranking(coverage_live):
    state = coverage_live
    query, scope, rows, origin, ranked, result = await rank_probe(state, "coverage_hybrid")
    required = set(query.required_source_ids)
    assert {hit.chunk.ref.source_id for hit in result.hits} >= required
    assert len(result.hits) == query.limit
    optional = query.model_copy(update={"required_source_ids": []})
    original, missing = state["documents"]._rank_hybrid(optional, scope, rows, origin["embedding"])
    indexed = {f"{row.snapshot_id}:{row.chunk_id}": row for row in rows}
    lexical = sorted([(key, lexical_score(query.question, row.text)) for key, row in indexed.items()
                      if lexical_score(query.question, row.text) > 0], key=lambda item: (-item[1], item[0]))
    distances = sorted([(key, cosine_distance(origin["embedding"], row.embedding))
                        for key, row in indexed.items()], key=lambda item: (item[1], item[0]))
    expected = rank_rrf(lexical, distances, limit=query.limit)
    assert missing == []
    assert [item["key"] for item in original] == [item["key"] for item in expected]
    assert [item["rrf_score"] for item in original] == [item["rrf_score"] for item in expected]
    observe(state, "hybrid-ranking-probe", {
        "query": query.model_dump(mode="json"), "scope": scope.model_dump(mode="json"),
        "probe_vector_origin": {key: origin[key] for key in ("evidence", "embedding_call_id")},
        "probe_kind": "真实片段向量用于排序检查", "ranked": ranked, "original_ranked": original,
        "result": result.model_dump(mode="json"), "quality_verified": False})


@pytest.mark.asyncio
async def test_real_graph_candidates_merge_with_pg_references(coverage_live):
    state = coverage_live
    documents = state["documents"]
    assert hasattr(documents, "_merge_source_coverage"), "Graph来源补充选择尚未完成"
    query, _, rows, origin, _, hybrid = await rank_probe(state, "coverage_hybrid")
    graph = DocumentSearchResult.model_validate_json(json.dumps(state["input"]["original_envelope"]["result"]))
    assert len({hit.chunk.ref.source_id for hit in graph.hits}) == 1
    graph_query = query.model_copy(update={"retrieval_mode": "graph"})
    merged = documents._merge_source_coverage(graph_query, graph, hybrid)
    assert merged.status == "completed"
    assert len(merged.hits) == len({(hit.chunk.ref.snapshot_id, hit.chunk.ref.chunk_id)
                                  for hit in merged.hits}) == query.limit
    assert {hit.chunk.ref.source_id for hit in merged.hits} == set(query.required_source_ids)
    assert merged.metadata["graph"] == graph.metadata["graph"]
    assert merged.metadata["graph_evidence"] == graph.metadata["graph_evidence"]
    assert len(merged.metadata["coverage_supplements"]) == 4
    for hit in merged.hits:
        assert await documents.read_evidence(hit.chunk.ref, state["context"]) == hit.chunk
    await GraphEngine(documents, documents.graph_config).validate_cached(merged, rows, state["context"])
    exact = documents._merge_source_coverage(graph_query.model_copy(update={"limit": 5}), graph, hybrid)
    assert len(exact.hits) == 5
    assert {hit.chunk.ref.source_id for hit in exact.hits} == set(query.required_source_ids)
    observe(state, "graph-merge-probe", {
        "query": graph_query.model_dump(mode="json"), "graph": graph.model_dump(mode="json"),
        "hybrid_probe": hybrid.model_dump(mode="json"), "result": merged.model_dump(mode="json"),
        "exact_limit_result": exact.model_dump(mode="json"), "graph_mapping_verified": True,
        "probe_vector_origin": origin["evidence"], "quality_verified": False})


@pytest.mark.asyncio
async def test_real_missing_sources_block_before_model_dispatch(coverage_live):
    state = coverage_live
    assert hasattr(state["documents"], "_coverage_metadata"), "来源缺失预检尚未完成"
    query = DataQuery.model_validate(state["input"]["query_inputs"]["missing_visible_chunks"])
    context = state["context"].model_copy(update={"request_id": str(uuid4())})
    result = await state["documents"].search(query, context, task_id=state["identity"]["task_id"],
                                               run_id=state["run_id"])
    assert result.status == "DATA_BLOCKED" and result.call_ids == [] and result.hits == []
    missing = set(result.metadata["missing_source_ids"])
    expected = set(query.required_source_ids) - {item["source_id"] for item in state["input"]["actual_chunks"]
                                                if item["record_id"] == query.record_id}
    assert missing == expected and len(missing) == 4
    repeated = await state["documents"].search(query, context, task_id=state["identity"]["task_id"],
                                                 run_id=state["run_id"])
    assert repeated == result
    with Session(state["database"]) as session:
        envelope = session.get(DocumentRequestRow, (context.actor_id, context.request_id, "search")).result_payload
        assert envelope["result"] == result.model_dump(mode="json")
    changed = query.model_copy(update={"required_source_ids": [next(iter(set(query.source_ids) - missing))]})
    with pytest.raises(ValueError, match="相同文档请求编号"):
        await state["documents"].search(changed, context, task_id=state["identity"]["task_id"],
                                         run_id=state["run_id"])
    observe(state, "missing-sources-preflight", {"request_id": context.request_id,
        "query": query.model_dump(mode="json"), "result": result.model_dump(mode="json"),
        "same_request_reused": True, "changed_required_denied": True, "envelope": envelope})


@pytest.mark.asyncio
async def test_real_original_graph_cache_keeps_envelope_and_usage(coverage_live):
    state = coverage_live
    query_payload = state["input"]["original_envelope"]["query"]
    query = DataQuery.model_validate(query_payload)
    assert query.required_source_ids == [] and query.model_dump(mode="json") == query_payload
    expected = DocumentSearchResult.model_validate_json(json.dumps(state["input"]["original_envelope"]["result"]))
    result = await state["documents"].search(query, state["context"], task_id=state["identity"]["task_id"],
                                               run_id=state["run_id"])
    assert result == expected
    with Session(state["database"]) as session:
        envelope = session.get(DocumentRequestRow, (state["context"].actor_id,
            state["context"].request_id, "search")).result_payload
        assert envelope == state["input"]["original_envelope"]
    observe(state, "original-cache-compatibility", {"result": result.model_dump(mode="json"),
        "query_payload_unchanged": True, "envelope_unchanged": True})
