import asyncio
import hashlib
import importlib.util
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from data_center_protocol import DataEvidenceRef, DataQuery
from sqlalchemy import create_engine, text

from data_center.documents.engine import DocumentEngine
from data_center.graph.engine import GraphConfig, GraphEngine
from data_center.graph.identity import graph_chunk_mapping

OUTPUT = Path(".scratch/evidence/dc03-source-coverage-full-20261004-r1")
EXPECTED_MISSING = {
    "Docker": "ent-c5fd214cdd0d2b3b4272e73b022ba5c2",
    "ColA": "ent-b363b6b29db8ea5da4f9a4c90e4ff0c9",
    "ColB": "ent-200d949e0a48d3d4a2ea5a3a2546e671",
    "ColC": "ent-a7263f8d265c22d95f8b69b83d8ffe7f",
    "ColD": "ent-4ed158807da81c73e83475eb614815d1",
    "Slide 1": "ent-6e566e6e3d24fdd3c702ac4bfa4402c1",
    "Picture 1": "ent-7a9cfef99d558628b086bd30c6e1dc4d",
}


def vector_state(database, fingerprint):
    with database.connect() as connection:
        nodes = connection.execute(text(
            "SELECT id, properties FROM lightrag_graph_nodes WHERE workspace=:workspace AND namespace='chunk_entity_relation'"
        ), {"workspace": "dc-" + fingerprint}).mappings().all()
        vectors = connection.execute(text(
            "SELECT id, entity_name, chunk_ids, content_vector IS NOT NULL AS vector_present "
            "FROM lightrag_vdb_entity_qwen3_7_text_embedding_1024d WHERE workspace=:workspace"
        ), {"workspace": "dc-" + fingerprint}).mappings().all()
        edges = connection.execute(text(
            "SELECT src_id, tgt_id FROM lightrag_graph_edges WHERE workspace=:workspace AND namespace='chunk_entity_relation'"
        ), {"workspace": "dc-" + fingerprint}).all()
        relations = connection.execute(text(
            "SELECT id, source_id, target_id, chunk_ids, content_vector IS NOT NULL AS vector_present "
            "FROM lightrag_vdb_relation_qwen3_7_text_embedding_1024d WHERE workspace=:workspace"
        ), {"workspace": "dc-" + fingerprint}).mappings().all()
    return {"nodes": [dict(row) for row in nodes], "vectors": [dict(row) for row in vectors],
            "edges": [list(row) for row in edges], "relations": [dict(row) for row in relations]}


@pytest_asyncio.fixture
async def recovery_live():
    configured = os.environ.get("DC03_GRAPH_RECOVERY_STATE")
    if configured is None:
        pytest.skip("Graph恢复检查需要明确的真实来源与费用状态")
    state_path = Path(configured)
    state = json.loads(state_path.read_text())
    module_spec = importlib.util.spec_from_file_location("dc03_coverage_driver", OUTPUT / "run_full_coverage.py")
    driver = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(driver)
    assert state["source_sha256"] == driver.source_digests()
    if "graph_recovery" in state:
        driver.validate_recovery_state(state)
    spec = json.loads(driver.SPEC.read_text())
    plan = json.loads(driver.PLAN.read_text())
    actors = json.loads(Path(spec["identities_path"]).read_text())["actors"]
    source_ids = list(plan["source_map"].values())
    database, shop = create_engine(spec["database_url"]), create_engine(driver.SHOP_DATABASE)
    budget = driver.BudgetAudit(shop, state)
    ports = driver.GuardedShopPorts(spec["host_url"], budget, source_ids, asyncio.Lock())
    entry = plan["graph_query"]
    actor = actors["customer"]
    context = await ports.login(actor["username"], Path(actor["password_file"]).read_text().strip(),
                                entry["identity"]["request_id"])
    documents = DocumentEngine(database, Path(spec["artifact_root"]), ports, ports, ports,
                               graph_config=GraphConfig(enabled=True))
    try:
        yield {"database": database, "documents": documents, "budget": budget, "context": context,
               "entry": entry, "query": DataQuery.model_validate(entry["payload"]), "state": state,
               "state_path": state_path}
    finally:
        await ports.close()
        database.dispose()
        shop.dispose()


@pytest.mark.asyncio
async def test_published_graph_restores_persistent_vectors_from_authorized_chunks(recovery_live):
    state = recovery_live
    documents, context, entry = state["documents"], state["context"], state["entry"]
    original = json.loads(Path(state["state"]["checks"]["graph_query"]).read_text())
    fingerprint = original["result"]["metadata"]["fingerprint"]
    before = vector_state(state["database"], fingerprint)
    budget_before = state["budget"].check()
    result = await documents.build_graph(state["query"], context, task_id=entry["identity"]["task_id"],
                                         run_id=entry["identity"]["run_id"])
    after = vector_state(state["database"], fingerprint)
    budget_after = state["budget"].check()
    report = {"milestone": "DC-03", "fingerprint": fingerprint, "identity": entry["identity"],
              "source_sha256": state["state"]["source_sha256"],
              "test_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "state_path": str(state["state_path"].resolve()), "before": before, "after": after,
              "budget_before": budget_before, "budget_after": budget_after, "result": result}
    target = OUTPUT / f"graph-vector-recovery-observation-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    assert result["state"] == "published" and result["fingerprint"] == fingerprint
    assert len(after["nodes"]) == len(after["vectors"]) == 172
    assert len(after["edges"]) == len(after["relations"]) == 186
    assert {row["id"] for row in after["nodes"]} == {row["entity_name"] for row in after["vectors"]}
    assert {tuple(sorted(row)) for row in after["edges"]} == {
        tuple(sorted((row["source_id"], row["target_id"]))) for row in after["relations"]}
    assert all(row["vector_present"] for row in [*after["vectors"], *after["relations"]])
    assert {row["entity_name"]: row["id"] for row in after["vectors"] if row["entity_name"] in EXPECTED_MISSING} == EXPECTED_MISSING
    current = {row["id"]: row for row in budget_after["local_requests"]}
    assert all(current[row["id"]] == row for row in budget_before["local_requests"])
    repeated = await documents.build_graph(state["query"], context, task_id=entry["identity"]["task_id"],
                                           run_id=entry["identity"]["run_id"])
    assert repeated == result and state["budget"].check()["local_requests"] == budget_after["local_requests"]
    _, _, rows = await documents._visible(state["query"], context)
    for row in rows:
        chunk = await documents.read_evidence(DataEvidenceRef.model_validate(row.evidence), context)
        assert chunk.text_sha256 == row.text_sha256


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["missing_source", "unauthorized_source", "invalid_key", "inconsistent_source"])
async def test_graph_vector_validation_rejects_invalid_saved_records(recovery_live, invalid):
    state = recovery_live
    documents, context, entry = state["documents"], state["context"], state["entry"]
    scope, snapshots, rows = await documents._visible(state["query"], context)
    engine = GraphEngine(documents, GraphConfig(enabled=True))
    fingerprint = engine._identity(scope, snapshots, rows)
    mapping = graph_chunk_mapping("doc-" + fingerprint,
                                  [(row.snapshot_id, row.chunk_id, row.text) for row in rows])
    before = state["budget"].check()
    rag = await engine._rag(fingerprint, state["query"], scope, context,
                            entry["identity"]["task_id"], entry["identity"]["run_id"])
    try:
        nodes = await rag.chunk_entity_relation_graph.get_all_nodes()
        edges = await rag.chunk_entity_relation_graph.get_all_edges()
        entities, relationships = engine._vector_payloads(nodes, edges, mapping, rag._build_global_config())
        assert not engine._missing_vectors(rag.entities_vdb, entities)
        assert not engine._missing_vectors(rag.relationships_vdb, relationships, True)
        # 以真实持久记录构造非法输入；数据库及原请求保持原内容。
        if invalid in {"missing_source", "unauthorized_source"}:
            changed = [dict(node) for node in nodes]
            changed[0]["source_id"] = "" if invalid == "missing_source" else "chunk-unapproved"
            with pytest.raises(ValueError, match="当前授权原文映射"):
                engine._vector_payloads(changed, edges, mapping, rag._build_global_config())
        else:
            changed = {key: dict(value) for key, value in entities.items()}
            key = next(iter(changed))
            if invalid == "invalid_key":
                changed["ent-invalid"] = changed.pop(key)
                expected_error = "向量编号"
            else:
                original = changed[key]["source_id"]
                changed[key]["source_id"] = next(value for value in mapping if value != original)
                expected_error = "来源"
            with pytest.raises(ValueError, match=expected_error):
                engine._missing_vectors(rag.entities_vdb, changed)
    finally:
        await engine._finalize(rag, fingerprint)
    after = state["budget"].check()
    assert after["local_requests"] == before["local_requests"]


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_build_graph_blocks_missing_required_source_before_sdk_dispatch(recovery_live, enabled):
    state = recovery_live
    documents, context, entry = state["documents"], state["context"], state["entry"]
    reduced = json.loads(Path(state["state"]["checks"]["graph-supplement-identity"]).read_text())
    missing = reduced["permission_target"]["source_id"]
    _, _, rows = await documents._visible(state["query"], context)
    assert missing not in {row.source_id for row in rows}
    documents.graph_config = GraphConfig(enabled=enabled)
    before = state["budget"].check()
    result = await documents.build_graph(state["query"], context, task_id=entry["identity"]["task_id"],
                                         run_id=entry["identity"]["run_id"])
    target = OUTPUT / f"graph-required-block-observation-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    target.write_text(json.dumps({"milestone": "DC-03", "source_sha256": state["state"]["source_sha256"],
        "identity": entry["identity"], "configuration": documents.graph_config.model_dump(mode="json"),
        "visible_source_ids": sorted({row.source_id for row in rows}), "missing_source_id": missing,
        "result": result, "budget_before": before, "budget_after": state["budget"].check()},
        ensure_ascii=False, indent=2, default=str) + "\n")
    assert result["state"] == "DATA_BLOCKED" and result.get("missing_source_ids") == [missing]
    assert state["budget"].check()["local_requests"] == before["local_requests"]
