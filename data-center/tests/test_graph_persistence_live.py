import asyncio
import hashlib
import json
import os
import signal
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import numpy as np
import pytest
import pytest_asyncio
from data_center_protocol import DataQuery, ModelInvocation, ModelReply
from sqlalchemy import MetaData, Table, create_engine, select, text, update
from sqlalchemy.orm import Session

from data_center.database import authorized_connection
from data_center.documents.engine import DocumentEngine
from data_center.documents.gateway import ModelStepPending, reply_vectors, reuse_completed_step
from data_center.documents.models import GraphIndexRow
from data_center.graph.engine import GraphConfig, GraphEngine
from data_center.graph.receipts import EmbeddingReceipts
from data_center.ports import ShopPorts
from data_center.store import DataStore

OUTPUT = Path(".scratch/evidence/dc-graph-persistence-20261004-r1")
TASK_ID = "52688e12-15bb-4c71-a8bd-ba4852d2e2d3"
FINGERPRINT = "8f33137a9e1c888cffe947e13cd5143f956b5df3ca80d4fc45a50b0be0d9655f"


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), default=str).encode()).hexdigest()


def observe(name, value):
    OUTPUT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    target = OUTPUT / f"{name}-{stamp}.json"
    report = {"milestone": "DC-03", "captured_at": datetime.now(UTC).isoformat(),
              "quality_verified": False, **value}
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    return target


def saved_graph(database):
    with database.connect() as connection:
        row = dict(connection.execute(text("SELECT * FROM dc_graph_indexes WHERE fingerprint=:key"),
                                      {"key": FINGERPRINT}).mappings().one())
        counts = {}
        for name, table in {
            "nodes": "lightrag_graph_nodes", "edges": "lightrag_graph_edges",
            "entity_vectors": "lightrag_vdb_entity_qwen3_7_text_embedding_1024d",
            "relationship_vectors": "lightrag_vdb_relation_qwen3_7_text_embedding_1024d",
            "chunk_vectors": "lightrag_vdb_chunks_qwen3_7_text_embedding_1024d",
            "sdk_cache": "lightrag_llm_cache",
        }.items():
            counts[name] = connection.scalar(text(f"SELECT count(*) FROM {table} WHERE workspace=:workspace"),
                                             {"workspace": "dc-" + FINGERPRINT})
    return {"index": row, "counts": counts, "index_sha256": digest(row)}


@pytest_asyncio.fixture
async def persistence_live():
    spec_path = os.environ.get("DC_GRAPH_PERSISTENCE_SPEC")
    if spec_path is None:
        pytest.skip("Graph保存检查需要明确的真实宿主配置")
    spec = json.loads(Path(spec_path).read_text())
    actor = json.loads(Path(spec["identities_path"]).read_text())["actors"]["admin"]
    database = create_engine(spec["database_url"])
    ports = ShopPorts(spec["host_url"])
    try:
        context = await ports.login(actor["username"], Path(actor["password_file"]).read_text().strip(),
                                    "dc-graph-persistence-readback-20261004")
        task = DataStore(database).get(TASK_ID, context.actor_id)
        assert task.status == "failed" and not task.cancel_requested
        documents = DocumentEngine(database, Path(spec["artifact_root"]), ports, ports, ports,
                                   graph_config=GraphConfig(enabled=True))
        yield {"database": database, "ports": ports, "context": context, "documents": documents,
               "task": task, "query": DataQuery.model_validate(task.payload), "spec": spec,
               "spec_path": str(Path(spec_path).resolve())}
    finally:
        await ports.close()
        database.dispose()


@pytest.mark.asyncio
async def test_current_authorization_reordered_sources_preserves_saved_graph(persistence_live):
    state = persistence_live
    documents, query, context = state["documents"], state["query"], state["context"]
    before = saved_graph(state["database"])
    usage_before = await state["ports"].usage(TASK_ID, context)
    original = await documents.graph_status(query, context)
    reordered = query.model_copy(update={"source_ids": sorted(query.source_ids)})
    assert reordered.source_ids != query.source_ids
    current = await documents.graph_status(reordered, context)
    reversed_query = query.model_copy(update={"source_ids": list(reversed(query.source_ids))})
    reversed_status = await documents.graph_status(reversed_query, context)
    usage_after = await state["ports"].usage(TASK_ID, context)
    after = saved_graph(state["database"])
    observe("source-order", {"task_id": TASK_ID, "actor_id": context.actor_id,
                            "original": original, "sorted": current, "reversed": reversed_status,
                            "graph_before": before, "graph_after": after,
                            "usage_before": usage_before, "usage_after": usage_after})
    assert current["fingerprint"] == original["fingerprint"] == FINGERPRINT
    assert reversed_status["fingerprint"] == FINGERPRINT
    assert current["state"] == original["state"] == reversed_status["state"]
    assert usage_after == usage_before and after == before


def provider_state(database):
    with database.connect() as connection:
        checksum = connection.scalar(text(
            "SELECT md5(string_agg(md5(row_to_json(row)::text), '' ORDER BY id)) "
            "FROM provider_requests AS row"))
        counts = [dict(row) for row in connection.execute(text(
            "SELECT status, count(*), sum(charged_cny) AS paid, "
            "sum(CASE WHEN charged_cny IS NULL THEN reservation_cny ELSE 0 END) AS reserved "
            "FROM provider_requests GROUP BY status ORDER BY status")).mappings()]
    return {"checksum": checksum, "counts": counts}


@pytest.mark.asyncio
@pytest.mark.parametrize("source_order", ["sorted", "reversed", "original"])
async def test_saved_custom_operation_preserves_original_order(persistence_live, source_order):
    from lightrag.utils import sanitize_text_for_encoding
    from lightrag.utils_pipeline import (
        doc_status_custom_chunk_patch,
        make_custom_chunk_id,
        make_custom_chunk_operation_id,
    )

    state = persistence_live
    database, documents, context = state["database"], state["documents"], state["context"]
    original_query = state["query"]
    sources = (sorted(original_query.source_ids) if source_order == "sorted" else
               list(reversed(original_query.source_ids)) if source_order == "reversed" else
               original_query.source_ids)
    query = original_query.model_copy(update={"source_ids": sources})
    doc_id, workspace = "doc-" + FINGERPRINT, "dc-" + FINGERPRINT
    with database.connect() as connection, connection.begin():
        connection.execute(text("SET TRANSACTION READ ONLY"))
        saved_status = dict(connection.execute(text(
            "SELECT * FROM lightrag_doc_status WHERE workspace=:workspace AND id=:id"
        ), {"workspace": workspace, "id": doc_id}).mappings().one())
        saved_content = connection.scalar(text(
            "SELECT content FROM lightrag_doc_full WHERE workspace=:workspace AND id=:id"
        ), {"workspace": workspace, "id": doc_id})
    journal = doc_status_custom_chunk_patch(saved_status)
    assert journal is not None and saved_content
    shop = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop")
    try:
        providers_before = provider_state(shop)
        scope, snapshots, rows = await documents._visible(query, context)
        graph = GraphEngine(documents, GraphConfig(enabled=True))
        fingerprint = await graph.resolve_identity(query, scope, snapshots, rows, context)
        assert fingerprint == FINGERPRINT
        prepared = await graph._insertion_rows(fingerprint, query, scope, snapshots, rows, context)
        chunk_ids = list(dict.fromkeys(make_custom_chunk_id(doc_id, clean)
            for row in prepared if (clean := sanitize_text_for_encoding(row.text))))
        actual_operation = make_custom_chunk_operation_id(doc_id, chunk_ids)
        content = sanitize_text_for_encoding("".join(row.text for row in prepared))
        with database.connect() as connection, connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            current_status = dict(connection.execute(text(
                "SELECT * FROM lightrag_doc_status WHERE workspace=:workspace AND id=:id"
            ), {"workspace": workspace, "id": doc_id}).mappings().one())
        providers_after = provider_state(shop)
        output = Path(".scratch/evidence/dc-graph-operation-order-20261004-r1")
        output.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        report = {"milestone": "DC-03", "quality_verified": False,
            "captured_at": datetime.now(UTC).isoformat(), "source_order": source_order,
            "fingerprint": fingerprint, "task_id": state["task"].task_id,
            "query_source_ids": query.source_ids,
            "prepared_source_ids": list(dict.fromkeys(row.source_id for row in prepared)),
            "chunk_count": len(prepared), "chunk_ids_sha256": digest(chunk_ids),
            "expected_operation_id": journal["operation_id"], "actual_operation_id": actual_operation,
            "saved_content_sha256": hashlib.sha256(saved_content.encode()).hexdigest(),
            "prepared_content_sha256": hashlib.sha256(content.encode()).hexdigest(),
            "sdk_status_before_sha256": digest(saved_status), "sdk_status_after_sha256": digest(current_status),
            "providers_before": providers_before, "providers_after": providers_after}
        (output / f"operation-{source_order}-{stamp}.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
        assert actual_operation == journal["operation_id"]
        assert content == saved_content
        assert len(prepared) == len(rows) == 247
        assert current_status == saved_status and providers_after == providers_before
        assert digest({"payload": state["task"].payload, "upstream": state["task"].upstream}) == state["task"].input_sha256
    finally:
        shop.dispose()


def completed_embedding(database, run_id):
    with database.connect() as connection:
        rows = connection.execute(text(
            "SELECT id, step_key, request_body, response_body, charged_cny FROM provider_requests "
            "WHERE run_id=:run_id AND status='completed' AND step_key LIKE 'graph-embedding:%' "
            "AND jsonb_array_length(request_body->'input')=20 ORDER BY created_at, id"),
            {"run_id": run_id}).mappings().all()
    for row in rows:
        texts = row["request_body"]["input"]
        if len(set(texts)) == 20 and sum(len(value.encode()) for value in texts) <= 12000:
            return dict(row)
    raise AssertionError("真实历史运行缺少可核查的completed完整batch")


@pytest.mark.asyncio
async def test_completed_embedding_survives_sigterm_and_batch_reordering(persistence_live):
    state = persistence_live
    shop = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop")
    try:
        origin = completed_embedding(shop, state["task"].run_id)
        before = provider_state(shop)
        task_id, run_id, request_id = str(uuid4()), str(uuid4()), str(uuid4())
        query = state["query"].model_copy(update={"source_ids": sorted(state["query"].source_ids)})
        await state["ports"].bind(task_id, run_id, "build_graph", query.source_ids,
                                   query.model_dump(mode="json"), state["context"])
        selected = origin["request_body"]["input"]
        reordered = [selected[7], selected[1], selected[13], selected[1]]
        expected = np.asarray([reply_vectors(ModelReply(request_id=origin["id"], status="completed",
            response=origin["response_body"]), 20)[index] for index in (7, 1, 13, 1)], dtype=np.float32)
        token = str(uuid4())
        definition = {"milestone": "DC-03", "spec_path": state["spec_path"],
            "fingerprint": FINGERPRINT, "request_id": request_id, "query": query.model_dump(mode="json"),
            "seed": {"task_id": TASK_ID, "run_id": state["task"].run_id},
            "restore": {"task_id": task_id, "run_id": run_id}, "texts": selected,
            "reordered_texts": reordered, "original_request_id": origin["id"],
            "seed_output": str((OUTPUT / f"process-seed-{token}.json").resolve()),
            "restore_output": str((OUTPUT / f"process-restore-{token}.json").resolve())}
        target = OUTPUT / f"process-input-{token}.json"
        target.write_text(json.dumps(definition, ensure_ascii=False, indent=2) + "\n")
        command = [sys.executable, "-P", "data-center/tests/graph_receipt_process.py", "--input", str(target)]
        seed = await asyncio.to_thread(subprocess.run, [*command, "--mode", "seed"],
                                       capture_output=True, text=True, timeout=40, check=False)
        (OUTPUT / f"process-seed-{token}.txt").write_text(seed.stdout + seed.stderr)
        assert seed.returncode == -signal.SIGTERM, seed.stderr
        restored = await asyncio.to_thread(subprocess.run, [*command, "--mode", "restore"],
                                           capture_output=True, text=True, timeout=40, check=False)
        (OUTPUT / f"process-restore-{token}.txt").write_text(restored.stdout + restored.stderr)
        assert restored.returncode == 0, restored.stderr
        result = json.loads(Path(definition["restore_output"]).read_text())
        assert result["shape"] == [4, 1024]
        assert result["values_sha256"] == hashlib.sha256(expected.tobytes()).hexdigest()
        assert result["call_ids"] == [origin["id"]]
        assert result["usage"]["requests"] == []
        assert {row["request_id"] for row in result["usage"]["reused_requests"]} == {origin["id"]}
        assert float(result["usage"]["spent_cny"]) == float(result["usage"]["reserved_cny"]) == 0
        assert provider_state(shop) == before
        observe("process-recovery", {"definition": definition, "seed_returncode": seed.returncode,
            "restore_returncode": restored.returncode, "result": result,
            "provider_before": before, "provider_after": provider_state(shop)})
    finally:
        shop.dispose()


@pytest.mark.parametrize("mutation", ["response_digest", "request_digest", "status", "source", "filename"])
def test_real_receipt_corruption_fails_before_dispatch(tmp_path, mutation):
    source = Path(".local/data-center/graph") / FINGERPRINT / "embedding-results"
    paths = sorted(path for path in source.glob("*.json") if not path.name.startswith("."))
    if not paths:
        pytest.skip("receipt损坏检查需要已保存的真实completed结果")
    original = json.loads(paths[0].read_text())
    request = ModelInvocation.model_validate(original["invocation"])
    receipt = json.loads(paths[0].read_text())
    if mutation == "response_digest":
        receipt["response_sha256"] = "0" * 64
    elif mutation == "request_digest":
        receipt["invocation_sha256"] = "0" * 64
    elif mutation == "status":
        receipt["reply"]["status"] = "unknown"
    elif mutation == "source":
        receipt["invocation"]["source_ids"] = request.source_ids[:-1]
    name = "invalid.json" if mutation == "filename" else paths[0].name
    (tmp_path / name).write_text(json.dumps(receipt, ensure_ascii=False) + "\n")
    with pytest.raises((ValueError, PermissionError)):
        EmbeddingReceipts(tmp_path, FINGERPRINT, original["actor_id"], request.source_ids)


@pytest.mark.asyncio
async def test_current_source_change_and_withdrawal_reject_saved_graph_alias(persistence_live):
    state = persistence_live
    source_id = "project-shop/overall-interface/eceffd4"
    shop = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop")
    source = Table("data_center_sources", MetaData(), autoload_with=shop)
    query = state["query"].model_copy(update={"source_ids": sorted(state["query"].source_ids)})
    before_graph = saved_graph(state["database"])
    providers_before = provider_state(shop)
    with shop.connect() as connection:
        original = dict(connection.execute(select(source).where(source.c.source_id == source_id)).mappings().one())
    assert original["active"] is True
    try:
        unchanged = await state["documents"].graph_status(query, state["context"])
        assert unchanged["fingerprint"] == FINGERPRINT
        changed_properties = {**original["properties"], "persistence_check": str(uuid4())}
        with shop.begin() as connection:
            connection.execute(update(source).where(source.c.source_id == source_id)
                               .values(properties=changed_properties))
        changed = await state["documents"].graph_status(query, state["context"])
        assert changed["fingerprint"] != FINGERPRINT and changed["state"] == "absent"
        scope = await state["ports"].authorize(state["context"], query.source_ids, "read")
        with authorized_connection(state["database"], scope) as connection, Session(connection) as session:
            assert session.get(GraphIndexRow, FINGERPRINT) is None
        with shop.begin() as connection:
            connection.execute(update(source).where(source.c.source_id == source_id)
                               .values(properties=original["properties"], active=False))
        with pytest.raises(PermissionError):
            await state["documents"].graph_status(query, state["context"])
    finally:
        with shop.begin() as connection:
            connection.execute(update(source).where(source.c.source_id == source_id)
                               .values(properties=original["properties"], active=original["active"]))
    try:
        with shop.connect() as connection:
            restored = dict(connection.execute(select(source).where(source.c.source_id == source_id)).mappings().one())
        assert digest(restored) == digest(original)
        assert (await state["documents"].graph_status(query, state["context"])) == unchanged
        assert saved_graph(state["database"]) == before_graph
        assert provider_state(shop) == providers_before
        observe("source-permission-changes", {"source_id": source_id,
            "source_before_sha256": digest(original), "source_restored_sha256": digest(restored),
            "unchanged": unchanged, "changed_properties": changed, "withdrawal_rejected": True,
            "current_rls_rejects_old_index": True, "graph_before": before_graph,
            "graph_after": saved_graph(state["database"]), "providers_before": providers_before,
            "providers_after": provider_state(shop)})
    finally:
        shop.dispose()


@pytest.mark.asyncio
async def test_real_receipt_wrong_request_id_unknown_and_reserved_reject(persistence_live):
    state = persistence_live
    shop = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop")
    try:
        origin = completed_embedding(shop, state["task"].run_id)
        before = provider_state(shop)
        with shop.connect() as connection:
            different = connection.execute(text(
                "SELECT id FROM provider_requests WHERE run_id=:run_id AND status='completed' "
                "AND step_key LIKE 'graph-embedding:%' AND id<>:id ORDER BY id LIMIT 1"),
                {"run_id": state["task"].run_id, "id": origin["id"]}).scalar_one()
            unknown = dict(connection.execute(text(
                "SELECT * FROM provider_requests WHERE status='unknown' "
                "AND price_basis->>'verification_scope'='data_center' ORDER BY created_at LIMIT 1"
            )).mappings().one())
            reserved = dict(connection.execute(text(
                "SELECT * FROM provider_requests WHERE run_id=:run_id AND status='reserved' "
                "AND step_key LIKE 'graph-embedding:%' ORDER BY created_at LIMIT 1"
            ), {"run_id": state["task"].run_id}).mappings().one())
        task_id, run_id = str(uuid4()), str(uuid4())
        await state["ports"].bind(task_id, run_id, "build_graph", state["query"].source_ids,
                                   state["query"].model_dump(mode="json"), state["context"])
        request = ModelInvocation(task_id=task_id, run_id=run_id, kind="embedding",
            model="qwen3.7-text-embedding", source_ids=state["query"].source_ids,
            step_key=origin["step_key"], texts=origin["request_body"]["input"])
        wrong = ModelReply(request_id=different, status="completed", response=origin["response_body"])
        with pytest.raises(ValueError):
            await reuse_completed_step(state["ports"], request, wrong, state["context"])
        actual_unknown = ModelReply(request_id=unknown["id"], status="unknown", response=unknown["response_body"],
                                    reserved_cny=unknown["reservation_cny"])
        with pytest.raises(ValueError, match="completed真实响应"):
            await reuse_completed_step(state["ports"], request, actual_unknown, state["context"])
        pending_request = request.model_copy(update={"task_id": TASK_ID, "run_id": state["task"].run_id,
            "step_key": reserved["step_key"], "texts": reserved["request_body"]["input"]})
        completed = ModelReply(request_id=origin["id"], status="completed", response=origin["response_body"])
        with pytest.raises(ModelStepPending) as observed:
            await reuse_completed_step(state["ports"], pending_request, completed, state["context"])
        assert observed.value.reply.request_id == reserved["id"]
        assert observed.value.reply.status == "reserved"
        assert observed.value.reply.reserved_cny == reserved["reservation_cny"]
        assert provider_state(shop) == before
        usage = await state["ports"].usage(task_id, state["context"])
        assert usage["requests"] == usage["reused_requests"] == []
        observe("receipt-rejections", {"wrong_request_id": different, "origin_request_id": origin["id"],
            "unknown_request_id": unknown["id"], "reserved_reply": observed.value.reply.model_dump(mode="json"),
            "new_task_id": task_id, "new_run_id": run_id, "usage": usage,
            "providers_before": before, "providers_after": provider_state(shop)})
    finally:
        shop.dispose()
