import asyncio
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from data_center_protocol import (
    DataEvidenceRef,
    DataQuery,
    DataSnapshot,
    DocumentIngest,
    DocumentSearchResult,
    SourceRegistration,
)
from pylatexenc.latex2text import LatexNodes2Text
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from data_center.database import authorized_connection
from data_center.documents.conversion import ConversionPolicy, identity_hash
from data_center.documents.engine import DocumentEngine
from data_center.documents.models import DocumentChunkRow, GraphIndexRow
from data_center.graph.engine import GraphConfig, GraphEngine
from data_center.ports import ShopPorts


@pytest_asyncio.fixture
async def live_documents():
    spec_path = os.environ.get("DC03_LIVE_SPEC")
    if spec_path is None:
        pytest.skip("真实服务检查需要明确提供固定输入与费用窗口")
    spec = json.loads(Path(spec_path).read_text())
    if os.environ.get("TIKTOKEN_CACHE_DIR") != spec["tokenizer_cache"]:
        raise ValueError("真实检查的 tokenizer 缓存目录与配置不一致")
    if spec["paid_window"] != "dc-live-20261004":
        raise ValueError("真实模型检查没有关联已批准费用窗口")
    for item in spec["fixed_inputs"].values():
        if hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError("真实检查的固定输入摘要发生变化")
    output = Path(spec["output_dir"])
    output.mkdir(parents=True, exist_ok=True)
    identity_path = output / "identity.json"
    digest = hashlib.sha256(Path(spec_path).read_bytes()).hexdigest()
    if identity_path.exists():
        identity = json.loads(identity_path.read_text())
        if identity["spec_sha256"] != digest:
            raise ValueError("恢复检查的固定输入发生变化")
    else:
        identity = {"spec_sha256": digest, "task_id": str(uuid4()), "run_id": str(uuid4()),
                    "ingest_request_id": str(uuid4()), "queries": {}}
        identity_path.write_text(json.dumps(identity, indent=2) + "\n")
    identities = json.loads(Path(spec["identities_path"]).read_text())
    if identities["window"] != spec["paid_window"]:
        raise ValueError("宿主身份登记没有开启所需费用窗口")
    admin = identities["actors"]["admin"]
    ports = ShopPorts(spec["host_url"])
    database = create_engine(spec["database_url"])
    context = await ports.login(admin["username"], Path(admin["password_file"]).read_text().strip(),
                                identity["ingest_request_id"])
    payload = DocumentIngest.model_validate(spec["ingest"])
    policy = ConversionPolicy(artifacts_path=Path(spec["docling_models"]),
                              tokenizer_cache=Path(spec["tokenizer_cache"]),
                              tesseract_command=spec["tesseract_command"])
    documents = DocumentEngine(database, Path(spec["artifact_root"]), ports, ports, ports,
                               conversion_policy=policy, graph_config=GraphConfig(enabled=True))
    state = {"spec": spec, "identity": identity, "identity_path": identity_path,
             "ports": ports, "database": database, "context": context, "payload": payload,
             "documents": documents, "output": output, "identities": identities}
    source_roots = (Path("data-center/src"), Path("packages/data-center-protocol"),
                    Path("packages/integration-protocol"))
    source_files = [path for root in source_roots for path in root.rglob("*.py")]
    source_files.extend(Path("src/shop").glob("data_center*.py"))
    source_files.append(Path(__file__))
    save_observation(state, "environment", {
        "milestone": "DC-03", "python": sys.version, "spec_sha256": digest,
        "source_sha256": {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in sorted(source_files)},
        "packages": {name: version(name) for name in (
            "data-center", "data-center-protocol", "integration-protocol", "docling", "docling-core",
            "lightrag-hku", "sqlalchemy", "psycopg", "pgvector", "tiktoken", "httpx", "pylatexenc")},
        "fixed_inputs": spec["fixed_inputs"], "conversion_policy": policy.model_dump(mode="json"),
        "graph_config": GraphConfig(enabled=True).model_dump(mode="json"),
        "runtime_configuration": {key: spec[key] for key in (
            "host_url", "database_url", "artifact_root", "docling_models", "tokenizer_cache",
            "tesseract_command", "paid_window")},
    })
    try:
        yield state
    finally:
        await ports.close()
        database.dispose()


def save_observation(state, name, value):
    target = state["output"] / f"{name}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    target.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def has_expected_text(text, expected):
    equivalent = LatexNodes2Text(strict_latex_spaces=True).latex_to_text(text)
    return all(value in text or value in equivalent for value in expected)


async def published_snapshot(state):
    identity, payload, ports, context = (state[key] for key in ("identity", "payload", "ports", "context"))
    sources = sorted({item.source_id for item in payload.documents})
    await ports.bind(identity["task_id"], identity["run_id"], "ingest", sources,
                     payload.model_dump(mode="json"), context)
    snapshot = await state["documents"].ingest(payload, context, task_id=identity["task_id"],
                                              run_id=identity["run_id"])
    usage = await ports.usage(identity["task_id"], context)
    save_observation(state, "publication", {"snapshot": snapshot.model_dump(mode="json"), "usage": usage})
    assert snapshot.state == "published"
    assert snapshot.record_count == len(payload.documents)
    return snapshot


async def run_query(state, key, query, actor_context=None):
    identity = state["identity"]
    entry = identity["queries"].get(key)
    query_digest = identity_hash(query.model_dump(mode="json"))
    if entry is None:
        entry = {"task_id": str(uuid4()), "run_id": str(uuid4()), "request_id": str(uuid4()),
                 "query_sha256": query_digest}
        identity["queries"][key] = entry
        state["identity_path"].write_text(json.dumps(identity, indent=2) + "\n")
    if entry["query_sha256"] != query_digest:
        raise ValueError("恢复检查的查询输入发生变化")
    context = (actor_context or state["context"]).model_copy(update={"request_id": entry["request_id"]})
    await state["ports"].bind(entry["task_id"], entry["run_id"], "query", query.source_ids,
                             query.model_dump(mode="json"), context)
    usage_before = await state["ports"].usage(entry["task_id"], context)
    graph_before = await state["documents"].graph_status(query, context) if query.retrieval_mode == "graph" else None
    started = datetime.now(UTC)
    result = await state["documents"].search(query, context, task_id=entry["task_id"], run_id=entry["run_id"])
    elapsed = (datetime.now(UTC) - started).total_seconds()
    usage_after = await state["ports"].usage(entry["task_id"], context)
    save_observation(state, "query", {"key": key, "query": query.model_dump(mode="json"),
                                      "result": result.model_dump(mode="json"),
                                      "elapsed_seconds": elapsed,
                                      "usage_before": usage_before, "usage_after": usage_after,
                                      "graph_before": graph_before,
                                      "task_id": entry["task_id"], "run_id": entry["run_id"]})
    return result, context, entry


def save_identity(state):
    state["identity_path"].write_text(json.dumps(state["identity"], indent=2) + "\n")


def operation_identity(state, key, payload):
    entries = state["identity"].setdefault("operations", {})
    digest = identity_hash(payload)
    if key not in entries:
        entries[key] = {"task_id": str(uuid4()), "run_id": str(uuid4()), "request_id": str(uuid4()),
                        "payload_sha256": digest}
        save_identity(state)
    entry = entries[key]
    if entry["payload_sha256"] != digest:
        raise ValueError("恢复边界检查的操作输入发生变化")
    return entry


async def manage_source(state, key, operation, payload):
    entry = operation_identity(state, key, payload)
    context = state["context"].model_copy(update={"request_id": entry["request_id"]})
    result = await state["ports"].manage(operation, payload, context)
    save_observation(state, "management", {"key": key, "operation": operation,
                                           "payload": payload, "result": result})
    return result


async def publish_history_revision(state, key, registration, original):
    document = original.model_copy(update={"source_id": registration.source_id,
                                          "resource": registration.resource})
    payload = DocumentIngest(documents=[document], graph_enabled=True)
    body = payload.model_dump(mode="json")
    entry = operation_identity(state, key, body)
    context = state["context"].model_copy(update={"request_id": entry["request_id"]})
    await state["ports"].bind(entry["task_id"], entry["run_id"], "ingest", [document.source_id], body, context)
    snapshot = await state["documents"].ingest(payload, context, task_id=entry["task_id"], run_id=entry["run_id"])
    save_observation(state, "history-publication", {"key": key, "snapshot": snapshot.model_dump(mode="json"),
                                                   "usage": await state["ports"].usage(entry["task_id"], context)})
    assert snapshot.state == "published"
    return snapshot


@pytest.mark.asyncio
async def test_real_publication_pgvector_reuse_conflicts_and_evidence(live_documents):
    state = live_documents
    snapshot = await published_snapshot(state)
    initial_usage = await state["ports"].usage(state["identity"]["task_id"], state["context"])
    repeated = await published_snapshot(state)
    assert repeated == snapshot
    assert await state["ports"].usage(state["identity"]["task_id"], state["context"]) == initial_usage
    scope = await state["ports"].authorize(state["context"], snapshot.source_ids, "read")
    with authorized_connection(state["database"], scope, snapshot.snapshot_id) as connection, Session(connection) as session:
        rows = list(session.scalars(select(DocumentChunkRow).where(DocumentChunkRow.snapshot_id == snapshot.snapshot_id)))
        assert rows
        assert all(row.embedding is not None and len(row.embedding) == 1024 and row.embedding_call_id for row in rows)
        references = [DataEvidenceRef.model_validate(row.evidence) for row in rows]
        call_ids = sorted({row.embedding_call_id for row in rows})
    for ref in references:
        chunk = await state["documents"].read_evidence(ref, state["context"])
        assert hashlib.sha256(chunk.text.encode()).hexdigest() == chunk.text_sha256
    save_observation(state, "index", {"snapshot_id": snapshot.snapshot_id, "chunks": len(references),
                                      "embedding_call_ids": call_ids})
    quality = []
    for item in state["spec"]["queries"]:
        query = DataQuery.model_validate_json(json.dumps(item["query"]))
        assert query.intent == "documents" and query.retrieval_mode == "hybrid"
        query = query.model_copy(update={"snapshot_id": snapshot.snapshot_id})
        result, context, entry = await run_query(state, f"hybrid-{item['case_id']}", query)
        assert result.status == "completed"
        assert result.hits and result.call_ids
        assert result.metadata["retrieval_mode"] == "hybrid"
        assert all(hit.vector_distance is not None and hit.rrf_score is not None for hit in result.hits)
        text = "\n".join(hit.chunk.text for hit in result.hits)
        quality.append({"case_id": item["case_id"], "expected_text": item["expected_text"],
                        "missing": [value for value in item["expected_text"] if not has_expected_text(text, [value])]})
        usage_before = await state["ports"].usage(entry["task_id"], context)
        repeated = await state["documents"].search(query, context, task_id=entry["task_id"], run_id=entry["run_id"])
        assert repeated == result
        assert await state["ports"].usage(entry["task_id"], context) == usage_before
        with pytest.raises(ValueError, match="相同文档请求编号"):
            await state["documents"].search(query.model_copy(update={"question": query.question + " additional"}),
                                            context, task_id=entry["task_id"], run_id=entry["run_id"])
        original = result.hits[0].chunk.ref
        with pytest.raises(ValueError, match="身份或位置"):
            await state["documents"].read_evidence(original.model_copy(update={"source_sha256": "0" * 64}), context)
    save_observation(state, "hybrid-quality", {"cases": quality, "snapshot_id": snapshot.snapshot_id})
    assert all(not item["missing"] for item in quality), quality


@pytest.mark.asyncio
async def test_real_graph_with_original_evidence_and_same_corpus_rrf(live_documents):
    state = live_documents
    snapshot = await published_snapshot(state)
    config_id = identity_hash(state["documents"].graph_config.model_dump(mode="json"))[:12]
    quality = []
    for item in state["spec"]["graph_queries"]:
        query = DataQuery.model_validate_json(json.dumps(item["query"]))
        assert query.intent == "documents" and query.retrieval_mode == "graph"
        query = query.model_copy(update={"snapshot_id": snapshot.snapshot_id})
        query_key = f"graph-{config_id}-{item['case_id']}"
        original_entry = state["identity"]["queries"].get(query_key)
        if original_entry is not None:
            original_context = state["context"].model_copy(update={"request_id": original_entry["request_id"]})
            usage = await state["ports"].usage(original_entry["task_id"], original_context)
            if any(value["status"] == "unknown" for value in usage["requests"]):
                quality.append({"case_id": item["case_id"], "status": "unknown", "missing": item["expected_text"]})
                save_observation(state, "graph-unknown-preserved", {"case_id": item["case_id"],
                                                                    "identity": original_entry, "usage": usage})
                continue
        graph, context, entry = await run_query(state, query_key, query)
        if graph.status != "completed":
            quality.append({"case_id": item["case_id"], "status": graph.status,
                            "missing": item["expected_text"]})
            continue
        assert graph.hits and graph.call_ids
        assert graph.metadata["retrieval_mode"] == "graph"
        assert graph.metadata["graph_evidence"]
        text = "\n".join(hit.chunk.text for hit in graph.hits)
        equivalent = LatexNodes2Text(strict_latex_spaces=True).latex_to_text(text)
        quality.append({"case_id": item["case_id"], "status": graph.status,
                        "missing": [value for value in item["expected_text"]
                                    if value not in text and value not in equivalent]})
        for hit in graph.hits:
            assert await state["documents"].read_evidence(hit.chunk.ref, context) == hit.chunk
        usage_before = await state["ports"].usage(entry["task_id"], context)
        repeated = await state["documents"].search(query, context,
                                                  task_id=entry["task_id"], run_id=entry["run_id"])
        assert repeated == graph
        assert await state["ports"].usage(entry["task_id"], context) == usage_before
        hybrid, hybrid_context, hybrid_entry = await run_query(
            state, f"hybrid-{item['case_id']}", query.model_copy(update={"retrieval_mode": "hybrid"}))
        assert hybrid.status == "completed"
        assert [value.snapshot_id for value in graph.snapshots] == [value.snapshot_id for value in hybrid.snapshots]
        save_observation(state, "comparison", {"case_id": item["case_id"], "question": query.question,
                                               "graph": graph.model_dump(mode="json"),
                                               "hybrid": hybrid.model_dump(mode="json"),
                                               "graph_usage": await state["ports"].usage(entry["task_id"], context),
                                               "hybrid_usage": await state["ports"].usage(hybrid_entry["task_id"], hybrid_context)})
    save_observation(state, "graph-quality", {"config_id": config_id, "cases": quality,
                                              "snapshot_id": snapshot.snapshot_id})
    assert all(item["status"] == "completed" and not item["missing"] for item in quality), quality


@pytest.mark.asyncio
async def test_real_source_revision_request_binding_and_permission_readback(live_documents):
    state = live_documents
    if "history" not in state["identity"]:
        state["identity"]["history"] = {"source_id": "dc03-history/" + uuid4().hex}
        save_identity(state)
    history = state["identity"]["history"]
    original_documents = {item.title: item for item in state["payload"].documents
                          if item.record_id == "markitdown-readme"}
    catalog = await state["ports"].manage("manage_sources", {}, state["context"])
    views = {item["source_id"]: item for item in catalog["items"]}
    registrations = {}
    for title in ("readme-v1.md", "readme-v2.md"):
        original = original_documents[title]
        view = views[original.source_id]
        values = {key: view[key] for key in SourceRegistration.model_fields}
        values["source_id"] = history["source_id"]
        values["properties"] = {**view["properties"], "boundary_origin_source_id": original.source_id}
        values["resource"] = {**view["resource"], "resource_id": (
            history["source_id"] + "/" + view["resource"]["resource_id"])}
        registrations[title] = SourceRegistration.model_validate(values)
    reader = state["identities"]["actors"]["customer"]
    login_entry = operation_identity(state, "history-reader-login", {"actor_id": reader["actor_id"]})
    reader_context = await state["ports"].login(
        reader["username"], Path(reader["password_file"]).read_text().strip(), login_entry["request_id"])
    query = DataQuery(question="MarkItDown README 的 Python 版本要求与 pip 安装方式是什么？",
                      intent="documents", source_ids=[history["source_id"]], limit=100)
    if "first_result" not in history:
        old = registrations["readme-v1.md"]
        await manage_source(state, "history-register-v1", "register_source", old.model_dump(mode="json"))
        await manage_source(state, "history-grant-record", "set_grant", {
            "actor_id": reader_context.actor_id, "source_id": history["source_id"],
            "permissions": ["read"], "asset_ids": ["markitdown-readme"], "source_wide": False})
        first_snapshot = await publish_history_revision(state, "history-publish-v1", old,
                                                        original_documents["readme-v1.md"])
        first, _, _ = await run_query(state, "history-original", query, reader_context)
        assert first.status == "completed"
        assert has_expected_text("\n".join(hit.chunk.text for hit in first.hits), ["Python 3.10 or higher"])
        history["first_snapshot"] = first_snapshot.model_dump(mode="json")
        history["first_result"] = first.model_dump(mode="json")
        save_identity(state)
    first_snapshot = DataSnapshot.model_validate_json(json.dumps(history["first_snapshot"]))
    first = DocumentSearchResult.model_validate_json(json.dumps(history["first_result"]))
    query_entry = state["identity"]["queries"]["history-original"]
    query_context = reader_context.model_copy(update={"request_id": query_entry["request_id"]})
    current = registrations["readme-v2.md"]
    if "second_snapshot" not in history:
        await manage_source(state, "history-register-v2", "register_source", current.model_dump(mode="json"))
        second_snapshot = await publish_history_revision(state, "history-publish-v2", current,
                                                         original_documents["readme-v2.md"])
        history["second_snapshot"] = second_snapshot.model_dump(mode="json")
        save_identity(state)
    elif not history.get("binding_verified"):
        old = registrations["readme-v1.md"]
        await manage_source(state, "history-baseline-readback", "register_source", old.model_dump(mode="json"))
        baseline = await state["documents"].search(
            query, query_context, task_id=query_entry["task_id"], run_id=query_entry["run_id"])
        assert baseline == first
        await manage_source(state, "history-current-readback", "register_source", current.model_dump(mode="json"))
    second_snapshot = DataSnapshot.model_validate_json(json.dumps(history["second_snapshot"]))
    assert second_snapshot.snapshot_id != first_snapshot.snapshot_id
    if not history.get("binding_verified"):
        usage_before = await state["ports"].usage(query_entry["task_id"], query_context)
        repeated = await state["documents"].search(
            query, query_context, task_id=query_entry["task_id"], run_id=query_entry["run_id"])
        assert repeated == first
        assert await state["ports"].usage(query_entry["task_id"], query_context) == usage_before
        assert all(hit.chunk.ref.source_revision == original_documents["readme-v1.md"].source_revision
                   for hit in repeated.hits)
        history["binding_verified"] = True
        save_identity(state)
        save_observation(state, "history-binding", {"original": first.model_dump(mode="json"),
                                                   "repeated": repeated.model_dump(mode="json"),
                                                   "current_snapshot": second_snapshot.model_dump(mode="json"),
                                                   "usage": usage_before})
    if "latest_result" not in history:
        latest, _, _ = await run_query(state, "history-latest", query, reader_context)
        assert latest.status == "completed"
        assert [item.snapshot_id for item in latest.snapshots] == [second_snapshot.snapshot_id]
        assert has_expected_text("\n".join(hit.chunk.text for hit in latest.hits), ["Python 3.10 through 3.14"])
        history["latest_result"] = latest.model_dump(mode="json")
        save_identity(state)
    graph_query = query.model_copy(update={"retrieval_mode": "graph", "snapshot_id": first_snapshot.snapshot_id})
    if "graph_result" not in history:
        graph, _, _ = await run_query(state, "history-graph", graph_query, reader_context)
        assert graph.status == "completed" and graph.metadata["graph_evidence"]
        for hit in graph.hits:
            assert await state["documents"].read_evidence(hit.chunk.ref, query_context) == hit.chunk
        history["graph_result"] = graph.model_dump(mode="json")
        save_identity(state)
    graph_entry = state["identity"]["queries"]["history-graph"]
    graph_context = reader_context.model_copy(update={"request_id": graph_entry["request_id"]})
    usage_before = {"hybrid": await state["ports"].usage(query_entry["task_id"], query_context),
                    "graph": await state["ports"].usage(graph_entry["task_id"], graph_context)}
    if not history.get("permission_reduced"):
        await manage_source(state, "history-grant-empty", "set_grant", {
            "actor_id": reader_context.actor_id, "source_id": history["source_id"],
            "permissions": ["read"], "asset_ids": [], "source_wide": False})
        history["permission_reduced"] = True
        save_identity(state)
    for value, context, entry in ((query, query_context, query_entry),
                                  (graph_query, graph_context, graph_entry)):
        with pytest.raises(PermissionError):
            await state["documents"].search(value, context, task_id=entry["task_id"], run_id=entry["run_id"])
    for hit in first.hits:
        with pytest.raises(PermissionError):
            await state["documents"].read_evidence(hit.chunk.ref, query_context)
    graph = DocumentSearchResult.model_validate_json(json.dumps(history["graph_result"]))
    for hit in graph.hits:
        with pytest.raises(PermissionError):
            await state["documents"].read_evidence(hit.chunk.ref, graph_context)
    assert await state["ports"].usage(query_entry["task_id"], query_context) == usage_before["hybrid"]
    assert await state["ports"].usage(graph_entry["task_id"], graph_context) == usage_before["graph"]
    if not history.get("source_withdrawn"):
        for hit in first.hits:
            assert await state["documents"].read_evidence(hit.chunk.ref, state["context"]) == hit.chunk
        await manage_source(state, "history-withdraw", "withdraw_source", {"source_id": history["source_id"]})
        history["source_withdrawn"] = True
        save_identity(state)
    for hit in first.hits:
        with pytest.raises(PermissionError):
            await state["documents"].read_evidence(hit.chunk.ref, state["context"])
    save_observation(state, "history-permissions", {"original_snapshot": first_snapshot.snapshot_id,
                                                   "current_snapshot": second_snapshot.snapshot_id,
                                                   "graph_reference_count": len(graph.metadata["graph_evidence"]),
                                                   "usage": usage_before, "history": history})


@pytest.mark.asyncio
async def test_real_graph_configuration_failure_and_actual_cancellation_save_state(live_documents):
    state = live_documents
    snapshot = await published_snapshot(state)
    definition = next(item for item in state["spec"]["graph_queries"] if item["case_id"] == "REL-03")
    query = DataQuery.model_validate_json(json.dumps(definition["query"])).model_copy(
        update={"snapshot_id": snapshot.snapshot_id})
    scope, snapshots, rows = await state["documents"]._visible(query, state["context"])
    usage_before = await state["ports"].usage(state["identity"]["task_id"], state["context"])
    failure = GraphEngine(state["documents"], GraphConfig(enabled=True, entity_extract_max_gleaning=1))
    fingerprint = failure._identity(scope, snapshots, rows)
    original_port = os.environ["POSTGRES_PORT"]
    try:
        os.environ["POSTGRES_PORT"] = "55433"
        with pytest.raises(ValueError, match="PostgreSQL配置"):
            await failure.build(query, scope, snapshots, rows, state["context"],
                                state["identity"]["task_id"], state["identity"]["run_id"])
    finally:
        os.environ["POSTGRES_PORT"] = original_port
    with Session(state["database"]) as session:
        failed = session.get(GraphIndexRow, fingerprint)
        assert failed.state == "failed" and failed.call_ids == []
    cancelled = GraphEngine(state["documents"], GraphConfig(enabled=True, entity_extract_max_gleaning=2))
    cancelled_id = cancelled._identity(scope, snapshots, rows)
    operation = asyncio.create_task(cancelled.build(query, scope, snapshots, rows, state["context"],
                                                   state["identity"]["task_id"], state["identity"]["run_id"]))
    for _ in range(1000):
        with Session(state["database"]) as session:
            present = session.get(GraphIndexRow, cancelled_id)
            if present is not None and present.state == "staged":
                break
        await asyncio.sleep(0.005)
    else:
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        pytest.fail("图构建没有进入可观察的staged状态")
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    with Session(state["database"]) as session:
        stopped = session.get(GraphIndexRow, cancelled_id)
        assert stopped.state == "cancelled" and stopped.call_ids == []
    assert await state["ports"].usage(state["identity"]["task_id"], state["context"]) == usage_before
    save_observation(state, "graph-failure-cancellation", {
        "configuration_failure": {"fingerprint": fingerprint, "state": "failed"},
        "actual_cancellation": {"fingerprint": cancelled_id, "state": "cancelled"},
        "usage": usage_before, "model_calls": 0})
