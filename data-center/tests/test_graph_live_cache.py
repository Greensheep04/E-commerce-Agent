import copy
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from data_center_protocol import DataQuery, DocumentSearchResult
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_documents_live import published_snapshot

from data_center.documents.models import DocumentChunkRow, DocumentRequestRow, GraphChunkRefRow
from data_center.graph.engine import GraphEngine

pytest_plugins = ["test_documents_live"]


@pytest.mark.asyncio
async def test_real_graph_cache_reference_order_and_integrity(live_documents):
    state = live_documents
    snapshot = await published_snapshot(state)
    definition = next(item for item in state["spec"]["graph_queries"] if item["case_id"] == "REL-05")
    query = DataQuery.model_validate(definition["query"]).model_copy(update={"snapshot_id": snapshot.snapshot_id})
    entry = state["identity"]["queries"]["graph-fc7cfbaf09de-REL-05"]
    context = state["context"].model_copy(update={"request_id": entry["request_id"]})
    key = (context.actor_id, context.request_id, "search")
    with Session(state["database"]) as session:
        original_envelope = copy.deepcopy(session.get(DocumentRequestRow, key).result_payload)
        cached = DocumentSearchResult.model_validate_json(json.dumps(original_envelope["result"]))
        references = list(session.scalars(select(GraphChunkRefRow).where(
            GraphChunkRefRow.fingerprint == cached.metadata["fingerprint"])))
        originals = {(item.snapshot_id, item.chunk_id): item.text_sha256 for item in references}
        additional = session.scalars(select(DocumentChunkRow).where(
            DocumentChunkRow.snapshot_id == snapshot.snapshot_id,
            DocumentChunkRow.source_id.not_in(query.source_ids))).first()
        added_reference = [additional.snapshot_id, additional.chunk_id]
    assert cached.status == "completed" and cached.metadata["graph_evidence"]
    scope, snapshots, rows = await state["documents"]._visible(query, context)
    assert {item.snapshot_id for item in snapshots} == {snapshot.snapshot_id}
    assert all(originals[(row.snapshot_id, row.chunk_id)] == row.text_sha256 for row in rows)
    assert len(rows) == 2 and rows[0].text_sha256 == rows[1].text_sha256
    before = await state["ports"].usage(entry["task_id"], context)
    graph = GraphEngine(state["documents"], state["documents"].graph_config)
    canonical = {name: sorted(values) for name, values in cached.metadata["graph_evidence"].items()}
    for reverse in (False, True):
        metadata = copy.deepcopy(cached.metadata)
        metadata["graph_evidence"] = {name: sorted(values, reverse=reverse) for name, values in canonical.items()}
        await graph.validate_cached(cached.model_copy(update={"metadata": metadata}), rows, context)
    sample = next(name for name, values in canonical.items() if len(values) == 2)
    mutations = {
        "missing": canonical[sample][:-1],
        "extra": [*canonical[sample], added_reference],
        "duplicate": [*canonical[sample], canonical[sample][0]],
    }
    rejected = []
    for name, values in mutations.items():
        metadata = copy.deepcopy(cached.metadata)
        metadata["graph_evidence"][sample] = values
        with pytest.raises(ValueError, match="原图关系引用与持久映射不一致"):
            await graph.validate_cached(cached.model_copy(update={"metadata": metadata}), rows, context)
        rejected.append(name)
    metadata = copy.deepcopy(cached.metadata)
    metadata["graph"]["chunks"][0]["content"] += " changed"
    with pytest.raises(ValueError, match="原图片段与不可变原文不一致"):
        await graph.validate_cached(cached.model_copy(update={"metadata": metadata}), rows, context)
    rejected.append("changed_content")
    original_result = await state["documents"].search(query, context,
                                                     task_id=entry["task_id"], run_id=entry["run_id"])
    assert original_result == cached
    assert await state["ports"].usage(entry["task_id"], context) == before
    with Session(state["database"]) as session:
        assert session.get(DocumentRequestRow, key).result_payload == original_envelope
    checks = {"order_accepts": True, "missing_denied": "missing" in rejected,
              "extra_denied": "extra" in rejected, "duplicate_denied": "duplicate" in rejected,
              "content_change_denied": "changed_content" in rejected,
              "source_mapping_sha256_match": True, "zero_new_model_calls": True,
              "original_cache_unchanged": True}
    source_paths = [Path("data-center/src/data_center/graph/engine.py"),
                    Path("data-center/src/data_center/documents/engine.py")]
    captured = datetime.now(UTC)
    report = {
        "schema": "dc03.graph_cache_integrity.v1", "status": "passed",
        "captured_at": captured.isoformat(), "checks": checks, "check_ids": list(checks),
        "source_sha256": {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in source_paths},
        "test_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "spec_sha256": state["identity"]["spec_sha256"],
        "milestone": "DC-03", "identity": entry, "fingerprint": cached.metadata["fingerprint"],
        "scope": scope.model_dump(mode="json"), "ordering_accepted": ["ascending", "descending"],
        "rejected": rejected, "usage_before": before, "usage_after": before, "new_model_calls": 0,
        "source_rows": [{"snapshot_id": row.snapshot_id, "chunk_id": row.chunk_id,
                         "source_id": row.source_id, "text_sha256": row.text_sha256,
                         "evidence": row.evidence} for row in rows],
    }
    output = Path(".scratch/evidence/dc03-cache-mapping-20261004-r1")
    output.mkdir(parents=True, exist_ok=True)
    suffix = captured.strftime("%Y%m%dT%H%M%S%fZ")
    target = output / f"graph-cache-integrity-{suffix}.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    manifest = {"schema": "dc03.graph_cache_integrity.manifest.v1", "status": "passed",
                "milestone": "DC-03", "report_path": str(target.resolve()),
                "report_file_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                "source_sha256": report["source_sha256"], "check_ids": list(checks)}
    (output / f"manifest-{suffix}.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
