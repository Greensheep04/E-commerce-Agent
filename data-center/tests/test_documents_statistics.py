import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest
from data_center_protocol import DataEvidenceRef, DocumentSearchResult, SourceRegistration
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_documents_live import published_snapshot, save_observation

from data_center.documents.models import DocumentChunkRow, DocumentRow

pytest_plugins = ["test_documents_live"]


@pytest.mark.asyncio
async def test_real_statistics_return_native_xlsx_rows_cells_and_ranges(live_documents):
    state = live_documents
    snapshot = await published_snapshot(state)
    with Session(state["database"]) as session:
        row = session.scalars(select(DocumentChunkRow).where(
            DocumentChunkRow.snapshot_id == snapshot.snapshot_id,
            DocumentChunkRow.record_id == "markitdown-workbook")).first()
        ref = DataEvidenceRef.model_validate(row.evidence)
        document = session.get(DocumentRow, (snapshot.snapshot_id, ref.source_id, ref.record_id))
        converted_before = json.loads(json.dumps(document.converted))
    usage_before = await state["ports"].usage(state["identity"]["task_id"], state["context"])
    result = await state["documents"].read_document_statistics(ref, state["context"])
    assert result["source_sha256"] == "a867be6ece38a4224bcfe34312e5c296ed520a76a431e0454603ceb171fbd4af"
    assert result["worksheet_reader"] == "openpyxl.load_workbook"
    assert result["workbook_scope"] == "complete_native_workbook"
    assert result["workbook_data_only"] is False
    assert result["worksheet_count"] == 2
    sheets = {item["worksheet"]: item for item in result["worksheets"]}
    sheet1 = sheets["Sheet1"]
    assert sheet1["cell_range"] == "A1:D24"
    assert sheet1["max_row"] == 24 and sheet1["max_column"] == 4
    assert [row["row_number"] for row in sheet1["rows"]] == list(range(1, 25))
    assert [cell["coordinate"] for cell in sheet1["rows"][0]["cells"]] == ["A1", "B1", "C1", "D1"]
    assert [cell["value"] for cell in sheet1["rows"][0]["cells"]] == ["Alpha", "Beta", "Gamma", "Delta"]
    assert [cell["value"] for cell in sheet1["rows"][1]["cells"]] == [89, 82, 100, 12]
    assert [cell["value"] for cell in sheet1["rows"][-1]["cells"]] == [87, 93, 95, 80]
    assert sheet1["rows"][0]["cell_range"] == "A1:D1"
    assert sheet1["rows"][1]["cell_range"] == "A2:D2"
    sheet2 = sheets["09060124-b5e7-4717-9d07-3c046eb"]
    assert sheet2["cell_range"] == "A1:D5" and sheet2["max_row"] == 5 and sheet2["max_column"] == 4
    assert [cell["coordinate"] for cell in sheet2["rows"][-1]["cells"]] == ["A5", "B5", "C5", "D5"]
    assert [cell["value"] for cell in sheet2["rows"][-1]["cells"]] == [
        13, 14, 15, "affc7dad-52dc-4b98-9b5d-51e65d8a8ad0"]
    assert sum(len(row["cells"]) for sheet in result["worksheets"] for row in sheet["rows"]) == 116
    repeated = await state["documents"].read_document_statistics(ref, state["context"])
    assert repeated == result
    assert await state["ports"].usage(state["identity"]["task_id"], state["context"]) == usage_before
    with Session(state["database"]) as session:
        assert session.get(DocumentRow, (snapshot.snapshot_id, ref.source_id, ref.record_id)).converted == converted_before
    with pytest.raises(ValueError):
        await state["documents"].read_document_statistics(ref.model_copy(update={"source_sha256": "0" * 64}),
                                                        state["context"])
    other = state["identities"]["actors"]["other"]
    other_context = state["context"].model_copy(update={"actor_id": other["actor_id"], "request_id": str(uuid4())})
    with pytest.raises(PermissionError):
        await state["documents"].read_document_statistics(ref, other_context)
    save_observation(state, "document-xlsx-native-statistics", {
        "milestone": "DC-03", "result": result, "credential_actor_mismatch_denied": True,
        "repeated_read_unchanged": True, "conversion_unchanged": True,
        "usage_before": usage_before, "usage_after": usage_before, "model_calls": 0})


@pytest.mark.asyncio
async def test_real_statistics_use_full_ocr_text_and_original_pptx(live_documents):
    state = live_documents
    snapshot = await published_snapshot(state)
    usage_before = await state["ports"].usage(state["identity"]["task_id"], state["context"])
    with Session(state["database"]) as session:
        rows = list(session.scalars(select(DocumentChunkRow).where(
            DocumentChunkRow.snapshot_id == snapshot.snapshot_id)))
        documents = list(session.scalars(select(DocumentRow).where(
            DocumentRow.snapshot_id == snapshot.snapshot_id)))
        references = {record_id: DataEvidenceRef.model_validate(next(row.evidence for row in rows
                      if row.record_id == record_id)) for record_id in (
                          "markitdown-slides", "tesseract-phototest-image", "tesseract-phototest-scan")}
        readme = next(row for row in documents if row.converted["input"]["title"] == "readme-v2.md")
        readme_reference = DataEvidenceRef.model_validate(next(row.evidence for row in rows
            if row.source_id == readme.source_id and row.record_id == readme.record_id))
        readme_text = readme.converted["text"]
    reference_path = Path(state["spec"]["fixed_inputs"]["ingest"]["path"]).parents[1] / "reference_documents.json"
    native_reference = json.loads(reference_path.read_text())
    native_text_path = Path(native_reference["raw_root"]) / "tesseract/phototest.txt"
    assert hashlib.sha256(native_text_path.read_bytes()).hexdigest() == (
        "76ef157baeb6ab275b7d90f310477a8060d5822aea2b2e05d480f459b016a6ed")
    expected_text = " ".join(native_text_path.read_text().split())
    results = {}
    for record_id, ref in references.items():
        result = await state["documents"].read_document_statistics(ref, state["context"])
        assert result["reference"] == ref.model_dump(mode="json")
        assert result["snapshot_id"] == snapshot.snapshot_id
        assert result["source_sha256"] == ref.source_sha256
        assert result["content_sha256"] == ref.content_sha256
        assert result["structured_sha256"] == ref.locator["structured_sha256"]
        assert result["normalization"] == "whitespace_collapsed"
        assert "str.split()" in result["normalization_rule"]
        if record_id == "markitdown-slides":
            assert result["page_count"] == 1
            assert result["page_count_method"] == "python-pptx.Presentation.slides"
            assert result["normalized_text"] == "Slide 1, Picture 1: Red square SVG"
        else:
            assert result["normalized_character_count"] == 284
            assert result["normalized_text"] == expected_text
            assert result["normalized_text"].count("The quick brown dog jumped over the lazy fox.") == 4
            assert "page_count" not in result
        altered = ref.model_copy(update={"source_sha256": "0" * 64})
        with pytest.raises(ValueError):
            await state["documents"].read_document_statistics(altered, state["context"])
        results[record_id] = result
    complete = await state["documents"].read_document_statistics(readme_reference, state["context"])
    assert complete["normalized_text"] == " ".join(readme_text.split())
    original_chunk = next(row for row in rows if row.chunk_id == readme_reference.chunk_id)
    assert complete["normalized_character_count"] > len(original_chunk.text)
    assert await state["ports"].usage(state["identity"]["task_id"], state["context"]) == usage_before
    save_observation(state, "document-statistics", {"results": results, "complete_readme": complete,
                                                   "native_reference": str(native_text_path),
                                                   "usage": usage_before, "model_calls": 0})


@pytest.mark.asyncio
async def test_real_statistics_keep_historical_revision_and_current_permissions(live_documents):
    state = live_documents
    history = state["identity"].get("history")
    if history is None:
        pytest.skip("历史统计检查需要已完成的独立来源修订证据")
    assert history["permission_reduced"] and history["source_withdrawn"]
    original = next(document for document in state["payload"].documents if document.title == "readme-v2.md")
    catalog = await state["ports"].manage("manage_sources", {}, state["context"])
    view = next(item for item in catalog["items"] if item["source_id"] == original.source_id)
    values = {key: view[key] for key in SourceRegistration.model_fields}
    values["source_id"] = history["source_id"]
    values["properties"] = {**view["properties"], "boundary_origin_source_id": original.source_id}
    values["resource"] = {**view["resource"], "resource_id": history["source_id"] + "/" + view["resource"]["resource_id"]}
    registration = SourceRegistration.model_validate(values)
    context = state["context"].model_copy(update={"request_id": str(uuid4())})
    first = DocumentSearchResult.model_validate_json(json.dumps(history["first_result"]))
    latest = DocumentSearchResult.model_validate_json(json.dumps(history["latest_result"]))
    first_ref, latest_ref = first.hits[0].chunk.ref, latest.hits[0].chunk.ref
    try:
        await state["ports"].manage("register_source", registration.model_dump(mode="json"), context)
        old_statistics = await state["documents"].read_document_statistics(first_ref, state["context"])
        new_statistics = await state["documents"].read_document_statistics(latest_ref, state["context"])
        assert old_statistics["source_revision"] == first_ref.source_revision
        assert new_statistics["source_revision"] == latest_ref.source_revision
        assert old_statistics["snapshot_id"] != new_statistics["snapshot_id"]
        assert "Python 3.10 or higher" in old_statistics["normalized_text"]
        assert "Python 3.10 through 3.14" in new_statistics["normalized_text"]
        reader = state["identities"]["actors"]["customer"]
        reader_context = await state["ports"].login(
            reader["username"], Path(reader["password_file"]).read_text().strip(), str(uuid4()))
        for ref in (first_ref, latest_ref):
            with pytest.raises(PermissionError):
                await state["documents"].read_document_statistics(ref, reader_context)
    finally:
        await state["ports"].manage("withdraw_source", {"source_id": history["source_id"]},
                                    state["context"].model_copy(update={"request_id": str(uuid4())}))
    for ref in (first_ref, latest_ref):
        with pytest.raises(PermissionError):
            await state["documents"].read_document_statistics(ref, state["context"])
    save_observation(state, "document-statistics-permissions", {"old": old_statistics, "new": new_statistics,
                                                               "permission_reduced": True,
                                                               "source_withdrawn": True, "model_calls": 0})
