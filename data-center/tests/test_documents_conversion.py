import hashlib
import json
from pathlib import Path

import pytest
from data_center_protocol import DocumentInput
from integration_protocol import ResourceRef

from data_center.documents.conversion import ConversionPolicy, convert_document, verify_converted

SOURCE_ROOT = Path("/Users/greensheep/.codex/worktrees/f4dc/industry-RAG")
MANIFEST = SOURCE_ROOT / "artifacts/raw/documents-v04/manifest.json"


def document_input(key):
    item = json.loads(MANIFEST.read_text())["documents"][key]
    path = Path(item["path"])
    payload = DocumentInput(
        source_id=item["source_id"], record_id=item["entity_id"],
        source_revision=item["source_revision"], source_nature=item["source_nature"],
        license_id=item["license_id"], title=path.name,
        resource=ResourceRef(module_id="industry_documents", resource_type="document",
                             resource_id=path.name, version=item["source_revision"],
                             sha256=item["source_sha256"]),
    )
    return payload, path.read_bytes()


@pytest.mark.parametrize("key,expected", [
    ("readme-v1", "pip install"), ("docx", "550"),
    ("xlsx", "09060124-b5e7-4717-9d07-3c046eb"), ("pptx", "Red square SVG"),
])
def test_fixed_native_conversion_and_canonical_line_readback(tmp_path, key, expected):
    payload, raw = document_input(key)
    converted = convert_document(payload, raw, tmp_path, ConversionPolicy())
    verify_converted(converted)
    assert expected in converted.text
    assert converted.source_sha256 == hashlib.sha256(raw).hexdigest()
    lines = converted.text.splitlines(keepends=True)
    assert converted.chunks
    if key == "xlsx":
        assert all(value in converted.text for value in ("Alpha", "Beta", "Gamma", "Delta"))
        assert all(len(chunk.locator["worksheets"]) == 1 for chunk in converted.chunks)
    for chunk in converted.chunks:
        assert chunk.text == "".join(lines[chunk.start_line - 1:chunk.end_line])
        assert chunk.text_sha256 == hashlib.sha256(chunk.text.encode()).hexdigest()
        assert chunk.locator["doc_items"]
    assert convert_document(payload, raw, tmp_path, ConversionPolicy()) == converted


def test_corrupt_original_rejects_before_conversion(tmp_path):
    payload, raw = document_input("readme-v1")
    with pytest.raises(ValueError, match="原件 SHA-256"):
        convert_document(payload, raw + b"changed", tmp_path, ConversionPolicy())


@pytest.mark.parametrize("key", ["pdf", "image", "scan"])
def test_pdf_and_ocr_against_fixed_original_truth(tmp_path, key):
    payload, raw = document_input(key)
    policy = ConversionPolicy(artifacts_path=Path.cwd() / ".local/data-center/models/docling",
                              tesseract_command=str(SOURCE_ROOT / ".local/ocr_v04/bin/tesseract"))
    converted = convert_document(payload, raw, tmp_path, policy)
    verify_converted(converted)
    if key == "pdf":
        for expected in ("SPARSE-2024-INV-1234", "4,287.50", "97.2%", "SKU-4563", "-24"):
            assert expected in converted.text
    else:
        expected = (SOURCE_ROOT / "artifacts/raw/documents-v04/tesseract/phototest.txt").read_text()
        assert " ".join(converted.text.split()) == " ".join(expected.split())
    assert any(item["provenance"] for chunk in converted.chunks for item in chunk.locator["doc_items"])
