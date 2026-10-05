import json
from hashlib import sha256
from pathlib import Path

import pytest

from scripts import freeze_data_center_inputs
from scripts.freeze_data_center_inputs import (
    PROJECT_ROOT,
    build_supplemental_document_sources,
    evidence_artifact_reference,
)


def test_generated_document_keeps_original_generation_provenance():
    root = Path("/Users/greensheep/.codex/worktrees/f4dc/industry-RAG")
    manifest_path = root / "artifacts/raw/documents-v04/manifest.json"
    manifest = json.loads(manifest_path.read_text())
    reference = json.loads((root / "artifacts/verification/v04/fixed/reference_documents.json").read_text())
    freeze_data_center_inputs.FIXTURE = json.loads(
        (PROJECT_ROOT / "tests/fixtures/data_center_cases.json").read_text())
    sources = freeze_data_center_inputs.build_document_sources(root, reference, manifest)
    by_key = {source["document_key"]: source for source in sources}
    generation = by_key["scan"].get("generation")
    assert generation is not None
    assert generation["program"] == manifest["generated"]["scan"]["program"]
    assert generation["source_sha256"] == by_key["image"]["source_sha256"]
    assert generation["source_resource"] == by_key["image"]["resource"]
    assert generation["output_sha256"] == by_key["scan"]["source_sha256"]
    assert generation["manifest_sha256"] == sha256(manifest_path.read_bytes()).hexdigest()
    registrations = freeze_data_center_inputs.build_host_sources(
        freeze_data_center_inputs.FIXTURE, sources)
    registered = next(source for source in registrations["sources"]
                      if source["source_id"] == by_key["scan"]["source_id"])
    assert registered["properties"]["generation"] == generation


def test_supplemental_resource_id_can_follow_container_filename():
    fixture = {
        "supplemental_document_sources": [{
            "source_key": "adventureworks:schema-document",
            "upstream_source_id": "microsoft/adventureworks",
            "record_id": "adventureworks-oltp-schema",
            "revision": "beaab06ef72831089ca80e5355d65e661fd19b26",
            "original_filename": "instawdb.sql",
            "original_path": ".local/assessment/source/instawdb.sql",
            "resource_path": ".local/assessment/source/instawdb.sql.md",
            "title": "instawdb.sql",
            "path": "adventureworks/schema/instawdb.sql.md",
            "sha256": "d979f036750f9c7b4fb7be3d58cdbb596a74f864a6325998501b3a96c6ad53f9",
            "license_id": "MIT",
        }],
    }

    source = build_supplemental_document_sources(fixture)[0]

    assert source["resource"]["resource_id"] == (
        "adventureworks:schema-document/beaab06ef72831089ca80e5355d65e661fd19b26/"
        ".local/assessment/source/instawdb.sql.md"
    )
    assert source["original_filename"] == "instawdb.sql"


def test_evidence_reference_uses_report_directory_and_file_digest(tmp_path):
    evidence_root = tmp_path / "dc01-inputs-20261004-r4"
    relative_path = Path("ingest/document_ingest.json")
    artifact_path = evidence_root / relative_path
    artifact_path.parent.mkdir(parents=True)
    content = b'{"revision":"r3"}\n'
    artifact_path.write_bytes(content)

    reference = evidence_artifact_reference(evidence_root, relative_path, content)

    assert reference == {
        "path": artifact_path.relative_to(PROJECT_ROOT).as_posix(),
        "sha256": sha256(artifact_path.read_bytes()).hexdigest(),
    }


def test_evidence_reference_rejects_existing_file_with_different_bytes(tmp_path):
    evidence_root = tmp_path / "dc01-inputs-20261004-r4"
    relative_path = Path("host_sources.json")
    artifact_path = evidence_root / relative_path
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b'{"revision":"r2"}\n')

    with pytest.raises(ValueError, match="冻结文件内容与报告输入不匹配"):
        evidence_artifact_reference(evidence_root, relative_path, b'{"revision":"r3"}\n')
