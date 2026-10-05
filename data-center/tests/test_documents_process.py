import json
import subprocess
import sys
from pathlib import Path

import pytest
from test_documents_conversion import document_input

from data_center.documents.conversion import ConversionPolicy, convert_document


def test_conversion_cache_is_verified_by_independent_process(tmp_path):
    payload, raw = document_input("docx")
    converted = convert_document(payload, raw, tmp_path, ConversionPolicy())
    result = subprocess.run([sys.executable, str(Path(__file__).resolve()), str(tmp_path)],
                            check=True, capture_output=True, text=True, timeout=45)
    identity = json.loads(result.stdout)
    assert identity["content_sha256"] == converted.content_sha256
    assert identity["chunk_ids"] == [item.chunk_id for item in converted.chunks]
    assert identity["converter_loaded"] is False


def test_original_corruption_is_rejected_on_cached_recovery(tmp_path):
    payload, raw = document_input("docx")
    converted = convert_document(payload, raw, tmp_path, ConversionPolicy())
    converted.original_path.write_bytes(raw + b"changed")
    with pytest.raises(ValueError, match="SHA-256"):
        convert_document(payload, raw, tmp_path, ConversionPolicy())


if __name__ == "__main__":
    payload, raw = document_input("docx")
    converted = convert_document(payload, raw, Path(sys.argv[1]), ConversionPolicy())
    print(json.dumps({"content_sha256": converted.content_sha256,
                      "chunk_ids": [item.chunk_id for item in converted.chunks],
                      "converter_loaded": "docling.document_converter" in sys.modules}))
