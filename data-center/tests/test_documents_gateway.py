from pathlib import Path

import pytest

from data_center.documents.gateway import embedding_batches


def test_embedding_batches_preserve_actual_source_text_and_gateway_bounds():
    source = Path("/Users/greensheep/.codex/worktrees/f4dc/industry-RAG/artifacts/raw/documents-v04/markitdown/readme-v1.md")
    lines = source.read_text().splitlines(keepends=True)
    texts = ["".join(lines[index:index + 30]) for index in range(0, len(lines), 30)]
    batches = embedding_batches(texts)
    assert [text for batch in batches for text in batch] == texts
    assert all(len(batch) <= 20 and sum(len(text.encode()) for text in batch) <= 12000 for batch in batches)


def test_embedding_single_text_exceeding_gateway_limit_rejects():
    with pytest.raises(ValueError, match="embedding单条"):
        embedding_batches(["a" * 12001])
