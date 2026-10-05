from test_documents_conversion import document_input

from data_center.documents.conversion import ConversionPolicy, convert_document
from data_center.graph.identity import graph_chunk_mapping, graph_fingerprint


def test_graph_identity_binds_exact_chunks_snapshots_policy_and_config():
    config = {"lightrag": "1.5.7", "model": "deepseek-flash"}
    first = graph_fingerprint(["snapshot-a"], ["a", "b"], "policy-a", config)
    assert first == graph_fingerprint(["snapshot-a"], ["b", "a"], "policy-a", config)
    assert first != graph_fingerprint(["snapshot-a"], ["a"], "policy-a", config)
    assert first != graph_fingerprint(["snapshot-b"], ["a", "b"], "policy-a", config)
    assert first != graph_fingerprint(["snapshot-a"], ["a", "b"], "policy-b", config)


def test_duplicate_source_text_has_multiple_reliable_graph_references(tmp_path):
    source, raw = document_input("docx")
    first = convert_document(source, raw, tmp_path, ConversionPolicy())
    second_input = source.model_copy(update={"record_id": source.record_id + "-copy"})
    second = convert_document(second_input, raw, tmp_path, ConversionPolicy())
    entries = [("snapshot-a", chunk.chunk_id, chunk.text) for chunk in first.chunks]
    entries.extend(("snapshot-b", chunk.chunk_id, chunk.text) for chunk in second.chunks)
    mapping = graph_chunk_mapping("graph-corpus", entries)
    assert len(mapping) == len(first.chunks)
    assert all(len(refs) == 2 for refs in mapping.values())
