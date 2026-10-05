from data_center.documents.conversion import identity_hash


def graph_fingerprint(snapshot_ids: list[str], chunk_ids: list[str], policy_version: str,
                      configuration: dict) -> str:
    if len(set(chunk_ids)) != len(chunk_ids):
        raise ValueError("图资料片段集合包含重复编号")
    return identity_hash({"snapshots": sorted(set(snapshot_ids)), "chunks": sorted(chunk_ids),
                          "policy_version": policy_version, "configuration": configuration})


def graph_chunk_mapping(doc_id: str, entries: list[tuple[str, str, str]]) -> dict:
    from lightrag.utils import sanitize_text_for_encoding
    from lightrag.utils_pipeline import make_custom_chunk_id

    mapping = {}
    for snapshot_id, chunk_id, content in entries:
        graph_id = make_custom_chunk_id(doc_id, sanitize_text_for_encoding(content))
        mapping.setdefault(graph_id, []).append((snapshot_id, chunk_id))
    return mapping
