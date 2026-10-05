import math
import re

RRF_K = 60
TOKEN_PATTERN = re.compile(r"[A-Za-z_][A-Za-z_0-9]*|\d+|[\u4e00-\u9fff]+")


def _bigrams(text: str) -> set[str]:
    normalized = "".join(char.casefold() for char in text if char.isalnum() or char == "_")
    return {normalized[index:index + 2] for index in range(len(normalized) - 1)}


def lexical_score(query: str, content: str) -> float:
    query_bigrams, content_bigrams = _bigrams(query), _bigrams(content)
    if not query_bigrams or not content_bigrams:
        return 0.0
    dice = 2 * len(query_bigrams & content_bigrams) / (len(query_bigrams) + len(content_bigrams))
    query_tokens = TOKEN_PATTERN.findall(query.casefold())
    content_tokens = set(TOKEN_PATTERN.findall(content.casefold()))
    exact = sum(token in content_tokens for token in query_tokens if len(token) >= 2)
    return dice + 0.25 * (exact / len(query_tokens) if query_tokens else 0.0)


def cosine_distance(left, right) -> float:
    if len(left) != len(right) or not left or any(not math.isfinite(value) for value in (*left, *right)):
        raise ValueError("文档检索向量无效")
    magnitude = math.sqrt(sum(value * value for value in left) * sum(value * value for value in right))
    if magnitude == 0:
        raise ValueError("文档检索向量模长为零")
    return 1 - sum(a * b for a, b in zip(left, right, strict=True)) / magnitude


def rank_rrf(lexical: list[tuple[str, float]], vector: list[tuple[str, float]], *, limit: int):
    if type(limit) is not int or limit < 1:
        raise ValueError("检索条数无效")
    scores = {}
    for ranking, field in ((lexical, "lexical_score"), (vector, "vector_distance")):
        if len({key for key, _ in ranking}) != len(ranking):
            raise ValueError("检索候选重复")
        for rank, (key, score) in enumerate(ranking, 1):
            entry = scores.setdefault(key, {"key": key, "lexical_score": None,
                                            "vector_distance": None, "rrf_score": 0.0})
            entry[field] = score
            entry["rrf_score"] += 1 / (RRF_K + rank)
    return sorted(scores.values(), key=lambda entry: (-entry["rrf_score"], entry["key"]))[:limit]


def select_source_coverage(ranked: list[dict], source_by_key: dict[str, str],
                           required_source_ids: list[str], *, limit: int) -> tuple[list[dict], list[str]]:
    if type(limit) is not int or limit < 1 or len(required_source_ids) > limit:
        raise ValueError("必需文档来源数量超过检索条数")
    if len(set(required_source_ids)) != len(required_source_ids):
        raise ValueError("必需文档来源重复")
    unique = {}
    for entry in ranked:
        key = entry["key"]
        if key not in source_by_key:
            raise ValueError("检索候选没有来源身份")
        unique.setdefault(key, entry)
    anchors = {}
    required = set(required_source_ids)
    for key in unique:
        source_id = source_by_key[key]
        if source_id in required:
            anchors.setdefault(source_id, key)
    missing = sorted(required - set(anchors))
    if missing:
        return [], missing
    selected = set(anchors.values())
    for key in unique:
        if len(selected) >= limit:
            break
        selected.add(key)
    return [entry for key, entry in unique.items() if key in selected], []
