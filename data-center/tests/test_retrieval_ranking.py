from data_center.retrieval.ranking import cosine_distance, lexical_score, rank_rrf


def test_rrf_unifies_real_text_results_and_preserves_unique_identities():
    first = "pip install 'markitdown[all]'"
    second = "pip install markitdown"
    assert lexical_score("install all", first) > lexical_score("install all", second)
    entries = rank_rrf([("first", 0.9), ("second", 0.7)],
                       [("second", 0.1), ("third", 0.2)], limit=3)
    assert [entry["key"] for entry in entries] == ["second", "first", "third"]
    assert entries[0]["rrf_score"] == 1 / 62 + 1 / 61


def test_cosine_distance_rejects_zero_or_nonfinite_vectors():
    import pytest

    with pytest.raises(ValueError):
        cosine_distance([0.0, 0.0], [1.0, 2.0])
    with pytest.raises(ValueError):
        cosine_distance([float("nan"), 1.0], [1.0, 2.0])

