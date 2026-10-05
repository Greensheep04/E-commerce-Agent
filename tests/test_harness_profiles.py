from pathlib import Path

import pytest

from shop.model_profiles import profiles


def test_model_requests_keep_thinking_separate_from_final_output():
    config = profiles(Path(".scratch/nonexistent-profile-file"))
    messages = [{"role": "user", "content": "请返回 JSON"}]
    description = config["description"].body(messages)
    review = config["review"].body(messages)
    text = config["compose"].body(messages)
    assert description["thinking_budget"] == 4096
    assert description["max_completion_tokens"] > description["thinking_budget"]
    assert review["thinking_budget"] == 25000
    assert review["max_completion_tokens"] > review["thinking_budget"]
    assert "reasoning_effort" not in review
    assert text["thinking"] == {"type": "enabled"}
    assert text["reasoning_effort"] == "max"
    assert "thinking_budget" not in text


def test_invalid_profile_override_is_rejected_before_model_request(tmp_path):
    path = tmp_path / "limits.json"
    path.write_text('{"review": 10}', encoding="utf-8")
    with pytest.raises(ValueError, match="输出上限"):
        profiles(path)
