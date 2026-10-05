import json
from hashlib import sha256
from pathlib import Path

import pytest
from pydantic import ValidationError
from tokenizers import Tokenizer

from agent_cluster.providers import estimate_input_tokens, input_limit_for_request
from agent_cluster.types import CoordinatorProfile

ROOT = Path(__file__).resolve().parents[2]
TOKENIZER = ROOT / ".scratch/evidence/clu03-real-context-20261003-r1/tokenizer/deepseek_v4_tokenizer/tokenizer.json"
TOKENIZER_SHA = "89085f12ef79460ac5f66d1119325ddfc694b4ab209d80bbd81d35f081dc9614"


@pytest.fixture
def official_profile():
    if not TOKENIZER.is_file():
        pytest.skip("当前环境未准备官方DeepSeek tokenizer验收来源")
    assert sha256(TOKENIZER.read_bytes()).hexdigest() == TOKENIZER_SHA
    return CoordinatorProfile(tokenizer_file=str(TOKENIZER), tokenizer_sha256=TOKENIZER_SHA)


def test_official_tokenizer_includes_reasoning_tool_arguments_and_schema(official_profile):
    messages = [{"role": "assistant", "content": "",
                 "reasoning_content": "查询Microsoft AdventureWorks公开来源订单的明细与金额。",
                 "tool_calls": [{"id": "protocol-call", "type": "function", "function": {
                     "name": "lookup_customer_order", "arguments": '{"order_number":"SO74480"}'}}]}]
    tools = [{"type": "function", "function": {"name": "lookup_customer_order", "parameters": {
        "type": "object", "properties": {"order_number": {"type": "string"}}}}}]
    serialized = json.dumps({"messages": messages, "tools": tools}, ensure_ascii=False,
                            separators=(",", ":"), sort_keys=True, allow_nan=False)
    raw = len(Tokenizer.from_file(str(TOKENIZER)).encode(serialized, add_special_tokens=False).ids)
    estimate = estimate_input_tokens(messages, tools, profile=official_profile)
    assert estimate == (raw * 110 + 99) // 100 + 32 * len(messages) + 128
    assert estimate > estimate_input_tokens([{"role": "assistant", "content": ""}], [], profile=official_profile)
    assert input_limit_for_request(official_profile, messages, tools) == estimate


def test_official_tokenizer_rejects_changed_source(official_profile):
    changed = official_profile.model_copy(update={"tokenizer_sha256": "0" * 64})
    with pytest.raises(ValueError, match="摘要不匹配"):
        estimate_input_tokens([{"role": "user", "content": "检查来源"}], [], profile=changed)


def test_tokenizer_configuration_requires_file_and_digest():
    with pytest.raises(ValidationError, match="同时配置"):
        CoordinatorProfile(tokenizer_file="tokenizer.json")
    with pytest.raises(ValidationError, match="同时配置"):
        CoordinatorProfile(tokenizer_sha256=TOKENIZER_SHA)
