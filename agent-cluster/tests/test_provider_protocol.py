import copy
import json

import pytest

from agent_cluster.providers import (
    CoordinatorProvider,
    build_body,
    estimate_input_tokens,
    input_limit_for_request,
    validate_history,
    validate_response,
)
from agent_cluster.types import CoordinatorProfile


def test_deepseek_body_keeps_tools_and_enables_thinking():
    messages = [{"role": "user", "content": "查询订单"}]
    tools = [{"type": "function", "function": {"name": "orders"}}]

    body = build_body(messages, tools)

    assert body == {
        "model": "deepseek-flash",
        "messages": messages,
        "tools": tools,
        "thinking": {"type": "enabled"},
        "reasoning_effort": "high",
        "max_tokens": 8192,
        "stream": False,
    }
    assert "temperature" not in body


def test_deepseek_body_omits_empty_tools_and_preserves_history_reasoning():
    messages = [
        {"role": "user", "content": "开始"},
        {"role": "assistant", "content": "处理中", "reasoning_content": "先查看条件"},
        {"role": "user", "content": "继续"},
    ]

    body = build_body(messages, [])

    assert "tools" not in body
    assert body["messages"] == messages
    assert body["messages"][1]["reasoning_content"] == "先查看条件"


def test_dashscope_body_uses_shop_qwen_request_fields():
    profile = CoordinatorProfile(service="dashscope", model="qwen3.8-flash")

    body = build_body([{"role": "user", "content": "继续"}], [], profile=profile)

    assert body == {
        "model": "qwen3.8-flash",
        "messages": [{"role": "user", "content": "继续"}],
        "enable_thinking": True,
        "thinking_budget": 6144,
        "max_completion_tokens": 8192,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    assert "thinking" not in body
    assert "max_tokens" not in body


def test_dashscope_thinking_budget_leaves_capacity_for_answer():
    for output_limit in (128, 8192, 65536):
        profile = CoordinatorProfile(service="dashscope", model="qwen3.8-flash", output_limit=output_limit)
        body = build_body([{"role": "user", "content": "查询订单"}], [], profile=profile)
        assert 0 < body["thinking_budget"] < body["max_completion_tokens"]


def test_input_estimate_counts_utf8_reasoning_and_tools_conservatively():
    messages = [{
        "role": "assistant", "content": "答案", "reasoning_content": "推理文字",
    }]
    tools = [{"type": "function", "function": {"name": "订单查询"}}]
    payload = {"messages": messages, "tools": tools}
    expected_bytes = len(json.dumps(
        payload, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8"))

    assert estimate_input_tokens(messages, tools) == expected_bytes
    assert expected_bytes > len(str(payload))


def test_model_input_limit_is_current_upper_bound_within_context_capacity():
    profile = CoordinatorProfile(context_tokens=4096, output_limit=1024)
    messages = [{"role": "user", "content": "短请求"}]
    tools = []

    input_limit = input_limit_for_request(profile, messages, tools)

    assert input_limit == estimate_input_tokens(messages, tools)
    assert input_limit < profile.context_tokens - profile.output_limit


def test_history_requires_every_tool_call_result_once():
    messages = [
        {"role": "user", "content": "查订单"},
        {"role": "assistant", "content": "", "reasoning_content": "调用工具",
         "tool_calls": [{"id": "call-1", "type": "function",
                         "function": {"name": "orders", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "call-1", "content": "订单结果"},
    ]

    validate_history(messages)
    assert build_body(messages, []).get("messages") == messages

    with pytest.raises(ValueError, match="未完成"):
        validate_history(messages[:-1])
    with pytest.raises(ValueError, match="重复"):
        validate_history(messages + [copy.deepcopy(messages[2])])
    with pytest.raises(ValueError, match="调用"):
        validate_history([{"role": "tool", "tool_call_id": "unknown", "content": "x"}])


def test_history_rejects_duplicate_call_ids_and_interleaved_messages():
    call = {"id": "call-1", "type": "function",
            "function": {"name": "orders", "arguments": "{}"}}

    with pytest.raises(ValueError, match="重复"):
        validate_history([
            {"role": "assistant", "content": "", "tool_calls": [call]},
            {"role": "tool", "tool_call_id": "call-1", "content": "done"},
            {"role": "assistant", "content": "", "tool_calls": [call]},
            {"role": "tool", "tool_call_id": "call-1", "content": "again"},
        ])
    with pytest.raises(ValueError, match="未完成"):
        validate_history([
            {"role": "assistant", "content": "", "tool_calls": [call]},
            {"role": "user", "content": "继续"},
        ])


def test_provider_response_requires_complete_stop_or_tool_call_finish():
    response = {"id": "resp-1", "choices": [{
        "finish_reason": "stop",
        "message": {"role": "assistant", "content": "已完成"},
    }]}

    assert validate_response(response) is response
    with pytest.raises(ValueError, match="截断"):
        validate_response({**response, "choices": [{**response["choices"][0],
                                                     "finish_reason": "length"}]})
    with pytest.raises(ValueError, match="结束状态"):
        validate_response({"id": "resp-2", "choices": []})


def test_deepseek_response_must_retain_reasoning_content():
    response = {"choices": [{
        "finish_reason": "stop",
        "message": {"role": "assistant", "content": "完成", "reasoning_content": "检查输入"},
    }]}

    assert validate_response(response, require_reasoning=True) is response
    response["choices"][0]["message"].pop("reasoning_content")
    with pytest.raises(ValueError, match="reasoning_content"):
        validate_response(response, require_reasoning=True)


async def test_request_rejects_input_that_leaves_no_output_capacity():
    provider = CoordinatorProvider(
        model=None,
        profile=CoordinatorProfile(context_tokens=4096, output_limit=4096),
    )

    with pytest.raises(ValueError, match="输出预留"):
        await provider.request(
            run_id="run-1", step_key="step-1",
            messages=[{"role": "user", "content": "问题"}], tools=[],
        )


async def test_request_rejects_input_over_context_before_model_port_call():
    provider = CoordinatorProvider(
        model=None,
        profile=CoordinatorProfile(context_tokens=4096, output_limit=2048),
    )

    with pytest.raises(ValueError, match="超过"):
        await provider.request(
            run_id="run-1", step_key="step-1",
            messages=[{"role": "user", "content": "x" * 2049}], tools=[],
        )
