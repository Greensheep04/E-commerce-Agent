import copy
from typing import Any

from agent_cluster.tokens import estimate_tokens
from agent_cluster.types import CoordinatorProfile, ModelPort

_DASHSCOPE_MODEL = "qwen3.8-flash"
_DASHSCOPE_THINKING_BUDGET = 25000


def validate_history(messages: list[dict]) -> None:
    issued: set[str] = set()
    pending: set[str] = set()
    completed: set[str] = set()

    for message in messages:
        if not isinstance(message, dict):
            raise ValueError("历史消息必须为对象")
        role = message.get("role")
        if role not in {"system", "developer", "user", "assistant", "tool"}:
            raise ValueError("历史消息角色无效")

        if role == "tool":
            call_id = message.get("tool_call_id")
            if not isinstance(call_id, str) or not call_id:
                raise ValueError("工具结果缺少tool_call_id")
            if call_id in completed:
                raise ValueError("工具调用结果重复")
            if call_id not in pending:
                raise ValueError("工具结果不属于未完成的assistant调用")
            pending.remove(call_id)
            completed.add(call_id)
            continue

        if pending:
            raise ValueError("工具调用尚未完成，不能继续追加其他消息")

        calls = message.get("tool_calls")
        if calls is None:
            calls = []
        if role != "assistant" and calls:
            raise ValueError("只有assistant消息可以包含tool_calls")
        if not isinstance(calls, list):
            raise ValueError("assistant.tool_calls必须为列表")
        for call in calls:
            if not isinstance(call, dict):
                raise ValueError("tool_call必须为对象")
            call_id = call.get("id")
            function = call.get("function")
            if not isinstance(call_id, str) or not call_id:
                raise ValueError("tool_call缺少有效id")
            if call_id in issued:
                raise ValueError("tool_call_id重复")
            if not isinstance(function, dict) or not isinstance(function.get("name"), str):
                raise ValueError("tool_call缺少有效function")
            issued.add(call_id)
            pending.add(call_id)

    if pending:
        raise ValueError("历史末尾存在尚未完成的工具调用轮次")


def estimate_input_tokens(messages: list[dict], tools: list[dict], *, profile: CoordinatorProfile | None = None) -> int:
    return estimate_tokens(messages, tools, profile)


def input_limit_for_request(
    profile: CoordinatorProfile,
    messages: list[dict],
    tools: list[dict],
) -> int:
    if profile.output_limit >= profile.context_tokens:
        raise ValueError("模型输出预留已达到上下文容量")
    available_input = profile.context_tokens - profile.output_limit
    input_upper_bound = estimate_input_tokens(messages, tools, profile=profile)
    if input_upper_bound > available_input:
        raise ValueError("模型输入估算超过可用上下文容量")
    return input_upper_bound


def build_body(
    messages: list[dict],
    tools: list[dict],
    *,
    profile: CoordinatorProfile | None = None,
) -> dict:
    profile = profile or CoordinatorProfile()
    validate_history(messages)
    body: dict[str, Any] = {
        "model": profile.model,
        "messages": copy.deepcopy(messages),
        "stream": False,
    }
    if tools:
        body["tools"] = copy.deepcopy(tools)

    if profile.service == "deepseek":
        body.update(
            thinking={"type": "enabled"},
            reasoning_effort=profile.reasoning_effort,
            max_tokens=profile.output_limit,
        )
    elif profile.service == "dashscope":
        if profile.model != _DASHSCOPE_MODEL:
            raise ValueError("DashScope协调模型必须为qwen3.8-flash")
        body.update(
            enable_thinking=True,
            thinking_budget=min(_DASHSCOPE_THINKING_BUDGET, profile.output_limit * 3 // 4),
            max_completion_tokens=profile.output_limit,
            stream=True,
            stream_options={"include_usage": True},
        )
    else:
        raise ValueError("未知模型服务")
    return body


def validate_response(response: dict, *, require_reasoning: bool = False) -> dict:
    if not isinstance(response, dict):
        raise ValueError("模型响应必须为对象")
    choices = response.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise ValueError("模型响应缺少唯一结束状态")
    choice = choices[0]
    if not isinstance(choice, dict):
        raise ValueError("模型响应choice必须为对象")
    finish_reason = choice.get("finish_reason")
    if finish_reason == "length":
        raise ValueError("模型响应因输出长度限制而截断")
    if finish_reason not in {"stop", "tool_calls"}:
        raise ValueError("模型响应结束状态无效")
    message = choice.get("message")
    if not isinstance(message, dict) or message.get("role") != "assistant":
        raise ValueError("模型响应缺少assistant消息")
    if require_reasoning and not isinstance(message.get("reasoning_content"), str):
        raise ValueError("DeepSeek思考响应缺少reasoning_content")

    calls = message.get("tool_calls")
    if calls is None:
        calls = []
    if finish_reason == "tool_calls":
        if not isinstance(calls, list) or not calls:
            raise ValueError("tool_calls结束状态缺少工具调用")
        seen: set[str] = set()
        for call in calls:
            if not isinstance(call, dict):
                raise ValueError("响应tool_call必须为对象")
            call_id = call.get("id")
            function = call.get("function")
            if (not isinstance(call_id, str) or not call_id or call_id in seen
                    or not isinstance(function, dict)
                    or not isinstance(function.get("name"), str)):
                raise ValueError("响应tool_call内容无效")
            seen.add(call_id)
    elif calls:
        raise ValueError("stop结束状态包含未结束的工具调用")
    return response


class CoordinatorProvider:
    def __init__(self, model: ModelPort, profile: CoordinatorProfile):
        self.model = model
        self.profile = profile

    def build_body(self, messages: list[dict], tools: list[dict]) -> dict:
        return build_body(messages, tools, profile=self.profile)

    async def request(
        self,
        *,
        run_id: str,
        step_key: str,
        messages: list[dict],
        tools: list[dict],
    ) -> dict:
        body = self.build_body(messages, tools)
        input_limit = input_limit_for_request(self.profile, messages, tools)
        response = await self.model.request(
            run_id=run_id,
            step_key=step_key,
            body=body,
            input_limit=input_limit,
            output_limit=self.profile.output_limit,
            timeout_seconds=self.profile.timeout_seconds,
            service=self.profile.service,
        )
        return validate_response(response, require_reasoning=self.profile.service == "deepseek")
