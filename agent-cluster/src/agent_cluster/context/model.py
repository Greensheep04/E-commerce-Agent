import copy
from collections.abc import Callable
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.outputs import ChatGeneration, ChatResult
from pydantic import Field, PrivateAttr

from agent_cluster.providers import (
    build_body,
    input_limit_for_request,
    validate_response,
)
from agent_cluster.providers import (
    estimate_input_tokens as provider_input_tokens,
)
from agent_cluster.types import CoordinatorProfile


def estimate_input_tokens(messages: list[dict], catalog: dict, *, profile: CoordinatorProfile | None = None) -> int:
    tools = catalog.get("coordinator_tools", catalog)
    return provider_input_tokens(messages, tools, profile=profile)


def provider_messages(messages: list[BaseMessage]) -> list[dict]:
    result = []
    for message in messages:
        if isinstance(message, SystemMessage):
            value = {"role": "system", "content": copy.deepcopy(message.content)}
        elif isinstance(message, HumanMessage):
            value = {"role": "user", "content": copy.deepcopy(message.content)}
        elif isinstance(message, AIMessage):
            value = {"role": "assistant", "content": copy.deepcopy(message.content)}
            if "reasoning_content" in message.additional_kwargs:
                value["reasoning_content"] = copy.deepcopy(message.additional_kwargs["reasoning_content"])
            if "tool_calls" in message.additional_kwargs:
                value["tool_calls"] = copy.deepcopy(message.additional_kwargs["tool_calls"])
            elif message.tool_calls:
                raise ValueError("摘要消息缺少提供商原始tool_calls")
        elif isinstance(message, ToolMessage):
            value = {"role": "tool", "content": copy.deepcopy(message.content), "tool_call_id": message.tool_call_id}
        else:
            raise ValueError("摘要消息类型不支持")
        result.append(value)
    return result


def build_summary_request(messages: list[BaseMessage], profile: CoordinatorProfile,
                          system_message: dict | None = None) -> tuple[dict, int]:
    if (not isinstance(system_message, dict) or system_message.get("role") != "system"
            or not isinstance(system_message.get("content"), str) or not system_message["content"].strip()):
        raise ValueError("摘要请求必须提供完整system信息")
    raw_messages = [copy.deepcopy(system_message), *provider_messages(messages)]
    body = build_body(raw_messages, [], profile=profile)
    return body, input_limit_for_request(profile, raw_messages, [])


class PortChatModel(BaseChatModel):
    port: Any = Field(exclude=True)
    run_id: str
    step_key: str
    coordinator_profile: CoordinatorProfile
    system_message: dict
    request_snapshot: Callable[[dict], dict] | None = Field(default=None, exclude=True)
    _last_response: dict | None = PrivateAttr(default=None)

    @property
    def _llm_type(self) -> str:
        return "agent-cluster-model-port"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        raise RuntimeError("摘要模型仅支持异步ModelPort调用")

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        if stop is not None or kwargs:
            raise ValueError("摘要模型收到未声明的生成参数")
        profile = self.coordinator_profile
        body, input_limit = build_summary_request(messages, profile, self.system_message)
        request = {
            "run_id": self.run_id, "step_key": self.step_key, "body": body,
            "input_limit": input_limit, "output_limit": profile.output_limit,
            "timeout_seconds": profile.timeout_seconds, "service": profile.service,
        }
        if self.request_snapshot is not None:
            request = self.request_snapshot(request)
        if self.port is None:
            raise ValueError("摘要生成需要宿主ModelPort")
        response = await self.port.request(**request)
        self._last_response = copy.deepcopy(response)
        validate_response(response, require_reasoning=profile.service == "deepseek")
        choices = response.get("choices")
        if not isinstance(choices, list) or len(choices) != 1:
            raise ValueError("摘要模型响应choices无效")
        choice = choices[0]
        if choice.get("finish_reason") != "stop":
            raise ValueError("摘要模型响应没有完整结束")
        message = choice.get("message")
        if not isinstance(message, dict) or message.get("role") != "assistant":
            raise ValueError("摘要模型响应message无效")
        content = message.get("content")
        if not isinstance(content, str) or not content.strip() or message.get("tool_calls"):
            raise ValueError("摘要模型响应必须提供完整文字正文")
        additional = {key: copy.deepcopy(value) for key, value in message.items() if key not in {"role", "content"}}
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=content, additional_kwargs=additional))])
