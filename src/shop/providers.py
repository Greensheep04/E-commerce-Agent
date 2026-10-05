import asyncio
from pathlib import Path
from time import monotonic
from urllib.parse import urlparse

import httpx
from dotenv import dotenv_values
from openai import AsyncOpenAI, omit
from openai.lib.streaming.chat import AsyncChatCompletionStream

from shop.config import Settings

DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"


def provider_values(settings: Settings) -> dict:
    if settings.provider_env_file is None:
        raise ValueError("SHOP_PROVIDER_ENV_FILE 未配置")
    return dotenv_values(settings.provider_env_file)


def dashscope_endpoint(settings: Settings, endpoint: str) -> tuple[str, str]:
    values = provider_values(settings)
    key = values.get("DASHSCOPE_API_KEY")
    base = settings.dashscope_base_url or values.get("DASHSCOPE_BASE_URL")
    if not key or not base:
        raise ValueError("DASHSCOPE_API_KEY 或 DASHSCOPE_BASE_URL 未配置")
    parsed = urlparse(base)
    if (parsed.scheme != "https" or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.port not in {None, 443}
            or parsed.path.rstrip("/") != "/compatible-mode/v1"
            or not parsed.hostname
            or not (parsed.hostname == "dashscope.aliyuncs.com"
                    or parsed.hostname.endswith(".cn-beijing.maas.aliyuncs.com"))):
        raise ValueError("当前费率只支持北京地区官方 DashScope 地址")
    return base.rstrip("/") + "/" + endpoint, key


def provider_call(
    settings: Settings, service: str, body: dict, timeout_seconds: float,
    progress: dict | None = None,
) -> tuple[dict, str | None]:
    return asyncio.run(_provider_call(settings, service, body, timeout_seconds, progress))


async def _provider_call(
    settings: Settings, service: str, body: dict, timeout_seconds: float,
    progress: dict | None = None,
) -> tuple[dict, str | None]:
    if timeout_seconds <= 0:
        raise TimeoutError("模型请求没有剩余执行时间")
    if service == "deepseek":
        endpoint, key = DEEPSEEK_URL, deepseek_key(settings)
    elif service == "dashscope":
        endpoint, key = dashscope_endpoint(
            settings, "embeddings" if body["model"] == "qwen3.7-text-embedding"
            else "chat/completions",
        )
    else:
        raise ValueError("未知模型提供商")
    headers = {"Authorization": f"Bearer {key}"}
    started = monotonic()
    async with asyncio.timeout(timeout_seconds), httpx.AsyncClient(timeout=timeout_seconds) as client:
        if not body.get("stream"):
            response = await client.post(endpoint, headers=headers, json=body)
            response.raise_for_status()
            result = response.json()
            return result, response.headers.get("x-request-id") or result.get("id")
        events = []
        reasoning_fragments = {}
        if progress is not None:
            progress.update(stream_events=events, complete=False, usage=None,
                            choices=[{"finish_reason": None,
                                      "message": {"content": "", "reasoning_content": ""}}])
        options = {name: value for name, value in body.items()
                   if name not in {"enable_thinking", "thinking_budget", "thinking"}}
        extra_body = {name: body[name] for name in ("enable_thinking", "thinking_budget", "thinking")
                      if name in body}
        async with AsyncOpenAI(api_key=key, base_url=endpoint.removesuffix("/chat/completions"),
                               max_retries=0, timeout=timeout_seconds) as sdk:
            raw = await sdk.chat.completions.create(**options, extra_body=extra_body)
            async with AsyncChatCompletionStream(raw_stream=raw, response_format=omit, input_tools=[]) as stream:
                async for event in stream:
                    if monotonic() - started > timeout_seconds:
                        raise TimeoutError("模型请求超过剩余执行时间")
                    if event.type == "chunk":
                        events.append(event.chunk.model_dump(mode="json", exclude_none=True))
                        for choice in event.chunk.choices:
                            fragment = getattr(choice.delta, "reasoning_content", None)
                            if fragment is not None:
                                reasoning_fragments.setdefault(choice.index, []).append(fragment)
                        if progress is not None:
                            progress.update(event.snapshot.model_dump(mode="json", exclude_none=True))
                            for choice in progress["choices"]:
                                if choice["index"] in reasoning_fragments:
                                    choice["message"]["reasoning_content"] = "".join(reasoning_fragments[choice["index"]])
                            progress["duration_seconds"] = monotonic() - started
                result = stream.current_completion_snapshot.model_dump(mode="json", exclude_none=True)
                for choice in result["choices"]:
                    if choice["index"] in reasoning_fragments:
                        choice["message"]["reasoning_content"] = "".join(reasoning_fragments[choice["index"]])
        if (not result.get("choices") or any(not choice.get("finish_reason") for choice in result["choices"])
                or not result.get("usage")):
            raise ValueError("流式请求缺少结束状态或用量")
        result.update(stream_events=events, duration_seconds=monotonic() - started)
        return result, result.get("id")


def deepseek_key(settings: Settings) -> str:
    path = settings.provider_env_file
    if path is None:
        raise ValueError("SHOP_PROVIDER_ENV_FILE 未配置")
    values = dotenv_values(Path(path))
    key = values.get("DEEPSEEK_API_KEY")
    if not key:
        raise ValueError("DEEPSEEK_API_KEY 未配置")
    return key


def create_completion(settings: Settings, body: dict, timeout_seconds: float) -> tuple[dict, str | None]:
    headers = {"Authorization": f"Bearer {deepseek_key(settings)}"}
    with httpx.Client(timeout=timeout_seconds) as client:
        response = client.post(DEEPSEEK_URL, headers=headers, json=body)
        response.raise_for_status()
        body = response.json()
        return body, response.headers.get("x-request-id") or body.get("id")
