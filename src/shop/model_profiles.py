import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ModelProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    service: str
    model: str
    thinking_budget: int | None = Field(default=None, ge=1)
    reasoning_effort: str | None = None
    output_limit: int = Field(ge=1)

    def body(self, messages: list[dict]) -> dict:
        body = {"model": self.model, "messages": messages,
                "response_format": {"type": "json_object"}, "stream": True,
                "stream_options": {"include_usage": True}}
        if self.service == "deepseek":
            body.update(thinking={"type": "enabled"}, reasoning_effort=self.reasoning_effort,
                        max_tokens=self.output_limit)
        else:
            body.update(enable_thinking=True, thinking_budget=self.thinking_budget,
                        max_completion_tokens=self.output_limit)
        return body


def profiles(limit_file: Path) -> dict[str, ModelProfile]:
    overrides = json.loads(limit_file.read_text(encoding="utf-8")) if limit_file.exists() else {}
    roles = {"description", "understand", "compose", "review", "revise", "rereview"}
    if not isinstance(overrides, dict) or set(overrides) - roles:
        raise ValueError("模型输出上限配置包含未知模块")
    result = {}
    for role in sorted(roles):
        vision = role in {"description", "review", "rereview"}
        thinking = 4096 if role == "description" else 25000
        default = 5120 if role == "description" else 27048
        limit = overrides.get(role, default)
        if not isinstance(limit, int) or isinstance(limit, bool) or not default <= limit <= 393216:
            raise ValueError(f"{role} 输出上限配置无效")
        result[role] = ModelProfile(
            service="dashscope" if vision else "deepseek",
            model="qwen3.8-flash" if vision else "deepseek-flash",
            thinking_budget=thinking if vision else None,
            reasoning_effort=None if vision else "max", output_limit=limit,
        )
    return result
