import hashlib
import json
import re
from datetime import datetime
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from shop.model_profiles import ModelProfile
from shop.models import now

KEYWORD_PROMPT_VERSION = "poster-keywords-v1"
POSTER_PROFILE = ModelProfile(
    service="deepseek", model="deepseek-flash", reasoning_effort="max", output_limit=27048,
)


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


class PosterModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MaterialSnapshot(PosterModel):
    asset_id: str
    version_id: str
    source_id: str
    source_revision: str
    record_id: str
    file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    mime_type: str
    title: str
    original_fields: dict
    owner_id: str
    source: dict

    @field_validator("asset_id", "version_id", "owner_id")
    @classmethod
    def valid_uuid(cls, value: str) -> str:
        return str(UUID(value))


class PosterInput(PosterModel):
    parent_harness_task_id: str | None = None
    parent_harness_run_id: str | None = None
    original_prompt: str
    harness_result: dict | None = None
    materials: tuple[MaterialSnapshot, ...] = Field(min_length=1, max_length=2)
    constraints: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def distinct_materials(self):
        if len({item.version_id for item in self.materials}) != len(self.materials):
            raise ValueError("素材版本不得重复")
        return self


class KeywordPlan(PosterModel):
    keywords: tuple[str, ...] = Field(min_length=2, max_length=5)

    @field_validator("keywords")
    @classmethod
    def valid_keywords(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value.strip() for value in values) or len(set(values)) != len(values):
            raise ValueError("关键词必须非空且互不重复")
        return values


def poster_job_id(inputs, owner_id, direct_keywords, prompt_version, model_profile):
    identity = digest({
        "actor_id": str(UUID(owner_id)), "input": inputs.model_dump(mode="json"),
        "direct_keywords": list(direct_keywords) if direct_keywords is not None else None,
        "prompt_version": prompt_version, "model_profile": model_profile.model_dump(),
    })
    return str(uuid5(NAMESPACE_URL, f"project-shop:poster:{identity}"))


class PosterJob(PosterModel):
    record_version: Literal[1] = 1
    job_id: str
    owner_id: str
    preparation_run_id: str
    input_data: PosterInput
    input_sha256: str
    prompt_version: str = KEYWORD_PROMPT_VERSION
    model_profile: ModelProfile = POSTER_PROFILE
    direct_keywords: KeywordPlan | None = None
    keyword_plan: KeywordPlan | None = None
    status: Literal[
        "created", "preparing", "prepared", "waiting_agents", "running",
        "completed", "needs_review", "failed",
    ] = "created"
    frame_records: tuple[str, ...] = ()
    agent_records: tuple[str, ...] = ()
    error: str | None = None
    created_at: datetime = Field(default_factory=now)

    @model_validator(mode="after")
    def validate_record(self):
        if str(UUID(self.job_id)) != self.job_id:
            raise ValueError("job_id格式无效")
        if self.preparation_run_id != self.job_id:
            raise ValueError("准备阶段运行身份不一致")
        if self.input_sha256 != digest(self.input_data.model_dump(mode="json")):
            raise ValueError("准备输入摘要不一致")
        if self.owner_id != str(UUID(self.owner_id)) or self.job_id != poster_job_id(
            self.input_data, self.owner_id,
            self.direct_keywords.keywords if self.direct_keywords is not None else None,
            self.prompt_version, self.model_profile,
        ):
            raise ValueError("准备配置与任务身份不一致")
        if (self.status in {"prepared", "waiting_agents", "running", "completed", "needs_review"}
                and self.keyword_plan is None):
            raise ValueError("准备结果缺少关键词")
        if (self.direct_keywords is not None and self.keyword_plan is not None
                and self.direct_keywords != self.keyword_plan):
            raise ValueError("直接输入关键词与准备结果不一致")
        if any(not re.fullmatch(r"agent-records/[0-9a-f]{32}\.json", item)
               for item in self.agent_records):
            raise ValueError("单角色记录路径无效")
        return self
