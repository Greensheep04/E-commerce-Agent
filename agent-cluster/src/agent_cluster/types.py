from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal, Protocol
from uuid import uuid4

from integration_protocol import SHA256, JSONObject, NonEmptyString, ResourceRef
from pydantic import BaseModel, ConfigDict, Field, model_validator


def now() -> datetime:
    return datetime.now(UTC)


def identifier() -> str:
    return str(uuid4())


class ClusterModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, revalidate_instances="always")


class CompletionCriterion(ClusterModel):
    tool_name: NonEmptyString
    action: str | None = None
    path: list[str | int] = Field(default_factory=list)
    operator: Literal["exists", "nonempty", "equals", "resource"] = "nonempty"
    expected: object = None
    module_id: str | None = None
    resource_type: str | None = None
    min_matches: int = Field(default=1, ge=1, le=500)


class GoalDefinition(ClusterModel):
    goal_id: NonEmptyString = Field(default_factory=identifier)
    actor_id: NonEmptyString
    objective: Annotated[str, Field(min_length=1, max_length=16000)]
    allowed_operations: dict[str, list[str]]
    criteria: list[CompletionCriterion] = Field(min_length=1)
    budget_cny: Decimal = Field(default=Decimal("30"), gt=0, le=30)
    revision: int = Field(default=1, ge=1)
    max_decisions: int = Field(default=40, ge=1, le=200)
    max_tasks: int = Field(default=80, ge=1, le=500)
    required_confirmations: list[str] = Field(default_factory=list)
    approved_confirmations: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=now)

    @model_validator(mode="after")
    def valid_authority(self):
        if not self.allowed_operations:
            raise ValueError("目标必须声明允许的操作")
        for name, actions in self.allowed_operations.items():
            if not name.strip() or not actions or any(not action.strip() for action in actions):
                raise ValueError("允许操作不能为空")
            if len(actions) != len(set(actions)):
                raise ValueError("允许操作不能重复")
        if any(item.tool_name not in self.allowed_operations for item in self.criteria):
            raise ValueError("完成条件必须属于已授权工具")
        return self


RunStatus = Literal[
    "queued", "running", "waiting", "waiting_confirmation", "waiting_dependencies",
    "paused", "cancelled", "completed", "failed", "unknown", "budget_exhausted",
]


class GoalRun(ClusterModel):
    run_id: NonEmptyString = Field(default_factory=identifier)
    goal_id: NonEmptyString
    actor_id: NonEmptyString
    goal_revision: int = Field(ge=1)
    status: RunStatus = "queued"
    trigger_id: str | None = None
    plan_version: int = Field(default=0, ge=0)
    decision_count: int = Field(default=0, ge=0)
    spent_cny: Decimal = Field(default=Decimal("0"), ge=0)
    reserved_cny: Decimal = Field(default=Decimal("0"), ge=0)
    summary: str = ""
    error: str | None = None
    created_at: datetime = Field(default_factory=now)
    updated_at: datetime = Field(default_factory=now)


class TriggerEvent(ClusterModel):
    event_id: NonEmptyString
    goal_id: NonEmptyString
    actor_id: NonEmptyString
    kind: Literal["manual", "schedule", "event"] = "event"
    payload: JSONObject = Field(default_factory=dict)
    scheduled_at: datetime | None = None
    created_at: datetime = Field(default_factory=now)


class NoteRef(ClusterModel):
    note_id: NonEmptyString
    goal_id: NonEmptyString
    actor_id: NonEmptyString
    version: int = Field(ge=1)
    sha256: SHA256
    topic: NonEmptyString
    summary: str
    source: NonEmptyString
    resources: list[ResourceRef] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=now)


class InputBinding(ClusterModel):
    source_task_id: NonEmptyString
    source_path: list[str | int] = Field(min_length=1)
    target_path: list[str | int] = Field(min_length=1)


class TaskSpec(ClusterModel):
    task_id: Annotated[str, Field(pattern=r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")]
    tool_name: NonEmptyString
    action: NonEmptyString
    payload: JSONObject = Field(default_factory=dict)
    upstream: list[ResourceRef] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    bindings: list[InputBinding] = Field(default_factory=list)
    purpose: str = ""


class TaskPlan(ClusterModel):
    tasks: list[TaskSpec]
    reason: str = ""

    @model_validator(mode="after")
    def valid_dependencies(self):
        tasks = {task.task_id: task for task in self.tasks}
        if len(tasks) != len(self.tasks):
            raise ValueError("任务编号不能重复")
        for task in self.tasks:
            if any(dependency not in tasks for dependency in task.dependencies):
                raise ValueError("任务依赖不存在")
            if any(binding.source_task_id not in task.dependencies for binding in task.bindings):
                raise ValueError("输入引用必须声明依赖")
        visited, active = set(), set()

        def visit(task_id):
            if task_id in active:
                raise ValueError("任务依赖包含循环")
            if task_id in visited:
                return
            active.add(task_id)
            for dependency in tasks[task_id].dependencies:
                visit(dependency)
            active.remove(task_id)
            visited.add(task_id)

        for task_id in tasks:
            visit(task_id)
        return self


class CoordinatorProfile(ClusterModel):
    service: Literal["deepseek", "dashscope"] = "deepseek"
    model: str = "deepseek-flash"
    reasoning_effort: Literal["low", "high", "max"] = "high"
    output_limit: int = Field(default=8192, ge=128, le=65536)
    context_tokens: int = Field(default=1_000_000, ge=4096, le=1_000_000)
    compression_ratio: float = Field(default=0.85, gt=0, lt=1)
    timeout_seconds: float = Field(default=180, gt=0)
    tokenizer_file: str | None = None
    tokenizer_sha256: SHA256 | None = None

    @model_validator(mode="after")
    def tokenizer_source(self):
        if (self.tokenizer_file is None) != (self.tokenizer_sha256 is None):
            raise ValueError("Tokenizer文件与来源摘要必须同时配置")
        return self


class ContextWindow(ClusterModel):
    messages: list[dict]
    input_tokens_estimate: int = Field(ge=0)
    compacted: bool = False
    archive_refs: list[NoteRef] = Field(default_factory=list)


class ModelPort(Protocol):
    async def request(self, *, run_id: str, step_key: str, body: dict,
                      input_limit: int, output_limit: int, timeout_seconds: float,
                      service: str) -> dict: ...


class ScheduleDefinition(ClusterModel):
    schedule_id: NonEmptyString = Field(default_factory=identifier)
    goal_id: NonEmptyString
    actor_id: NonEmptyString
    interval_seconds: int | None = Field(default=None, ge=1)
    cron: str | None = None
    timezone: str = "Asia/Shanghai"
    enabled: bool = True

    @model_validator(mode="after")
    def exactly_one_trigger(self):
        if (self.interval_seconds is None) == (self.cron is None):
            raise ValueError("日程必须指定一个interval或cron")
        return self
