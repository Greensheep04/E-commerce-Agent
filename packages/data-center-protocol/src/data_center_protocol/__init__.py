from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Protocol, runtime_checkable

from integration_protocol import (
    SHA256,
    CallContext,
    Identifier,
    JSONObject,
    NonEmptyString,
    ProtocolModel,
    ResourceRef,
)
from pydantic import Field, model_validator

SourceNature = Literal["technical_sample", "public_business_sample", "business_source"]
QueryIntent = Literal["documents", "business", "metrics"]
DataStatus = Literal[
    "queued", "running", "waiting", "completed", "partial", "DATA_BLOCKED",
    "failed", "cancelled", "unknown", "budget_exhausted",
]


class DataAsset(ProtocolModel):
    asset_id: NonEmptyString
    source_id: NonEmptyString
    record_id: NonEmptyString
    title: NonEmptyString
    kind: Literal["document", "dataset", "metric", "index"]
    source_revision: NonEmptyString
    source_nature: SourceNature
    license_id: NonEmptyString
    owner_id: NonEmptyString
    sha256: SHA256
    graph_enabled: bool = False
    active: bool = True
    properties: JSONObject = Field(default_factory=dict)


class DataSnapshot(ProtocolModel):
    snapshot_id: SHA256
    source_ids: list[NonEmptyString] = Field(min_length=1)
    source_revisions: dict[str, NonEmptyString]
    created_at: datetime
    data_as_of: datetime | None = None
    state: Literal["staged", "published", "withdrawn"]
    record_count: int = Field(ge=0)
    schema_sha256: SHA256
    content_sha256: SHA256
    quality: JSONObject = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_identity(self):
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("快照来源重复")
        if set(self.source_ids) != set(self.source_revisions):
            raise ValueError("快照来源版本不完整")
        for value in (self.created_at, self.data_as_of):
            if value is not None and value.utcoffset() is None:
                raise ValueError("快照时间必须包含时区")
        return self


class MetricDefinition(ProtocolModel):
    metric_id: Identifier
    title: NonEmptyString
    version: NonEmptyString
    source_ids: list[NonEmptyString] = Field(min_length=1)
    formula: NonEmptyString
    dimensions: list[Identifier] = Field(default_factory=list)
    unit: NonEmptyString
    currency: NonEmptyString | None = None
    properties: JSONObject = Field(default_factory=dict)


class DataEvidenceRef(ProtocolModel):
    kind: Literal["document", "record", "metric"]
    source_id: NonEmptyString
    snapshot_id: SHA256
    record_id: NonEmptyString
    source_revision: NonEmptyString
    source_sha256: SHA256
    content_sha256: SHA256
    chunk_id: NonEmptyString | None = None
    start_line: int | None = Field(default=None, ge=1)
    end_line: int | None = Field(default=None, ge=1)
    locator: JSONObject = Field(default_factory=dict)
    resource: ResourceRef | None = None

    @model_validator(mode="after")
    def validate_location(self):
        if self.kind == "document":
            if self.chunk_id is None or self.start_line is None or self.end_line is None:
                raise ValueError("文档引用缺少片段或行范围")
            if self.end_line < self.start_line:
                raise ValueError("文档引用行范围无效")
        elif any(value is not None for value in (self.chunk_id, self.start_line, self.end_line)):
            raise ValueError("业务记录引用不能包含文档行范围")
        return self


class DataQuery(ProtocolModel):
    question: NonEmptyString
    intent: QueryIntent
    source_ids: list[NonEmptyString] = Field(min_length=1)
    required_source_ids: list[NonEmptyString] = Field(
        default_factory=list, exclude_if=lambda value: not value,
    )
    snapshot_id: SHA256 | None = None
    retrieval_mode: Literal["hybrid", "graph"] = "hybrid"
    metric_ids: list[Identifier] = Field(default_factory=list)
    dimensions: list[Identifier] = Field(default_factory=list)
    start_date: date | None = None
    end_date: date | None = None
    record_id: NonEmptyString | None = None
    filters: JSONObject = Field(default_factory=dict)
    limit: int = Field(default=10, ge=1, le=100)

    @model_validator(mode="after")
    def validate_query(self):
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("查询来源重复")
        if len(set(self.required_source_ids)) != len(self.required_source_ids):
            raise ValueError("必需来源重复")
        if not set(self.required_source_ids).issubset(self.source_ids):
            raise ValueError("必需来源必须属于候选来源")
        if self.required_source_ids and self.intent != "documents":
            raise ValueError("必需来源只适用于文档")
        if len(self.required_source_ids) > self.limit:
            raise ValueError("必需来源数量超过查询limit")
        if self.intent == "metrics":
            if not self.metric_ids or self.start_date is None or self.end_date is None:
                raise ValueError("指标查询需要指标和完整时间范围")
        elif self.metric_ids or self.dimensions or self.start_date or self.end_date:
            raise ValueError("文档及当前业务查询不能包含指标条件")
        if (self.start_date is not None and self.end_date is not None
                and self.end_date <= self.start_date):
            raise ValueError("查询结束日期必须晚于开始日期")
        if self.intent != "documents" and self.retrieval_mode != "hybrid":
            raise ValueError("图检索只适用于文档")
        return self


class DataQuestion(ProtocolModel):
    question: NonEmptyString
    source_ids: list[NonEmptyString] = Field(min_length=1)
    required_source_ids: list[NonEmptyString] = Field(
        default_factory=list, exclude_if=lambda value: not value,
    )
    retrieval_mode: Literal["hybrid", "graph"] = "hybrid"
    snapshot_id: SHA256 | None = None

    @model_validator(mode="after")
    def unique_sources(self):
        if len(self.source_ids) != len(set(self.source_ids)):
            raise ValueError("问题来源重复")
        if len(set(self.required_source_ids)) != len(self.required_source_ids):
            raise ValueError("必需来源重复")
        if not set(self.required_source_ids).issubset(self.source_ids):
            raise ValueError("必需来源必须属于候选来源")
        return self


class SourceRegistration(ProtocolModel):
    source_id: NonEmptyString
    kind: Literal["document", "analytics", "business", "unavailable"]
    title: NonEmptyString
    revision: NonEmptyString
    nature: SourceNature
    license_id: NonEmptyString
    sha256: SHA256
    properties: JSONObject = Field(default_factory=dict)
    relative_path: NonEmptyString | None = None
    resource: ResourceRef | None = None

    @model_validator(mode="after")
    def document_identity(self):
        if self.kind == "document":
            if not self.relative_path or self.resource is None or not self.properties.get("record_id"):
                raise ValueError("文档登记需要原件路径、引用和逻辑记录编号")
            if self.resource.module_id != "data_center" or self.resource.sha256 != self.sha256:
                raise ValueError("文档原件引用与登记摘要不一致")
            if self.resource.version != self.revision:
                raise ValueError("文档原件引用与登记版本不一致")
        return self


class GrantRegistration(ProtocolModel):
    actor_id: NonEmptyString
    source_id: NonEmptyString
    permissions: list[Literal["read", "sync"]] = Field(default_factory=list)
    asset_ids: list[NonEmptyString] = Field(default_factory=list)
    source_wide: bool = False

    @model_validator(mode="after")
    def unique_grant(self):
        if len(self.permissions) != len(set(self.permissions)) or len(self.asset_ids) != len(set(self.asset_ids)):
            raise ValueError("授权操作或资料编号重复")
        return self


class DataResult(ProtocolModel):
    result_id: NonEmptyString
    status: DataStatus
    task_id: NonEmptyString | None = None
    run_id: NonEmptyString | None = None
    answer: NonEmptyString | None = None
    rows: list[JSONObject] = Field(default_factory=list)
    evidence: list[DataEvidenceRef] = Field(default_factory=list)
    snapshots: list[DataSnapshot] = Field(default_factory=list)
    metric_versions: dict[str, NonEmptyString] = Field(default_factory=dict)
    unknowns: list[NonEmptyString] = Field(default_factory=list)
    call_ids: list[NonEmptyString] = Field(default_factory=list)
    spent_cny: Decimal = Field(default=Decimal(0), ge=0)
    reserved_cny: Decimal = Field(default=Decimal(0), ge=0)
    metadata: JSONObject = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_completion(self):
        if self.status in {"completed", "partial"}:
            if not self.evidence or not self.snapshots:
                raise ValueError("结果缺少来源快照或证据")
            snapshots = {snapshot.snapshot_id: snapshot for snapshot in self.snapshots}
            for ref in self.evidence:
                snapshot = snapshots.get(ref.snapshot_id)
                if snapshot is None or snapshot.state != "published":
                    raise ValueError("引用没有绑定已发布快照")
                if (ref.source_id not in snapshot.source_ids
                        or snapshot.source_revisions[ref.source_id] != ref.source_revision):
                    raise ValueError("引用与来源快照不一致")
            if self.status == "completed" and self.unknowns:
                raise ValueError("有未解决事项的结果不能完成")
        elif self.answer is not None or self.rows:
            raise ValueError("未完成状态不能提供最终答案或结果表")
        if self.status == "DATA_BLOCKED" and not self.unknowns:
            raise ValueError("资料不足状态必须说明缺失内容")
        return self


class AuthorizedScope(ProtocolModel):
    actor_id: NonEmptyString
    source_ids: list[NonEmptyString]
    source_wide: list[NonEmptyString] = Field(default_factory=list)
    asset_ids: list[NonEmptyString] = Field(default_factory=list)
    employee_ids: list[NonEmptyString] = Field(default_factory=list)
    customer_id: int | None = Field(default=None, ge=1)
    organization: bool = False
    source_admin: bool = False
    sync_actor: bool = False
    policy_version: SHA256
    fingerprint: SHA256

    @model_validator(mode="after")
    def validate_sources(self):
        if not set(self.source_wide).issubset(self.source_ids):
            raise ValueError("来源全范围授权超出允许来源")
        return self


class SourcePage(ProtocolModel):
    snapshot: DataSnapshot
    rows: list[JSONObject]
    next_cursor: NonEmptyString | None = None
    deleted_ids: list[NonEmptyString] = Field(default_factory=list)
    mode: Literal["snapshot", "incremental"]


class DocumentInput(ProtocolModel):
    source_id: NonEmptyString
    record_id: NonEmptyString
    source_revision: NonEmptyString
    source_nature: SourceNature
    license_id: NonEmptyString
    resource: ResourceRef
    title: NonEmptyString
    language: NonEmptyString = "eng"


class DocumentChunk(ProtocolModel):
    ref: DataEvidenceRef
    text: NonEmptyString
    text_sha256: SHA256
    metadata: JSONObject = Field(default_factory=dict)


class DocumentIngest(ProtocolModel):
    documents: list[DocumentInput] = Field(min_length=1, max_length=100)
    graph_enabled: bool = False


class DocumentSearchHit(ProtocolModel):
    rank: int = Field(ge=1)
    chunk: DocumentChunk
    lexical_score: float | None = None
    vector_distance: float | None = None
    rrf_score: float | None = None


class DocumentSearchResult(ProtocolModel):
    status: DataStatus
    snapshots: list[DataSnapshot] = Field(default_factory=list)
    hits: list[DocumentSearchHit] = Field(default_factory=list)
    call_ids: list[NonEmptyString] = Field(default_factory=list)
    metadata: JSONObject = Field(default_factory=dict)


class ModelInvocation(ProtocolModel):
    task_id: NonEmptyString
    run_id: NonEmptyString
    step_key: NonEmptyString
    kind: Literal["chat", "embedding"]
    model: Literal["deepseek-flash", "qwen3.8-flash", "qwen3.7-text-embedding"]
    messages: list[JSONObject] = Field(default_factory=list)
    texts: list[NonEmptyString] = Field(default_factory=list)
    source_ids: list[NonEmptyString] = Field(min_length=1)
    reasoning_effort: Literal["low", "high"] = "low"
    json_mode: bool = False
    max_output_tokens: int = Field(default=4096, ge=1, le=16384)
    timeout_seconds: float = Field(default=90.0, gt=0, le=90)

    @model_validator(mode="after")
    def validate_inputs(self):
        if self.max_output_tokens > 8192 and (self.kind != "chat" or self.step_key != "answer"):
            raise ValueError("只有最终回答允许超过8192输出tokens")
        if self.kind == "embedding":
            if self.model != "qwen3.7-text-embedding" or not self.texts or self.messages or self.json_mode:
                raise ValueError("embedding模型或输入无效")
            if len(self.texts) > 20:
                raise ValueError("embedding批次超过20条")
        elif (not self.messages or self.texts
              or self.model == "qwen3.7-text-embedding"):
            raise ValueError("文字模型或消息输入无效")
        return self


class ModelReply(ProtocolModel):
    request_id: NonEmptyString
    status: Literal["reserved", "sent", "completed", "failed", "cancelled", "unknown"]
    response: JSONObject | None = None
    spent_cny: Decimal = Field(default=Decimal(0), ge=0)
    reserved_cny: Decimal = Field(default=Decimal(0), ge=0)

    @model_validator(mode="after")
    def validate_response(self):
        if self.status == "completed" and self.response is None:
            raise ValueError("完成的模型请求缺少真实响应")
        return self


@runtime_checkable
class AuthorizationPort(Protocol):
    async def authorize(self, context: CallContext, source_ids: list[str],
                        action: str) -> AuthorizedScope: ...


@runtime_checkable
class SourceReadPort(Protocol):
    async def snapshot(self, source_id: str, context: CallContext) -> DataSnapshot: ...
    async def read_page(self, source_id: str, snapshot_id: str, cursor: str | None,
                        limit: int, context: CallContext) -> SourcePage: ...
    async def read_current(self, source_id: str, record_id: str,
                           context: CallContext) -> JSONObject: ...
    async def read_file(self, ref: ResourceRef, context: CallContext) -> bytes: ...


@runtime_checkable
class ModelGateway(Protocol):
    async def invoke(self, request: ModelInvocation, context: CallContext) -> ModelReply: ...
    async def reuse(self, request: ModelInvocation, original_request_id: str,
                    context: CallContext) -> ModelReply: ...
    async def lookup(self, task_id: str, step_key: str,
                     context: CallContext) -> ModelReply | None: ...


__all__ = [
    "AuthorizationPort", "AuthorizedScope", "DataAsset", "DataEvidenceRef", "DataQuery", "DataQuestion",
    "DataResult", "DataSnapshot", "DataStatus", "DocumentChunk", "DocumentIngest",
    "DocumentInput", "DocumentSearchHit", "DocumentSearchResult", "GrantRegistration", "MetricDefinition",
    "ModelGateway", "ModelInvocation", "ModelReply",
    "QueryIntent", "SourceNature", "SourcePage", "SourceReadPort", "SourceRegistration",
]
