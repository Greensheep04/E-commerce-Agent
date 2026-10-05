import asyncio
import base64
from datetime import date
from decimal import Decimal
from hashlib import sha256
from pathlib import PurePosixPath
from uuid import UUID

from integration_protocol import (
    HostCapability,
    ModuleDescriptor,
    OperationDescriptor,
    OperationResult,
    ResourceRef,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_serializer

from shop.assessment import AssessmentService
from shop.assessment_service import principal_for
from shop.campaign import CampaignService
from shop.materials import MaterialInput, MaterialRepository
from shop.models import Asset, AssetVersion, Source
from shop.orders import OrderQueryService
from shop.poster import PosterService


class InputModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EmptyInput(InputModel):
    pass


class PromptInput(InputModel):
    prompt: str = Field(min_length=1, max_length=4000,
                        description="用户的业务需求原文。具体处理流程、输出结构和来源保存由整体Agent提供。")


class TaskInput(InputModel):
    task_id: str = Field(min_length=36, max_length=36)


class AssetInput(InputModel):
    asset_id: str = Field(min_length=36, max_length=36)


class MaterialSearch(InputModel):
    title: str | None = None
    kind: str | None = None
    tag: str | None = None
    owner_id: str | None = None
    source_id: str | None = None
    source_revision: str | None = None
    attributes: dict[str, str] = Field(default_factory=dict, description="匹配素材原始属性；Fashion来源字段包括gender（Men/Women）、baseColour（Black/Blue等）、articleType（Tshirts等）、usage（Casual等）。字段和值按来源原文匹配。")
    limit: int = Field(default=50, ge=1, le=200)
    offset: int = Field(default=0, ge=0)


class MaterialWrite(InputModel):
    source_id: str
    source_revision: str
    record_id: str
    title: str
    kind: str
    source_order: int = Field(ge=0)
    original_fields: dict[str, JsonValue]
    filename: str
    mime_type: str
    file_base64: str = Field(min_length=1)
    tags: list[str] = Field(default_factory=list)


class ConversationInput(InputModel):
    conversation_id: str = Field(min_length=36, max_length=36)


class ConversationCreate(InputModel):
    title: str = Field(default="客服咨询", min_length=1, max_length=200)


class MessageInput(ConversationInput):
    message: str = Field(min_length=1, max_length=4000)
    confirm_pending: str | None = None
    require_ticket_confirmation: bool = False

    @model_serializer(mode="wrap")
    def serialize(self, handler):
        data = handler(self)
        if not self.require_ticket_confirmation:
            data.pop("require_ticket_confirmation")
        return data


class TurnInput(InputModel):
    turn_id: str = Field(min_length=36, max_length=36)


class EventsInput(ConversationInput):
    after: int = Field(default=0, ge=0)


class OrdersInput(InputModel):
    order_number: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    product: str | None = None
    limit: int = Field(default=20, ge=1, le=20)
    offset: int = Field(default=0, ge=0)


class OrderInput(InputModel):
    order_number: str


class AssessmentCreate(PromptInput):
    parent_id: str | None = None


class RunInput(InputModel):
    run_id: str = Field(min_length=36, max_length=36)


class MetricsInput(InputModel):
    scope: str = Field(default="self", pattern="^(self|team|employees|organization)$")
    employee_ids: list[int] = Field(default_factory=list)
    period_start: str = "2023-07-01"
    period_end: str = "2023-10-01"


class EvidenceInput(InputModel):
    employee_id: int


class PosterPrepare(InputModel):
    task_id: str = Field(min_length=36, max_length=36)


class PosterDirect(InputModel):
    keywords: list[str] = Field(min_length=2, max_length=5)
    version_ids: list[str] = Field(min_length=1, max_length=2)
    constraints: dict[str, JsonValue] = Field(default_factory=dict)


class JobInput(InputModel):
    job_id: str = Field(min_length=36, max_length=36)


class RestoreInput(JobInput):
    run_id: str


class DataOutput(BaseModel):
    model_config = ConfigDict(extra="allow")


class ListOutput(DataOutput):
    items: list[dict[str, JsonValue]]


class TaskOutput(DataOutput):
    task_id: str
    owner_id: str | None
    run_id: str
    status: str


class AssetOutput(DataOutput):
    id: str
    source_id: str
    source_revision: str
    record_id: str
    versions: list[dict[str, JsonValue]]


class ConversationOutput(DataOutput):
    id: str
    title: str
    orders: list[dict[str, JsonValue]]
    turns: list[dict[str, JsonValue]]


class TurnOutput(DataOutput):
    id: str
    conversation_id: str
    status: str


class AssessmentOutput(DataOutput):
    id: str
    run_id: str
    status: str


class IdentityOutput(DataOutput):
    name: str
    person_name: str | None
    store_name: str | None
    customer_id: int
    source_id: str
    revision: str
    is_sample: bool


class OrderOutput(DataOutput):
    order_id: int
    order_number: str
    order_date: str
    subtotal: str
    total_due: str
    items: list[dict[str, JsonValue]]
    source: dict[str, JsonValue]
    is_sample: bool


class OrdersOutput(DataOutput):
    orders: list[OrderOutput]
    total: int
    limit: int
    offset: int
    has_more: bool


class EventsOutput(DataOutput):
    events: list[dict[str, JsonValue]]
    last_id: int


class MetricsOutput(DataOutput):
    period_start: str
    period_end: str
    rule_version: str
    notice: str
    source: dict[str, JsonValue]
    employees: list[dict[str, JsonValue]]
    summary: dict[str, JsonValue]
    unassigned: dict[str, JsonValue] | None


class EvidenceOutput(DataOutput):
    employee_id: int
    orders: list[dict[str, JsonValue]]
    source: dict[str, JsonValue]
    quota: dict[str, JsonValue] | None


class PosterStatusOutput(DataOutput):
    roles: dict[str, str]
    interface_version: str
    budget: dict[str, JsonValue]


class RestoredOutput(DataOutput):
    run_id: str
    configuration: dict[str, JsonValue]
    usage: dict[str, JsonValue]
    final_result: JsonValue


class PosterOutput(DataOutput):
    job: dict[str, JsonValue]
    registration: dict[str, JsonValue]
    cost: dict[str, JsonValue]


class ShopAdapter:
    module_id = ""
    operations = {}
    unsupported = ()
    operation_descriptions = {}

    def __init__(self, gateway):
        self.gateway = gateway
        self.input_models = {name: item[0] for name, item in self.operations.items()}
        self.output_models = {name: item[1] for name, item in self.operations.items()}

    def describe(self):
        capabilities = [HostCapability.AUTHENTICATION, HostCapability.PERSISTENCE]
        if self.module_id in {"harness", "customer", "assessment", "poster"}:
            capabilities.append(HostCapability.BUDGET)
        return ModuleDescriptor(module_id=self.module_id, operations=[
            OperationDescriptor(name=name, description=self.operation_descriptions.get(name, ""))
            for name in self.operations
        ] + [OperationDescriptor(name=name, supported=False) for name in self.unsupported],
            required_capabilities=capabilities)

    async def startup(self):
        pass

    async def shutdown(self):
        pass

    async def invoke(self, operation, payload, context):
        self.gateway.auth.verify(context)
        if operation not in self.operations:
            raise ValueError("模块未支持该操作")
        await asyncio.to_thread(self.gateway.validate_upstream, context)
        return await asyncio.to_thread(getattr(self, "_" + operation), payload, context)

    def bind(self, operation, payload, context, create):
        return self.gateway.store.bind(self.module_id, operation, payload.model_dump(mode="json"),
                                       context, create)

    def result(self, data, *, status="completed", reference=None, run_id=None):
        requests = data.get("requests", [])
        costs = {"spent_cny": str(sum((Decimal(item["charged_cny"])
                                       for item in requests if item["charged_cny"] is not None), Decimal(0))),
                 "reserved_cny": str(sum((Decimal(item.get("reservation_cny", item.get("reserved_cny")))
                                          for item in requests if item["charged_cny"] is None), Decimal(0)))}
        return OperationResult(data=data, status=status, run_id=run_id,
                               resources=[] if reference is None else [reference],
                               costs=data.get("cost", costs))


class MaterialsAdapter(ShopAdapter):
    module_id = "materials"
    operations = {"search": (MaterialSearch, ListOutput), "show": (AssetInput, AssetOutput),
                  "save": (MaterialWrite, AssetOutput)}
    operation_descriptions = {
        "search": "按原始属性查询来源素材，返回data.items。素材条目的id为asset_id，versions中各项id为version_id。",
        "show": "读取素材及其来源版本，返回data.id和data.versions，各版本包含id、version、file_sha256及original_fields。",
    }

    @staticmethod
    def payload(asset):
        return {"id": asset.id, "source_id": asset.source_id,
                "source_revision": asset.source_revision, "record_id": asset.record_id,
                "title": asset.title, "kind": asset.kind, "owner_id": asset.owner_id,
                "versions": [{"id": item.id, "version": item.version,
                              "file_sha256": item.file_sha256, "mime_type": item.mime_type,
                              "original_fields": item.original_fields} for item in asset.versions]}

    def _search(self, payload, context):
        with self.gateway.maker() as session:
            rows = MaterialRepository(session, self.gateway.settings.data_dir).search(**payload.model_dump())
            return self.result({"items": [self.payload(item) for item in rows]})

    def _show(self, payload, context):
        with self.gateway.maker() as session:
            asset = MaterialRepository(session, self.gateway.settings.data_dir).get(payload.asset_id)
            if asset is None:
                raise LookupError("素材不存在")
            result = self.result(self.payload(asset), reference=ResourceRef(
                module_id=self.module_id, resource_type="asset", resource_id=asset.id))
            result.resources.extend(ResourceRef(
                module_id=self.module_id, resource_type="asset_version", resource_id=version.id,
                version=str(version.version), sha256=version.file_sha256,
            ) for version in asset.versions)
            return result

    def _save(self, payload, context):
        user = self.gateway.auth.actor(context.actor_id)
        if user.role != "admin":
            raise PermissionError("素材写入需要管理员权限")
        content = base64.b64decode(payload.file_base64, validate=True)
        if len(content) > self.gateway.settings.upload_max_bytes:
            raise ValueError("文件超过上传大小上限")
        item = MaterialInput(**payload.model_dump(exclude={"file_base64"}),
                             owner_id=user.id, file_bytes=content)

        def create(session):
            asset = MaterialRepository(session, self.gateway.settings.data_dir).save_in_session(item)
            return ResourceRef(module_id=self.module_id, resource_type="asset", resource_id=asset.id)

        reference = self.bind("save", payload, context, create)
        return self._show(AssetInput(asset_id=reference.resource_id), context)

    def resolve(self, resource_type, resource_id, actor_id):
        self.gateway.auth.actor(actor_id)
        with self.gateway.maker() as session:
            if resource_type == "asset":
                asset = session.get(Asset, str(UUID(resource_id)))
                if asset is None:
                    raise LookupError("素材不存在")
                if session.get(Source, (asset.source_id, asset.source_revision)) is None:
                    raise ValueError("素材来源版本不存在")
                if not asset.versions:
                    raise ValueError("素材缺少版本")
                MaterialRepository(session, self.gateway.settings.data_dir).read_file(asset.versions[-1])
                return ResourceRef(module_id=self.module_id, resource_type=resource_type, resource_id=asset.id)
            if resource_type != "asset_version":
                raise ValueError("素材引用类型无效")
            version = session.get(AssetVersion, str(UUID(resource_id)))
            if version is None:
                raise LookupError("素材版本不存在")
            if session.get(Source, (version.asset.source_id, version.asset.source_revision)) is None:
                raise ValueError("素材来源版本不存在")
            MaterialRepository(session, self.gateway.settings.data_dir).read_file(version)
            return ResourceRef(module_id=self.module_id, resource_type=resource_type,
                               resource_id=version.id, version=str(version.version), sha256=version.file_sha256)

    def read_file(self, resource_id, relative_path, actor_id):
        if relative_path != "content":
            raise ValueError("素材读取路径无效")
        self.resolve("asset_version", resource_id, actor_id)
        with self.gateway.maker() as session:
            version = session.get(AssetVersion, resource_id)
            content = MaterialRepository(session, self.gateway.settings.data_dir).read_file(version)
            return content, version.mime_type, version.file_sha256


class HarnessAdapter(ShopAdapter):
    module_id = "harness"
    operations = {"create": (PromptInput, TaskOutput), "show": (TaskInput, TaskOutput),
                  "list": (EmptyInput, ListOutput), "run": (TaskInput, TaskOutput),
                  "resume": (TaskInput, TaskOutput), "cancel": (TaskInput, TaskOutput)}
    operation_descriptions = {
        "create": "登记宣传页生成任务，prompt使用商品与页面需求原文，返回data.task_id；此操作尚未生成宣传页。",
        "run": "执行已登记任务：理解需求、查找来源素材、生成中文宣传页并审核，保存来源、文案及审核结论。宣传页位于data.result，审核位于data.result.review。",
        "show": "查询原任务及全部请求状态，返回data.task_id、data.status、data.result及data.requests。",
        "resume": "恢复原宣传页任务，继续使用已保存的请求和来源。",
    }

    def owned(self, service, task_id, actor_id):
        user = self.gateway.auth.actor(actor_id)
        data = service.show(task_id)
        if user.role != "admin" and data["owner_id"] != user.id:
            raise PermissionError("无权访问该任务")
        return data

    def _create(self, payload, context):
        service = CampaignService(self.gateway.settings)
        try:
            def create(session):
                row = service.create_in_session(session, payload.prompt, context.actor_id)
                return ResourceRef(module_id=self.module_id, resource_type="task", resource_id=row.id)
            reference = self.bind("create", payload, context, create)
            data = service.show(reference.resource_id)
            return self.result(data, status=data["status"], reference=reference, run_id=data["run_id"])
        finally:
            service.engine.dispose()

    def _task(self, operation, payload, context):
        service = CampaignService(self.gateway.settings)
        try:
            self.owned(service, payload.task_id, context.actor_id)
            reference = ResourceRef(module_id=self.module_id, resource_type="task", resource_id=payload.task_id)
            if operation != "show":
                self.bind(operation, payload, context, lambda session: reference)
                getattr(service, operation)(payload.task_id)
            data = service.show(payload.task_id)
            return self.result(data, status=data["status"], reference=reference, run_id=data["run_id"])
        finally:
            service.engine.dispose()

    def _show(self, payload, context):
        return self._task("show", payload, context)

    def _run(self, payload, context):
        return self._task("run", payload, context)

    def _resume(self, payload, context):
        return self._task("resume", payload, context)

    def _cancel(self, payload, context):
        return self._task("cancel", payload, context)

    def _list(self, payload, context):
        service = CampaignService(self.gateway.settings)
        try:
            user = self.gateway.auth.actor(context.actor_id)
            return self.result({"items": service.listing(None if user.role == "admin" else user.id)})
        finally:
            service.engine.dispose()

    def resolve(self, resource_type, resource_id, actor_id):
        if resource_type != "task":
            raise ValueError("Harness引用类型无效")
        service = CampaignService(self.gateway.settings)
        try:
            self.owned(service, resource_id, actor_id)
            return ResourceRef(module_id=self.module_id, resource_type="task", resource_id=resource_id)
        finally:
            service.engine.dispose()

    def read_file(self, resource_id, relative_path, actor_id):
        if relative_path != "page.html":
            raise ValueError("Harness读取路径无效")
        service = CampaignService(self.gateway.settings)
        try:
            self.owned(service, resource_id, actor_id)
            content = service.render(resource_id).encode()
            return content, "text/html; charset=utf-8", sha256(content).hexdigest()
        finally:
            service.engine.dispose()


class CustomerAdapter(ShopAdapter):
    module_id = "customer"
    operations = {"identity": (EmptyInput, IdentityOutput), "orders": (OrdersInput, OrdersOutput),
                  "order": (OrderInput, OrderOutput), "create_conversation": (ConversationCreate, ConversationOutput),
                  "conversation": (ConversationInput, ConversationOutput), "list": (EmptyInput, ListOutput),
                  "submit": (MessageInput, TurnOutput), "turn": (TurnInput, TurnOutput),
                  "cancel": (TurnInput, TurnOutput), "resume": (TurnInput, TurnOutput),
                  "events": (EventsInput, EventsOutput)}
    operation_descriptions = {
        "orders": "直接查询本人历史订单，支持商品、完整订单编号及分页，返回data.orders及data.source。",
        "order": "直接读取本人选定订单的完整明细和金额，返回data.order_number、data.items及data.source。",
        "create_conversation": "创建文字客服会话，返回data.id作为后续submit的conversation_id。",
        "submit": "向文字客服Agent提交订单咨询或人工工单申请，返回data.id作为turn_id；执行器随后读取turn的实际状态。客服支持订单查询、商品说明及人工工单，无法执行退款或修改订单，也不会返回refund_id。目标要求直接完成退款时，应等待所需业务能力接入；人工工单需要目标明确提出申请。待确认工单只有实际业务结果提供pending_ticket后才能确认。",
        "turn": "读取提交的客服消息及真实结果，返回data.status、data.result和data.requests；待确认内容位于data.result.pending_ticket。",
        "conversation": "查询客服会话及待确认内容，返回data.pending_ticket；为空表示当前没有待确认工单。",
    }

    async def startup(self):
        if self.gateway.owns_worker:
            await self.gateway.worker.setup()

    async def shutdown(self):
        if self.gateway.owns_worker:
            await self.gateway.worker.shutdown()

    async def invoke(self, operation, payload, context):
        result = await super().invoke(operation, payload, context)
        if operation in {"submit", "resume"} and result.data["status"] in {"queued", "running"}:
            self.gateway.worker.schedule(result.data["id"])
        return result

    def _identity(self, payload, context):
        with self.gateway.maker() as session:
            return self.result(OrderQueryService(session, context.actor_id).identity())

    def _orders(self, payload, context):
        values = payload.model_dump()
        for name in ("start_date", "end_date"):
            if values[name] is not None:
                values[name] = date.fromisoformat(values[name])
        with self.gateway.maker() as session:
            return self.result(OrderQueryService(session, context.actor_id).list_orders(**values))

    def _order(self, payload, context):
        with self.gateway.maker() as session:
            result = OrderQueryService(session, context.actor_id).get_order(payload.order_number)
            if result is None:
                raise LookupError("当前账号无法查询该订单")
            return self.result(result)

    def _create_conversation(self, payload, context):
        def create(session):
            row = self.gateway.customer.create_conversation_in_session(session, context.actor_id, payload.title)
            return ResourceRef(module_id=self.module_id, resource_type="conversation", resource_id=row.id)
        reference = self.bind("create_conversation", payload, context, create)
        return self.result(self.gateway.customer.show_conversation(reference.resource_id, context.actor_id),
                           reference=reference)

    def _conversation(self, payload, context):
        return self.result(self.gateway.customer.show_conversation(payload.conversation_id, context.actor_id))

    def _list(self, payload, context):
        return self.result({"items": self.gateway.customer.list_conversations(context.actor_id)})

    def _submit(self, payload, context):
        request_id = str(UUID(context.request_id))
        def create(session):
            data = self.gateway.customer.submit_in_session(
                session, payload.conversation_id, context.actor_id, request_id,
                payload.message, confirm_pending=payload.confirm_pending,
                require_ticket_confirmation=payload.require_ticket_confirmation,
            )
            return ResourceRef(module_id=self.module_id, resource_type="turn", resource_id=data["id"])
        reference = self.bind("submit", payload, context, create)
        data = self.gateway.customer.show_turn(reference.resource_id, context.actor_id)
        return self.result(data, status=data["status"], reference=reference, run_id=data["id"])

    def _turn(self, payload, context):
        data = self.gateway.customer.show_turn(payload.turn_id, context.actor_id)
        return self.result(data, status=data["status"], run_id=data["id"])

    def _cancel(self, payload, context):
        self.gateway.customer.show_turn(payload.turn_id, context.actor_id)
        reference = ResourceRef(module_id=self.module_id, resource_type="turn", resource_id=payload.turn_id)
        self.bind("cancel", payload, context, lambda session: reference)
        data = self.gateway.customer.cancel(payload.turn_id, context.actor_id)
        return self.result(data, status=data["status"], reference=reference)

    def _resume(self, payload, context):
        self.gateway.customer.show_turn(payload.turn_id, context.actor_id)
        reference = ResourceRef(module_id=self.module_id, resource_type="turn", resource_id=payload.turn_id)
        self.bind("resume", payload, context, lambda session: reference)
        data = self.gateway.customer.resume(payload.turn_id, context.actor_id)
        return self.result(data, status=data["status"], reference=reference)

    def _events(self, payload, context):
        return self.result(self.gateway.customer.events(payload.conversation_id, context.actor_id, payload.after))

    def resolve(self, resource_type, resource_id, actor_id):
        if resource_type == "conversation":
            self.gateway.customer.show_conversation(resource_id, actor_id)
        elif resource_type == "turn":
            self.gateway.customer.show_turn(resource_id, actor_id)
        else:
            raise ValueError("客服引用类型无效")
        return ResourceRef(module_id=self.module_id, resource_type=resource_type, resource_id=resource_id)


class AssessmentAdapter(ShopAdapter):
    module_id = "assessment"
    operations = {"create": (AssessmentCreate, AssessmentOutput), "show": (RunInput, AssessmentOutput),
                  "list": (EmptyInput, ListOutput), "run": (RunInput, AssessmentOutput),
                  "resume": (RunInput, AssessmentOutput), "cancel": (RunInput, AssessmentOutput),
                  "metrics": (MetricsInput, MetricsOutput), "evidence": (EvidenceInput, EvidenceOutput)}
    operation_descriptions = {
        "create": "登记销售考核报告任务，返回data.id作为后续run的run_id；此操作尚未生成报告。",
        "run": "执行已登记考核任务，保存授权范围内的真实指标、订单证据和报告，报告位于data.result。",
        "metrics": "直接查询授权员工范围的销售指标和统计结果，返回data.employees、data.summary及data.source。",
        "evidence": "直接读取授权员工的真实考核订单证据，返回data.employee_id、data.orders及data.source。",
    }

    def service(self):
        return AssessmentService(self.gateway.settings, self.gateway.metrics)

    def _create(self, payload, context):
        service = self.service()
        try:
            def create(session):
                row = service.create_in_session(session, payload.prompt, context.actor_id, payload.parent_id)
                return ResourceRef(module_id=self.module_id, resource_type="run", resource_id=row)
            reference = self.bind("create", payload, context, create)
            data = service.show(reference.resource_id, context.actor_id)
            return self.result(data, status=data["status"], reference=reference, run_id=data["run_id"])
        finally:
            service.close()

    def _task(self, operation, payload, context):
        service = self.service()
        try:
            service.show(payload.run_id, context.actor_id)
            reference = ResourceRef(module_id=self.module_id, resource_type="run", resource_id=payload.run_id)
            if operation != "show":
                self.bind(operation, payload, context, lambda session: reference)
                if operation == "cancel":
                    service.cancel(payload.run_id, context.actor_id)
                else:
                    service.execute(payload.run_id, context.actor_id)
            data = service.show(payload.run_id, context.actor_id)
            return self.result(data, status=data["status"], reference=reference, run_id=data["run_id"])
        finally:
            service.close()

    def _show(self, payload, context):
        return self._task("show", payload, context)

    def _run(self, payload, context):
        return self._task("run", payload, context)

    def _resume(self, payload, context):
        return self._task("resume", payload, context)

    def _cancel(self, payload, context):
        return self._task("cancel", payload, context)

    def _list(self, payload, context):
        service = self.service()
        try:
            return self.result({"items": service.listing(context.actor_id)})
        finally:
            service.close()

    def _metrics(self, payload, context):
        with self.gateway.maker() as session:
            return self.result(self.gateway.metrics.query(session, principal_for(session, context.actor_id),
                                                          **payload.model_dump()))

    def _evidence(self, payload, context):
        with self.gateway.maker() as session:
            return self.result(self.gateway.metrics.evidence(session, principal_for(session, context.actor_id),
                                                             payload.employee_id))

    def resolve(self, resource_type, resource_id, actor_id):
        if resource_type != "run":
            raise ValueError("考核引用类型无效")
        service = self.service()
        try:
            service.show(resource_id, actor_id)
            return ResourceRef(module_id=self.module_id, resource_type=resource_type, resource_id=resource_id)
        finally:
            service.close()


class PosterAdapter(ShopAdapter):
    module_id = "poster"
    operations = {"status": (EmptyInput, PosterStatusOutput), "prepare": (PosterPrepare, PosterOutput),
                  "prepare_direct": (PosterDirect, PosterOutput), "show": (JobInput, PosterOutput),
                  "run": (JobInput, PosterOutput), "restore": (RestoreInput, RestoredOutput)}
    unsupported = ("cancel", "resume")
    operation_descriptions = {
        "status": "查询完整海报所需角色的登记状态，data.roles中的待接入表示缺少业务角色。",
        "prepare": "依据已完成并审核的Harness宣传页准备海报关键词，输入task_id为Harness任务编号，返回data.job.job_id及data.job.keyword_plan。",
        "prepare_direct": "依据真实素材版本准备海报关键词，version_ids使用素材版本id，keywords为2至5个商品或风格关键词，返回data.job.job_id及data.job.keyword_plan。",
        "run": "生成完整海报。输入job_id来自data.job.job_id；所需角色尚未登记时返回waiting_agents，等待角色接入。",
        "show": "查询原海报任务、关键词及角色状态，返回data.job。",
    }

    def service(self):
        return PosterService(self.gateway.settings)

    def _status(self, payload, context):
        service = self.service()
        try:
            return self.result(service.status())
        finally:
            service.engine.dispose()

    def _prepared(self, operation, payload, context):
        service = self.service()
        try:
            def create(session):
                if operation == "prepare":
                    inputs = service.read_harness(payload.task_id, context.actor_id)
                    job = service.create_job(inputs, context.actor_id)
                else:
                    job = service.prepare_direct(tuple(payload.keywords), payload.version_ids,
                                                 context.actor_id, payload.constraints)
                return ResourceRef(module_id=self.module_id, resource_type="job", resource_id=job.job_id)
            reference = self.bind(operation, payload, context, create)
            if operation == "prepare":
                service.prepare(payload.task_id, context.actor_id)
            data = service.show(reference.resource_id, context.actor_id)
            return self.result(data, status=data["job"]["status"], reference=reference,
                               run_id=data["job"]["preparation_run_id"])
        finally:
            service.engine.dispose()

    def _prepare(self, payload, context):
        return self._prepared("prepare", payload, context)

    def _prepare_direct(self, payload, context):
        return self._prepared("prepare_direct", payload, context)

    def _show(self, payload, context):
        service = self.service()
        try:
            data = service.show(payload.job_id, context.actor_id)
            result = self.result(data, status=data["job"]["status"], reference=ResourceRef(
                module_id=self.module_id, resource_type="job", resource_id=payload.job_id))
            result.costs = data["cost"]
            return result
        finally:
            service.engine.dispose()

    def _run(self, payload, context):
        service = self.service()
        try:
            job = service.load_job(payload.job_id, context.actor_id)
            reference = ResourceRef(module_id=self.module_id, resource_type="job", resource_id=job.job_id)
            self.bind("run", payload, context, lambda session: reference)
            if any(value == "待接入" for value in service.status()["roles"].values()):
                service.save_job(job.model_copy(update={"status": "waiting_agents"}))
            else:
                service.run(payload.job_id, context.actor_id)
            data = service.show(payload.job_id, context.actor_id)
            return self.result(data, status=data["job"]["status"], reference=reference)
        finally:
            service.engine.dispose()

    def _restore(self, payload, context):
        service = self.service()
        try:
            saved = service.restore(payload.job_id, payload.run_id, context.actor_id)
            return self.result({"run_id": saved.run_id, "configuration": saved.configuration,
                                "usage": saved.usage.model_dump(), "final_result": saved.final_result})
        finally:
            service.engine.dispose()

    def resolve(self, resource_type, resource_id, actor_id):
        if resource_type != "job":
            raise ValueError("自媒体引用类型无效")
        service = self.service()
        try:
            service.show(resource_id, actor_id)
            return ResourceRef(module_id=self.module_id, resource_type=resource_type, resource_id=resource_id)
        finally:
            service.engine.dispose()

    def read_file(self, resource_id, relative_path, actor_id):
        path = PurePosixPath(relative_path)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != relative_path:
            raise ValueError("自媒体文件路径无效")
        service = self.service()
        try:
            job = service.load_job(resource_id, actor_id)
            runs = [service.restore(resource_id, run_id, actor_id) for run_id in job.frame_records]
            runs.extend(service.load_session(job, service.frame_root(job) / item) for item in job.agent_records)
            matches = [(run, reference) for run in runs for reference in run.file_store.artifacts
                       if "framework/" + reference.uri == relative_path]
            identities = {(ref.run_id, ref.artifact_id, ref.revision, ref.sha256) for _, ref in matches}
            if len(identities) != 1:
                raise LookupError("文件没有登记到当前海报任务")
            run, reference = matches[0]
            content = run.file_store.read(reference)
            return content, reference.media_type, reference.sha256
        finally:
            service.engine.dispose()


def build_adapters(gateway):
    adapters = {adapter.module_id: adapter(gateway) for adapter in (
        MaterialsAdapter, HarnessAdapter, CustomerAdapter, AssessmentAdapter, PosterAdapter,
    )}
    if gateway.settings.data_center_url is not None:
        from shop.data_center_adapter import DataCenterAdapter

        adapters["data_center"] = DataCenterAdapter(gateway)
    return adapters
