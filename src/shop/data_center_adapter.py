import asyncio
from datetime import date
from hashlib import sha256
from mimetypes import guess_type
from pathlib import Path
from uuid import uuid4

import httpx
from data_center_protocol import (
    DataEvidenceRef,
    DataQuery,
    DataQuestion,
    DocumentIngest,
    GrantRegistration,
    SourceRegistration,
)
from integration_protocol import (
    HostCapability,
    ModuleDescriptor,
    OperationDescriptor,
    OperationResult,
    ProtocolModel,
    ResourceRef,
)
from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import select

from shop.data_center_host import ShopDataAuthorization, ShopSourceReadPort
from shop.data_center_models import DataCenterSource


class HostDataQuery(DataQuery):
    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def json_date(cls, value):
        return date.fromisoformat(value) if isinstance(value, str) else value


class EmptyInput(ProtocolModel):
    pass


class SourceInput(ProtocolModel):
    source_id: str


class TaskInput(ProtocolModel):
    task_id: str


class ExportInput(TaskInput):
    format: str


class DataOutput(BaseModel):
    model_config = ConfigDict(extra="allow")


class DataCenterAdapter:
    module_id = "data_center"

    def __init__(self, gateway):
        self.gateway = gateway
        self.base_url = gateway.settings.data_center_url.rstrip("/")
        self.client = httpx.AsyncClient(
            base_url=self.base_url, timeout=300, trust_env=False,
            limits=httpx.Limits(keepalive_expiry=2.0),
        )
        self.authorization = ShopDataAuthorization(gateway.auth, gateway.maker)
        self.sources = ShopSourceReadPort(gateway.settings, gateway.maker, self.authorization)
        self.input_models = {"query": HostDataQuery, "ask": DataQuestion, "ingest": DocumentIngest, "sync": SourceInput,
                             "build_graph": HostDataQuery, "graph_status": HostDataQuery,
                             "show": TaskInput, "run": TaskInput, "cancel": TaskInput,
                             "reference": DataEvidenceRef, "export": ExportInput,
                             "catalog": EmptyInput, "tasks": EmptyInput, "metrics": EmptyInput,
                             "manage_sources": EmptyInput, "register_source": SourceRegistration,
                             "set_grant": GrantRegistration, "withdraw_source": SourceInput}
        self.output_models = {name: DataOutput for name in self.input_models}

    def describe(self):
        descriptions = {"query": "创建持久查询并自动执行，show读取最终答案和引用",
                        "ask": "根据中文问题选择登记来源和指标并执行持久查询",
                        "ingest": "转换并发布登记文档", "sync": "以后台账号同步登记业务来源",
                        "build_graph": "建立精确授权范围的图索引", "graph_status": "查询当前图索引状态",
                        "show": "读取任务状态和已核实产物", "run": "恢复原任务并复用原编号",
                        "cancel": "停止任务新增调度与模型调用", "reference": "按当前权限回读原始引用",
                        "export": "导出当前授权结果的Markdown或CSV", "catalog": "列出当前账号可见资料",
                        "tasks": "列出当前账号任务", "metrics": "列出指标定义与公式",
                        "manage_sources": "管理员列出全部来源及撤回状态",
                        "register_source": "管理员登记有明确原件和来源的资料",
                        "set_grant": "管理员设置资料读取或后台同步授权",
                        "withdraw_source": "管理员撤回资料来源"}
        return ModuleDescriptor(module_id=self.module_id,
                                operations=[OperationDescriptor(name=name, description=descriptions[name])
                                            for name in self.input_models],
                                required_capabilities=[HostCapability.AUTHENTICATION,
                                                       HostCapability.PERSISTENCE, HostCapability.BUDGET])

    async def startup(self):
        response = await self.client.get("/health")
        response.raise_for_status()
        if response.json() != {"status": "ok"}:
            raise ValueError("数据中台服务健康检查无效")

    async def shutdown(self):
        await self.client.aclose()

    @staticmethod
    def headers(context):
        if context.credential is None:
            raise PermissionError("数据中台调用缺少宿主认证凭据")
        return {"Authorization": "Bearer " + context.credential.get_secret_value(),
                "X-Shop-Actor": context.actor_id, "X-Request-ID": context.request_id}

    async def invoke(self, operation, payload, context):
        self.gateway.auth.verify(context)
        await asyncio.to_thread(self.gateway.validate_upstream, context)
        response = await self.client.post("/operations/" + operation,
                                          json={"payload": payload.model_dump(mode="json"),
                                                "upstream": [ref.model_dump(mode="json") for ref in context.upstream]},
                                          headers=self.headers(context))
        self._check(response)
        result = OperationResult.model_validate_json(response.content)
        if operation in {"query", "ask", "ingest", "sync", "build_graph"}:
            task_id = self.task_id_from_result(result)
            reference = ResourceRef(module_id=self.module_id, resource_type="task", resource_id=task_id)
            self.gateway.store.bind(self.module_id, operation, payload.model_dump(mode="json"),
                                    context, lambda session: reference)
        return result

    @staticmethod
    def task_id_from_result(result):
        task_id, result_id = result.data.get("task_id"), result.data.get("result_id")
        if task_id is not None and result_id is not None and task_id != result_id:
            raise ValueError("数据中台返回的任务编号冲突")
        identifier = task_id if task_id is not None else result_id
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("数据中台没有返回原始任务编号")
        return identifier

    @staticmethod
    def _check(response):
        if response.status_code in {401, 403}:
            raise PermissionError("数据中台拒绝当前身份的操作")
        if response.status_code in {404, 422}:
            raise ValueError(response.json()["detail"])
        response.raise_for_status()

    def resolve(self, resource_type, resource_id, actor_id):
        context = self.gateway.auth.issue(actor_id, str(uuid4()))
        if resource_type == "source_metadata":
            if self.gateway.auth.actor(actor_id).role != "admin":
                raise PermissionError("来源管理资源需要管理员权限")
            with self.gateway.maker() as session:
                source = session.get(DataCenterSource, resource_id)
                if source is None:
                    raise LookupError("来源没有登记")
                from shop.data_center_host import content_digest

                return ResourceRef(module_id=self.module_id, resource_type=resource_type,
                                   resource_id=resource_id, version=source.revision,
                                   sha256=content_digest({"source": source.source_id, "revision": source.revision,
                                                          "active": source.active, "sha256": source.sha256}))
        if resource_type in {"source", "document"}:
            with self.gateway.maker() as session:
                sources = session.scalars(select(DataCenterSource).where(DataCenterSource.active.is_(True))).all()
                source = next((value for value in sources if value.resource is not None
                               and value.resource["resource_id"] == resource_id
                               and value.resource["resource_type"] == resource_type), None)
                if source is None:
                    raise PermissionError("来源资源未登记")
                scope = self.authorization.authorize_sync(context, [source.source_id], "read")
                if (source.source_id not in scope.source_wide
                        and f"{source.source_id}::{source.properties['record_id']}" not in scope.asset_ids):
                    raise PermissionError("来源资源不在当前授权范围")
                return ResourceRef.model_validate(source.resource)
        with httpx.Client(base_url=self.base_url, timeout=100, trust_env=False) as client:
            response = client.post("/operations/resolve", json={"payload": {
                "resource_type": resource_type, "resource_id": resource_id}}, headers=self.headers(context))
        self._check(response)
        result = OperationResult.model_validate_json(response.content)
        return ResourceRef.model_validate(result.data["reference"])

    def read_file(self, resource_id, relative_path, actor_id):
        context = self.gateway.auth.issue(actor_id, str(uuid4()))
        with self.gateway.maker() as session:
            sources = session.scalars(select(DataCenterSource).where(
                DataCenterSource.active.is_(True), DataCenterSource.kind == "document")).all()
            source = next((value for value in sources if value.resource is not None
                           and value.resource["resource_id"] == resource_id), None)
            if source is not None:
                if relative_path != Path(source.relative_path).name:
                    raise ValueError("请求文件名与登记原件不一致")
                ref = ResourceRef.model_validate(source.resource)
                raw = self.sources._read_file(ref, context)
                mime = guess_type(relative_path)[0] or "application/octet-stream"
                return raw, mime, ref.sha256
        with httpx.Client(base_url=self.base_url, timeout=100, trust_env=False) as client:
            response = client.post("/files", json={"payload": {"task_id": resource_id,
                "relative_path": relative_path}}, headers=self.headers(context))
        self._check(response)
        digest = sha256(response.content).hexdigest()
        if response.headers.get("x-content-sha256") != digest:
            raise ValueError("数据中台文件摘要不一致")
        return response.content, response.headers["content-type"].split(";")[0], digest
