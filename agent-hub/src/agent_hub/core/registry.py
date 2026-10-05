import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy

from integration_protocol import (
    Identifier,
    JSONObject,
    ModuleDescriptor,
    NonEmptyString,
    OperationResult,
    ProtocolModel,
    ResourceRef,
)
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError, ValidationError
from pydantic import Field, TypeAdapter
from pydantic import ValidationError as ModelValidationError

from agent_hub.core.types import (
    AgentRegistration,
    BackendConnector,
    HubError,
    ResourceContent,
    SharedToolRegistration,
)


class Invocation(ProtocolModel):
    payload: JSONObject
    request_id: NonEmptyString
    upstream: list[ResourceRef] = Field(default_factory=list)


class Registry:
    def __init__(self):
        self._connectors: dict[str, BackendConnector] = {}
        self._agents: dict[str, AgentRegistration] = {}
        self._shared: dict[str, SharedToolRegistration] = {}
        self._tool_names = {"list_agents"}
        self._descriptors: dict[tuple[str, str], ModuleDescriptor] = {}
        self._validators = {}
        self._closed_connectors = set()
        self._phase = "new"
        self._lifecycle_lock = asyncio.Lock()
        self._activity = asyncio.Condition()
        self._active = 0

    def _require_new(self):
        if self._phase != "new":
            raise HubError("registration_closed", "启动后不能变更登记内容")

    def register_connector(self, connector_id: str, connector: BackendConnector):
        self._require_new()
        TypeAdapter(Identifier).validate_python(connector_id)
        if connector_id in self._connectors or any(item is connector for item in self._connectors.values()):
            raise HubError("duplicate_connector", "connector名称和生命周期负责人必须唯一")
        if not isinstance(connector, BackendConnector):
            raise HubError("invalid_connector", "connector缺少必要的异步接口")
        self._connectors[connector_id] = connector

    def _register_tool(self, entry):
        if entry.connector_id not in self._connectors:
            raise HubError("unknown_connector", "登记项引用了未知connector")
        if entry.tool_name in self._tool_names:
            raise HubError("duplicate_tool", "工具名称必须唯一")
        self._tool_names.add(entry.tool_name)

    def register_agent(self, entry: AgentRegistration):
        self._require_new()
        entry = AgentRegistration.model_validate(entry).model_copy(deep=True)
        if entry.agent_id in self._agents:
            raise HubError("duplicate_agent", "Agent名称必须唯一")
        self._register_tool(entry)
        self._agents[entry.agent_id] = entry

    def register_shared_tool(self, entry: SharedToolRegistration):
        self._require_new()
        entry = SharedToolRegistration.model_validate(entry).model_copy(deep=True)
        self._register_tool(entry)
        self._shared[entry.tool_name] = entry

    def agent_registrations(self):
        return [entry.model_copy(deep=True) for entry in self._agents.values()]

    def shared_tool_registrations(self):
        return [entry.model_copy(deep=True) for entry in self._shared.values()]

    async def _close_connectors(self):
        pending = [(name, connector) for name, connector in self._connectors.items()
                   if name not in self._closed_connectors]
        results = await asyncio.gather(*(connector.close() for _, connector in pending), return_exceptions=True)
        self._closed_connectors.update(name for name, _ in pending)
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise BaseExceptionGroup("connector关闭失败", failures)

    async def startup(self):
        async with self._lifecycle_lock:
            self._require_new()
            self._phase = "starting"
            try:
                if not self._agents and not self._shared:
                    raise HubError("empty_registration", "至少登记一个Agent或共享工具")
                for connector_id, connector in self._connectors.items():
                    modules = await connector.describe()
                    if not isinstance(modules, list) or not all(isinstance(row, ModuleDescriptor) for row in modules):
                        raise HubError("invalid_catalog", "connector目录必须使用公共ModuleDescriptor")
                    for module in modules:
                        key = (connector_id, module.module_id)
                        if key in self._descriptors:
                            raise HubError("duplicate_module", "同一connector的模块名称必须唯一")
                        self._descriptors[key] = module.model_copy(deep=True)
                for entry in list(self._agents.values()) + list(self._shared.values()):
                    module = self._descriptors.get((entry.connector_id, entry.module_id))
                    if module is None:
                        raise HubError("unknown_module", "后端没有提供已登记模块")
                    if module.version != entry.expected_version:
                        raise HubError("version_mismatch", "后端接口版本与登记版本不一致")
                    allowed = entry.allowed_operations if isinstance(entry, AgentRegistration) else [entry.operation]
                    operations = {operation.name: operation for operation in module.operations}
                    for name in allowed:
                        operation = operations.get(name)
                        if operation is None:
                            raise HubError("unknown_operation", "后端没有提供已登记操作")
                        if not operation.supported:
                            raise HubError("unsupported_operation", "登记操作尚未受到后端支持")
                        for direction, schema in (("input", operation.input_schema), ("output", operation.output_schema)):
                            if not schema:
                                raise HubError("missing_schema", "登记操作缺少完整Schema")
                            try:
                                Draft202012Validator.check_schema(schema)
                            except SchemaError as error:
                                raise HubError("invalid_schema", "登记操作的Schema格式无效") from error
                            self._validators[(entry.connector_id, entry.module_id, name, direction)] = (
                                Draft202012Validator(deepcopy(schema), format_checker=FormatChecker())
                            )
                self._phase = "ready"
            except BaseException as error:
                self._phase = "failed"
                try:
                    await self._close_connectors()
                except BaseException as cleanup:
                    raise BaseExceptionGroup("启动及清理失败", [error, cleanup]) from error
                raise

    def _require_ready(self):
        if self._phase != "ready":
            raise HubError("registry_unavailable", "登记框架尚未启动或已经关闭")

    async def close(self):
        async with self._lifecycle_lock:
            if self._phase == "closed":
                return
            self._phase = "closing"
            async with self._activity:
                await self._activity.wait_for(lambda: self._active == 0)
            try:
                await self._close_connectors()
            finally:
                self._phase = "closed"

    @asynccontextmanager
    async def _call(self):
        async with self._activity:
            self._require_ready()
            self._active += 1
        try:
            yield
        finally:
            async with self._activity:
                self._active -= 1
                self._activity.notify_all()

    def describe_agents(self):
        self._require_ready()
        agents = []
        for entry in self._agents.values():
            module = self._descriptors[(entry.connector_id, entry.module_id)]
            agents.append({**entry.model_dump(mode="json"), "version": module.version,
                           "operations": [operation.model_dump(mode="json") for operation in module.operations
                                          if operation.name in entry.allowed_operations]})
        shared_tools = []
        for entry in self._shared.values():
            module = self._descriptors[(entry.connector_id, entry.module_id)]
            operation = next(operation for operation in module.operations if operation.name == entry.operation)
            shared_tools.append({**entry.model_dump(mode="json"), "version": module.version,
                                 "input_schema": deepcopy(operation.input_schema),
                                 "output_schema": deepcopy(operation.output_schema)})
        return deepcopy({"agents": agents, "shared_tools": shared_tools})

    async def _invoke(self, entry, action, payload, request_id, upstream):
        try:
            invocation = Invocation(payload=payload, request_id=request_id,
                                    upstream=[] if upstream is None else upstream)
        except ModelValidationError as error:
            raise HubError("invalid_call", "调用参数没有通过公共协议检查") from error
        async with self._call():
            validator = self._validators[(entry.connector_id, entry.module_id, action, "input")]
            try:
                validator.validate(invocation.payload)
            except ValidationError as error:
                path = ".".join(str(part) for part in error.absolute_path) or "payload"
                raise HubError("invalid_payload", f"业务输入没有通过Schema检查，字段：{path}") from error
            result = await self._connectors[entry.connector_id].invoke(
                entry.module_id, action, invocation.payload, version=entry.expected_version,
                request_id=invocation.request_id, upstream=invocation.upstream,
            )
            try:
                result = OperationResult.model_validate(result)
                self._validators[(entry.connector_id, entry.module_id, action, "output")].validate(result.data)
            except (ModelValidationError, ValidationError) as error:
                raise HubError("invalid_result", "后端业务结果没有通过登记Schema检查") from error
            return result

    async def invoke_agent(self, agent_id, action, payload, *, request_id, upstream=None):
        self._require_ready()
        entry = self._agents.get(agent_id)
        if entry is None:
            raise HubError("unknown_agent", "Agent没有登记")
        if action not in entry.allowed_operations:
            raise HubError("operation_not_allowed", "操作没有纳入启动允许范围")
        return await self._invoke(entry, action, payload, request_id, upstream)

    async def invoke_shared_tool(self, tool_name, payload, *, request_id, upstream=None):
        self._require_ready()
        entry = self._shared.get(tool_name)
        if entry is None:
            raise HubError("unknown_tool", "共享工具没有登记")
        return await self._invoke(entry, entry.operation, payload, request_id, upstream)

    async def read_file(self, connector_id, reference, relative_path):
        try:
            reference = ResourceRef.model_validate(reference)
        except ModelValidationError as error:
            raise HubError("invalid_resource", "资源引用没有通过公共协议检查") from error
        entries = list(self._agents.values()) + list(self._shared.values())
        if not any(entry.connector_id == connector_id and entry.module_id == reference.module_id for entry in entries):
            raise HubError("resource_not_allowed", "资源所属connector或模块没有纳入启动允许范围")
        if (not isinstance(relative_path, str) or not relative_path or relative_path.startswith("/")
                or "\\" in relative_path or "\x00" in relative_path
                or any(part in {"", ".", ".."} for part in relative_path.split("/"))):
            raise HubError("invalid_resource_path", "文件路径必须为规范的相对路径")
        async with self._call():
            content = await self._connectors[connector_id].read_file(reference, relative_path)
            try:
                content = ResourceContent.model_validate(content)
            except ModelValidationError as error:
                raise HubError("invalid_resource_content", "后端资源内容没有通过完整性检查") from error
            if reference.sha256 is not None and reference.sha256 != content.sha256:
                raise HubError("resource_digest_mismatch", "文件内容与引用摘要不一致")
            return content
