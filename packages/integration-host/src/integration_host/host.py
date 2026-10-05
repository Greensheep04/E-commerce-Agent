import asyncio
import inspect
from dataclasses import dataclass

from integration_protocol import (
    AuthenticationCapability,
    CallContext,
    HostCapabilities,
    HostCapability,
    ModuleAdapter,
    ModuleDescriptor,
    OperationResult,
)
from pydantic import BaseModel, ValidationError

from .errors import (
    DuplicateModule,
    HostNotStarted,
    InvalidAdapter,
    InvalidInput,
    InvalidOutput,
    MissingCapability,
    UnknownModule,
    UnknownOperation,
    UnsupportedOperation,
    UnsupportedVersion,
)


@dataclass
class RegisteredModule:
    adapter: ModuleAdapter
    descriptor: ModuleDescriptor
    input_models: dict[str, type[BaseModel]]
    output_models: dict[str, type[BaseModel]]


class ModuleHost:
    def __init__(self, capabilities: HostCapabilities | None = None):
        if capabilities is not None and not isinstance(capabilities, HostCapabilities):
            raise TypeError("capabilities必须使用HostCapabilities")
        self.capabilities = capabilities if capabilities is not None else HostCapabilities()
        self._modules: dict[str, RegisteredModule] = {}
        self._started: list[RegisteredModule] = []
        self._running = False
        self._transition = asyncio.Lock()
        self._idle = asyncio.Event()
        self._idle.set()
        self._active = 0

    def register(self, adapter: ModuleAdapter) -> None:
        if self._running or self._transition.locked():
            raise InvalidAdapter("宿主启动后不能登记模块")
        for name in ("describe", "startup", "shutdown", "invoke"):
            if not callable(getattr(adapter, name, None)):
                raise InvalidAdapter(f"adapter缺少{name}方法")
        for name in ("startup", "shutdown", "invoke"):
            if not inspect.iscoroutinefunction(getattr(adapter, name)):
                raise InvalidAdapter(f"adapter的{name}方法必须使用async")
        descriptor = adapter.describe()
        if not isinstance(descriptor, ModuleDescriptor):
            raise InvalidAdapter("describe必须返回ModuleDescriptor")
        try:
            descriptor = ModuleDescriptor.model_validate(descriptor)
        except ValidationError as error:
            raise InvalidAdapter("模块说明未通过校验") from error
        if descriptor.module_id in self._modules:
            raise DuplicateModule(f"模块已经登记：{descriptor.module_id}")
        input_models = getattr(adapter, "input_models", None)
        output_models = getattr(adapter, "output_models", None)
        if not isinstance(input_models, dict) or not isinstance(output_models, dict):
            raise InvalidAdapter("adapter必须登记输入与输出模型字典")
        supported = {operation.name for operation in descriptor.operations if operation.supported}
        declared = {operation.name for operation in descriptor.operations}
        if not supported.issubset(input_models) or not supported.issubset(output_models):
            raise InvalidAdapter("受支持操作必须登记输入与输出模型")
        if not set(input_models).issubset(declared) or not set(output_models).issubset(declared):
            raise InvalidAdapter("模型登记包含未声明操作")
        for operation in descriptor.operations:
            for models, schema_field in (
                (input_models, "input_schema"), (output_models, "output_schema")
            ):
                if operation.name not in models:
                    continue
                model = models[operation.name]
                if not inspect.isclass(model) or not issubclass(model, BaseModel):
                    raise InvalidAdapter("登记模型必须继承Pydantic BaseModel")
                if schema_field == "input_schema" and model.model_config.get("extra") != "forbid":
                    raise InvalidAdapter("输入模型必须禁止额外字段")
                schema = model.model_json_schema()
                declared_schema = getattr(operation, schema_field)
                if declared_schema and declared_schema != schema:
                    raise InvalidAdapter("操作说明与登记模型的JSON Schema不一致")
                setattr(operation, schema_field, schema)
        self._modules[descriptor.module_id] = RegisteredModule(
            adapter=adapter, descriptor=descriptor,
            input_models=dict(input_models), output_models=dict(output_models),
        )

    def describe(self) -> list[ModuleDescriptor]:
        return [item.descriptor.model_copy(deep=True) for item in self._modules.values()]

    def _check_capabilities(self) -> None:
        required = {
            capability for item in self._modules.values()
            for capability in item.descriptor.required_capabilities
        }
        for capability in sorted(required, key=lambda item: item.value):
            service = self.capabilities.get(capability)
            if service is None or not callable(getattr(service, "check_available", None)):
                raise MissingCapability(f"缺少宿主能力：{capability.value}")
            if (
                capability == HostCapability.AUTHENTICATION
                and not isinstance(service, AuthenticationCapability)
            ):
                raise MissingCapability("身份验证能力缺少verify方法")
            if inspect.iscoroutinefunction(service.check_available):
                raise MissingCapability("check_available必须使用同步方法")
            if capability == HostCapability.AUTHENTICATION and inspect.iscoroutinefunction(service.verify):
                raise MissingCapability("verify必须使用同步方法")
            service.check_available()

    async def startup(self) -> None:
        async with self._transition:
            if self._running:
                return
            self._check_capabilities()
            try:
                for item in self._modules.values():
                    await item.adapter.startup()
                    self._started.append(item)
            except BaseException:
                while self._started:
                    await self._started.pop().adapter.shutdown()
                raise
            self._running = True

    async def shutdown(self) -> None:
        async with self._transition:
            self._running = False
            await self._idle.wait()
            while self._started:
                await self._started.pop().adapter.shutdown()

    async def invoke(
        self, module_id: str, operation: str, payload: dict,
        context: CallContext, version: str = "1",
    ) -> OperationResult:
        if not self._running:
            raise HostNotStarted("宿主尚未启动")
        item = self._modules.get(module_id)
        if item is None:
            raise UnknownModule(f"模块未登记：{module_id}")
        if version != item.descriptor.version:
            raise UnsupportedVersion(f"模块不支持接口版本：{version}")
        description = next(
            (entry for entry in item.descriptor.operations if entry.name == operation), None
        )
        if description is None:
            raise UnknownOperation(f"操作未登记：{operation}")
        if not description.supported:
            raise UnsupportedOperation(f"操作未支持：{operation}")
        try:
            context = CallContext.model_validate(context)
        except ValidationError as error:
            raise InvalidInput("调用上下文未通过校验") from error
        if HostCapability.AUTHENTICATION in item.descriptor.required_capabilities:
            self.capabilities.authentication.verify(context)
        if not isinstance(payload, dict):
            raise InvalidInput("payload必须使用JSON对象")
        try:
            validated = item.input_models[operation].model_validate(payload, strict=True)
        except ValidationError as error:
            raise InvalidInput("操作输入未通过登记模型校验") from error
        self._active += 1
        self._idle.clear()
        try:
            result = await item.adapter.invoke(operation, validated, context)
            if not isinstance(result, OperationResult):
                raise InvalidOutput("adapter必须返回OperationResult")
            try:
                result = OperationResult.model_validate(result)
                item.output_models[operation].model_validate(result.data, strict=True)
            except ValidationError as error:
                raise InvalidOutput("操作输出未通过登记模型校验") from error
            return result
        finally:
            self._active -= 1
            if self._active == 0:
                self._idle.set()
