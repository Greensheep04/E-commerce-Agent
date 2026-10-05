from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Protocol, runtime_checkable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    SecretStr,
    StringConstraints,
    model_validator,
)

NonEmptyString = Annotated[str, StringConstraints(min_length=1, pattern=r"\S")]
Identifier = Annotated[str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_.-]*$")]
SHA256 = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
JSONObject = dict[str, JsonValue]


class ProtocolModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, allow_inf_nan=False, revalidate_instances="always"
    )


class HostCapability(str, Enum):
    AUTHENTICATION = "authentication"
    PERSISTENCE = "persistence"
    BUDGET = "budget"


class ResourceRef(ProtocolModel):
    module_id: Identifier
    resource_type: NonEmptyString
    resource_id: NonEmptyString
    version: NonEmptyString | None = None
    sha256: SHA256 | None = None


class CallContext(ProtocolModel):
    actor_id: NonEmptyString
    request_id: NonEmptyString
    upstream: list[ResourceRef] = Field(default_factory=list)
    credential: SecretStr | None = Field(default=None, exclude=True, repr=False)


class OperationResult(ProtocolModel):
    data: JSONObject
    resources: list[ResourceRef] = Field(default_factory=list)
    status: NonEmptyString
    run_id: NonEmptyString | None = None
    costs: JSONObject = Field(default_factory=dict)


class OperationDescriptor(ProtocolModel):
    name: Identifier
    description: str = ""
    supported: bool = True
    input_schema: JSONObject = Field(default_factory=dict)
    output_schema: JSONObject = Field(default_factory=dict)


class ModuleDescriptor(ProtocolModel):
    module_id: Identifier
    version: NonEmptyString = "1"
    operations: list[OperationDescriptor]
    required_capabilities: list[HostCapability] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_declarations(self):
        names = [operation.name for operation in self.operations]
        if len(names) != len(set(names)):
            raise ValueError("操作名称不能重复")
        if len(self.required_capabilities) != len(set(self.required_capabilities)):
            raise ValueError("宿主能力不能重复")
        return self


@runtime_checkable
class AuthenticationCapability(Protocol):
    def check_available(self) -> None: ...

    def verify(self, context: CallContext) -> None: ...


@runtime_checkable
class PersistenceCapability(Protocol):
    def check_available(self) -> None: ...


@runtime_checkable
class BudgetCapability(Protocol):
    def check_available(self) -> None: ...


@dataclass(frozen=True)
class HostCapabilities:
    authentication: AuthenticationCapability | None = None
    persistence: PersistenceCapability | None = None
    budget: BudgetCapability | None = None

    def get(
        self, capability: HostCapability
    ) -> AuthenticationCapability | PersistenceCapability | BudgetCapability | None:
        return getattr(self, capability.value)


@runtime_checkable
class ModuleAdapter(Protocol):
    input_models: dict[str, type[BaseModel]]
    output_models: dict[str, type[BaseModel]]

    def describe(self) -> ModuleDescriptor: ...

    async def startup(self) -> None: ...

    async def shutdown(self) -> None: ...

    async def invoke(
        self, operation: str, payload: BaseModel, context: CallContext
    ) -> OperationResult: ...


__all__ = [
    "SHA256",
    "AuthenticationCapability", "BudgetCapability", "CallContext", "HostCapabilities",
    "HostCapability", "Identifier", "JSONObject", "ModuleAdapter", "ModuleDescriptor",
    "NonEmptyString", "OperationDescriptor", "OperationResult", "PersistenceCapability",
    "ProtocolModel", "ResourceRef",
]
