from hashlib import sha256
from typing import Protocol, runtime_checkable

from integration_protocol import (
    SHA256,
    Identifier,
    JSONObject,
    ModuleDescriptor,
    NonEmptyString,
    OperationResult,
    ProtocolModel,
    ResourceRef,
)
from pydantic import Field, model_validator


class HubError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class ResourceContent(ProtocolModel):
    content: bytes
    mime_type: NonEmptyString
    sha256: SHA256

    @model_validator(mode="after")
    def verified_content(self):
        if sha256(self.content).hexdigest() != self.sha256:
            raise ValueError("资源内容与摘要不一致")
        return self


class AgentRegistration(ProtocolModel):
    connector_id: Identifier
    module_id: Identifier
    agent_id: Identifier
    tool_name: Identifier
    description: NonEmptyString
    allowed_operations: list[Identifier] = Field(min_length=1)
    expected_version: NonEmptyString = "1"

    @model_validator(mode="after")
    def unique_operations(self):
        if len(self.allowed_operations) != len(set(self.allowed_operations)):
            raise ValueError("允许操作不能重复")
        return self


class SharedToolRegistration(ProtocolModel):
    connector_id: Identifier
    module_id: Identifier
    tool_name: Identifier
    description: NonEmptyString
    operation: Identifier
    expected_version: NonEmptyString = "1"


@runtime_checkable
class BackendConnector(Protocol):
    async def describe(self) -> list[ModuleDescriptor]: ...

    async def invoke(self, module_id: str, operation: str, payload: JSONObject, *,
                     version: str, request_id: str, upstream: list[ResourceRef]) -> OperationResult: ...

    async def read_file(self, reference: ResourceRef, relative_path: str) -> ResourceContent: ...

    async def close(self) -> None: ...
