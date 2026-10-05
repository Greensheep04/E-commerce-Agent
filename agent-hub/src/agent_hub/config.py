from pathlib import Path
from urllib.parse import urlsplit

from integration_protocol import Identifier, NonEmptyString
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class ConnectorConfig(ConfigModel):
    connector_id: Identifier
    base_url: NonEmptyString
    username: NonEmptyString
    password_file: Path
    expected_actor_id: NonEmptyString
    connect_timeout: float = Field(default=10, gt=0)
    read_timeout: float = Field(default=300, gt=0)

    @field_validator("base_url")
    @classmethod
    def valid_url(cls, value):
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("后端地址必须使用完整HTTP或HTTPS地址")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("后端地址不能包含凭据、查询参数或fragment")
        if parsed.path not in {"", "/"}:
            raise ValueError("后端地址必须指向服务根路径")
        return value.rstrip("/")


class AgentConfig(ConfigModel):
    connector_id: Identifier
    module_id: Identifier
    agent_id: Identifier
    tool_name: Identifier
    description: NonEmptyString
    allowed_operations: list[Identifier] = Field(min_length=1)
    expected_version: NonEmptyString = "1"

    @field_validator("allowed_operations")
    @classmethod
    def unique_operations(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("允许操作不能重复")
        return value


class SharedToolConfig(ConfigModel):
    connector_id: Identifier
    module_id: Identifier
    tool_name: Identifier
    description: NonEmptyString
    operation: Identifier
    expected_version: NonEmptyString = "1"


class HubConfig(ConfigModel):
    connectors: list[ConnectorConfig] = Field(min_length=1)
    agents: list[AgentConfig] = Field(default_factory=list)
    shared_tools: list[SharedToolConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def consistent_registration(self):
        connector_ids = [entry.connector_id for entry in self.connectors]
        agent_ids = [entry.agent_id for entry in self.agents]
        names = ["list_agents"] + [entry.tool_name for entry in self.agents + self.shared_tools]
        for identifiers in (connector_ids, agent_ids, names):
            if len(identifiers) != len(set(identifiers)):
                raise ValueError("connector、Agent和工具名称必须分别唯一")
        if not self.agents and not self.shared_tools:
            raise ValueError("至少登记一个Agent或共享工具")
        for entry in self.agents + self.shared_tools:
            if entry.connector_id not in connector_ids:
                raise ValueError("登记项引用了未知connector")
        return self


def load_config(path: Path) -> HubConfig:
    path = path.resolve(strict=True)
    config = HubConfig.model_validate_json(path.read_text(encoding="utf-8"))
    connectors = [entry.model_copy(update={
        "password_file": entry.password_file if entry.password_file.is_absolute()
        else path.parent / entry.password_file,
    }) for entry in config.connectors]
    return config.model_copy(update={"connectors": connectors})
