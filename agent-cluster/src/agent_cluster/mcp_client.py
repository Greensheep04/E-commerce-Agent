from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path

from integration_protocol import OperationResult, ResourceRef
from jsonschema import Draft202012Validator, FormatChecker
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from pydantic import BaseModel, ConfigDict, Field

from agent_cluster.types import GoalDefinition, TaskSpec


class MCPCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    command: str
    args: list[str]
    cwd: Path
    env: dict[str, str] | None = None
    timeout_seconds: float = Field(default=240, gt=0)


class BusinessCallError(RuntimeError):
    def __init__(self, detail):
        self.detail = detail
        super().__init__(str(detail))

    @property
    def uncertain(self):
        code = self.detail.get("error", {}).get("code", "") if isinstance(self.detail, dict) else ""
        return code in {"backend_timeout", "backend_unavailable"} or code.startswith("backend_http_5")


class MCPCatalog:
    def __init__(self, document):
        self.document = deepcopy(document)
        self.entries = {}
        for agent in document["agents"]:
            for operation in agent["operations"]:
                self._add(agent["tool_name"], operation["name"], agent["module_id"], operation, False)
        for tool in document["shared_tools"]:
            self._add(tool["tool_name"], tool["operation"], tool["module_id"], tool, True)

    def _add(self, tool, action, module, schema, shared):
        key = (tool, action)
        if key in self.entries:
            raise ValueError("能力目录重复登记")
        for field in ("input_schema", "output_schema"):
            Draft202012Validator.check_schema(schema[field])
        self.entries[key] = {"module_id": module, "shared": shared,
                             "input": Draft202012Validator(schema["input_schema"], format_checker=FormatChecker()),
                             "output": Draft202012Validator(schema["output_schema"], format_checker=FormatChecker())}

    def validate_goal(self, goal: GoalDefinition):
        for tool, actions in goal.allowed_operations.items():
            for action in actions:
                if (tool, action) not in self.entries:
                    raise ValueError("目标包含未登记操作")
        customer = goal.allowed_operations.get("shop_customer_agent", [])
        if "submit" in customer and "turn" not in customer:
            raise PermissionError("客服submit需要授权turn以查询已提交任务的实际状态")
        data_actions = goal.allowed_operations.get("shop_data_center_agent", [])
        if set(data_actions) & {"ask", "query", "sync", "ingest", "build_graph", "run"} and "show" not in data_actions:
            raise PermissionError("数据中台任务需要授权show以读取实际执行状态")
        for criterion in goal.criteria:
            if criterion.action is not None and criterion.action not in goal.allowed_operations[criterion.tool_name]:
                raise PermissionError("完成条件包含未授权操作")

    def arguments(self, spec: TaskSpec, payload: dict, request_id: str, upstream: list[ResourceRef]):
        entry = self.entries.get((spec.tool_name, spec.action))
        if entry is None:
            raise ValueError("任务工具操作没有登记")
        entry["input"].validate(payload)
        arguments = {"payload": payload, "request_id": request_id,
                     "upstream": [item.model_dump(mode="json") for item in upstream]}
        if not entry["shared"]:
            arguments["action"] = spec.action
        return arguments

    def validate_result(self, spec, result):
        self.entries[(spec.tool_name, spec.action)]["output"].validate(result.data)

    def authorized(self, goal):
        document = deepcopy(self.document)
        document["agents"] = [
            {**agent, "operations": [op for op in agent["operations"]
                                      if op["name"] in goal.allowed_operations[agent["tool_name"]]]}
            for agent in document["agents"] if agent["tool_name"] in goal.allowed_operations
        ]
        document["shared_tools"] = [tool for tool in document["shared_tools"]
                                    if tool["operation"] in goal.allowed_operations.get(tool["tool_name"], [])]
        return document


class MCPBusinessClient:
    def __init__(self, session, catalog):
        self.session = session
        self.catalog = catalog

    @staticmethod
    def structured(result):
        if result.is_error:
            raise BusinessCallError(result.structured_content or [block.model_dump() for block in result.content])
        if result.structured_content is None:
            raise ValueError("MCP返回缺少结构化内容")
        return result.structured_content

    async def invoke(self, spec, arguments):
        result = self.structured(await self.session.call_tool(spec.tool_name, arguments))
        operation = OperationResult.model_validate(result)
        self.catalog.validate_result(spec, operation)
        return operation


@asynccontextmanager
async def connect(command: MCPCommand):
    parameters = StdioServerParameters(command=command.command, args=command.args,
                                       cwd=str(command.cwd), env=command.env)
    async with stdio_client(parameters) as streams:
        async with ClientSession(*streams, read_timeout_seconds=command.timeout_seconds) as session:
            await session.initialize()
            document = MCPBusinessClient.structured(await session.call_tool("list_agents", {}))
            yield MCPBusinessClient(session, MCPCatalog(document))
