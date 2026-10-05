from contextlib import asynccontextmanager

from fastmcp import FastMCP
from fastmcp.exceptions import ResourceError
from fastmcp.resources import ResourceResult
from fastmcp.resources.types import ResourceContent as MCPResourceContent
from fastmcp.server.middleware import Middleware
from fastmcp.tools import ToolResult
from integration_protocol import (
    JSONObject,
    NonEmptyString,
    OperationResult,
    ResourceRef,
)

from agent_hub.core import AgentRegistration, HubError, Registry, SharedToolRegistration
from agent_hub.mcp.resources import build_resource_uri, parse_resource_uri


class ResourceValidation(Middleware):
    async def on_read_resource(self, context, call_next):
        try:
            parse_resource_uri(str(context.message.uri))
        except HubError as error:
            raise ResourceError(str(error)) from error
        return await call_next(context)


def agent_handler(registry: Registry, entry: AgentRegistration):
    async def invoke(action: NonEmptyString, payload: JSONObject, request_id: NonEmptyString,
                     upstream: list[ResourceRef] | None = None) -> ToolResult:
        try:
            result = await registry.invoke_agent(entry.agent_id, action, payload,
                request_id=request_id, upstream=upstream)
        except HubError as error:
            return ToolResult(content=str(error), is_error=True,
                              structured_content={"error": {"code": error.code, "message": error.message}})
        return ToolResult(structured_content=result.model_dump(mode="json"))
    return invoke


def shared_handler(registry: Registry, entry: SharedToolRegistration):
    async def invoke(payload: JSONObject, request_id: NonEmptyString,
                     upstream: list[ResourceRef] | None = None) -> ToolResult:
        try:
            result = await registry.invoke_shared_tool(entry.tool_name, payload,
                request_id=request_id, upstream=upstream)
        except HubError as error:
            return ToolResult(content=str(error), is_error=True,
                              structured_content={"error": {"code": error.code, "message": error.message}})
        return ToolResult(structured_content=result.model_dump(mode="json"))
    return invoke


def create_server(registry: Registry) -> FastMCP:
    @asynccontextmanager
    async def lifespan(server):
        await registry.startup()
        try:
            yield
        finally:
            await registry.close()

    server = FastMCP("Agent Hub", version="0.1.0", lifespan=lifespan,
                     dereference_schemas=False, strict_input_validation=True,
                     middleware=[ResourceValidation()])

    @server.tool(name="list_agents", description="读取整体Agent、共享工具、允许操作及原始输入输出Schema")
    async def list_agents() -> ToolResult:
        return ToolResult(structured_content=registry.describe_agents())

    for entry in registry.agent_registrations():
        server.tool(agent_handler(registry, entry), name=entry.tool_name, description=entry.description,
                    output_schema=OperationResult.model_json_schema())
    for entry in registry.shared_tool_registrations():
        server.tool(shared_handler(registry, entry), name=entry.tool_name, description=entry.description,
                    output_schema=OperationResult.model_json_schema())

    @server.resource("agenthub://{connector_id}/{module_id}/{resource_type}/{resource_id}/{relative_path*}{?version,sha256}",
                     name="agent_file", description="按原资源引用读取授权文件及MIME")
    async def read_file(connector_id: str, module_id: str, resource_type: str, resource_id: str,
                        relative_path: str, version: str | None = None, sha256: str | None = None) -> ResourceResult:
        try:
            reference = ResourceRef(module_id=module_id, resource_type=resource_type,
                                    resource_id=resource_id, version=version, sha256=sha256)
            uri = build_resource_uri(connector_id, reference, relative_path)
            connector, reference, relative = parse_resource_uri(uri)
            content = await registry.read_file(connector, reference, relative)
        except HubError as error:
            raise ResourceError(str(error)) from error
        return ResourceResult(contents=[MCPResourceContent(content=content.content, mime_type=content.mime_type)],
                              meta={"sha256": content.sha256})

    return server
