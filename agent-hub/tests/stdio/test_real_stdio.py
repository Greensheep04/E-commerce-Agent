import base64
import hashlib
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from integration_protocol import ResourceRef
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.shared.exceptions import MCPError

from agent_hub.config import load_config
from agent_hub.connectors.http import HttpConnector

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def load_test_inputs():
    path = Path(os.environ["AGENT_HUB_TEST_INPUTS"]).resolve(strict=True)
    return json.loads(path.read_text(encoding="utf-8"))


def config_path(name=None):
    if name is None:
        return Path(os.environ["AGENT_HUB_TEST_CONFIG"]).resolve(strict=True)
    inputs = load_test_inputs()
    return Path(inputs["hub_configs"][name]).resolve(strict=True)


@asynccontextmanager
async def connected_session(name=None):
    env = os.environ.copy()
    env["AGENT_HUB_TEST_CONFIG"] = str(config_path(name))
    env["AGENT_HUB_TEST_INPUTS"] = str(Path(os.environ["AGENT_HUB_TEST_INPUTS"]).resolve(strict=True))
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "agent_hub", "--config", str(config_path(name))],
        env=env,
        cwd=PROJECT_ROOT,
    )
    async with stdio_client(server) as streams:
        async with ClientSession(*streams) as session:
            await session.initialize()
            yield session


def result_data(result):
    structured = result.structured_content
    assert structured is not None, "MCP工具没有返回structuredContent"
    return structured


def require_success(result):
    assert not result.is_error, "MCP工具调用返回错误"
    return result_data(result)


def assert_stdout_uses_only_mcp_messages(caplog):
    assert not any(
        record.name == "mcp.client.stdio"
        and "Failed to parse JSONRPC message from server" in record.getMessage()
        for record in caplog.records
    )


def operation_result(result):
    value = require_success(result)
    assert set(value) == {"data", "resources", "status", "run_id", "costs"}
    assert isinstance(value["data"], dict)
    assert isinstance(value["resources"], list)
    assert isinstance(value["status"], str)
    assert value["run_id"] is None or isinstance(value["run_id"], str)
    assert isinstance(value["costs"], dict)
    return value


def call_agent(session, name, action, payload):
    return session.call_tool(name, {"action": action, "payload": payload,
                                   "request_id": "mcp03-readonly-stdio",
                                   "upstream": []})


def resource_uri(connector_id, reference, relative_path, query_overrides=None):
    from urllib.parse import urlencode

    ref = ResourceRef.model_validate(reference)
    uri = (f"agenthub://{connector_id}/{ref.module_id}/{ref.resource_type}/"
           f"{ref.resource_id}/{relative_path}")
    query = {}
    if ref.version is not None:
        query["version"] = ref.version
    if ref.sha256 is not None:
        query["sha256"] = ref.sha256
    query.update(query_overrides or {})
    return uri + ("?" + urlencode(query) if query else "")


async def backend_file(config_name, reference, relative_path):
    connector = HttpConnector(load_config(config_path(config_name)).connectors[0])
    try:
        return await connector.read_file(ResourceRef.model_validate(reference), relative_path)
    finally:
        await connector.close()


async def backend_catalog(config_name=None):
    connector = HttpConnector(load_config(config_path(config_name)).connectors[0])
    try:
        return await connector.describe()
    finally:
        await connector.close()


def resource_bytes(item):
    blob = getattr(item, "blob", None)
    if blob is not None:
        return base64.b64decode(blob, validate=True)
    return item.text.encode("utf-8")


async def first_material_reference(session):
    searched = operation_result(await session.call_tool("shop_materials_search", {
        "payload": {"title": None, "kind": None, "tag": None, "owner_id": None,
                    "source_id": None, "source_revision": None, "attributes": {},
                    "limit": 1, "offset": 0},
        "request_id": "mcp03-material-search",
        "upstream": [],
    }))
    asset_id = searched["data"]["items"][0]["id"]
    shown = operation_result(await session.call_tool("shop_materials_show", {
        "payload": {"asset_id": asset_id},
        "request_id": "mcp03-material-show",
        "upstream": [],
    }))
    reference = next(item for item in shown["resources"] if item["resource_type"] == "asset_version")
    return reference, shown


@pytest.mark.asyncio
async def test_admin_stdio_exposes_seven_tools_and_complete_agent_catalog(caplog):
    original_modules = {item.module_id: item for item in await backend_catalog()}
    config = json.loads(config_path().read_text(encoding="utf-8"))
    async with connected_session() as session:
        tools = await session.list_tools()
        tool_by_name = {tool.name: tool for tool in tools.tools}
        assert set(tool_by_name) == {
            "list_agents",
            "shop_harness_agent",
            "shop_customer_agent",
            "shop_assessment_agent",
            "shop_poster_agent",
            "shop_materials_search",
            "shop_materials_show",
        }

        catalog = require_success(await session.call_tool("list_agents", {}))
        assert {agent["tool_name"] for agent in catalog["agents"]} == {
            "shop_harness_agent", "shop_customer_agent", "shop_assessment_agent", "shop_poster_agent",
        }
        assert {entry["tool_name"] for entry in catalog["shared_tools"]} == {
            "shop_materials_search", "shop_materials_show",
        }

        for agent in catalog["agents"]:
            tool = tool_by_name[agent["tool_name"]]
            assert set(tool.input_schema["properties"]) == {"action", "payload", "request_id", "upstream"}
            original = {item.name: item for item in original_modules[agent["module_id"]].operations}
            assert agent["version"] == original_modules[agent["module_id"]].version
            for operation in agent["operations"]:
                descriptor = original[operation["name"]]
                assert operation["input_schema"] == descriptor.input_schema
                assert operation["output_schema"] == descriptor.output_schema
                assert operation["supported"] == descriptor.supported
        for shared_tool in catalog["shared_tools"]:
            tool = tool_by_name[shared_tool["tool_name"]]
            assert set(tool.input_schema["properties"]) == {"payload", "request_id", "upstream"}
            config_entry = next(item for item in config["shared_tools"]
                                if item["tool_name"] == shared_tool["tool_name"])
            descriptor = next(item for item in original_modules[config_entry["module_id"]].operations
                              if item.name == config_entry["operation"])
            assert shared_tool["input_schema"] == descriptor.input_schema
            assert shared_tool["output_schema"] == descriptor.output_schema
        assert_stdout_uses_only_mcp_messages(caplog)


@pytest.mark.asyncio
async def test_customer_orders_return_the_full_structured_operation_result(caplog):
    inputs = load_test_inputs()
    async with connected_session("customer") as session:
        result = await call_agent(session, "shop_customer_agent", "orders", {
            "order_number": inputs["order_number"], "limit": 1, "offset": 0,
        })
        value = operation_result(result)
        assert value["data"]["orders"]
        assert_stdout_uses_only_mcp_messages(caplog)


@pytest.mark.asyncio
async def test_stdio_rejects_invalid_payload_and_unknown_operation():
    async with connected_session("customer") as session:
        invalid = await call_agent(session, "shop_customer_agent", "orders", {
            "limit": 21, "offset": 0,
        })
        assert invalid.is_error

        unknown = await call_agent(session, "shop_customer_agent", "missing_operation", {})
        assert unknown.is_error


@pytest.mark.asyncio
async def test_real_material_file_bytes_mime_digest_and_version(caplog):
    async with connected_session() as session:
        reference, _ = await first_material_reference(session)
        connector_id = json.loads(config_path().read_text(encoding="utf-8"))["connectors"][0]["connector_id"]
        uri = resource_uri(connector_id, reference, "content")
        read = await session.read_resource(uri)
        assert len(read.contents) == 1
        item = read.contents[0]
        content = resource_bytes(item)
        expected = await backend_file(None, reference, "content")
        assert content == expected.content
        assert hashlib.sha256(content).hexdigest() == expected.sha256 == reference["sha256"]
        assert item.mime_type == expected.mime_type
        assert_stdout_uses_only_mcp_messages(caplog)


@pytest.mark.asyncio
async def test_resource_errors_reject_invalid_path_digest_and_version():
    async with connected_session() as session:
        reference, _ = await first_material_reference(session)
        connector_id = json.loads(config_path().read_text(encoding="utf-8"))["connectors"][0]["connector_id"]
        invalid_digest = "0" * 64 if reference["sha256"] != "0" * 64 else "1" * 64
        cases = [
            resource_uri(connector_id, reference, "wrong-file.txt"),
            resource_uri(connector_id, reference, "content", {"sha256": invalid_digest}),
            resource_uri(connector_id, reference, "content", {"version": "0"}),
        ]
        for uri in cases:
            with pytest.raises(MCPError) as error:
                await session.read_resource(uri)
            assert error.value.message


@pytest.mark.asyncio
async def test_real_harness_html_resource_preserves_bytes_digest_and_version(caplog):
    inputs = load_test_inputs()
    async with connected_session() as session:
        shown = operation_result(await call_agent(session, "shop_harness_agent", "show", {
            "task_id": inputs["harness_task_id"],
        }))
        reference = next(item for item in shown["resources"] if item["resource_type"] == "task")
        connector_id = json.loads(config_path().read_text(encoding="utf-8"))["connectors"][0]["connector_id"]
        uri = resource_uri(connector_id, reference, "page.html")
        read = await session.read_resource(uri)
        assert len(read.contents) == 1
        item = read.contents[0]
        content = resource_bytes(item)
        expected = await backend_file(None, reference, "page.html")
        assert item.mime_type == "text/html"
        assert content[:15].lower() == b"<!doctype html>" or content.startswith(b"<html")
        assert content == expected.content
        assert hashlib.sha256(content).hexdigest() == expected.sha256
        assert item.mime_type == expected.mime_type
        assert_stdout_uses_only_mcp_messages(caplog)


@pytest.mark.asyncio
async def test_poster_unsupported_operation_is_reported_without_running_a_job():
    async with connected_session() as session:
        result = await call_agent(session, "shop_poster_agent", "resume", {
            "job_id": "00000000-0000-4000-8000-000000000001",
        })
        assert result.is_error
