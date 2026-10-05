import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from integration_protocol import ResourceRef

from agent_hub.config import load_config
from agent_hub.connectors.http import HttpConnector
from agent_hub.core import AgentRegistration, HubError, Registry, SharedToolRegistration


@pytest.fixture
def inputs():
    return json.loads(Path(os.environ["AGENT_HUB_TEST_INPUTS"]).read_text(encoding="utf-8"))


@pytest.fixture
async def registry_setup():
    config = load_config(Path(os.environ["AGENT_HUB_TEST_CONFIG"]))
    connector = HttpConnector(config.connectors[0])
    registry = Registry()
    registry.register_connector("shop", connector)
    try:
        yield registry, connector
    finally:
        await registry.close()


def agent_entry(**updates):
    return AgentRegistration(**{
        "connector_id": "shop",
        "module_id": "materials",
        "agent_id": "material_reader",
        "tool_name": "material_agent",
        "description": "读取真实素材目录",
        "allowed_operations": ["search"],
        **updates,
    })


def shared_entry(**updates):
    return SharedToolRegistration(**{
        "connector_id": "shop",
        "module_id": "materials",
        "tool_name": "material_show",
        "description": "读取真实素材详情",
        "operation": "show",
        **updates,
    })


def register_readers(registry):
    registry.register_agent(agent_entry())
    registry.register_shared_tool(shared_entry())


@pytest.mark.parametrize("connector_id", ["shop", "other"])
async def test_connector_name_and_lifecycle_owner_are_unique(registry_setup, connector_id):
    registry, connector = registry_setup
    with pytest.raises(HubError) as failure:
        registry.register_connector(connector_id, connector)
    assert failure.value.code == "duplicate_connector"


@pytest.mark.parametrize("registration_kind", ["agent", "shared"])
async def test_unknown_connector_is_rejected(registry_setup, registration_kind):
    registry, _ = registry_setup
    with pytest.raises(HubError) as failure:
        if registration_kind == "agent":
            registry.register_agent(agent_entry(connector_id="missing"))
        else:
            registry.register_shared_tool(shared_entry(connector_id="missing"))
    assert failure.value.code == "unknown_connector"


async def test_agent_identity_is_unique(registry_setup):
    registry, _ = registry_setup
    registry.register_agent(agent_entry())
    with pytest.raises(HubError) as failure:
        registry.register_agent(agent_entry(tool_name="another_agent"))
    assert failure.value.code == "duplicate_agent"


@pytest.mark.parametrize("registration_kind", ["agent", "shared"])
async def test_reserved_tool_name_is_rejected(registry_setup, registration_kind):
    registry, _ = registry_setup
    with pytest.raises(HubError) as failure:
        if registration_kind == "agent":
            registry.register_agent(agent_entry(tool_name="list_agents"))
        else:
            registry.register_shared_tool(shared_entry(tool_name="list_agents"))
    assert failure.value.code == "duplicate_tool"


@pytest.mark.parametrize("first_kind", ["agent", "shared"])
async def test_tool_names_are_unique_across_registration_types(registry_setup, first_kind):
    registry, _ = registry_setup
    if first_kind == "agent":
        registry.register_agent(agent_entry(tool_name="reader"))
    else:
        registry.register_shared_tool(shared_entry(tool_name="reader"))
    with pytest.raises(HubError) as failure:
        if first_kind == "agent":
            registry.register_shared_tool(shared_entry(tool_name="reader"))
        else:
            registry.register_agent(agent_entry(tool_name="reader"))
    assert failure.value.code == "duplicate_tool"


@pytest.mark.parametrize(("updates", "expected_code"), [
    ({"expected_version": "999"}, "version_mismatch"),
    ({"module_id": "missing"}, "unknown_module"),
    ({"allowed_operations": ["missing"]}, "unknown_operation"),
    ({"module_id": "poster", "allowed_operations": ["resume"]}, "unsupported_operation"),
])
async def test_real_catalog_rejects_invalid_registration_and_closes_client(
    registry_setup, updates, expected_code,
):
    registry, connector = registry_setup
    registry.register_agent(agent_entry(**updates))
    with pytest.raises(HubError) as failure:
        await registry.startup()
    assert failure.value.code == expected_code
    assert connector._client.is_closed
    with pytest.raises(HubError) as unavailable:
        registry.describe_agents()
    assert unavailable.value.code == "registry_unavailable"


async def test_empty_registration_closes_real_http_client(registry_setup):
    registry, connector = registry_setup
    with pytest.raises(HubError) as failure:
        await registry.startup()
    assert failure.value.code == "empty_registration"
    assert connector._client.is_closed


async def test_directory_preserves_independent_original_schemas(registry_setup):
    registry, connector = registry_setup
    module = next(row for row in await connector.describe() if row.module_id == "materials")
    original_operations = {row.name: row.model_dump(mode="json") for row in module.operations}
    register_readers(registry)
    await registry.startup()
    directory = registry.describe_agents()
    agent = directory["agents"][0]
    shared = directory["shared_tools"][0]
    assert agent["allowed_operations"] == ["search"]
    assert agent["version"] == module.version
    assert agent["operations"] == [original_operations["search"]]
    assert shared["version"] == module.version
    assert shared["input_schema"] == original_operations["show"]["input_schema"]
    assert shared["output_schema"] == original_operations["show"]["output_schema"]
    directory["agents"][0]["operations"][0]["input_schema"].clear()
    directory["shared_tools"][0]["output_schema"].clear()
    assert registry.describe_agents()["agents"][0]["operations"] == [original_operations["search"]]
    assert registry.describe_agents()["shared_tools"][0]["output_schema"] == original_operations["show"]["output_schema"]


async def test_real_query_and_shared_result_preserve_original_operation_result(registry_setup, inputs):
    registry, connector = registry_setup
    register_readers(registry)
    await registry.startup()
    direct = await connector.invoke("materials", "search", {"limit": 1}, version="1",
                                    request_id=str(uuid4()), upstream=[])
    result = await registry.invoke_agent("material_reader", "search", {"limit": 1}, request_id=str(uuid4()))
    assert result.model_dump(mode="json") == direct.model_dump(mode="json")
    assert len(result.data["items"]) == 1
    payload = {"asset_id": inputs["poster_input"]["materials"][0]["asset_id"]}
    shared = await registry.invoke_shared_tool("material_show", payload, request_id=str(uuid4()))
    direct_shared = await connector.invoke("materials", "show", payload, version="1",
                                           request_id=str(uuid4()), upstream=[])
    assert shared.model_dump(mode="json") == direct_shared.model_dump(mode="json")
    assert set(shared.model_dump()) == {"data", "resources", "status", "run_id", "costs"}


@pytest.mark.parametrize("payload", [
    {"limit": 0}, {"limit": 201}, {"limit": "1"}, {"offset": -1},
    {"attributes": {"color": 1}}, {"actor_id": "supplied-identity"},
])
async def test_business_input_schema_rejects_invalid_fields_types_and_bounds(registry_setup, payload):
    registry, _ = registry_setup
    register_readers(registry)
    await registry.startup()
    with pytest.raises(HubError) as failure:
        await registry.invoke_agent("material_reader", "search", payload, request_id=str(uuid4()))
    assert failure.value.code == "invalid_payload"


@pytest.mark.parametrize("upstream", [False, {}, "", (), "resource"])
async def test_upstream_requires_list_even_when_value_is_empty(registry_setup, upstream):
    registry, _ = registry_setup
    register_readers(registry)
    await registry.startup()
    with pytest.raises(HubError) as failure:
        await registry.invoke_agent("material_reader", "search", {"limit": 1},
                                    request_id=str(uuid4()), upstream=upstream)
    assert failure.value.code == "invalid_call"


async def test_unknown_agent_and_tool_and_unregistered_operation_are_rejected(registry_setup):
    registry, _ = registry_setup
    register_readers(registry)
    await registry.startup()
    with pytest.raises(HubError) as unknown_agent:
        await registry.invoke_agent("missing", "search", {"limit": 1}, request_id=str(uuid4()))
    assert unknown_agent.value.code == "unknown_agent"
    with pytest.raises(HubError) as unknown_tool:
        await registry.invoke_shared_tool("missing", {}, request_id=str(uuid4()))
    assert unknown_tool.value.code == "unknown_tool"
    with pytest.raises(HubError) as denied_operation:
        await registry.invoke_agent("material_reader", "show", {}, request_id=str(uuid4()))
    assert denied_operation.value.code == "operation_not_allowed"


async def test_startup_freezes_registration_and_close_disables_calls(registry_setup):
    registry, connector = registry_setup
    register_readers(registry)
    await registry.startup()
    for register in (
        lambda: registry.register_connector("another", connector),
        lambda: registry.register_agent(agent_entry(agent_id="another", tool_name="another_agent")),
        lambda: registry.register_shared_tool(shared_entry(tool_name="another_tool")),
    ):
        with pytest.raises(HubError) as failure:
            register()
        assert failure.value.code == "registration_closed"
    await registry.close()
    assert connector._client.is_closed
    with pytest.raises(HubError) as failure:
        await registry.invoke_agent("material_reader", "search", {"limit": 1}, request_id=str(uuid4()))
    assert failure.value.code == "registry_unavailable"


async def test_real_resource_content_is_verified_and_scope_is_frozen(registry_setup, inputs):
    registry, _ = registry_setup
    register_readers(registry)
    await registry.startup()
    material = inputs["poster_input"]["materials"][0]
    reference = ResourceRef(module_id="materials", resource_type="asset_version",
                            resource_id=material["version_id"], sha256=material["file_sha256"])
    content = await registry.read_file("shop", reference, "content")
    assert content.sha256 == material["file_sha256"]
    assert content.mime_type == material["mime_type"]
    assert content.content
    for connector_id, update in (("missing", {}), ("shop", {"module_id": "harness"})):
        with pytest.raises(HubError) as failure:
            await registry.read_file(connector_id, reference.model_copy(update=update), "content")
        assert failure.value.code == "resource_not_allowed"
    for relative_path in ("", "/content", "../content", "nested/../content", "nested//content", "nested\\content", "\x00content"):
        with pytest.raises(HubError) as failure:
            await registry.read_file("shop", reference, relative_path)
        assert failure.value.code == "invalid_resource_path"
