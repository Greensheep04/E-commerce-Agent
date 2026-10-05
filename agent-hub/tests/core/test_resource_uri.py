import pytest
from integration_protocol import ResourceRef

from agent_hub.core import HubError
from agent_hub.mcp.resources import build_resource_uri, parse_resource_uri


def test_uri_preserves_encoded_resource_id_and_registered_query():
    uri = (
        "agenthub://shop/materials/asset_version/resource%20identifier/content"
        "?version=2&sha256=" + "a" * 64
    )
    connector_id, reference, relative_path = parse_resource_uri(uri)
    assert connector_id == "shop"
    assert reference.resource_id == "resource identifier"
    assert reference.version == "2"
    assert reference.sha256 == "a" * 64
    assert relative_path == "content"


@pytest.mark.parametrize("uri", [
    "file:///materials/asset_version/identifier/content",
    "agenthub://identity@shop/materials/asset_version/identifier/content",
    "agenthub://shop:80/materials/asset_version/identifier/content",
    "agenthub://shop/materials/asset_version/identifier/../content",
    "agenthub://shop/materials/asset_version/identifier/%2e%2e/content",
    "agenthub://shop/materials/asset_version/identifier/%2fcontent",
    "agenthub://shop/materials/asset_version/identifier/%00content",
    "agenthub://shop/materials/asset_version/identifier/content#fragment",
    "agenthub://shop/materials/asset_version/identifier/content?actor_id=admin",
    "agenthub://shop/materials/asset_version/identifier/content?version=1&version=2",
    "agenthub://shop/materials/asset_version/identifier/content?sha256=invalid",
])
def test_uri_rejects_invalid_address_path_and_query(uri):
    with pytest.raises(HubError):
        parse_resource_uri(uri)


def test_build_uri_keeps_nested_file_path_and_resource_version():
    reference = ResourceRef(
        module_id="poster", resource_type="job", resource_id="job identifier", version="2",
    )
    uri = build_resource_uri("shop", reference, "framework/output.png")
    assert uri == "agenthub://shop/poster/job/job%20identifier/framework/output.png?version=2"
    connector_id, parsed, relative_path = parse_resource_uri(uri)
    assert connector_id == "shop"
    assert parsed == reference
    assert relative_path == "framework/output.png"
