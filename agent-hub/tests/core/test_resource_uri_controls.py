import pytest

from agent_hub.core import HubError
from agent_hub.mcp.resources import parse_resource_uri


@pytest.mark.parametrize("relative_path", [
    "%0acontent", "%0dcontent", "%09content", "%7fcontent",
    "\ncontent", "\rcontent", "\tcontent",
])
def test_resource_uri_rejects_control_characters_in_file_path(relative_path):
    uri = "agenthub://shop/materials/asset_version/identifier/" + relative_path
    with pytest.raises(HubError) as failure:
        parse_resource_uri(uri)
    assert failure.value.code == "invalid_resource_uri"
