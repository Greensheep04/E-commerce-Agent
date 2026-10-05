import json
import subprocess
import sys

import pytest
from pydantic import ValidationError

from agent_hub.config import HubConfig, load_config


def configuration():
    return {
        "connectors": [{"connector_id": "shop", "base_url": "http://127.0.0.1:8001",
                        "username": "local-test", "password_file": "password.txt",
                        "expected_actor_id": "local-test-actor"}],
        "agents": [{"connector_id": "shop", "module_id": "customer",
                    "agent_id": "customer", "tool_name": "shop_customer_agent",
                    "description": "读取授权客户资料", "allowed_operations": ["identity", "orders"],
                    "expected_version": "1"}],
        "shared_tools": [],
    }


def test_configuration_resolves_password_relative_to_config(tmp_path):
    path = tmp_path / "hub.json"
    path.write_text(json.dumps(configuration()), encoding="utf-8")
    config = load_config(path)
    assert config.connectors[0].password_file == tmp_path / "password.txt"
    assert config.connectors[0].connect_timeout == 10
    assert config.connectors[0].read_timeout == 300
    assert config.agents[0].allowed_operations == ["identity", "orders"]


@pytest.mark.parametrize("field,value", [
    ("base_url", "file:///private/config"), ("base_url", "http://name:secret@localhost"),
    ("read_timeout", 0), ("unexpected", "anything"),
])
def test_configuration_rejects_unsafe_or_unknown_connector_fields(field, value):
    data = configuration()
    data["connectors"][0][field] = value
    with pytest.raises(ValidationError):
        HubConfig.model_validate(data)


@pytest.mark.parametrize("change", ["duplicate_connector", "unknown_connector", "duplicate_tool", "duplicate_operation"])
def test_configuration_rejects_inconsistent_registration(change):
    data = configuration()
    if change == "duplicate_connector":
        data["connectors"].append(dict(data["connectors"][0]))
    elif change == "unknown_connector":
        data["agents"][0]["connector_id"] = "unknown"
    elif change == "duplicate_tool":
        data["agents"].append({**data["agents"][0], "agent_id": "second"})
    else:
        data["agents"][0]["allowed_operations"] = ["orders", "orders"]
    with pytest.raises(ValidationError):
        HubConfig.model_validate(data)


def test_cli_help_does_not_require_credentials():
    result = subprocess.run([sys.executable, "-m", "agent_hub", "--help"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "--config" in result.stdout
