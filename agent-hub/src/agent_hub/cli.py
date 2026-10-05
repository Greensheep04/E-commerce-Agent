import argparse
from pathlib import Path

from agent_hub.config import load_config
from agent_hub.connectors import HttpConnector
from agent_hub.core import AgentRegistration, Registry, SharedToolRegistration
from agent_hub.mcp import create_server


def build_registry(config):
    registry = Registry()
    for entry in config.connectors:
        registry.register_connector(entry.connector_id, HttpConnector(entry))
    for entry in config.agents:
        registry.register_agent(AgentRegistration(**entry.model_dump()))
    for entry in config.shared_tools:
        registry.register_shared_tool(SharedToolRegistration(**entry.model_dump()))
    return registry


def main():
    parser = argparse.ArgumentParser(description="通过MCP stdio连接已认证的Agent与共享工具")
    parser.add_argument("--config", required=True, type=Path, help="启动身份及能力登记JSON配置")
    args = parser.parse_args()
    server = create_server(build_registry(load_config(args.config)))
    server.run(transport="stdio", show_banner=False)
