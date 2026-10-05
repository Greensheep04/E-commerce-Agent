import argparse
import asyncio
import importlib
import importlib.util
import json
import os
import pkgutil
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

import agent_cluster
from agent_cluster.mcp_client import MCPCommand, connect
from agent_cluster.notes import NoteStore
from agent_cluster.store import ClusterStore
from agent_cluster.types import CompletionCriterion, GoalDefinition, TaskSpec, TriggerEvent
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema


async def verify(args):
    assert importlib.util.find_spec("shop") is None
    modules = []
    for entry in pkgutil.walk_packages(agent_cluster.__path__, agent_cluster.__name__ + "."):
        if entry.name == "agent_cluster.__main__":
            continue
        importlib.import_module(entry.name)
        modules.append(entry.name)
    config = json.loads(args.cluster_config.read_text())
    inputs = json.loads(args.inputs.read_text())
    source = json.loads((args.inputs.parent / "sources.json").read_text())
    source_digest = sha256(json.dumps(source, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()
    assert source_digest == inputs["source_sha256"], "独立安装检查的固定来源摘要不一致"
    expected_orders = source["orders"]["customer"]["orders"][:1]
    assert expected_orders, "独立安装检查缺少固定本人订单"
    actor = inputs["accounts"]["customer"]["actor_id"]
    goal = GoalDefinition(actor_id=actor, objective="查询本人最近一条订单，保存来源",
                          allowed_operations={"shop_customer_agent": ["orders"]},
                          criteria=[CompletionCriterion(tool_name="shop_customer_agent", path=["data", "orders"])])
    async with connect(MCPCommand.model_validate(config["actors"][actor])) as client:
        client.catalog.validate_goal(goal)
        spec = TaskSpec(task_id="independent-orders", tool_name="shop_customer_agent", action="orders")
        arguments = client.catalog.arguments(spec, {"limit": 1}, str(uuid4()), [])
        result = await client.invoke(spec, arguments)
    assert result.status == "completed" and result.data["orders"] == expected_orders
    assert all(order["source"]["source_id"] and order["source"]["revision"]
               and order["source"]["order_id"] == order["order_id"] for order in result.data["orders"])
    schema = "clu_install_" + uuid4().hex
    engine = create_engine(os.environ["SHOP_TEST_DATABASE_URL"])
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema))
    engine.dispose()
    engine = create_engine(os.environ["SHOP_TEST_DATABASE_URL"],
                           connect_args={"options": f"-csearch_path={schema}"})
    try:
        store = ClusterStore(engine)
        store.initialize()
        store.create_goal(goal)
        event = TriggerEvent(event_id="independent-install", actor_id=actor, goal_id=goal.goal_id)
        first = store.trigger(event)
        assert store.trigger(event).run_id == first.run_id
        assert ClusterStore(engine).get_goal(goal.goal_id, actor) == goal
        notes = NoteStore(args.output.parent / "independent-notes")
        content = result.model_dump_json()
        ref = notes.write(goal_id=goal.goal_id, actor_id=actor, topic="orders", content=content,
                          source="mcp:" + arguments["request_id"], summary="本人订单及来源",
                          resources=result.resources)
        assert notes.read(ref, goal_id=goal.goal_id, actor_id=actor) == content
        report = {"status": "passed", "shop_installed": False, "modules": modules,
                  "schema": schema, "goal_id": goal.goal_id, "run_id": first.run_id,
                  "source_sha256": source_digest,
                  "request_id": arguments["request_id"], "operation_result": result.model_dump(mode="json"),
                  "note_sha256": sha256(content.encode()).hexdigest(),
                  "versions": {name: version(name) for name in (
                      "agent-cluster", "integration-protocol", "deepagents", "langgraph", "mcp",
                      "APScheduler", "SQLAlchemy", "psycopg", "tokenizers")}}
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps({"status": "passed", "shop_installed": False, "imported_modules": len(modules)}))
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="独立wheel环境真实依赖及MCP、存储、Note检查")
    parser.add_argument("--cluster-config", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(verify(parser.parse_args()))


if __name__ == "__main__":
    main()
