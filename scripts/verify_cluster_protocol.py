import argparse
import asyncio
import json
import traceback
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import monotonic

from agent_cluster.failures import failure_detail, failure_status
from agent_cluster.mcp_client import connect
from agent_cluster.providers import CoordinatorProvider, validate_history
from agent_cluster.types import (
    CompletionCriterion,
    CoordinatorProfile,
    GoalDefinition,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
)
from jsonschema import Draft202012Validator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.verify_integration_live import save, snapshot, source_manifest
from shop.cluster import ShopCluster
from shop.config import Settings

ORDER_TOOL = {"type": "function", "function": {
    "name": "lookup_customer_order", "description": "通过真实MCP查询本人订单的完整明细与来源",
    "parameters": {"type": "object", "properties": {"order_number": {"type": "string"}},
                   "required": ["order_number"], "additionalProperties": False}}}


async def verify_profile(cluster, settings, account, profile, directory):
    directory.mkdir(parents=True, exist_ok=False)
    goal = GoalDefinition(actor_id=account["actor_id"], objective="核查公开样例订单SO74480的多轮模型协议",
                          allowed_operations={"shop_customer_agent": ["order"]}, budget_cny="2",
                          criteria=[CompletionCriterion(tool_name="shop_customer_agent", action="order",
                                                        path=["data", "source"])])
    cluster.store.create_goal(goal)
    run = cluster.store.trigger(TriggerEvent(goal_id=goal.goal_id, actor_id=goal.actor_id,
                                            kind="manual", event_id=directory.name + ":protocol"))
    save(directory / "goal.json", goal.model_dump(mode="json"))
    save(directory / "profile.json", profile.model_dump(mode="json"))
    provider = CoordinatorProvider(cluster.model, profile)
    messages = [{"role": "system", "content":
                 "按当前授权查询订单，首先调用lookup_customer_order查询SO74480。取得真实工具结果后返回JSON对象，"
                 "仅包含order_number、total_due、items（完整商品名称数组）、source，字段值严格依据工具结果。"
                 "直接输出JSON正文，不添加Markdown代码围栏及其他解释。\n"
                 + json.dumps(goal.model_dump(mode="json"), ensure_ascii=False)},
                {"role": "user", "content": "查询本人订单SO74480，并保存来源、总金额及全部商品名称。"}]
    error = None
    started = monotonic()
    try:
        first = await provider.request(run_id=run.run_id, step_key="protocol-0", messages=messages,
                                       tools=[ORDER_TOOL])
        save(directory / "response-0.json", first)
        assistant = first["choices"][0]["message"]
        calls = assistant.get("tool_calls", [])
        assert len(calls) == 1 and calls[0]["function"]["name"] == "lookup_customer_order"
        payload = json.loads(calls[0]["function"]["arguments"])
        Draft202012Validator(ORDER_TOOL["function"]["parameters"]).validate(payload)
        assert payload["order_number"] == "SO74480"
        spec = TaskSpec(task_id="protocol-order", tool_name="shop_customer_agent", action="order", payload=payload)
        cluster.store.save_plan(run.run_id, goal.actor_id, TaskPlan(tasks=[spec]), expected_version=0)
        task = cluster.store.tasks(run.run_id, goal.actor_id)[0]
        async with connect(cluster.command_for_actor(goal.actor_id)) as client:
            client.catalog.validate_goal(goal)
            arguments = client.catalog.arguments(spec, payload, task["request_id"], [])
            cluster.store.set_task(run.run_id, goal.actor_id, spec.task_id, status="sent", arguments=arguments)
            operation = await client.invoke(spec, arguments)
        assert operation.status == "completed"
        document = operation.model_dump(mode="json")
        cluster.store.set_task(run.run_id, goal.actor_id, spec.task_id, status="completed", result=document)
        cluster.engine._record_task(run, task, "completed", document)
        save(directory / "operation.json", document)
        messages.extend([assistant, {"role": "tool", "tool_call_id": calls[0]["id"],
                                    "content": json.dumps(document, ensure_ascii=False)}])
        validate_history(messages)
        save(directory / "messages-1.json", messages)
        second = await provider.request(run_id=run.run_id, step_key="protocol-1", messages=messages, tools=[])
        save(directory / "response-1.json", second)
        answer = json.loads(second["choices"][0]["message"]["content"])
        data = document["data"]
        expected = {"order_number": data["order_number"], "total_due": data["total_due"],
                    "items": [item["name"] for item in data["items"]], "source": data["source"]}
        assert answer == expected
        assert first.get("usage", {}).get("prompt_tokens", 0) > 0
        assert second.get("usage", {}).get("prompt_tokens", 0) > 0
        if profile.service == "deepseek":
            assert messages[-2]["reasoning_content"] == assistant["reasoning_content"]
        if profile.service == "dashscope":
            for response in (first, second):
                fragments = [choice["delta"]["reasoning_content"]
                             for event in response["stream_events"] for choice in event["choices"]
                             if choice["index"] == 0 and choice["delta"].get("reasoning_content") is not None]
                assert fragments and response["choices"][0]["message"]["reasoning_content"] == "".join(fragments)
        cluster.store.set_run(run.run_id, goal.actor_id, status="completed", summary="真实多轮模型及MCP来源核查通过")
    except Exception as exception:  # noqa: BLE001
        error = failure_detail(exception)
        (directory / "exception.txt").write_text("".join(traceback.format_exception(exception)))
        if getattr(exception, "response", None) is not None:
            (directory / "provider-error.txt").write_text(exception.response.text)
        cluster.store.set_run(run.run_id, goal.actor_id, status=failure_status(exception), error=error)
    spent, reserved = await cluster.usage(run.run_id)
    save(directory / "tasks.json", cluster.store.tasks(run.run_id, goal.actor_id))
    report = {"passed": error is None, "error": error, "run_id": run.run_id, "goal_id": goal.goal_id,
              "profile": profile.model_dump(mode="json"), "elapsed_seconds": monotonic() - started,
              "spent_cny": str(spent), "reserved_cny": str(reserved)}
    save(directory / "report.json", report)
    return report


async def execute(args):
    inputs = json.loads(args.inputs.read_text())
    args.directory.mkdir(parents=True, exist_ok=False)
    settings = Settings(cluster_config_file=args.cluster_config)
    engine = create_engine(settings.database_url)
    cluster = ShopCluster(settings, engine, sessionmaker(engine, expire_on_commit=False))
    before = snapshot(settings)
    save(args.directory / "baseline.json", before)
    profiles = [CoordinatorProfile(), CoordinatorProfile(service="dashscope", model="qwen3.8-flash")]
    if args.service:
        profiles = [profile for profile in profiles if profile.service in args.service]
    try:
        reports = []
        for profile in profiles:
            report = await verify_profile(cluster, settings, inputs["accounts"]["customer"], profile,
                                          args.directory / profile.service)
            reports.append(report)
            print(f"{profile.service}: passed={report['passed']}", flush=True)
        manifest = source_manifest()
        for path in Path("agent-cluster/src").rglob("*.py"):
            manifest[str(path)] = sha256(path.read_bytes()).hexdigest()
        save(args.directory / "report.json", {"passed": all(item["passed"] for item in reports),
                                              "profiles": reports, "source_sha256": manifest,
                                              "fixture_sha256": inputs["fixture_sha256"],
                                              "finished_at": datetime.now(UTC).isoformat()})
        save(args.directory / "final-usage.json", snapshot(settings))
        assert all(item["passed"] for item in reports)
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="真实DeepSeek与Qwen多轮协议及MCP来源验证")
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--cluster-config", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--service", choices=["deepseek", "dashscope"], action="append")
    asyncio.run(execute(parser.parse_args()))


if __name__ == "__main__":
    main()
