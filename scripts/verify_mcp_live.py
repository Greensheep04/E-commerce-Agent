import argparse
import asyncio
import json
import os
import subprocess
import sys
from contextlib import AsyncExitStack
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic
from uuid import uuid4

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parents[1]


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def digest(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


def source_manifest():
    paths = [ROOT / "agent-hub/pyproject.toml", ROOT / "agent-hub/uv.lock", ROOT / "uv.lock",
             ROOT / "scripts/verify_mcp_live.py", ROOT / "scripts/mcp_runtime_evidence.py"]
    for name in ("agent-hub/src", "src/shop", "packages/integration-protocol/src"):
        paths.extend(path for path in (ROOT / name).rglob("*.py") if "__pycache__" not in path.parts)
    return {str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest() for path in sorted(set(paths))}


def runtime(mode, inputs_path, directory, source_root, run_id, name):
    path = directory / name
    env = {**os.environ, "PYTHONPATH": f"{ROOT}:{ROOT / 'src'}", "TMPDIR": str(ROOT / ".scratch/runtime")}
    result = subprocess.run([
        str(source_root / ".venv/bin/python"), str(ROOT / "scripts/mcp_runtime_evidence.py"), mode,
        "--run-id", run_id, "--report", str(path), "--inputs", str(inputs_path),
    ], cwd=source_root, env=env, capture_output=True, text=True, check=False)
    (directory / (name + ".stdout.txt")).write_text(result.stdout, encoding="utf-8")
    (directory / (name + ".stderr.txt")).write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, f"业务账目检查失败，输出位于{directory}"
    return json.loads(path.read_text(encoding="utf-8"))


async def verify(inputs_path, directory, source_root):
    inputs = json.loads(inputs_path.read_text(encoding="utf-8"))
    run_id = inputs["run_id"]
    before = runtime("activate", inputs_path, directory, source_root, run_id, "before.json")
    report = {"run_id": run_id, "milestone": "MCP-05", "status": "running", "checks": {}, "records": {},
              "source_sha256": source_manifest(),
              "inputs_sha256": sha256(inputs_path.read_bytes()).hexdigest(), "config_sha256": {
                  name: sha256(Path(path).read_bytes()).hexdigest() for name, path in inputs["hub_configs"].items()}}
    save(directory / "report.json", report)
    clients = {}

    def record(name, value):
        report["checks"][name] = True
        report["records"][name] = value
        save(directory / "report.json", report)

    async def call(account, module, operation, payload, request_id=None, upstream=None, error=False):
        result = await clients[account].call_tool(f"shop_{module}_agent", {
            "action": operation, "payload": payload, "request_id": request_id or str(uuid4()),
            "upstream": upstream or [],
        })
        if error:
            assert result.is_error, "非法调用必须拒绝"
            return result.model_dump(mode="json")
        assert not result.is_error, result.model_dump(mode="json")
        value = result.structured_content
        assert isinstance(value, dict) and set(value) == {"data", "resources", "status", "run_id", "costs"}, value
        return value

    def state(name):
        return runtime("snapshot", inputs_path, directory, source_root, run_id, name)

    async def turn(payload, request_id):
        first = await call("customer", "customer", "submit", payload, request_id)
        repeated = await call("customer", "customer", "submit", payload, request_id)
        assert first["data"]["id"] == repeated["data"]["id"]
        deadline = monotonic() + 160
        while monotonic() < deadline:
            result = await call("customer", "customer", "turn", {"turn_id": first["data"]["id"]})
            if result["status"] not in {"queued", "running"}:
                assert result["status"] == "completed", result
                return result
            await asyncio.sleep(0.25)
        raise TimeoutError("真实客服业务未在限定时间完成")

    try:
        async with AsyncExitStack() as stack:
            for account in ("admin", "customer", "assessment"):
                stderr = stack.enter_context((directory / f"{account}-stdio.stderr.txt").open("w", encoding="utf-8"))
                parameters = StdioServerParameters(command=sys.executable,
                    args=["-m", "agent_hub", "--config", inputs["hub_configs"][account]], cwd=str(ROOT),
                    env={**os.environ, "TMPDIR": str(ROOT / ".scratch/runtime")})
                streams = await stack.enter_async_context(stdio_client(parameters, errlog=stderr))
                client = await stack.enter_async_context(ClientSession(*streams))
                await client.initialize()
                clients[account] = client
            harness = await call("admin", "harness", "show", {"task_id": inputs["harness_task_id"]})
            assert digest(harness["data"]["result"]) == inputs["harness_result_sha256"]
            material = inputs["poster_input"]["materials"][0]
            asset_result = await clients["admin"].call_tool("shop_materials_show", {
                "payload": {"asset_id": material["asset_id"]}, "request_id": str(uuid4()), "upstream": []})
            assert not asset_result.is_error
            asset = asset_result.structured_content
            assert any(item["sha256"] == material["file_sha256"] for item in asset["resources"]
                       if item["resource_type"] == "asset_version")
            record("fixed_harness_material_sources", {"harness": harness, "material": asset})
            conversation = await call("customer", "customer", "create_conversation",
                {"title": run_id}, inputs["request_ids"]["conversation"])
            conversation_id = conversation["data"]["id"]
            repeated = await call("customer", "customer", "create_conversation",
                {"title": run_id}, inputs["request_ids"]["conversation"])
            assert repeated["data"]["id"] == conversation_id
            await call("customer", "customer", "create_conversation", {"title": run_id + "冲突"},
                       inputs["request_ids"]["conversation"], error=True)
            order_turn = await turn({"conversation_id": conversation_id, "message": inputs["order_prompt"]},
                                    inputs["request_ids"]["order"])
            result = order_turn["data"]["result"]
            assert any(row["order_number"] == inputs["order_number"] for row in result["orders"])
            assert result["sources"] and result["text"]
            record("customer_actual_order", order_turn)
            pending_turn = await turn({"conversation_id": conversation_id, "message": inputs["ticket_prompt"]},
                                      inputs["request_ids"]["ticket"])
            pending = pending_turn["data"]["result"]["pending_ticket"]
            assert pending and pending_turn["data"]["result"]["ticket"] is None
            confirmation = {"conversation_id": conversation_id, "message": "确认", "confirm_pending": pending["id"]}
            confirmed = await turn(confirmation, inputs["request_ids"]["confirm"])
            ticket = confirmed["data"]["result"]["ticket"]
            assert ticket and ticket["order_number"] == inputs["order_number"]
            stable = state("ticket-before-reuse.json")
            reused = await call("customer", "customer", "submit", confirmation, inputs["request_ids"]["confirm"])
            assert reused["data"]["id"] == confirmed["data"]["id"]
            assert state("ticket-after-reuse.json") == stable
            record("confirmed_ticket_and_zero_duplicate", {"pending": pending_turn, "confirmed": confirmed})
            assessment = await call("assessment", "assessment", "create",
                {"prompt": inputs["assessment_prompt"]}, inputs["request_ids"]["assessment_create"])
            assessment_id = assessment["data"]["id"]
            assessment = await call("assessment", "assessment", "run", {"run_id": assessment_id},
                                    inputs["request_ids"]["assessment_run"])
            assert assessment["status"] == "completed"
            assert assessment["data"]["result"]["observations"] and assessment["data"]["result"]["evidence"]
            assert {row["employee_id"] for row in assessment["data"]["metrics"]["employees"]} == {275}
            stable = state("assessment-before-reuse.json")
            repeated = await call("assessment", "assessment", "resume", {"run_id": assessment_id})
            assert repeated["data"]["result"] == assessment["data"]["result"]
            assert state("assessment-after-reuse.json") == stable
            record("assessment_actual_report_and_reuse", assessment)
            upstream = harness["resources"] + [item for item in asset["resources"]
                if item["resource_type"] == "asset_version" and item["resource_id"] == material["version_id"]]
            prepared = await call("admin", "poster", "prepare", {"task_id": inputs["harness_task_id"]},
                                 inputs["request_ids"]["poster_prepare"], upstream)
            assert prepared["data"]["job"]["job_id"] == inputs["poster_job_id"]
            assert 2 <= len(prepared["data"]["job"]["keyword_plan"]["keywords"]) <= 5
            stable = state("poster-before-reuse.json")
            repeated = await call("admin", "poster", "prepare", {"task_id": inputs["harness_task_id"]},
                                 inputs["request_ids"]["poster_prepare"], upstream)
            assert repeated["data"]["job"]["keyword_plan"] == prepared["data"]["job"]["keyword_plan"]
            waiting = await call("admin", "poster", "run", {"job_id": inputs["poster_job_id"]},
                                 inputs["request_ids"]["poster_run"])
            assert waiting["status"] == "waiting_agents"
            after_waiting = state("poster-after-reuse.json")
            assert after_waiting["requests"] == stable["requests"]
            assert after_waiting["spent_cny"] == stable["spent_cny"]
            assert after_waiting["reserved_cny"] == stable["reserved_cny"]
            record("poster_actual_keywords_upstream_and_waiting", {"prepared": prepared, "waiting": waiting})
            report["status"] = "checking"
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        after = runtime("close", inputs_path, directory, source_root, run_id, "after.json")
        original = {row["id"]: row for row in before["requests"]}
        assert all(row == original[row["id"]] for row in after["requests"] if row["id"] in original)
        baseline = json.loads((inputs_path.parent / "baseline.json").read_text(encoding="utf-8"))
        baseline_ids = {row["id"] for row in baseline["requests"]}
        added = [row for row in after["requests"] if row["id"] not in baseline_ids]
        assert all(row["integration_run_id"] == run_id for row in added)
        amount = sum((Decimal(row["charged_cny"] or row["reservation_cny"]) for row in added), Decimal(0))
        assert amount <= Decimal(5)
        charged = sum((Decimal(row["charged_cny"] or "0") for row in added), Decimal(0))
        reserved = sum((Decimal(row["reservation_cny"]) for row in added
                        if row["status"] in {"reserved", "sent", "unknown"}), Decimal(0))
        assert Decimal(after["spent_cny"]) - Decimal(baseline["spent_cny"]) == charged
        assert Decimal(after["reserved_cny"]) - Decimal(baseline["reserved_cny"]) == reserved
        report["window_requests"] = added
        report["window_spent_and_reserved_cny"] = str(amount)
        report["ticket_increment"] = sorted(set(after["ticket_ids"]) - set(baseline["ticket_ids"]))
        report["integration_calls"] = after["integration_calls"]
        if report["status"] == "checking":
            assert all(row["status"] == "completed" for row in added)
            assert {row["stage"] for row in added} == {"customer", "assessment", "m3"}
            assert len(report["ticket_increment"]) == 1
            record("historical_requests_and_unknown_preserved", True)
            record("five_cny_persistent_budget", {"amount": str(amount), "window": after["window"]})
            record("account_delta_matches_provider_requests", {"charged_cny": str(charged), "reserved_cny": str(reserved)})
            report["status"] = "passed"
        report["source_after_sha256"] = source_manifest()
        assert report["source_after_sha256"] == report["source_sha256"]
        save(directory / "report.json", report)
        print(directory / "report.json")


def main():
    parser = argparse.ArgumentParser(description="通过实际MCP stdio执行固定业务验收")
    parser.add_argument("--inputs", required=True, type=Path)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=False)
    asyncio.run(verify(args.inputs.resolve(), args.directory.resolve(), args.source_root.resolve()))


if __name__ == "__main__":
    main()
