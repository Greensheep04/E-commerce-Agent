import argparse
import asyncio
import json
import traceback
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic

from agent_cluster.failures import failure_detail, failure_status
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from scripts.cluster_quality import verify_result
from scripts.verify_integration_live import save, snapshot, source_manifest
from shop.cluster import ShopCluster
from shop.config import Settings
from shop.runtime_models import ProviderRequest

ROOT = Path(__file__).resolve().parents[1]


def criterion(tool, action, path, operator="nonempty", expected=None, **kwargs):
    return {"tool_name": tool, "action": action, "path": path,
            "operator": operator, "expected": expected, **kwargs}


def definition(case):
    customer = "shop_customer_agent"
    harness = "shop_harness_agent"
    poster = "shop_poster_agent"
    assessment = "shop_assessment_agent"
    allowed = {}
    criteria = []
    name = case["id"]
    if case["category"] == "marketing" or name in {"CN01", "CN04"}:
        allowed = {harness: ["create", "run", "show"], poster: ["prepare", "prepare_direct", "show", "run", "status"],
                   "shop_materials_search": ["search"], "shop_materials_show": ["show"]}
        if name in {"CM01", "CM02", "CM03", "CM04", "CN04"}:
            criteria.extend([criterion(harness, "run", ["data", "result", "status"], "equals", "materials_found"),
                             criterion(harness, "run", ["data", "result", "review", "approved"], "equals", True),
                             criterion(harness, "run", ["resources"], "resource", module_id="harness", resource_type="task")])
        if name in {"CM04", "CM05", "CM06", "CN01"}:
            action = "prepare" if name == "CM04" else "prepare_direct"
            criteria.append(criterion(poster, action, ["data", "job", "keyword_plan", "keywords"],
                                      min_matches=2 if name == "CM06" else 1))
        if name == "CN01":
            criteria.append(criterion(poster, "run", ["data", "job", "status"], "equals", "completed"))
        material_fields = {"gender": "Men", "articleType": "Tshirts"}
        if name == "CM03":
            material_fields["usage"] = "Casual"
        colours = ["Black", "Blue"] if name == "CM06" else [
            "Blue" if name == "CM02" else "Black" if name in {"CM01", "CM04", "CM05", "CN01"} else None]
        for colour in colours:
            expected_fields = {**material_fields, **({"baseColour": colour} if colour else {})}
            if name == "CN04":
                expected_fields["baseColour"] = "cluster-unavailable-colour-20261003"
            if name in {"CM01", "CM02", "CM03", "CM04", "CN04"}:
                for field, value in expected_fields.items():
                    criteria.append(criterion(harness, "run", ["data", "result", "recommendations", 0,
                                                              "original_fields", field], "equals", value))
            if name in {"CM04", "CM05", "CM06", "CN01"}:
                action = "prepare" if name == "CM04" else "prepare_direct"
                for field, value in expected_fields.items():
                    criteria.append(criterion(poster, action, ["data", "job", "input_data", "materials", 0,
                                                               "original_fields", field], "equals", value))
    elif case["category"] == "customer" or name in {"CN02", "CN03"}:
        allowed = {customer: ["orders", "order", "identity", "create_conversation", "submit", "turn", "conversation", "events"]}
        if name in {"CC01", "CC03", "CC06"}:
            criteria.append(criterion(customer, "orders", ["data", "orders"]))
        if name in {"CC01", "CC06"}:
            criteria.extend([criterion(customer, "orders", ["data", "limit"], "equals", 5 if name == "CC01" else 3),
                             criterion(customer, "orders", ["data", "offset"], "equals", 0)])
        if name in {"CC02", "CC06", "CN02"}:
            criteria.extend([criterion(customer, "order", ["data", "items"]),
                             criterion(customer, "order", ["data", "source"])])
        if name in {"CC02", "CN02"}:
            criteria.append(criterion(customer, "order", ["data", "order_number"], "equals", "SO74480"))
        if name in {"CC04", "CC05"}:
            criteria.append(criterion(customer, "submit", ["data", "result"]))
            if name == "CC04":
                criteria.append(criterion(customer, "submit", ["data", "result", "ticket_id"]))
        if name == "CN03":
            criteria.append(criterion(customer, None, ["data", "refund_id"]))
    else:
        allowed = {assessment: ["create", "run", "show", "metrics", "evidence"]}
        action = "run" if name == "CA03" else "evidence" if name == "CA02" else "metrics"
        path = ["data", "result"] if name == "CA03" else ["data", "orders"] if name == "CA02" else ["data", "employees"]
        criteria.append(criterion(assessment, action, path))
        if name in {"CA01", "CA02", "CA04"}:
            criteria.append(criterion(assessment, action, ["data", "source"]))
    return {"objective": case["objective"], "allowed_operations": allowed, "criteria": criteria,
            "required_confirmations": ["customer_ticket"] if name == "CC04" else [],
            "max_decisions": 12, "max_tasks": 80, "budget_cny": "30"}


def execution_manifest():
    manifest = source_manifest()
    paths = [*sorted((ROOT / "agent-cluster/src").rglob("*.py")),
             *sorted((ROOT / "agent-hub/src").rglob("*.py")),
             ROOT / "agent-hub/pyproject.toml", ROOT / "agent-hub/uv.lock",
             ROOT / "agent-cluster/pyproject.toml", ROOT / "agent-cluster/uv.lock",
             ROOT / "scripts/cluster_quality.py", ROOT / "scripts/verify_cluster_live.py"]
    for path in paths:
        manifest[str(path.relative_to(ROOT))] = sha256(path.read_bytes()).hexdigest()
    return manifest


def usage_records(maker, run_id):
    with maker() as session:
        rows = session.scalars(select(ProviderRequest).where(
            ProviderRequest.price_basis["cluster_run_id"].astext == run_id).order_by(ProviderRequest.id)).all()
        return [{"id": row.id, "run_id": row.run_id, "step_key": row.step_key,
                 "status": row.status, "service": row.service, "model": row.model,
                 "usage": row.usage, "price_basis": row.price_basis,
                 "charged_cny": None if row.charged_cny is None else str(row.charged_cny),
                 "reservation_cny": str(row.reservation_cny),
                 "request_sha256": sha256(json.dumps(row.request_body, sort_keys=True).encode()).hexdigest(),
                 "response_sha256": None if row.response_body is None else sha256(
                     json.dumps(row.response_body, sort_keys=True).encode()).hexdigest(),
                 "duration_seconds": (row.response_body or {}).get("client_duration_seconds")}
                for row in rows]


def verify_configuration(current_path, saved_path):
    current = json.loads(current_path.read_text())
    saved = json.loads(saved_path.read_text())
    if current != saved:
        raise ValueError("验收配置与登记配置不一致")
    return current


def aggregate(directory, *, formal):
    records = [json.loads(path.read_text()) for path in sorted(directory.glob("*-*/report.json"))]
    passed = sum(record["quality"]["passed"] for record in records)
    safety = [issue for record in records for issue in record["quality"].get("safety_issues", [])]
    report = {"kind": "formal" if formal else "development", "expected_runs": 60,
              "executed_runs": len(records), "passed_runs": passed, "failed_runs": len(records) - passed,
              "safety_issues": safety, "records": records,
              "passed": formal and len(records) == 60 and passed >= 57 and not safety,
              "updated_at": datetime.now(UTC).isoformat()}
    save(directory / "report.json", report)
    return report


async def execute(args):
    inputs = json.loads(args.inputs.read_text())
    cases = inputs["fixture"]["cases"]
    directory = args.directory
    directory.mkdir(parents=True, exist_ok=True)
    if args.formal and (args.case or args.repetitions != 3):
        raise ValueError("正式验收必须执行全部20项各三次")
    definitions_path = directory / "definitions.json"
    if definitions_path.exists():
        fixed = json.loads(definitions_path.read_text())
        assert fixed["source_sha256"] == inputs["source_sha256"]
        assert fixed["fixture_sha256"] == inputs["fixture_sha256"]
        assert fixed["execution_source_sha256"] == execution_manifest()
        verify_configuration(args.cluster_config, directory / "configuration.json")
    else:
        fixed = {"source_sha256": inputs["source_sha256"], "fixture_sha256": inputs["fixture_sha256"],
                 "cases": [{**case, "definition": definition(case)} for case in cases],
                 "repetitions": 3, "minimum_successes": 57,
                 "execution_source_sha256": execution_manifest(),
                 "frozen_at": datetime.now(UTC).isoformat()}
        save(definitions_path, fixed)
        save(directory / "baseline.json", snapshot(Settings()))
        save(directory / "source.json", execution_manifest())
        save(directory / "configuration.json", json.loads(args.cluster_config.read_text()))
    settings = Settings(cluster_config_file=args.cluster_config)
    engine = create_engine(settings.database_url, pool_size=10, max_overflow=10)
    maker = sessionmaker(engine, expire_on_commit=False)
    cluster = ShopCluster(settings, engine, maker)
    from agent_cluster.types import GoalDefinition, TriggerEvent

    try:
        for case in fixed["cases"]:
            if args.case and case["id"] not in args.case:
                continue
            for repetition in range(1, args.repetitions + 1):
                item_dir = directory / f"{case['id']}-{repetition}"
                item_dir.mkdir(exist_ok=True)
                report_path = item_dir / "report.json"
                if report_path.exists():
                    continue
                actor_id = inputs["accounts"][case["actor"]]["actor_id"]
                goal_path = item_dir / "goal.json"
                if goal_path.exists():
                    goal = GoalDefinition.model_validate_json(goal_path.read_text())
                else:
                    goal = GoalDefinition(actor_id=actor_id, **case["definition"])
                    await cluster.validate_goal(goal)
                    cluster.store.create_goal(goal)
                    save(goal_path, goal.model_dump(mode="json"))
                event = TriggerEvent(event_id=f"acceptance:{directory.name}:{case['id']}:{repetition}",
                                     goal_id=goal.goal_id, actor_id=actor_id, kind="manual")
                run = cluster.store.trigger(event)
                save(item_dir / "run-start.json", run.model_dump(mode="json"))
                started = monotonic()
                error = None
                try:
                    run = await cluster.engine.run(run.run_id, actor_id)
                except Exception as exception:  # noqa: BLE001
                    # 验收保存完整失败并计入登记样本。
                    error = failure_detail(exception)
                    (item_dir / "exception.txt").write_text("".join(traceback.format_exception(exception)))
                    current = cluster.store.get_run(run.run_id, actor_id)
                    if current.status in {"queued", "running"}:
                        cluster.store.set_run(run.run_id, actor_id, status=failure_status(exception), error=error)
                    run = cluster.store.get_run(run.run_id, actor_id)
                tasks = cluster.store.tasks(run.run_id, actor_id)
                history = cluster.store.history(run.run_id, actor_id)
                save(item_dir / "tasks.json", tasks)
                save(item_dir / "history.json", history)
                save(item_dir / "run.json", run.model_dump(mode="json"))
                quality = verify_result(case, run, tasks, settings, inputs,
                                        json.loads((directory / "baseline.json").read_text()))
                usage = usage_records(maker, run.run_id)
                save(item_dir / "usage.json", usage)
                spent = sum((Decimal(row["charged_cny"]) for row in usage if row["charged_cny"] is not None), Decimal(0))
                reserved = sum((Decimal(row["reservation_cny"]) for row in usage if row["charged_cny"] is None), Decimal(0))
                save(report_path, {"case_id": case["id"], "repetition": repetition, "run_id": run.run_id,
                                   "goal_id": goal.goal_id, "elapsed_seconds": monotonic() - started,
                                   "model_decisions": run.decision_count,
                                   "registered_business_tasks": len(tasks),
                                   "dispatched_business_tasks": sum(task["arguments"] is not None for task in tasks),
                                   "coordinator_tool_calls": sum(len(message.get("tool_calls", [])) for message in history),
                                   "actual_status": run.status, "expected_status": case["expected"],
                                   "spent_cny": str(spent), "reserved_cny": str(reserved),
                                   "prompt_tokens": sum((row["usage"] or {}).get("prompt_tokens", 0) for row in usage),
                                   "completion_tokens": sum((row["usage"] or {}).get("completion_tokens", 0) for row in usage),
                                   "quality": quality, "error": error})
                print(f"{case['id']}-{repetition}: {run.status}, quality={quality['passed']}", flush=True)
                save(directory / "current-usage.json", snapshot(settings))
                aggregate(directory, formal=args.formal)
        assert fixed["execution_source_sha256"] == execution_manifest()
        report = aggregate(directory, formal=args.formal)
        if args.formal:
            assert report["passed"]
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="执行固定目标真实MCP及模型集成验收")
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--cluster-config", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--case", action="append")
    parser.add_argument("--repetitions", type=int, default=3, choices=[1, 2, 3])
    parser.add_argument("--formal", action="store_true")
    args = parser.parse_args()
    asyncio.run(execute(args))


if __name__ == "__main__":
    main()
