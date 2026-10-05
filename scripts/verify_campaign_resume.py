import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from langgraph.checkpoint.postgres import PostgresSaver
from sqlalchemy import select
from verify_campaign_checkpoint import database_snapshot, inspect_task, source_hashes

from shop.asset_index import MAIN_SOURCE
from shop.campaign import CampaignService
from shop.config import Settings
from shop.models import Asset


def main() -> None:
    parser = argparse.ArgumentParser(description="继续已保存的真实宣传任务并核验跨进程恢复")
    parser.add_argument("checkpoint_report", type=Path)
    parser.add_argument("--resume-report", type=Path)
    args = parser.parse_args()
    original = json.loads(args.checkpoint_report.read_text(encoding="utf-8"))
    task_id = original["task_id"]
    reference = original["new_process_read"]["state_read_by_new_process"]
    settings = Settings()
    service = CampaignService(settings)
    connection = settings.database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    run_id = f"har-resume-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    directory = Path(".scratch/evidence") / run_id
    directory.mkdir(parents=True, exist_ok=False)
    recorded = (json.loads(args.resume_report.read_text(encoding="utf-8"))
                if args.resume_report else None)
    if recorded:
        assert recorded["task_id"] == task_id
        before = recorded["before"]
    else:
        with PostgresSaver.from_conn_string(connection) as checkpointer:
            before = inspect_task(service, checkpointer, task_id)
    assert before["next"] == ["retrieve"]
    assert before["task_status"] == "running"
    for field in ("model_calls", "tool_calls", "elapsed_seconds", "plan_sha256", "requests"):
        assert before[field] == reference[field], field
    budget_before = database_snapshot(settings.database_url)
    command = [sys.executable, "scripts/harness.py", "run", task_id]
    started = {"run_id": run_id, "task_id": task_id,
               "checkpoint_report": str(args.checkpoint_report), "before": before,
               "budget_before": budget_before, "command": command,
               "source_sha256": source_hashes()}
    (directory / "started.json").write_text(
        json.dumps(started, ensure_ascii=False, indent=2), encoding="utf-8")
    if recorded:
        process_id, exit_code = recorded["process_id"], recorded["exit_code"]
        started["resume_report"] = str(args.resume_report)
        started["execution"] = "reuse_saved_resume_execution"
    else:
        with (
            (directory / "resume.stdout.txt").open("w", encoding="utf-8") as stdout,
            (directory / "resume.stderr.txt").open("w", encoding="utf-8") as stderr,
        ):
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr, text=True)
            exit_code = process.wait(timeout=150)
            process_id = process.pid
    with PostgresSaver.from_conn_string(connection) as checkpointer:
        after = inspect_task(service, checkpointer, task_id)
    task = service.show(task_id)
    if recorded:
        for field in ("next", "requests", "model_calls", "tool_calls", "elapsed_seconds",
                      "plan_sha256", "checkpoint_ids"):
            assert after[field] == recorded["after"][field], field
        assert task == recorded["task"]
    plan = task["result"]["plan"] if task["result"] else None
    matching = []
    if plan:
        with service.maker() as session:
            assets = session.scalars(select(Asset).where(Asset.source_id == MAIN_SOURCE)).all()
            for asset in assets:
                fields = asset.versions[-1].original_fields
                if (all(fields.get(key) == value for key, value in plan["attributes"].items())
                        and all(fields.get(key) not in values
                                for key, values in plan["forbidden_attributes"].items())):
                    matching.append(asset.id)
    result = task["result"]
    outcome_valid = bool(result and (
        (result["status"] == "materials_found" and result["review"]["approved"]
         and len(result["recommendations"]) == plan["count"]
         and all(item["asset_id"] in matching for item in result["recommendations"]))
        or (result["status"] == "insufficient" and len(matching) < plan["count"]
            and not result["recommendations"] and result["missing_info"])))
    original_ids = {request["id"] for request in before["requests"]}
    retained = [request for request in after["requests"] if request["id"] in original_ids]
    checks = {
        "new_process": process_id != original["campaign_run_process"]["process_id"],
        "saved_understand_unchanged": retained == before["requests"],
        "one_understand": sum(row["step_key"] == "understand" for row in after["requests"]) == 1,
        "plan_unchanged": before["plan_sha256"] == after["plan_sha256"],
        "counters_preserved": after["model_calls"] >= before["model_calls"]
                              and after["tool_calls"] >= before["tool_calls"],
        "time_preserved": Decimal(after["elapsed_seconds"]) >= Decimal(before["elapsed_seconds"]),
        "limits": after["model_calls"] <= 6 and after["tool_calls"] <= 10
                  and Decimal(after["elapsed_seconds"]) < 120,
        "completed": exit_code == 0 and task["status"] == "completed" and outcome_valid,
        "graph_finished": after["next"] == [],
    }
    report = {**started, "milestone": "HAR-01", "after": after, "task": task,
              "process_id": process_id, "exit_code": exit_code, "checks": checks,
              "source_match_asset_ids": matching,
              "budget_after": database_snapshot(settings.database_url),
              "status": "passed" if all(checks.values()) else "failed"}
    (directory / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(directory / "report.json"),
                      "task_id": task_id, "status": report["status"], "checks": checks}))
    if not all(checks.values()):
        raise AssertionError("完整恢复检查未通过，原始输出与任务状态已保存")
    if result["status"] == "materials_found":
        (directory / "campaign.html").write_text(service.render(task_id), encoding="utf-8")


if __name__ == "__main__":
    main()
