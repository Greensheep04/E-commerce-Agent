import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from langgraph.checkpoint.postgres import PostgresSaver
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.budget import VERIFICATION_LIMIT
from shop.campaign import CampaignService
from shop.campaign_types import RequirementPlan
from shop.config import Settings
from shop.fashion import REVISION, SOURCE_ID
from shop.model_profiles import profiles
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest, ToolCall

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_FILES = (
    "src/shop/campaign.py",
    "src/shop/harness.py",
    "src/shop/paid_models.py",
    "src/shop/budget.py",
    "src/shop/runtime_models.py",
    "src/shop/model_profiles.py",
    "src/shop/config.py",
    "src/shop/fashion.py",
    "tests/test_campaign_runtime.py",
    "scripts/verify_campaign_checkpoint.py",
)
PROMPT = "请为一款基础白色女士T恤撰写宣传页，推荐一件。不得编造价格、库存或材质信息。"


def source_hashes() -> dict[str, str]:
    return {name: sha256((PROJECT_ROOT / name).read_bytes()).hexdigest() for name in SOURCE_FILES}


def database_snapshot(database_url: str) -> dict:
    engine = create_engine(database_url)
    try:
        with Session(engine) as session:
            account = session.get(BudgetAccount, "project")
            requests = session.scalars(
                select(ProviderRequest).where(
                    ProviderRequest.price_basis["verification"].as_boolean()
                )
            ).all()
            total = sum(
                (item.charged_cny if item.charged_cny is not None else item.reservation_cny)
                for item in requests
            )
            return {
                "spent_cny": str(account.spent_cny if account is not None else 0),
                "reserved_cny": str(account.reserved_cny if account is not None else 0),
                "verification_total_cny": str(total),
                "verification_limit_cny": str(VERIFICATION_LIMIT),
                "verification_requests": len(requests),
            }
    finally:
        engine.dispose()


def inspect_task(service: CampaignService, checkpointer, task_id: str) -> dict:
    graph = service._graph(checkpointer, task_id)
    config = {"configurable": {"thread_id": task_id}}
    state = graph.get_state(config)
    checkpoints = list(checkpointer.list(config))
    with service.maker() as session:
        task = session.get(HarnessTask, task_id)
        requests = session.scalars(
            select(ProviderRequest)
            .where(ProviderRequest.task_id == task_id)
            .order_by(ProviderRequest.created_at)
        ).all()
        tool_calls = session.scalars(select(ToolCall).where(ToolCall.task_id == task_id)).all()
        plan = state.values.get("plan")
        if plan is not None:
            RequirementPlan.model_validate(plan)
        return {
            "process_id": os.getpid(),
            "task_id": task.id,
            "task_status": task.status,
            "model_calls": task.model_calls,
            "tool_calls": task.tool_calls,
            "elapsed_seconds": str(task.elapsed_seconds),
            "result_is_empty": task.result is None,
            "next": list(state.next),
            "state_keys": sorted(state.values),
            "plan_sha256": (
                sha256(
                    json.dumps(plan, ensure_ascii=False, sort_keys=True).encode("utf-8")
                ).hexdigest()
                if plan is not None
                else None
            ),
            "checkpoint_count": len(checkpoints),
            "checkpoint_ids": [
                item.config["configurable"].get("checkpoint_id") for item in checkpoints
            ],
            "requests": [
                {
                    "id": item.id,
                    "provider_request_id": item.provider_request_id,
                    "step_key": item.step_key,
                    "service": item.service,
                    "model": item.model,
                    "status": item.status,
                    "usage": item.usage,
                    "charged_cny": str(item.charged_cny) if item.charged_cny is not None else None,
                    "has_saved_response": item.response_body is not None,
                }
                for item in requests
            ],
            "tool_record_count": len(tool_calls),
        }


def worker_run(task_id: str) -> None:
    service = CampaignService(Settings())
    try:
        result = service.run(task_id, hold_at_checkpoint=True)
    except KeyboardInterrupt:
        print(json.dumps({"process_id": os.getpid(), "interrupted_after_checkpoint": True}))
        return
    print(
        json.dumps(
            {
                "process_id": os.getpid(),
                "run_returned": True,
                "task_status": result["status"],
                "next": result.get("next"),
            },
            ensure_ascii=False,
        )
    )


def worker_read(task_id: str) -> None:
    settings = Settings()
    service = CampaignService(settings)
    conn_string = settings.database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    with PostgresSaver.from_conn_string(conn_string) as checkpointer:
        print(json.dumps(inspect_task(service, checkpointer, task_id), ensure_ascii=False))


def write_process_output(evidence_dir: Path, name: str, stdout: str, stderr: str) -> None:
    (evidence_dir / f"{name}.stdout.txt").write_text(stdout, encoding="utf-8")
    (evidence_dir / f"{name}.stderr.txt").write_text(stderr, encoding="utf-8")


def run_verification() -> dict:
    settings = Settings()
    run_id = f"har-campaign-checkpoint-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_dir = PROJECT_ROOT / ".scratch" / "evidence" / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    before_budget = database_snapshot(settings.database_url)
    if float(before_budget["verification_total_cny"]) >= VERIFICATION_LIMIT:
        raise ValueError("验证额度已用完，停止发送模型请求")

    service = CampaignService(settings)
    created = service.create(PROMPT)
    task_id = created["task_id"]
    conn_string = settings.database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    runner_command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "run",
        "--task-id",
        task_id,
    ]
    reader_command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "read",
        "--task-id",
        task_id,
    ]

    with PostgresSaver.from_conn_string(conn_string) as checkpointer:
        checkpointer.setup()
        graph = service._graph(checkpointer, task_id)
        config = {"configurable": {"thread_id": task_id}}
        runner = subprocess.Popen(
            runner_command,
            cwd=PROJECT_ROOT,
            env=dict(os.environ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 135
        reached_retrieve = False
        observed = None
        while time.monotonic() < deadline:
            if runner.poll() is not None:
                break
            snapshot = graph.get_state(config)
            if snapshot.next == ("retrieve",):
                reached_retrieve = True
                observed = inspect_task(service, checkpointer, task_id)
                runner.send_signal(signal.SIGINT)
                break
            time.sleep(0.25)

        try:
            runner_stdout, runner_stderr = runner.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            runner.wait()
            runner_stdout, runner_stderr = runner.communicate()
        write_process_output(evidence_dir, "campaign-run", runner_stdout, runner_stderr)

        if runner.returncode != 0:
            raise RuntimeError(f"CampaignService.run 进程退出，状态码为 {runner.returncode}")
        if not reached_retrieve:
            raise RuntimeError("Campaign 图未到达 retrieve 前的 checkpoint")
        runner_result = json.loads(runner_stdout)
        if not runner_result.get("interrupted_after_checkpoint"):
            raise AssertionError("CampaignService.run 进程没有在 checkpoint 后按预期结束")

        after_run = inspect_task(service, checkpointer, task_id)

    no_provider_file = evidence_dir / "no-provider.env"
    no_provider_file.write_text("", encoding="utf-8")
    reader_environment = dict(os.environ)
    reader_environment["SHOP_PROVIDER_ENV_FILE"] = str(no_provider_file)
    reader = subprocess.run(
        reader_command,
        cwd=PROJECT_ROOT,
        env=reader_environment,
        capture_output=True,
        text=True,
        check=False,
    )
    write_process_output(evidence_dir, "checkpoint-reader", reader.stdout, reader.stderr)
    if reader.returncode != 0:
        raise RuntimeError(f"checkpoint 读取进程退出，状态码为 {reader.returncode}")
    restored = json.loads(reader.stdout)

    if restored["process_id"] == runner_result["process_id"]:
        raise AssertionError("checkpoint 读取没有使用独立进程")
    if restored["next"] != ["retrieve"]:
        raise AssertionError("新进程读取到的下一个节点不是 retrieve")
    if "plan" not in restored["state_keys"] or restored["plan_sha256"] is None:
        raise AssertionError("checkpoint 未保存可验证的需求计划")
    if {"vector", "candidates"}.intersection(restored["state_keys"]):
        raise AssertionError("checkpoint 已包含 retrieve 阶段数据")
    if restored["model_calls"] != 1 or len(restored["requests"]) != 1:
        raise AssertionError("单次 understand 请求的任务计数不一致")
    request = restored["requests"][0]
    if request["step_key"] != "understand" or request["status"] != "completed":
        raise AssertionError("understand 请求没有保存为 completed")
    if not request["has_saved_response"]:
        raise AssertionError("understand 响应没有持久保存")
    if restored["tool_calls"] != 0 or restored["tool_record_count"] != 0:
        raise AssertionError("retrieve 前出现了工具调用")
    if not restored["result_is_empty"] or restored["task_status"] != "running":
        raise AssertionError("暂停任务状态与预期不符")
    stable_fields = (
        "model_calls",
        "tool_calls",
        "elapsed_seconds",
        "next",
        "plan_sha256",
        "checkpoint_count",
        "checkpoint_ids",
        "requests",
    )
    for field in stable_fields:
        if after_run[field] != restored[field]:
            raise AssertionError(f"跨进程只读期间状态发生变化：{field}")

    after_budget = database_snapshot(settings.database_url)
    report = {
        "run_id": run_id,
        "status": "checkpoint_read_verified_full_resume_pending",
        "milestone": "HAR-01",
        "task_id": task_id,
        "workflow_version": created["workflow_version"],
        "prompt": PROMPT,
        "source": {"id": SOURCE_ID, "revision": REVISION, "queried": False},
        "model_profiles": {
            role: profile.model_dump()
            for role, profile in profiles(settings.harness_token_limits_file).items()
        },
        "budget": {"before": before_budget, "after": after_budget},
        "campaign_run_process": {
            "command": runner_command,
            "process_id": runner_result["process_id"],
            "return_code": runner.returncode,
            "stdout": str((evidence_dir / "campaign-run.stdout.txt").relative_to(PROJECT_ROOT)),
            "stderr": str((evidence_dir / "campaign-run.stderr.txt").relative_to(PROJECT_ROOT)),
            "checkpoint_observed_before_interrupt": observed,
        },
        "new_process_read": {
            "command": reader_command,
            "provider_environment_file": str(no_provider_file.relative_to(PROJECT_ROOT)),
            "stdout": str(
                (evidence_dir / "checkpoint-reader.stdout.txt").relative_to(PROJECT_ROOT)
            ),
            "stderr": str(
                (evidence_dir / "checkpoint-reader.stderr.txt").relative_to(PROJECT_ROOT)
            ),
            "state_after_campaign_process": after_run,
            "state_read_by_new_process": restored,
        },
        "full_resume": {
            "status": "pending",
            "reason": "本次只读取 checkpoint，未调用 invoke(None)；主代理将在图库齐备后继续同一 task_id。",
            "task_id": task_id,
        },
        "source_sha256": source_hashes(),
    }
    report_path = evidence_dir / "report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return {
        "run_id": run_id,
        "status": report["status"],
        "task_id": task_id,
        "report": str(report_path.relative_to(PROJECT_ROOT)),
        "provider_request_id": request["provider_request_id"],
        "provider_request_record_id": request["id"],
        "checkpoint_count": restored["checkpoint_count"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="验证 Campaign checkpoint 的跨进程读取")
    parser.add_argument("--worker", choices=("run", "read"))
    parser.add_argument("--task-id")
    args = parser.parse_args()
    if args.worker == "run":
        if not args.task_id:
            parser.error("run worker 必须提供 --task-id")
        worker_run(args.task_id)
        return
    if args.worker == "read":
        if not args.task_id:
            parser.error("read worker 必须提供 --task-id")
        worker_read(args.task_id)
        return
    print(json.dumps(run_verification(), ensure_ascii=False))


if __name__ == "__main__":
    main()
