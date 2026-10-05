import argparse
import asyncio
import json
import stat
import sys
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from time import monotonic
from uuid import uuid4

import httpx
from agent_cluster.context import ContextManager
from agent_cluster.engine import SYSTEM_PROMPT, completion_check
from agent_cluster.notes import NoteStore
from agent_cluster.runtime import schedule_trigger
from agent_cluster.store import (
    ClusterStore,
    DecisionRecord,
    EventRecord,
    GoalRecord,
    RunRecord,
    TaskRecord,
)
from agent_cluster.types import (
    GoalDefinition,
    NoteRef,
    ScheduleDefinition,
    TriggerEvent,
)
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from scripts.verify_cluster_live import execution_manifest, usage_records
from scripts.verify_cluster_reliability import SOURCE_FILES as CLU05_SOURCE_FILES
from shop.cluster import ShopCluster
from shop.config import Settings
from shop.customer_runtime_models import CustomerConversation, CustomerTicket, CustomerTurn
from shop.integration_models import IntegrationBudgetWindow, IntegrationCall
from shop.models import Asset
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_DIR = ROOT / ".scratch/evidence/clu-longflow-20261003-r1"
FIXED_INPUTS = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json"
CLUSTER_CONFIG = ROOT / ".scratch/cluster-http-config.json"
HISTORICAL_NOTE_ROOT = ROOT / ".scratch/evidence/clu03-real-context-20261003-r1/summary-budget-green-r1-notes"
HISTORICAL_NOTE_SHA256 = "90f6082d4236f4af7f80664d8f528d5a02c60a77f6a8a25bf5aef62b64a3295c"
ACTIVE_WINDOW_ID = "clu-live-20261003"
BASE_URL = "http://127.0.0.1:8001"
ADDITIONAL_BUDGET_CNY = Decimal("0.50")
SCHEDULE_BUDGET_CNY = Decimal("0.35")
CUSTOMER_BUDGET_CNY = Decimal("0.15")
SCHEDULE_INTERVAL_SECONDS = 8
IDLE_WAIT_SECONDS = 1.0
RUN_WAIT_SECONDS = 240
SENT_WAIT_SECONDS = 90
TERMINAL_TASK_STATUSES = {"completed", "unknown", "failed", "cancelled"}
TERMINAL_RUN_STATUSES = {"completed", "failed", "cancelled", "unknown", "budget_exhausted"}
TEST_CASES = [
    {
        "test": "test_event_deduplication_is_atomic_across_processes",
        "requirement": "相同事件编号与内容并发触发后只生成一个运行",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_goal_revision_rejects_a_competing_stale_update",
        "requirement": "并发目标修改拒绝过期版本",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_plan_version_rejects_a_competing_stale_plan",
        "requirement": "并发计划保存拒绝过期版本",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_cancelled_goal_excludes_run_and_rejects_new_dispatch",
        "requirement": "取消目标阻止排队运行及新业务任务",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_task_request_id_and_arguments_survive_a_new_process",
        "requirement": "新进程恢复时保留任务编号、业务request_id和调用参数",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_coordinator_request_survives_a_new_process",
        "requirement": "新进程恢复时保留协调请求与step编号",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_commit_decision_persists_a_real_response_once_across_processes",
        "requirement": "协调模型真实响应跨进程只提交一次",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_finalize_tool_rolls_back_run_update_when_history_sequence_overflows",
        "requirement": "工具结果与运行状态写入保持事务一致",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py"],
    },
    {
        "test": "test_finalize_tool_persists_run_change_and_tool_result_once",
        "requirement": "工具结果与运行状态可重复读取且只保存一次",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py"],
    },
    {
        "test": "test_advisory_lease_is_released_after_holder_process_termination",
        "requirement": "持锁进程终止后PostgreSQL advisory lease可由新进程取得",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_schedule_coalesces_missed_periods_and_serializes_competing_processes",
        "requirement": "错过周期合并，并发进程只触发一个周期事件",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/src/agent_cluster/runtime.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_schedule_definition_change_and_goal_cancellation_compete",
        "requirement": "日程定义修改与目标取消竞争时拒绝旧日程触发",
        "source_files": ["agent-cluster/src/agent_cluster/store.py", "agent-cluster/src/agent_cluster/runtime.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_concurrent_cluster_budget_reservations_respect_goal_budget",
        "requirement": "并发模型请求费用预留不超过目标费用上限",
        "source_files": ["src/shop/budget.py", "agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_store_processes.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
    {
        "test": "test_real_stdio_runs_multiple_targets_with_shared_slots_and_goal_lease",
        "requirement": "真实MCP stdio支持多个目标并发并共享四个业务slot与目标lease",
        "source_files": ["agent-cluster/src/agent_cluster/engine.py", "agent-cluster/src/agent_cluster/mcp_client.py", "agent-cluster/src/agent_cluster/runtime.py", "agent-cluster/tests/reliability/test_live_reliability.py", "agent-cluster/tests/reliability/conftest.py"],
    },
    {
        "test": "test_completed_real_business_result_recovers_after_process_termination",
        "requirement": "真实MCP业务结果提交后进程终止，新进程复用原结果与request_id",
        "source_files": ["agent-cluster/src/agent_cluster/engine.py", "agent-cluster/src/agent_cluster/mcp_client.py", "agent-cluster/src/agent_cluster/store.py", "agent-cluster/tests/reliability/test_live_reliability.py", "agent-cluster/tests/reliability/clu05_workers.py"],
    },
]


def file_sha256(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save_json(path, value):
    target = Path(path)
    target.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    target.chmod(0o600)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def path_info(path):
    source = Path(path)
    return {"path": str(source.relative_to(ROOT)), "exists": source.is_file(),
            "size_bytes": source.stat().st_size if source.is_file() else None,
            "sha256": file_sha256(source) if source.is_file() else None}


def build_evidence_directory(requested):
    if requested is None:
        run_id = "clu-longflow-independent-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
        directory = ROOT / ".scratch/evidence" / run_id
    else:
        directory = Path(requested).resolve()
        evidence_root = (ROOT / ".scratch/evidence").resolve()
        require(directory.is_relative_to(evidence_root), "证据目录必须位于项目独立证据目录内")
        require(directory.name.startswith("clu-longflow-independent-"), "证据目录名称必须使用本次独立证据前缀")
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    return directory


def fixed_account(inputs, account_name):
    account = inputs["accounts"][account_name]
    password_path = Path(account["password_file"])
    if not password_path.is_absolute():
        password_path = ROOT / password_path
    metadata = password_path.stat()
    require(stat.S_ISREG(metadata.st_mode), f"{account_name}密码文件必须是普通文件")
    require(stat.S_IMODE(metadata.st_mode) == 0o600, f"{account_name}密码文件权限必须为0600")
    password = password_path.read_text(encoding="utf-8").rstrip("\r\n")
    require(bool(password), f"{account_name}密码文件内容不能为空")
    return {**account, "password": password}


def audit_reliability_evidence(report):
    prior_directory = ROOT / ".scratch/evidence/clu05-20261002T182904Z-21e0a8a0"
    prior_report_path = prior_directory / "report.json"
    prior_stdout_path = prior_directory / "pytest.stdout.txt"
    prior = load_json(prior_report_path)
    prior_stdout = prior_stdout_path.read_text(encoding="utf-8")
    prior_before = prior.get("source_sha256_before", {})
    prior_after = prior.get("source_sha256_after", {})
    current_sources = {
        str(path.relative_to(ROOT)): file_sha256(path)
        for path in CLU05_SOURCE_FILES
    }
    require("15 passed" in prior_stdout, "既有CLU-05可靠性输出没有15项通过记录")
    cases = []
    for test in TEST_CASES:
        changed = [path for path in test["source_files"]
                   if path in prior_after and current_sources.get(path) != prior_after[path]]
        cases.append({
            **test,
            "source_changed_since_prior_run": changed,
            "historical_source_sha256": {path: prior_after.get(path) for path in test["source_files"]},
            "current_source_sha256": {path: current_sources.get(path) for path in test["source_files"]},
            "evidence_status": "historical_source_changed" if changed else "same_relevant_source",
        })
    report["clu05_previous_reliability"] = {
        "report": path_info(prior_report_path),
        "stdout": path_info(prior_stdout_path),
        "status": prior.get("status"),
        "exit_code": prior.get("exit_code"),
        "test_count_from_output": 15,
        "paid_model_calls": prior.get("paid_model_calls"),
        "source_unchanged_during_prior_run": prior.get("source_unchanged"),
        "coverage_matrix": cases,
        "all_relevant_sources_unchanged": all(not item["source_changed_since_prior_run"] for item in cases),
        "source_hashes_before": prior_before,
        "source_hashes_after": prior_after,
        "source_hashes_current": current_sources,
    }


def audit_historical_longflow(report, engine, settings):
    state = load_json(HISTORICAL_DIR / "state.json")
    recovered_run = load_json(HISTORICAL_DIR / "recovered-run.json")
    before = load_json(HISTORICAL_DIR / "interruption-ready.json")
    recovery_start = load_json(HISTORICAL_DIR / "recovery-start.json")
    initial_tasks = load_json(HISTORICAL_DIR / "recovered-tasks.json")
    recovered_tasks = initial_tasks
    recovered_history = load_json(HISTORICAL_DIR / "recovered-history.json")
    saved_usage = load_json(HISTORICAL_DIR / "recovered-usage.json")
    note_reference = NoteRef.model_validate(load_json(HISTORICAL_DIR / "early-note.json"))
    interruption = load_json(HISTORICAL_DIR / "process-interruption.json")
    recovered_stderr = (HISTORICAL_DIR / "recovered.stderr.txt").read_text(encoding="utf-8")
    current_manifest = execution_manifest()
    source_changes = {
        path: {"saved": digest, "current": current_manifest.get(path)}
        for path, digest in state["source_sha256"].items()
        if current_manifest.get(path) != digest
    }
    report_path = HISTORICAL_DIR / "report.json"
    require(state["run_id"] == recovered_run["run_id"], "长流程state与恢复运行编号不一致")
    require(recovered_run["goal_id"] == state["goal"]["goal_id"], "恢复运行目标编号不一致")
    require(recovered_run["status"] == "completed", "保存的长流程最终状态不是completed")
    require(interruption["signal"] == "SIGTERM" and interruption["exit_code"] == -15,
            "长流程原进程中断记录不匹配")
    require(recovery_start["process_id"] != interruption["process_id"], "恢复进程与初始进程编号相同")
    require(not report_path.exists(), "原始长流程目录已包含report.json，需重新审阅其来源")
    require(bool(source_changes), "长流程检查源码摘要没有发现差异")

    goal = GoalDefinition.model_validate(state["goal"])
    with Session(engine) as session:
        goal_row = session.get(GoalRecord, goal.goal_id)
        run_row = session.get(RunRecord, state["run_id"])
        require(goal_row is not None and goal_row.actor_id == goal.actor_id,
                "PostgreSQL目标记录不存在或身份不一致")
        require(run_row is not None, "PostgreSQL长流程运行记录不存在")
        require(run_row.document == recovered_run, "PostgreSQL运行文档与恢复进程输出不一致")
        store = ClusterStore(engine)
        database_tasks = store.tasks(state["run_id"], goal.actor_id)
        database_history = store.history(state["run_id"], goal.actor_id)
        require(database_tasks == recovered_tasks, "PostgreSQL任务记录与恢复进程输出不一致")
        require(database_history == recovered_history, "PostgreSQL历史与恢复进程输出不一致")
        decisions = session.scalars(select(DecisionRecord).where(
            DecisionRecord.run_id == state["run_id"]).order_by(DecisionRecord.step_key)).all()
        require(len(decisions) == recovered_run["decision_count"] == 9,
                "PostgreSQL协调决策数与恢复运行决策数不一致")
        require(all(item.response is not None for item in decisions), "存在尚未提交的协调响应")
        decision_shapes = []
        for item in decisions:
            request = item.request
            response = item.response
            messages = request.get("messages", [])
            system_messages = [message for message in messages if message.get("role") == "system"]
            required_response_fields = ["id", "object", "created", "model", "choices"]
            message = response.get("choices", [{}])[0].get("message", {})
            require(len(system_messages) == 1, f"协调步骤{item.step_key}的system消息数量不是1")
            system_content = system_messages[0].get("content")
            saved_prompt_prefix = state["system_prompt"] + "\n\n"
            require(isinstance(system_content, str) and system_content.startswith(saved_prompt_prefix),
                    f"协调步骤{item.step_key}的system消息没有保留已保存提示全文")
            authority_context = json.loads(system_content[len(saved_prompt_prefix):])
            require(authority_context["goal"]["goal_id"] == goal.goal_id
                    and authority_context["goal"]["actor_id"] == goal.actor_id,
                    f"协调步骤{item.step_key}的system授权上下文目标身份不匹配")
            require(authority_context["authorizations"] == goal.allowed_operations
                    and authority_context["criteria"] == [
                        criterion.model_dump(mode="json") for criterion in goal.criteria
                    ], f"协调步骤{item.step_key}的system授权内容与目标定义不匹配")
            require(authority_context["run"]["run_id"] == state["run_id"]
                    and authority_context["run"]["goal_id"] == goal.goal_id,
                    f"协调步骤{item.step_key}的system授权上下文运行编号不匹配")
            require(all(field in response for field in required_response_fields),
                    f"协调步骤{item.step_key}的模型响应缺少标准字段")
            require(message.get("role") == "assistant", f"协调步骤{item.step_key}未保存assistant消息")
            decision_shapes.append({
                "step_key": item.step_key,
                "request_fields": sorted(request),
                "system_message_fields": sorted(system_messages[0]),
                "system_message_count": len(system_messages),
                "saved_prompt_is_exact_system_prefix": True,
                "authority_context_fields": sorted(authority_context),
                "response_fields": sorted(response),
                "required_response_fields": required_response_fields,
                "assistant_message_fields": sorted(message),
            })
        actual_usage = usage_records(store.maker, state["run_id"])
        require(actual_usage == saved_usage, "PostgreSQL模型请求或费用记录与恢复进程输出不一致")
        prior_usage = {row["id"]: row for row in before["usage"]}
        current_usage = {row["id"]: row for row in actual_usage}
        require(all(current_usage.get(request_id) == row for request_id, row in prior_usage.items()),
                "SIGTERM前已保存的ProviderRequest记录发生变化")
        source_tasks = {item["task_id"]: item for item in database_tasks}
        before_tasks = {item["task_id"]: item for item in before["tasks"]}
        require(all(source_tasks[name]["request_id"] == item["request_id"]
                    for name, item in before_tasks.items()),
                "SIGTERM前的业务request_id没有在PostgreSQL恢复记录中保留")
        note_store = NoteStore(HISTORICAL_NOTE_ROOT)
        note_body = note_store.read(note_reference, goal_id=goal.goal_id, actor_id=goal.actor_id)
        note_record = json.loads(note_body)
        note_task = source_tasks[note_record["task_id"]]
        note_body_sha256 = sha256(note_body.encode("utf-8")).hexdigest()
        require(note_reference.sha256 == HISTORICAL_NOTE_SHA256 == note_body_sha256,
                "早期Note索引摘要与Note正文摘要不一致")
        require(note_record["request_id"] == note_task["request_id"], "早期Note引用的业务request_id不匹配")
        require(note_record["status"] == note_task["status"] == "completed",
                "早期Note与PostgreSQL业务任务状态不匹配")
        require(note_reference.resources == [
            type(note_reference.resources[0]).model_validate(item)
            for item in note_task["result"]["resources"]
        ], "早期Note资源引用与原业务结果不一致")
        result_summary = json.loads(recovered_run["summary"])
        passed, checks = completion_check(goal, database_tasks)
        require(passed and result_summary["completion_checks"] == checks,
                "运行摘要中的完成条件与PostgreSQL业务结果不一致")
        source_revision = "3859c76db2f6f3d3b9a3863345e3ccdbff75879d"
        expected_records = {"p1_search_black_tshirt": "30650", "p3_search_blue_tshirt": "14293"}
        source_results = {}
        for task_id, record_id in expected_records.items():
            task = source_tasks[task_id]
            items = task["result"]["data"]["items"]
            matches = [item for item in items if str(item.get("record_id")) == record_id]
            require(matches, f"{task_id}结果中缺少固定来源记录{record_id}")
            require(all(item.get("source_id") == "ashraq/fashion-product-images-small"
                        and item.get("source_revision") == source_revision for item in matches),
                    f"{task_id}来源模块或revision不匹配")
            asset = session.scalar(select(Asset).where(
                Asset.source_id == "ashraq/fashion-product-images-small",
                Asset.source_revision == source_revision,
                Asset.record_id == record_id,
            ))
            require(asset is not None, f"PostgreSQL素材表缺少来源记录{record_id}")
            source_results[task_id] = {
                "request_id": task["request_id"],
                "record_id": record_id,
                "asset_id": asset.id,
                "source_id": asset.source_id,
                "source_revision": asset.source_revision,
                "candidate_count": len(matches),
            }
    compactions = [load_json(path) for path in sorted(HISTORICAL_DIR.glob("compaction-*.json"))]
    require(len(compactions) >= 2, "长流程压缩记录少于两次")
    require(before["early_note_sha256"] == note_body_sha256,
            "恢复前后早期Note正文摘要不一致")
    require(len(database_tasks) == 8 and all(task["status"] == "completed" for task in database_tasks),
            "PostgreSQL长流程任务数量或最终状态不匹配")
    report["historical_longflow"] = {
        "source_directory": str(HISTORICAL_DIR.relative_to(ROOT)),
        "original_report_exists": False,
        "original_parent_traceback_available": False,
        "original_parent_failure_cause": "reported_source_hash_assertion",
        "original_parent_failure_cause_source": "prior_execution_summary",
        "original_parent_failure_cause_directly_observed": False,
        "saved_source_file_count": len(state["source_sha256"]),
        "source_hash_mismatch_files": source_changes,
        "original_process": interruption,
        "recovery_process": recovery_start["process_id"],
        "run_id": state["run_id"],
        "goal_id": goal.goal_id,
        "run_status": recovered_run["status"],
        "task_count": len(database_tasks),
        "task_statuses": {task["task_id"]: task["status"] for task in database_tasks},
        "preserved_request_ids": {task["task_id"]: task["request_id"] for task in database_tasks},
        "decision_count": len(decisions),
        "decision_shapes": decision_shapes,
        "provider_request_count": len(actual_usage),
        "provider_request_records_match_database": True,
        "preserved_preinterruption_request_ids": sorted(prior_usage),
        "compaction_count": len(compactions),
        "early_note": {
            "note_id": note_reference.note_id,
            "version": note_reference.version,
            "sha256": note_body_sha256,
            "topic": note_reference.topic,
            "source": note_reference.source,
            "summary": note_reference.summary,
            "resources": [resource.model_dump(mode="json") for resource in note_reference.resources],
            "body_chars": len(note_body),
            "task_id": note_record["task_id"],
            "task_request_id": note_record["request_id"],
            "recovery_hash_matches": before["early_note_sha256"] == note_body_sha256,
        },
        "completion_check_summary": result_summary,
        "source_records": source_results,
        "recovered_stderr_has_traceback": "Traceback (most recent call last)" in recovered_stderr,
        "original_files": {path.name: path_info(path) for path in sorted(HISTORICAL_DIR.iterdir()) if path.is_file()},
        "checks": [
            "postgres_goal_run_tasks_history_match_original_files",
            "nine_coordinator_responses_preserve_saved_system_prompt_and_authority_context",
            "five_standard_provider_response_fields_present",
            "provider_requests_and_original_request_ids_preserved",
            "early_note_manifest_and_body_digest_match",
            "completion_checks_match_saved_business_results",
            "black_blue_source_records_match_database_and_revision",
            "two_or_more_compactions_and_new_process_recovery",
        ],
    }


def historical_audit(report, engine):
    audit_reliability_evidence(report)
    settings = Settings(cluster_config_file=CLUSTER_CONFIG)
    audit_historical_longflow(report, engine, settings)
    cron = ScheduleDefinition(goal_id="cron-five-field-audit", actor_id="cron-five-field-audit",
                              cron="0 8 * * *", timezone="Asia/Shanghai")
    trigger = schedule_trigger(cron)
    require(type(trigger).__name__ == "CronTrigger", "五字段cron没有构造APScheduler CronTrigger")
    report["scheduler_definition"] = {
        "cron_expression": cron.cron,
        "cron_field_count": len(cron.cron.split()),
        "trigger_type": type(trigger).__name__,
        "apscheduler_version": version("APScheduler"),
    }
    require(report["scheduler_definition"]["cron_field_count"] == 5,
            "cron表达式不是五个字段")


def ledger_snapshot(engine):
    with Session(engine) as session:
        budget = session.get(BudgetAccount, "project")
        require(budget is not None, "项目费用账户不存在")
        window = session.get(IntegrationBudgetWindow, ACTIVE_WINDOW_ID)
        require(window is not None and window.active, "CLU验证费用窗口不存在或未处于活动状态")
        rows = session.scalars(select(ProviderRequest).where(
            ProviderRequest.price_basis["integration_run_id"].astext == ACTIVE_WINDOW_ID)).all()
        total = sum((row.charged_cny if row.charged_cny is not None else row.reservation_cny
                     for row in rows), Decimal(0))
        open_rows = [row for row in rows if row.charged_cny is None]
        return {
            "window_id": window.id,
            "window_limit_cny": str(window.limit_cny),
            "window_consumed_cny": str(total),
            "window_provider_request_count": len(rows),
            "window_open_reservation_count": len(open_rows),
            "window_open_reservation_cny": str(sum((row.reservation_cny for row in open_rows), Decimal(0))),
            "project_spent_cny": str(budget.spent_cny),
            "project_reserved_cny": str(budget.reserved_cny),
        }


def scoped_provider_rows(session, goal_ids, turn_ids):
    rows = session.scalars(select(ProviderRequest).order_by(ProviderRequest.created_at, ProviderRequest.id)).all()
    return [row for row in rows if row.price_basis.get("cluster_goal_id") in goal_ids
            or row.run_id in turn_ids]


def provider_row_json(row):
    request_body = row.request_body
    response_body = row.response_body
    return {
        "provider_request_id": row.id,
        "run_id": row.run_id,
        "step_key": row.step_key,
        "status": row.status,
        "service": row.service,
        "model": row.model,
        "reservation_cny": str(row.reservation_cny),
        "charged_cny": None if row.charged_cny is None else str(row.charged_cny),
        "amount_cny": str(row.charged_cny if row.charged_cny is not None else row.reservation_cny),
        "price_basis": {key: row.price_basis.get(key) for key in (
            "cluster_goal_id", "cluster_run_id", "integration_run_id", "scope", "verification_scope")},
        "provider_request_id_from_provider": row.provider_request_id,
        "usage": row.usage,
        "request_sha256": sha256(json.dumps(request_body, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest(),
        "response_sha256": None if response_body is None else sha256(
            json.dumps(response_body, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest(),
        "error_present": row.error is not None,
    }


def goal_cost_cny(engine, goal_ids, turn_ids):
    with Session(engine) as session:
        rows = scoped_provider_rows(session, goal_ids, turn_ids)
        return sum((row.charged_cny if row.charged_cny is not None else row.reservation_cny
                    for row in rows), Decimal(0)), rows


def business_counts(engine, owner_id):
    with Session(engine) as session:
        conversation_ids = select(CustomerConversation.id).where(CustomerConversation.owner_id == owner_id)
        return {
            "customer_conversations": session.scalar(select(func.count()).select_from(CustomerConversation)
                                                      .where(CustomerConversation.owner_id == owner_id)),
            "customer_turns": session.scalar(select(func.count()).select_from(CustomerTurn)
                                             .where(CustomerTurn.conversation_id.in_(conversation_ids))),
            "customer_tickets": session.scalar(select(func.count()).select_from(CustomerTicket)
                                               .where(CustomerTicket.owner_id == owner_id)),
            "harness_tasks": session.scalar(select(func.count()).select_from(HarnessTask)
                                            .where(HarnessTask.owner_id == owner_id)),
        }


def read_password_file(inputs, account_name):
    return fixed_account(inputs, account_name)


def api_request(client, report, method, path, body=None, expected=200):
    response = client.request(method, path, json=body)
    report["http_requests"].append({"method": method, "path": path,
                                    "status_code": response.status_code})
    require(response.status_code == expected,
            f"HTTP {method} {path}返回{response.status_code}，期望{expected}")
    return response.json() if response.content else {}


def login_customer(inputs, report):
    customer = read_password_file(inputs, "customer")
    client = httpx.Client(base_url=BASE_URL, trust_env=False, timeout=30)
    value = api_request(client, report, "POST", "/api/login",
                        {"username": customer["username"], "password": customer["password"]})
    require(value.get("id") == customer["actor_id"], "登录身份与固定customer actor_id不一致")
    return customer, client


def current_runs(cluster, goal_id, actor_id):
    return cluster.store.list_runs(goal_id, actor_id)


def current_goal_usage(engine, goal_ids):
    with Session(engine) as session:
        rows = session.scalars(select(ProviderRequest).order_by(ProviderRequest.created_at, ProviderRequest.id)).all()
        selected = [row for row in rows if row.price_basis.get("cluster_goal_id") in goal_ids]
        return {
            "provider_request_count": len(selected),
            "provider_request_ids": [row.id for row in selected],
            "amount_cny": str(sum((row.charged_cny if row.charged_cny is not None else row.reservation_cny
                                    for row in selected), Decimal(0))),
            "unknown_or_open_reservation_cny": str(sum((row.reservation_cny for row in selected
                if row.charged_cny is None or row.status == "unknown"), Decimal(0))),
        }


def profile_report(profile):
    return profile.model_dump(mode="json")


async def wait_for_scheduler(cluster, timeout_seconds=15):
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        if cluster.runtime.is_leader and cluster.runtime.scheduler.running:
            return
        await asyncio.sleep(0.1)
    raise TimeoutError("独立ClusterRuntime未取得PostgreSQL scheduler lease")


async def wait_for_run_count(cluster, goal_id, actor_id, count, timeout_seconds):
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        runs = current_runs(cluster, goal_id, actor_id)
        if len(runs) >= count:
            return runs
        await asyncio.sleep(0.1)
    raise TimeoutError(f"日程在限定时间内只生成{len(current_runs(cluster, goal_id, actor_id))}/{count}个运行")


async def wait_for_terminal_runs(cluster, goal_id, actor_id, run_ids, timeout_seconds):
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        runs = current_runs(cluster, goal_id, actor_id)
        selected = [run for run in runs if run.run_id in run_ids]
        if len(selected) == len(run_ids) and all(run.status in {"completed", "failed", "unknown", "cancelled", "budget_exhausted"}
                                                 for run in selected):
            return selected
        await asyncio.sleep(0.2)
    raise TimeoutError("独立日程运行未在限定时间内结束")


def completed_business_call_ids(store, run_ids, actor_id):
    calls = []
    for run_id in run_ids:
        tasks = store.tasks(run_id, actor_id)
        require(len(tasks) == 1 and tasks[0]["spec"]["tool_name"] == "shop_customer_agent"
                and tasks[0]["spec"]["action"] == "orders", "周期GoalRun没有执行唯一orders业务任务")
        require(tasks[0]["status"] == "completed", "周期orders业务任务没有完成")
        data = tasks[0]["result"]["data"]
        require(bool(data["orders"]), "周期orders业务结果为空")
        calls.append({"run_id": run_id, "task_id": tasks[0]["task_id"],
                      "request_id": tasks[0]["request_id"], "order_count": len(data["orders"])})
    return calls


def schedule_event_records(engine, goal_id, run_ids):
    with Session(engine) as session:
        return session.scalars(select(EventRecord).where(
            EventRecord.goal_id == goal_id, EventRecord.run_id.in_(run_ids)).order_by(EventRecord.run_id)).all()


def replay_schedule_events(cluster, engine, goal_id, run_ids):
    events = schedule_event_records(engine, goal_id, run_ids)
    require(len(events) == len(run_ids), "日程运行与持久化事件数量不一致")
    before = {
        "run_count": len(cluster.store.list_runs(goal_id, events[0].document["actor_id"])),
        "event_count": len(events),
    }
    results = []
    for event_record in events:
        replay = TriggerEvent.model_validate(event_record.document)
        prior = cluster.store.trigger(replay)
        require(prior.run_id == event_record.run_id, "重复日程事件没有复用原GoalRun")
        results.append({"event_id": replay.event_id, "run_id": prior.run_id,
                        "scheduled_at": replay.scheduled_at.isoformat() if replay.scheduled_at else None})
    after = {
        "run_count": len(cluster.store.list_runs(goal_id, events[0].document["actor_id"])),
        "event_count": len(schedule_event_records(engine, goal_id, run_ids)),
    }
    require(before == after, "重复日程事件增加了GoalRun或EventRecord")
    return {"before": before, "after": after, "replayed_events": results}


def reuse_schedule_checks(args, report, cluster, engine, actor_id):
    prior_path = args.schedule_evidence.resolve()
    require(prior_path.is_relative_to(ROOT / ".scratch/evidence"), "日程证据必须来自项目证据目录")
    prior = load_json(prior_path)
    require(prior["fixed_inputs"]["sha256"] == file_sha256(args.inputs)
            and prior["cluster_config"]["sha256"] == file_sha256(args.cluster_config)
            and prior["live_profile"] == profile_report(cluster.configuration.profile),
            "日程证据输入或完整模型配置不一致")
    saved_sources = load_json(HISTORICAL_DIR / "state.json")["source_sha256"]
    saved_sources.update({path: item["current"] for path, item in
                          prior["historical_longflow"]["source_hash_mismatch_files"].items()})
    current_sources = execution_manifest()
    changes = {path: {"saved": digest, "current": current_sources.get(path)}
               for path, digest in saved_sources.items() if current_sources.get(path) != digest}
    require(set(changes) <= {"src/shop/cluster.py"}, "日程证据存在未覆盖的源码变化")
    schedule = prior["live_schedule"]
    goal_id = schedule["goal_id"]
    run_ids = schedule["run_ids"]
    require(len(set(run_ids)) == 3 and schedule["disabled_after_three_events"]
            and schedule["idle_wait"]["before"] == schedule["idle_wait"]["after"]
            and schedule["duplicate_event_replay"]["before"]
                == schedule["duplicate_event_replay"]["after"], "原日程检查没有完整通过")
    goal = cluster.store.get_goal(goal_id, actor_id)
    require(goal.model_dump(mode="json") == schedule["goal_definition"], "日程目标定义已经变化")
    runs = cluster.store.list_runs(goal_id, actor_id)
    require({run.run_id for run in runs} == set(run_ids), "日程运行数量已经变化")
    saved_runs = {run["run_id"]: run for run in schedule["runs"]}
    saved_tasks = {item["run_id"]: item["items"] for item in schedule["tasks"]}
    for run in runs:
        expected = saved_runs[run.run_id]
        require(run.status == "completed" and all(
            run.model_dump(mode="json")[key] == expected[key]
            for key in ("decision_count", "trigger_id", "plan_version", "error")),
            "日程运行状态或决策记录已经变化")
        require(Decimal(expected["spent_cny"]) == run.spent_cny
                and Decimal(expected["reserved_cny"]) == run.reserved_cny, "日程运行费用已经变化")
        tasks = cluster.store.tasks(run.run_id, actor_id)
        observed = [{"task_id": task["task_id"], "request_id": task["request_id"],
                     "status": task["status"], "error": task["error"],
                     "tool_name": task["spec"]["tool_name"], "action": task["spec"]["action"],
                     "payload": task["spec"]["payload"]} for task in tasks]
        require(observed == saved_tasks[run.run_id], "日程业务任务或request_id已经变化")
    require(completed_business_call_ids(cluster.store, run_ids, actor_id) == schedule["business_calls"],
            "日程真实业务结果已经变化")
    events = schedule_event_records(engine, goal_id, run_ids)
    observed_events = [{"event_id": event.event_id, "run_id": event.run_id,
                        "scheduled_at": event.document["scheduled_at"],
                        "coalesced_periods": event.document["payload"].get("coalesced_periods")}
                       for event in events]
    require(observed_events == schedule["scheduled_events"]
            and all(event.document["kind"] == "schedule" for event in events), "日程持久事件已经变化")
    current_schedule = next(item for item in cluster.store.schedules(actor_id)
                            if item.schedule_id == schedule["schedule_id"])
    require(not current_schedule.enabled, "复用的日程仍然启用")
    require(current_schedule.model_dump(mode="json") == {
        **schedule["schedule_definition"], "enabled": False}, "日程定义已经变化")
    usage = current_goal_usage(engine, {goal_id})
    require(usage == schedule["provider_requests"], "日程模型请求或费用已经变化")
    with Session(engine) as session:
        conversations = session.scalars(select(CustomerConversation).where(
            CustomerConversation.owner_id == actor_id,
            CustomerConversation.title == "CLU-05已发送客服任务取消检查",
            CustomerConversation.created_at >= datetime.fromisoformat(prior["started_at"]),
            CustomerConversation.created_at <= datetime.fromisoformat(prior["finished_at"]))).all()
        require(len(conversations) == 1, "原取消检查会话没有唯一匹配记录")
        conversation = conversations[0]
        require(session.scalar(select(func.count()).select_from(CustomerTurn).where(
            CustomerTurn.conversation_id == conversation.id)) == 0
            and session.scalar(select(func.count()).select_from(CustomerTicket).where(
                CustomerTicket.conversation_id == conversation.id)) == 0, "原取消检查会话已经包含业务记录")
        conversation_id = conversation.id
    report["live_schedule"] = schedule
    report["schedule_evidence_reuse"] = {
        "report": path_info(prior_path), "original_overall_status": prior["status"],
        "reused_section": "live_schedule", "database_recheck_passed": True,
        "source_changes": changes, "covering_checks": [
            "tests/test_cluster_goal_authorization.py", "final-release-http-cli"],
        "source_change_scope": "目标授权检查在MCP连接关闭后执行；日程存储、触发及执行源码保持",
        "existing_empty_conversation_id": conversation_id,
        "additional_schedule_model_requests": 0,
    }
    return goal_id, current_schedule, usage, conversation_id


async def run_schedule_checks(report, cluster, database_engine, client, actor_id, phase_one_counts):
    schedule_goal = api_request(client, report, "POST", "/api/cluster/goals", {
        "objective": "每个周期读取本人最近订单并保留真实结果；每次触发只执行一次只读orders查询，不创建会话、工单或其他业务记录。",
        "allowed_operations": {"shop_customer_agent": ["orders"]},
        "criteria": [{"tool_name": "shop_customer_agent", "action": "orders",
                      "path": ["data", "orders"], "operator": "exists"}],
        "budget_cny": str(SCHEDULE_BUDGET_CNY), "max_decisions": 6, "max_tasks": 1,
    })
    schedule_goal_id = schedule_goal["goal_id"]
    require(schedule_goal["actor_id"] == actor_id, "HTTP创建的周期goal身份不匹配")
    schedule = api_request(client, report, "POST", "/api/cluster/schedules", {
        "goal_id": schedule_goal_id, "interval_seconds": SCHEDULE_INTERVAL_SECONDS,
        "timezone": "Asia/Shanghai", "enabled": True,
    })
    schedule_id = schedule["schedule_id"]
    listed_schedules = api_request(client, report, "GET", "/api/cluster/schedules")
    require(any(item["schedule_id"] == schedule_id and item["enabled"] for item in listed_schedules),
            "HTTP日程列表没有返回已启用测试日程")
    report["live_schedule"].update({"goal_id": schedule_goal_id, "schedule_id": schedule_id,
                                    "goal_definition": schedule_goal,
                                    "schedule_definition": schedule, "http_create": True})
    idle_before = current_goal_usage(database_engine, {schedule_goal_id})
    await asyncio.sleep(IDLE_WAIT_SECONDS)
    idle_after = current_goal_usage(database_engine, {schedule_goal_id})
    require(idle_before == idle_after, "没有到期日程时新增了周期Goal模型请求")
    report["live_schedule"]["idle_wait"] = {
        "before": idle_before, "after": idle_after, "no_model_request_added": True}
    runs = await wait_for_run_count(cluster, schedule_goal_id, actor_id, 3, RUN_WAIT_SECONDS)
    run_ids = [run.run_id for run in runs[:3]]
    require(len(set(run_ids)) == 3, "三次日程触发没有生成三个独立GoalRun")
    report["live_schedule"]["run_ids"] = run_ids
    disabled_schedule = api_request(client, report, "PUT", f"/api/cluster/schedules/{schedule_id}",
                                    {**schedule, "enabled": False})
    require(disabled_schedule["enabled"] is False, "HTTP没有停用专用周期日程")
    report["live_schedule"]["disabled_after_three_events"] = True
    terminal_runs = await wait_for_terminal_runs(cluster, schedule_goal_id, actor_id, set(run_ids), RUN_WAIT_SECONDS)
    schedule_usage = current_goal_usage(database_engine, {schedule_goal_id})
    report["live_schedule"].update({
        "runs": [{"run_id": run.run_id, "status": run.status, "error": run.error,
                  "trigger_id": run.trigger_id, "decision_count": run.decision_count,
                  "plan_version": run.plan_version, "spent_cny": str(run.spent_cny),
                  "reserved_cny": str(run.reserved_cny)} for run in terminal_runs],
        "tasks": [{"run_id": run.run_id, "items": [
            {"task_id": task["task_id"], "request_id": task["request_id"],
             "status": task["status"], "error": task["error"],
             "tool_name": task["spec"]["tool_name"], "action": task["spec"]["action"],
             "payload": task["spec"]["payload"]}
            for task in cluster.store.tasks(run.run_id, actor_id)]} for run in terminal_runs],
        "provider_requests": schedule_usage,
    })
    require(len(terminal_runs) == 3 and all(run.status == "completed" for run in terminal_runs),
            "三次独立日程GoalRun没有全部completed")
    business_calls = completed_business_call_ids(cluster.store, run_ids, actor_id)
    deduplication = replay_schedule_events(cluster, database_engine, schedule_goal_id, run_ids)
    counts_after_schedule = business_counts(database_engine, actor_id)
    require(phase_one_counts == counts_after_schedule, "只读周期运行改变了业务记录数量")
    require(Decimal(schedule_usage["amount_cny"]) <= SCHEDULE_BUDGET_CNY, "三轮周期Goal费用超过上限")
    report["live_schedule"].update({
        "business_calls": business_calls, "duplicate_event_replay": deduplication,
        "business_counts_before": phase_one_counts, "business_counts_after": counts_after_schedule,
        "read_only_business_counts_unchanged": True, "provider_requests": schedule_usage,
        "mcp_stdio_goal_validation": True,
        "checks": ["one_http_created_schedule", "three_independent_goal_runs",
                   "real_apscheduler_due_times", "mcp_stdio_goal_validation",
                   "duplicate_schedule_event_reuses_run", "idle_wait_adds_no_model_request",
                   "dedicated_schedule_disabled_after_three_events"],
    })
    schedule_events = schedule_event_records(database_engine, schedule_goal_id, run_ids)
    require(all(event.document["kind"] == "schedule" for event in schedule_events),
            "三轮GoalRun没有由实际schedule事件触发")
    report["live_schedule"]["scheduled_events"] = [
        {"event_id": event.event_id, "run_id": event.run_id,
         "scheduled_at": event.document["scheduled_at"],
         "coalesced_periods": event.document["payload"].get("coalesced_periods")}
        for event in schedule_events]
    report["live_schedule"]["enabled_after_test"] = False
    report["live_schedule"]["database_schedule_enabled"] = False
    return schedule_goal_id, disabled_schedule, schedule_usage


async def run_live_checks(args, report, evidence_dir):
    report["http_requests"] = []
    inputs = load_json(args.inputs)
    customer, client = login_customer(inputs, report)
    actor_id = customer["actor_id"]
    settings = Settings(cluster_config_file=args.cluster_config,
                        cluster_notes_dir=evidence_dir / "cluster-notes")
    runtime_started = False
    schedule_id = None
    schedule_goal_id = None
    cancel_goal_id = None
    cancel_run_id = None
    turn_ids = set()
    database_engine = create_engine(settings.database_url, pool_size=10, max_overflow=10)
    phase_one_counts = business_counts(database_engine, actor_id)
    cluster = ShopCluster(settings, database_engine,
                          sessionmaker(database_engine, expire_on_commit=False))
    profile = cluster.configuration.profile
    cluster.engine.profile = profile
    cluster.engine.provider.profile = profile
    cluster.engine.context = ContextManager(cluster.notes, profile, SYSTEM_PROMPT, cluster.model)
    report["live_profile"] = profile_report(profile)
    report["live_schedule"] = {"interval_seconds": SCHEDULE_INTERVAL_SECONDS,
                                "idle_wait_seconds": IDLE_WAIT_SECONDS}
    report["budget"] = {"additional_limit_cny": str(ADDITIONAL_BUDGET_CNY),
                        "schedule_goal_max_cny": str(SCHEDULE_BUDGET_CNY),
                        "customer_goal_max_cny": str(CUSTOMER_BUDGET_CNY)}
    budget_before = ledger_snapshot(database_engine)
    require(budget_before["window_id"] == ACTIVE_WINDOW_ID,
            "项目当前费用窗口与本次检查配置不匹配")
    report["budget"]["before"] = budget_before
    with Session(database_engine) as session:
        runnable = cluster.store.runnable()
        require(not runnable, "啟動独立Runtime前存在其他queued或running目标")
    report["runtime_preflight"] = {
        "root_http_autorun": load_json(args.cluster_config)["autorun"],
        "existing_runnable_goal_runs": 0,
        "active_budget_window": budget_before["window_id"],
    }
    try:
        conversation_id = None
        if args.schedule_evidence is not None:
            schedule_goal_id, reused_schedule, schedule_usage, conversation_id = reuse_schedule_checks(
                args, report, cluster, database_engine, actor_id)
            schedule_id = reused_schedule.schedule_id
            disabled_schedule = reused_schedule.model_dump(mode="json")
            save_json(report["report_path"], report)
        await cluster.runtime.startup()
        runtime_started = True
        await wait_for_scheduler(cluster)
        report["runtime_preflight"].update({"scheduler_leader": cluster.runtime.is_leader,
                                             "apscheduler_running": cluster.runtime.scheduler.running})
        if args.schedule_evidence is None:
            schedule_goal_id, disabled_schedule, schedule_usage = await run_schedule_checks(
                report, cluster, database_engine, client, actor_id, phase_one_counts)
            schedule_id = disabled_schedule["schedule_id"]

        cancellation_budget = min(CUSTOMER_BUDGET_CNY,
                                  ADDITIONAL_BUDGET_CNY - Decimal(schedule_usage["amount_cny"]))
        require(cancellation_budget > Decimal("0.01"), "新增费用上限剩余额度不足以启动Customer submit取消检查")
        customer_baseline = business_counts(database_engine, actor_id)
        conversation_increment = int(conversation_id is None)
        if conversation_id is None:
            conversation = api_request(client, report, "POST", "/api/customer/conversations", {
                "title": "CLU-05已发送客服任务取消检查",
            })
            conversation_id = conversation["id"]
        conversation_counts = business_counts(database_engine, actor_id)
        require(conversation_counts["customer_conversations"] == customer_baseline["customer_conversations"] + conversation_increment
                and conversation_counts["customer_turns"] == customer_baseline["customer_turns"]
                and conversation_counts["customer_tickets"] == customer_baseline["customer_tickets"]
                and conversation_counts["harness_tasks"] == customer_baseline["harness_tasks"],
                "创建客服会话时新增记录数量超出预期")
        request_before = ledger_snapshot(database_engine)
        cancel_goal = api_request(client, report, "POST", "/api/cluster/goals", {
            "objective": f"在客服会话{conversation_id}中提交一条普通商品咨询并保留真实客服turn结果；只提交一次，不申请创建人工工单。",
            "allowed_operations": {"shop_customer_agent": ["submit", "turn"]},
            "criteria": [{"tool_name": "shop_customer_agent", "action": "submit",
                          "path": ["data", "result"], "operator": "exists"}],
            "budget_cny": str(cancellation_budget),
            "max_decisions": 6,
            "max_tasks": 1,
        })
        cancel_goal_id = cancel_goal["goal_id"]
        require(cancel_goal["actor_id"] == actor_id, "HTTP创建的Customer submit goal身份不匹配")
        cancel_event_id = "clu-longflow-independent-customer-submit-" + uuid4().hex
        trigger = api_request(client, report, "POST", f"/api/cluster/goals/{cancel_goal_id}/trigger", {
            "event_id": cancel_event_id,
            "payload": {},
        })
        cancel_run_id = trigger["run_id"]
        report["live_cancel"] = {
            "goal_id": cancel_goal_id,
            "run_id": cancel_run_id,
            "event_id": cancel_event_id,
            "conversation_id": conversation_id,
            "budget_cny": str(cancellation_budget),
            "goal_definition": cancel_goal,
            "http_created_goal": True,
            "conversation_reused": conversation_increment == 0,
            "business_counts_before": customer_baseline,
            "business_counts_after_conversation_create": conversation_counts,
            "provider_request_ledger_before": request_before,
        }
        save_json(report["report_path"], report)
        submit_evidence = None
        deadline = monotonic() + SENT_WAIT_SECONDS
        while monotonic() < deadline:
            with Session(database_engine) as session:
                run_row = session.get(RunRecord, cancel_run_id)
                tasks = session.scalars(select(TaskRecord).where(TaskRecord.run_id == cancel_run_id)).all()
                task = tasks[0] if tasks else None
                calls = [] if task is None else session.scalars(select(IntegrationCall).where(
                    IntegrationCall.actor_id == actor_id,
                    IntegrationCall.request_id == task.request_id,
                    IntegrationCall.operation == "submit",
                )).all()
                turns = [] if not calls else session.scalars(select(CustomerTurn).where(
                    CustomerTurn.id.in_([call.resource_id for call in calls]))).all()
                sent_requests = [] if not turns else session.scalars(select(ProviderRequest).where(
                    ProviderRequest.run_id.in_([turn.id for turn in turns]),
                    ProviderRequest.status == "sent",
                ).order_by(ProviderRequest.created_at)).all()
                if (run_row is not None and run_row.status == "running" and task is not None
                        and task.status in {"sent", "running"} and calls and turns and sent_requests
                        and any(turn.status == "running" for turn in turns)):
                    turn = next(turn for turn in turns if turn.status == "running")
                    provider_request = next(row for row in sent_requests if row.run_id == turn.id)
                    submit_evidence = {
                        "task_id": task.task_id,
                        "task_request_id": task.request_id,
                        "task_status_before_cancel": task.status,
                        "integration_call": {
                            "module_id": calls[0].module_id,
                            "operation": calls[0].operation,
                            "request_id": calls[0].request_id,
                            "resource_type": calls[0].resource_type,
                            "resource_id": calls[0].resource_id,
                        },
                        "customer_turn": {"id": turn.id, "request_id": turn.request_id,
                                          "status": turn.status, "model_calls": turn.model_calls,
                                          "cancel_requested": turn.cancel_requested},
                        "provider_request": provider_row_json(provider_request),
                    }
                    turn_ids.add(turn.id)
                    break
                if ((run_row is not None and run_row.status in TERMINAL_RUN_STATUSES)
                        or (task is not None and task.status in TERMINAL_TASK_STATUSES)):
                    report["live_cancel"]["terminal_before_sent_observation"] = {
                        "run_status": None if run_row is None else run_row.status,
                        "task_status": None if task is None else task.status,
                    }
                    save_json(report["report_path"], report)
                    break
            await asyncio.sleep(0.05)
        require(submit_evidence is not None,
                "未能在Customer submit预算窗口内观察到数据库sent ProviderRequest与running customer turn")
        report["live_cancel"]["mcp_stdio_goal_validation"] = True
        report["live_cancel"]["sent_task_before_cancel"] = submit_evidence
        save_json(report["report_path"], report)
        cancel_response = api_request(client, report, "POST",
                                      f"/api/cluster/goals/{cancel_goal_id}/control/cancel")
        require(cancel_response["state"] == "cancelled", "HTTP取消没有把Customer submit目标标记为cancelled")
        report["live_cancel"]["cancel_http_response"] = cancel_response
        report["live_cancel"]["cancelled_after_sent_request"] = True
        cancel_deadline = monotonic() + RUN_WAIT_SECONDS
        final_task = None
        final_turn = None
        final_provider_rows = []
        while monotonic() < cancel_deadline:
            with Session(database_engine) as session:
                run_row = session.get(RunRecord, cancel_run_id)
                task = session.get(TaskRecord, (cancel_run_id, submit_evidence["task_id"]))
                turn = session.get(CustomerTurn, submit_evidence["customer_turn"]["id"])
                provider_rows = session.scalars(select(ProviderRequest).where(
                    ProviderRequest.run_id == turn.id).order_by(ProviderRequest.created_at, ProviderRequest.id)).all()
                if (task is not None and task.status in TERMINAL_TASK_STATUSES
                        and turn is not None and turn.status in {"completed", "failed", "cancelled", "unknown"}):
                    final_task = {"task_id": task.task_id, "request_id": task.request_id,
                                  "status": task.status, "result": task.result, "error": task.error,
                                  "run_status": None if run_row is None else run_row.status}
                    final_turn = {"id": turn.id, "request_id": turn.request_id, "status": turn.status,
                                  "model_calls": turn.model_calls, "tool_calls": turn.tool_calls,
                                  "cancel_requested": turn.cancel_requested,
                                  "error_present": turn.error is not None}
                    final_provider_rows = [provider_row_json(row) for row in provider_rows]
                    break
            await asyncio.sleep(0.2)
        require(final_task is not None and final_turn is not None,
                "取消后Customer submit任务或business turn未到达最终状态")
        require(final_task["request_id"] == submit_evidence["task_request_id"]
                == final_turn["request_id"], "取消后business request_id没有保持一致")
        require(final_task["status"] in {"completed", "unknown"},
                f"已发送Customer submit任务最终状态为{final_task['status']}，不符合completed或unknown要求")
        require(final_provider_rows and any(row["provider_request_id"] ==
            submit_evidence["provider_request"]["provider_request_id"] for row in final_provider_rows),
            "已发送ProviderRequest在取消后未保留")
        customer_counts_after = business_counts(database_engine, actor_id)
        require(customer_counts_after["customer_conversations"] == customer_baseline["customer_conversations"] + conversation_increment,
                "Customer submit没有只新增预期客服会话")
        require(customer_counts_after["customer_turns"] == customer_baseline["customer_turns"] + 1,
                "Customer submit没有只新增一个真实business turn")
        require(customer_counts_after["customer_tickets"] == customer_baseline["customer_tickets"],
                "Customer submit检查产生了人工工单")
        all_goal_ids = {schedule_goal_id, cancel_goal_id}
        total_test_cost, all_test_rows = goal_cost_cny(database_engine, all_goal_ids, turn_ids)
        require(total_test_cost <= ADDITIONAL_BUDGET_CNY,
                "本次检查的模型费用与unknown预留超过0.50元")
        budget_after = ledger_snapshot(database_engine)
        baseline_window = Decimal(budget_before["window_consumed_cny"])
        final_window = Decimal(budget_after["window_consumed_cny"])
        require(final_window - baseline_window <= ADDITIONAL_BUDGET_CNY,
                "本次期间费用窗口新增总额超过0.50元")
        report["live_cancel"].update({
            "final_task": final_task,
            "final_customer_turn": final_turn,
            "provider_requests_after_cancel": final_provider_rows,
            "original_provider_request_preserved": True,
            "request_id_preserved": True,
            "business_counts_after": customer_counts_after,
            "ticket_count_unchanged": True,
            "accepted_final_status": final_task["status"] in {"completed", "unknown"},
            "checks": ["customer_turn_running_before_cancel", "provider_request_status_sent_before_cancel",
                       "goal_cancelled_over_http", "final_task_completed_or_unknown",
                       "original_provider_request_and_request_id_preserved"],
        })
        report["budget"].update({
            "after": budget_after,
            "test_goal_ids": sorted(all_goal_ids),
            "customer_turn_ids": sorted(turn_ids),
            "test_provider_requests": [provider_row_json(row) for row in all_test_rows],
            "test_scoped_cost_and_open_reservations_cny": str(total_test_cost),
            "window_delta_cny": str(final_window - baseline_window),
            "within_additional_0_50_cny": True,
        })
        require(disabled_schedule["enabled"] is False, "专用周期日程最终没有停用")
        report["live_schedule"]["disabled_after_three_events"] = True
        report["live_cancel"]["conversation_message"] = final_task["result"]["data"]["message"]
        api_request(client, report, "POST", "/api/logout")
    finally:
        try:
            if cancel_goal_id is not None and cancel_run_id is not None:
                cancel_runs = cluster.store.list_runs(cancel_goal_id, actor_id)
                active_run_ids = {run.run_id for run in cancel_runs
                                  if run.status not in TERMINAL_RUN_STATUSES}
                if active_run_ids:
                    cleanup_cancel = api_request(
                        client, report, "POST",
                        f"/api/cluster/goals/{cancel_goal_id}/control/cancel")
                    require(cleanup_cancel["state"] == "cancelled",
                            "清理请求没有取消未结束的Customer submit目标")
                    await wait_for_terminal_runs(
                        cluster, cancel_goal_id, actor_id, active_run_ids, RUN_WAIT_SECONDS)
                    report["live_cleanup"] = {
                        "cancelled_goal_id": cancel_goal_id,
                        "run_ids": sorted(active_run_ids),
                        "goal_cancelled_over_http": True,
                        "runs_reached_terminal_status": True,
                    }
        finally:
            try:
                if schedule_id is not None and schedule_goal_id is not None:
                    with httpx.Client(base_url=BASE_URL, trust_env=False, timeout=30) as cleanup_client:
                        cleanup_customer = read_password_file(inputs, "customer")
                        cleanup_response = cleanup_client.post("/api/login", json={
                            "username": cleanup_customer["username"],
                            "password": cleanup_customer["password"],
                        })
                        cleanup_response.raise_for_status()
                        cleanup_schedule = cluster.store.schedules(actor_id)
                        current = next((item for item in cleanup_schedule
                                        if item.schedule_id == schedule_id), None)
                        require(current is not None, "清理时找不到专用周期日程")
                        if current.enabled:
                            cleanup_update = cleanup_client.put(
                                f"/api/cluster/schedules/{schedule_id}",
                                json={**current.model_dump(mode="json"), "enabled": False},
                            )
                            cleanup_update.raise_for_status()
                            require(cleanup_update.json()["enabled"] is False,
                                    "清理请求没有停用专用周期日程")
            finally:
                try:
                    if runtime_started:
                        await cluster.runtime.shutdown()
                finally:
                    try:
                        database_engine.dispose()
                    finally:
                        client.close()


def audit_cancel_result(args, report, engine):
    if args.cancel_evidence is not None:
        prior_path = args.cancel_evidence.resolve()
        require(prior_path.is_relative_to(ROOT / ".scratch/evidence"), "取消证据必须来自项目证据目录")
        prior = load_json(prior_path)
        require(prior["status"] == "passed"
                and prior["fixed_inputs"]["sha256"] == file_sha256(args.inputs)
                and prior["cluster_config"]["sha256"] == file_sha256(args.cluster_config),
                "取消证据状态、输入或配置不符合要求")
        saved_sources = load_json(HISTORICAL_DIR / "state.json")["source_sha256"]
        saved_sources.update({path: item["current"] for path, item in
                              prior["historical_longflow"]["source_hash_mismatch_files"].items()})
        current_sources = execution_manifest()
        require(all(current_sources.get(path) == digest for path, digest in saved_sources.items()),
                "取消证据的执行源码已经变化")
        additional_sources = {path: digest for path, digest in current_sources.items() if path not in saved_sources}
        require(all(path.startswith("agent-hub/") or path in {
            "agent-cluster/pyproject.toml", "agent-cluster/uv.lock"} for path in additional_sources),
            "取消证据存在未登记的执行源码")
        for key in ("live_schedule", "live_cancel", "budget", "schedule_evidence_reuse", "live_profile"):
            report[key] = prior[key]
        report["cancel_evidence_reuse"] = {
            "report": path_info(prior_path), "paid_requests_added": 0,
            "matching_saved_source_count": len(saved_sources),
            "additional_current_sources": additional_sources,
            "additional_source_covering_checks": ["final-release-hub-stdio", "final-release-independent-install"],
        }
    cancel = report["live_cancel"]
    actor_id = load_json(args.inputs)["accounts"]["customer"]["actor_id"]
    before = ledger_snapshot(engine)
    with Session(engine) as session:
        goal = session.get(GoalRecord, cancel["goal_id"])
        run = session.get(RunRecord, cancel["run_id"])
        require(goal.actor_id == run.actor_id == actor_id
                and goal.state == run.status == "cancelled", "取消后目标或运行没有保持cancelled")
        tasks = session.scalars(select(TaskRecord).where(TaskRecord.run_id == run.run_id)).all()
        require(len(tasks) == 1, "取消后业务任务集合发生变化")
        task = tasks[0]
        expected = cancel["final_task"]
        require(task.task_id == expected["task_id"] and task.request_id == expected["request_id"]
                and task.status == expected["status"] and task.result == expected["result"]
                and task.error == expected["error"], "取消任务真实结果与原报告不一致")
        turn = session.get(CustomerTurn, cancel["final_customer_turn"]["id"])
        require(turn.conversation_id == cancel["conversation_id"]
                and turn.request_id == task.request_id
                and turn.status == cancel["final_customer_turn"]["status"], "取消业务turn记录不一致")
        sent_id = cancel["sent_task_before_cancel"]["provider_request"]["provider_request_id"]
        original = session.get(ProviderRequest, sent_id)
        require(original.run_id == turn.id, "取消前sent请求不属于当前业务turn")
        rows = scoped_provider_rows(session, {goal.goal_id}, {turn.id})
        require(all(row.created_at <= original.created_at for row in rows),
                "取消前已观察的sent业务请求之后新增了模型请求")
        require([provider_row_json(row) for row in sorted(
            [row for row in rows if row.run_id == turn.id], key=lambda row: (row.created_at, row.id))]
                == cancel["provider_requests_after_cancel"], "取消业务模型结果与原报告不一致")
        decisions = session.scalars(select(DecisionRecord).where(DecisionRecord.run_id == run.run_id)).all()
        require(run.document["decision_count"] == len(decisions) == 1
                and run.document["plan_version"] == 1, "取消流程产生了额外协调决策或计划")
        schedule = report["live_schedule"]
        store = ClusterStore(engine)
        require(current_goal_usage(engine, {schedule["goal_id"]}) == schedule["provider_requests"]
                and completed_business_call_ids(store, schedule["run_ids"], actor_id)
                    == schedule["business_calls"], "复用的日程模型或业务记录已经变化")
        require(not next(item for item in store.schedules(actor_id)
                         if item.schedule_id == schedule["schedule_id"]).enabled, "复用日程已经重新启用")
        cancel["conversation_message"] = turn.message
        cancel["post_cancel_database_audit"] = {
            "goal_state": goal.state, "run_status": run.status,
            "task_count": len(tasks), "decision_count": len(decisions),
            "plan_version": run.document["plan_version"],
            "provider_request_ids": sorted(row.id for row in rows),
            "sent_request_created_at": original.created_at.isoformat(),
            "provider_request_created_at": {row.id: row.created_at.isoformat() for row in rows},
            "no_request_after_observed_sent_request": True,
        }
    require(before == ledger_snapshot(engine), "只读取消复核改变了费用账目")
    report["cancel_readonly_audit"] = {"status": "passed", "ledger_unchanged": True,
                                      "before": before, "after": ledger_snapshot(engine)}


def main():
    parser = argparse.ArgumentParser(description="核查CLU长流程证据、真实三轮日程与已发送客服任务取消")
    parser.add_argument("--inputs", type=Path, default=FIXED_INPUTS)
    parser.add_argument("--cluster-config", type=Path, default=CLUSTER_CONFIG)
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--offline-only", action="store_true")
    parser.add_argument("--schedule-evidence", type=Path)
    parser.add_argument("--cancel-evidence", type=Path)
    args = parser.parse_args()
    require(args.base_url == BASE_URL, "此次本地验收只允许连接项目固定HTTP地址")
    require(args.inputs.is_file() and args.cluster_config.is_file(), "固定输入或MCP配置文件不存在")
    evidence_dir = build_evidence_directory(args.directory)
    report_path = evidence_dir / "report.json"
    report = {
        "run_id": evidence_dir.name,
        "milestone": ["CLU-03", "CLU-05"],
        "status": "running",
        "started_at": datetime.now(UTC).isoformat(),
        "report_path": str(report_path),
        "command": sys.argv,
        "validator_sha256": file_sha256(__file__),
        "execution_source_sha256_before": execution_manifest(),
        "fixed_inputs": {
            "path": str(args.inputs.resolve().relative_to(ROOT)),
            "sha256": file_sha256(args.inputs),
        },
        "cluster_config": {
            "path": str(args.cluster_config.resolve().relative_to(ROOT)),
            "sha256": file_sha256(args.cluster_config),
            "autorun": load_json(args.cluster_config)["autorun"],
        },
        "environment": {
            "python": sys.version,
            "sqlalchemy": version("SQLAlchemy"),
            "httpx": version("httpx"),
            "apscheduler": version("APScheduler"),
        },
    }
    save_json(report_path, report)
    engine = create_engine(Settings().database_url, pool_size=10, max_overflow=10)
    try:
        historical_audit(report, engine)
        report["offline_audit_status"] = "passed"
        save_json(report_path, report)
        if args.offline_only:
            report["status"] = "offline_audit_complete_live_checks_pending"
            report["finished_at"] = datetime.now(UTC).isoformat()
            save_json(report_path, report)
            print(json.dumps({"run_id": report["run_id"], "status": report["status"],
                              "report": str(report_path.relative_to(ROOT))}, ensure_ascii=False))
            return
    except Exception as error:
        report["status"] = "failed"
        report["error"] = {"type": type(error).__name__, "detail": str(error)}
        raise
    finally:
        engine.dispose()
        save_json(report_path, report)
    try:
        if args.cancel_evidence is None:
            asyncio.run(run_live_checks(args, report, evidence_dir))
        engine = create_engine(Settings().database_url, pool_size=10, max_overflow=10)
        try:
            audit_cancel_result(args, report, engine)
        finally:
            engine.dispose()
        report["execution_source_sha256_after"] = execution_manifest()
        require(report["execution_source_sha256_before"] == report["execution_source_sha256_after"],
                "验收期间执行源码发生变化")
        report["status"] = "passed"
        report["finished_at"] = datetime.now(UTC).isoformat()
        save_json(report_path, report)
        print(json.dumps({"run_id": report["run_id"], "status": report["status"],
                          "report": str(report_path.relative_to(ROOT)),
                          "test_cost_and_reservations_cny": report["budget"]["test_scoped_cost_and_open_reservations_cny"]},
                         ensure_ascii=False))
    except Exception as error:
        report["status"] = "failed"
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["error"] = {"type": type(error).__name__, "detail": str(error)}
        raise
    finally:
        save_json(report_path, report)


if __name__ == "__main__":
    main()
