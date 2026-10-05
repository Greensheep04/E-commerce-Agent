import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from agent_cluster.engine import SYSTEM_PROMPT, coordinator_tools
from agent_cluster.providers import build_body, input_limit_for_request
from agent_cluster.store import ClusterStore, DecisionRecord, EventRecord, GoalRecord, RunRecord
from agent_cluster.types import (
    CompletionCriterion,
    CoordinatorProfile,
    GoalDefinition,
    TriggerEvent,
)
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from shop.budget import BudgetService, rate_card, reserved_cost
from shop.config import Settings
from shop.integration_models import IntegrationBudgetWindow
from shop.paid_models import PaidModels
from shop.runtime_models import BudgetAccount, ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_ROOT = ROOT / ".scratch/evidence"
FIXED_INPUTS = EVIDENCE_ROOT / "clu-inputs-20261002T164037Z-5958ff50/inputs.json"
CLUSTER_CONFIG = ROOT / ".scratch/cluster-http-config.json"
ACTIVE_WINDOW_ID = "clu-live-20261003"
MAX_ADDITIONAL_CNY = Decimal("0.15")
STEP_KEY = "unsent-resume-0001"
PAID_MODELS_RED_SHA256 = "a4de72b103f3314d120a3affa31a816235a5d93a2e2b815cd70e4f6d473d7088"
SOURCE_FILES = (
    Path(__file__).resolve(),
    ROOT / "agent-cluster/src/agent_cluster/providers.py",
    ROOT / "agent-cluster/src/agent_cluster/store.py",
    ROOT / "agent-cluster/src/agent_cluster/types.py",
    ROOT / "src/shop/budget.py",
    ROOT / "src/shop/cluster.py",
    ROOT / "src/shop/paid_models.py",
    ROOT / "src/shop/runtime_models.py",
)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def digest_bytes(value):
    return sha256(value).hexdigest()


def digest_file(path):
    return digest_bytes(Path(path).read_bytes())


def digest_json(value):
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                         default=str).encode("utf-8")
    return digest_bytes(encoded)


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n",
                          encoding="utf-8")


def timestamp():
    return datetime.now(UTC).isoformat()


def source_hashes():
    return {str(path.relative_to(ROOT)): digest_file(path) for path in SOURCE_FILES}


def database_identity(database_url):
    url = make_url(database_url)
    return {"driver": url.drivername, "host": url.host, "port": url.port, "database": url.database}


def unknown_rows(session):
    rows = session.scalars(select(ProviderRequest).where(
        ProviderRequest.status == "unknown").order_by(ProviderRequest.id)).all()
    return [{
        "id": row.id,
        "run_id": row.run_id,
        "step_key": row.step_key,
        "status": row.status,
        "reservation_cny": str(row.reservation_cny),
        "charged_cny": None if row.charged_cny is None else str(row.charged_cny),
        "error": row.error,
        "request_body_sha256": digest_json(row.request_body),
        "price_basis_sha256": digest_json(row.price_basis),
    } for row in rows]


def ledger_snapshot(engine):
    with Session(engine) as session:
        account = session.get(BudgetAccount, "project")
        window = session.get(IntegrationBudgetWindow, ACTIVE_WINDOW_ID)
        require(account is not None, "项目费用账户不存在")
        require(window is not None and window.active, "指定费用窗口不存在或未启用")
        rows = session.scalars(select(ProviderRequest)).all()
        window_rows = [row for row in rows
                       if row.price_basis.get("integration_run_id") == window.id]
        window_total = sum((row.charged_cny if row.charged_cny is not None
                            else row.reservation_cny for row in window_rows), Decimal(0))
        return {
            "window_id": window.id,
            "window_limit_cny": str(window.limit_cny),
            "window_total_cny": str(window_total),
            "window_provider_request_count": len(window_rows),
            "window_open_reservation_count": sum(row.charged_cny is None for row in window_rows),
            "project_spent_cny": str(account.spent_cny),
            "project_reserved_cny": str(account.reserved_cny),
            "unknown_requests": unknown_rows(session),
        }


def unknown_preservation(before, after):
    after_by_id = {row["id"]: row for row in after}
    missing = sorted(row["id"] for row in before if row["id"] not in after_by_id)
    changed = sorted(row["id"] for row in before
                     if row["id"] in after_by_id and after_by_id[row["id"]] != row)
    return {"preserved": not missing and not changed,
            "missing_ids": missing, "changed_ids": changed,
            "new_unknown_ids": sorted(set(after_by_id) - {row["id"] for row in before})}


def request_snapshot(engine, run_id, step_key):
    with Session(engine) as session:
        rows = session.scalars(select(ProviderRequest).where(
            ProviderRequest.run_id == run_id,
            ProviderRequest.step_key == step_key).order_by(ProviderRequest.id)).all()
        return [{
            "id": row.id,
            "status": row.status,
            "service": row.service,
            "model": row.model,
            "reservation_cny": str(row.reservation_cny),
            "charged_cny": None if row.charged_cny is None else str(row.charged_cny),
            "error": row.error,
            "provider_request_id": row.provider_request_id,
            "request_body_sha256": digest_json(row.request_body),
            "response_body_sha256": None if row.response_body is None else digest_json(row.response_body),
            "usage": row.usage,
            "price_basis": row.price_basis,
        } for row in rows]


def state_snapshot(engine, goal_id, run_id, step_key, event_id):
    with Session(engine) as session:
        goal = session.get(GoalRecord, goal_id)
        run = session.get(RunRecord, run_id)
        decision = session.get(DecisionRecord, (run_id, step_key))
        event = session.get(EventRecord, (goal_id, event_id))
        return {
            "goal_state": None if goal is None else goal.state,
            "run_status": None if run is None else run.status,
            "decision_exists": decision is not None,
            "decision_request_sha256": None if decision is None else digest_json(decision.request),
            "decision_has_response": None if decision is None else decision.response is not None,
            "event_exists": event is not None,
            "provider_requests": request_snapshot(engine, run_id, step_key),
        }


def append_transition(report, label, before, after):
    report["state_transitions"].append({
        "label": label,
        "recorded_at": timestamp(),
        "before": before,
        "after": after,
    })


def child_environment():
    environment = os.environ.copy()
    paths = [str(ROOT / "src"), str(ROOT / "agent-cluster/src"), str(ROOT)]
    if environment.get("PYTHONPATH"):
        paths.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(paths)
    return environment


def run_child(action, directory, stdout_name, stderr_name, exit_code_name):
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--internal-action", action,
         "--evidence-directory", str(directory)],
        cwd=ROOT,
        env=child_environment(),
        capture_output=True,
        text=True,
        check=False,
    )
    (directory / stdout_name).write_text(result.stdout, encoding="utf-8")
    (directory / stderr_name).write_text(result.stderr, encoding="utf-8")
    (directory / exit_code_name).write_text(f"{result.returncode}\n", encoding="utf-8")
    return {
        "exit_code": result.returncode,
        "stdout_path": stdout_name,
        "stderr_path": stderr_name,
        "stderr_sha256": digest_bytes(result.stderr.encode("utf-8")),
        "stderr": result.stderr,
        "stdout": result.stdout,
    }


def internal_action(action, directory):
    report = load_json(directory / "report.json")
    settings = Settings(cluster_config_file=CLUSTER_CONFIG)
    engine = create_engine(settings.database_url, pool_size=2, max_overflow=0)
    maker = sessionmaker(engine, expire_on_commit=False)
    request = report["original_request"]
    if action == "mark-sent":
        BudgetService(maker, verification_scope="cluster").mark_sent(
            report["provider_request_id"])
        print(json.dumps({"provider_request_id": report["provider_request_id"],
                          "status": "mark_sent_returned"}, ensure_ascii=False))
        engine.dispose()
        return 0
    if report["mode"] == "red":
        current = digest_file(ROOT / "src/shop/paid_models.py")
        require(current == PAID_MODELS_RED_SHA256,
                "红测模型调用来源已变化，已停止执行，避免发起模型请求")
    response = PaidModels(settings, BudgetService(maker, verification_scope="cluster")).request(
        run_id=request["run_id"],
        step_key=request["step_key"],
        service=request["service"],
        body=request["body"],
        input_limit=request["input_limit"],
        output_limit=request["output_limit"],
        timeout_seconds=request["timeout_seconds"],
    )
    print(json.dumps({"response": response}, ensure_ascii=False, default=str))
    engine.dispose()
    return 0


def safe_directory(path):
    resolved = Path(path).resolve()
    resolved.relative_to(EVIDENCE_ROOT.resolve())
    return resolved


def model_inputs(profile, goal_id):
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"恢复检查编号 {goal_id}。请返回一条简短中文确认，不调用业务工具。"},
    ]
    tools = coordinator_tools()
    body = build_body(messages, tools, profile=profile)
    input_limit = input_limit_for_request(profile, messages, tools)
    return messages, tools, body, input_limit


def create_report(directory, actor_id, profile, config_sha256, inputs_sha256, request):
    source_before = source_hashes()
    return {
        "run_id": directory.name,
        "milestone": ["CLU-05"],
        "mode": "red",
        "status": "running",
        "started_at": timestamp(),
        "evidence_directory": str(directory.relative_to(ROOT)),
        "database": database_identity(Settings().database_url),
        "budget_window_id": ACTIVE_WINDOW_ID,
        "max_additional_cny": str(MAX_ADDITIONAL_CNY),
        "actor_id": actor_id,
        "fixed_inputs": {"path": str(FIXED_INPUTS.relative_to(ROOT)), "sha256": inputs_sha256},
        "cluster_config": {"path": str(CLUSTER_CONFIG.relative_to(ROOT)), "sha256": config_sha256},
        "model_profile": profile.model_dump(mode="json"),
        "source_sha256_before": source_before,
        "original_request": request,
        "original_request_sha256": digest_json(request),
        "original_body_sha256": digest_json(request["body"]),
        "provider_request_id": None,
        "state_transitions": [],
        "commands": {},
    }


def run_red(directory):
    require(digest_file(ROOT / "src/shop/paid_models.py") == PAID_MODELS_RED_SHA256,
            "PaidModels源码已变化；红测已停止，避免产生付费请求")
    require(FIXED_INPUTS.is_file() and CLUSTER_CONFIG.is_file(), "固定输入或集群配置不存在")
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    inputs = load_json(FIXED_INPUTS)
    actor_id = inputs["accounts"]["customer"]["actor_id"]
    config = load_json(CLUSTER_CONFIG)
    profile = CoordinatorProfile.model_validate(config["profile"])
    require((profile.service, profile.model, profile.reasoning_effort, profile.output_limit)
            == ("deepseek", "deepseek-flash", "high", 8192),
            "集群协调模型配置与本次红测要求不一致")
    require(actor_id in config["actors"], "固定客户身份没有对应MCP登记配置")
    settings = Settings(cluster_config_file=CLUSTER_CONFIG)
    engine = create_engine(settings.database_url, pool_size=10, max_overflow=10)
    maker = sessionmaker(engine, expire_on_commit=False)
    store = ClusterStore(engine)
    goal_id = f"clu-unsent-resume-{uuid4()}"
    event_id = f"clu-unsent-resume-{uuid4().hex}"
    messages, tools, body, input_limit = model_inputs(profile, goal_id)
    planned_reservation = reserved_cost(input_limit, profile.output_limit,
                                        rate_card(profile.service, profile.model))
    require(planned_reservation <= MAX_ADDITIONAL_CNY,
            "单次模型请求预留超过0.15元检查上限")
    request = {
        "run_id": None,
        "step_key": STEP_KEY,
        "service": profile.service,
        "body": body,
        "input_limit": input_limit,
        "output_limit": profile.output_limit,
        "timeout_seconds": profile.timeout_seconds,
        "verification_scope": "cluster",
        "body_messages": messages,
        "tool_definitions": tools,
    }
    report = create_report(directory, actor_id, profile, digest_file(CLUSTER_CONFIG),
                           digest_file(FIXED_INPUTS), request)
    save_json(directory / "report.json", report)
    (directory / "request.json").write_text(
        json.dumps(request, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    goal_created = False
    try:
        report["budget_before"] = ledger_snapshot(engine)
        require(report["budget_before"]["window_id"] == ACTIVE_WINDOW_ID,
                "当前活动费用窗口与本次配置不一致")
        require(Decimal(report["budget_before"]["window_total_cny"]) + planned_reservation
                <= Decimal(report["budget_before"]["window_limit_cny"]),
                "活动费用窗口余额不足以容纳本次预留")
        report["model_request_limits"] = {
            "input_limit": input_limit,
            "output_limit": profile.output_limit,
            "planned_reservation_cny": str(planned_reservation),
            "scope": "cluster",
        }
        with engine.connect() as connection:
            report["database"]["identity"] = dict(connection.execute(
                text("select current_database() as database, current_schema() as schema")).mappings().one())
        save_json(directory / "report.json", report)
        goal = GoalDefinition(
            goal_id=goal_id,
            actor_id=actor_id,
            objective="验证协调请求预留、暂停、恢复时沿用原始未发送请求。",
            allowed_operations={"shop_customer_agent": ["orders"]},
            criteria=[CompletionCriterion(tool_name="shop_customer_agent", action="orders",
                                          path=["data", "orders"], operator="exists")],
            budget_cny=MAX_ADDITIONAL_CNY,
            max_decisions=1,
            max_tasks=1,
        )
        store.create_goal(goal)
        goal_created = True
        run = store.trigger(TriggerEvent(goal_id=goal_id, actor_id=actor_id,
                                         event_id=event_id, kind="event", payload={}))
        store.set_run(run.run_id, actor_id, status="running")
        request["run_id"] = run.run_id
        decision_request = {**request}
        saved_request, saved_response = store.prepare_decision(
            run.run_id, actor_id, STEP_KEY, decision_request)
        require(saved_request == decision_request and saved_response is None,
                "PostgreSQL没有保存完整的原始协调请求")
        report["goal_id"] = goal_id
        report["run_id"] = run.run_id
        report["event_id"] = event_id
        report["decision"] = {"run_id": run.run_id, "step_key": STEP_KEY,
                               "request_sha256": digest_json(saved_request),
                               "response_is_null": saved_response is None}
        reservation_before = state_snapshot(engine, goal_id, run.run_id, STEP_KEY, event_id)
        budget = BudgetService(maker, verification_scope="cluster")
        provider_request = budget.reserve(
            task_id=None,
            run_id=run.run_id,
            step_key=STEP_KEY,
            service=profile.service,
            model=profile.model,
            request_body=body,
            input_limit=input_limit,
            output_limit=profile.output_limit,
            verification=True,
        )
        require(provider_request.reservation_cny == planned_reservation,
                "数据库预留金额与费用计算结果不一致")
        require(provider_request.price_basis["verification_scope"] == "cluster",
                "ProviderRequest没有绑定cluster费用范围")
        report["provider_request_id"] = provider_request.id
        report["provider_request_reservation_cny"] = str(provider_request.reservation_cny)
        report["provider_request_initial_status"] = provider_request.status
        report["cluster_goal_binding"] = provider_request.price_basis.get("cluster_goal_id")
        report["cluster_run_binding"] = provider_request.price_basis.get("cluster_run_id")
        require(report["cluster_goal_binding"] == goal_id
                and report["cluster_run_binding"] == run.run_id,
                "预算预留没有绑定当前Goal与GoalRun")
        report["state_after_reserve"] = state_snapshot(
            engine, goal_id, run.run_id, STEP_KEY, event_id)
        (directory / "request.json").write_text(
            json.dumps(request, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        append_transition(report, "request_reserved", reservation_before, report["state_after_reserve"])
        save_json(directory / "report.json", report)

        before_pause = state_snapshot(engine, goal_id, run.run_id, STEP_KEY, event_id)
        store.control_goal(goal_id, actor_id, "pause")
        after_pause = state_snapshot(engine, goal_id, run.run_id, STEP_KEY, event_id)
        require(after_pause["goal_state"] == "paused" and after_pause["run_status"] == "paused",
                "暂停操作没有暂停专用Goal与GoalRun")
        append_transition(report, "pause_before_mark_sent", before_pause, after_pause)
        report["state_after_pause"] = after_pause
        save_json(directory / "report.json", report)

        mark_sent_result = run_child("mark-sent", directory, "mark-sent.stdout.txt",
                                     "mark-sent.stderr.txt", "mark-sent.exit-code.txt")
        report["commands"]["mark_sent"] = {
            key: value for key, value in mark_sent_result.items() if key != "stdout"
        }
        report["state_after_mark_sent"] = state_snapshot(
            engine, goal_id, run.run_id, STEP_KEY, event_id)
        report["budget_after_mark_sent"] = ledger_snapshot(engine)
        request_rows = report["state_after_mark_sent"]["provider_requests"]
        require(len(request_rows) == 1 and request_rows[0]["id"] == provider_request.id,
                "mark_sent改变了ProviderRequest编号或数量")
        require(request_rows[0]["status"] == "cancelled"
                and request_rows[0]["charged_cny"] is not None
                and Decimal(request_rows[0]["charged_cny"]) == 0,
                "暂停期间mark_sent没有将未发送请求标记为零费用cancelled")
        require(request_rows[0]["error"] == "cluster_stopped",
                "mark_sent没有保存cluster_stopped取消原因")
        require(report["commands"]["mark_sent"]["exit_code"] == 1
                and "BudgetExceeded: cancelled" in mark_sent_result["stderr"],
                "mark_sent红测没有保存预期的原始BudgetExceeded错误")
        before_unknown = report["budget_before"]["unknown_requests"]
        unknown_check = unknown_preservation(before_unknown,
                                             report["budget_after_mark_sent"]["unknown_requests"])
        require(unknown_check["preserved"], "红测改变了既有unknown请求记录")
        report["existing_unknown_records"] = unknown_check
        append_transition(report, "mark_sent_cancelled_and_released", after_pause,
                          report["state_after_mark_sent"])
        save_json(directory / "report.json", report)

        paid_models_sha256 = digest_file(ROOT / "src/shop/paid_models.py")
        require(paid_models_sha256 == PAID_MODELS_RED_SHA256,
                "PaidModels源码已变化；未运行恢复请求，以保证红测不产生付费调用")
        before_resume = state_snapshot(engine, goal_id, run.run_id, STEP_KEY, event_id)
        store.control_goal(goal_id, actor_id, "resume")
        after_resume = state_snapshot(engine, goal_id, run.run_id, STEP_KEY, event_id)
        require(after_resume["goal_state"] == "active" and after_resume["run_status"] == "queued",
                "恢复操作没有将专用Goal与GoalRun置于active/queued")
        append_transition(report, "resume_before_paid_models_retry", before_resume, after_resume)
        report["state_after_resume"] = after_resume
        save_json(directory / "report.json", report)

        retry_result = run_child("paid-request", directory, "paid-request-red.stdout.txt",
                                 "paid-request-red.stderr.txt", "paid-request-red.exit-code.txt")
        report["commands"]["paid_request_red"] = {
            key: value for key, value in retry_result.items() if key != "stdout"
        }
        report["state_after_paid_request_red"] = state_snapshot(
            engine, goal_id, run.run_id, STEP_KEY, event_id)
        require(report["commands"]["paid_request_red"]["exit_code"] == 1
                and "ValueError: 此请求已经取消" in retry_result["stderr"],
                "PaidModels没有在恢复时按预期拒绝已取消的原始请求")
        after_retry_rows = report["state_after_paid_request_red"]["provider_requests"]
        require(len(after_retry_rows) == 1 and after_retry_rows[0]["id"] == provider_request.id
                and after_retry_rows[0]["status"] == "cancelled"
                and after_retry_rows[0]["charged_cny"] is not None
                and Decimal(after_retry_rows[0]["charged_cny"]) == 0,
                "红测恢复调用新增或替换了ProviderRequest")
        require(after_retry_rows[0]["request_body_sha256"] == digest_json(body),
                "红测恢复调用改变了原始请求正文")
        before_final_pause = report["state_after_paid_request_red"]
        store.control_goal(goal_id, actor_id, "pause")
        report["state_after_final_pause"] = state_snapshot(
            engine, goal_id, run.run_id, STEP_KEY, event_id)
        require(report["state_after_final_pause"]["goal_state"] == "paused"
                and report["state_after_final_pause"]["run_status"] == "paused",
                "红测结束时专用Goal或GoalRun仍可调度")
        append_transition(report, "red_retry_rejected_and_goal_paused", before_final_pause,
                          report["state_after_final_pause"])
        report["budget_after_red"] = ledger_snapshot(engine)
        final_unknown_check = unknown_preservation(
            before_unknown, report["budget_after_red"]["unknown_requests"])
        require(final_unknown_check["preserved"], "红测结束时既有unknown请求记录发生变化")
        report["existing_unknown_records"] = final_unknown_check
        report["source_sha256_after"] = source_hashes()
        require(report["source_sha256_before"] == report["source_sha256_after"],
                "红测期间项目源码摘要发生变化")
        report["paid_model_calls"] = 0
        test_request = after_retry_rows[0]
        report["test_spent_cny"] = str(Decimal(test_request["charged_cny"]))
        report["test_open_reservation_cny"] = str(
            Decimal(test_request["reservation_cny"])
            if test_request["charged_cny"] is None else Decimal(0))
        require(Decimal(report["test_spent_cny"]) == 0
                and Decimal(report["test_open_reservation_cny"]) == 0,
                "红测结束时原请求仍有费用或开放预留")
        report["checks"] = [
            "real_goal_run_and_decision_request_saved",
            "real_cluster_budget_reservation_bound_to_goal",
            "paused_mark_sent_cancels_unsent_request_and_releases_reservation",
            "resumed_paid_models_request_rejects_cancelled_original_request",
            "original_provider_request_id_and_body_preserved",
            "goal_and_run_paused_after_red_stage",
            "existing_unknown_requests_preserved",
            "zero_paid_model_calls_and_zero_open_test_reservation",
        ]
        report["status"] = "red_reproduced"
        report["finished_at"] = timestamp()
        save_json(directory / "report.json", report)
        return 0
    finally:
        if goal_created and store.goal_state(goal_id, actor_id) not in {"paused", "cancelled"}:
            store.control_goal(goal_id, actor_id, "pause")
        if report["status"] == "running":
            report["status"] = "incomplete"
            report["finished_at"] = timestamp()
        report["source_sha256_after_finally"] = source_hashes()
        save_json(directory / "report.json", report)
        if report["status"] == "red_reproduced":
            (directory / "report-red.json").write_bytes((directory / "report.json").read_bytes())
        engine.dispose()


def run_green(directory):
    report_path = directory / "report.json"
    red_report_path = directory / "report-red.json"
    report_bytes = report_path.read_bytes()
    if not red_report_path.exists():
        (directory / "report-red.json").write_bytes(report_bytes)
    else:
        require(red_report_path.read_bytes() == report_bytes,
                "独立红测报告与绿色续跑前的原始报告不一致")
    report = json.loads(report_bytes)
    require(report["status"] == "red_reproduced" and report["mode"] == "red",
            "绿色续跑要求同一目录存在完整红测报告")
    request = load_json(directory / "request.json")
    require(request == report["original_request"], "原始请求文件与红测报告不一致")
    require(digest_json(request["body"]) == report["original_body_sha256"],
            "原始请求正文摘要不匹配")
    require(request["run_id"] == report["run_id"]
            and digest_json(request) == report["decision"]["request_sha256"],
            "完整请求与登记的运行或决策摘要不一致")
    require(digest_file(CLUSTER_CONFIG) == report["cluster_config"]["sha256"],
            "绿色续跑时集群配置已经改变")
    settings = Settings(cluster_config_file=CLUSTER_CONFIG)
    engine = create_engine(settings.database_url, pool_size=10, max_overflow=10)
    store = ClusterStore(engine)
    actor_id = report["actor_id"]
    goal_id = report["goal_id"]
    run_id = report["run_id"]
    step_key = request["step_key"]
    event_id = report["event_id"]
    before = state_snapshot(engine, goal_id, run_id, step_key, event_id)
    require(before["goal_state"] == "paused" and before["run_status"] == "paused",
            "绿色续跑前专用Goal与GoalRun必须保持paused")
    require(len(before["provider_requests"]) == 1
            and before["provider_requests"][0]["id"] == report["provider_request_id"]
            and before["provider_requests"][0]["status"] == "cancelled"
            and Decimal(before["provider_requests"][0]["charged_cny"]) == 0,
            "绿色续跑前ProviderRequest状态或编号不符合原始红测")
    with Session(engine) as session:
        decision = session.get(DecisionRecord, (run_id, step_key))
        require(decision is not None and decision.request == request and decision.response is None,
                "绿色续跑前DecisionRecord原始请求或响应状态不匹配")
    require(before["decision_request_sha256"] == digest_json(request)
            and before["provider_requests"][0]["request_body_sha256"] == report["original_body_sha256"],
            "数据库请求及决策内容与原始正文不一致")
    report["green_request_sha256"] = digest_json(request)
    report["green_started_at"] = timestamp()
    report["source_sha256_before_green"] = source_hashes()
    report["budget_before_green"] = ledger_snapshot(engine)
    require(report["budget_before_green"]["window_id"] == ACTIVE_WINDOW_ID,
            "绿色续跑时活动费用窗口已经改变")
    require(Decimal(report["provider_request_reservation_cny"]) <= MAX_ADDITIONAL_CNY,
            "原始请求预留超过0.15元检查上限")
    green_unknown_before = report["budget_before_green"]["unknown_requests"]
    report["mode"] = "green"
    append_transition(report, "green_resume", before, before)
    save_json(report_path, report)
    resumed = False
    try:
        store.control_goal(goal_id, actor_id, "resume")
        resumed = True
        report["state_after_green_resume"] = state_snapshot(engine, goal_id, run_id, step_key, event_id)
        require(report["state_after_green_resume"]["goal_state"] == "active"
                and report["state_after_green_resume"]["run_status"] == "queued",
                "绿色续跑没有恢复专用Goal与GoalRun")
        append_transition(report, "green_goal_resumed", before, report["state_after_green_resume"])
        save_json(report_path, report)
        retry_result = run_child("paid-request", directory, "paid-request-green.stdout.txt",
                                 "paid-request-green.stderr.txt", "paid-request-green.exit-code.txt")
        report["commands"]["paid_request_green"] = {
            key: value for key, value in retry_result.items() if key != "stdout"
        }
        report["state_after_paid_request_green"] = state_snapshot(engine, goal_id, run_id, step_key, event_id)
        if retry_result["exit_code"] != 0:
            store.control_goal(goal_id, actor_id, "pause")
            report["green_status"] = "failed_or_unknown_requires_review"
            report["budget_after_green_failure"] = ledger_snapshot(engine)
            report["source_sha256_after_green"] = source_hashes()
            report["finished_at"] = timestamp()
            save_json(report_path, report)
            return retry_result["exit_code"]
        requests = report["state_after_paid_request_green"]["provider_requests"]
        require(len(requests) == 1 and requests[0]["id"] == report["provider_request_id"],
                "绿色续跑创建了第二条ProviderRequest或改变了编号")
        completed = requests[0]
        require(completed["status"] == "completed" and completed["response_body_sha256"] is not None
                and completed["usage"] is not None and completed["provider_request_id"],
                "绿色续跑没有将模型响应、用量和ProviderRequest编号保存到PostgreSQL")
        require(completed["request_body_sha256"] == report["original_body_sha256"],
                "绿色续跑的请求正文与原始正文不一致")
        require(Decimal(completed["charged_cny"]) <= MAX_ADDITIONAL_CNY,
                "绿色续跑新增模型费用超过0.15元")
        with Session(engine) as session:
            row = session.get(ProviderRequest, report["provider_request_id"])
            decision = session.get(DecisionRecord, (run_id, step_key))
            require(row is not None and row.response_body is not None and row.usage is not None,
                    "PostgreSQL缺少原ProviderRequest响应或用量")
            save_json(directory / "provider-response.json", row.response_body)
            save_json(directory / "provider-usage.json", row.usage)
            require(decision is not None and decision.request == request,
                    "绿色续跑改变或删除了原始DecisionRecord")
            resumptions = row.price_basis["unsent_resumptions"]
            require(len(resumptions) == 1 and resumptions[0]["previous_status"] == "cancelled"
                    and resumptions[0]["previous_error"] == "cluster_stopped"
                    and Decimal(resumptions[0]["previous_charged_cny"]) == 0,
                    "原请求没有完整保存未发送取消的恢复历史")
        store.control_goal(goal_id, actor_id, "cancel")
        report["state_after_green_cancel"] = state_snapshot(engine, goal_id, run_id, step_key, event_id)
        require(report["state_after_green_cancel"]["goal_state"] == "cancelled"
                and report["state_after_green_cancel"]["run_status"] == "cancelled",
                "绿色续跑结束后专用Goal仍可调度")
        report["budget_after_green"] = ledger_snapshot(engine)
        spent_change = (Decimal(report["budget_after_green"]["project_spent_cny"])
                        - Decimal(report["budget_before_green"]["project_spent_cny"]))
        reserved_change = (Decimal(report["budget_after_green"]["project_reserved_cny"])
                           - Decimal(report["budget_before_green"]["project_reserved_cny"]))
        window_change = (Decimal(report["budget_after_green"]["window_total_cny"])
                         - Decimal(report["budget_before_green"]["window_total_cny"]))
        require(spent_change == window_change == Decimal(completed["charged_cny"])
                and reserved_change == 0,
                "绿色恢复费用与项目账目或验证窗口增量不一致")
        report["green_expenses"] = {"charged_cny": str(spent_change),
                                    "reserved_change_cny": str(reserved_change),
                                    "window_change_cny": str(window_change)}
        report["existing_unknown_records"] = unknown_preservation(
            green_unknown_before, report["budget_after_green"]["unknown_requests"])
        require(report["existing_unknown_records"]["preserved"],
                "绿色续跑改变了既有unknown请求记录")
        report["source_sha256_after_green"] = source_hashes()
        report["green_paid_model_calls"] = 1
        report["green_status"] = "passed"
        report["status"] = "passed"
        report["finished_at"] = timestamp()
        append_transition(report, "green_model_response_saved_and_goal_cancelled",
                          report["state_after_green_resume"], report["state_after_green_cancel"])
        save_json(report_path, report)
        return 0
    finally:
        if resumed and store.goal_state(goal_id, actor_id) not in {"paused", "cancelled"}:
            store.control_goal(goal_id, actor_id, "pause")
        report["source_sha256_after_finally"] = source_hashes()
        save_json(report_path, report)
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="复核CLU协调请求暂停后的未发送预留恢复")
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--green", action="store_true")
    parser.add_argument("--internal-action", choices=["mark-sent", "paid-request"],
                        help=argparse.SUPPRESS)
    parser.add_argument("--evidence-directory", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.internal_action:
        require(args.evidence_directory is not None, "内部运行缺少证据目录")
        return internal_action(args.internal_action, safe_directory(args.evidence_directory))
    if args.green:
        require(args.resume is not None and args.directory is None,
                "绿色续跑需要提供--resume目录")
        return run_green(safe_directory(args.resume))
    require(args.resume is None, "--resume需要同时提供--green")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    directory = args.directory or EVIDENCE_ROOT / f"clu-unsent-resume-20261003-{stamp}-{uuid4().hex[:8]}"
    directory = safe_directory(directory)
    exit_code = run_red(directory)
    print(json.dumps({"run_id": directory.name, "status": load_json(directory / "report.json")["status"],
                      "report": str((directory / "report.json").relative_to(ROOT)),
                      "green_resume_command": f"--green --resume {directory.relative_to(ROOT)}"},
                     ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    arguments = argparse.ArgumentParser(add_help=False)
    arguments.add_argument("--internal-action", choices=["mark-sent", "paid-request"])
    arguments.add_argument("--evidence-directory", type=Path)
    internal, _ = arguments.parse_known_args()
    if internal.internal_action:
        raise SystemExit(main())
    raise SystemExit(main())
