import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
from agent_cluster.store import ClusterStore, EventRecord, GoalRecord, RunRecord, TaskRecord
from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
)
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from shop.budget import BudgetExceeded, BudgetService, rate_card, reserved_cost
from shop.config import Settings
from shop.customer_agent import CustomerServiceAgent
from shop.customer_provider import CustomerModels
from shop.customer_runtime import CustomerService
from shop.customer_runtime_models import CustomerConversation, CustomerTurn
from shop.integration import ShopIntegration
from shop.integration_adapters import MessageInput
from shop.integration_models import IntegrationBudgetWindow, IntegrationCall
from shop.models import User
from shop.runtime_models import BudgetAccount, ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
FIXED_INPUTS = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json"
CLUSTER_CONFIG = ROOT / ".scratch/cluster-http-config.json"
EVIDENCE_DIRECTORY = ROOT / ".scratch/evidence/clu-customer-unsent-20261003-r1"
ACTIVE_WINDOW_ID = "clu-live-20261003"
MAX_RESERVATION_CNY = Decimal("0.14")
STEP_KEY = "chat-0"
MESSAGE = "查询我的订单"
SOURCE_FILES = (
    Path(__file__).resolve(),
    ROOT / "agent-cluster/src/agent_cluster/engine.py",
    ROOT / "agent-cluster/src/agent_cluster/store.py",
    ROOT / "agent-cluster/src/agent_cluster/types.py",
    ROOT / "src/shop/budget.py",
    ROOT / "src/shop/cluster.py",
    ROOT / "src/shop/customer_agent.py",
    ROOT / "src/shop/customer_provider.py",
    ROOT / "src/shop/customer_runtime.py",
    ROOT / "src/shop/customer_runtime_models.py",
    ROOT / "src/shop/integration.py",
    ROOT / "src/shop/integration_adapters.py",
    ROOT / "src/shop/integration_models.py",
    ROOT / "src/shop/integration_store.py",
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
    target = Path(path)
    target.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n",
                      encoding="utf-8")
    target.chmod(0o600)


def timestamp():
    return datetime.now(UTC).isoformat()


def source_hashes():
    return {str(path.relative_to(ROOT)): digest_file(path) for path in SOURCE_FILES}


def database_identity(database_url):
    url = make_url(database_url)
    return {"driver": url.drivername, "host": url.host, "port": url.port,
            "database": url.database}


def prepare_evidence_directory():
    EVIDENCE_DIRECTORY.parent.mkdir(parents=True, exist_ok=True)
    if EVIDENCE_DIRECTORY.exists():
        if any(EVIDENCE_DIRECTORY.iterdir()):
            raise FileExistsError("本轮证据目录已有记录，拒绝覆盖或重复执行")
    else:
        EVIDENCE_DIRECTORY.mkdir(mode=0o700)
    EVIDENCE_DIRECTORY.chmod(0o700)


def health_snapshot():
    with httpx.Client(trust_env=False, timeout=5) as client:
        response = client.get("http://127.0.0.1:8001/health")
        response.raise_for_status()
        body = response.json()
    require(body.get("status") == "ok", "后端健康检查没有返回ok")
    return {"url": "http://127.0.0.1:8001/health", "status_code": response.status_code,
            "status": body["status"]}


def provider_snapshot(row):
    return {
        "id": row.id,
        "task_id": row.task_id,
        "run_id": row.run_id,
        "step_key": row.step_key,
        "service": row.service,
        "model": row.model,
        "status": row.status,
        "reservation_cny": str(row.reservation_cny),
        "charged_cny": None if row.charged_cny is None else str(row.charged_cny),
        "error": row.error,
        "provider_request_id": row.provider_request_id,
        "request_body_sha256": digest_json(row.request_body),
        "response_body_sha256": None if row.response_body is None else digest_json(row.response_body),
        "usage": row.usage,
        "price_basis": row.price_basis,
        "completed_at": None if row.completed_at is None else row.completed_at.isoformat(),
    }


def unknown_rows(session):
    rows = session.scalars(select(ProviderRequest).where(
        ProviderRequest.status == "unknown").order_by(ProviderRequest.id)).all()
    return [{
        "id": row.id,
        "run_id": row.run_id,
        "step_key": row.step_key,
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
                       if row.price_basis.get("integration_run_id") == ACTIVE_WINDOW_ID]
        total = sum((row.charged_cny if row.charged_cny is not None
                     else row.reservation_cny for row in window_rows), Decimal(0))
        return {
            "window_id": window.id,
            "window_limit_cny": str(window.limit_cny),
            "window_total_cny": str(total),
            "window_request_count": len(window_rows),
            "window_open_reservation_count": sum(row.charged_cny is None for row in window_rows),
            "project_spent_cny": str(account.spent_cny),
            "project_reserved_cny": str(account.reserved_cny),
            "unknown_requests": unknown_rows(session),
        }


def unknown_preservation(before, after):
    after_by_id = {row["id"]: row for row in after}
    before_by_id = {row["id"]: row for row in before}
    missing = sorted(set(before_by_id) - set(after_by_id))
    changed = sorted(key for key in before_by_id.keys() & after_by_id.keys()
                     if before_by_id[key] != after_by_id[key])
    return {"preserved": not missing and not changed, "missing_ids": missing,
            "changed_ids": changed, "new_unknown_ids": sorted(set(after_by_id) - set(before_by_id))}


def state_snapshot(customer, store, actor_id, goal_id, run_id, task_id,
                   conversation_id, turn_id, business_request_id):
    store.get_goal(goal_id, actor_id)
    run = store.get_run(run_id, actor_id)
    task = next(item for item in store.tasks(run_id, actor_id) if item["task_id"] == task_id)
    with customer.maker() as session:
        conversation = session.get(CustomerConversation, conversation_id)
        turn = session.get(CustomerTurn, turn_id) if turn_id is not None else None
        requests = [] if turn_id is None else session.scalars(select(ProviderRequest).where(
            ProviderRequest.run_id == turn_id,
            ProviderRequest.step_key.like(STEP_KEY + "%")).order_by(ProviderRequest.created_at)).all()
        linked_calls = session.scalars(select(IntegrationCall).where(
            IntegrationCall.actor_id == actor_id,
            IntegrationCall.module_id == "customer",
            IntegrationCall.operation == "submit",
            IntegrationCall.request_id == business_request_id)).all()
        event = session.get(EventRecord, (goal_id, run.trigger_id))
        record_goal = session.get(GoalRecord, goal_id)
        record_run = session.get(RunRecord, run_id)
        record_task = session.get(TaskRecord, (run_id, task_id))
        return {
            "goal_state": None if record_goal is None else record_goal.state,
            "run_status": None if record_run is None else record_run.status,
            "run_id": run.run_id,
            "run_trigger_id": run.trigger_id,
            "event_exists": event is not None,
            "task": {"task_id": task["task_id"], "request_id": task["request_id"],
                     "status": task["status"], "spec": task["spec"],
                     "arguments_sha256": None if record_task.arguments is None
                     else digest_json(record_task.arguments)},
            "conversation_exists": conversation is not None,
            "turn": None if turn is None else {
                "id": turn.id,
                "request_id": turn.request_id,
                "status": turn.status,
                "model_calls": turn.model_calls,
                "cancel_requested": turn.cancel_requested,
            },
            "integration_call_count": len(linked_calls),
            "integration_call_resource_ids": [item.resource_id for item in linked_calls],
            "provider_requests": [provider_snapshot(item) for item in requests],
        }


def customer_request_body(profile, messages, tools):
    body = {key: profile[key] for key in (
        "model", "thinking", "reasoning_effort", "max_tokens")}
    body.update(messages=messages, tools=tools, stream=False)
    if "response_format" in profile:
        body["response_format"] = profile["response_format"]
    return body


def main():
    os.chdir(ROOT)
    backend = health_snapshot()
    prepare_evidence_directory()
    report_path = EVIDENCE_DIRECTORY / "report.json"
    report = {
        "run_id": EVIDENCE_DIRECTORY.name,
        "status": "running",
        "started_at": timestamp(),
        "milestone": "CLU-04 customer unsent request recovery",
        "maximum_customer_models_chat_calls": 1,
        "maximum_reservation_cny": str(MAX_RESERVATION_CNY),
        "source_sha256_before": source_hashes(),
        "fixed_inputs_sha256": digest_file(FIXED_INPUTS),
        "cluster_config_sha256": digest_file(CLUSTER_CONFIG),
        "backend": backend,
        "phases": [],
    }
    save_json(report_path, report)

    settings = None
    customer = None
    integration = None
    store = None
    actor_id = None
    goal_id = None
    run_id = None
    event_id = None
    task_id = "customer-unsent-submit"
    conversation_id = None
    turn_id = None
    business_request_id = None
    provider_request_id = None
    expected_body = None
    initial_price_basis = None
    unknown_before = []
    call_started = False
    succeeded = False

    try:
        inputs = load_json(FIXED_INPUTS)
        actor_id = inputs["accounts"]["customer"]["actor_id"]
        settings = Settings()
        customer = CustomerService(settings)
        store = ClusterStore(customer.engine)
        integration = ShopIntegration(settings, customer=customer)
        configuration = load_json(CLUSTER_CONFIG)
        require(configuration.get("autorun") is False, "集群后端必须使用autorun=false配置")
        require(actor_id in configuration.get("actors", {}), "固定customer账号没有集群MCP配置")
        report["database"] = database_identity(settings.database_url)
        report["customer_actor_id"] = actor_id
        report["cluster_profile"] = configuration.get("profile", {})
        report["budget_before"] = ledger_snapshot(customer.engine)
        require(report["budget_before"]["window_id"] == ACTIVE_WINDOW_ID,
                "活动费用窗口与本轮固定窗口不一致")
        unknown_before = report["budget_before"]["unknown_requests"]
        with customer.maker() as session:
            user = session.get(User, actor_id)
            require(user is not None and user.role == "customer", "固定账号不是customer账号")

        conversation = customer.create_conversation(actor_id, "客服未发送请求恢复验证")
        conversation_id = conversation["id"]
        report["conversation_id"] = conversation_id
        report["customer_context"] = customer.model_context(conversation_id)
        save_json(EVIDENCE_DIRECTORY / "customer-context.json", report["customer_context"])
        report["conversation_orders_sha256"] = digest_json(conversation["orders"])
        save_json(EVIDENCE_DIRECTORY / "conversation-orders.json", conversation["orders"])

        goal = GoalDefinition(
            actor_id=actor_id,
            objective="恢复本次集群提交的客服订单查询消息",
            allowed_operations={"shop_customer_agent": ["submit"]},
            criteria=[CompletionCriterion(tool_name="shop_customer_agent", action="submit",
                                          path=["data", "id"])],
            budget_cny=MAX_RESERVATION_CNY,
            max_decisions=1,
            max_tasks=1,
        )
        store.create_goal(goal)
        goal_id = goal.goal_id
        event = TriggerEvent(event_id="customer-unsent-" + uuid4().hex,
                             goal_id=goal_id, actor_id=actor_id, kind="manual",
                             payload={"source": "customer-unsent-live-verification"})
        run = store.trigger(event)
        run_id = run.run_id
        event_id = event.event_id
        task_spec = TaskSpec(task_id=task_id, tool_name="shop_customer_agent", action="submit",
                             payload={"conversation_id": conversation_id, "message": MESSAGE},
                             purpose="建立关联的客服Turn并验证同一模型请求恢复")
        store.save_plan(run_id, actor_id, TaskPlan(tasks=[task_spec]), expected_version=0)
        task = next(item for item in store.tasks(run_id, actor_id) if item["task_id"] == task_id)
        business_request_id = task["request_id"]
        arguments = {"action": "submit", "payload": task_spec.payload,
                     "request_id": business_request_id, "upstream": []}
        store.set_task(run_id, actor_id, task_id, status="sent", arguments=arguments)
        call_context = integration.auth.issue(actor_id, business_request_id)
        integration.auth.verify(call_context)
        customer_adapter = integration.adapters["customer"]
        require(customer_adapter.module_id == "customer", "customer adapter注册编号异常")
        submitted = customer_adapter._submit(
            MessageInput(conversation_id=conversation_id, message=MESSAGE), call_context)
        turn_id = submitted.data["id"]
        require(submitted.data["request_id"] == business_request_id,
                "customer adapter没有保留集群request_id")
        require(submitted.data["status"] == "queued", "新客服Turn没有保持queued状态")
        report["identity"] = {"goal_id": goal_id, "run_id": run_id, "event_id": event_id,
                               "task_id": task_id, "task_request_id": business_request_id,
                               "conversation_id": conversation_id, "turn_id": turn_id,
                               "turn_request_id": submitted.data["request_id"]}
        state_created = state_snapshot(customer, store, actor_id, goal_id, run_id, task_id,
                                        conversation_id, turn_id, business_request_id)
        require(state_created["integration_call_count"] == 1
                and state_created["integration_call_resource_ids"] == [turn_id],
                "registered customer adapter没有通过IntegrationStore关联Turn")
        require(len(state_created["provider_requests"]) == 0,
                "专用Turn在预留之前已经产生ProviderRequest")
        report["state_after_registration"] = state_created
        report["phases"].append({"name": "registered_goal_run_task_conversation_turn",
                                 "recorded_at": timestamp(), "status": "passed"})
        save_json(report_path, report)

        agent = CustomerServiceAgent(customer, turn_id)
        initial_state = agent.initial()
        tools = agent.tool_definitions()
        with customer.maker() as session:
            turn = session.get(CustomerTurn, turn_id)
            profile = dict(turn.config)
            require(turn.model_calls == 0, "新客服Turn已有模型调用计数")
        expected_body = customer_request_body(profile, initial_state["messages"], tools)
        input_limit = len(json.dumps(expected_body, ensure_ascii=False).encode("utf-8"))
        output_limit = expected_body["max_tokens"]
        card = rate_card("deepseek", expected_body["model"])
        planned_reservation = reserved_cost(input_limit, output_limit, card)
        report["model_request"] = {
            "service": "deepseek",
            "model": expected_body["model"],
            "step_key": STEP_KEY,
            "input_limit_bytes": input_limit,
            "output_limit_tokens": output_limit,
            "request_body_sha256": digest_json(expected_body),
            "request_body_path": "provider-request.json",
            "tool_names": [item["function"]["name"] for item in tools],
            "turn_model_calls_before_reservation": 0,
            "turn_model_calls_after_reservation": 1,
            "planned_reservation_cny": str(planned_reservation),
        }
        save_json(EVIDENCE_DIRECTORY / "provider-request.json", expected_body)
        save_json(EVIDENCE_DIRECTORY / "customer-initial-messages.json", initial_state["messages"])
        require(planned_reservation <= MAX_RESERVATION_CNY,
                "单次客服请求预留超过0.14元，未登记ProviderRequest")
        with customer.maker.begin() as session:
            turn = session.get(CustomerTurn, turn_id, with_for_update=True)
            require(turn.model_calls == 0 and turn.status == "queued",
                    "预留前客服Turn状态或模型调用计数已改变")
            request = BudgetService(customer.maker).reserve(
                task_id=None, run_id=turn_id, step_key=STEP_KEY, service="deepseek",
                model=expected_body["model"], request_body=expected_body,
                input_limit=input_limit, output_limit=output_limit,
                verification=False, scope="customer", session=session)
            require(request.reservation_cny == planned_reservation,
                    "ProviderRequest预留与预先计算金额不一致")
            require(request.reservation_cny <= MAX_RESERVATION_CNY,
                    "ProviderRequest预留超过0.14元")
            provider_request_id = request.id
            initial_price_basis = dict(request.price_basis)
            turn.model_calls += 1
        report["model_request"].update({
            "provider_request_id": provider_request_id,
            "business_request_id": business_request_id,
            "provider_status_before_pause": "reserved",
            "reservation_cny": str(planned_reservation),
            "price_basis": initial_price_basis,
        })
        report["state_before_pause"] = state_snapshot(
            customer, store, actor_id, goal_id, run_id, task_id,
            conversation_id, turn_id, business_request_id)
        save_json(report_path, report)

        store.control_goal(goal_id, actor_id, "pause")
        pause_state = state_snapshot(customer, store, actor_id, goal_id, run_id, task_id,
                                     conversation_id, turn_id, business_request_id)
        require(pause_state["goal_state"] == "paused" and pause_state["run_status"] == "paused",
                "专用Goal及GoalRun没有进入paused状态")
        try:
            BudgetService(customer.maker).mark_sent(provider_request_id)
        except BudgetExceeded as error:
            report["expected_mark_sent_rejection"] = {
                "exception_type": type(error).__name__, "message": str(error)}
        else:
            raise AssertionError("暂停Goal后mark_sent没有拒绝未发送客服请求")
        paused_request = state_snapshot(customer, store, actor_id, goal_id, run_id, task_id,
                                        conversation_id, turn_id, business_request_id)
        require(len(paused_request["provider_requests"]) == 1,
                "暂停阶段新增了重复ProviderRequest")
        cancelled_request = paused_request["provider_requests"][0]
        require(cancelled_request["id"] == provider_request_id
                and cancelled_request["status"] == "cancelled"
                and cancelled_request["error"] == "cluster_stopped"
                and Decimal(cancelled_request["charged_cny"]) == 0
                and cancelled_request["response_body_sha256"] is None
                and cancelled_request["usage"] is None
                and cancelled_request["provider_request_id"] is None
                and cancelled_request["completed_at"] is None
                and cancelled_request["price_basis"]["cluster_goal_id"] == goal_id
                and cancelled_request["price_basis"]["cluster_run_id"] == run_id
                and cancelled_request["price_basis"]["integration_run_id"] == ACTIVE_WINDOW_ID,
                "暂停后原客服预留没有按cluster_stopped零费用取消")
        report["state_after_unsent_cancellation"] = paused_request
        report["budget_after_unsent_cancellation"] = ledger_snapshot(customer.engine)
        report["phases"].append({"name": "paused_customer_request_cancelled_without_send",
                                 "recorded_at": timestamp(), "status": "passed"})
        save_json(report_path, report)

        store.control_goal(goal_id, actor_id, "resume")
        resumed = state_snapshot(customer, store, actor_id, goal_id, run_id, task_id,
                                 conversation_id, turn_id, business_request_id)
        require(resumed["goal_state"] == "active" and resumed["run_status"] == "queued",
                "显式恢复没有重新启用专用Goal及GoalRun")
        report["state_before_single_model_call"] = resumed
        report["customer_models_chat_calls_started"] = 1
        report["customer_models_chat_call_started_at"] = timestamp()
        report["phases"].append({"name": "goal_resumed_for_original_customer_request",
                                 "recorded_at": timestamp(), "status": "passed"})
        save_json(report_path, report)

        call_started = True
        returned_message = asyncio.run(CustomerModels(customer).chat(
            turn_id, 0, initial_state["messages"], tools, 60))
        with customer.maker() as session:
            request = session.get(ProviderRequest, provider_request_id)
            current_turn = session.get(CustomerTurn, turn_id)
            require(request is not None, "ProviderRequest在客服模型响应后不存在")
            require(request.request_body == expected_body,
                    "恢复请求修改了ProviderRequest正文")
            require(request.status == "completed" and request.response_body is not None
                    and request.usage is not None and request.provider_request_id,
                    "一次真实客服模型请求没有保存completed响应及用量")
            require(current_turn.model_calls == 1,
                    "恢复原客服请求改变了model_calls计数")
            require(request.id == provider_request_id and request.run_id == turn_id
                    and request.step_key == STEP_KEY,
                    "恢复过程改变了ProviderRequest编号或客服请求归属")
            require(request.price_basis["cluster_goal_id"] == goal_id
                    and request.price_basis["cluster_run_id"] == run_id
                    and request.price_basis["integration_run_id"] == ACTIVE_WINDOW_ID,
                    "恢复过程改变了目标运行或费用窗口归属")
            require(request.service == "deepseek" and request.model == expected_body["model"],
                    "恢复过程改变了模型服务或模型编号")
            require(request.reservation_cny == planned_reservation
                    and request.charged_cny is not None
                    and Decimal(0) <= request.charged_cny <= request.reservation_cny,
                    "客服请求实际费用超过原始预留金额")
            save_json(EVIDENCE_DIRECTORY / "provider-response.json", request.response_body)
            save_json(EVIDENCE_DIRECTORY / "provider-usage.json", request.usage)
            save_json(EVIDENCE_DIRECTORY / "customer-model-message.json", returned_message)
            history = request.price_basis.get("unsent_resumptions", [])
            require(len(history) == 1 and history[0]["previous_status"] == "cancelled"
                    and history[0]["previous_error"] == "cluster_stopped"
                    and Decimal(history[0]["previous_charged_cny"]) == 0
                    and Decimal(history[0]["reservation_cny"]) == planned_reservation,
                    "恢复后ProviderRequest没有保留原始取消历史")
            recovered_basis = {key: value for key, value in request.price_basis.items()
                               if key != "unsent_resumptions"}
            require(recovered_basis == initial_price_basis,
                    "恢复过程改变了原始计价依据")
            report["provider_response"] = {
                "provider_request_id": request.provider_request_id,
                "response_sha256": digest_json(request.response_body),
                "response_path": "provider-response.json",
                "usage_sha256": digest_json(request.usage),
                "usage_path": "provider-usage.json",
                "charged_cny": str(request.charged_cny),
                "completed_at": request.completed_at.isoformat(),
                "returned_message_sha256": digest_json(returned_message),
                "returned_message_path": "customer-model-message.json",
            }
            report["unsent_resumptions"] = history
            report["provider_request_after_recovery"] = provider_snapshot(request)
            report["customer_model_calls_after_recovery"] = current_turn.model_calls

        state_after_call = state_snapshot(customer, store, actor_id, goal_id, run_id, task_id,
                                          conversation_id, turn_id, business_request_id)
        require(len(state_after_call["provider_requests"]) == 1,
                "客服恢复创建了重复ProviderRequest")
        require(state_after_call["integration_call_count"] == 1
                and state_after_call["task"]["request_id"] == business_request_id
                and state_after_call["turn"]["request_id"] == business_request_id,
                "恢复过程改变或重复创建了business request_id")
        report["state_after_single_model_call"] = state_after_call
        report["budget_after_customer_model_call"] = ledger_snapshot(customer.engine)
        budget_before = report["budget_before"]
        budget_after = report["budget_after_customer_model_call"]
        own_request = state_after_call["provider_requests"][0]
        report["request_specific_expense"] = {
            "reservation_cny": own_request["reservation_cny"],
            "charged_cny": own_request["charged_cny"],
            "open_reservation_cny": "0",
            "window_id": own_request["price_basis"]["integration_run_id"],
            "window_total_before_cny": budget_before["window_total_cny"],
            "window_total_after_cny": budget_after["window_total_cny"],
            "window_total_delta_is_not_used_as_request_cost": True,
        }
        report["unknown_preservation_before_cleanup"] = unknown_preservation(
            unknown_before, budget_after["unknown_requests"])
        require(report["unknown_preservation_before_cleanup"]["preserved"],
                "本次执行期间改变了既有unknown请求记录")
        require(all(row["id"] != provider_request_id
                    for row in budget_after["unknown_requests"]),
                "客服模型请求以unknown结束，禁止再次发送")
        report["phases"].append({"name": "single_customer_model_request_completed_and_saved",
                                 "recorded_at": timestamp(), "status": "passed"})
        succeeded = True
    finally:
        active_error = sys.exc_info()[1]
        if active_error is not None:
            report["failure"] = {"exception_type": type(active_error).__name__}
        cleanup_completed = False
        try:
            if store is not None and goal_id is not None:
                store.control_goal(goal_id, actor_id, "cancel")
                tasks = store.tasks(run_id, actor_id) if run_id is not None else []
                if any(item["task_id"] == task_id for item in tasks):
                    store.set_task(run_id, actor_id, task_id, status="cancelled",
                                   error="验证流程已结束")
            if customer is not None and turn_id is not None:
                customer.cancel(turn_id, actor_id)
            if customer is not None and store is not None and goal_id is not None and run_id is not None:
                final_state = state_snapshot(customer, store, actor_id, goal_id, run_id, task_id,
                                             conversation_id, turn_id, business_request_id)
                report["state_after_cleanup"] = final_state
                report["budget_after_cleanup"] = ledger_snapshot(customer.engine)
                report["unknown_preservation_after_cleanup"] = unknown_preservation(
                    unknown_before, report["budget_after_cleanup"]["unknown_requests"])
                if succeeded:
                    require(final_state["goal_state"] == "cancelled"
                            and final_state["run_status"] == "cancelled"
                            and final_state["task"]["status"] == "cancelled"
                            and final_state["turn"]["status"] == "cancelled",
                            "真实取消没有终止专用Goal、GoalRun、TaskSpec及CustomerTurn")
                    require(len(final_state["provider_requests"]) == 1
                            and final_state["provider_requests"][0]["status"] == "completed",
                            "取消未完成客服消息时改变了已完成ProviderRequest")
                    require(report["unknown_preservation_after_cleanup"]["preserved"],
                            "最终取消期间改变了既有unknown请求记录")
            cleanup_completed = True
        finally:
            final_error = sys.exc_info()[1]
            if final_error is not None and final_error is not active_error:
                report["status"] = "cleanup_failed"
                report["cleanup_failure"] = {"exception_type": type(final_error).__name__}
            elif active_error is not None:
                report["status"] = "failed"
            elif succeeded and cleanup_completed:
                report["status"] = "passed"
                report["phases"].append({
                    "name": "goal_and_customer_turn_cancelled_after_response_saved",
                    "recorded_at": timestamp(), "status": "passed"})
            else:
                report["status"] = "failed"
            report["finished_at"] = timestamp()
            report["customer_models_chat_calls_started"] = int(call_started)
            report["source_sha256_after"] = source_hashes()
            report["source_identity_stable"] = (
                report["source_sha256_before"] == report["source_sha256_after"])
            if report["status"] == "passed" and not report["source_identity_stable"]:
                report["status"] = "failed"
                report["failure"] = {"exception_type": "SourceIdentityChanged"}
            report["unknown_preservation_after_cleanup"] = report.get(
                "unknown_preservation_after_cleanup", {"preserved": False})
            try:
                save_json(report_path, report)
            finally:
                if integration is not None:
                    integration.engine.dispose()
                if customer is not None:
                    customer.engine.dispose()

    print(json.dumps({"run_id": report["run_id"], "status": report["status"],
                      "report": str(report_path.relative_to(ROOT)),
                      "provider_request_id": provider_request_id,
                      "customer_models_chat_calls_started": report["customer_models_chat_calls_started"],
                      "request_charged_cny": report.get("request_specific_expense", {}).get("charged_cny")},
                     ensure_ascii=False))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
