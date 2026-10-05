import asyncio
import json
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from agent_cluster.store import ClusterStore, digest
from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
)
from sqlalchemy import func, select

from shop.adventureworks import REVISION, import_orders
from shop.auth import hash_password
from shop.budget import BudgetExceeded, BudgetService
from shop.config import Settings
from shop.customer_provider import CustomerModels
from shop.customer_runtime import CustomerService
from shop.customer_runtime_models import CustomerTurn
from shop.customer_worker import CustomerWorker
from shop.integration_models import IntegrationCall
from shop.models import User
from shop.orders import bind_customer
from shop.paid_models import UnknownRequest
from shop.runtime_models import BudgetAccount, ProviderRequest

MESSAGE = "查询我的订单"
BODY = {
    "model": "deepseek-flash",
    "thinking": {"type": "enabled"},
    "reasoning_effort": "low",
    "max_tokens": 4096,
    "messages": [{"role": "user", "content": MESSAGE}],
    "tools": [],
    "stream": False,
    "response_format": {"type": "json_object"},
}


def _customer_with_turn(db_session, *, model_calls=3):
    import_orders(db_session, Path(".local/data/orders") / REVISION)
    user = User(username=f"cluster-customer-recovery-{uuid4().hex}",
                password_hash=hash_password("local-check"), role="customer")
    db_session.add(user)
    db_session.flush()
    bind_customer(db_session, user.id, 11091)
    settings = Settings(database_url=db_session.info["test_database_url"])
    service = CustomerService(settings)
    conversation = service.create_conversation(user.id, "集群客服恢复")
    turn_data = service.submit(conversation["id"], user.id, str(uuid4()), MESSAGE)
    turn = db_session.get(CustomerTurn, turn_data["id"])
    turn.status = "running"
    turn.model_calls = model_calls
    db_session.commit()
    return user, service, conversation, turn


def _cluster_link(db_session, owner_id, conversation_id, turn_id):
    store = ClusterStore(db_session.bind)
    store.initialize()
    goal = store.create_goal(GoalDefinition(
        actor_id=owner_id,
        objective="恢复集群发起的客服消息",
        allowed_operations={"shop_customer_agent": ["submit", "turn"]},
        criteria=[CompletionCriterion(tool_name="shop_customer_agent", action="submit",
                                      path=["data", "id"])],
    ))
    run = store.trigger(TriggerEvent(event_id="customer-recovery-" + uuid4().hex,
                                     goal_id=goal.goal_id, actor_id=owner_id))
    store.save_plan(run.run_id, owner_id, TaskPlan(tasks=[TaskSpec(
        task_id="customer-submit",
        tool_name="shop_customer_agent",
        action="submit",
        payload={"conversation_id": conversation_id, "message": MESSAGE},
    )]), expected_version=0)
    task = store.tasks(run.run_id, owner_id)[0]
    arguments = {"action": "submit", "payload": {"conversation_id": conversation_id,
                                                    "message": MESSAGE},
                 "request_id": task["request_id"], "upstream": []}
    store.set_task(run.run_id, owner_id, task["task_id"], status="sent", arguments=arguments)
    db_session.add(IntegrationCall(
        actor_id=owner_id,
        module_id="customer",
        operation="submit",
        request_id=task["request_id"],
        payload_sha256=digest(arguments["payload"]),
        resource_type="turn",
        resource_id=turn_id,
    ))
    db_session.commit()
    return store, goal, run, task


def _seed_request(db_session, service, turn_id, *, step_key="chat-0", cluster=False):
    budget = BudgetService(service.maker)
    request = budget.reserve(
        task_id=None,
        run_id=turn_id,
        step_key=step_key,
        service="deepseek",
        model=BODY["model"],
        request_body=BODY,
        input_limit=len(json.dumps(BODY, ensure_ascii=False).encode("utf-8")),
        output_limit=BODY["max_tokens"],
        verification=False,
        scope="customer",
    )
    budget.mark_sent(request.id)
    assert (request.price_basis["cluster_run_id"] is not None) is cluster
    return budget, request


@pytest.mark.parametrize("initial_status", ["sent", "unknown"])
def test_cluster_customer_unknown_recovery_preserves_request_and_reports_unknown(
    db_session, initial_status,
):
    owner, service, conversation, turn = _customer_with_turn(db_session)
    store, _goal, run, task = _cluster_link(
        db_session, owner.id, conversation["id"], turn.id)
    budget, request = _seed_request(db_session, service, turn.id, cluster=True)
    if initial_status == "unknown":
        budget.mark_unknown(request.id, "process_interrupted")
    db_session.expire_all()
    spent_before = db_session.get(BudgetAccount, "project").spent_cny
    reserved_before = db_session.get(BudgetAccount, "project").reserved_cny

    models = CustomerModels(service)
    for _ in range(2):
        with pytest.raises(UnknownRequest):
            asyncio.run(models.request(turn.id, "chat-0", "deepseek", BODY, 30))

    db_session.expire_all()
    stored = db_session.get(ProviderRequest, request.id)
    current_turn = db_session.get(CustomerTurn, turn.id)
    assert stored.status == "unknown"
    assert stored.charged_cny is None
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)
                              .where(ProviderRequest.run_id == turn.id)) == 1
    assert db_session.get(BudgetAccount, "project").spent_cny == spent_before
    assert db_session.get(BudgetAccount, "project").reserved_cny == reserved_before
    assert current_turn.model_calls == 3
    assert store.get_run(run.run_id, owner.id).status == "queued"
    assert store.tasks(run.run_id, owner.id)[0]["status"] == "sent"
    assert task["request_id"] == store.tasks(run.run_id, owner.id)[0]["request_id"]

    CustomerWorker(service).finish_error(turn.id, "UnknownRequest: 提供商用量未知", interrupted=True)
    public_turn = service.show_turn(turn.id, owner.id)
    events = service.events(conversation["id"], owner.id, after=0)["events"]
    assert public_turn["status"] == "unknown"
    assert public_turn["error"]
    assert public_turn["requests"] == [{
        "id": request.id,
        "status": "unknown",
        "model": "deepseek-flash",
        "usage": None,
        "reservation_cny": str(request.reservation_cny),
        "charged_cny": None,
        "error": "process_interrupted" if initial_status == "unknown"
        else "进程中断后无法确认提供商用量",
    }]
    assert events[-1]["type"] == "unknown"
    assert events[-1]["turn_id"] == turn.id
    assert events[-1]["payload"]["error"] == public_turn["error"]
    assert Decimal(str(public_turn["requests"][0]["reservation_cny"])) == request.reservation_cny
    service.engine.dispose()


def test_paused_customer_unsent_request_checks_goal_before_new_model_limit(db_session):
    owner, service, conversation, turn = _customer_with_turn(db_session)
    store, goal, _, _ = _cluster_link(db_session, owner.id, conversation["id"], turn.id)
    budget = BudgetService(service.maker)
    request = budget.reserve(
        task_id=None, run_id=turn.id, step_key="chat-0", service="deepseek",
        model=BODY["model"], request_body=BODY,
        input_limit=len(json.dumps(BODY, ensure_ascii=False).encode("utf-8")),
        output_limit=BODY["max_tokens"], verification=False, scope="customer",
    )
    store.control_goal(goal.goal_id, owner.id, "pause")
    with pytest.raises(BudgetExceeded):
        budget.mark_sent(request.id)
    db_session.expire_all()
    original = db_session.get(ProviderRequest, request.id)
    assert original.status == "cancelled" and original.error == "cluster_stopped"
    assert original.charged_cny == 0
    with pytest.raises(BudgetExceeded, match="目标尚未恢复"):
        asyncio.run(CustomerModels(service).request(turn.id, "chat-0", "deepseek", BODY, 30))
    db_session.expire_all()
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)
                              .where(ProviderRequest.run_id == turn.id)) == 1
    assert db_session.get(CustomerTurn, turn.id).model_calls == 3
    assert db_session.get(ProviderRequest, request.id).request_body == BODY
    assert db_session.get(ProviderRequest, request.id).status == "cancelled"
    assert db_session.get(BudgetAccount, "project").spent_cny == 0
    assert db_session.get(BudgetAccount, "project").reserved_cny == 0
    service.engine.dispose()


def test_standalone_customer_recovery_keeps_explicit_retry_boundary(db_session):
    _owner, service, _, turn = _customer_with_turn(db_session, model_calls=0)
    budget, original = _seed_request(db_session, service, turn.id, cluster=False)
    budget.mark_unknown(original.id, "process_interrupted")
    db_session.expire_all()
    spent_before = db_session.get(BudgetAccount, "project").spent_cny

    with pytest.raises(TimeoutError, match="没有剩余执行时间"):
        asyncio.run(CustomerModels(service).request(
            turn.id, "chat-0", "deepseek", BODY, 0))

    db_session.expire_all()
    requests = db_session.scalars(select(ProviderRequest).where(
        ProviderRequest.run_id == turn.id).order_by(ProviderRequest.step_key)).all()
    assert len(requests) == 2
    assert requests[0].id == original.id
    assert requests[0].status == "unknown"
    retry = next(item for item in requests if item.id != original.id)
    assert retry.step_key == "chat-0-retry-1"
    assert retry.status == "unknown"
    assert retry.price_basis["cluster_run_id"] is None
    assert db_session.get(CustomerTurn, turn.id).model_calls == 1
    assert db_session.get(BudgetAccount, "project").spent_cny == spent_before
    assert db_session.get(BudgetAccount, "project").reserved_cny == sum(
        (item.reservation_cny for item in requests), Decimal(0))
    service.engine.dispose()
