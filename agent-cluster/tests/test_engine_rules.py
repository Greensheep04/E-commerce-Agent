import json
from pathlib import Path

import pytest
from integration_protocol import OperationResult

from agent_cluster.engine import (
    bind_payload,
    completion_check,
    confirmation_authorized,
    customer_payload,
)
from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    InputBinding,
    TaskSpec,
)

ROOT = Path(__file__).resolve().parents[2]
INPUTS = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json"


@pytest.fixture
def actual_orders():
    inputs = json.loads(INPUTS.read_text())
    source = json.loads(Path(inputs["source_file"]).read_text())
    actor = inputs["accounts"]["customer"]["actor_id"]
    return actor, source["orders"]["customer"]


def goal(actor):
    return GoalDefinition(actor_id=actor, objective="保存本人订单及来源",
                          allowed_operations={"shop_customer_agent": ["orders"]},
                          criteria=[CompletionCriterion(tool_name="shop_customer_agent", action="orders",
                                                        path=["data", "orders"], min_matches=2)])


def test_completion_requires_registered_number_of_actual_results(actual_orders):
    actor, data = actual_orders
    definition = goal(actor)
    operation = OperationResult(data=data, status="completed").model_dump(mode="json")
    spec = TaskSpec(task_id="orders-one", tool_name="shop_customer_agent", action="orders")
    task = {"task_id": spec.task_id, "spec": spec.model_dump(mode="json"), "status": "completed", "result": operation}
    assert completion_check(definition, [task])[0] is False
    assert completion_check(definition, [task, {**task, "task_id": "orders-two"}])[0] is True
    assert completion_check(definition, [{**task, "status": "queued"}])[0] is False


def test_dependency_binding_reads_real_source_and_checks_finished_status(actual_orders):
    _, data = actual_orders
    operation = OperationResult(data=data, status="completed").model_dump(mode="json")
    spec = TaskSpec(task_id="detail", tool_name="shop_customer_agent", action="order",
                    dependencies=["orders"], bindings=[InputBinding(source_task_id="orders",
                    source_path=["data", "orders", 0, "order_number"], target_path=["order_number"])])
    tasks = {"orders": {"status": "completed", "result": operation}}
    assert bind_payload(spec, tasks)["order_number"] == data["orders"][0]["order_number"]
    tasks["orders"]["status"] = "running"
    with pytest.raises(ValueError, match="前置任务"):
        bind_payload(spec, tasks)


@pytest.mark.parametrize("message", ["确认", "确认创建", "同意", "是的", "确认创建人工工单"])
def test_confirmation_text_needs_explicit_owner_approval(actual_orders, message):
    actor, _ = actual_orders
    definition = goal(actor)
    assert not confirmation_authorized({"message": message, "confirm_pending": None}, definition)
    assert not confirmation_authorized({"message": message, "confirm_pending": "unapproved"}, definition)
    assert confirmation_authorized({"message": "查询本人订单"}, definition)


def test_customer_submit_preserves_required_confirmation(actual_orders):
    actor, _ = actual_orders
    definition = goal(actor).model_copy(update={"required_confirmations": ["customer_ticket"]})
    spec = TaskSpec(task_id="ticket", tool_name="shop_customer_agent", action="submit")
    payload = {"conversation_id": "conversation-one", "message": "订单商品有问题，需要工单"}
    assert customer_payload(spec, payload, definition)["require_ticket_confirmation"] is True
    assert "require_ticket_confirmation" not in payload
    assert customer_payload(spec, payload, goal(actor)) == payload


def test_dependency_binding_rejects_failed_business_operation():
    directory = ROOT / ".scratch/evidence/clu-live-customer-probe-20261003-r2/CC04-1"
    tasks = {task["task_id"]: task for task in json.loads((directory / "tasks.json").read_text())}
    source = next(task for task in tasks.values() if task["result"] and task["result"]["status"] == "failed")
    spec = TaskSpec(task_id="followup", tool_name="shop_customer_agent", action="turn",
                   dependencies=[source["task_id"]], bindings=[InputBinding(
                       source_task_id=source["task_id"], source_path=["data", "id"], target_path=["turn_id"])])
    with pytest.raises(ValueError, match="业务结果"):
        bind_payload(spec, tasks)


def test_dependency_binding_builds_array_target_from_real_order(actual_orders):
    _, data = actual_orders
    operation = OperationResult(data=data, status="completed").model_dump(mode="json")
    spec = TaskSpec(task_id="details", tool_name="shop_customer_agent", action="order",
                   dependencies=["orders"], bindings=[InputBinding(
                       source_task_id="orders", source_path=["data", "orders", 0, "order_number"],
                       target_path=["queries", 0, "order_number"])])
    tasks = {"orders": {"status": "completed", "result": operation}}
    assert bind_payload(spec, tasks) == {"queries": [{"order_number": data["orders"][0]["order_number"]}]}


def test_customer_schema_preserves_historical_default_payload():
    from shop.integration_adapters import MessageInput

    payload = {"conversation_id": "1d86862a-62db-46e1-a023-d5fec3f97748",
               "message": "查询本人订单", "confirm_pending": None}
    assert MessageInput.model_validate(payload).model_dump(mode="json") == payload
    protected = MessageInput.model_validate({**payload, "require_ticket_confirmation": True})
    assert protected.model_dump(mode="json")["require_ticket_confirmation"] is True


def test_failure_status_reads_original_exception_in_nested_groups():
    from shop.budget import BudgetExceeded
    from shop.paid_models import UnknownRequest

    from agent_cluster.failures import failure_detail, failure_status

    unknown = ExceptionGroup("MCP", [ExceptionGroup("session", [UnknownRequest("原请求结果未知")])])
    assert failure_status(unknown) == "unknown"
    assert "原请求结果未知" in failure_detail(unknown)
    budget = ExceptionGroup("MCP", [BudgetExceeded("验证预算不足")])
    assert failure_status(budget) == "budget_exhausted"
