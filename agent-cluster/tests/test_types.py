from decimal import Decimal

import pytest
from pydantic import ValidationError


def definition(**changes):
    from agent_cluster.types import CompletionCriterion, GoalDefinition

    return GoalDefinition(
        actor_id="actor-one", objective="查询本人订单并保存有来源的结果",
        allowed_operations={"shop_customer_agent": ["orders"]},
        criteria=[CompletionCriterion(tool_name="shop_customer_agent", path=["data", "orders"],
                                      operator="exists")],
        **changes,
    )


def test_goal_budget_and_authority_have_validated_types():
    goal = definition()
    assert goal.budget_cny == Decimal("30")
    assert goal.allowed_operations == {"shop_customer_agent": ["orders"]}
    assert goal.revision == 1
    with pytest.raises(ValidationError):
        definition(budget_cny=Decimal("-1"))


def test_goal_rejects_unknown_authority_fields():
    with pytest.raises(ValidationError):
        definition(automatic_admin=True)


def test_task_plan_rejects_cycles_and_missing_dependencies():
    from agent_cluster.types import TaskPlan, TaskSpec

    first = TaskSpec(task_id="first", tool_name="shop_customer_agent", action="orders",
                     payload={}, dependencies=["second"])
    second = TaskSpec(task_id="second", tool_name="shop_customer_agent", action="identity",
                      payload={}, dependencies=["first"])
    with pytest.raises(ValidationError, match="循环"):
        TaskPlan(tasks=[first, second])
    with pytest.raises(ValidationError, match="依赖"):
        TaskPlan(tasks=[first])


def test_note_reference_requires_scope_version_and_digest():
    from agent_cluster.types import NoteRef

    reference = NoteRef(note_id="note-one", goal_id="goal-one", actor_id="actor-one",
                        version=1, sha256="a" * 64, topic="订单来源", summary="订单查询来源",
                        source="shop_customer_agent")
    assert reference.version == 1
    with pytest.raises(ValidationError):
        NoteRef(**{**reference.model_dump(), "sha256": "wrong"})
