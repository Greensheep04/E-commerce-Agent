import json
from decimal import Decimal

from agent_cluster.types import CompletionCriterion, GoalDefinition, GoalRun


def goal_and_run():
    goal = GoalDefinition(
        goal_id="goal-one", actor_id="actor-one", objective="人工消息协议检查目标",
        allowed_operations={"unit_protocol": ["read"]},
        criteria=[CompletionCriterion(tool_name="unit_protocol", path=["data"], operator="exists")],
        budget_cny=Decimal("3"), required_confirmations=["用户确认后执行"],
    )
    run = GoalRun(run_id="run-one", goal_id=goal.goal_id, actor_id=goal.actor_id,
                  goal_revision=goal.revision, spent_cny=Decimal("0.25"), reserved_cny=Decimal("0.5"))
    return goal, run


def protocol_round(number, *, status="completed"):
    return [
        {"role": "assistant", "content": None, "reasoning_content": f"人工协议消息 {number}",
         "tool_calls": [{"id": f"call-{number}", "type": "function", "function": {
             "name": "unit_protocol", "arguments": '{"action":"read"}'}}]},
        {"role": "tool", "tool_call_id": f"call-{number}", "content": json.dumps({
            "task_id": f"task-{number}", "request_id": f"request-{number}", "status": status,
            "result": {"status": status, "data": {"protocol_fixture": "人工协议结构"}},
        }, ensure_ascii=False)},
    ]
