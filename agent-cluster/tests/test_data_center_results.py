import json
from pathlib import Path

from agent_cluster.engine import completion_check
from agent_cluster.types import CompletionCriterion, GoalDefinition, TaskSpec

ROOT = Path(__file__).resolve().parents[2]


def test_real_queued_data_task_cannot_satisfy_completion():
    result = json.loads((ROOT / ".scratch/evidence/dc05-integration-20261004-r1/initial.json").read_text())
    assert result["status"] == "queued"
    identities = json.loads((ROOT / ".scratch/evidence/dc02-host-20261004-r1/identities.json").read_text())
    goal = GoalDefinition(actor_id=identities["actors"]["sync"]["actor_id"], objective="完成来源同步",
                          allowed_operations={"shop_data_center_agent": ["sync", "show"]},
                          criteria=[CompletionCriterion(tool_name="shop_data_center_agent", action="sync",
                                                        path=["data", "task_id"], operator="exists")])
    spec = TaskSpec(task_id="sync", tool_name="shop_data_center_agent", action="sync")
    task = {"task_id": "sync", "spec": spec.model_dump(mode="json"), "status": "completed", "result": result}
    assert not completion_check(goal, [task])[0]


def test_real_partial_answer_cannot_satisfy_completion():
    result = json.loads((ROOT / ".scratch/evidence/dc-question-20261004-r3/result.json").read_text())
    assert result["status"] == "partial"
    identities = json.loads((ROOT / ".scratch/evidence/dc02-host-20261004-r3/identities.json").read_text())
    goal = GoalDefinition(
        actor_id=identities["actors"]["assessment"]["actor_id"], objective="完整回答经营问题",
        allowed_operations={"shop_data_center_agent": ["ask"]},
        criteria=[CompletionCriterion(tool_name="shop_data_center_agent", action="ask",
                                      path=["data", "answer"], operator="nonempty")],
    )
    specification = TaskSpec(task_id=result["data"]["task_id"], tool_name="shop_data_center_agent",
                             action="ask")
    task = {"task_id": specification.task_id, "spec": specification.model_dump(mode="json"),
            "status": "completed", "result": result}
    assert not completion_check(goal, [task])[0]
