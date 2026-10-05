import json
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import sql
from sqlalchemy import create_engine

from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
)

FIXED_INPUTS = Path(".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json")


@pytest.fixture
def actors():
    return json.loads(FIXED_INPUTS.read_text())["accounts"]


@pytest.fixture
def store(actors):
    from agent_cluster.store import ClusterStore

    schema = "clu_test_" + uuid4().hex
    engine = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop_test")
    with engine.begin() as connection:
        connection.connection.driver_connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    engine.dispose()
    engine = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop_test",
                           connect_args={"options": f"-csearch_path={schema}"})
    instance = ClusterStore(engine)
    instance.initialize()
    yield instance
    engine.dispose()


def goal(actors):
    return GoalDefinition(actor_id=actors["customer"]["actor_id"], objective="查询本人订单",
                          allowed_operations={"shop_customer_agent": ["orders"]},
                          criteria=[CompletionCriterion(tool_name="shop_customer_agent",
                                                        path=["data", "orders"], operator="exists")])


def test_goal_owner_and_persistence(store, actors):
    from agent_cluster.store import ClusterStore

    definition = goal(actors)
    store.create_goal(definition)
    restored = ClusterStore(store.engine).get_goal(definition.goal_id, definition.actor_id)
    assert restored == definition
    with pytest.raises(PermissionError):
        store.get_goal(definition.goal_id, actors["other"]["actor_id"])
    assert store.list_goals(actors["other"]["actor_id"]) == []


def test_event_deduplication_is_persistent_and_content_sensitive(store, actors):
    definition = goal(actors)
    store.create_goal(definition)
    event = TriggerEvent(event_id="source-ready", goal_id=definition.goal_id,
                         actor_id=definition.actor_id, payload={"version": 1})
    first = store.trigger(event)
    second = store.trigger(event)
    assert first.run_id == second.run_id
    assert len(store.list_runs(definition.goal_id, definition.actor_id)) == 1
    with pytest.raises(ValueError, match="内容"):
        store.trigger(event.model_copy(update={"payload": {"version": 2}}))


def test_goal_controls_stop_new_events_and_preserve_history(store, actors):
    definition = goal(actors)
    store.create_goal(definition)
    event = TriggerEvent(event_id="manual-one", goal_id=definition.goal_id, actor_id=definition.actor_id)
    run = store.trigger(event)
    store.control_goal(definition.goal_id, definition.actor_id, "pause")
    assert store.get_run(run.run_id, definition.actor_id).status == "paused"
    with pytest.raises(ValueError, match="暂停"):
        store.trigger(event.model_copy(update={"event_id": "manual-two"}))
    store.control_goal(definition.goal_id, definition.actor_id, "resume")
    assert store.get_run(run.run_id, definition.actor_id).status == "queued"
    store.control_goal(definition.goal_id, definition.actor_id, "cancel")
    assert store.get_run(run.run_id, definition.actor_id).status == "cancelled"
    assert len(store.list_runs(definition.goal_id, definition.actor_id)) == 1


def test_plan_versions_authority_and_request_ids(store, actors):
    definition = goal(actors)
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(event_id="manual", goal_id=definition.goal_id,
                                   actor_id=definition.actor_id))
    plan = TaskPlan(tasks=[TaskSpec(task_id="orders", tool_name="shop_customer_agent", action="orders")])
    store.save_plan(run.run_id, definition.actor_id, plan, expected_version=0)
    task = store.tasks(run.run_id, definition.actor_id)[0]
    store.save_plan(run.run_id, definition.actor_id, plan, expected_version=1)
    assert store.tasks(run.run_id, definition.actor_id)[0]["request_id"] == task["request_id"]
    with pytest.raises(ValueError, match="版本"):
        store.save_plan(run.run_id, definition.actor_id, plan, expected_version=0)
    forbidden = TaskPlan(tasks=[TaskSpec(task_id="forbidden", tool_name="shop_customer_agent", action="refund")])
    with pytest.raises(PermissionError):
        store.save_plan(run.run_id, definition.actor_id, forbidden, expected_version=2)


def test_history_survives_reopening_without_message_changes(store, actors):
    from agent_cluster.store import ClusterStore

    definition = goal(actors)
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(event_id="manual", goal_id=definition.goal_id,
                                   actor_id=definition.actor_id))
    message = {"role": "user", "content": definition.objective}
    store.append_message(run.run_id, definition.actor_id, message)
    restored = ClusterStore(store.engine).history(run.run_id, definition.actor_id)
    assert restored == [message]


async def test_rejected_real_plan_preserves_tasks_and_reports_tool_result(store, tmp_path):
    from shop.cluster import ShopCluster
    from shop.config import Settings
    from sqlalchemy.orm import sessionmaker

    evidence = Path(".scratch/evidence/clu06-remaining-probe-20261003-r1/CM02-1")
    definition = GoalDefinition.model_validate_json((evidence / "goal.json").read_text())
    definition = definition.model_copy(update={"goal_id": str(uuid4())})
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(event_id="real-plan-conflict", goal_id=definition.goal_id,
                                   actor_id=definition.actor_id))
    original = json.loads((evidence / "tasks.json").read_text())
    store.save_plan(run.run_id, run.actor_id,
                    TaskPlan(tasks=[TaskSpec.model_validate(task["spec"]) for task in original]),
                    expected_version=0)
    saved_tasks = store.tasks(run.run_id, run.actor_id)
    history = json.loads((evidence / "history.json").read_text())
    rejected_message = history[-1]
    assert rejected_message["role"] == "assistant"
    store.append_message(run.run_id, run.actor_id, rejected_message)
    cluster = ShopCluster(Settings(cluster_config_file=Path(".scratch/cluster-live-config.json"),
                                   cluster_notes_dir=tmp_path / "notes"),
                          store.engine, sessionmaker(store.engine, expire_on_commit=False))
    await cluster.engine._pending_tools(run)
    result = store.history(run.run_id, run.actor_id)[-1]
    assert result["tool_call_id"] == rejected_message["tool_calls"][0]["id"]
    output = json.loads(result["content"])
    assert output["status"] == "rejected"
    assert "原任务编号" in output["error"]["message"]
    assert store.tasks(run.run_id, run.actor_id) == saved_tasks
    assert store.get_run(run.run_id, run.actor_id).plan_version == 1
    await cluster.engine._pending_tools(run)
    assert len(store.history(run.run_id, run.actor_id)) == 2

    next_plan = TaskPlan(tasks=[TaskSpec(task_id="new-search", tool_name="shop_materials_search",
                                       action="search", payload={"limit": 1})])
    accepted = await cluster.engine._model_tool(run, "submit_plan", next_plan.model_dump(mode="json"),
                                               "registered-after-rejection")
    assert accepted["registered"] == ["new-search"]
    assert len(store.tasks(run.run_id, run.actor_id)) == len(saved_tasks) + 1
