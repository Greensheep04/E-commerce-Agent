import asyncio
import json
import os
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from shop.cluster import ShopCluster
from shop.config import Settings
from shop.models import Base
from shop.runtime_models import HarnessTask, ProviderRequest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema

from agent_cluster.engine import bind_payload
from agent_cluster.mcp_client import connect
from agent_cluster.store import TaskRecord
from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    InputBinding,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
)

ROOT = Path(__file__).resolve().parents[2]
CLUSTER_CONFIG = Path(os.environ.get(
    "CLU05_CLUSTER_CONFIG", str(ROOT / ".scratch/cluster-live-config.json"),
)).resolve(strict=True)
MAIN_DATABASE_URL = "postgresql+psycopg://shop@127.0.0.1:55432/shop"
ACTOR_ID = "291eeb2a-094f-4dba-ae90-efcd9222cc65"
HARNESS_TASK_ID = "75e541e3-2201-4b99-b78d-72b657b0a453"
UNKNOWN_REQUEST_ID = "78f38fe9-b3dd-49ff-8d1b-c21e77cb0595"


@pytest.fixture
def db_session():
    database_url = os.environ["SHOP_TEST_DATABASE_URL"]
    schema = "unknown_" + uuid4().hex
    admin_engine = create_engine(database_url)
    with admin_engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"))
        connection.execute(CreateSchema(schema))
    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    import shop.assessment_models  # noqa: F401
    import shop.cluster_models  # noqa: F401
    import shop.customer_models  # noqa: F401
    import shop.customer_runtime_models  # noqa: F401
    import shop.integration_models  # noqa: F401

    Base.metadata.create_all(engine)
    maker = sessionmaker(engine, expire_on_commit=False)
    try:
        with maker() as session:
            session.info["test_database_url"] = str(
                engine.url.set(query={"options": f"-csearch_path={schema},public"}))
            yield session
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()


def cluster_for_test(db_session, actor_id):
    notes_dir = ROOT / ".scratch/runtime/unknown-results" / uuid4().hex
    settings = Settings(
        database_url=db_session.info["test_database_url"],
        cluster_config_file=CLUSTER_CONFIG,
        cluster_notes_dir=notes_dir,
        provider_env_file=None,
    )
    cluster = ShopCluster(settings, db_session.bind,
                          sessionmaker(db_session.bind, expire_on_commit=False))
    assert actor_id in cluster.configuration.actors
    cluster.store.initialize()
    return cluster


def test_real_harness_show_unknown_stops_cluster_and_preserves_full_result(db_session):
    main_engine = create_engine(MAIN_DATABASE_URL, execution_options={"postgresql_readonly": True})
    try:
        with Session(main_engine) as session:
            source_task = session.get(HarnessTask, HARNESS_TASK_ID)
            source_request = session.get(ProviderRequest, UNKNOWN_REQUEST_ID)
            assert source_task is not None
            assert source_task.stop_reason == "time_limit"
            assert source_request is not None
            assert source_request.status == "unknown"
            assert source_request.task_id == HARNESS_TASK_ID
    finally:
        main_engine.dispose()

    cluster = cluster_for_test(db_session, ACTOR_ID)
    goal = GoalDefinition(
        actor_id=ACTOR_ID,
        objective="读取现有Harness任务并保留未知请求结果",
        allowed_operations={"shop_harness_agent": ["show"]},
        criteria=[CompletionCriterion(tool_name="shop_harness_agent", action="show",
                                      path=["data", "status"], operator="nonempty")],
        max_decisions=1,
    )
    cluster.store.create_goal(goal)
    run = cluster.store.trigger(TriggerEvent(
        event_id="unknown-harness-show-" + uuid4().hex,
        goal_id=goal.goal_id,
        actor_id=ACTOR_ID,
    ))
    cluster.store.save_plan(run.run_id, ACTOR_ID, TaskPlan(tasks=[TaskSpec(
        task_id="show-stopped-harness",
        tool_name="shop_harness_agent",
        action="show",
        payload={"task_id": HARNESS_TASK_ID},
    )]), expected_version=0)

    requests_before = db_session.scalar(select(func.count()).select_from(ProviderRequest).where(
        ProviderRequest.price_basis["cluster_goal_id"].astext == goal.goal_id))
    assert requests_before == 0

    cluster.store.set_run(run.run_id, ACTOR_ID, status="running")

    async def execute_real_show():
        async with connect(cluster.command_for_actor(ACTOR_ID)) as client:
            cluster.engine.clients[run.run_id] = client
            try:
                return await cluster.engine._execute_node({
                    "run_id": run.run_id,
                    "actor_id": ACTOR_ID,
                    "proceed": True,
                })
            finally:
                cluster.engine.clients.pop(run.run_id)

    result = asyncio.run(execute_real_show())

    stored_run = cluster.store.get_run(run.run_id, ACTOR_ID)
    task = cluster.store.tasks(run.run_id, ACTOR_ID)[0]
    assert result["proceed"] is False
    operation = task["result"]
    assert operation["status"] == "stopped"
    assert operation["data"]["status"] == "stopped"
    assert operation["data"]["stop_reason"] == "time_limit"
    unknown_requests = [item for item in operation["data"]["requests"]
                        if item["status"] == "unknown"]
    assert any(item["id"] == UNKNOWN_REQUEST_ID for item in unknown_requests)

    history = cluster.store.history(run.run_id, ACTOR_ID)
    history_value = json.loads(next(message["content"] for message in history
                                    if message.get("role") == "user"))
    assert history_value["result"] == operation
    note_id = str(uuid5(NAMESPACE_URL, task["request_id"]))
    note = next(reference for reference in cluster.notes.index(
        goal_id=goal.goal_id, actor_id=ACTOR_ID) if reference.note_id == note_id)
    assert json.loads(cluster.notes.read(note, goal_id=goal.goal_id,
                                         actor_id=ACTOR_ID)) == history_value
    requests_after = db_session.scalar(select(func.count()).select_from(ProviderRequest).where(
        ProviderRequest.price_basis["cluster_goal_id"].astext == goal.goal_id))
    assert requests_after == requests_before
    assert history_value["status"] == "unknown"
    assert stored_run.decision_count == 0
    assert task["status"] == "unknown"
    assert stored_run.status == "unknown"


@pytest.fixture
def stopped_harness_result(db_session, tmp_path):
    task_id = "8b43531f-7306-4935-a0f8-abe0af8df690"
    main_engine = create_engine(MAIN_DATABASE_URL, execution_options={"postgresql_readonly": True})
    try:
        with Session(main_engine) as session:
            source = session.get(HarnessTask, task_id)
            assert source.status == "stopped" and source.owner_id == ACTOR_ID
            requests_before = [(row.id, row.status, row.charged_cny, row.reservation_cny)
                for row in session.scalars(select(ProviderRequest).where(
                    ProviderRequest.task_id == task_id).order_by(ProviderRequest.id))]
            assert all(row[1] != "unknown" for row in requests_before)
        cluster = cluster_for_test(db_session, ACTOR_ID)
        goal = GoalDefinition(
            actor_id=ACTOR_ID, objective="检查真实任务的业务产物完成条件",
            allowed_operations={"shop_harness_agent": ["show"]},
            criteria=[CompletionCriterion(tool_name="shop_harness_agent", action="show",
                                          path=["data", "status"], operator="nonempty")],
            max_decisions=1,
        )
        cluster.store.create_goal(goal)
        run = cluster.store.trigger(TriggerEvent(event_id="stopped-result-" + uuid4().hex,
                                                goal_id=goal.goal_id, actor_id=ACTOR_ID))
        cluster.store.save_plan(run.run_id, ACTOR_ID, TaskPlan(tasks=[TaskSpec(
            task_id="show-stopped", tool_name="shop_harness_agent", action="show",
            payload={"task_id": task_id})]), expected_version=0)
        cluster.store.set_run(run.run_id, ACTOR_ID, status="running")

        async def execute_real_show():
            async with connect(cluster.command_for_actor(ACTOR_ID)) as client:
                cluster.engine.clients[run.run_id] = client
                try:
                    return await cluster.engine._execute_node({"run_id": run.run_id,
                                                               "actor_id": ACTOR_ID, "proceed": True})
                finally:
                    cluster.engine.clients.pop(run.run_id)

        asyncio.run(execute_real_show())
        tasks = cluster.store.tasks(run.run_id, ACTOR_ID)
        stored_run = cluster.store.get_run(run.run_id, ACTOR_ID)
        (tmp_path / "stopped-business-result.json").write_text(json.dumps({
            "goal": goal.model_dump(mode="json"), "run": stored_run.model_dump(mode="json"),
            "tasks": tasks}, ensure_ascii=False, indent=2))
        assert tasks[0]["result"]["status"] == "stopped"
        assert tasks[0]["result"]["data"]["stop_reason"] == "error"
        with Session(main_engine) as session:
            requests_after = [(row.id, row.status, row.charged_cny, row.reservation_cny)
                for row in session.scalars(select(ProviderRequest).where(
                    ProviderRequest.task_id == task_id).order_by(ProviderRequest.id))]
        assert requests_before == requests_after
        assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
        yield {"run": stored_run, "tasks": tasks}
    finally:
        main_engine.dispose()


def test_real_stopped_harness_cannot_satisfy_goal_completion(stopped_harness_result):
    assert stopped_harness_result["run"].status != "completed"


def test_real_stopped_harness_cannot_supply_input_binding(stopped_harness_result):
    source = stopped_harness_result["tasks"][0]
    spec = TaskSpec(task_id="consume-stopped-result", tool_name="shop_harness_agent", action="show",
                    dependencies=[source["task_id"]], bindings=[InputBinding(
                        source_task_id=source["task_id"], source_path=["data", "task_id"],
                        target_path=["task_id"])])
    with pytest.raises(ValueError, match="业务结果尚未成功"):
        bind_payload(spec, {source["task_id"]: source})


def test_real_missing_poster_roles_waits_without_another_coordinator_request(db_session):
    source_tasks = json.loads((ROOT / ".scratch/evidence/clu-formal-20261003-r1/CN01-1/tasks.json").read_text())
    source = next(task for task in source_tasks if task["result"]
                  and task["result"]["status"] == "waiting_agents")
    job_id = source["result"]["data"]["job"]["job_id"]
    cluster = cluster_for_test(db_session, ACTOR_ID)
    goal = GoalDefinition(
        actor_id=ACTOR_ID, objective="读取完整海报任务的业务角色等待状态",
        allowed_operations={"shop_poster_agent": ["show"]},
        criteria=[CompletionCriterion(tool_name="shop_poster_agent", action="show",
                                      path=["data", "job", "status"], operator="nonempty")],
    )
    cluster.store.create_goal(goal)
    run = cluster.store.trigger(TriggerEvent(event_id="missing-poster-roles-" + uuid4().hex,
                                             goal_id=goal.goal_id, actor_id=ACTOR_ID))
    cluster.store.save_plan(run.run_id, ACTOR_ID, TaskPlan(tasks=[TaskSpec(
        task_id="show-waiting-poster", tool_name="shop_poster_agent", action="show",
        payload={"job_id": job_id})]), expected_version=0)
    cluster.store.set_run(run.run_id, ACTOR_ID, status="running")

    async def execute_real_show():
        async with connect(cluster.command_for_actor(ACTOR_ID)) as client:
            cluster.engine.clients[run.run_id] = client
            try:
                return await cluster.engine._execute_node({"run_id": run.run_id,
                                                           "actor_id": ACTOR_ID, "proceed": True})
            finally:
                cluster.engine.clients.pop(run.run_id)

    result = asyncio.run(execute_real_show())
    task = cluster.store.tasks(run.run_id, ACTOR_ID)[0]
    assert task["result"]["status"] == "waiting_agents"
    assert result["proceed"] is False
    assert cluster.store.get_run(run.run_id, ACTOR_ID).status == "waiting_dependencies"
    assert cluster.store.get_run(run.run_id, ACTOR_ID).decision_count == 0
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0


def test_unknown_run_rejects_task_resend(db_session):
    cluster = cluster_for_test(db_session, ACTOR_ID)
    goal = GoalDefinition(
        actor_id=ACTOR_ID,
        objective="保留未知运行并阻止重新发送业务任务",
        allowed_operations={"shop_harness_agent": ["show"]},
        criteria=[CompletionCriterion(tool_name="shop_harness_agent", action="show",
                                      path=["data", "status"], operator="nonempty")],
    )
    cluster.store.create_goal(goal)
    run = cluster.store.trigger(TriggerEvent(
        event_id="unknown-run-no-resend-" + uuid4().hex,
        goal_id=goal.goal_id,
        actor_id=ACTOR_ID,
    ))
    cluster.store.save_plan(run.run_id, ACTOR_ID, TaskPlan(tasks=[TaskSpec(
        task_id="stopped-task",
        tool_name="shop_harness_agent",
        action="show",
        payload={"task_id": HARNESS_TASK_ID},
    )]), expected_version=0)
    cluster.store.set_run(run.run_id, ACTOR_ID, status="unknown")

    result = asyncio.run(cluster.engine.run(run.run_id, ACTOR_ID))

    assert result.status == "unknown"
    task = cluster.store.tasks(run.run_id, ACTOR_ID)[0]
    with pytest.raises(ValueError, match="运行停止后禁止新增调用"):
        cluster.store.set_task(run.run_id, ACTOR_ID, task["task_id"], status="sent")


def test_setting_unknown_task_stops_active_run(db_session):
    cluster = cluster_for_test(db_session, ACTOR_ID)
    goal = GoalDefinition(
        actor_id=ACTOR_ID,
        objective="保存未知任务结果并停止运行",
        allowed_operations={"shop_harness_agent": ["show"]},
        criteria=[CompletionCriterion(tool_name="shop_harness_agent", action="show",
                                      path=["data", "status"], operator="nonempty")],
    )
    cluster.store.create_goal(goal)
    run = cluster.store.trigger(TriggerEvent(
        event_id="unknown-task-stops-run-" + uuid4().hex,
        goal_id=goal.goal_id,
        actor_id=ACTOR_ID,
    ))
    cluster.store.save_plan(run.run_id, ACTOR_ID, TaskPlan(tasks=[TaskSpec(
        task_id="unknown-result",
        tool_name="shop_harness_agent",
        action="show",
        payload={"task_id": HARNESS_TASK_ID},
    )]), expected_version=0)
    cluster.store.set_run(run.run_id, ACTOR_ID, status="running")

    task = cluster.store.tasks(run.run_id, ACTOR_ID)[0]
    cluster.store.set_task(run.run_id, ACTOR_ID, task["task_id"], status="unknown",
                           error="提供商结果未知")

    assert cluster.store.get_run(run.run_id, ACTOR_ID).status == "unknown"


def test_persisted_unknown_task_stops_running_run_without_result_or_model_call(db_session):
    cluster = cluster_for_test(db_session, ACTOR_ID)
    goal = GoalDefinition(
        actor_id=ACTOR_ID,
        objective="恢复持久化的未知任务状态",
        allowed_operations={"shop_harness_agent": ["show"]},
        criteria=[CompletionCriterion(tool_name="shop_harness_agent", action="show",
                                      path=["data", "status"], operator="nonempty")],
    )
    cluster.store.create_goal(goal)
    run = cluster.store.trigger(TriggerEvent(
        event_id="persisted-unknown-task-" + uuid4().hex,
        goal_id=goal.goal_id,
        actor_id=ACTOR_ID,
    ))
    cluster.store.save_plan(run.run_id, ACTOR_ID, TaskPlan(tasks=[TaskSpec(
        task_id="persisted-unknown",
        tool_name="shop_harness_agent",
        action="show",
        payload={"task_id": HARNESS_TASK_ID},
    )]), expected_version=0)
    cluster.store.set_run(run.run_id, ACTOR_ID, status="running")
    persisted_task = db_session.get(TaskRecord, (run.run_id, "persisted-unknown"))
    persisted_task.status = "unknown"
    persisted_task.result = None
    persisted_task.error = "进程重启时已记录未知结果"
    db_session.commit()

    result = asyncio.run(cluster.engine._execute_node({
        "run_id": run.run_id,
        "actor_id": ACTOR_ID,
        "proceed": True,
    }))

    assert result["proceed"] is False
    stored_run = cluster.store.get_run(run.run_id, ACTOR_ID)
    assert stored_run.status == "unknown"
    assert stored_run.decision_count == 0
    assert cluster.store.tasks(run.run_id, ACTOR_ID)[0]["result"] is None
