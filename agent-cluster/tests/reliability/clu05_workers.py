from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import create_engine


def trigger_in_process(database_url, schema, event_document, barrier):
    from agent_cluster.store import ClusterStore
    from agent_cluster.types import TriggerEvent

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        store = ClusterStore(engine)
        barrier.wait(timeout=30)
        run = store.trigger(TriggerEvent.model_validate(event_document))
        return run.run_id
    finally:
        engine.dispose()


def update_goal_in_process(database_url, schema, goal_document, barrier):
    from agent_cluster.store import ClusterStore
    from agent_cluster.types import GoalDefinition

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        store = ClusterStore(engine)
        barrier.wait(timeout=30)
        try:
            value = store.update_goal(GoalDefinition.model_validate(goal_document), expected_revision=1)
        except ValueError as error:
            return {"status": "conflict", "message": str(error)}
        return {"status": "updated", "revision": value.revision, "objective": value.objective}
    finally:
        engine.dispose()


def save_plan_in_process(database_url, schema, run_id, actor_id, plan_document, barrier):
    from agent_cluster.store import ClusterStore
    from agent_cluster.types import TaskPlan

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        store = ClusterStore(engine)
        barrier.wait(timeout=30)
        try:
            value = store.save_plan(run_id, actor_id, TaskPlan.model_validate(plan_document), expected_version=0)
        except ValueError as error:
            return {"status": "conflict", "message": str(error)}
        return {"status": "saved", "plan_version": value.plan_version}
    finally:
        engine.dispose()


def schedule_due_in_process(database_url, schema, schedule_id, barrier):
    from agent_cluster.store import ClusterStore

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        store = ClusterStore(engine)
        trigger = IntervalTrigger(seconds=60, timezone="UTC")
        barrier.wait(timeout=30)
        run = store.schedule_due(schedule_id, trigger)
        return None if run is None else run.run_id
    finally:
        engine.dispose()


def hold_lease(database_url, schema, key, acquired, release):
    from agent_cluster.store import ClusterStore

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"})
    store = ClusterStore(engine)
    with store.lease(key) as held:
        if not held:
            raise AssertionError("持锁进程未获取互斥租约")
        acquired.set()
        if not release.wait(timeout=60):
            raise TimeoutError("测试进程未收到租约等待信号")


def read_task_in_process(database_url, schema, run_id, actor_id, task_id):
    from agent_cluster.store import ClusterStore

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        store = ClusterStore(engine)
        return next(task for task in store.tasks(run_id, actor_id) if task["task_id"] == task_id)
    finally:
        engine.dispose()


def read_decision_in_process(database_url, schema, run_id, actor_id, step_key):
    from agent_cluster.store import ClusterStore

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        store = ClusterStore(engine)
        return store.decision(run_id, actor_id, step_key)
    finally:
        engine.dispose()


def reserve_cluster_budget_in_process(database_url, schema, run_id, slot, barrier):
    from shop.budget import BudgetExceeded, BudgetService
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    try:
        budget = BudgetService(sessionmaker(engine, expire_on_commit=False), verification_scope="cluster")
        barrier.wait(timeout=30)
        try:
            request = budget.reserve(
                task_id=None,
                run_id=run_id,
                step_key=f"clu05-budget-{slot}",
                service="deepseek",
                model="deepseek-flash",
                request_body={"model": "deepseek-flash", "messages": [
                    {"role": "user", "content": f"CLU-05 concurrent budget reservation {slot}"}
                ]},
                input_limit=100,
                output_limit=1,
            )
        except BudgetExceeded as error:
            return {"status": "rejected", "reason": str(error)}
        return {"status": "reserved", "request_id": request.id,
                "reservation_cny": str(request.reservation_cny)}
    finally:
        engine.dispose()


def update_schedule_after_snapshot_in_process(database_url, schema, schedule_id, ready, proceed):
    from agent_cluster.runtime import schedule_trigger
    from agent_cluster.store import ClusterStore

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    try:
        store = ClusterStore(engine)
        current = next(item for item in store.schedules() if item.schedule_id == schedule_id)
        snapshot = current.model_dump(mode="json")
        ready.set()
        if not proceed.wait(timeout=60):
            raise TimeoutError("日程快照检查没有收到继续信号")
        result = store.schedule_due(schedule_id, schedule_trigger(current), expected_definition=snapshot)
        return {"snapshot": snapshot, "run_id": None if result is None else result.run_id}
    finally:
        engine.dispose()


def schedule_due_racing_with_cancel_in_process(database_url, schema, schedule_id, expected, barrier):
    from apscheduler.triggers.interval import IntervalTrigger

    from agent_cluster.store import ClusterStore

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    try:
        store = ClusterStore(engine)
        trigger = IntervalTrigger(seconds=expected["interval_seconds"], timezone=expected["timezone"])
        barrier.wait(timeout=30)
        run = store.schedule_due(schedule_id, trigger, expected_definition=expected)
        return None if run is None else run.run_id
    finally:
        engine.dispose()


def cancel_goal_in_process(database_url, schema, goal_id, actor_id, barrier):
    from agent_cluster.store import ClusterStore

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    try:
        store = ClusterStore(engine)
        barrier.wait(timeout=30)
        store.control_goal(goal_id, actor_id, "cancel")
        return store.goal_state(goal_id, actor_id)
    finally:
        engine.dispose()


def try_goal_lease_in_process(database_url, schema, goal_id):
    from agent_cluster.store import ClusterStore

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    try:
        store = ClusterStore(engine)
        with store.lease("goal:" + goal_id) as acquired:
            return bool(acquired)
    finally:
        engine.dispose()


def run_preplanned_business_in_process(
    database_url, schema, config_path, notes_dir, run_id, actor_id, ready
):
    import asyncio
    from pathlib import Path

    from shop.cluster import ShopCluster
    from shop.config import Settings
    from sqlalchemy.orm import sessionmaker

    from agent_cluster.mcp_client import connect
    from agent_cluster.store import digest

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    settings = Settings(database_url=database_url, cluster_config_file=Path(config_path),
                        cluster_notes_dir=Path(notes_dir))
    cluster = ShopCluster(settings, engine, sessionmaker(engine, expire_on_commit=False))
    try:
        store = cluster.store
        run = store.get_run(run_id, actor_id)
        goal = store.get_goal(run.goal_id, actor_id)
        task = store.tasks(run_id, actor_id)[0]
        with store.lease("goal:" + goal.goal_id) as acquired:
            if not acquired:
                return {"goal_lease_acquired": False, "run_id": run_id}

            async def execute():
                async with connect(cluster.command_for_actor(actor_id)) as client:
                    client.catalog.validate_goal(goal)
                    ready.set()
                    cluster.engine.clients[run_id] = client
                    try:
                        await cluster.engine._business_task(run, task, {task["task_id"]: task})
                    finally:
                        cluster.engine.clients.pop(run_id)

            asyncio.run(execute())
        completed = next(item for item in store.tasks(run_id, actor_id)
                         if item["task_id"] == task["task_id"])
        result = completed["result"]
        return {
            "goal_lease_acquired": True,
            "goal_id": goal.goal_id,
            "run_id": run_id,
            "task_id": task["task_id"],
            "request_id": task["request_id"],
            "task_status": completed["status"],
            "result_sha256": digest(result) if result is not None else None,
        }
    finally:
        engine.dispose()


def save_completed_business_result_then_exit(
    database_url, schema, config_path, notes_dir, run_id, actor_id, exit_code
):
    import asyncio
    import os
    from pathlib import Path

    from shop.cluster import ShopCluster
    from shop.config import Settings
    from sqlalchemy.orm import sessionmaker

    from agent_cluster.mcp_client import connect
    from agent_cluster.types import TaskSpec

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    settings = Settings(database_url=database_url, cluster_config_file=Path(config_path),
                        cluster_notes_dir=Path(notes_dir))
    cluster = ShopCluster(settings, engine, sessionmaker(engine, expire_on_commit=False))
    try:
        store = cluster.store
        run = store.get_run(run_id, actor_id)
        goal = store.get_goal(run.goal_id, actor_id)
        task = store.tasks(run_id, actor_id)[0]
        spec = TaskSpec.model_validate(task["spec"])
        with store.lease("goal:" + goal.goal_id) as goal_acquired:
            if not goal_acquired:
                raise RuntimeError("恢复检查没有获取目标lease")
            with store.lease("business-slot:0") as slot_acquired:
                if not slot_acquired:
                    raise RuntimeError("恢复检查没有获取业务槽位")

                async def execute():
                    async with connect(cluster.command_for_actor(actor_id)) as client:
                        client.catalog.validate_goal(goal)
                        arguments = client.catalog.arguments(spec, spec.payload, task["request_id"], [])
                        store.set_task(run_id, actor_id, task["task_id"], status="sent", arguments=arguments)
                        result = await client.invoke(spec, arguments)
                        store.set_task(run_id, actor_id, task["task_id"], status="completed",
                                       result=result.model_dump(mode="json"))

                asyncio.run(execute())
    finally:
        engine.dispose()
    os._exit(exit_code)


def recover_run_in_process(database_url, schema, config_path, notes_dir, run_id, actor_id):
    import asyncio
    from pathlib import Path

    from shop.cluster import ShopCluster
    from shop.config import Settings
    from sqlalchemy.orm import sessionmaker

    from agent_cluster.store import digest

    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    settings = Settings(database_url=database_url, cluster_config_file=Path(config_path),
                        cluster_notes_dir=Path(notes_dir))
    cluster = ShopCluster(settings, engine, sessionmaker(engine, expire_on_commit=False))
    try:
        run = asyncio.run(cluster.engine.run(run_id, actor_id))
        tasks = cluster.store.tasks(run_id, actor_id)
        history = cluster.store.history(run_id, actor_id)
        notes = cluster.notes.index(goal_id=run.goal_id, actor_id=actor_id)
        return {
            "run_status": run.status,
            "decision_count": run.decision_count,
            "task_statuses": [task["status"] for task in tasks],
            "history_count": len(history),
            "history_roles": [message["role"] for message in history],
            "history_sha256": [digest(message) for message in history],
            "note_references": [{"note_id": note.note_id, "version": note.version,
                                 "sha256": note.sha256, "topic": note.topic,
                                 "source": note.source} for note in notes],
        }
    finally:
        engine.dispose()
