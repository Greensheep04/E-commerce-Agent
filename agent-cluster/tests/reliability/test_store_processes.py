import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from apscheduler.triggers.interval import IntervalTrigger
from clu05_workers import (
    cancel_goal_in_process,
    hold_lease,
    read_decision_in_process,
    read_task_in_process,
    reserve_cluster_budget_in_process,
    save_plan_in_process,
    schedule_due_in_process,
    schedule_due_racing_with_cancel_in_process,
    trigger_in_process,
    update_goal_in_process,
    update_schedule_after_snapshot_in_process,
)
from conftest import save_observation
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    ScheduleDefinition,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
    now,
)


def make_goal(accounts, objective="查询本人订单", budget_cny=Decimal("30")):
    return GoalDefinition(
        actor_id=accounts["customer"]["actor_id"],
        objective=objective,
        allowed_operations={"shop_customer_agent": ["orders"]},
        criteria=[CompletionCriterion(tool_name="shop_customer_agent", path=["data", "orders"], operator="exists")],
        budget_cny=budget_cny,
    )


def race(worker, database, argument_lists):
    context = multiprocessing.get_context("spawn")
    with context.Manager() as manager:
        barrier = manager.Barrier(len(argument_lists) + 1)
        with ProcessPoolExecutor(max_workers=len(argument_lists), mp_context=context) as executor:
            futures = [executor.submit(
                worker,
                database["database_url"],
                database["schema"],
                *arguments,
                barrier,
            ) for arguments in argument_lists]
            barrier.wait(timeout=30)
            return [future.result(timeout=60) for future in futures]


def test_event_deduplication_is_atomic_across_processes(database, accounts):
    store = database["store"]
    definition = make_goal(accounts)
    store.create_goal(definition)
    event = TriggerEvent(
        event_id="clu05-event-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
        payload={"case_id": "CC02"},
    )
    event_document = event.model_dump(mode="json")
    run_ids = race(trigger_in_process, database, [[event_document], [event_document]])
    assert run_ids[0] == run_ids[1]
    assert len(store.list_runs(definition.goal_id, definition.actor_id)) == 1
    with pytest.raises(ValueError, match="内容"):
        store.trigger(event.model_copy(update={"payload": {"case_id": "CC03"}}))


def test_goal_revision_rejects_a_competing_stale_update(database, accounts):
    store = database["store"]
    definition = make_goal(accounts)
    store.create_goal(definition)
    first = definition.model_copy(update={"objective": "查询本人最近订单"}).model_dump(mode="json")
    second = definition.model_copy(update={"objective": "查询本人订单明细"}).model_dump(mode="json")
    outcomes = race(update_goal_in_process, database, [[first], [second]])
    assert sorted(outcome["status"] for outcome in outcomes) == ["conflict", "updated"]
    current = store.get_goal(definition.goal_id, definition.actor_id)
    assert current.revision == 2
    assert current.objective in {"查询本人最近订单", "查询本人订单明细"}


def test_plan_version_rejects_a_competing_stale_plan(database, accounts):
    store = database["store"]
    definition = make_goal(accounts)
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(
        event_id="clu05-plan-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
    ))
    plans = [
        TaskPlan(tasks=[TaskSpec(task_id="orders-first", tool_name="shop_customer_agent", action="orders")]),
        TaskPlan(tasks=[TaskSpec(task_id="orders-second", tool_name="shop_customer_agent", action="orders")]),
    ]
    outcomes = race(
        save_plan_in_process,
        database,
        [[run.run_id, definition.actor_id, plan.model_dump(mode="json")] for plan in plans],
    )
    assert sorted(outcome["status"] for outcome in outcomes) == ["conflict", "saved"]
    current = store.get_run(run.run_id, definition.actor_id)
    assert current.plan_version == 1
    tasks = store.tasks(run.run_id, definition.actor_id)
    assert len(tasks) == 1
    assert tasks[0]["task_id"] in {"orders-first", "orders-second"}


def test_cancelled_goal_excludes_run_and_rejects_new_dispatch(database, accounts):
    store = database["store"]
    definition = make_goal(accounts)
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(
        event_id="clu05-cancel-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
    ))
    store.save_plan(
        run.run_id,
        definition.actor_id,
        TaskPlan(tasks=[TaskSpec(task_id="orders", tool_name="shop_customer_agent", action="orders")]),
        expected_version=0,
    )
    store.control_goal(definition.goal_id, definition.actor_id, "cancel")
    assert store.get_run(run.run_id, definition.actor_id).status == "cancelled"
    assert all(item.run_id != run.run_id for item in store.runnable())
    with pytest.raises(ValueError, match="调度"):
        store.save_plan(
            run.run_id,
            definition.actor_id,
            TaskPlan(tasks=[TaskSpec(task_id="orders-next", tool_name="shop_customer_agent", action="orders")]),
            expected_version=1,
        )
    with pytest.raises(ValueError, match="停止"):
        store.set_task(run.run_id, definition.actor_id, "orders", status="sent")
    assert store.tasks(run.run_id, definition.actor_id)[0]["status"] == "queued"


def test_task_request_id_and_arguments_survive_a_new_process(database, accounts, fixed_inputs, fixed_sources):
    store = database["store"]
    order = next(item for item in fixed_sources["orders"]["customer"]["orders"]
                 if item["order_number"] == "SO74480")
    case = next(item for item in fixed_inputs["fixture"]["cases"] if item["id"] == "CC02")
    definition = make_goal(accounts, case["objective"])
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(
        event_id="clu05-request-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
    ))
    store.save_plan(
        run.run_id,
        definition.actor_id,
        TaskPlan(tasks=[TaskSpec(
            task_id="order-details",
            tool_name="shop_customer_agent",
            action="orders",
            payload={"order_number": order["order_number"]},
        )]),
        expected_version=0,
    )
    original = store.tasks(run.run_id, definition.actor_id)[0]
    arguments = {"request_id": original["request_id"], "payload": {"order_number": order["order_number"]}}
    store.set_task(run.run_id, definition.actor_id, original["task_id"], status="queued", arguments=arguments)
    restored = read_task_in_process(
        database["database_url"], database["schema"], run.run_id, definition.actor_id, original["task_id"]
    )
    assert restored["request_id"] == original["request_id"]
    assert restored["arguments"] == arguments
    with pytest.raises(ValueError, match="不能改变"):
        store.set_task(
            run.run_id,
            definition.actor_id,
            original["task_id"],
            status="queued",
            arguments={"request_id": original["request_id"], "payload": {"order_number": "SO74373"}},
        )


def test_coordinator_request_survives_a_new_process(database, accounts, fixed_inputs):
    store = database["store"]
    case = next(item for item in fixed_inputs["fixture"]["cases"] if item["id"] == "CC02")
    objective = case["objective"]
    definition = make_goal(accounts, objective)
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(
        event_id="clu05-decision-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
    ))
    step_key = "coordinator-1"
    request = {"messages": [{"role": "user", "content": objective}], "model": "deepseek-flash"}
    assert store.prepare_decision(run.run_id, definition.actor_id, step_key, request) == (request, None)
    restored = read_decision_in_process(
        database["database_url"], database["schema"], run.run_id, definition.actor_id, step_key
    )
    assert restored == (request, None)


def test_commit_decision_persists_a_real_response_once_across_processes(
    database, accounts, real_deepseek_response
):
    from agent_cluster.store import digest

    store = database["store"]
    definition = make_goal(accounts)
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(
        event_id="clu05-real-response-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
    ))
    step_key = "decision-0"
    request = real_deepseek_response["request"]
    response = real_deepseek_response["response"]
    store.prepare_decision(run.run_id, definition.actor_id, step_key, request)
    assert store.get_run(run.run_id, definition.actor_id).decision_count == 0
    assert store.history(run.run_id, definition.actor_id) == []
    store.commit_decision(run.run_id, definition.actor_id, step_key, response)
    store.commit_decision(run.run_id, definition.actor_id, step_key, response)
    restored = read_decision_in_process(
        database["database_url"], database["schema"], run.run_id, definition.actor_id, step_key
    )
    assert digest(restored[0]) == real_deepseek_response["source"]["request_sha256"]
    assert digest(restored[1]) == real_deepseek_response["source"]["response_sha256"]
    history = store.history(run.run_id, definition.actor_id)
    assert len(history) == 1
    assert digest(history[0]) == digest(response["choices"][0]["message"])
    assert store.get_run(run.run_id, definition.actor_id).decision_count == 1


def test_finalize_tool_rolls_back_run_update_when_history_sequence_overflows(database, accounts):
    from sqlalchemy.exc import DBAPIError

    from agent_cluster.store import MessageRecord

    store = database["store"]
    definition = make_goal(accounts)
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(
        event_id="clu05-finalize-rollback-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
    ))
    call_id = "clu05-finalize-rollback-" + uuid4().hex
    boundary_message = {"role": "user", "content": "已保存的最高历史序号"}
    with store.maker.begin() as session:
        session.add(MessageRecord(run_id=run.run_id, sequence=2147483647, document=boundary_message))

    with pytest.raises(DBAPIError):
        store.finalize_tool(
            run.run_id,
            definition.actor_id,
            call_id,
            {"completed": True},
            status="completed",
            summary="结果写入检查",
        )

    assert store.get_run(run.run_id, definition.actor_id).status == "queued"
    history = store.history(run.run_id, definition.actor_id)
    assert history == [boundary_message]
    save_observation("finalize-tool-rollback.json", {
        "goal_id": definition.goal_id,
        "run_id": run.run_id,
        "call_id": call_id,
        "preexisting_history_sequence": 2147483647,
        "attempted_history_sequence": 2147483648,
        "run_status_after_rollback": store.get_run(run.run_id, definition.actor_id).status,
        "history_after_rollback": history,
    })


def test_finalize_tool_persists_run_change_and_tool_result_once(database, accounts):
    import json

    store = database["store"]
    definition = make_goal(accounts)
    store.create_goal(definition)
    run = store.trigger(TriggerEvent(
        event_id="clu05-finalize-success-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
    ))
    call_id = "clu05-finalize-success-" + uuid4().hex
    result = {"completed": True, "checks": [{"path": ["data", "orders"], "ok": True}]}

    first = store.finalize_tool(
        run.run_id,
        definition.actor_id,
        call_id,
        result,
        status="completed",
        summary="本人订单检查已完成",
    )
    repeated = store.finalize_tool(
        run.run_id,
        definition.actor_id,
        call_id,
        {"completed": False},
        status="failed",
        summary="重复调用不能覆盖原结果",
    )

    current = store.get_run(run.run_id, definition.actor_id)
    history = store.history(run.run_id, definition.actor_id)
    assert first == result
    assert repeated == result
    assert current.status == "completed"
    assert current.summary == "本人订单检查已完成"
    assert len(history) == 1
    assert history[0]["role"] == "tool"
    assert history[0]["tool_call_id"] == call_id
    assert json.loads(history[0]["content"]) == result
    save_observation("finalize-tool-success.json", {
        "goal_id": definition.goal_id,
        "run_id": run.run_id,
        "call_id": call_id,
        "first_result": first,
        "repeated_result": repeated,
        "run_status": current.status,
        "summary": current.summary,
        "history_count": len(history),
        "history_tool_call_ids": [message["tool_call_id"] for message in history],
    })


def test_advisory_lease_is_released_after_holder_process_termination(database):
    context = multiprocessing.get_context("spawn")
    acquired = context.Event()
    release = context.Event()
    key = "clu05-lease-" + uuid4().hex
    process = context.Process(
        target=hold_lease,
        args=(database["database_url"], database["schema"], key, acquired, release),
    )
    process.start()
    try:
        assert acquired.wait(timeout=30)
        with database["store"].lease(key) as second_lease:
            assert second_lease is False
    finally:
        if process.is_alive():
            process.terminate()
        process.join(timeout=30)
    assert not process.is_alive()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        with database["store"].lease(key) as next_lease:
            if next_lease:
                break
        time.sleep(0.05)
    else:
        pytest.fail("持锁进程终止后PostgreSQL仍未释放会话级互斥锁")


def test_schedule_coalesces_missed_periods_and_serializes_competing_processes(database, accounts):
    from agent_cluster.store import EventRecord, ScheduleRecord

    store = database["store"]
    definition = make_goal(accounts)
    store.create_goal(definition)
    schedule = ScheduleDefinition(
        schedule_id="clu05-schedule-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
        interval_seconds=60,
        timezone="UTC",
    )
    store.save_schedule(schedule)
    trigger = IntervalTrigger(seconds=60, timezone="UTC")
    assert store.schedule_due(schedule.schedule_id, trigger) is None
    due = now() - timedelta(seconds=130)
    with store.maker.begin() as session:
        row = session.get(ScheduleRecord, schedule.schedule_id, with_for_update=True)
        row.next_fire_at = due
    run_ids = race(
        schedule_due_in_process,
        database,
        [[schedule.schedule_id], [schedule.schedule_id]],
    )
    created = [run_id for run_id in run_ids if run_id is not None]
    assert len(created) == 1
    runs = store.list_runs(definition.goal_id, definition.actor_id)
    assert len(runs) == 1
    assert runs[0].run_id == created[0]
    with store.maker() as session:
        event = session.get(EventRecord, (definition.goal_id, runs[0].trigger_id))
        schedule_row = session.get(ScheduleRecord, schedule.schedule_id)
    assert event.document["payload"]["coalesced_periods"] == 2
    assert datetime.fromisoformat(event.document["scheduled_at"]) == due + timedelta(seconds=120)
    assert schedule_row.next_fire_at > now()
    assert store.schedule_due(schedule.schedule_id, trigger) is None
    assert len(store.list_runs(definition.goal_id, definition.actor_id)) == 1


def test_schedule_definition_change_and_goal_cancellation_compete(database, accounts):
    from agent_cluster.store import ScheduleRecord

    store = database["store"]
    context = multiprocessing.get_context("spawn")
    definition = make_goal(accounts)
    store.create_goal(definition)
    schedule = ScheduleDefinition(
        schedule_id="clu05-schedule-revision-" + uuid4().hex,
        goal_id=definition.goal_id,
        actor_id=definition.actor_id,
        interval_seconds=60,
        timezone="UTC",
    )
    store.save_schedule(schedule)
    due = now() - timedelta(seconds=30)
    with store.maker.begin() as session:
        row = session.get(ScheduleRecord, schedule.schedule_id, with_for_update=True)
        row.next_fire_at = due

    with context.Manager() as manager:
        ready = manager.Event()
        proceed = manager.Event()
        with ProcessPoolExecutor(max_workers=1, mp_context=context) as pool:
            stale_attempt = pool.submit(
                update_schedule_after_snapshot_in_process,
                database["database_url"], database["schema"], schedule.schedule_id, ready, proceed,
            )
            assert ready.wait(timeout=30)
            changed = schedule.model_copy(update={"interval_seconds": 120})
            store.save_schedule(changed)
            proceed.set()
            stale_result = stale_attempt.result(timeout=30)

    assert stale_result["snapshot"]["interval_seconds"] == 60
    assert stale_result["run_id"] is None
    assert store.list_runs(definition.goal_id, definition.actor_id) == []
    current_trigger = IntervalTrigger(seconds=120, timezone="UTC")
    assert store.schedule_due(schedule.schedule_id, current_trigger,
                              expected_definition=changed.model_dump(mode="json")) is None
    with store.maker.begin() as session:
        row = session.get(ScheduleRecord, schedule.schedule_id, with_for_update=True)
        row.next_fire_at = now() - timedelta(seconds=10)
    updated_run = store.schedule_due(schedule.schedule_id, current_trigger,
                                     expected_definition=changed.model_dump(mode="json"))
    assert updated_run is not None
    assert len(store.list_runs(definition.goal_id, definition.actor_id)) == 1

    cancelled_goal = make_goal(accounts)
    store.create_goal(cancelled_goal)
    cancellable = ScheduleDefinition(
        schedule_id="clu05-schedule-cancel-" + uuid4().hex,
        goal_id=cancelled_goal.goal_id,
        actor_id=cancelled_goal.actor_id,
        interval_seconds=60,
        timezone="UTC",
    )
    store.save_schedule(cancellable)
    with store.maker.begin() as session:
        row = session.get(ScheduleRecord, cancellable.schedule_id, with_for_update=True)
        row.next_fire_at = now() - timedelta(seconds=10)

    expected = cancellable.model_dump(mode="json")
    with context.Manager() as manager:
        barrier = manager.Barrier(3)
        with ProcessPoolExecutor(max_workers=2, mp_context=context) as pool:
            due_future = pool.submit(
                schedule_due_racing_with_cancel_in_process,
                database["database_url"], database["schema"], cancellable.schedule_id, expected, barrier,
            )
            cancel_future = pool.submit(
                cancel_goal_in_process,
                database["database_url"], database["schema"],
                cancelled_goal.goal_id, cancelled_goal.actor_id, barrier,
            )
            barrier.wait(timeout=30)
            due_result = due_future.result(timeout=60)
            cancel_result = cancel_future.result(timeout=60)

    cancelled_runs = store.list_runs(cancelled_goal.goal_id, cancelled_goal.actor_id)
    assert cancel_result == "cancelled"
    assert store.goal_state(cancelled_goal.goal_id, cancelled_goal.actor_id) == "cancelled"
    assert len(cancelled_runs) <= 1
    assert all(run.status == "cancelled" for run in cancelled_runs)
    assert (due_result is None) == (len(cancelled_runs) == 0)
    assert store.schedule_due(cancellable.schedule_id, IntervalTrigger(seconds=60, timezone="UTC"),
                              expected_definition=expected) is None
    save_observation("schedule-definition-cancel-race.json", {
        "schedule_id": schedule.schedule_id,
        "stale_definition_interval_seconds": stale_result["snapshot"]["interval_seconds"],
        "saved_definition_interval_seconds": changed.interval_seconds,
        "stale_definition_created_run_id": stale_result["run_id"],
        "updated_definition_created_run_id": updated_run.run_id,
        "cancel_race_schedule_id": cancellable.schedule_id,
        "cancel_race_due_run_id": due_result,
        "cancel_race_goal_state": cancel_result,
        "cancel_race_remaining_runs": [{"run_id": run.run_id, "status": run.status}
                                        for run in cancelled_runs],
        "post_cancel_schedule_due_run_id": None,
    })


def test_concurrent_cluster_budget_reservations_respect_goal_budget(database, accounts):
    from shop.budget import BudgetExceeded, BudgetService
    from shop.runtime_models import BudgetAccount, ProviderRequest

    store = database["store"]
    goal = make_goal(accounts, budget_cny=Decimal("0.00020800"))
    store.create_goal(goal)
    run = store.trigger(TriggerEvent(
        event_id="clu05-budget-" + uuid4().hex,
        goal_id=goal.goal_id,
        actor_id=goal.actor_id,
    ))
    outcomes = race(reserve_cluster_budget_in_process, database,
                    [[run.run_id, "first"], [run.run_id, "second"]])
    accepted = [item for item in outcomes if item["status"] == "reserved"]
    rejected = [item for item in outcomes if item["status"] == "rejected"]
    assert len(accepted) == 1
    assert len(rejected) == 1
    assert rejected[0]["reason"] == "目标预算不足"
    assert accepted[0]["reservation_cny"] == "0.00020800"

    maker = sessionmaker(database["engine"], expire_on_commit=False)
    budget = BudgetService(maker, verification_scope="cluster")
    with pytest.raises(BudgetExceeded, match="目标预算不足"):
        budget.reserve(
            task_id=None,
            run_id=run.run_id,
            step_key="clu05-budget-after-race",
            service="deepseek",
            model="deepseek-flash",
            request_body={"model": "deepseek-flash", "messages": [
                {"role": "user", "content": "CLU-05 additional reserved amount"}
            ]},
            input_limit=1,
            output_limit=0,
        )

    with maker() as session:
        rows = session.scalars(select(ProviderRequest).where(
            ProviderRequest.price_basis["cluster_run_id"].astext == run.run_id)).all()
        account = session.get(BudgetAccount, "project")
        total = session.scalar(select(func.sum(ProviderRequest.reservation_cny)).where(
            ProviderRequest.price_basis["cluster_goal_id"].astext == goal.goal_id))
    assert len(rows) == 1
    assert rows[0].status == "reserved"
    assert rows[0].price_basis["cluster_goal_id"] == goal.goal_id
    assert account.reserved_cny == Decimal("0.00020800")
    assert total == Decimal("0.00020800")
    save_observation("budget-concurrency.json", {
        "goal_id": goal.goal_id,
        "run_id": run.run_id,
        "limit_cny": str(goal.budget_cny),
        "reservation_inputs": {"input_limit": 100, "output_limit": 1},
        "concurrent_results": outcomes,
        "accepted_request_id": accepted[0]["request_id"],
        "accepted_status": rows[0].status,
        "account_reserved_cny": str(account.reserved_cny),
        "goal_reserved_cny": str(total),
        "additional_reservation_rejected": True,
        "provider_call_made": False,
    })
