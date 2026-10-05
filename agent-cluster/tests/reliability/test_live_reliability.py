import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest
from clu05_workers import (
    hold_lease,
    recover_run_in_process,
    run_preplanned_business_in_process,
    save_completed_business_result_then_exit,
    try_goal_lease_in_process,
)
from conftest import (
    integration_call_snapshot,
    provider_request_snapshot,
    save_observation,
)

from agent_cluster.notes import NoteStore
from agent_cluster.store import digest
from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
)


def create_orders_target(store, actor_id, label):
    goal = GoalDefinition(
        goal_id=f"clu05-{label}-" + uuid4().hex,
        actor_id=actor_id,
        objective="读取本人订单列表并保留实际来源",
        allowed_operations={"shop_customer_agent": ["orders"]},
        criteria=[CompletionCriterion(
            tool_name="shop_customer_agent",
            action="orders",
            path=["data", "orders"],
            operator="nonempty",
        )],
        max_decisions=1,
        max_tasks=1,
    )
    store.create_goal(goal)
    run = store.trigger(TriggerEvent(
        event_id=f"clu05-{label}-" + uuid4().hex,
        goal_id=goal.goal_id,
        actor_id=actor_id,
        kind="manual",
        payload={"case_id": "CC01"},
    ))
    plan = TaskPlan(tasks=[TaskSpec(
        task_id="orders-read",
        tool_name="shop_customer_agent",
        action="orders",
        payload={},
    )])
    store.save_plan(run.run_id, actor_id, plan, expected_version=0)
    task = store.tasks(run.run_id, actor_id)[0]
    return goal, run, task


def verify_real_orders(result, actor_id, payload):
    from shop.orders import OrderQueryService
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    if result["status"] != "completed":
        raise AssertionError("真实订单读取没有返回completed状态")
    orders = result["data"]["orders"]
    if not orders:
        raise AssertionError("真实订单读取没有返回订单")
    engine = create_engine(
        "postgresql+psycopg://shop@127.0.0.1:55432/shop",
        execution_options={"postgresql_readonly": True},
    )
    try:
        with Session(engine) as session:
            source = OrderQueryService(session, actor_id)
            if source.list_orders(**payload) != result["data"]:
                raise AssertionError("stdio订单列表与本人订单来源不一致")
    finally:
        engine.dispose()
    return len(orders)


def wait_for_ready(futures, ready_events, timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    while not all(event.is_set() for event in ready_events):
        for future in futures:
            if future.done():
                result = future.result()
                if not result["goal_lease_acquired"]:
                    pytest.fail("独立目标没有获取目标lease")
        if time.monotonic() >= deadline:
            pytest.fail("所有目标没有建立真实MCP stdio会话")
        time.sleep(0.05)


def test_real_stdio_runs_multiple_targets_with_shared_slots_and_goal_lease(
    database, accounts, cluster_config
):
    actor_id = accounts["customer"]["actor_id"]
    assert actor_id in cluster_config["actors"]
    evidence_dir = Path(os.environ["CLU05_EVIDENCE_DIR"])
    notes_dir = evidence_dir / "multiple-target-notes"
    targets = [create_orders_target(database["store"], actor_id, f"target-{index}")
               for index in range(5)]
    goals = [item[0] for item in targets]
    runs = [item[1] for item in targets]
    tasks = [item[2] for item in targets]
    run_ids = [run.run_id for run in runs]
    goal_ids = [goal.goal_id for goal in goals]
    request_ids = [task["request_id"] for task in tasks]
    provider_before = provider_request_snapshot(run_ids, goal_ids)
    calls_before = integration_call_snapshot(request_ids)
    assert provider_before == []
    assert calls_before == []

    context = multiprocessing.get_context("spawn")
    with context.Manager() as manager:
        holders = []
        releases = []
        pool = None
        for slot in range(4):
            acquired = manager.Event()
            release = manager.Event()
            holder = context.Process(
                target=hold_lease,
                args=(database["database_url"], database["schema"],
                      f"business-slot:{slot}", acquired, release),
            )
            holder.start()
            holders.append((holder, acquired))
            releases.append(release)
        try:
            for _, acquired in holders:
                assert acquired.wait(timeout=30)
            ready_events = [manager.Event() for _ in targets]
            pool = ProcessPoolExecutor(max_workers=len(targets), mp_context=context)
            futures = [pool.submit(
                    run_preplanned_business_in_process,
                    database["database_url"],
                    database["schema"],
                    cluster_config["path"],
                    str(notes_dir),
                    run.run_id,
                    actor_id,
                    ready,
                ) for (_, run, _), ready in zip(targets, ready_events)]
            try:
                wait_for_ready(futures, ready_events, 180)

                waiting_tasks = [item for run in runs
                                 for item in database["store"].tasks(run.run_id, actor_id)]
                assert all(item["status"] == "queued" for item in waiting_tasks)
                assert integration_call_snapshot(request_ids) == []
                assert try_goal_lease_in_process(
                    database["database_url"], database["schema"], goals[0].goal_id
                ) is False

                for release in releases:
                    release.set()
                results = [future.result(timeout=240) for future in futures]
            finally:
                for release in releases:
                    release.set()
                pool.shutdown(wait=True, cancel_futures=True)
        finally:
            for release in releases:
                release.set()
            for holder, _ in holders:
                holder.join(timeout=30)
                if holder.is_alive():
                    holder.terminate()
                    holder.join(timeout=30)

    assert all(result["goal_lease_acquired"] for result in results)
    assert all(result["task_status"] == "completed" for result in results)
    assert {result["run_id"] for result in results} == set(run_ids)
    assert {result["request_id"] for result in results} == set(request_ids)
    assert all(result["result_sha256"] for result in results)

    with ExitStack() as stack:
        reusable_slots = [stack.enter_context(database["store"].lease(f"business-slot:{slot}"))
                          for slot in range(4)]
        assert all(reusable_slots)

    stored_tasks = {run.run_id: database["store"].tasks(run.run_id, actor_id)[0] for run in runs}
    verified_order_counts = {
        run_id: verify_real_orders(
            stored_tasks[run_id]["result"], actor_id, stored_tasks[run_id]["arguments"]["payload"]
        )
        for run_id in run_ids
    }
    calls_after = integration_call_snapshot(request_ids)
    providers_after = provider_request_snapshot(run_ids, goal_ids)
    assert calls_after == []
    assert providers_after == []
    save_observation("multiple-targets-stdio.json", {
        "target_count": len(goals),
        "open_real_stdio_sessions": len(ready_events),
        "occupied_business_slots_before_release": 4,
        "waiting_task_statuses": [item["status"] for item in waiting_tasks],
        "competing_same_goal_lease_acquired": False,
        "targets": [{"goal_id": goal.goal_id, "run_id": run.run_id,
                     "task_id": task["task_id"], "request_id": task["request_id"],
                     "task_status": next(result["task_status"] for result in results
                                          if result["run_id"] == run.run_id),
                     "verified_order_count": verified_order_counts[run.run_id],
                     "result_sha256": next(result["result_sha256"] for result in results
                                          if result["run_id"] == run.run_id)}
                    for goal, run, task in targets],
        "integration_calls_before": calls_before,
        "integration_calls_after": calls_after,
        "integration_calls_expected_for_read_operations": 0,
        "provider_requests_before": provider_before,
        "provider_requests_after": providers_after,
        "business_slots_released": reusable_slots,
    })


def test_completed_real_business_result_recovers_after_process_termination(
    database, accounts, cluster_config
):
    import json
    from uuid import NAMESPACE_URL, uuid5

    actor_id = accounts["customer"]["actor_id"]
    assert actor_id in cluster_config["actors"]
    evidence_dir = Path(os.environ["CLU05_EVIDENCE_DIR"])
    notes_dir = evidence_dir / "recovery-notes"
    goal, run, task = create_orders_target(database["store"], actor_id, "result-recovery")
    notes = NoteStore(notes_dir)
    before_history = database["store"].history(run.run_id, actor_id)
    before_notes = notes.index(goal_id=goal.goal_id, actor_id=actor_id)
    providers_before = provider_request_snapshot([run.run_id], [goal.goal_id])
    calls_before = integration_call_snapshot([task["request_id"]])
    assert before_history == []
    assert before_notes == []
    assert providers_before == []
    assert calls_before == []

    context = multiprocessing.get_context("spawn")
    terminated = context.Process(
        target=save_completed_business_result_then_exit,
        args=(database["database_url"], database["schema"], cluster_config["path"],
              str(notes_dir), run.run_id, actor_id, 86),
    )
    terminated.start()
    terminated.join(timeout=180)
    if terminated.is_alive():
        terminated.terminate()
        terminated.join(timeout=30)
        pytest.fail("真实MCP业务操作在终止检查时仍未结束")
    assert terminated.exitcode == 86

    after_termination = database["store"].tasks(run.run_id, actor_id)[0]
    history_after_termination = database["store"].history(run.run_id, actor_id)
    notes_after_termination = notes.index(goal_id=goal.goal_id, actor_id=actor_id)
    run_after_termination = database["store"].get_run(run.run_id, actor_id)
    calls_after_termination = integration_call_snapshot([task["request_id"]])
    providers_after_termination = provider_request_snapshot([run.run_id], [goal.goal_id])
    assert after_termination["task_id"] == task["task_id"]
    assert after_termination["request_id"] == task["request_id"]
    assert after_termination["status"] == "completed"
    assert after_termination["result"] is not None
    verified_order_count = verify_real_orders(
        after_termination["result"], actor_id, after_termination["arguments"]["payload"]
    )
    assert run_after_termination.status == "queued"
    assert history_after_termination == []
    assert notes_after_termination == []
    assert calls_after_termination == []
    assert providers_after_termination == []

    with ProcessPoolExecutor(max_workers=1, mp_context=context) as pool:
        recovered = pool.submit(
            recover_run_in_process,
            database["database_url"],
            database["schema"],
            cluster_config["path"],
            str(notes_dir),
            run.run_id,
            actor_id,
        ).result(timeout=180)
        repeated = pool.submit(
            recover_run_in_process,
            database["database_url"],
            database["schema"],
            cluster_config["path"],
            str(notes_dir),
            run.run_id,
            actor_id,
        ).result(timeout=60)

    final_run = database["store"].get_run(run.run_id, actor_id)
    final_tasks = database["store"].tasks(run.run_id, actor_id)
    final_history = database["store"].history(run.run_id, actor_id)
    final_notes = notes.index(goal_id=goal.goal_id, actor_id=actor_id)
    final_calls = integration_call_snapshot([task["request_id"]])
    providers_after_recovery = provider_request_snapshot([run.run_id], [goal.goal_id])
    assert recovered["run_status"] == "completed"
    assert recovered["task_statuses"] == ["completed"]
    assert recovered["history_count"] == 1
    assert recovered["history_roles"] == ["user"]
    assert len(recovered["note_references"]) == 1
    assert repeated["run_status"] == "completed"
    assert repeated["history_count"] == 1
    assert len(repeated["note_references"]) == 1
    assert final_run.status == "completed"
    assert final_tasks[0]["request_id"] == task["request_id"]
    assert len(final_history) == 1
    assert len(final_notes) == 1
    assert digest(final_tasks[0]["result"]) == digest(after_termination["result"])
    history_record = json.loads(final_history[0]["content"])
    assert history_record["task_id"] == task["task_id"]
    assert history_record["request_id"] == task["request_id"]
    assert history_record["tool_name"] == "shop_customer_agent"
    assert history_record["action"] == "orders"
    assert history_record["goal_run_id"] == run.run_id
    assert history_record["status"] == "completed"
    assert history_record["result"] == final_tasks[0]["result"]
    assert history_record["error"] is None
    note_reference = final_notes[0]
    note_content = notes.read(note_reference, goal_id=goal.goal_id, actor_id=actor_id)
    assert note_reference.note_id == str(uuid5(NAMESPACE_URL, task["request_id"]))
    assert note_reference.source == "cluster-task:" + task["request_id"]
    assert note_content == final_history[0]["content"]
    assert recovered["history_sha256"] == [digest(final_history[0])]
    assert repeated["history_sha256"] == recovered["history_sha256"]
    assert repeated["note_references"] == recovered["note_references"]
    assert final_calls == []
    assert providers_after_recovery == []
    save_observation("completed-result-recovery.json", {
        "goal_id": goal.goal_id,
        "run_id": run.run_id,
        "task_id": task["task_id"],
        "request_id": task["request_id"],
        "verified_order_count": verified_order_count,
        "process_exit_code_after_result_commit": terminated.exitcode,
        "before_process": {"task_status": "queued", "history_count": len(before_history),
                           "note_reference_count": len(before_notes),
                           "integration_calls": calls_before,
                           "provider_requests": providers_before},
        "after_process_before_recovery": {
            "run_status": run_after_termination.status,
            "task_status": after_termination["status"],
            "result_sha256": digest(after_termination["result"]),
            "history_count": len(history_after_termination),
            "note_reference_count": len(notes_after_termination),
            "integration_calls": calls_after_termination,
            "integration_calls_expected_for_read_operation": 0,
            "provider_requests": providers_after_termination,
        },
        "after_recovery": {
            "run_status": final_run.status,
            "task_status": final_tasks[0]["status"],
            "result_sha256": digest(final_tasks[0]["result"]),
            "history_count": len(final_history),
            "history_sha256": [digest(message) for message in final_history],
            "history_result_sha256": digest(history_record["result"]),
            "note_content_matches_history": note_content == final_history[0]["content"],
            "note_references": [{"note_id": note.note_id, "version": note.version,
                                 "sha256": note.sha256, "topic": note.topic,
                                 "source": note.source} for note in final_notes],
            "integration_calls": final_calls,
            "integration_calls_expected_for_read_operation": 0,
            "provider_requests": providers_after_recovery,
        },
        "repeated_recovery": {
            "run_status": repeated["run_status"],
            "history_count": repeated["history_count"],
            "history_sha256": repeated["history_sha256"],
            "note_references": repeated["note_references"],
        },
    })
