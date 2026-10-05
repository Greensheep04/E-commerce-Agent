import asyncio
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url

from shop.adventureworks import import_orders, source_manifest
from shop.customer_runtime_models import CustomerEvent, CustomerTurn
from shop.integration import ShopIntegration
from shop.runtime_models import BudgetAccount, ProviderRequest
from tests import test_integration_permissions as permissions

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATHS = (
    "src/shop/customer_worker.py",
    "src/shop/customer_runtime.py",
    "src/shop/customer_runtime_models.py",
    "src/shop/integration.py",
    "src/shop/integration_adapters.py",
    "src/shop/runtime_models.py",
    "tests/test_integration_permissions.py",
    "tests/test_integration_worker_shutdown.py",
    "uv.lock",
)
CONVERSATION_REQUEST_ID = "00000000-0000-4000-8000-000000000501"
SUBMIT_REQUEST_ID = "00000000-0000-4000-8000-000000000502"
CANCEL_REQUEST_ID = "00000000-0000-4000-8000-000000000503"
MESSAGE = "查询我的订单"


@pytest.fixture
def worker_shutdown_evidence_dir():
    run_id = os.environ.get("IF_WORKER_SHUTDOWN_RUN_ID")
    if run_id is None:
        run_id = (
            "if02-worker-shutdown-"
            + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + uuid4().hex[:8]
        )
    path = PROJECT_ROOT / ".scratch" / "evidence" / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _source_sha256():
    return {
        path: hashlib.sha256((PROJECT_ROOT / path).read_bytes()).hexdigest()
        for path in SOURCE_PATHS
    }


def _turn_snapshot(maker, conversation_id, turn_id):
    with maker() as session:
        turn = session.get(CustomerTurn, turn_id)
        assert turn is not None
        events = session.scalars(
            select(CustomerEvent)
            .where(CustomerEvent.conversation_id == conversation_id)
            .order_by(CustomerEvent.id)
        ).all()
        return {
            "turn": {
                "status": turn.status,
                "error": turn.error,
                "result": turn.result,
                "model_calls": turn.model_calls,
                "tool_calls": turn.tool_calls,
                "cancel_requested": turn.cancel_requested,
                "started_at": turn.started_at.isoformat() if turn.started_at else None,
                "started_at_present": turn.started_at is not None,
                "elapsed_seconds": str(turn.elapsed_seconds),
                "finished_at": turn.finished_at.isoformat() if turn.finished_at else None,
            },
            "events": [
                {
                    "id": event.id,
                    "type": event.type,
                    "turn_id": event.turn_id,
                    "payload": event.payload,
                    "created_at": event.created_at.isoformat(),
                }
                for event in events
            ],
        }


def _request_cost_snapshot(maker):
    with maker() as session:
        requests = session.scalars(select(ProviderRequest).order_by(ProviderRequest.id)).all()
        accounts = session.scalars(select(BudgetAccount).order_by(BudgetAccount.id)).all()
        return {
            "provider_request_count": len(requests),
            "provider_requests": [
                {
                    "id": request.id,
                    "run_id": request.run_id,
                    "status": request.status,
                    "reservation_cny": str(request.reservation_cny),
                    "charged_cny": (
                        str(request.charged_cny) if request.charged_cny is not None else None
                    ),
                }
                for request in requests
            ],
            "reserved_cny": str(sum((request.reservation_cny for request in requests), Decimal(0))),
            "charged_cny": str(
                sum((request.charged_cny or Decimal(0) for request in requests), Decimal(0))
            ),
            "budget_accounts": [
                {
                    "id": account.id,
                    "spent_cny": str(account.spent_cny),
                    "reserved_cny": str(account.reserved_cny),
                }
                for account in accounts
            ],
        }


def _budget_account_total(snapshot, field):
    return sum((Decimal(account[field]) for account in snapshot["budget_accounts"]), Decimal(0))


def _activity_rows(engine, application_name, query_text):
    statement = text(
        "SELECT pid, state, query, wait_event_type, wait_event FROM pg_stat_activity "
        "WHERE application_name = :application_name AND pid <> pg_backend_pid() "
        "AND query ILIKE :query_text ORDER BY pid"
    )
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                statement,
                {"application_name": application_name, "query_text": f"%{query_text}%"},
            ).mappings()
        ]


async def _wait_for(check, description, timeout=15):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = check()
        if value:
            return value
        await asyncio.sleep(0.05)
    raise AssertionError(f"等待条件超时：{description}")


def _record_report(
    evidence_dir,
    db_session,
    inputs,
    data_sources,
    source_before,
    source_after,
    observations,
    status,
):
    database, schema, version = db_session.execute(
        text("SELECT current_database(), current_schema(), current_setting('server_version')")
    ).one()
    input_bytes = json.dumps(
        inputs, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    report = {
        "milestone": "IF-02",
        "check": "if02-running-worker-waiter-shutdown",
        "status": status,
        "run_id": evidence_dir.name,
        "input_sha256": hashlib.sha256(input_bytes).hexdigest(),
        "inputs": inputs,
        "database": {
            "database": database,
            "schema": schema,
            "postgresql_version": version,
        },
        "data_sources": data_sources,
        "run_conditions": {
            "python_version": sys.version.split()[0],
            "dependencies": "真实 PostgreSQL 与项目安装依赖",
            "checkpoint_table_lock": "ACCESS EXCLUSIVE 持有至两实例观察结束",
            "provider_requests_expected": 0,
            "cloud_requests": 0,
        },
        "source_sha256": source_before,
        "source_sha256_before": source_before,
        "source_sha256_after": source_after,
        "source_stable": source_before == source_after,
        "observations": observations,
    }
    (evidence_dir / "if02-running-worker-waiter-shutdown.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )


def test_if02_running_worker_is_unchanged_when_waiting_instance_shuts_down(
    db_session, data_dir, worker_shutdown_evidence_dir
):
    source_before = _source_sha256()
    import_result = import_orders(db_session, permissions.ORDER_DIRECTORY)
    assert import_result["counts"] == permissions.CUSTOMER_COUNTS
    users = permissions._customer_users(db_session)
    order_numbers = permissions._orders_for_customer(permissions.ORDER_DIRECTORY, 11091)
    assert order_numbers

    run_id = worker_shutdown_evidence_dir.name
    owner_application_name = "if02-owner-" + run_id[-8:]
    waiter_application_name = "if02-waiter-" + run_id[-8:]
    probe_application_name = "if02-probe-" + run_id[-8:]
    owner_settings = permissions._settings(
        db_session, data_dir, application_name=owner_application_name
    )
    waiter_settings = permissions._settings(
        db_session, data_dir, application_name=waiter_application_name
    )
    control_url = make_url(owner_settings.database_url).update_query_dict(
        {"application_name": probe_application_name}
    )
    control_engine = create_engine(control_url, pool_pre_ping=True)
    owner = ShopIntegration(owner_settings)
    waiter = ShopIntegration(waiter_settings)
    owner_started = False
    waiter_started = False
    waiter_stopped = False
    checkpoint_connection = None
    conversation_id = None
    turn_id = None
    observations = {}
    inputs = {
        "run_id": run_id,
        "source_revision": permissions.CUSTOMER_REVISION,
        "customer_id": 11091,
        "source_order_number": order_numbers[0],
        "submit_request_id": SUBMIT_REQUEST_ID,
        "submit_message": MESSAGE,
        "conversation_request_id": CONVERSATION_REQUEST_ID,
        "gateway_instances": 2,
        "checkpoint_table": "checkpoints",
        "shutdown_target": "等待同一conversation锁的worker实例",
    }
    data_sources = {
        "customer_orders": source_manifest(permissions.ORDER_DIRECTORY),
        "customer_order_import": import_result,
        "customer_account": {"username": users[0].username, "customer_id": 11091},
    }
    status = "failed"

    async def exercise():
        nonlocal checkpoint_connection, conversation_id, turn_id, owner_started
        nonlocal waiter_started, waiter_stopped
        owner_task = None
        waiter_task = None
        try:
            await owner.startup()
            owner_started = True
            await waiter.startup()
            waiter_started = True
            owner_context = owner.login(
                users[0].username, permissions.TEST_PASSWORD, CONVERSATION_REQUEST_ID
            )
            conversation = await owner.invoke(
                "customer", "create_conversation", {"title": "IF-02并发关闭检查"}, owner_context
            )
            conversation_id = conversation.data["id"]
            checkpoint_connection = control_engine.connect()
            checkpoint_connection.execute(text("LOCK TABLE checkpoints IN ACCESS EXCLUSIVE MODE"))

            submit_payload = {"conversation_id": conversation_id, "message": MESSAGE}
            owner_submit_context = owner.login(
                users[0].username, permissions.TEST_PASSWORD, SUBMIT_REQUEST_ID
            )
            submitted = await owner.invoke(
                "customer", "submit", submit_payload, owner_submit_context
            )
            turn_id = submitted.data["id"]
            owner_task = owner.worker.tasks.get(turn_id)
            assert owner_task is not None

            await _wait_for(
                lambda: _turn_status(owner, turn_id) == "running",
                "持锁worker将客服消息状态更新为running",
            )
            checkpoint_wait = await _wait_for(
                lambda: [
                    row
                    for row in _activity_rows(control_engine, owner_application_name, "checkpoints")
                    if row["state"] == "active" and row["wait_event_type"] == "Lock"
                ],
                "持锁worker在checkpoint读取处等待表锁",
            )

            waiter_submit_context = waiter.login(
                users[0].username, permissions.TEST_PASSWORD, SUBMIT_REQUEST_ID
            )
            repeated = await waiter.invoke(
                "customer", "submit", submit_payload, waiter_submit_context
            )
            assert repeated.data["id"] == turn_id
            waiter_task = waiter.worker.tasks.get(turn_id)
            assert waiter_task is not None
            await asyncio.sleep(0)
            with control_engine.connect() as probe:
                lock_still_held = (
                    probe.scalar(
                        text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                        {"key": "customer-conversation:" + conversation_id},
                    )
                    is False
                )
                probe.commit()

            assert checkpoint_wait
            assert lock_still_held is True
            assert not owner_task.done()
            assert not waiter_task.done()

            state_before = _turn_snapshot(owner.maker, conversation_id, turn_id)
            cost_before = _request_cost_snapshot(owner.maker)
            assert state_before["turn"]["status"] == "running"
            assert state_before["turn"]["started_at_present"] is True
            assert state_before["turn"]["model_calls"] == 0
            assert cost_before["provider_request_count"] == 0
            assert cost_before["provider_requests"] == []
            observations.update(
                {
                    "turn_id": turn_id,
                    "conversation_id": conversation_id,
                    "owner_task_running": not owner_task.done(),
                    "waiter_task_running": not waiter_task.done(),
                    "owner_checkpoint_wait": checkpoint_wait,
                    "owner_conversation_lock_still_held": lock_still_held,
                    "state_before_waiter_shutdown": state_before,
                    "requests_and_cost_before_waiter_shutdown": cost_before,
                }
            )

            await waiter.shutdown()
            waiter_stopped = True
            await asyncio.sleep(0)
            state_after = _turn_snapshot(owner.maker, conversation_id, turn_id)
            cost_after = _request_cost_snapshot(owner.maker)
            owner_still_running = not owner_task.done()
            waiter_task_removed = turn_id not in waiter.worker.tasks
            with control_engine.connect() as probe:
                lock_still_held_after = (
                    probe.scalar(
                        text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                        {"key": "customer-conversation:" + conversation_id},
                    )
                    is False
                )
                probe.commit()
            observations.update(
                {
                    "state_after_waiter_shutdown": state_after,
                    "requests_and_cost_after_waiter_shutdown": cost_after,
                    "request_count_delta": (
                        cost_after["provider_request_count"] - cost_before["provider_request_count"]
                    ),
                    "reserved_cny_delta": str(
                        Decimal(cost_after["reserved_cny"]) - Decimal(cost_before["reserved_cny"])
                    ),
                    "charged_cny_delta": str(
                        Decimal(cost_after["charged_cny"]) - Decimal(cost_before["charged_cny"])
                    ),
                    "budget_spent_cny_delta": str(
                        _budget_account_total(cost_after, "spent_cny")
                        - _budget_account_total(cost_before, "spent_cny")
                    ),
                    "budget_reserved_cny_delta": str(
                        _budget_account_total(cost_after, "reserved_cny")
                        - _budget_account_total(cost_before, "reserved_cny")
                    ),
                    "owner_task_running_after_waiter_shutdown": owner_still_running,
                    "waiter_task_removed_after_shutdown": waiter_task_removed,
                    "owner_conversation_lock_still_held_after_waiter_shutdown": lock_still_held_after,
                }
            )
            state_before_business = {
                key: value
                for key, value in state_before["turn"].items()
                if key not in {"elapsed_seconds", "started_at"}
            }
            state_after_business = {
                key: value
                for key, value in state_after["turn"].items()
                if key not in {"elapsed_seconds", "started_at"}
            }
            assert state_after_business == state_before_business
            assert state_after["events"] == state_before["events"]
            assert Decimal(state_after["turn"]["elapsed_seconds"]) >= Decimal(
                state_before["turn"]["elapsed_seconds"]
            )
            assert cost_after == cost_before
            assert observations["request_count_delta"] == 0
            assert Decimal(observations["reserved_cny_delta"]) == 0
            assert Decimal(observations["charged_cny_delta"]) == 0
            assert Decimal(observations["budget_spent_cny_delta"]) == 0
            assert Decimal(observations["budget_reserved_cny_delta"]) == 0
            assert owner_still_running is True
            assert waiter_task_removed is True
            assert lock_still_held_after is True
        finally:
            try:
                if owner_started and turn_id is not None:
                    cancel_context = owner.login(
                        users[0].username, permissions.TEST_PASSWORD, CANCEL_REQUEST_ID
                    )
                    cancelled = await owner.invoke(
                        "customer", "cancel", {"turn_id": turn_id}, cancel_context
                    )
                    observations["owner_cancel_response"] = cancelled.data
                    if owner_task is not None and not owner_task.done():
                        await asyncio.wait_for(owner_task, timeout=10)
                    final_state = _turn_snapshot(owner.maker, conversation_id, turn_id)
                    final_costs = _request_cost_snapshot(owner.maker)
                    observations["owner_state_after_real_cancel"] = final_state
                    observations["requests_and_cost_after_owner_cancel"] = final_costs
                    assert cancelled.data["status"] == "cancelled"
                    assert final_state["turn"]["status"] == "cancelled"
                    assert final_state["turn"]["started_at_present"] is False
                    assert final_state["turn"]["cancel_requested"] is True
                    assert final_costs["provider_request_count"] == 0
                    assert final_costs["provider_requests"] == []
                    assert _budget_account_total(final_costs, "spent_cny") == 0
                    assert _budget_account_total(final_costs, "reserved_cny") == 0
            finally:
                try:
                    if waiter_started and not waiter_stopped:
                        await waiter.shutdown()
                        waiter_stopped = True
                    if owner_started:
                        await owner.shutdown()
                finally:
                    if checkpoint_connection is not None:
                        checkpoint_connection.rollback()
                        checkpoint_connection.close()

    try:
        asyncio.run(exercise())
        status = "passed"
    finally:
        source_after = _source_sha256()
        try:
            _record_report(
                worker_shutdown_evidence_dir,
                db_session,
                inputs,
                data_sources,
                source_before,
                source_after,
                observations,
                status,
            )
        finally:
            control_engine.dispose()


def _turn_status(gateway, turn_id):
    with gateway.maker() as session:
        turn = session.get(CustomerTurn, turn_id)
        return turn.status if turn is not None else None
