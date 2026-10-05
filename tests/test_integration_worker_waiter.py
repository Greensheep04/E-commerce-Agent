import asyncio
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, select, text

from shop.adventureworks import import_orders, source_manifest
from shop.customer_runtime_models import CustomerConversation, CustomerEvent, CustomerTurn
from shop.integration import ShopIntegration
from shop.runtime_models import BudgetAccount, ProviderRequest
from tests.test_integration_permissions import (
    ORDER_DIRECTORY,
    TEST_PASSWORD,
    _customer_users,
    _settings,
    _worker_lock_activity,
)

ROOT = Path(__file__).resolve().parents[1]


def _snapshot(maker, turn_id):
    with maker() as session:
        turn = session.get(CustomerTurn, turn_id)
        conversation = session.get(CustomerConversation, turn.conversation_id)
        events = list(session.scalars(select(CustomerEvent).where(
            CustomerEvent.conversation_id == turn.conversation_id
        ).order_by(CustomerEvent.id)))
        account = session.get(BudgetAccount, "project")
        result = {
            "turn": {column.name: getattr(turn, column.name) for column in CustomerTurn.__table__.columns},
            "next_event": conversation.next_event,
            "events": [{column.name: getattr(event, column.name)
                        for column in CustomerEvent.__table__.columns} for event in events],
            "requests": list(session.scalars(select(ProviderRequest.id).order_by(ProviderRequest.id))),
            "account": None if account is None else {
                "spent_cny": account.spent_cny, "reserved_cny": account.reserved_cny,
            },
        }
        return json.loads(json.dumps(result, default=str, ensure_ascii=False))


def test_waiting_worker_shutdown_preserves_queued_turn(db_session, data_dir, tmp_path_factory):
    evidence_dir = tmp_path_factory.getbasetemp().parent
    evidence_dir.mkdir(parents=True, exist_ok=True)
    inputs = {"title": "IF-05等待worker关闭检查", "message": "查询我的订单",
              "create_request_id": str(uuid4()), "submit_request_id": str(uuid4())}
    imported = import_orders(db_session, ORDER_DIRECTORY)
    user = _customer_users(db_session)[0]
    application_name = "if05-waiter-" + uuid4().hex[:16]
    settings = _settings(db_session, data_dir, application_name=application_name)
    database, schema, version = db_session.execute(text(
        "SELECT current_database(), current_schema(), current_setting('server_version')"
    )).one()
    report = {
        "status": "running", "milestone": "IF-05", "run_id": evidence_dir.name,
        "check": "waiting_worker_shutdown_preserves_queued_turn", "inputs": inputs,
        "database": {"name": database, "schema": schema, "version": version},
        "data_sources": {"manifest": source_manifest(ORDER_DIRECTORY), "import": imported},
        "source_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                          for name in ("src/shop/customer_worker.py", "src/shop/integration.py",
                                       "tests/test_integration_worker_waiter.py")},
        "observations": {},
    }
    report_path = evidence_dir / "if05-worker-waiter.json"

    def save_report():
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    save_report()

    async def exercise():
        gateway = ShopIntegration(settings)
        lock_engine = create_engine(settings.database_url)
        lock_connection = lock_engine.connect()
        lock_key = None
        try:
            await gateway.startup()
            created = await gateway.invoke("customer", "create_conversation", {"title": inputs["title"]},
                gateway.login(user.username, TEST_PASSWORD, inputs["create_request_id"]))
            conversation_id = created.data["id"]
            lock_key = "customer-conversation:" + conversation_id
            lock_connection.execute(text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"),
                                    {"key": lock_key})
            lock_connection.commit()
            submitted = await gateway.invoke("customer", "submit", {
                "conversation_id": conversation_id, "message": inputs["message"],
            }, gateway.login(user.username, TEST_PASSWORD, inputs["submit_request_id"]))
            turn_id = submitted.data["id"]
            task = gateway.worker.tasks[turn_id]
            await asyncio.sleep(0.15)
            activity = _worker_lock_activity(lock_engine, application_name)
            assert activity
            with lock_engine.connect() as probe:
                acquired = probe.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                                        {"key": lock_key})
                if acquired:
                    probe.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                                  {"key": lock_key})
                probe.commit()
            assert acquired is False
            before = _snapshot(gateway.maker, turn_id)
            assert not task.done()
            assert before["turn"]["status"] == "queued"
            assert before["turn"]["started_at"] is None
            assert before["requests"] == []
            await gateway.shutdown()
            after = _snapshot(gateway.maker, turn_id)
            report["observations"].update({"conversation_id": conversation_id, "turn_id": turn_id,
                "worker_activity": activity, "before": before, "after": after,
                "worker_task_done": task.done(), "remaining_tasks": list(gateway.worker.tasks)})
            save_report()
            assert after == before
            assert task.done() and not gateway.worker.tasks
        finally:
            await gateway.shutdown()
            if lock_key is not None:
                lock_connection.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                                        {"key": lock_key})
                lock_connection.commit()
            lock_connection.close()
            lock_engine.dispose()

    try:
        asyncio.run(exercise())
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save_report()
