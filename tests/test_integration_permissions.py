import asyncio
import csv
import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from integration_host import InvalidInput
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url

from shop.adventureworks import REVISION as CUSTOMER_REVISION
from shop.adventureworks import TABLE_COUNTS as CUSTOMER_COUNTS
from shop.adventureworks import import_orders, source_manifest
from shop.app import create_app
from shop.assessment_models import AssessmentEmployee, AssessmentRun, AssessmentSource
from shop.assessment_types import REVISION as ASSESSMENT_REVISION
from shop.assessment_types import SOURCE_ID as ASSESSMENT_SOURCE_ID
from shop.auth import hash_password
from shop.config import Settings
from shop.customer_models import CustomerOrder
from shop.customer_runtime_models import CustomerConversation, CustomerTurn
from shop.integration import ShopIntegration
from shop.integration_models import IntegrationCall
from shop.models import User
from shop.orders import bind_customer
from shop.runtime_models import ProviderRequest
from tests.assessment_fixtures import SOURCE_DIRECTORY

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ORDER_DIRECTORY = PROJECT_ROOT / ".local/data/orders" / CUSTOMER_REVISION
ORDER_FILE = ORDER_DIRECTORY / "SalesOrderHeader.csv"
ASSESSMENT_ORDER_DATABASE = "postgresql+psycopg://shop@127.0.0.1:55432/shop"
TEST_PASSWORD = "if02-if05-local-permission-password"
SESSION_SECRET = "if02-if05-local-permission-session-secret"
CREATE_CONVERSATION_REQUEST_ID = "00000000-0000-4000-8000-000000000301"
SUBMIT_REQUEST_ID = "00000000-0000-4000-8000-000000000302"
CONFLICTING_SUBMIT_REQUEST_ID = "00000000-0000-4000-8000-000000000303"
CROSS_CUSTOMER_SUBMIT_REQUEST_ID = "00000000-0000-4000-8000-000000000304"
CANCEL_REQUEST_ID = "00000000-0000-4000-8000-000000000305"
RESTART_CONVERSATION_REQUEST_ID = "00000000-0000-4000-8000-000000000411"
RESTART_SUBMIT_REQUEST_ID = "00000000-0000-4000-8000-000000000412"
RESTART_CANCEL_REQUEST_ID = "00000000-0000-4000-8000-000000000413"


@pytest.fixture(scope="session")
def permissions_evidence_dir(tmp_path_factory):
    path = tmp_path_factory.getbasetemp().parent
    path.mkdir(parents=True, exist_ok=True)
    return path


def _directory_sha256(directory):
    return {
        path.relative_to(directory).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.rglob("*")) if path.is_file()
    }


def _report(evidence_dir, name, milestone, db_session, inputs, data_sources,
            observations, status, application_name=None):
    source_paths = (
        "src/shop/adventureworks.py",
        "src/shop/app.py",
        "src/shop/assessment.py",
        "src/shop/assessment_models.py",
        "src/shop/assessment_order_adapter.py",
        "src/shop/assessment_service.py",
        "src/shop/customer_api.py",
        "src/shop/customer_runtime.py",
        "src/shop/customer_runtime_models.py",
        "src/shop/customer_worker.py",
        "src/shop/integration.py",
        "src/shop/integration_adapters.py",
        "src/shop/integration_api.py",
        "src/shop/integration_models.py",
        "src/shop/integration_store.py",
        "src/shop/orders.py",
        "tests/assessment_fixtures.py",
        "tests/test_customer_orders_api.py",
        "tests/test_integration_permissions.py",
        "uv.lock",
    )
    source_hashes = {
        path: hashlib.sha256((PROJECT_ROOT / path).read_bytes()).hexdigest()
        for path in source_paths
    }
    input_text = json.dumps(inputs, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    database, schema, version = db_session.execute(text(
        "SELECT current_database(), current_schema(), current_setting('server_version')"
    )).one()
    report = {
        "milestone": milestone,
        "check": name,
        "status": status,
        "run_id": evidence_dir.name,
        "input_sha256": hashlib.sha256(input_text.encode()).hexdigest(),
        "inputs": inputs,
        "database": {"database": database, "schema": schema, "postgresql_version": version},
        "configuration": {
            "python_version": __import__("sys").version.split()[0],
            "worker_application_name": application_name,
            "cloud_requests_expected": 0,
            "test_order_database_imported_for_assessment": False,
        },
        "data_sources": data_sources,
        "source_sha256": source_hashes,
        "observations": observations,
    }
    (evidence_dir / f"{name}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )


def _settings(db_session, data_dir, *, application_name=None, assessment_orders=False):
    url = make_url(db_session.info["test_database_url"])
    if application_name is not None:
        url = url.update_query_dict({"application_name": application_name})
    values = {
        "database_url": url.render_as_string(hide_password=False),
        "data_dir": data_dir,
        "poster_dir": data_dir.parent / "poster",
        "session_secret": SESSION_SECRET,
    }
    if assessment_orders:
        values.update({
            "assessment_order_adapter": "shop.assessment_order_adapter:create_order_port",
            "assessment_order_database_url": ASSESSMENT_ORDER_DATABASE,
            "assessment_order_source_file": ORDER_FILE,
        })
    return Settings(**values)


def _orders_for_customer(directory, customer_id):
    with (directory / "SalesOrderHeader.csv").open(encoding="utf-8", newline="") as source:
        return [row[7] for row in csv.reader(source, delimiter="\t") if row[10] == str(customer_id)]


def _customer_users(db_session):
    users = [
        User(username=username, password_hash=hash_password(TEST_PASSWORD), role="customer")
        for username in ("if02-customer-11091", "if02-customer-11176", "if02-customer-unbound")
    ]
    db_session.add_all(users)
    db_session.flush()
    bind_customer(db_session, users[0].id, 11091)
    bind_customer(db_session, users[1].id, 11176)
    db_session.commit()
    return users


def _worker_lock_activity(engine, application_name):
    statement = text(
        "SELECT pid, state, query, wait_event_type, wait_event FROM pg_stat_activity "
        "WHERE application_name = :application_name AND pid <> pg_backend_pid() "
        "ORDER BY pid"
    )
    with engine.connect() as connection:
        rows = connection.execute(statement, {"application_name": application_name}).mappings()
        return [dict(row) for row in rows]


def test_if02_if05_customer_identity_orders_and_worker_schedule(
    db_session, data_dir, permissions_evidence_dir
):
    manifest = source_manifest(ORDER_DIRECTORY)
    import_result = import_orders(db_session, ORDER_DIRECTORY)
    assert import_result["counts"] == CUSTOMER_COUNTS
    users = _customer_users(db_session)
    first_orders = _orders_for_customer(ORDER_DIRECTORY, 11091)
    second_orders = _orders_for_customer(ORDER_DIRECTORY, 11176)
    assert first_orders and second_orders
    application_name = "if05-worker-" + permissions_evidence_dir.name[-24:]
    settings = _settings(db_session, data_dir, application_name=application_name)
    data_sources = {
        "customer_order_source": manifest,
        "import_result": import_result,
        "account_bindings": {users[0].username: 11091, users[1].username: 11176,
                             users[2].username: None},
    }
    inputs = {
        "source_revision": CUSTOMER_REVISION,
        "first_order_number": first_orders[0],
        "second_order_number": second_orders[0],
        "customer_ids": [11091, 11176],
        "submit_request_id": SUBMIT_REQUEST_ID,
        "submit_message": "查询我的订单",
        "conversation_title": "IF-02身份与IF-05客服调度检查",
    }
    observations = {}
    completed = False
    _report(permissions_evidence_dir, "if02-customer-identity-orders", "IF-02",
            db_session, inputs, data_sources, observations, "running", application_name)
    try:
        async def exercise_python_interface():
            gateway = ShopIntegration(settings)
            lock_engine = None
            lock_connection = None
            lock_key = None
            try:
                await gateway.startup()
                lock_engine = create_engine(settings.database_url, pool_pre_ping=True)
                lock_connection = lock_engine.connect()
                first = gateway.login(users[0].username, TEST_PASSWORD,
                                      "00000000-0000-4000-8000-000000000311")
                second = gateway.login(users[1].username, TEST_PASSWORD,
                                       "00000000-0000-4000-8000-000000000312")
                unbound = gateway.login(users[2].username, TEST_PASSWORD,
                                        "00000000-0000-4000-8000-000000000313")
                first_identity = (await gateway.invoke("customer", "identity", {}, first)).data
                first_listing = (await gateway.invoke("customer", "orders", {}, first)).data
                first_order = (await gateway.invoke(
                    "customer", "order", {"order_number": first_orders[0]}, first
                )).data
                second_identity = (await gateway.invoke("customer", "identity", {}, second)).data
                second_order = (await gateway.invoke(
                    "customer", "order", {"order_number": second_orders[0]}, second
                )).data
                with pytest.raises(LookupError):
                    await gateway.invoke("customer", "order", {"order_number": second_orders[0]}, first)
                with pytest.raises(PermissionError):
                    await gateway.invoke("customer", "identity", {}, unbound)
                with pytest.raises(InvalidInput):
                    await gateway.invoke("customer", "orders", {"customer_id": 11176}, first)

                with pytest.raises(PermissionError):
                    await gateway.invoke("customer", "conversation", {"conversation_id":
                        "00000000-0000-4000-8000-000000000399"}, second)
                conversation = (await gateway.invoke(
                    "customer", "create_conversation", {"title": inputs["conversation_title"]},
                    gateway.login(users[0].username, TEST_PASSWORD, CREATE_CONVERSATION_REQUEST_ID),
                )).data
                conversation_id = conversation["id"]
                lock_key = "customer-conversation:" + conversation_id
                lock_connection.execute(
                    text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"), {"key": lock_key}
                )
                lock_connection.commit()
                probe_engine = create_engine(settings.database_url, pool_pre_ping=True)
                with probe_engine.connect() as probe:
                    acquired = probe.scalar(
                        text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                        {"key": lock_key},
                    )
                    probe.commit()
                    if acquired:
                        probe.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                                      {"key": lock_key})
                        probe.commit()
                probe_engine.dispose()
                assert acquired is False

                submit_context = gateway.login(users[0].username, TEST_PASSWORD, SUBMIT_REQUEST_ID)
                submit_payload = {"conversation_id": conversation_id, "message": inputs["submit_message"]}
                before_provider_requests = gateway.maker().scalar(
                    select(func.count()).select_from(ProviderRequest)
                )
                submitted = await gateway.invoke("customer", "submit", submit_payload, submit_context)
                turn_id = submitted.data["id"]
                repeated = await gateway.invoke("customer", "submit", submit_payload, submit_context)
                with pytest.raises(ValueError, match="相同请求编号"):
                    await gateway.invoke("customer", "submit", {
                        "conversation_id": conversation_id, "message": "查询其他订单"
                    }, submit_context)
                with pytest.raises(PermissionError):
                    await gateway.invoke("customer", "conversation", {
                        "conversation_id": conversation_id
                    }, second)
                with pytest.raises(PermissionError):
                    await gateway.invoke("customer", "submit", {
                        "conversation_id": conversation_id, "message": "查询我的订单"
                    }, gateway.login(users[1].username, TEST_PASSWORD,
                                     CROSS_CUSTOMER_SUBMIT_REQUEST_ID))

                worker_task = gateway.worker.tasks.get(turn_id)
                await asyncio.sleep(0.25)
                worker_lock_activity = _worker_lock_activity(lock_engine, application_name)
                with gateway.maker() as session:
                    waiting_turn = session.get(CustomerTurn, turn_id)
                    conversation_count = session.scalar(
                        select(func.count()).select_from(CustomerConversation)
                    )
                    turn_count = session.scalar(select(func.count()).select_from(CustomerTurn))
                    provider_requests_waiting = session.scalar(
                        select(func.count()).select_from(ProviderRequest)
                    )
                assert worker_task is not None
                assert not worker_task.done()
                assert waiting_turn.status == "queued"
                assert waiting_turn.cancel_requested is False
                assert repeated.data["id"] == turn_id
                assert turn_count == 1
                assert provider_requests_waiting == before_provider_requests == 0

                cancel_context = gateway.login(users[0].username, TEST_PASSWORD, CANCEL_REQUEST_ID)
                cancelled = await gateway.invoke("customer", "cancel", {"turn_id": turn_id}, cancel_context)
                lock_connection.execute(
                    text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"), {"key": lock_key}
                )
                lock_connection.commit()
                lock_key = None
                await asyncio.wait_for(worker_task, timeout=5)
                await asyncio.sleep(0)
                with gateway.maker() as session:
                    final_turn = session.get(CustomerTurn, turn_id)
                    final_provider_requests = session.scalar(
                        select(func.count()).select_from(ProviderRequest)
                    )
                assert turn_id not in gateway.worker.tasks
                assert cancelled.data["status"] == final_turn.status == "cancelled"
                assert final_turn.cancel_requested is True
                assert final_provider_requests == before_provider_requests == 0
                return {
                    "python_identity_customer_ids": [first_identity["customer_id"],
                                                     second_identity["customer_id"]],
                    "python_order_numbers": [first_order["order_number"], second_order["order_number"]],
                    "first_customer_order_total": first_listing["total"],
                    "conversation_id": conversation_id,
                    "turn_id": turn_id,
                    "worker_task_name": worker_task.get_name(),
                    "worker_lock_activity": worker_lock_activity,
                    "conversation_count": conversation_count,
                    "turn_count": turn_count,
                    "provider_requests_before": before_provider_requests,
                    "provider_requests_after": final_provider_requests,
                    "turn_status_after_cancel": final_turn.status,
                }
            finally:
                try:
                    await gateway.shutdown()
                finally:
                    if lock_key is not None and lock_connection is not None:
                        lock_connection.execute(
                            text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                            {"key": lock_key},
                        )
                        lock_connection.commit()
                    if lock_connection is not None:
                        lock_connection.close()
                    if lock_engine is not None:
                        lock_engine.dispose()

        observations["python"] = asyncio.run(exercise_python_interface())
        _report(permissions_evidence_dir, "if02-customer-identity-orders", "IF-02",
                db_session, inputs, data_sources, observations, "running", application_name)
        assert observations["python"]["python_identity_customer_ids"] == [11091, 11176]
        assert observations["python"]["python_order_numbers"] == [first_orders[0], second_orders[0]]
        assert observations["python"]["first_customer_order_total"] == 28

        app = create_app(settings)
        with TestClient(app) as client:
            assert client.post("/api/login", json={
                "username": users[0].username, "password": TEST_PASSWORD,
            }).status_code == 200
            body = {"version": "1", "request_id": "00000000-0000-4000-8000-000000000321",
                    "payload": {}}
            http_identity = client.post("/api/modules/customer/operations/identity", json=body)
            http_orders = client.post("/api/modules/customer/operations/orders", json=body)
            http_cross_order = client.post("/api/modules/customer/operations/order", json={
                **body, "payload": {"order_number": second_orders[0]},
            })
            http_payload_identity = client.post("/api/modules/customer/operations/orders", json={
                **body, "request_id": "00000000-0000-4000-8000-000000000322",
                "payload": {"customer_id": 11176},
            })
            http_envelope_identity = client.post("/api/modules/customer/operations/orders", json={
                **body, "customer_id": 11176,
            })
            assert client.post("/api/logout").status_code == 200
            assert client.post("/api/login", json={
                "username": users[1].username, "password": TEST_PASSWORD,
            }).status_code == 200
            second_http_identity = client.post(
                "/api/modules/customer/operations/identity", json=body
            )
            assert client.post("/api/logout").status_code == 200
            assert client.post("/api/login", json={
                "username": users[2].username, "password": TEST_PASSWORD,
            }).status_code == 200
            unbound_http_identity = client.post(
                "/api/modules/customer/operations/identity", json=body
            )
        assert http_identity.status_code == http_orders.status_code == 200
        assert http_identity.json()["data"]["customer_id"] == 11091
        assert http_orders.json()["data"]["total"] == 28
        assert http_cross_order.status_code == 404
        assert http_payload_identity.status_code == http_envelope_identity.status_code == 422
        assert second_http_identity.status_code == 200
        assert second_http_identity.json()["data"]["customer_id"] == 11176
        assert unbound_http_identity.status_code == 403
        observations["http"] = {
            "first_identity_customer_id": http_identity.json()["data"]["customer_id"],
            "first_order_total": http_orders.json()["data"]["total"],
            "cross_customer_order_status": http_cross_order.status_code,
            "payload_customer_id_status": http_payload_identity.status_code,
            "envelope_customer_id_status": http_envelope_identity.status_code,
            "second_identity_customer_id": second_http_identity.json()["data"]["customer_id"],
            "unbound_identity_status": unbound_http_identity.status_code,
        }
        _report(permissions_evidence_dir, "if02-customer-identity-orders", "IF-02",
                db_session, inputs, data_sources, observations, "passed", application_name)
        worker_inputs = {**inputs, "worker_turn_id": observations["python"]["turn_id"]}
        _report(permissions_evidence_dir, "if05-customer-worker-scheduling", "IF-05",
                db_session, worker_inputs, data_sources, observations["python"], "passed",
                application_name)
        completed = True
    finally:
        if not completed:
            _report(permissions_evidence_dir, "if02-customer-identity-orders", "IF-02",
                    db_session, inputs, data_sources, observations, "failed", application_name)


def test_if05_customer_worker_restarts_same_gateway(
    db_session, data_dir, permissions_evidence_dir
):
    manifest = source_manifest(ORDER_DIRECTORY)
    import_result = import_orders(db_session, ORDER_DIRECTORY)
    users = _customer_users(db_session)
    application_name = "if05-restart-" + permissions_evidence_dir.name[-24:]
    settings = _settings(db_session, data_dir, application_name=application_name)
    data_sources = {
        "customer_order_source": manifest,
        "import_result": import_result,
        "account_binding": {users[0].username: 11091},
    }
    inputs = {
        "source_revision": CUSTOMER_REVISION,
        "customer_id": 11091,
        "startup_cycles": ["startup", "shutdown", "startup"],
        "conversation_request_id": RESTART_CONVERSATION_REQUEST_ID,
        "submit_request_id": RESTART_SUBMIT_REQUEST_ID,
        "cancel_request_id": RESTART_CANCEL_REQUEST_ID,
        "submit_message": "查询我的订单",
    }
    observations = {}
    completed = False
    _report(permissions_evidence_dir, "if05-customer-worker-restart", "IF-05",
            db_session, inputs, data_sources, observations, "running", application_name)
    try:
        async def exercise_restarted_worker():
            gateway = ShopIntegration(settings)
            worker = gateway.worker
            lock_engine = None
            lock_connection = None
            lock_key = None
            try:
                await gateway.startup()
                await gateway.shutdown()
                await gateway.startup()
                same_worker = gateway.worker is worker
                stopping_after_restart = worker.stopping
                context = gateway.login(users[0].username, TEST_PASSWORD,
                                        RESTART_CONVERSATION_REQUEST_ID)
                conversation = await gateway.invoke(
                    "customer", "create_conversation", {"title": "IF-05 worker重启调度检查"}, context
                )
                conversation_id = conversation.data["id"]
                lock_key = "customer-conversation:" + conversation_id
                lock_engine = create_engine(settings.database_url, pool_pre_ping=True)
                lock_connection = lock_engine.connect()
                lock_connection.execute(
                    text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"),
                    {"key": lock_key},
                )
                lock_connection.commit()
                with lock_engine.connect() as probe:
                    lock_was_held = probe.scalar(
                        text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                        {"key": lock_key},
                    ) is False
                    probe.commit()
                assert lock_was_held is True

                before_provider_requests = gateway.maker().scalar(
                    select(func.count()).select_from(ProviderRequest)
                )
                submitted = await gateway.invoke("customer", "submit", {
                    "conversation_id": conversation_id,
                    "message": inputs["submit_message"],
                }, gateway.login(users[0].username, TEST_PASSWORD, RESTART_SUBMIT_REQUEST_ID))
                turn_id = submitted.data["id"]
                worker_task = worker.tasks.get(turn_id)
                await asyncio.sleep(0.25)
                worker_task_exception = None
                if (worker_task is not None and worker_task.done()
                        and not worker_task.cancelled()):
                    error = worker_task.exception()
                    if error is not None:
                        worker_task_exception = {
                            "type": type(error).__name__,
                            "message": str(error),
                        }
                worker_lock_activity = _worker_lock_activity(lock_engine, application_name)
                with gateway.maker() as session:
                    waiting_turn = session.get(CustomerTurn, turn_id)
                    provider_requests_waiting = session.scalar(
                        select(func.count()).select_from(ProviderRequest)
                    )
                worker_done_before_cancel = (
                    worker_task.done() if worker_task is not None else None
                )

                cancelled = await gateway.invoke(
                    "customer", "cancel", {"turn_id": turn_id},
                    gateway.login(users[0].username, TEST_PASSWORD, RESTART_CANCEL_REQUEST_ID),
                )
                lock_connection.execute(
                    text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                    {"key": lock_key},
                )
                lock_connection.commit()
                lock_key = None
                if worker_task is not None:
                    await asyncio.wait_for(worker_task, timeout=5)
                    await asyncio.sleep(0)
                with gateway.maker() as session:
                    final_turn = session.get(CustomerTurn, turn_id)
                    final_provider_requests = session.scalar(
                        select(func.count()).select_from(ProviderRequest)
                    )
                return {
                    "same_worker_instance": same_worker,
                    "worker_stopping_after_restart": stopping_after_restart,
                    "advisory_lock_held_by_test_connection": lock_was_held,
                    "worker_task_registered": worker_task is not None,
                    "worker_task_done_before_cancel": worker_done_before_cancel,
                    "worker_task_exception_before_cancel": worker_task_exception,
                    "worker_lock_activity": worker_lock_activity,
                    "turn_status_while_locked": waiting_turn.status,
                    "provider_requests_before": before_provider_requests,
                    "provider_requests_while_locked": provider_requests_waiting,
                    "cancelled_status": cancelled.data["status"],
                    "turn_status_after_cancel": final_turn.status,
                    "worker_task_removed_after_cancel": turn_id not in worker.tasks,
                    "provider_requests_after": final_provider_requests,
                }
            finally:
                try:
                    await gateway.shutdown()
                finally:
                    if lock_key is not None and lock_connection is not None:
                        lock_connection.execute(
                            text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                            {"key": lock_key},
                        )
                        lock_connection.commit()
                    if lock_connection is not None:
                        lock_connection.close()
                    if lock_engine is not None:
                        lock_engine.dispose()

        observations.update(asyncio.run(exercise_restarted_worker()))
        _report(permissions_evidence_dir, "if05-customer-worker-restart", "IF-05",
                db_session, inputs, data_sources, observations, "running", application_name)
        assert observations["same_worker_instance"] is True
        assert observations["worker_stopping_after_restart"] is False
        assert observations["advisory_lock_held_by_test_connection"] is True
        assert observations["worker_task_registered"] is True
        assert observations["worker_task_done_before_cancel"] is False
        assert observations["turn_status_while_locked"] == "queued"
        assert observations["provider_requests_before"] == 0
        assert observations["provider_requests_while_locked"] == 0
        assert observations["cancelled_status"] == "cancelled"
        assert observations["turn_status_after_cancel"] == "cancelled"
        assert observations["worker_task_removed_after_cancel"] is True
        assert observations["provider_requests_after"] == 0
        _report(permissions_evidence_dir, "if05-customer-worker-restart", "IF-05",
                db_session, inputs, data_sources, observations, "passed", application_name)
        completed = True
    finally:
        if not completed:
            _report(permissions_evidence_dir, "if05-customer-worker-restart", "IF-05",
                    db_session, inputs, data_sources, observations, "failed", application_name)


def test_if02_if05_assessment_scopes_atomic_create_and_safe_controls(
    db_session, data_dir, assessment_accounts, permissions_evidence_dir
):
    source = db_session.scalars(select(AssessmentSource)).one()
    employee_count = db_session.scalar(select(func.count()).select_from(AssessmentEmployee))
    accounts = assessment_accounts
    application_name = "if02-assessment-" + permissions_evidence_dir.name[-24:]
    settings = _settings(db_session, data_dir, application_name=application_name,
                         assessment_orders=True)
    inputs = {
        "assessment_source_id": ASSESSMENT_SOURCE_ID,
        "assessment_revision": ASSESSMENT_REVISION,
        "employee_ids": {"employee": 275, "peer": 276, "manager": 285,
                         "peer_manager": 286},
        "managed_team": {"manager": [275], "peer_manager": [276]},
        "official_order_source_file": str(ORDER_FILE.relative_to(PROJECT_ROOT)),
        "assessment_create_request_id": "00000000-0000-4000-8000-000000000331",
        "assessment_create_prompt": "查询本人2023年第三季度销售额和订单数",
    }
    data_sources = {
        "employee_source": {
            "source_id": source.source_id,
            "revision": source.revision,
            "fingerprint": source.fingerprint,
            "manifest": source.manifest,
            "file_sha256": _directory_sha256(SOURCE_DIRECTORY),
            "employee_count": employee_count,
        },
        "official_order_file_sha256": hashlib.sha256(ORDER_FILE.read_bytes()).hexdigest(),
        "assessment_order_database": "主 PostgreSQL shop，端口55432，只读连接",
        "assessment_schema_customer_order_count": db_session.scalar(
            select(func.count()).select_from(CustomerOrder)
        ),
    }
    observations = {}
    completed = False
    _report(permissions_evidence_dir, "if02-assessment-permissions", "IF-02",
            db_session, inputs, data_sources, observations, "running", application_name)
    try:
        async def exercise_assessment_interface():
            gateway = ShopIntegration(settings)
            try:
                await gateway.startup()
                contexts = {
                    name: gateway.login(user.username, "assessment-test-password",
                                        f"00000000-0000-4000-8000-00000000034{index}")
                    for index, (name, user) in enumerate(accounts.items(), start=1)
                }
                before_runs = gateway.maker().scalar(select(func.count()).select_from(AssessmentRun))
                before_calls = gateway.maker().scalar(select(func.count()).select_from(IntegrationCall))
                before_requests = gateway.maker().scalar(select(func.count()).select_from(ProviderRequest))
                with pytest.raises(LookupError):
                    await gateway.invoke("assessment", "create", {
                        "prompt": "IF-02 不存在的上游报告追问",
                        "parent_id": "00000000-0000-4000-8000-000000000399",
                    }, gateway.login(accounts["employee"].username, "assessment-test-password",
                                     "00000000-0000-4000-8000-000000000332"))
                with gateway.maker() as session:
                    assert session.scalar(select(func.count()).select_from(AssessmentRun)) == before_runs
                    assert session.scalar(select(func.count()).select_from(IntegrationCall)) == before_calls

                owner_context = gateway.login(accounts["employee"].username, "assessment-test-password",
                                              inputs["assessment_create_request_id"])
                create_payload = {"prompt": inputs["assessment_create_prompt"]}
                created = await gateway.invoke("assessment", "create", create_payload, owner_context)
                repeated = await gateway.invoke("assessment", "create", create_payload, owner_context)
                with pytest.raises(ValueError, match="相同请求编号"):
                    await gateway.invoke("assessment", "create", {
                        "prompt": "IF-02 相同编号但不同考核需求"
                    }, owner_context)
                run_id = created.data["id"]
                with gateway.maker() as session:
                    persisted_run = session.get(AssessmentRun, run_id)
                    create_calls = session.scalars(select(IntegrationCall).where(
                        IntegrationCall.actor_id == accounts["employee"].id,
                        IntegrationCall.module_id == "assessment",
                        IntegrationCall.operation == "create",
                        IntegrationCall.request_id == inputs["assessment_create_request_id"],
                    )).all()
                    run_count = session.scalar(select(func.count()).select_from(AssessmentRun))
                    provider_request_count = session.scalar(
                        select(func.count()).select_from(ProviderRequest)
                    )
                assert run_count == before_runs + 1
                assert len(create_calls) == 1
                assert create_calls[0].resource_id == persisted_run.id == repeated.data["id"]
                assert provider_request_count == before_requests == 0

                employee_report = await gateway.invoke("assessment", "show", {"run_id": run_id},
                                                       contexts["employee"])
                manager_report = await gateway.invoke("assessment", "show", {"run_id": run_id},
                                                      contexts["manager"])
                admin_report = await gateway.invoke("assessment", "show", {"run_id": run_id},
                                                    contexts["admin"])
                with pytest.raises(PermissionError):
                    await gateway.invoke("assessment", "show", {"run_id": run_id},
                                         contexts["other"])
                with pytest.raises(PermissionError):
                    await gateway.invoke("assessment", "show", {"run_id": run_id},
                                         contexts["manager2"])

                employee_metrics = await gateway.invoke("assessment", "metrics", {
                    "scope": "self",
                }, gateway.login(accounts["employee"].username, "assessment-test-password",
                                 "00000000-0000-4000-8000-000000000333"))
                manager_metrics = await gateway.invoke("assessment", "metrics", {
                    "scope": "team",
                }, gateway.login(accounts["manager"].username, "assessment-test-password",
                                 "00000000-0000-4000-8000-000000000334"))
                manager_self_metrics = await gateway.invoke("assessment", "metrics", {
                    "scope": "self",
                }, gateway.login(accounts["manager"].username, "assessment-test-password",
                                 "00000000-0000-4000-8000-000000000346"))
                peer_metrics = await gateway.invoke("assessment", "metrics", {
                    "scope": "self",
                }, gateway.login(accounts["other"].username, "assessment-test-password",
                                 "00000000-0000-4000-8000-000000000335"))
                admin_metrics = await gateway.invoke("assessment", "metrics", {
                    "scope": "organization",
                }, gateway.login(accounts["admin"].username, "assessment-test-password",
                                 "00000000-0000-4000-8000-000000000336"))
                with pytest.raises(PermissionError):
                    await gateway.invoke("assessment", "metrics", {
                        "scope": "employees", "employee_ids": [276],
                    }, gateway.login(accounts["manager"].username, "assessment-test-password",
                                     "00000000-0000-4000-8000-000000000337"))
                with pytest.raises(PermissionError):
                    await gateway.invoke("assessment", "metrics", {
                        "scope": "employees", "employee_ids": [275],
                    }, gateway.login(accounts["other"].username, "assessment-test-password",
                                     "00000000-0000-4000-8000-000000000338"))

                cancel_context = gateway.login(accounts["employee"].username, "assessment-test-password",
                                               "00000000-0000-4000-8000-000000000339")
                cancel_run = await gateway.invoke("assessment", "create", {
                    "prompt": "IF-02 取消与继续执行权限检查"
                }, cancel_context)
                cancel_id = cancel_run.data["id"]
                with pytest.raises(PermissionError):
                    await gateway.invoke("assessment", "cancel", {"run_id": cancel_id},
                                         gateway.login(accounts["other"].username,
                                                       "assessment-test-password",
                                                       "00000000-0000-4000-8000-000000000340"))
                cancelled = await gateway.invoke("assessment", "cancel", {"run_id": cancel_id},
                                                 gateway.login(accounts["employee"].username,
                                                               "assessment-test-password",
                                                               "00000000-0000-4000-8000-000000000341"))
                cancelled_resume = await gateway.invoke("assessment", "resume", {
                    "run_id": cancel_id,
                }, gateway.login(accounts["employee"].username, "assessment-test-password",
                                 "00000000-0000-4000-8000-000000000342"))
                with gateway.maker() as session:
                    final_request_count = session.scalar(
                        select(func.count()).select_from(ProviderRequest)
                    )
                    final_run_count = session.scalar(select(func.count()).select_from(AssessmentRun))
                assert cancelled.data["status"] == cancelled_resume.data["status"] == "cancelled"
                assert final_request_count == before_requests == 0
                assert final_run_count == before_runs + 2
                return {
                    "employee_report_status": employee_report.data["status"],
                    "manager_report_status": manager_report.data["status"],
                    "admin_report_status": admin_report.data["status"],
                    "employee_self_ids": [row["employee_id"] for row in employee_metrics.data["employees"]],
                    "manager_self_ids": [row["employee_id"] for row in manager_self_metrics.data["employees"]],
                    "manager_team_ids": [row["employee_id"] for row in manager_metrics.data["employees"]],
                    "peer_self_ids": [row["employee_id"] for row in peer_metrics.data["employees"]],
                    "admin_organization_count": len(admin_metrics.data["employees"]),
                    "admin_unassigned_order_count": admin_metrics.data["unassigned"]["metrics"][1]["value"],
                    "official_order_snapshot": admin_metrics.data["source"]["orders"],
                    "created_run_id": run_id,
                    "create_run_count": run_count,
                    "create_mapping_count": len(create_calls),
                    "cancelled_status": cancelled.data["status"],
                    "cancelled_resume_status": cancelled_resume.data["status"],
                    "provider_request_count": final_request_count,
                    "final_run_count": final_run_count,
                }
            finally:
                try:
                    await gateway.shutdown()
                finally:
                    gateway.metrics.order_port.engine.dispose()

        observations.update(asyncio.run(exercise_assessment_interface()))
        _report(permissions_evidence_dir, "if02-assessment-permissions", "IF-02",
                db_session, inputs, data_sources, observations, "running", application_name)
        assert observations["employee_report_status"] == "created"
        assert observations["manager_report_status"] == "created"
        assert observations["admin_report_status"] == "created"
        assert observations["employee_self_ids"] == [275]
        assert observations["manager_self_ids"] == [285]
        assert observations["manager_team_ids"] == [275]
        assert observations["peer_self_ids"] == [276]
        assert observations["admin_organization_count"] == 17
        assert observations["admin_unassigned_order_count"] == "705"
        assert observations["official_order_snapshot"]["row_count"] == 1022
        assert observations["official_order_snapshot"]["assigned_count"] == 317
        assert observations["create_run_count"] == 1
        assert observations["create_mapping_count"] == 1
        assert observations["provider_request_count"] == 0
        _report(permissions_evidence_dir, "if02-assessment-permissions", "IF-02",
                db_session, inputs, data_sources, observations, "passed", application_name)
        _report(permissions_evidence_dir, "if05-assessment-create-controls", "IF-05",
                db_session, inputs, data_sources, observations, "passed", application_name)
        completed = True
    finally:
        if not completed:
            _report(permissions_evidence_dir, "if02-assessment-permissions", "IF-02",
                    db_session, inputs, data_sources, observations, "failed", application_name)
