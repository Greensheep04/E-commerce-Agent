import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from data_center_protocol import DataQuery
from integration_protocol import CallContext, OperationResult
from opentelemetry.sdk.trace import TracerProvider
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from data_center.api import create_app
from data_center.config import Settings
from data_center.observability import RunObserver
from data_center.runtime import DataRuntime
from data_center.store import DataStore, DataTask

ROOT = Path(__file__).resolve().parents[2]
IDENTITY_PATH = ROOT / ".scratch/evidence/dc02-host-20261004-r3/identities.json"
DATA_CENTER_API_URL = "http://127.0.0.1:8100"


def test_observer_running_does_not_choose_terminal_state_from_exception():
    run_id, task_id = str(uuid4()), str(uuid4())
    directory = ROOT / ".scratch/evidence" / f"dc06-lineage-deferred-{run_id}"
    observations = {"run_id": run_id, "task_id": task_id, "status": "started"}
    observer = RunObserver(directory, provider=TracerProvider())
    try:
        with (pytest.raises(ValueError, match="source lookup stopped"),
              observer.running(task_id, run_id, "query", ["test/source"])):
            raise ValueError("source lookup stopped")
        events = [json.loads(line) for line in (directory / "lineage.jsonl").read_text(
            encoding="utf-8").splitlines()]
        observations["event_types_after_exception"] = [event["eventType"] for event in events]
        assert observations["event_types_after_exception"] == ["START"]

        observer.fail(run_id, "query", ["test/source"])
        observer.close()
        events = [json.loads(line) for line in (directory / "lineage.jsonl").read_text(
            encoding="utf-8").splitlines()]
        observations.update({"event_types_after_final_state": [event["eventType"] for event in events],
                             "event_run_ids": [event["run"]["runId"] for event in events],
                             "status": "passed"})
        assert observations["event_types_after_final_state"] == ["START", "FAIL"]
        assert observations["event_run_ids"] == [run_id, run_id]
    except Exception as error:
        observations.update({"status": "failed", "failure": f"{type(error).__name__}: {error}"})
        raise
    finally:
        observer.close()
        observations["source_sha256"] = {
            path: sha256((ROOT / path).read_bytes()).hexdigest()
            for path in (
                "data-center/src/data_center/observability.py",
                "data-center/tests/test_cancel_race_persistence.py",
            )
        }
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "report.json").write_text(
            json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_runtime_terminal_lineage_follows_persisted_postgresql_status():
    verification_run_id = str(uuid4())
    schema = "dc_lineage_" + uuid4().hex
    database = create_engine(
        "postgresql+psycopg://shop@127.0.0.1:55432/shop_test",
        pool_pre_ping=True,
        execution_options={"schema_translate_map": {None: schema}},
    )
    with database.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    directory = ROOT / ".scratch/evidence" / f"dc06-lineage-persisted-{verification_run_id}"
    observations = {"verification_run_id": verification_run_id, "schema": schema, "status": "started"}
    observer = RunObserver(directory / "observability", provider=TracerProvider())
    try:
        DataTask.__table__.create(database)
        store = DataStore(database)
        runtime = DataRuntime(store, None, None, None, observer=observer)
        cases = (
            ("completed", "completed", {"snapshots": [{"snapshot_id": "completed-snapshot"}]}, None,
             "COMPLETE", ["completed-snapshot"]),
            ("partial", "partial", {"snapshot": {"snapshot_id": "partial-snapshot"}}, None,
             "COMPLETE", ["partial-snapshot"]),
            ("cancelled", "cancelled", None, "user cancellation acknowledged", "ABORT", []),
            ("failed", "failed", None, "source read failed", "FAIL", []),
            ("blocked", "DATA_BLOCKED", {"unknowns": ["source is unavailable"]}, None, "OTHER", []),
            ("unknown", "unknown", {"unknowns": ["request is pending"]}, "request is pending", "OTHER", []),
            ("budget", "budget_exhausted", {"unknowns": ["budget limit"]}, None, "OTHER", []),
        )
        run_expectations = {}
        for label, status, data, error, expected_event, expected_outputs in cases:
            context = CallContext(actor_id="lineage-state-test", request_id=str(uuid4()))
            task = store.create("sync", {"source_id": f"test/{label}"}, context)
            task = store.update(task.task_id, status="running")
            with observer.running(task.task_id, task.run_id, task.operation, [f"test/{label}"]):
                pass
            result = None if data is None else OperationResult(
                run_id=task.run_id, status=status, data={"task_id": task.task_id, **data},
                costs={"spent_cny": "0.125", "reserved_cny": "0.004"},
            ).model_dump(mode="json")
            persisted = store.update(task.task_id, status=status, result=result, error=error)
            runtime._observe_terminal(persisted, [f"test/{label}"])
            run_expectations[task.run_id] = {
                "task_id": task.task_id,
                "persisted_status": persisted.status,
                "persisted_result": persisted.result,
                "persisted_error": persisted.error,
                "expected_event": expected_event,
                "expected_outputs": expected_outputs,
            }

        observer.close()
        events = [json.loads(line) for line in (directory / "observability/lineage.jsonl").read_text(
            encoding="utf-8").splitlines()]
        terminal_events = [event for event in events if event["eventType"] != "START"]
        observations.update({"database": {"driver": "postgresql", "host": "127.0.0.1",
                                           "port": 55432, "database": "shop_test", "schema": schema},
                             "run_expectations": run_expectations,
                             "event_types": [event["eventType"] for event in events],
                             "event_run_ids": [event["run"]["runId"] for event in events],
                             "terminal_outputs": {
                                 event["run"]["runId"]: [item["name"] for item in event.get("outputs", [])]
                                 for event in terminal_events
                             },
                             "status": "passed"})
        assert len(events) == len(cases) * 2
        assert [event["eventType"] for event in terminal_events] == [case[4] for case in cases]
        assert [event["run"]["runId"] for event in events] == [
            run_id for run_id in run_expectations for _ in range(2)
        ]
        assert observations["terminal_outputs"] == {
            run_id: value["expected_outputs"] for run_id, value in run_expectations.items()
        }
    except Exception as error:
        observations.update({"status": "failed", "failure": f"{type(error).__name__}: {error}"})
        raise
    finally:
        observer.close()
        database.dispose()
        with database.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        observations["source_sha256"] = {
            path: sha256((ROOT / path).read_bytes()).hexdigest()
            for path in (
                "data-center/src/data_center/store.py",
                "data-center/src/data_center/runtime.py",
                "data-center/src/data_center/observability.py",
                "data-center/tests/test_cancel_race_persistence.py",
            )
        }
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "report.json").write_text(
            json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_lineage_write_failure_does_not_change_saved_success():
    verification_run_id = str(uuid4())
    schema = "dc_lineage_error_" + uuid4().hex
    database = create_engine(
        "postgresql+psycopg://shop@127.0.0.1:55432/shop_test",
        pool_pre_ping=True,
        execution_options={"schema_translate_map": {None: schema}},
    )
    with database.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    directory = ROOT / ".scratch/evidence" / f"dc06-lineage-write-error-{verification_run_id}"
    observations = {"verification_run_id": verification_run_id, "schema": schema, "status": "started"}
    observer = RunObserver(directory / "observability", provider=TracerProvider())
    try:
        DataTask.__table__.create(database)
        store = DataStore(database)
        runtime = DataRuntime(store, None, None, None, observer=observer)
        context = CallContext(actor_id="lineage-write-error", request_id=str(uuid4()))
        task = store.create("sync", {"source_id": "test/lineage-write-error"}, context)
        task = store.update(task.task_id, status="running")
        with observer.running(task.task_id, task.run_id, task.operation, ["test/lineage-write-error"]):
            pass
        result = OperationResult(
            run_id=task.run_id,
            status="completed",
            data={"task_id": task.task_id, "snapshot": {"snapshot_id": "saved-snapshot"}},
            costs={"spent_cny": "0.125", "reserved_cny": "0.004"},
        ).model_dump(mode="json")
        persisted = store.update(task.task_id, status="completed", result=result)
        lineage_path = directory / "observability/lineage.jsonl"
        preserved_lineage_path = directory / "observability/start-events.jsonl"
        lineage_path.rename(preserved_lineage_path)
        lineage_path.mkdir()
        with pytest.raises(RuntimeError, match="Failed to write to log file") as write_error:
            runtime._observe_terminal(persisted, ["test/lineage-write-error"])
        current = store.get(task.task_id, context.actor_id)
        observations.update({"task_id": current.task_id,
                             "run_id": current.run_id,
                             "persisted_status": current.status,
                             "persisted_result": current.result,
                             "persisted_error": current.error,
                             "saved_result": result,
                             "lineage_write_error": f"{type(write_error.value).__name__}: {write_error.value}",
                             "status": "passed"})
        assert current.status == "completed"
        assert current.result == result
        assert current.error is None
    except Exception as error:
        observations.update({"status": "failed", "failure": f"{type(error).__name__}: {error}"})
        raise
    finally:
        observer.close()
        database.dispose()
        with database.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        observations["source_sha256"] = {
            path: sha256((ROOT / path).read_bytes()).hexdigest()
            for path in (
                "data-center/src/data_center/store.py",
                "data-center/src/data_center/runtime.py",
                "data-center/src/data_center/observability.py",
                "data-center/tests/test_cancel_race_persistence.py",
            )
        }
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "report.json").write_text(
            json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_postgresql_row_lock_preserves_first_terminal_commit():
    verification_run_id = str(uuid4())
    schema = "dc_row_lock_" + uuid4().hex
    database = create_engine(
        "postgresql+psycopg://shop@127.0.0.1:55432/shop_test",
        pool_pre_ping=True,
        execution_options={"schema_translate_map": {None: schema}},
    )
    with database.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    observations = {"verification_run_id": verification_run_id, "schema": schema, "status": "started"}
    writer_database = None
    try:
        with database.connect() as connection:
            observations["database"] = {
                "driver": "postgresql", "host": "127.0.0.1", "port": 55432,
                "database": "shop_test", "server_version": connection.scalar(text("SHOW server_version")),
                "data_directory": connection.scalar(text("SHOW data_directory")),
            }
        assert observations["database"]["data_directory"] == str(ROOT / ".local/postgres")
        DataTask.__table__.create(database)
        store = DataStore(database)
        cases = (
            ("completion_first", "completed", "cancelled"),
            ("cancellation_first", "cancelled", "completed"),
        )
        case_observations = {}
        for label, first_status, second_status in cases:
            context = CallContext(actor_id=f"row-lock-{label}", request_id=str(uuid4()))
            task = store.create("sync", {"source_id": f"test/{label}"}, context)
            store.update(task.task_id, status="running")
            app_name = f"dc06-{verification_run_id[:8]}-{label}"
            writer_backend_pids = []
            writer_database = create_engine(
                "postgresql+psycopg://shop@127.0.0.1:55432/shop_test",
                pool_pre_ping=True,
                execution_options={"schema_translate_map": {None: schema}},
                connect_args={"application_name": app_name, "options": "-c lock_timeout=600ms"},
            )
            event.listen(
                writer_database, "connect",
                lambda connection, record, backend_pids=writer_backend_pids: backend_pids.append(
                    connection.info.backend_pid),
            )
            completed_result = OperationResult(
                data={"task_id": task.task_id, "marker": label},
                run_id=task.run_id,
                status="completed",
                costs={"spent_cny": "0.125", "reserved_cny": "0.004"},
            ).model_dump(mode="json")
            holder = Session(database)
            held_task = holder.scalar(select(DataTask).where(
                DataTask.task_id == task.task_id).with_for_update())
            assert held_task is not None
            holder_pid = holder.scalar(text("SELECT pg_backend_pid()"))
            second_changes = (
                {"status": "cancelled", "cancel_requested": True}
                if second_status == "cancelled"
                else {"status": "completed", "result": completed_result, "error": None}
            )
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(DataStore(writer_database).update, task.task_id,
                                          **second_changes)
                try:
                    with pytest.raises(OperationalError) as lock_error:
                        pending.result(timeout=10)
                    assert lock_error.value.orig.sqlstate == "55P03"
                    if first_status == "completed":
                        held_task.status = "completed"
                        held_task.result = completed_result
                        held_task.error = None
                        held_task.cancel_requested = False
                    else:
                        held_task.status = "cancelled"
                        held_task.result = None
                        held_task.error = "user cancellation acknowledged"
                        held_task.cancel_requested = True
                    holder.flush()
                    holder.commit()
                finally:
                    if holder.in_transaction():
                        holder.rollback()
                    holder.close()
            updated = DataStore(writer_database).update(task.task_id, **second_changes)
            assert writer_backend_pids
            assert all(pid != holder_pid for pid in writer_backend_pids)
            persisted = store.get(task.task_id, context.actor_id)
            case_observations[label] = {
                "task_id": task.task_id,
                "run_id": task.run_id,
                "request_id": context.request_id,
                "backend_pids": [holder_pid, *writer_backend_pids],
                "distinct_backend_pids": all(pid != holder_pid for pid in writer_backend_pids),
                "lock_timeout_sqlstate": lock_error.value.orig.sqlstate,
                "first_committed_status": first_status,
                "second_requested_status": second_status,
                "second_writer_returned_status": updated.status,
                "persisted_status": persisted.status,
                "cancel_requested": persisted.cancel_requested,
                "result": persisted.result,
                "costs": None if persisted.result is None else persisted.result.get("costs"),
                "error": persisted.error,
            }
            assert persisted.status == first_status
            assert updated.status == first_status
            if first_status == "completed":
                assert persisted.result == completed_result
                assert persisted.result["costs"] == {"spent_cny": "0.125", "reserved_cny": "0.004"}
                assert persisted.error is None
                assert persisted.cancel_requested is True
            else:
                assert persisted.result is None
                assert persisted.error == "user cancellation acknowledged"
                assert persisted.cancel_requested is True
            writer_database.dispose()
            writer_database = None
        observations.update({"cases": case_observations, "status": "passed"})
    except Exception as error:
        observations.update({"status": "failed", "failure": f"{type(error).__name__}: {error}"})
        raise
    finally:
        if writer_database is not None:
            writer_database.dispose()
        database.dispose()
        with database.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        observations["source_sha256"] = {
            path: sha256((ROOT / path).read_bytes()).hexdigest()
            for path in (
                "data-center/src/data_center/store.py",
                "data-center/tests/test_cancel_race_persistence.py",
            )
        }
        directory = ROOT / ".scratch/evidence" / f"dc06-postgresql-row-lock-{verification_run_id}"
        directory.mkdir(parents=True, exist_ok=False)
        (directory / "report.json").write_text(
            json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_late_failure_does_not_overwrite_cancellation_in_postgresql():
    schema = "dc_cancel_" + uuid4().hex
    database = create_engine(
        "postgresql+psycopg://shop@127.0.0.1:55432/shop_test",
        pool_pre_ping=True,
        execution_options={"schema_translate_map": {None: schema}},
    )
    with database.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    context = CallContext(actor_id="cancel-race-test", request_id=str(uuid4()))
    observations = {
        "verification_run_id": str(uuid4()),
        "checked_at": datetime.now(UTC).isoformat(),
        "database": {"driver": "postgresql", "host": "127.0.0.1", "port": 55432,
                     "database": "shop_test", "schema": schema},
        "actor_id": context.actor_id,
        "request_id": context.request_id,
        "shop_http_calls": 0,
        "provider_requests": "not queried; this test exercises DataStore only",
        "status": "started",
    }
    try:
        with database.connect() as connection:
            observations["database"].update({
                "server_version": connection.scalar(text("SHOW server_version")),
                "data_directory": connection.scalar(text("SHOW data_directory")),
            })
        assert observations["database"]["data_directory"] == str(ROOT / ".local/postgres")
        DataTask.__table__.create(database)
        store = DataStore(database)
        task = store.create("sync", {"source_id": "cancel-race-test/source"}, context)
        observations.update({"task_id": task.task_id, "run_id": task.run_id,
                             "status_after_create": task.status})
        running = store.update(task.task_id, status="running")
        observations["status_before_cancellation"] = running.status
        cancelled = store.update(task.task_id, status="cancelled", cancel_requested=True,
                                 error="user cancellation acknowledged")
        observations["status_after_cancellation"] = cancelled.status

        late_failure = store.update(task.task_id, status="failed",
                                    error="PermissionError: task is cancelled")
        observations["status_after_late_failure_write"] = late_failure.status

        persisted = store.get(task.task_id, context.actor_id)
        cancelled_response = DataRuntime._stored_outcome(persisted)
        completed_context = CallContext(actor_id=context.actor_id, request_id=str(uuid4()))
        completed_task = store.create(
            "sync", {"source_id": "cancel-race-test/completed"}, completed_context)
        completed_result = OperationResult(
            data={"task_id": completed_task.task_id}, run_id=completed_task.run_id,
            status="completed",
        ).model_dump(mode="json")
        store.update(completed_task.task_id, status="completed", result=completed_result)
        stale_cancel = store.update(
            completed_task.task_id, status="cancelled", cancel_requested=True)
        late_completed_failure = store.update(
            completed_task.task_id, status="failed", error="late failure after completion")
        persisted_completed = store.get(completed_task.task_id, context.actor_id)
        completed_response = DataRuntime._stored_outcome(persisted_completed)

        terminal_result_observations = {}
        for terminal_status in ("partial", "DATA_BLOCKED", "unknown", "failed", "budget_exhausted"):
            terminal_context = CallContext(actor_id=context.actor_id, request_id=str(uuid4()))
            terminal_task = store.create(
                "sync", {"source_id": f"cancel-race-test/{terminal_status}"}, terminal_context)
            terminal_result = OperationResult(
                data={"task_id": terminal_task.task_id, "marker": terminal_status},
                run_id=terminal_task.run_id,
                status=terminal_status,
                costs={"spent_cny": "0.031", "reserved_cny": "0.004"},
            ).model_dump(mode="json")
            store.update(terminal_task.task_id, status=terminal_status, result=terminal_result)
            stale_terminal_cancel = store.update(
                terminal_task.task_id, status="cancelled", cancel_requested=True)
            persisted_terminal = store.get(terminal_task.task_id, context.actor_id)
            terminal_response = DataRuntime._stored_outcome(persisted_terminal)
            terminal_result_observations[terminal_status] = {
                "task_id": terminal_task.task_id,
                "run_id": terminal_task.run_id,
                "request_id": terminal_context.request_id,
                "status_after_stale_cancel": stale_terminal_cancel.status,
                "persisted_status": persisted_terminal.status,
                "returned_status": terminal_response.status,
                "result": persisted_terminal.result,
                "costs": persisted_terminal.result["costs"],
            }
            assert stale_terminal_cancel.status == terminal_status
            assert persisted_terminal.status == terminal_status
            assert persisted_terminal.result == terminal_result
            assert terminal_response.model_dump(mode="json") == terminal_result

        failed_context = CallContext(actor_id=context.actor_id, request_id=str(uuid4()))
        failed_task = store.create(
            "sync", {"source_id": "cancel-race-test/failed"}, failed_context)
        store.update(failed_task.task_id, status="running")
        persisted_failure = store.update(
            failed_task.task_id, status="failed", error="persisted failure without a result")
        stale_failed_cancel = store.update(
            failed_task.task_id, status="cancelled", cancel_requested=True)
        failed_after_cancel = store.get(failed_task.task_id, context.actor_id)
        failed_response = DataRuntime._stored_outcome(failed_after_cancel)

        budget_context = CallContext(actor_id=context.actor_id, request_id=str(uuid4()))
        budget_task = store.create(
            "sync", {"source_id": "cancel-race-test/budget"}, budget_context)
        store.update(budget_task.task_id, status="running")
        persisted_budget = store.update(
            budget_task.task_id, status="budget_exhausted", error="persisted budget limit")
        stale_budget_cancel = store.update(
            budget_task.task_id, status="cancelled", cancel_requested=True)
        budget_after_cancel = store.get(budget_task.task_id, context.actor_id)
        budget_response = DataRuntime._stored_outcome(budget_after_cancel)
        observations.update({"persisted_status": persisted.status,
                             "returned_status": cancelled_response.status,
                             "cancel_requested": persisted.cancel_requested,
                             "persisted_error": persisted.error,
                             "completed_task": {
                                 "task_id": completed_task.task_id,
                                 "run_id": completed_task.run_id,
                                 "request_id": completed_context.request_id,
                                 "status_after_stale_cancel_write": stale_cancel.status,
                                 "status_after_late_failure_write": late_completed_failure.status,
                                 "persisted_status": persisted_completed.status,
                                 "returned_status": completed_response.status,
                                 "result": persisted_completed.result,
                             },
                             "terminal_result_observations": terminal_result_observations,
                             "failed_task": {
                                 "task_id": failed_task.task_id,
                                 "run_id": failed_task.run_id,
                                 "request_id": failed_context.request_id,
                                 "status_before_stale_cancel": persisted_failure.status,
                                 "status_after_stale_cancel": stale_failed_cancel.status,
                                 "persisted_status": failed_after_cancel.status,
                                 "returned_status": failed_response.status,
                                 "error": failed_after_cancel.error,
                             },
                             "budget_task": {
                                 "task_id": budget_task.task_id,
                                 "run_id": budget_task.run_id,
                                 "request_id": budget_context.request_id,
                                 "status_before_stale_cancel": persisted_budget.status,
                                 "status_after_stale_cancel": stale_budget_cancel.status,
                                 "persisted_status": budget_after_cancel.status,
                                 "returned_status": budget_response.status,
                                 "error": budget_after_cancel.error,
                             },
                             "source_sha256": {
                                 path: sha256((ROOT / path).read_bytes()).hexdigest()
                                 for path in (
                                     "data-center/src/data_center/store.py",
                                     "data-center/src/data_center/runtime.py",
                                     "data-center/tests/test_cancel_race_persistence.py",
                                 )
                             },
                             "status": "passed"})
        assert persisted.status == "cancelled"
        assert cancelled_response.status == persisted.status
        assert persisted.cancel_requested is True
        assert persisted.error == "user cancellation acknowledged"
        assert stale_cancel.status == "completed"
        assert late_completed_failure.status == "completed"
        assert persisted_completed.status == "completed"
        assert completed_response.status == persisted_completed.status
        assert completed_response.model_dump(mode="json") == completed_result
        assert persisted_completed.result == completed_result
        assert persisted_failure.status == "failed"
        assert stale_failed_cancel.status == "failed"
        assert failed_after_cancel.status == "failed"
        assert failed_after_cancel.error == "persisted failure without a result"
        assert failed_response.status == "failed"
        assert persisted_budget.status == "budget_exhausted"
        assert stale_budget_cancel.status == "budget_exhausted"
        assert budget_after_cancel.status == "budget_exhausted"
        assert budget_after_cancel.error == "persisted budget limit"
        assert budget_response.status == "budget_exhausted"
    except Exception as error:
        observations["status"] = "failed"
        observations["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        with database.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        database.dispose()
        directory = ROOT / ".scratch/evidence" / f"dc06-cancel-race-db-{observations['verification_run_id']}"
        directory.mkdir(parents=True, exist_ok=False)
        (directory / "report.json").write_text(
            json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


@pytest.mark.skipif(os.environ.get("DC_CANCEL_LOCK_LIVE") != "1",
                    reason="需要显式启用真实宿主 HTTP、数据中台 HTTP 与 PostgreSQL 检查")
@pytest.mark.asyncio
async def test_cancelled_run_under_external_worker_lock_reads_persisted_status():
    run_id = str(uuid4())
    observations = {
        "verification_run_id": run_id,
        "checked_at": datetime.now(UTC).isoformat(),
        "data_center_api_url": DATA_CENTER_API_URL,
        "provider_requests_added": 0,
        "status": "started",
    }
    app = None
    task = None
    context = None
    lock_connection = None
    try:
        app = create_app(Settings())
        async with app.router.lifespan_context(app):
            try:
                identity = json.loads(IDENTITY_PATH.read_text(encoding="utf-8")).get("actors", {}).get("admin")
                if identity is None:
                    raise LookupError("真实验收身份清单缺少admin账号")
                password = (ROOT / identity["password_file"]).read_text(encoding="utf-8").strip()
                context = await app.state.ports.login(
                    identity["username"], password, str(uuid4()))
                catalog = await app.state.ports.catalog(context)
                source = next(item for item in catalog["items"] if item["kind"] == "document")
                payload = DataQuery(
                    question="并发取消状态检查",
                    intent="documents",
                    source_ids=[source["source_id"]],
                    limit=1,
                ).model_dump(mode="json")
                task = await app.state.runtime.submit("query", payload, context)
                usage_before = await app.state.ports.usage(task.task_id, context)
                assert usage_before["requests"] == []

                configured_database = make_url(app.state.runtime.store.database.url)
                with app.state.runtime.store.database.connect() as connection:
                    observations["database"] = {
                        "host": configured_database.host,
                        "port": configured_database.port,
                        "database": configured_database.database,
                        "server_version": connection.scalar(text("SHOW server_version")),
                        "connected_database": connection.scalar(text("SELECT current_database()")),
                        "connected_port": connection.scalar(text("SELECT inet_server_port()")),
                    }
                assert observations["database"]["database"] == observations["database"]["connected_database"]
                assert observations["database"]["port"] == observations["database"]["connected_port"]
                lock_connection = app.state.runtime.store.database.connect()
                lock_key = f"dc-worker:{task.task_id}"
                locked = lock_connection.scalar(
                    text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"),
                    {"key": lock_key},
                )
                assert locked is True

                cancel_request_id = str(uuid4())
                cancel_headers = {
                    "Authorization": "Bearer " + context.credential.get_secret_value(),
                    "X-Shop-Actor": context.actor_id,
                    "X-Request-ID": cancel_request_id,
                }
                run_request_id = str(uuid4())
                run_headers = {**cancel_headers, "X-Request-ID": run_request_id}
                async with httpx.AsyncClient(base_url=DATA_CENTER_API_URL, timeout=30) as client:
                    cancel_response = await client.post(
                        "/operations/cancel", headers=cancel_headers,
                        json={"payload": {"task_id": task.task_id}},
                    )
                    cancel_response.raise_for_status()
                    cancelled = cancel_response.json()
                    run_response = await client.post(
                        "/operations/run", headers=run_headers,
                        json={"payload": {"task_id": task.task_id}},
                    )
                    run_response.raise_for_status()
                    returned = run_response.json()

                persisted = await asyncio.to_thread(
                    app.state.runtime.store.get, task.task_id, context.actor_id)
                usage_after = await app.state.ports.usage(task.task_id, context)
                observations.update({
                    "actor_id": context.actor_id,
                    "source_id": source["source_id"],
                    "task_id": task.task_id,
                    "run_id": task.run_id,
                    "submit_request_id": context.request_id,
                    "cancel_request_id": cancel_request_id,
                    "run_request_id": run_request_id,
                    "advisory_lock_key": lock_key,
                    "cancel_http_status": cancel_response.status_code,
                    "cancel_response_status": cancelled["status"],
                    "run_http_status": run_response.status_code,
                    "run_response_status": returned["status"],
                    "run_response_data": returned["data"],
                    "persisted_status": persisted.status,
                    "cancel_requested": persisted.cancel_requested,
                    "result": persisted.result,
                    "usage_before": usage_before,
                    "usage_after": usage_after,
                    "provider_requests_added": len(usage_after["requests"]) - len(usage_before["requests"]),
                    "source_sha256": {
                        path: sha256((ROOT / path).read_bytes()).hexdigest()
                        for path in (
                            "data-center/src/data_center/store.py",
                            "data-center/src/data_center/runtime.py",
                            "data-center/tests/test_cancel_race_persistence.py",
                        )
                    },
                })
                assert cancelled["status"] == "cancelled"
                assert cancelled["data"]["status"] == "cancelled"
                assert returned["status"] == "cancelled"
                assert returned["run_id"] == task.run_id
                assert returned["data"] == {"task_id": task.task_id}
                assert persisted.status == "cancelled"
                assert persisted.cancel_requested is True
                assert persisted.result is None
                assert usage_after == usage_before
                observations["status"] = "passed"
            finally:
                if task is not None and context is not None:
                    current = await asyncio.to_thread(
                        app.state.runtime.store.get, task.task_id, context.actor_id)
                    if current.status in {"queued", "running"}:
                        await app.state.runtime.cancel(task.task_id, context)
                    observations.update({"task_id": task.task_id, "run_id": task.run_id,
                                         "actor_id": context.actor_id})
                if lock_connection is not None:
                    lock_connection.scalar(
                        text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"),
                        {"key": f"dc-worker:{task.task_id}"},
                    )
                    lock_connection.close()
    except Exception as error:
        observations["status"] = "failed"
        observations["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        directory = ROOT / ".scratch/evidence" / f"dc06-cancel-lock-http-{run_id}"
        directory.mkdir(parents=True, exist_ok=False)
        (directory / "report.json").write_text(
            json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
