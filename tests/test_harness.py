from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from shop.budget import VERIFICATION_LIMIT, BudgetExceeded, BudgetService
from shop.config import Settings
from shop.harness import HarnessService, SearchArgs, TaskStopped
from shop.materials import MaterialInput, MaterialRepository
from shop.models import Asset, Source, User
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest, ToolCall


def service_for_test(db_session, data_dir: Path) -> HarnessService:
    settings = Settings(
        database_url=db_session.info["test_database_url"],
        data_dir=data_dir,
        session_secret="test-session-secret",
        provider_env_file=Settings().provider_env_file,
    )
    return HarnessService(settings)


def test_invalid_tool_arguments_are_rejected(db_session, data_dir):
    service = service_for_test(db_session, data_dir)
    task = service.create("测试实际错误工具参数")
    state = {"messages": [{"tool_calls": [{
        "id": str(uuid4()), "function": {"name": "search_materials",
                                         "arguments": '{"attributes":{"unknown":"x"},"limit":0}'},
    }]}], "round": 1, "awaiting_tools": True}
    with pytest.raises(TaskStopped, match="invalid_tool_arguments"):
        service._tool_node(task["task_id"], state)
    with service.maker() as session:
        calls = session.scalars(select(ToolCall).where(ToolCall.task_id == task["task_id"])).all()
        assert len(calls) == 1
        assert calls[0].status == "rejected"
        assert session.get(HarnessTask, task["task_id"]).tool_calls == 0


def test_tool_limit_and_cancel_use_persistent_state(db_session, data_dir):
    service = service_for_test(db_session, data_dir)
    task_id = service.create("测试工具次数限制")["task_id"]
    for _ in range(10):
        state = {"messages": [{"tool_calls": [{
            "id": str(uuid4()), "function": {"name": "search_materials",
                                             "arguments": '{"attributes":{},"limit":1}'},
        }]}], "round": 1, "awaiting_tools": True}
        service._tool_node(task_id, state)
    with pytest.raises(TaskStopped, match="tool_limit"):
        service._tool_node(task_id, {"messages": [{"tool_calls": [{
            "id": str(uuid4()), "function": {"name": "search_materials",
                                             "arguments": '{"attributes":{},"limit":1}'},
        }]}], "round": 1, "awaiting_tools": True})
    assert service.show(task_id)["tool_calls"] == 10
    assert len(service.show(task_id)["tools"]) == 10
    service.cancel(task_id)
    with pytest.raises(TaskStopped, match="cancelled"):
        service._guard(task_id, model=True)
    assert service.show(task_id)["model_calls"] == 0


def test_budget_rejects_before_request_record(db_session):
    maker = sessionmaker(create_engine(db_session.info["test_database_url"]), expire_on_commit=False)
    budget = BudgetService(maker)
    with maker.begin() as session:
        session.add(BudgetAccount(id="project", spent_cny=269, reserved_cny=1))
    with pytest.raises(BudgetExceeded, match="项目云 API 预算不足"):
        budget.reserve(task_id=None, run_id=None, step_key="test", service="deepseek",
                       model="deepseek-flash", request_body={}, input_limit=1,
                       output_limit=1)
    with maker() as session:
        assert session.scalars(select(ProviderRequest)).all() == []


def test_verification_limit_includes_inflight_reservation(db_session):
    maker = sessionmaker(create_engine(db_session.info["test_database_url"]), expire_on_commit=False)
    budget = BudgetService(maker)
    with maker.begin() as session:
        session.add(BudgetAccount(id="project", spent_cny=0, reserved_cny=VERIFICATION_LIMIT))
        session.add(ProviderRequest(
            id=str(uuid4()), task_id=None, run_id=None, step_key="other",
            service="deepseek", model="deepseek-flash", status="sent",
            reservation_cny=VERIFICATION_LIMIT, price_basis={"verification": True},
            request_body={},
        ))
    with pytest.raises(BudgetExceeded, match="M2 验证预算不足"):
        budget.reserve(task_id=None, run_id=None, step_key="test", service="deepseek",
                       model="deepseek-flash", request_body={}, input_limit=1,
                       output_limit=1)


def test_run_stops_before_network_when_project_budget_is_exhausted(db_session, data_dir):
    service = service_for_test(db_session, data_dir)
    task_id = service.create("请推荐两件黑色男士 T 恤")["task_id"]
    with service.maker.begin() as session:
        session.add(BudgetAccount(id="project", spent_cny=270, reserved_cny=0))
    result = service.run(task_id)
    assert result["status"] == "stopped"
    assert result["stop_reason"] == "budget_limit"
    assert result["model_calls"] == 0
    assert result["requests"] == []
    with service.maker() as session:
        assert session.scalars(select(ProviderRequest)).all() == []


def test_cancel_releases_reserved_request_before_network(db_session, data_dir):
    service = service_for_test(db_session, data_dir)
    task_id = service.create("检查未发送的费用预留")["task_id"]
    with service.maker.begin() as session:
        task = session.get(HarnessTask, task_id, with_for_update=True)
        request = service.budget.reserve(
            task_id=task_id, run_id=task.run_id, step_key="model-0",
            service="deepseek", model="deepseek-flash", request_body={"model": "deepseek-flash"},
            input_limit=12000, output_limit=1000, session=session,
        )
        task.model_calls += 1
    cancelled = service.cancel(task_id)
    assert cancelled["status"] == "cancelled"
    assert cancelled["model_calls"] == 1
    with service.maker() as session:
        assert session.get(ProviderRequest, request.id).status == "cancelled"
        account = session.get(BudgetAccount, "project")
        assert account.reserved_cny == 0
        assert account.spent_cny == 0


def test_project_threshold_serializes_concurrent_reservations(db_session):
    maker = sessionmaker(create_engine(db_session.info["test_database_url"]), expire_on_commit=False)
    budget = BudgetService(maker)
    with maker.begin() as session:
        session.add(BudgetAccount(id="project", spent_cny=Decimal("269.95"), reserved_cny=0))

    def attempt(number: int) -> str:
        try:
            budget.reserve(task_id=None, run_id=None, step_key=f"concurrent-{number}",
                           service="deepseek", model="deepseek-flash", request_body={},
                           input_limit=12000, output_limit=1000)
        except BudgetExceeded:
            return "rejected"
        return "reserved"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(attempt, (1, 2)))
    assert sorted(outcomes) == ["rejected", "reserved"]
    with maker() as session:
        account = session.get(BudgetAccount, "project")
        assert account.spent_cny + account.reserved_cny <= 270
        assert len(session.scalars(select(ProviderRequest)).all()) == 1


def test_search_args_reject_unknown_fields():
    with pytest.raises(ValidationError):
        SearchArgs.model_validate({"attributes": {}, "unexpected": 1})


def test_created_task_wait_does_not_consume_execution_time(db_session, data_dir):
    service = service_for_test(db_session, data_dir)
    task_id = service.create("检查执行时间")["task_id"]
    with service.maker.begin() as session:
        task = session.get(HarnessTask, task_id)
        task.created_at = task.created_at.replace(year=task.created_at.year - 1)
        task.elapsed_seconds = Decimal("119.5")
    service._base_elapsed = Decimal("119.5")
    service._active_started = monotonic()
    service._guard(task_id)
    service._base_elapsed = Decimal(120)
    with pytest.raises(TaskStopped, match="time_limit"):
        service._guard(task_id)
    with service.maker() as session:
        assert session.get(HarnessTask, task_id).elapsed_seconds >= 120


def test_cancel_waits_for_actual_tool_then_blocks_next_tool(db_session, data_dir):
    service = service_for_test(db_session, data_dir)
    task_id = service.create("检查取消与工具调用的先后顺序")["task_id"]
    blocking_connection = service.engine.connect()
    transaction = blocking_connection.begin()
    blocking_connection.execute(text("LOCK TABLE assets IN ACCESS EXCLUSIVE MODE"))

    def tool_state():
        return {"messages": [{"tool_calls": [{
            "id": str(uuid4()), "function": {"name": "search_materials",
                                             "arguments": '{"attributes":{},"limit":1}'},
        }]}], "round": 1, "awaiting_tools": True}

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            tool_future = pool.submit(service._locked_tool_node, task_id, tool_state())
            deadline = monotonic() + 3
            while monotonic() < deadline:
                with service.maker() as session:
                    if session.scalar(select(ToolCall).where(ToolCall.task_id == task_id)):
                        break
                sleep(0.01)
            else:
                raise AssertionError("工具调用没有进入数据库等待状态")
            cancel_future = pool.submit(service.cancel, task_id)
            sleep(0.05)
            assert not cancel_future.done()
            transaction.commit()
            tool_future.result(timeout=3)
            cancelled = cancel_future.result(timeout=3)
    finally:
        if transaction.is_active:
            transaction.rollback()
        blocking_connection.close()
    assert cancelled["cancel_requested"]
    assert cancelled["tool_calls"] == 1
    with pytest.raises(TaskStopped, match="cancelled"):
        service._locked_tool_node(task_id, tool_state())
    assert service.show(task_id)["tool_calls"] == 1


def test_citation_must_come_from_actual_search(db_session, data_dir):
    production_settings = Settings()
    production_engine = create_engine(production_settings.database_url)
    with sessionmaker(production_engine)() as production_session:
        production_asset = production_session.scalar(select(Asset).order_by(Asset.source_order))
        assert production_asset is not None
        production_version = production_asset.versions[-1]
        material = MaterialInput(
            source_id=production_asset.source_id,
            source_revision=production_asset.source_revision,
            record_id=production_asset.record_id,
            title=production_version.title, kind=production_asset.kind,
            tags=production_version.tags, owner_id="pending", source_order=production_asset.source_order,
            original_fields=production_version.original_fields,
            filename=production_version.filename, mime_type=production_version.mime_type,
            file_bytes=MaterialRepository(
                production_session, production_settings.data_dir
            ).read_file(production_version),
        )
    production_engine.dispose()
    owner = User(username="source-owner", password_hash="test-only", role="admin")
    db_session.add_all([owner, Source(
        source_id=material.source_id, revision=material.source_revision
    )])
    db_session.commit()
    material.owner_id = owner.id
    repository = MaterialRepository(db_session, data_dir)
    asset = repository.save(material)
    service = service_for_test(db_session, data_dir)
    task_id = service.create("检查引用来源")["task_id"]
    empty_search = service._execute_tool(
        "search_materials", {"attributes": {"gender": "Alien"}, "limit": 2}
    )
    actual_read = service._execute_tool("read_material", {"asset_id": asset.id})
    assert empty_search["assets"] == []
    with service.maker.begin() as session:
        session.add_all([
            ToolCall(id=str(uuid4()), task_id=task_id, call_id="search-actual",
                     name="search_materials", arguments={"attributes": {"gender": "Alien"}},
                     result=empty_search, status="completed"),
            ToolCall(id=str(uuid4()), task_id=task_id, call_id="read-actual",
                     name="read_material", arguments={"asset_id": asset.id},
                     result=actual_read, status="completed"),
        ])
    with pytest.raises(TaskStopped, match="unverified_citation"):
        service._finalize(task_id, '{"status":"materials_found","asset_ids":["'
                          + asset.id + '"],"missing_info":""}')


def test_final_result_checks_remaining_execution_time(db_session, data_dir):
    service = service_for_test(db_session, data_dir)
    task_id = service.create("检查完成结果的执行时间")["task_id"]
    empty_search = service._execute_tool(
        "search_materials", {"attributes": {"gender": "Alien"}, "limit": 2}
    )
    with service.maker.begin() as session:
        task = session.get(HarnessTask, task_id)
        task.elapsed_seconds = Decimal(120)
        session.add(ToolCall(
            id=str(uuid4()), task_id=task_id, call_id="search-actual",
            name="search_materials", arguments={"attributes": {"gender": "Alien"}},
            result=empty_search, status="completed",
        ))
    with pytest.raises(TaskStopped, match="time_limit"):
        service._finalize(
            task_id, '{"status":"insufficient","asset_ids":[],"missing_info":"缺少匹配素材"}'
        )
    task = service.show(task_id)
    assert task["result"] is None
    assert Decimal(task["elapsed_seconds"]) >= 120


def test_cancel_flag_precedes_operation_completion(db_session, data_dir):
    service = service_for_test(db_session, data_dir)
    task_id = service.create("检查取消与最终结果的先后顺序")["task_id"]
    empty_search = service._execute_tool(
        "search_materials", {"attributes": {"gender": "Alien"}, "limit": 2}
    )
    with service.maker.begin() as session:
        session.add(ToolCall(
            id=str(uuid4()), task_id=task_id, call_id="search-actual",
            name="search_materials", arguments={"attributes": {"gender": "Alien"}},
            result=empty_search, status="completed",
        ))
    connection = service.engine.connect()
    key = f"{task_id}:operation"
    connection.execute(text("SELECT pg_advisory_lock(hashtext(:key))"), {"key": key})
    connection.commit()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(service.cancel, task_id)
            try:
                deadline = monotonic() + 3
                while monotonic() < deadline:
                    with service.maker() as session:
                        if session.get(HarnessTask, task_id).cancel_requested:
                            break
                    sleep(0.01)
                else:
                    raise AssertionError("取消标记没有提交")
                assert not future.done()
                with pytest.raises(TaskStopped, match="cancelled"):
                    service._finalize(
                        task_id, '{"status":"insufficient","asset_ids":[],"missing_info":""}'
                    )
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(hashtext(:key))"), {"key": key})
                connection.commit()
            assert future.result(timeout=3)["status"] == "cancelled"
    finally:
        connection.close()
    assert service.show(task_id)["result"] is None


@pytest.mark.parametrize("status", ["running", "cancelled"])
def test_resume_finishes_persisted_cancellation(db_session, data_dir, status):
    service = service_for_test(db_session, data_dir)
    task_id = service.create("检查取消状态恢复后的未发送费用")["task_id"]
    with service.maker.begin() as session:
        task = session.get(HarnessTask, task_id)
        request = service.budget.reserve(
            task_id=task_id, run_id=task.run_id, step_key="model-0",
            service="deepseek", model="deepseek-flash", request_body={},
            input_limit=12000, output_limit=1000, session=session,
        )
        task.model_calls = 1
        task.cancel_requested = True
        task.status = status
    result = service.resume(task_id)
    assert result["status"] == "cancelled"
    assert result["model_calls"] == 1
    with service.maker() as session:
        stored = session.get(ProviderRequest, request.id)
        assert stored.status == "cancelled"
        assert stored.charged_cny == 0
        assert session.get(BudgetAccount, "project").reserved_cny == 0
    assert service.resume(task_id) == result
