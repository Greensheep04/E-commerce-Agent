import json
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session, sessionmaker

from shop.asset_index import MAIN_SOURCE
from shop.budget import BudgetService
from shop.campaign import CampaignService
from shop.campaign_types import RequirementPlan
from shop.config import Settings
from shop.harness import TaskStopped
from shop.materials import MaterialInput, MaterialRepository
from shop.models import Asset, Source, User
from shop.paid_models import PaidModels
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest, ToolCall

MODELS_REPORT = Path(".scratch/evidence/har-models-20260930T071204-0c5fd569/report.json")


def campaign_service(db_session, data_dir: Path, token_limits_file: Path | None = None):
    settings = Settings(
        database_url=db_session.info["test_database_url"],
        data_dir=data_dir,
        session_secret="test-session-secret",
        provider_env_file=data_dir.parent / "provider-credentials-disabled.env",
        harness_token_limits_file=token_limits_file or data_dir.parent / "token-limits.json",
    )
    return CampaignService(settings)


def copy_existing_main_image(db_session, data_dir: Path) -> dict:
    source_settings = Settings()
    source_engine = create_engine(source_settings.database_url)
    try:
        with Session(source_engine) as source_session:
            source_asset = source_session.scalar(
                select(Asset)
                .where(Asset.source_id == MAIN_SOURCE)
                .order_by(Asset.source_order, Asset.id)
                .limit(1)
            )
            assert source_asset is not None
            source_version = source_asset.versions[-1]
            source = {
                "source_id": source_asset.source_id,
                "source_revision": source_asset.source_revision,
                "record_id": source_asset.record_id,
                "source_order": source_asset.source_order,
                "title": source_version.title,
                "kind": source_asset.kind,
                "tags": list(source_version.tags),
                "original_fields": dict(source_version.original_fields),
                "filename": source_version.filename,
                "mime_type": source_version.mime_type,
                "file_bytes": MaterialRepository(
                    source_session, source_settings.data_dir
                ).read_file(source_version),
            }
    finally:
        source_engine.dispose()

    owner = User(
        username=f"har-boundary-{uuid4().hex}",
        password_hash="test-only",
        role="admin",
    )
    db_session.add_all(
        [
            Source(
                source_id=source["source_id"],
                revision=source["source_revision"],
            ),
            owner,
        ]
    )
    db_session.flush()
    asset = MaterialRepository(db_session, data_dir).save(
        MaterialInput(owner_id=owner.id, **source)
    )
    version = asset.versions[-1]
    return {
        "asset_id": asset.id,
        "version_id": version.id,
        "file_sha256": version.file_sha256,
        "original_fields": dict(version.original_fields),
        "title": version.title,
    }


def test_cancel_after_reservation_releases_cost_and_blocks_replay(db_session, data_dir):
    service = campaign_service(db_session, data_dir)
    task_id = service.create("取消后不发送新的理解请求")["task_id"]
    body = {"model": "deepseek-flash", "messages": [{"role": "user", "content": "需求"}]}
    with service.maker.begin() as session:
        task = session.get(HarnessTask, task_id, with_for_update=True)
        request = service.budget.reserve(
            task_id=task_id,
            run_id=task.run_id,
            step_key="understand",
            service="deepseek",
            model="deepseek-flash",
            request_body=body,
            input_limit=12,
            output_limit=100,
            session=session,
        )
        task.model_calls = 1
    reserved = request.reservation_cny

    cancelled = service.cancel(task_id)
    assert cancelled["status"] == "cancelled"
    assert cancelled["model_calls"] == 1
    with service.maker() as session:
        stored = session.get(ProviderRequest, request.id)
        account = session.get(BudgetAccount, "project")
        assert stored.status == "cancelled"
        assert stored.response_body is None
        assert stored.charged_cny == 0
        assert account.reserved_cny == 0
        assert account.spent_cny == 0
        assert reserved > 0

    with pytest.raises(ValueError, match="请求已经取消"):
        PaidModels(service.settings, service.budget).request(
            run_id=cancelled["run_id"],
            step_key="understand",
            service="deepseek",
            body=body,
            output_limit=100,
            timeout_seconds=1,
            task_id=task_id,
            input_limit=12,
        )
    assert service.run(task_id)["requests"][0]["status"] == "cancelled"
    assert len(service.show(task_id)["requests"]) == 1


def test_concurrent_campaign_run_is_rejected_by_postgres_lock(db_session, data_dir):
    service = campaign_service(db_session, data_dir)
    task_id = service.create("检查同一任务的并发运行限制")["task_id"]
    lock_connection = service.engine.connect()
    lock_connection.execute(
        text("SELECT pg_advisory_lock(hashtext(:task_id))"), {"task_id": task_id}
    )
    lock_connection.commit()
    try:
        with pytest.raises(ValueError, match="任务已经在运行"):
            service.run(task_id)
    finally:
        lock_connection.execute(
            text("SELECT pg_advisory_unlock(hashtext(:task_id))"), {"task_id": task_id}
        )
        lock_connection.commit()
        lock_connection.close()

    task = service.show(task_id)
    assert task["status"] == "created"
    assert task["requests"] == []
    assert task["model_calls"] == 0


def test_six_model_calls_stop_before_creating_a_seventh_request(db_session, data_dir):
    service = campaign_service(db_session, data_dir)
    task_id = service.create("检查模型请求上限")["task_id"]
    for number in range(6):
        with service.maker.begin() as session:
            task = session.get(HarnessTask, task_id, with_for_update=True)
            request = service.budget.reserve(
                task_id=task_id,
                run_id=task.run_id,
                step_key=f"cancelled-before-send-{number}",
                service="deepseek",
                model="deepseek-flash",
                request_body={"model": "deepseek-flash", "messages": []},
                input_limit=1,
                output_limit=1,
                session=session,
            )
            task.model_calls += 1
        service.budget.cancel_unsent(request.id, "cancelled before send")

    messages = [{"role": "user", "content": "检查第七个请求"}]
    with pytest.raises(TaskStopped, match="model_limit"):
        service._model(task_id, "understand", messages, RequirementPlan)

    with service.maker() as session:
        task = session.get(HarnessTask, task_id)
        requests = session.scalars(
            select(ProviderRequest).where(ProviderRequest.task_id == task_id)
        ).all()
        assert task.model_calls == 6
        assert len(requests) == 6
        assert all(item.status == "cancelled" and item.response_body is None for item in requests)


def test_cumulative_execution_time_stops_before_request_creation(db_session, data_dir):
    service = campaign_service(db_session, data_dir)
    task_id = service.create("检查累计执行时间上限")["task_id"]
    with service.maker.begin() as session:
        session.get(HarnessTask, task_id).elapsed_seconds = Decimal("120.000")

    with pytest.raises(TaskStopped, match="time_limit"):
        service._model(
            task_id,
            "understand",
            [{"role": "user", "content": "达到累计时间上限"}],
            RequirementPlan,
        )

    with service.maker() as session:
        task = session.get(HarnessTask, task_id)
        assert task.elapsed_seconds == Decimal("120.000")
        assert (
            session.scalars(select(ProviderRequest).where(ProviderRequest.task_id == task_id)).all()
            == []
        )


def test_unknown_external_request_stops_campaign_without_releasing_reservation(
    db_session, data_dir
):
    service = campaign_service(db_session, data_dir)
    task_id = service.create("检查未知请求的停止行为")["task_id"]
    messages = [{"role": "user", "content": "检查未知请求"}]
    profile = service._profile(task_id, "understand")
    body = profile.body(messages)
    input_limit = PaidModels.input_bound(messages)
    with service.maker.begin() as session:
        task = session.get(HarnessTask, task_id, with_for_update=True)
        request = service.budget.reserve(
            task_id=task_id,
            run_id=task.run_id,
            step_key="understand",
            service=profile.service,
            model=profile.model,
            request_body=body,
            input_limit=input_limit,
            output_limit=profile.output_limit,
            session=session,
        )
        task.model_calls = 1
    service.budget.mark_sent(request.id)
    service.budget.mark_unknown(request.id, "TimeoutException")

    with pytest.raises(TaskStopped, match="external_request_unknown"):
        service._model(task_id, "understand", messages, RequirementPlan)

    with service.maker() as session:
        stored = session.get(ProviderRequest, request.id)
        account = session.get(BudgetAccount, "project")
        assert stored.status == "unknown"
        assert stored.response_body is None
        assert stored.usage is None
        assert account.reserved_cny == request.reservation_cny
        assert account.spent_cny == 0
        assert session.scalars(
            select(ProviderRequest).where(ProviderRequest.task_id == task_id)
        ).all() == [stored]


def test_tenth_campaign_tool_is_allowed_and_eleventh_is_rejected(db_session, data_dir):
    service = campaign_service(db_session, data_dir)
    task_id = service.create("检查工具调用次数上限")["task_id"]
    candidate = copy_existing_main_image(db_session, data_dir)
    state = {
        "draft": {
            "headline": candidate["title"],
            "summary": candidate["title"],
            "recommendations": [{"asset_id": candidate["asset_id"], "caption": candidate["title"]}],
        },
        "candidates": [candidate],
    }

    for number in range(10):
        result = service._tool(
            task_id,
            f"evidence_review_{number}",
            state["draft"],
            lambda: service._evidence(state),
        )
        assert result["records"][0]["file_sha256"] == candidate["file_sha256"]
        assert result["images"][0]["url"].startswith("data:image/")

    with pytest.raises(TaskStopped, match="tool_limit"):
        service._tool(
            task_id,
            "evidence_review_10",
            state["draft"],
            lambda: service._evidence(state),
        )

    with service.maker() as session:
        task = session.get(HarnessTask, task_id)
        statuses = dict(
            session.execute(
                select(ToolCall.status, func.count())
                .where(ToolCall.task_id == task_id)
                .group_by(ToolCall.status)
            ).all()
        )
        assert task.tool_calls == 10
        assert statuses == {"completed": 10}
        assert (
            session.scalars(select(ProviderRequest).where(ProviderRequest.task_id == task_id)).all()
            == []
        )


def test_campaign_profile_snapshot_is_saved_with_task_atomically(db_session, data_dir, tmp_path):
    config_file = tmp_path / "token-limits.json"
    config_file.write_text(json.dumps({"understand": 27049}), encoding="utf-8")
    service = campaign_service(db_session, data_dir, config_file)
    task = service.create("检查任务保存的模型配置快照")

    with service.maker() as session:
        saved = session.get(HarnessTask, task["task_id"])
        assert saved.workflow_version == "campaign-v1"
        assert saved.model_profile["understand"]["output_limit"] == 27049
        assert saved.model_profile["compose"]["model"] == "deepseek-flash"

    config_file.write_text(json.dumps({"understand": 27050}), encoding="utf-8")
    assert service.show(task["task_id"])["model_profile"]["understand"]["output_limit"] == 27049

    with service.maker() as session:
        before = session.scalar(select(func.count()).select_from(HarnessTask))
    config_file.write_text(json.dumps({"understand": 1}), encoding="utf-8")
    with pytest.raises(ValueError, match="输出上限配置无效"):
        service.create("无效配置不能创建任务")
    with service.maker() as session:
        assert session.scalar(select(func.count()).select_from(HarnessTask)) == before


def test_saved_real_understand_response_replays_without_parameter_drift():
    report = json.loads(MODELS_REPORT.read_text(encoding="utf-8"))
    run_id = report["run_id"]
    settings = Settings()
    engine = create_engine(settings.database_url)
    maker = sessionmaker(engine, expire_on_commit=False)
    client = PaidModels(settings, BudgetService(maker))
    try:
        with maker() as session:
            original = session.scalar(
                select(ProviderRequest).where(
                    ProviderRequest.run_id == run_id,
                    ProviderRequest.step_key == "understand",
                )
            )
            assert original is not None
            assert original.status == "completed"
            assert original.response_body is not None
            original_id = original.id
            body = json.loads(json.dumps(original.request_body))
            response_hash = sha256(
                json.dumps(original.response_body, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()
            request_count = session.scalar(
                select(func.count())
                .select_from(ProviderRequest)
                .where(ProviderRequest.run_id == run_id)
            )
            request_args = {
                "run_id": run_id,
                "step_key": "understand",
                "service": original.service,
                "body": body,
                "output_limit": original.price_basis["output_limit"],
                "timeout_seconds": 1,
                "task_id": original.task_id,
                "input_limit": original.price_basis["input_limit"],
            }
            saved_response = original.response_body

        replayed = client.request(**request_args)
        assert replayed == saved_response

        request_args["body"]["max_tokens"] += 1
        with pytest.raises(ValueError, match="恢复请求参数与原始记录不一致"):
            client.request(**request_args)

        with maker() as session:
            stored = session.get(ProviderRequest, original_id)
            after_count = session.scalar(
                select(func.count())
                .select_from(ProviderRequest)
                .where(ProviderRequest.run_id == run_id)
            )
            stored_hash = sha256(
                json.dumps(stored.response_body, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()
            assert stored.status == "completed"
            assert stored_hash == response_hash
            assert after_count == request_count
    finally:
        engine.dispose()
