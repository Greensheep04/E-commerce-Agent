from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from time import monotonic, sleep

import pytest
from sqlalchemy import select, text

from shop.campaign import CampaignService
from shop.campaign_types import RequirementPlan
from shop.config import Settings
from shop.harness import TaskStopped
from shop.paid_models import PaidModels, RequestLimit
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest


def wait_until(check):
    deadline = monotonic() + 5
    while not check():
        if monotonic() >= deadline:
            raise TimeoutError("等待真实数据库状态超时")
        sleep(0.02)


@pytest.mark.parametrize("mode", ["text", "embedding"])
def test_cancel_between_precheck_and_mark_sent_preserves_reason(db_session, data_dir, mode):
    service = CampaignService(Settings(
        database_url=db_session.info["test_database_url"], data_dir=data_dir,
        session_secret="test-session-secret",
        harness_token_limits_file=data_dir.parent / "token-limits.json",
    ))
    created = service.create("发送前取消的真实数据库检查")
    task_id = created["task_id"]
    messages = [{"role": "user", "content": created["prompt"]}]
    profile = service._profile(task_id, "understand")
    if mode == "text":
        step, provider, body = "understand", profile.service, profile.body(messages)
        input_limit, output_limit = PaidModels.input_bound(messages), profile.output_limit
    else:
        step, provider = "query_embedding", "dashscope"
        body = {"model": "qwen3.7-text-embedding", "input": [created["prompt"]],
                "dimensions": 1024, "encoding_format": "float"}
        input_limit, output_limit = len(created["prompt"].encode()), 0
    with service.maker.begin() as session:
        task = session.get(HarnessTask, task_id, with_for_update=True)
        request = service.budget.reserve(
            task_id=task_id, run_id=task.run_id, step_key=step, service=provider,
            model=body["model"], request_body=body, input_limit=input_limit,
            output_limit=output_limit, session=session,
        )
        task.model_calls = 1
        request_id = request.id
    def run_request():
        if mode == "text":
            return service._model(task_id, "understand", messages, RequirementPlan)
        return PaidModels(service.settings, service.budget).embed(
            [created["prompt"]], run_id=created["run_id"], step_key=step,
            timeout_seconds=5, task_id=task_id,
        )

    connection = service.engine.connect()
    transaction = connection.begin()
    blocker = connection.scalar(text("SELECT pg_backend_pid()"))
    connection.execute(select(ProviderRequest).where(
        ProviderRequest.id == request_id).with_for_update()).one()
    with ThreadPoolExecutor(max_workers=2) as executor:
        worker = executor.submit(run_request)
        canceller = None
        try:
            def blocked_at_request_row():
                if worker.done():
                    worker.result()
                with service.maker() as session:
                    return session.scalar(text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE wait_event_type = 'Lock' AND :blocker = ANY(pg_blocking_pids(pid)) "
                        "AND query LIKE '%provider_requests%')"
                    ), {"blocker": blocker})

            wait_until(blocked_at_request_row)
            canceller = executor.submit(service.cancel, task_id)

            def cancellation_committed():
                with service.maker() as session:
                    return session.get(HarnessTask, task_id).cancel_requested

            wait_until(cancellation_committed)
        finally:
            transaction.commit()
            connection.close()
        assert canceller is not None
        cancellation = canceller.result(timeout=5)
        assert cancellation["status"] == cancellation["stop_reason"] == "cancelled"
        expected_type = TaskStopped if mode == "text" else RequestLimit
        with pytest.raises(expected_type, match="^cancelled$"):
            worker.result(timeout=5)
    with service.maker() as session:
        stored = session.get(ProviderRequest, request_id)
        account = session.get(BudgetAccount, "project")
        task = session.get(HarnessTask, task_id)
        assert stored.status == "cancelled" and stored.response_body is None
        assert stored.charged_cny == Decimal(0)
        assert account.spent_cny == account.reserved_cny == Decimal(0)
        assert task.model_calls == 1 and task.tool_calls == 0
        assert list(session.scalars(select(ProviderRequest).where(
            ProviderRequest.task_id == task_id))) == [stored]
