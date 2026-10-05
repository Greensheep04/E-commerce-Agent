from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.orm import sessionmaker

from shop.assessment import PROFILE, AssessmentService, AssessmentStopped
from shop.assessment_models import AssessmentRun, AssessmentToolCall
from shop.assessment_service import SalesMetricService, configure_access
from shop.budget import BudgetExceeded, BudgetService
from shop.config import Settings
from shop.models import now
from shop.paid_models import PaidModels, RequestLimit, UnknownRequest
from shop.runtime_models import BudgetAccount, ProviderRequest


@pytest.fixture
def assessment_service(db_session, assessment_accounts):
    service = AssessmentService(Settings(database_url=db_session.info["test_database_url"]),
                                SalesMetricService())
    try:
        yield service
    finally:
        service.close()


def test_cancelled_task_does_not_send_request(db_session, assessment_accounts, assessment_service):
    owner = assessment_accounts["employee"].id
    run = assessment_service.create("查看我的销售额", owner)
    cancelled = assessment_service.cancel(run["id"], owner)
    assert cancelled["status"] == "cancelled"
    assert assessment_service.execute(run["id"], owner)["status"] == "cancelled"
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0


def test_cumulative_execution_time_is_retained(db_session, assessment_accounts, assessment_service):
    owner = assessment_accounts["employee"].id
    created = assessment_service.create("查看我的销售额", owner)
    run = db_session.get(AssessmentRun, created["id"])
    run.elapsed_seconds = Decimal(180)
    db_session.commit()
    result = assessment_service.execute(run.id, owner)
    assert result["status"] == "stopped"
    assert result["stop_reason"] == "time_limit"
    assert result["elapsed_seconds"] >= 180
    assert result["model_calls"] == 0


def test_cross_process_execution_lock_is_enforced(db_session, assessment_accounts, assessment_service):
    owner = assessment_accounts["employee"].id
    run = assessment_service.create("查看我的销售额", owner)
    key = f"assessment:{run['id']}"
    with db_session.bind.connect() as connection:
        connection.execute(text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"), {"key": key})
        connection.commit()
        try:
            with pytest.raises(ValueError, match="已经在执行"):
                assessment_service.execute(run["id"], owner)
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"), {"key": key})
            connection.commit()


def test_model_call_limit_rejects_before_provider_call(db_session, assessment_accounts, assessment_service):
    owner = assessment_accounts["employee"].id
    created = assessment_service.create("查看我的销售额", owner)
    run = db_session.get(AssessmentRun, created["id"])
    run.status = "running"
    run.model_calls = 4
    db_session.commit()
    models = PaidModels(assessment_service.settings, assessment_service.budget)
    with pytest.raises(RequestLimit, match="model_limit"):
        models.request(run_id=run.run_id, step_key="over-limit", service="deepseek",
                       body=PROFILE.body([{"role": "user", "content": run.prompt}]),
                       output_limit=27048, timeout_seconds=30, assessment_id=run.id)
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0


def test_tool_call_limit_rejects_boundary_input(db_session, assessment_accounts, assessment_service):
    owner = assessment_accounts["employee"].id
    created = assessment_service.create("查看我的销售额", owner)
    run = db_session.get(AssessmentRun, created["id"])
    run.status = "running"
    # 明确设置调用次数边界输入，不进入中文业务评价。
    run.tool_calls = 6
    db_session.commit()
    with pytest.raises(AssessmentStopped, match="tool_limit"):
        assessment_service._tool(run.id, "boundary-definitions", "metric_definitions", {})
    assert db_session.scalar(select(func.count()).select_from(AssessmentToolCall)) == 0
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0


@pytest.mark.parametrize("change,reason", [("permission", "permission_changed"), ("time", "time_limit")])
def test_reservation_rechecks_state_before_sending(db_session, assessment_accounts, assessment_service,
                                                 change, reason):
    owner = assessment_accounts["manager"].id
    created = assessment_service.create("查看团队销售额", owner)
    run = db_session.get(AssessmentRun, created["id"])
    run.status = "running"
    db_session.commit()
    budget = BudgetService(sessionmaker(db_session.bind, expire_on_commit=False))
    request = budget.reserve(task_id=None, assessment_id=run.id, run_id=run.run_id,
                             step_key="state-change", service="deepseek", model="deepseek-flash",
                             request_body=PROFILE.body([{"role": "user", "content": run.prompt}]),
                             input_limit=12000, output_limit=27048, verification_scope="assessment")
    if change == "permission":
        configure_access(db_session, user_id=owner, level="manager", employee_id=285,
                         managed_employee_ids=[])
    else:
        run.active_since = now() - timedelta(seconds=181)
    db_session.commit()
    with pytest.raises(BudgetExceeded, match=reason):
        budget.mark_sent(request.id)
    record = db_session.get(ProviderRequest, request.id)
    assert record.status == "cancelled"
    assert record.charged_cny == Decimal(0)


@pytest.mark.parametrize("interrupted_status", ["sent", "unknown"])
def test_cluster_assessment_unknown_keeps_reservation_without_retry(
    db_session, assessment_accounts, assessment_service, interrupted_status,
):
    from agent_cluster.store import ClusterStore, digest
    from agent_cluster.types import (
        CompletionCriterion,
        GoalDefinition,
        TaskPlan,
        TaskSpec,
        TriggerEvent,
    )

    from shop.integration_models import IntegrationCall

    owner = assessment_accounts["employee"].id
    created = assessment_service.create("查看我的销售额", owner)
    assessment = db_session.get(AssessmentRun, created["id"])
    assessment.status = "running"
    assessment.model_calls = 4
    db_session.commit()
    store = ClusterStore(db_session.bind)
    store.initialize()
    goal = store.create_goal(GoalDefinition(
        actor_id=owner, objective=assessment.prompt,
        allowed_operations={"shop_assessment_agent": ["run"]},
        criteria=[CompletionCriterion(tool_name="shop_assessment_agent", path=["data", "result"])],
    ))
    run = store.trigger(TriggerEvent(event_id="assessment-interruption", goal_id=goal.goal_id, actor_id=owner))
    store.save_plan(run.run_id, owner, TaskPlan(tasks=[TaskSpec(
        task_id="assessment", tool_name="shop_assessment_agent", action="run",
        payload={"task_id": assessment.id},
    )]), expected_version=0)
    task = store.tasks(run.run_id, owner)[0]
    store.set_task(run.run_id, owner, task["task_id"], status="sent")
    db_session.add(IntegrationCall(
        actor_id=owner, module_id="assessment", operation="run", request_id=task["request_id"],
        payload_sha256=digest({"task_id": assessment.id}), resource_type="run", resource_id=assessment.id,
    ))
    db_session.commit()
    body = PROFILE.body([{"role": "user", "content": assessment.prompt}])
    budget = assessment_service.budget
    request = budget.reserve(
        task_id=None, assessment_id=assessment.id, run_id=assessment.run_id,
        step_key="interrupted-request", service="deepseek", model="deepseek-flash",
        request_body=body, input_limit=12000, output_limit=27048, verification_scope="assessment",
    )
    assert request.price_basis["cluster_run_id"] == run.run_id
    budget.mark_sent(request.id)
    if interrupted_status == "unknown":
        budget.mark_unknown(request.id, "process_interrupted")
    models = PaidModels(assessment_service.settings, budget)
    for _ in range(2):
        with pytest.raises(UnknownRequest):
            models.request(run_id=assessment.run_id, step_key=request.step_key, service="deepseek",
                           body=body, output_limit=27048, timeout_seconds=30, assessment_id=assessment.id)
    db_session.expire_all()
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 1
    stored = db_session.get(ProviderRequest, request.id)
    assert stored.status == "unknown"
    assert stored.charged_cny is None
    assert db_session.get(BudgetAccount, "project").reserved_cny == request.reservation_cny
    assert db_session.get(AssessmentRun, assessment.id).model_calls == 4
