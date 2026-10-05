import pytest
from pydantic import ValidationError

from shop.assessment import AssessmentService
from shop.assessment_models import AssessmentRun
from shop.assessment_orders import DataBlocked
from shop.assessment_service import AccessDenied, SalesMetricService, principal_for
from shop.assessment_types import AssessmentQuery
from shop.config import Settings


def test_team_query_retains_requested_comparison_employees():
    query = AssessmentQuery.model_validate({
        "action": "metrics", "scope": "team", "metric_ids": ["sales_amount", "order_count"],
        "comparison_employee_ids": [275, 277],
    })
    assert query.comparison_employee_ids == [275, 277]


def test_illegal_duplicate_metric_ids_are_rejected():
    with pytest.raises(ValidationError, match="指标编号不能重复"):
        AssessmentQuery.model_validate({"action": "metrics", "metric_ids": ["sales_amount", "sales_amount"]})


def test_comparison_requires_authorized_distinct_employees(db_session, assessment_accounts):
    service = SalesMetricService()
    principal = principal_for(db_session, assessment_accounts["manager"].id)
    selected = service.select_employees(principal, "team", [])
    assert service.select_comparison_employees(selected, []) == ()
    with pytest.raises(AccessDenied):
        service.select_comparison_employees(selected, [275, 276])
    with pytest.raises(ValueError):
        service.select_comparison_employees(selected, [275, 275])


def test_unsupported_period_has_explicit_reason(db_session, assessment_accounts):
    service = SalesMetricService()
    principal = principal_for(db_session, assessment_accounts["employee"].id)
    with pytest.raises(DataBlocked, match="期间不支持"):
        service.query(db_session, principal, scope="self", employee_ids=[],
                      period_start="2022-10-01", period_end="2023-01-01")


def test_illegal_employee_query_is_saved_without_reading_foreign_metrics(db_session, assessment_accounts):
    owner = assessment_accounts["employee"].id
    service = AssessmentService(Settings(database_url=db_session.info["test_database_url"]), SalesMetricService())
    try:
        created = service.create("请求员工276的指标，检查越权拒绝", owner)
        run = db_session.get(AssessmentRun, created["id"])
        run.status = "running"
        db_session.commit()
        query = AssessmentQuery(action="metrics", scope="employees", employee_ids=[276])
        with pytest.raises(AccessDenied):
            service._authorize_query(run.id, query)
        db_session.refresh(run)
        assert run.query == query.model_dump()
        assert run.scope_employee_ids == [275]
        assert run.metrics is None and run.result is None
        assert run.model_calls == 0 and run.tool_calls == 0
    finally:
        service.close()
