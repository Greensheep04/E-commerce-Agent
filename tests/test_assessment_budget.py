from decimal import Decimal

import pytest
from sqlalchemy.orm import sessionmaker

from shop.assessment_models import AssessmentRun
from shop.assessment_service import configure_access, import_employee_source
from shop.auth import hash_password
from shop.budget import BudgetExceeded, BudgetService
from shop.models import User
from shop.runtime_models import ProviderRequest
from tests.assessment_fixtures import SOURCE_DIRECTORY


def test_assessment_budget_has_independent_scope(db_session):
    import_employee_source(db_session, SOURCE_DIRECTORY)
    user = User(username="budget-owner", password_hash=hash_password("assessment-budget-test"), role="viewer")
    db_session.add(user)
    db_session.flush()
    configure_access(db_session, user_id=user.id, level="employee", employee_id=275,
                     managed_employee_ids=[])
    run = AssessmentRun(owner_id=user.id, owner_level="employee", prompt="查看销售额",
                        scope_employee_ids=[275], model_profile={}, status="running")
    db_session.add(run)
    db_session.commit()
    budget = BudgetService(sessionmaker(db_session.bind, expire_on_commit=False))
    request = budget.reserve(task_id=None, assessment_id=run.id, run_id=run.run_id,
                             step_key="boundary", service="deepseek", model="deepseek-flash",
                             request_body={"model": "deepseek-flash"}, input_limit=12000,
                             output_limit=27048, verification_scope="assessment")
    assert request.price_basis["verification_scope"] == "assessment"
    assert request.assessment_id == run.id
    db_session.refresh(run)
    run.cancel_requested = True
    db_session.commit()
    with pytest.raises(BudgetExceeded, match="cancelled"):
        budget.mark_sent(request.id)
    request = db_session.get(ProviderRequest, request.id)
    assert request.status == "cancelled"
    assert request.charged_cny == Decimal(0)


def test_assessment_reservation_over_limit_is_rejected(db_session):
    budget = BudgetService(sessionmaker(db_session.bind, expire_on_commit=False))
    for index in range(5):
        budget.reserve(task_id=None, run_id="budget-boundary-input", step_key=str(index),
                       service="deepseek", model="deepseek-flash", request_body={},
                       input_limit=0, output_limit=500000, verification_scope="assessment")
    with pytest.raises(BudgetExceeded, match="考核"):
        budget.reserve(task_id=None, run_id="invalid-budget-input", step_key="oversize",
                       service="deepseek", model="deepseek-flash", request_body={},
                       input_limit=0, output_limit=100, verification_scope="assessment")
    harness = budget.reserve(task_id=None, run_id="harness-independent-input", step_key="boundary",
                             service="deepseek", model="deepseek-flash", request_body={},
                             input_limit=0, output_limit=100, verification_scope="harness")
    assert harness.price_basis["verification_scope"] == "harness"
