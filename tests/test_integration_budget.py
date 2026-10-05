from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetExceeded, BudgetService
from shop.runtime_models import BudgetAccount, ProviderRequest


def test_poster_stage_reservation_has_its_own_limit(db_session):
    budget = BudgetService(sessionmaker(db_session.bind, expire_on_commit=False), verification_scope="m3")
    first = budget.reserve(
        task_id=None, run_id="poster-budget-input", step_key="first", service="deepseek",
        model="deepseek-flash", request_body={"model": "deepseek-flash"},
        input_limit=400000, output_limit=1,
    )
    assert first.price_basis["verification_scope"] == "m3"
    assert first.reservation_cny == Decimal("0.80000800")
    budget.mark_sent(first.id)
    budget.mark_unknown(first.id, "检查未知预留")
    with pytest.raises(BudgetExceeded):
        budget.reserve(
            task_id=None, run_id="poster-budget-input", step_key="second", service="deepseek",
            model="deepseek-flash", request_body={"model": "deepseek-flash"},
            input_limit=120000, output_limit=1,
        )
    rows = db_session.scalars(select(ProviderRequest)).all()
    assert len(rows) == 1
    assert rows[0].status == "unknown"
    assert db_session.get(BudgetAccount, "project").reserved_cny == Decimal("0.80000800")


def test_customer_scope_keeps_its_existing_budget_behavior(db_session):
    budget = BudgetService(sessionmaker(db_session.bind, expire_on_commit=False))
    request = budget.reserve(
        task_id=None, run_id="customer-budget-input", step_key="reserve", service="deepseek",
        model="deepseek-flash", request_body={"model": "deepseek-flash"},
        input_limit=10, output_limit=1, verification=False, scope="customer",
    )
    assert request.price_basis["scope"] == "customer"
    assert request.price_basis["verification_scope"] == "customer"
    budget.cancel_unsent(request.id, "预算接口检查结束")
    assert db_session.get(BudgetAccount, "project").reserved_cny == 0


def test_stage_name_is_checked_before_creating_a_request(db_session):
    with pytest.raises(ValueError):
        BudgetService(sessionmaker(db_session.bind), verification_scope="unregistered")
    assert db_session.scalar(select(ProviderRequest.id)) is None
