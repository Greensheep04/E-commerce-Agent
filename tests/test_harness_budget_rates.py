from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetService
from shop.runtime_models import BudgetAccount, ProviderRequest


def test_vision_and_embedding_reserve_their_own_prices(db_session):
    maker = sessionmaker(create_engine(db_session.info["test_database_url"]), expire_on_commit=False)
    budget = BudgetService(maker)
    vision = budget.reserve(
        task_id=None, run_id=str(uuid4()), step_key="description", service="dashscope",
        model="qwen3.8-flash", request_body={}, input_limit=1000, output_limit=5120,
    )
    embedding = budget.reserve(
        task_id=None, run_id=str(uuid4()), step_key="embedding", service="dashscope",
        model="qwen3.7-text-embedding", request_body={}, input_limit=1000, output_limit=0,
    )
    assert vision.reservation_cny == Decimal("0.01465100")
    assert embedding.reservation_cny == Decimal("0.00050000")
    with maker() as session:
        account = session.get(BudgetAccount, "project")
        assert account.reserved_cny == Decimal("0.01515100")
        assert len(session.scalars(select(ProviderRequest)).all()) == 2


def test_unsupported_model_cannot_reserve_money(db_session):
    maker = sessionmaker(create_engine(db_session.info["test_database_url"]), expire_on_commit=False)
    with pytest.raises(ValueError, match="计价"):
        BudgetService(maker).reserve(
            task_id=None, run_id=None, step_key="unsupported", service="dashscope",
            model="unsupported", request_body={}, input_limit=1000, output_limit=1000,
        )
    with maker() as session:
        assert session.scalars(select(ProviderRequest)).all() == []
