from uuid import uuid4

import pytest
from sqlalchemy import func, inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetService
from shop.runtime_models import BudgetAccount, ProviderRequest


def test_index_request_run_step_is_unique(db_session):
    maker = sessionmaker(db_session.get_bind(), expire_on_commit=False)
    budget = BudgetService(maker)
    arguments = {
        "task_id": None, "run_id": str(uuid4()), "step_key": "description",
        "service": "dashscope", "model": "qwen3.8-flash",
        "request_body": {"model": "qwen3.8-flash", "messages": []},
        "input_limit": 1, "output_limit": 1,
    }
    first = budget.reserve(**arguments)
    with pytest.raises(IntegrityError):
        budget.reserve(**arguments)
    with maker() as session:
        assert session.scalar(select(func.count()).select_from(ProviderRequest)) == 1
        assert session.get(BudgetAccount, "project").reserved_cny == first.reservation_cny


def test_asset_version_lookup_index_exists(db_session):
    indexes = inspect(db_session.get_bind()).get_indexes("asset_indexes")
    matching = [item for item in indexes if item["name"] == "ix_asset_indexes_asset_version"]
    assert len(matching) == 1
    assert matching[0]["column_names"] == ["asset_version_id"]
