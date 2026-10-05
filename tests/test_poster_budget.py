from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetExceeded, BudgetService
from shop.runtime_models import ProviderRequest


def test_stage_cap_shared_by_service_instances(db_session):
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    for number in range(4):
        BudgetService(maker, verification_scope="m3").reserve(
            task_id=None, run_id=str(uuid4()), step_key=f"stage-{number}",
            service="deepseek", model="deepseek-flash", request_body={"number": number},
            input_limit=12000, output_limit=27048,
        )
    with pytest.raises(BudgetExceeded, match="m3 自媒体验证预算不足"):
        BudgetService(maker, verification_scope="m3").reserve(
            task_id=None, run_id=str(uuid4()), step_key="stage-limit",
            service="deepseek", model="deepseek-flash", request_body={},
            input_limit=12000, output_limit=27048,
        )
    requests = db_session.scalars(select(ProviderRequest)).all()
    assert len(requests) == 4
    assert all(item.price_basis["verification_scope"] == "m3" for item in requests)
    assert sum(item.reservation_cny for item in requests) <= Decimal(1)


def test_unknown_stage_rejected(db_session):
    with pytest.raises(ValueError, match="阶段"):
        BudgetService(sessionmaker(db_session.bind), verification_scope="unknown")


def test_stage_cannot_disable_verification(db_session):
    service = BudgetService(sessionmaker(db_session.bind), verification_scope="m3")
    with pytest.raises(ValueError, match="验证预算"):
        service.reserve(
            task_id=None, run_id=str(uuid4()), step_key="unbounded",
            service="deepseek", model="deepseek-flash", request_body={},
            input_limit=12000, output_limit=27048, verification=False,
        )
    assert db_session.scalars(select(ProviderRequest)).all() == []
