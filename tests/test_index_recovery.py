import json

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from shop.asset_index import IndexService
from shop.config import Settings
from shop.paid_models import UnknownRequest
from shop.runtime_models import AssetIndex, ProviderRequest


@pytest.mark.parametrize("authorized", [False, True])
def test_unknown_description_repair_requires_its_own_authorization(authorized):
    settings = Settings()
    engine = create_engine(settings.database_url)
    maker = sessionmaker(engine, expire_on_commit=False)
    request_id = "9346b4f9-3b9f-419f-8251-e779e9dd2fa4"
    try:
        with maker() as session:
            original = session.get(ProviderRequest, request_id)
            assert original.status == "unknown"
            record = session.get(AssetIndex, original.run_id)
            content = original.request_body["messages"][-1]["content"]
            fields = json.loads(content[0]["text"].removeprefix("来源字段：").split("。请依据", 1)[0])
            image_url = content[1]["image_url"]["url"]
            before = session.scalar(select(func.count()).select_from(ProviderRequest))
            service = IndexService(settings, maker, {request_id} if authorized else set(), True)
            step, messages = service.description_request(
                record.id, original.step_key, fields, image_url,
            )
            expected_step = ("description-json-repair1-repeat-" + request_id
                             if authorized else "description-json-repair1")
            assert step == expected_step
            assert messages == original.request_body["messages"]
        if not authorized:
            with pytest.raises(UnknownRequest):
                service.client.text(service.profile, messages, run_id=record.id,
                                    step_key=step, timeout_seconds=1)
        with maker() as session:
            assert session.get(ProviderRequest, request_id).status == "unknown"
            assert session.scalar(select(func.count()).select_from(ProviderRequest)) == before
    finally:
        engine.dispose()


def test_saved_description_repair_reuses_the_real_response():
    settings = Settings()
    engine = create_engine(settings.database_url)
    maker = sessionmaker(engine, expire_on_commit=False)
    try:
        with maker() as session:
            original = session.get(ProviderRequest, "827ebbb6-60a5-4975-8656-06c31419c4a5")
            assert original.status == "completed"
            record = session.get(AssetIndex, original.run_id)
            content = original.request_body["messages"][-1]["content"]
            fields = json.loads(content[0]["text"].removeprefix("来源字段：").split("。请依据", 1)[0])
            before = session.scalar(select(func.count()).select_from(ProviderRequest))
            service = IndexService(settings, maker)
            step, messages = service.description_request(
                record.id, record.configuration["description_step_key"], fields,
                content[1]["image_url"]["url"],
            )
            assert step == "description-json-repair1"
            assert messages == original.request_body["messages"]
        response = service.client.text(service.profile, messages, run_id=record.id,
                                       step_key=step, timeout_seconds=1)
        assert response == original.response_body
        with maker() as session:
            assert session.scalar(select(func.count()).select_from(ProviderRequest)) == before
    finally:
        engine.dispose()
