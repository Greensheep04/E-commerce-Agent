from sqlalchemy import func, select

from shop.asset_index import IndexService
from shop.campaign import CampaignService
from shop.config import Settings
from shop.runtime_models import ProviderRequest


def test_understanding_schema_uses_actual_main_source_values():
    service = CampaignService(Settings())
    index = IndexService(service.settings, service.maker)
    with service.maker() as session:
        before = session.scalar(select(func.count()).select_from(ProviderRequest))
    assert hasattr(index, "attribute_values"), "理解阶段需要读取当前来源实际的属性值"
    values = index.attribute_values()
    assert "Kurtas" in values["articleType"]
    assert "Kurta" not in values["articleType"]
    assert "Tops" in values["articleType"]
    assert values["gender"] == ["Boys", "Girls", "Men", "Unisex", "Women"]
    with service.maker() as session:
        assert session.scalar(select(func.count()).select_from(ProviderRequest)) == before
    service.engine.dispose()
