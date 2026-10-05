import json
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from shop.asset_index import MAIN_SOURCE, IndexService
from shop.campaign_types import RequirementPlan
from shop.config import Settings
from shop.materials import MaterialRepository
from shop.models import Asset, AssetVersion, Source, User
from shop.runtime_models import AssetIndex


@pytest.fixture
def indexed_source(db_session):
    settings = Settings()
    engine = create_engine(settings.database_url)
    try:
        with Session(engine) as source:
            index = source.scalar(select(AssetIndex).join(AssetVersion).join(Asset).where(
                Asset.source_id == MAIN_SOURCE, AssetIndex.status == "ready"
            ).order_by(Asset.source_order))
            assert index is not None
            version = source.get(AssetVersion, index.asset_version_id)
            asset = source.get(Asset, version.asset_id)
            MaterialRepository(source, settings.data_dir).read_file(version)
            records = (
                source.get(Source, (asset.source_id, asset.source_revision)),
                source.get(User, asset.owner_id), asset, version, index,
            )
            # 在独立schema复用真实来源、图片摘要及已保存模型索引。
            for record in records:
                copied = type(record)(**{
                    column.key: getattr(record, column.key)
                    for column in record.__mapper__.columns
                })
                db_session.add(copied)
                db_session.flush()
            db_session.commit()
            return settings, asset.id, version.id, list(index.embedding)
    finally:
        engine.dispose()


def test_new_asset_version_excludes_older_ready_index(db_session, indexed_source):
    settings, asset_id, version_id, vector = indexed_source
    service = IndexService(settings, sessionmaker(db_session.get_bind(), expire_on_commit=False))
    plan = RequirementPlan(count=1, queries=["来源素材"])
    assert len(service.search(plan, vector)) == 1
    assert service.coverage(MAIN_SOURCE) == 1
    original = db_session.get(AssetVersion, version_id)
    updated = AssetVersion(**{
        column.key: getattr(original, column.key)
        for column in original.__mapper__.columns
    })
    updated.id = str(uuid4())
    updated.version = 2
    updated.title = "来源版本更新检查"
    db_session.add(updated)
    db_session.commit()
    assert updated.asset_id == asset_id
    assert service.search(plan, vector) == []
    assert service.coverage(MAIN_SOURCE) == 0


def test_new_description_configuration_excludes_existing_index(
    db_session, indexed_source, tmp_path,
):
    settings, _, _, vector = indexed_source
    limit_path = tmp_path / "token-limits.json"
    limit_path.write_text(json.dumps({"description": 10240}), encoding="utf-8")
    changed = settings.model_copy(update={"harness_token_limits_file": limit_path})
    service = IndexService(changed, sessionmaker(db_session.get_bind(), expire_on_commit=False))
    assert service.search(RequirementPlan(count=1, queries=["来源素材"]), vector) == []
    assert service.coverage(MAIN_SOURCE) == 0
