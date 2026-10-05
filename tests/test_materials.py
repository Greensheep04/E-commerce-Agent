from copy import deepcopy

import pytest
from PIL import UnidentifiedImageError
from sqlalchemy import create_engine, select, text

from shop.config import Settings
from shop.fashion import REVISION, SOURCE_ID, import_rows, row_to_material
from shop.materials import MaterialRepository
from shop.models import Asset, AssetVersion, ImportBatch, Source, User


def test_version_and_idempotent_import(db_session, data_dir, source_row):
    owner = User(username="owner", password_hash="test-only", role="admin")
    source = Source(source_id=SOURCE_ID, revision=REVISION, license=None)
    db_session.add_all([owner, source])
    db_session.commit()
    repository = MaterialRepository(db_session, data_dir)
    payload = row_to_material(source_row, 0, owner.id)
    first = repository.save(payload)
    second = repository.save(payload)
    assert first.id == second.id
    assert len(db_session.scalars(select(Asset)).all()) == 1
    assert len(db_session.scalars(select(AssetVersion)).all()) == 1
    payload.title = f"测试输入：{payload.title}"
    third = repository.save(payload)
    assert third.id == first.id
    assert [version.version for version in third.versions] == [1, 2]
    assert repository.read_file(third.versions[-1]) == payload.file_bytes
    assert repository.search(
        source_id=SOURCE_ID, source_revision=REVISION, kind="image",
        tag=payload.tags[0], owner_id=owner.id,
        attributes={"masterCategory": source_row["masterCategory"]},
    )[0].id == first.id


def test_bad_file(db_session, data_dir, source_row):
    owner = User(username="owner", password_hash="test-only", role="admin")
    db_session.add_all([owner, Source(source_id=SOURCE_ID, revision=REVISION)])
    db_session.commit()
    repository = MaterialRepository(db_session, data_dir)
    payload = row_to_material(source_row, 0, owner.id)
    payload.file_bytes = b"test-input: damaged image"
    with pytest.raises(ValueError, match="图片文件损坏"):
        repository.save(payload)
    assert db_session.scalars(select(Asset)).all() == []


def test_batch_failure_is_visible(db_session, data_dir, source_row):
    owner = User(username="owner", password_hash="test-only", role="admin")
    db_session.add(owner)
    db_session.commit()
    damaged_row = deepcopy(source_row)
    damaged_row["id"] = -1
    damaged_row["image"] = {"path": "test-input.png", "bytes": b"test-input: damaged image"}
    settings = Settings(database_url=db_session.info["test_database_url"], data_dir=data_dir)
    with pytest.raises(UnidentifiedImageError):
        import_rows(settings, owner.id, [source_row, damaged_row])
    db_session.expire_all()
    batch = db_session.scalar(select(ImportBatch))
    assert batch.status == "failed"
    assert batch.imported_count == 1
    assert "第 2 条" in batch.error
    assert len(batch.manifest) == 1


def test_same_source_import_is_exclusive(db_session, data_dir):
    engine = create_engine(db_session.info["test_database_url"])
    with engine.connect() as connection:
        connection.execute(
            text("SELECT pg_advisory_lock(hashtextextended(:identity, 0))"),
            {"identity": f"{SOURCE_ID}@{REVISION}"},
        )
        settings = Settings(database_url=db_session.info["test_database_url"], data_dir=data_dir)
        with pytest.raises(RuntimeError, match="正在运行"):
            import_rows(settings, "test-owner", [])
        connection.execute(
            text("SELECT pg_advisory_unlock(hashtextextended(:identity, 0))"),
            {"identity": f"{SOURCE_ID}@{REVISION}"},
        )
    engine.dispose()
