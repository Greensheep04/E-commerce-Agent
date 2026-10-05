from uuid import uuid4

import pytest
from langgraph.checkpoint.postgres import PostgresSaver
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url

from shop.auth import hash_password
from shop.integration_history import (
    HISTORY_TABLES,
    copy_history_files,
    file_manifest,
    import_assessment_history,
    records,
    tables_for,
)
from shop.integration_models import IntegrationIdentityMap
from shop.models import User

SOURCE_URL = "postgresql+psycopg://shop@127.0.0.1:55436/shop"


def test_if03_real_history_and_checkpoint_import_is_idempotent(db_session):
    target_url = db_session.info["test_database_url"]
    with PostgresSaver.from_conn_string(make_url(target_url).set(drivername="postgresql").render_as_string(
            hide_password=False)) as saver:
        saver.setup()
    source_engine = create_engine(SOURCE_URL, execution_options={"postgresql_readonly": True})
    try:
        with source_engine.connect() as source, source.begin():
            source_tables = tables_for(source, HISTORY_TABLES)
            source_rows = {name: records(source, table) for name, table in source_tables.items()}
            assert source_rows["assessment_runs"] and source_rows["checkpoints"]
        collision = source_rows["users"][0]
        local = User(username=collision["username"], password_hash=hash_password("history-local-boundary"),
                     role="viewer")
        second = User(username="assessment-" + collision["username"],
                      password_hash=hash_password("history-second-boundary"), role="viewer")
        db_session.add_all([local, second])
        db_session.commit()
        original = (local.id, local.username, local.password_hash, second.id, second.username)
        import_id = "if-history-" + uuid4().hex
        first = import_assessment_history(SOURCE_URL, target_url, import_id)
        assert first["preserved_original_rows"]
        for name in HISTORY_TABLES:
            if name == "checkpoint_migrations":
                assert first["increments"][name] == 0
            else:
                assert first["increments"][name] == len(source_rows[name])
        assert first["after_ledger"] == first["source_ledger"]
        db_session.expire_all()
        mapping = db_session.get(IntegrationIdentityMap, ("assessment", collision["id"]))
        assert mapping.target_user_id == collision["id"]
        assert mapping.target_username == "assessment-" + collision["username"] + "-" + collision["id"]
        local = db_session.get(User, original[0])
        second = db_session.get(User, original[3])
        assert (local.id, local.username, local.password_hash, second.id, second.username) == original
        repeated = import_assessment_history(SOURCE_URL, target_url, import_id)
        assert not any(repeated["increments"].values())
        assert repeated["before_ledger"] == repeated["after_ledger"] == first["after_ledger"]
        assert repeated["before"] == repeated["after"]
        assert db_session.scalar(select(func.count()).select_from(IntegrationIdentityMap)) == len(source_rows["users"])
        with db_session.bind.begin() as target:
            table = tables_for(target, ("assessment_runs",))["assessment_runs"]
            first_run = source_rows["assessment_runs"][0]
            target.execute(table.update().where(table.c.id == first_run["id"]).values(prompt="历史冲突边界输入"))
        with pytest.raises(ValueError, match="内容冲突"):
            import_assessment_history(SOURCE_URL, target_url, import_id)
        with source_engine.connect() as source, source.begin():
            assert source.scalar(text("SELECT current_setting('transaction_read_only')")) == "on"
            for name, table in source_tables.items():
                assert records(source, table) == source_rows[name]
    finally:
        source_engine.dispose()


def test_if03_source_files_keep_hashes_and_repeated_copy_has_no_increment(data_dir):
    source = ".local/assessment/source"
    destination = data_dir / "assessment-source"
    first = copy_history_files(source, destination)
    assert first["copied"] == len(first["source"]) == 5
    assert file_manifest(destination) == file_manifest(source)
    repeated = copy_history_files(source, destination)
    assert repeated["copied"] == 0
    assert repeated["source_sha256"] == first["source_sha256"]
    destination.joinpath("manifest.json").write_text("文件冲突边界输入", encoding="utf-8")
    with pytest.raises(ValueError, match="内容冲突"):
        copy_history_files(source, destination)
