from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from data_center_protocol import AuthorizedScope
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from data_center.database import authorized_connection, install_read_policies


def test_reader_enforces_asset_permissions_and_read_only_transaction():
    database = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop_test")
    schema = "dc_rls_" + uuid4().hex
    content = (Path(__file__).resolve().parents[2] / "docs/03-技术设计/总体接口.md").read_text()
    with database.begin() as connection:
        connection.execute(text("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='data_center_reader') THEN CREATE ROLE data_center_reader NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$"))
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection.execute(text(f'CREATE TABLE "{schema}".dc_document_chunks (source_id text, asset_id text, snapshot_id text, content text)'))
        connection.execute(text(f'INSERT INTO "{schema}".dc_document_chunks VALUES (:source,:asset,:snapshot,:content)'),
                           [{"source": "project-shop/interfaces", "asset": "project-shop/interfaces::allowed", "snapshot": "a" * 64, "content": content},
                            {"source": "project-shop/interfaces", "asset": "project-shop/interfaces::restricted", "snapshot": "a" * 64, "content": content}])
    install_read_policies(database, schema)
    scope = AuthorizedScope(actor_id="local-reader", source_ids=["project-shop/interfaces"],
                            asset_ids=["project-shop/interfaces::allowed"],
                            policy_version="b" * 64, fingerprint=sha256(b"local-reader").hexdigest())
    with authorized_connection(database, scope, "a" * 64) as connection:
        assert connection.execute(text(f'SELECT asset_id FROM "{schema}".dc_document_chunks')).scalars().all() == ["project-shop/interfaces::allowed"]
        assert connection.scalar(text("SELECT current_user")) == "data_center_reader"
        assert connection.scalar(text("SHOW transaction_read_only")) == "on"
    with authorized_connection(database, scope.model_copy(update={"asset_ids": []}), "a" * 64) as connection:
        assert connection.execute(text(f'SELECT asset_id FROM "{schema}".dc_document_chunks')).all() == []
    with pytest.raises(DBAPIError) as denied, authorized_connection(database, scope) as connection:
        connection.execute(text(f'UPDATE "{schema}".dc_document_chunks SET content=:content'), {"content": content})
    assert denied.value.orig.sqlstate in {"25006", "42501"}
    database.dispose()
