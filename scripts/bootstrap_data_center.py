import argparse
import json
from pathlib import Path

import data_center.analytics.models
import data_center.documents.models
import data_center.store  # noqa: F401
import psycopg
from data_center.database import Base, install_read_policies
from sqlalchemy import create_engine, text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin-url", default="postgresql://shop@127.0.0.1:55432/postgres")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with psycopg.connect(args.admin_url, autocommit=True) as connection:
        for role, login in (("data_center_reader", False), ("data_center_writer", True)):
            exists = connection.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)).fetchone()
            if exists is None:
                connection.execute(psycopg.sql.SQL("CREATE ROLE {} {} NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE").format(
                    psycopg.sql.Identifier(role), psycopg.sql.SQL("LOGIN" if login else "NOLOGIN")))
            record = connection.execute("SELECT rolsuper,rolbypassrls,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=%s", (role,)).fetchone()
            if any(record):
                raise PermissionError("数据中台角色包含超出批准范围的数据库权限")
        connection.execute("GRANT data_center_reader TO data_center_writer")
        if connection.execute("SELECT 1 FROM pg_database WHERE datname='shop_data_center'").fetchone() is None:
            connection.execute("CREATE DATABASE shop_data_center OWNER data_center_writer")
    admin = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop_data_center")
    with admin.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"))
    writer = create_engine("postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
    Base.metadata.create_all(writer)
    with writer.begin() as connection:
        connection.execute(text("ALTER TABLE dc_graph_indexes ADD COLUMN IF NOT EXISTS scope_fingerprint varchar(64) NOT NULL"))
        connection.execute(text("ALTER TABLE data_center_analytics_snapshot ALTER COLUMN quality TYPE jsonb USING quality::jsonb"))
        connection.execute(text("ALTER TABLE data_center_analytics_sync_page ALTER COLUMN rows TYPE jsonb USING rows::jsonb"))
        connection.execute(text("ALTER TABLE data_center_analytics_record ALTER COLUMN subtotal TYPE numeric(38,6), ALTER COLUMN quota_amount TYPE numeric(38,6)"))
    install_read_policies(writer)
    with writer.begin() as connection:
        connection.execute(text("CREATE TABLE IF NOT EXISTS dc_schema_versions (version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT now())"))
        connection.execute(text("INSERT INTO dc_schema_versions(version) VALUES(1) ON CONFLICT DO NOTHING"))
        tables = connection.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")).scalars().all()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"database": "shop_data_center", "schema_version": 1,
                                      "writer": "data_center_writer", "reader": "data_center_reader",
                                      "tables": tables}, ensure_ascii=False, indent=2) + "\n")
    writer.dispose()
    admin.dispose()
    print(json.dumps({"database": "shop_data_center", "table_count": len(tables), "status": "completed"}))


if __name__ == "__main__":
    main()
