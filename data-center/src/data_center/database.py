import json
from contextlib import contextmanager

from data_center_protocol import AuthorizedScope
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


@contextmanager
def authorized_connection(database, scope: AuthorizedScope, snapshot_id: str | None = None):
    with database.connect() as connection, connection.begin():
        connection.execute(text("SET TRANSACTION READ ONLY"))
        values = {
            "actor_id": scope.actor_id, "source_ids": json.dumps(scope.source_ids),
            "source_wide": json.dumps(scope.source_wide), "asset_ids": json.dumps(scope.asset_ids),
            "employee_ids": json.dumps(scope.employee_ids),
            "organization": "true" if scope.organization else "false",
            "snapshot_id": snapshot_id or "", "policy_version": scope.policy_version,
            "fingerprint": scope.fingerprint,
        }
        for key, value in values.items():
            connection.execute(text("SELECT set_config(:key, :value, true)"),
                               {"key": f"app.{key}", "value": value})
        connection.execute(text("SET LOCAL ROLE data_center_reader"))
        yield connection


def install_read_policies(database, schema="public"):
    quoted = database.dialect.identifier_preparer.quote(schema)
    allowed_source = "source_id IN (SELECT jsonb_array_elements_text(current_setting('app.source_ids', true)::jsonb))"
    allowed_asset = "(source_id IN (SELECT jsonb_array_elements_text(current_setting('app.source_wide', true)::jsonb)) OR asset_id IN (SELECT jsonb_array_elements_text(current_setting('app.asset_ids', true)::jsonb)))"
    allowed_snapshot = "(current_setting('app.snapshot_id', true) = '' OR snapshot_id = current_setting('app.snapshot_id', true))"
    rules = {
        "dc_documents": f"{allowed_source} AND {allowed_asset} AND {allowed_snapshot}",
        "dc_document_chunks": f"{allowed_source} AND {allowed_asset} AND {allowed_snapshot}",
        "dc_graph_chunk_refs": f"{allowed_source} AND {allowed_asset} AND {allowed_snapshot}",
        "dc_document_snapshots": "state = 'published' AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(payload->'source_ids') AS item(source_id) WHERE item.source_id IN (SELECT jsonb_array_elements_text(current_setting('app.source_ids', true)::jsonb)))",
        "dc_graph_scope_aliases": "actor_id = current_setting('app.actor_id', true) AND policy_version = current_setting('app.policy_version', true) AND scope_fingerprint = current_setting('app.fingerprint', true)",
        "dc_graph_indexes": "(policy_version = current_setting('app.policy_version', true) AND scope_fingerprint = current_setting('app.fingerprint', true)) OR EXISTS (SELECT 1 FROM dc_graph_scope_aliases a WHERE a.graph_fingerprint = fingerprint AND a.actor_id = current_setting('app.actor_id', true) AND a.policy_version = current_setting('app.policy_version', true) AND a.scope_fingerprint = current_setting('app.fingerprint', true))",
        "data_center_analytics_record": f"{allowed_source} AND {allowed_snapshot} AND (current_setting('app.organization', true) = 'true' OR employee_id::text IN (SELECT jsonb_array_elements_text(current_setting('app.employee_ids', true)::jsonb)))",
        "data_center_analytics_snapshot": f"{allowed_source} AND {allowed_snapshot} AND state = 'published'",
        "sales_orders": f"{allowed_source} AND {allowed_snapshot} AND (current_setting('app.organization', true) = 'true' OR employee_id::text IN (SELECT jsonb_array_elements_text(current_setting('app.employee_ids', true)::jsonb)))",
        "sales_quotas": f"{allowed_source} AND {allowed_snapshot} AND (current_setting('app.organization', true) = 'true' OR employee_id::text IN (SELECT jsonb_array_elements_text(current_setting('app.employee_ids', true)::jsonb)))",
    }
    with database.begin() as connection:
        connection.execute(text(f"GRANT USAGE ON SCHEMA {quoted} TO data_center_reader"))
        tables = set(connection.scalars(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = :schema"), {"schema": schema}))
        for table, predicate in rules.items():
            if table not in tables:
                continue
            name = f"{quoted}.{database.dialect.identifier_preparer.quote(table)}"
            connection.execute(text(f"ALTER TABLE {name} ENABLE ROW LEVEL SECURITY"))
            connection.execute(text(f"GRANT SELECT ON {name} TO data_center_reader"))
            exists = connection.scalar(text(
                "SELECT 1 FROM pg_policies WHERE schemaname=:schema AND tablename=:table AND policyname='dc_authorized_read'"),
                {"schema": schema, "table": table})
            if exists:
                connection.execute(text(f"ALTER POLICY dc_authorized_read ON {name} TO data_center_reader USING ({predicate})"))
            else:
                connection.execute(text(f"CREATE POLICY dc_authorized_read ON {name} FOR SELECT TO data_center_reader USING ({predicate})"))
        if "mf_time_spine" in tables:
            connection.execute(text(f"GRANT SELECT ON {quoted}.mf_time_spine TO data_center_reader"))
