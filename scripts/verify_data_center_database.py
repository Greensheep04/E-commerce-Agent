from __future__ import annotations

import json
import math
import uuid
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg import sql


def fetch_records(connection, statement: str, parameters: tuple = ()) -> list[dict]:
    cursor = connection.execute(statement, parameters)
    columns = [column.name for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def amount_text(value: Decimal) -> str:
    return format(value, "f")


def read_shop_snapshot(expected_data_directory: str) -> dict:
    with psycopg.connect(
        host="127.0.0.1",
        port=55432,
        dbname="shop",
        user="shop",
        connect_timeout=5,
        application_name="dc01_database_readonly_snapshot",
        autocommit=True,
    ) as connection:
        connection.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        server = fetch_records(
            connection,
            """
            SELECT version(), current_setting('server_version_num') AS version_num,
                   current_setting('data_directory') AS data_directory,
                   current_database() AS database, current_user AS role
            """,
        )[0]
        if not server["version"].startswith("PostgreSQL 18.6 "):
            raise RuntimeError(f"要求 PostgreSQL 18.6，当前版本为：{server['version']}")
        if server["data_directory"] != expected_data_directory:
            raise RuntimeError(
                f"要求数据库目录为 {expected_data_directory}，当前目录为 {server['data_directory']}"
            )
        if server["database"] != "shop" or server["role"] != "shop":
            raise RuntimeError(
                f"要求连接 shop 数据库和 shop 账号，当前为 {server['database']} / {server['role']}"
            )

        database_permission = fetch_records(
            connection,
            """
            SELECT role_row.rolsuper AS is_superuser,
                   role_row.rolcreatedb AS has_createdb_attribute,
                   (role_row.rolsuper OR role_row.rolcreatedb) AS can_create_database,
                   EXISTS (SELECT 1 FROM pg_database
                           WHERE datname = 'shop_data_center') AS database_name_exists
            FROM pg_roles AS role_row
            WHERE role_row.rolname = current_user
            """,
        )[0]
        database_permission["can_create_shop_data_center"] = (
            database_permission["can_create_database"]
            and not database_permission["database_name_exists"]
        )

        accounts = fetch_records(
            connection,
            """
            SELECT id, spent_cny::text AS spent_cny,
                   reserved_cny::text AS reserved_cny
            FROM budget_accounts
            ORDER BY id
            """,
        )
        request_status_totals = fetch_records(
            connection,
            """
            SELECT status, COUNT(*)::int AS request_count,
                   COALESCE(SUM(reservation_cny), 0)::text AS reservation_total_cny,
                   COALESCE(SUM(charged_cny), 0)::text AS charged_total_cny
            FROM provider_requests
            GROUP BY status
            ORDER BY status
            """,
        )
        open_requests = fetch_records(
            connection,
            """
            SELECT id, status, task_id, assessment_id, run_id, service, model,
                   reservation_cny::text AS reservation_cny,
                   charged_cny::text AS charged_cny,
                   created_at::text AS created_at,
                   COALESCE(price_basis->>'verification_scope',
                            price_basis->>'scope',
                            CASE WHEN assessment_id IS NOT NULL
                                 THEN 'assessment' ELSE 'harness' END) AS verification_scope,
                   price_basis->>'integration_run_id' AS cost_window_id,
                   price_basis->>'cluster_goal_id' AS cluster_goal_id,
                   price_basis->>'cluster_run_id' AS cluster_run_id
            FROM provider_requests
            WHERE status IN ('reserved', 'sent', 'unknown')
            ORDER BY created_at, id
            """,
        )
        request_amounts = fetch_records(
            connection,
            """
            SELECT COALESCE(SUM(COALESCE(charged_cny, 0)), 0)::text
                       AS requests_charged_cny,
                   COALESCE(SUM(reservation_cny) FILTER
                       (WHERE status IN ('reserved', 'sent', 'unknown')), 0)::text
                       AS open_reserved_cny,
                   COALESCE(SUM(reservation_cny) FILTER (WHERE status = 'unknown'),
                            0)::text AS unknown_reserved_cny,
                   COUNT(*) FILTER
                       (WHERE status IN ('reserved', 'sent', 'unknown'))::int
                       AS open_request_count,
                   COUNT(*) FILTER (WHERE status = 'unknown')::int
                       AS unknown_request_count
            FROM provider_requests
            """,
        )[0]

        active_windows = fetch_records(
            connection,
            """
            SELECT budget_window.id, budget_window.limit_cny::text AS limit_cny,
                   budget_window.created_at::text AS created_at,
                   COALESCE(SUM(COALESCE(request.charged_cny,
                                         request.reservation_cny)), 0)::text
                       AS committed_cny,
                   COALESCE(SUM(request.reservation_cny) FILTER
                       (WHERE request.status IN ('reserved', 'sent', 'unknown')),
                       0)::text AS open_reserved_cny,
                   COUNT(request.id)::int AS request_count,
                   COUNT(request.id) FILTER (WHERE request.status = 'unknown')::int
                       AS unknown_request_count
            FROM integration_budget_windows AS budget_window
            LEFT JOIN provider_requests AS request
              ON request.price_basis->>'integration_run_id' = budget_window.id
            WHERE budget_window.active
            GROUP BY budget_window.id, budget_window.limit_cny, budget_window.created_at
            ORDER BY budget_window.created_at, budget_window.id
            """,
        )
        for window in active_windows:
            window["remaining_cny"] = amount_text(
                Decimal(window["limit_cny"]) - Decimal(window["committed_cny"])
            )

        task_queries = {
            "harness_tasks": """
                SELECT id AS task_id, status, model_calls, tool_calls,
                       elapsed_seconds::text AS elapsed_seconds,
                       cancel_requested, created_at::text AS created_at,
                       updated_at::text AS updated_at
                FROM harness_tasks
                WHERE status IN ('created', 'queued', 'running')
                ORDER BY created_at, id
            """,
            "customer_turns": """
                SELECT id AS task_id, status, model_calls, tool_calls,
                       elapsed_seconds::text AS elapsed_seconds,
                       cancel_requested, received_at::text AS received_at,
                       started_at::text AS started_at
                FROM customer_turns
                WHERE status IN ('queued', 'running')
                ORDER BY received_at, id
            """,
            "assessment_runs": """
                SELECT id AS task_id, run_id, status, model_calls, tool_calls,
                       elapsed_seconds::text AS elapsed_seconds,
                       cancel_requested, created_at::text AS created_at,
                       updated_at::text AS updated_at
                FROM assessment_runs
                WHERE status IN ('created', 'queued', 'running')
                ORDER BY created_at, id
            """,
            "cluster_runs": """
                SELECT run.run_id AS task_id, run.goal_id, run.status,
                       run.document->>'updated_at' AS updated_at,
                       run.document->>'spent_cny' AS spent_cny,
                       run.document->>'reserved_cny' AS reserved_cny,
                       goal.document->>'budget_cny' AS goal_budget_cny
                FROM cluster_runs AS run
                JOIN cluster_goals AS goal ON goal.goal_id = run.goal_id
                WHERE run.status IN ('queued', 'running')
                ORDER BY run.run_id
            """,
            "cluster_tasks": """
                SELECT task.task_id, task.run_id, task.status
                FROM cluster_tasks AS task
                WHERE task.status IN ('queued', 'sent', 'running')
                ORDER BY task.run_id, task.task_id
            """,
        }
        task_summary = {}
        for table, statement in task_queries.items():
            records = fetch_records(connection, statement)
            counts = Counter(record["status"] for record in records)
            task_summary[table] = {
                "active_or_pending_count": len(records),
                "running_count": counts.get("running", 0),
                "queued_count": counts.get("queued", 0),
                "sent_count": counts.get("sent", 0),
                "created_count": counts.get("created", 0),
                "records": records,
            }

        project_account = next(
            (account for account in accounts if account["id"] == "project"), None
        )
        if project_account is None:
            reconciliation = {"project_account_found": False}
        else:
            spent_delta = Decimal(project_account["spent_cny"]) - Decimal(
                request_amounts["requests_charged_cny"]
            )
            reserved_delta = Decimal(project_account["reserved_cny"]) - Decimal(
                request_amounts["open_reserved_cny"]
            )
            reconciliation = {
                "project_account_found": True,
                "spent_matches_request_charges": spent_delta == 0,
                "reserved_matches_open_requests": reserved_delta == 0,
                "requests_charged_cny": request_amounts["requests_charged_cny"],
                "open_reserved_cny": request_amounts["open_reserved_cny"],
                "spent_delta_cny": amount_text(spent_delta),
                "reserved_delta_cny": amount_text(reserved_delta),
            }

        connection.execute("ROLLBACK")

    return {
        "captured_at_utc": datetime.now(UTC).isoformat(),
        "server": server,
        "database_creation_permission": database_permission,
        "accounts": accounts,
        "request_status_totals": request_status_totals,
        "open_requests": open_requests,
        "open_request_summary": request_amounts,
        "ledger_reconciliation": reconciliation,
        "active_cost_windows": active_windows,
        "tasks": task_summary,
        "read_only_transaction": True,
        "database_creation_attempted": False,
    }


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    expected_data_directory = str(project_root / ".local" / "postgres")
    probe_schema = f"dc01_vector_probe_{uuid.uuid4().hex}"

    with psycopg.connect(
        host="127.0.0.1",
        port=55432,
        dbname="shop_test",
        user="shop",
        connect_timeout=5,
        application_name="dc01_database_verification",
        autocommit=True,
    ) as connection:
        connection.execute("BEGIN")
        server = connection.execute(
            """
            SELECT version(), current_setting('server_version_num'),
                   current_setting('data_directory'), current_database(),
                   current_user,
                   (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
            """
        ).fetchone()
        version, version_num, data_directory, database, role, is_superuser = server
        if not version.startswith("PostgreSQL 18.6 "):
            raise RuntimeError(f"要求 PostgreSQL 18.6，当前版本为：{version}")
        if data_directory != expected_data_directory:
            raise RuntimeError(
                f"要求数据库目录为 {expected_data_directory}，当前目录为 {data_directory}"
            )
        if database != "shop_test":
            raise RuntimeError(f"要求数据库为 shop_test，当前数据库为 {database}")

        available = connection.execute(
            "SELECT default_version, installed_version "
            "FROM pg_available_extensions WHERE name = 'vector'"
        ).fetchone()
        if available is None:
            raise RuntimeError("当前 PostgreSQL 安装目录无法找到 pgvector")
        available_version, available_installed_version = available

        installed = connection.execute(
            """
            SELECT extension.extversion, namespace.nspname
            FROM pg_extension AS extension
            JOIN pg_namespace AS namespace ON namespace.oid = extension.extnamespace
            WHERE extension.extname = 'vector'
            """
        ).fetchone()
        if installed is None:
            raise RuntimeError("shop_test 尚未安装 pgvector")
        extension_version, extension_schema = installed
        if available_installed_version != extension_version:
            raise RuntimeError("pg_available_extensions 与 pg_extension 返回的版本不同")

        connection.execute("LOAD 'vector'")
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(probe_schema)))
        connection.execute(
            sql.SQL("SET LOCAL search_path TO {}, {}").format(
                sql.Identifier(probe_schema), sql.Identifier(extension_schema)
            )
        )
        connection.execute(
            """
            CREATE TABLE vector_distance_probe (
                left_vector vector(3) NOT NULL,
                right_vector vector(3) NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO vector_distance_probe (left_vector, right_vector)
            VALUES (%s::vector(3), %s::vector(3))
            """,
            ("[1,2,3]", "[4,6,3]"),
        )
        distance = connection.execute(
            "SELECT left_vector <-> right_vector FROM vector_distance_probe"
        ).fetchone()[0]
        row_count = connection.execute("SELECT count(*) FROM vector_distance_probe").fetchone()[0]
        if row_count != 1:
            raise RuntimeError(f"要求数学检查记录为 1 条，当前记录为 {row_count} 条")
        if not math.isclose(distance, 5.0, rel_tol=0.0, abs_tol=1e-12):
            raise RuntimeError(f"要求欧氏距离为 5.0，当前结果为 {distance}")

        connection.execute("ROLLBACK")
        rollback_state = connection.execute(
            """
            SELECT
                EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s),
                extension.extversion,
                namespace.nspname
            FROM pg_extension AS extension
            JOIN pg_namespace AS namespace ON namespace.oid = extension.extnamespace
            WHERE extension.extname = 'vector'
            """,
            (probe_schema,),
        ).fetchone()
        if rollback_state is None:
            raise RuntimeError("事务回退后未找到 pgvector 扩展")
        probe_schema_persisted, version_after_rollback, schema_after_rollback = rollback_state
        if probe_schema_persisted:
            raise RuntimeError(f"事务回退后仍存在检查模式 {probe_schema}")
        if version_after_rollback != extension_version:
            raise RuntimeError("事务回退后 pgvector 版本发生变化")
        if schema_after_rollback != extension_schema:
            raise RuntimeError("事务回退后 pgvector 所在模式发生变化")

    project_snapshot = read_shop_snapshot(expected_data_directory)
    print(
        json.dumps(
            {
                "status": "passed",
                "server": {
                    "version": version,
                    "version_num": version_num,
                    "data_directory": data_directory,
                    "database": database,
                    "role": role,
                    "is_superuser": is_superuser,
                },
                "extension": {
                    "name": "vector",
                    "available_default_version": available_version,
                    "installed_version": extension_version,
                    "schema": extension_schema,
                    "dynamic_load": "LOAD vector succeeded",
                },
                "transaction_probe": {
                    "schema": probe_schema,
                    "table": "vector_distance_probe",
                    "vectors": ["[1,2,3]", "[4,6,3]"],
                    "operator": "<->",
                    "expected_euclidean_distance": 5.0,
                    "actual_euclidean_distance": distance,
                    "row_count": row_count,
                    "rolled_back": True,
                    "probe_schema_persisted": probe_schema_persisted,
                },
                "shop_database_snapshot": project_snapshot,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
