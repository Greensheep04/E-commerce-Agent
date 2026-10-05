import argparse
import json
from pathlib import Path

from sqlalchemy import create_engine, func, inspect, select, text

from shop.config import Settings
from shop.integration_history import ledger, tables_for


def inspect_database(url):
    engine = create_engine(url, execution_options={"postgresql_readonly": True})
    try:
        with engine.connect() as connection, connection.begin():
            names = inspect(connection).get_table_names()
            tables = tables_for(connection, names)
            running = {}
            for name in ("harness_tasks", "customer_turns", "assessment_runs"):
                if name in tables:
                    table = tables[name]
                    running[name] = [dict(row) for row in connection.execute(select(
                        table.c.id, table.c.status,
                    ).where(table.c.status.in_(["queued", "running"]))).mappings()]
            active_requests = connection.scalar(select(func.count()).select_from(tables["provider_requests"]).where(
                tables["provider_requests"].c.status == "sent"))
            activity = [dict(row) for row in connection.execute(text(
                "SELECT pid, application_name, state, wait_event_type, wait_event "
                "FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"
            )).mappings()]
            return {"running": running, "sent_requests": active_requests,
                    "ledger": ledger(connection, tables), "activity": activity,
                    "tables": {name: connection.scalar(select(func.count()).select_from(table))
                               for name, table in tables.items()}}
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = {"main": inspect_database(Settings().database_url),
              "assessment": inspect_database("postgresql+psycopg://shop@127.0.0.1:55436/shop")}
    path = args.report
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({name: {"running": data["running"], "sent_requests": data["sent_requests"],
                            "ledger": data["ledger"]} for name, data in result.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
