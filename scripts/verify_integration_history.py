import argparse
import json
from pathlib import Path

from sqlalchemy import create_engine, inspect, select

from shop.config import Settings
from shop.integration_history import digest, json_value, key_for, ledger, tables_for


def freeze(url):
    engine = create_engine(url, isolation_level="REPEATABLE READ",
                           execution_options={"postgresql_readonly": True})
    try:
        with engine.connect() as connection, connection.begin():
            tables = tables_for(connection, inspect(connection).get_table_names())
            result = {"ledger": ledger(connection, tables), "tables": {}}
            for name, table in tables.items():
                if name in {"alembic_version", "budget_accounts"}:
                    continue
                rows = [{"key": list(key_for(table, row)), "sha256": digest(dict(row))}
                        for row in connection.execute(select(table)).mappings()]
                result["tables"][name] = {"columns": list(table.c.keys()), "rows": rows,
                                           "count": len(rows), "sha256": digest(sorted(
                                               row["sha256"] for row in rows))}
            return result
    finally:
        engine.dispose()


def verify(url, original):
    engine = create_engine(url, execution_options={"postgresql_readonly": True})
    try:
        with engine.connect() as connection, connection.begin():
            tables = tables_for(connection, inspect(connection).get_table_names())
            result = {}
            for name, expected in original["tables"].items():
                table = tables[name]
                columns = expected["columns"]
                current = {digest(list(key_for(table, row))): digest({column: row[column] for column in columns})
                           for row in connection.execute(select(table)).mappings()}
                if any(current.get(digest(row["key"])) != row["sha256"] for row in expected["rows"]):
                    raise ValueError(f"原始数据库记录的内容发生变化：{name}")
                result[name] = {"preserved": len(expected["rows"]), "current": len(current),
                                "original_sha256": expected["sha256"]}
            return {"milestone": "IF-03", "preserved": result, "ledger": ledger(connection, tables)}
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["freeze", "verify"])
    parser.add_argument("--database-url")
    parser.add_argument("--original", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    url = Settings().database_url if args.database_url is None else args.database_url
    if args.action == "freeze":
        result = freeze(url)
    else:
        result = verify(url, json.loads(args.original.read_text(encoding="utf-8")))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=json_value), encoding="utf-8")
    print(json.dumps({"report": str(args.report), "action": args.action}, ensure_ascii=False))


if __name__ == "__main__":
    main()
