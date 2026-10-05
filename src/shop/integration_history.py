import hashlib
import json
import shutil
from datetime import date, datetime
from decimal import Decimal
from graphlib import TopologicalSorter
from pathlib import Path

import numpy as np
from pgvector.sqlalchemy import Vector  # noqa: F401
from sqlalchemy import MetaData, Table, and_, create_engine, inspect, select, text

from shop.models import now

SOURCE_NAME = "assessment"
HISTORY_TABLES = (
    "users", "assessment_sources", "assessment_employees", "assessment_quotas",
    "assessment_access", "assessment_runs", "assessment_tool_calls", "provider_requests",
    "checkpoint_migrations", "checkpoints", "checkpoint_blobs", "checkpoint_writes",
)
AUDIT_EXCLUSIONS = {
    "budget_accounts", "alembic_version", "integration_identity_maps", "integration_history_imports",
}


def json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, memoryview)):
        return {"bytes_hex": bytes(value).hex()}
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(f"不支持的记录字段类型：{type(value).__name__}")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), default=json_value).encode()).hexdigest()


def tables_for(connection, names):
    metadata = MetaData()
    return {name: Table(name, metadata, autoload_with=connection) for name in names}


def key_for(table, row):
    columns = list(table.primary_key.columns)
    if not columns:
        raise ValueError(f"历史表没有主键：{table.name}")
    return tuple(row[column.name] for column in columns)


def records(connection, table):
    return [dict(row) for row in connection.execute(select(table)).mappings()]


def ledger(connection, tables):
    rows = records(connection, tables["provider_requests"])
    spent = sum((row["charged_cny"] for row in rows if row["charged_cny"] is not None), Decimal(0))
    reserved = sum((row["reservation_cny"] for row in rows if row["charged_cny"] is None), Decimal(0))
    account = connection.execute(select(tables["budget_accounts"]).where(
        tables["budget_accounts"].c.id == "project")).mappings().one_or_none()
    actual = (Decimal(0), Decimal(0)) if account is None else (account["spent_cny"], account["reserved_cny"])
    if actual != (spent, reserved):
        raise ValueError("请求金额与预算账户不一致")
    return {"spent_cny": str(spent), "reserved_cny": str(reserved),
            "unknown_count": sum(row["status"] == "unknown" for row in rows),
            "unknown_reserved_cny": str(sum((row["reservation_cny"] for row in rows
                                             if row["status"] == "unknown"), Decimal(0)))}


def row_digests(connection, table):
    return {key_for(table, row): digest(dict(row))
            for row in connection.execute(select(table)).mappings()}


def audit_snapshot(connection):
    names = sorted(set(inspect(connection).get_table_names()) - AUDIT_EXCLUSIONS)
    tables = tables_for(connection, names)
    snapshots = {name: row_digests(connection, table) for name, table in tables.items()}
    return tables, snapshots


def safe_summary(snapshots):
    return {name: {"count": len(rows), "sha256": digest(sorted(rows.values()))}
            for name, rows in snapshots.items()}


def copy_row(connection, table, original):
    if set(original) - set(table.c.keys()):
        raise ValueError(f"来源表存在未接入的字段：{table.name}")
    row = {column.name: original.get(column.name) for column in table.columns}
    condition = and_(*(column == row[column.name] for column in table.primary_key.columns))
    existing = connection.execute(select(table).where(condition)).mappings().one_or_none()
    if existing is not None:
        if dict(existing) != row:
            raise ValueError(f"历史记录主键存在内容冲突：{table.name}")
        return False
    connection.execute(table.insert().values(row))
    return True


def copy_users(connection, table, mapping_table, rows):
    destination = records(connection, table)
    by_id = {row["id"]: row for row in destination}
    occupied = {row["username"] for row in destination}
    # 来源已有账号名称也参与检查，保证新增前缀不占用另一来源身份。
    source_names = {row["username"] for row in rows}
    imported, renamed = 0, 0
    for original in sorted(rows, key=lambda item: item["id"]):
        identity = {"source_name": SOURCE_NAME, "source_user_id": original["id"]}
        previous = connection.execute(select(mapping_table).where(and_(*(
            mapping_table.c[name] == value for name, value in identity.items()
        )))).mappings().one_or_none()
        row = dict(original)
        if previous is not None:
            if previous["source_username"] != original["username"] or previous["target_user_id"] != original["id"]:
                raise ValueError("保存的身份映射与来源不一致")
            row["username"] = previous["target_username"]
        elif original["id"] not in by_id and original["username"] in occupied:
            candidate = "assessment-" + original["username"]
            if candidate in occupied or candidate in source_names:
                candidate += "-" + original["id"]
            if len(candidate) > table.c.username.type.length or candidate in occupied or candidate in source_names:
                raise ValueError("考核账号名称无法满足长度或唯一性要求")
            row["username"] = candidate
        imported += copy_row(connection, table, row)
        occupied.add(row["username"])
        renamed += row["username"] != original["username"]
        mapping = {**identity, "target_user_id": row["id"],
                   "source_username": original["username"], "target_username": row["username"]}
        copy_row(connection, mapping_table, mapping)
    return imported, renamed


def ordered_runs(rows):
    by_id = {row["id"]: row for row in rows}
    graph = {row["id"]: (() if row["parent_id"] is None else (row["parent_id"],)) for row in rows}
    order = tuple(TopologicalSorter(graph).static_order())
    if set(order) != set(by_id):
        raise ValueError("考核任务的父任务不在来源中")
    return [by_id[run_id] for run_id in order]


def import_assessment_history(source_url, target_url, import_id):
    source_engine = create_engine(source_url, isolation_level="REPEATABLE READ",
                                  execution_options={"postgresql_readonly": True})
    target_engine = create_engine(target_url)
    try:
        with source_engine.connect() as source, source.begin():
            if source.scalar(text("SELECT current_setting('transaction_read_only')")) != "on":
                raise ValueError("考核来源连接必须为只读事务")
            source_tables = tables_for(source, (*HISTORY_TABLES, "budget_accounts"))
            history = {name: records(source, source_tables[name]) for name in HISTORY_TABLES}
            source_summary = {name: {"count": len(rows), "sha256": digest(sorted(digest(row) for row in rows))}
                              for name, rows in history.items()}
            source_sha256 = digest(source_summary)
            source_ledger = ledger(source, source_tables)
            with target_engine.begin() as target:
                target.execute(text("SELECT pg_advisory_xact_lock(hashtext('shop:history-import'))"))
                target.execute(text("SELECT pg_advisory_xact_lock(hashtext('shop:budget-account'))"))
                target_tables, before = audit_snapshot(target)
                auxiliary = tables_for(target, ("budget_accounts", "integration_identity_maps",
                                                "integration_history_imports"))
                target_tables.update(auxiliary)
                before_ledger = ledger(target, target_tables)
                imports = target_tables["integration_history_imports"]
                prior = target.execute(select(imports).where(imports.c.id == import_id)).mappings().one_or_none()
                if prior is not None and (prior["source_sha256"] != source_sha256
                                          or prior["source_name"] != SOURCE_NAME):
                    raise ValueError("导入编号对应的来源快照不一致")
                increments = {}
                increments["users"], renamed = copy_users(
                    target, target_tables["users"], target_tables["integration_identity_maps"], history["users"],
                )
                request_spent, request_reserved = Decimal(0), Decimal(0)
                for name in HISTORY_TABLES[1:]:
                    rows = ordered_runs(history[name]) if name == "assessment_runs" else history[name]
                    increments[name] = 0
                    for row in rows:
                        inserted = copy_row(target, target_tables[name], row)
                        increments[name] += inserted
                        if inserted and name == "provider_requests":
                            if row["charged_cny"] is None:
                                request_reserved += row["reservation_cny"]
                            else:
                                request_spent += row["charged_cny"]
                accounts = target_tables["budget_accounts"]
                account = target.execute(select(accounts).where(accounts.c.id == "project").with_for_update()
                                         ).mappings().one_or_none()
                if account is None:
                    target.execute(accounts.insert().values(id="project", spent_cny=request_spent,
                                                            reserved_cny=request_reserved))
                else:
                    target.execute(accounts.update().where(accounts.c.id == "project").values(
                        spent_cny=account["spent_cny"] + request_spent,
                        reserved_cny=account["reserved_cny"] + request_reserved,
                    ))
                after = {name: row_digests(target, target_tables[name]) for name in before}
                for name, rows in before.items():
                    if any(after[name].get(key) != value for key, value in rows.items()):
                        raise ValueError(f"主库原始记录发生变化：{name}")
                after_ledger = ledger(target, target_tables)
                report = {"milestone": "IF-03", "import_id": import_id,
                          "source_sha256": source_sha256, "source": source_summary,
                          "source_ledger": source_ledger, "before": safe_summary(before),
                          "after": safe_summary(after), "before_ledger": before_ledger,
                          "after_ledger": after_ledger, "increments": increments,
                          "renamed_users": renamed, "preserved_original_rows": True,
                          "added_spent_cny": str(request_spent), "added_reserved_cny": str(request_reserved)}
                if prior is not None and any(increments.values()):
                    raise ValueError("相同快照重复导入出现新增记录")
                if prior is None:
                    target.execute(imports.insert().values(id=import_id, source_name=SOURCE_NAME,
                                                          source_sha256=source_sha256,
                                                          report=report, created_at=now()))
                return report
    finally:
        source_engine.dispose()
        target_engine.dispose()


def file_manifest(directory):
    root = Path(directory).resolve(strict=True)
    rows = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError("历史文件目录包含不允许的路径")
        if path.is_file():
            content = path.read_bytes()
            rows[path.relative_to(root).as_posix()] = {"sha256": hashlib.sha256(content).hexdigest(),
                                                      "size": len(content)}
    return rows


def copy_history_files(source_directory, target_directory):
    source, target = Path(source_directory).resolve(strict=True), Path(target_directory).resolve()
    if source == target or target.is_relative_to(source) or source.is_relative_to(target):
        raise ValueError("历史文件来源与目标目录不能相互包含")
    source_manifest = file_manifest(source)
    target.mkdir(parents=True, exist_ok=True)
    previous = file_manifest(target)
    copied = 0
    for relative, expected in source_manifest.items():
        destination = target / relative
        if relative in previous:
            if previous[relative] != expected:
                raise ValueError("历史文件存在同名内容冲突")
            continue
        if not destination.resolve().is_relative_to(target):
            raise ValueError("历史文件目标路径超出目录")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, destination)
        copied += 1
    after = file_manifest(target)
    if file_manifest(source) != source_manifest:
        raise ValueError("历史文件来源在汇集期间发生变化")
    if any(after.get(name) != expected for name, expected in {**previous, **source_manifest}.items()):
        raise ValueError("历史文件复制后的摘要不一致")
    return {"milestone": "IF-03", "source": source_manifest, "copied": copied,
            "before": previous, "after": after, "source_sha256": digest(source_manifest)}
