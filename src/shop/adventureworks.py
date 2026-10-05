import csv
import hashlib
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pandas as pd
from sqlalchemy import func, insert, select
from sqlalchemy.orm import Session

from shop.customer_models import (
    CustomerOrder,
    CustomerProfile,
    OrderDataset,
    OrderLine,
    OrderProduct,
)
from shop.models import Source

REVISION = "beaab06ef72831089ca80e5355d65e661fd19b26"
SOURCE_ID = "microsoft/adventureworks-oltp"
BASE_URL = f"https://raw.githubusercontent.com/microsoft/sql-server-samples/{REVISION}/samples/databases/adventure-works/oltp-install-script/"
FILES = {
    "SalesOrderHeader": (31465, 26, "cf637c7d58006ebbc13daf0b8ed6cf7f738165d298ae0b5479a59237429c1882"),
    "SalesOrderDetail": (121317, 11, "220129977aa63e34fa4a1d66c2604bd2184373151efb09c98b343371ba77ab8a"),
    "Product": (504, 25, "df0379a7cba8b11b97ea5d104cf1313ef9f683137b6426f2fac6bb8c74c2a2ea"),
    "Customer": (19820, 7, "4704aa5b46aed326599014524c821aca50e938587e6a1133c0f6d9aae5c5efc6"),
    "Person": (19972, 13, "e22b399865b5f322eda6b34c4c4532165a1ee62cb2362d3f6bb8c1eebbecd896"),
    "Store": (701, 6, "5f9d31ef5c8a4dfa2243c861654d9962a7e151ed0a5f0f7834d55184952b0520"),
}
TABLE_COUNTS = {"customers": 19820, "products": 504, "orders": 31465, "lines": 121317}


def source_manifest(directory: Path) -> dict:
    files = []
    for name, (count, columns, expected) in FILES.items():
        path = directory / f"{name}.csv"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected:
            raise ValueError(f"{name}来源文件摘要不符合固定版本")
        files.append({"name": path.name, "sha256": digest, "size": path.stat().st_size,
                      "rows": count, "columns": columns})
    return {"source_id": SOURCE_ID, "revision": REVISION, "files": files}


def download_sources(directory: Path) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for name, (_, _, expected) in FILES.items():
            path = directory / f"{name}.csv"
            if path.exists():
                if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                    raise ValueError(f"{name}已有文件摘要错误")
                continue
            response = client.get(BASE_URL + path.name)
            response.raise_for_status()
            if hashlib.sha256(response.content).hexdigest() != expected:
                raise ValueError(f"{name}下载文件摘要错误")
            path.write_bytes(response.content)
    return source_manifest(directory)


def load_source_rows(directory: Path) -> dict[str, list[list[str]]]:
    source_manifest(directory)
    result = {}
    for name, (count, columns, _) in FILES.items():
        path = directory / f"{name}.csv"
        if name in {"Person", "Store"}:
            frame = pd.read_csv(path, sep=r"\+\|", engine="python", header=None,
                                dtype=str, quoting=csv.QUOTE_NONE, keep_default_na=False,
                                on_bad_lines="error")
            rows = frame.values.tolist()
        else:
            with path.open(encoding="utf-8-sig", newline="") as stream:
                rows = list(csv.reader(stream, delimiter="\t", quoting=csv.QUOTE_NONE))
        if len(rows) != count or any(len(row) != columns for row in rows):
            raise ValueError(f"{name}字段数量或记录数量错误")
        result[name] = rows
    return result


def database_counts(session: Session) -> dict:
    return {name: session.scalar(select(func.count()).select_from(model).where(
        model.source_revision == REVISION)) for name, model in (
            ("customers", CustomerProfile), ("products", OrderProduct),
            ("orders", CustomerOrder), ("lines", OrderLine))}


def bulk_insert(session: Session, model, records: list[dict]) -> None:
    for offset in range(0, len(records), 2000):
        session.execute(insert(model), records[offset:offset + 2000])


def import_orders(session: Session, directory: Path) -> dict:
    manifest = source_manifest(directory)
    existing = session.get(OrderDataset, REVISION)
    if existing is not None:
        counts = database_counts(session)
        if existing.manifest != manifest or counts != TABLE_COUNTS:
            raise ValueError("已导入数据的来源或数量不完整")
        return {"counts": counts, "reused": True, "revision": REVISION}
    rows = load_source_rows(directory)
    people = {int(row[0]): " ".join(part for part in row[4:7] if part) for row in rows["Person"]}
    stores = {int(row[0]): row[1] for row in rows["Store"]}
    profiles = []
    for row in rows["Customer"]:
        person_id = int(row[1]) if row[1] else None
        store_id = int(row[2]) if row[2] else None
        person_name = people[person_id] if person_id is not None else None
        store_name = stores[store_id] if store_id is not None else None
        if person_name is None and store_name is None:
            raise ValueError("客户缺少姓名与商店关联")
        profiles.append({"source_revision": REVISION, "customer_id": int(row[0]),
                         "person_id": person_id, "store_id": store_id,
                         "name": store_name or person_name, "person_name": person_name,
                         "store_name": store_name, "source_fields": row})
    products = [{"source_revision": REVISION, "product_id": int(row[0]), "name": row[1],
                 "product_number": row[2], "color": row[5] or None, "size": row[10] or None,
                 "list_price": Decimal(row[9]), "source_fields": row} for row in rows["Product"]]
    orders = [{"source_revision": REVISION, "order_id": int(row[0]), "order_number": row[7],
               "order_date": datetime.fromisoformat(row[2]), "due_date": datetime.fromisoformat(row[3]),
               "ship_date": datetime.fromisoformat(row[4]) if row[4] else None,
               "status": int(row[5]), "online": bool(int(row[6])), "customer_id": int(row[10]),
               "sales_person_id": int(row[11]) if row[11] else None,
               "subtotal": Decimal(row[19]), "tax": Decimal(row[20]),
               "freight": Decimal(row[21]), "total_due": Decimal(row[22]),
               "source_fields": row} for row in rows["SalesOrderHeader"]]
    lines = [{"source_revision": REVISION, "order_id": int(row[0]), "line_id": int(row[1]),
              "quantity": int(row[3]), "product_id": int(row[4]), "unit_price": Decimal(row[6]),
              "discount": Decimal(row[7]), "line_total": Decimal(row[8]),
              "source_fields": row} for row in rows["SalesOrderDetail"]]
    with session.begin_nested():
        if session.get(Source, (SOURCE_ID, REVISION)) is None:
            session.add(Source(source_id=SOURCE_ID, revision=REVISION, license="MIT",
                               license_note="Microsoft AdventureWorks官方业务样例",
                               url=BASE_URL))
            session.flush()
        session.add(OrderDataset(revision=REVISION, source_id=SOURCE_ID,
                                 manifest=manifest, counts=TABLE_COUNTS))
        session.flush()
        bulk_insert(session, CustomerProfile, profiles)
        bulk_insert(session, OrderProduct, products)
        bulk_insert(session, CustomerOrder, orders)
        bulk_insert(session, OrderLine, lines)
        counts = database_counts(session)
        if counts != TABLE_COUNTS:
            raise ValueError("导入后的数据库数量错误")
    session.commit()
    return {"counts": counts, "reused": False, "revision": REVISION}
