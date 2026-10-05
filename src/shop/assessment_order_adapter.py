import csv
import hashlib
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal

from sqlalchemy import MetaData, String, Table, cast, create_engine, func, or_, select

from shop.assessment_orders import DataBlocked, OrderEvidence, OrderSnapshot, SalesOrderAggregate
from shop.assessment_types import PERIOD_END, PERIOD_START, REVISION, SOURCE_ID
from shop.config import Settings

UPSTREAM_SOURCE_ID = "microsoft/adventureworks-oltp"
ORDER_FILE_SHA256 = "cf637c7d58006ebbc13daf0b8ed6cf7f738165d298ae0b5479a59237429c1882"
EXPECTED_COUNTS = {"customers": 19820, "products": 504, "orders": 31465, "lines": 121317}


class AdventureWorksOrderPort:
    def __init__(self, settings: Settings):
        if settings.assessment_order_database_url is None or settings.assessment_order_source_file is None:
            raise DataBlocked("订单只读数据库与官方来源文件尚未配置")
        self.source_file = settings.assessment_order_source_file
        if hashlib.sha256(self.source_file.read_bytes()).hexdigest() != ORDER_FILE_SHA256:
            raise DataBlocked("订单文件摘要与固定官方来源不一致")
        self.source_rows = {}
        with self.source_file.open(encoding="utf-8-sig", newline="") as stream:
            for line, row in enumerate(csv.reader(stream, delimiter="\t", quoting=csv.QUOTE_NONE), start=1):
                if len(row) != 26 or int(row[0]) in self.source_rows:
                    raise DataBlocked("订单来源字段或编号无效")
                self.source_rows[int(row[0])] = (line, row)
        if len(self.source_rows) != EXPECTED_COUNTS["orders"]:
            raise DataBlocked("订单来源数量不完整")
        self.engine = create_engine(settings.assessment_order_database_url, pool_pre_ping=True,
                                   isolation_level="REPEATABLE READ",
                                   execution_options={"postgresql_readonly": True})
        metadata = MetaData()
        self.orders = Table("customer_orders", metadata, autoload_with=self.engine)
        self.datasets = Table("order_datasets", metadata, autoload_with=self.engine)
        self.related_tables = {
            name: Table(table_name, metadata, autoload_with=self.engine)
            for name, table_name in (("customers", "customer_profiles"), ("products", "order_products"),
                                     ("lines", "order_lines"))
        }
        with self.engine.connect() as connection, connection.begin():
            self.dataset = dict(self._dataset(connection))
            records = connection.execute(select(self.orders).where(
                self.orders.c.source_revision == REVISION)).mappings().all()
            if len(records) != EXPECTED_COUNTS["orders"]:
                raise DataBlocked("订单数据库数量不完整")
            for record in records:
                self._verify_record(record)
            self.quarter_digest = self._quarter_digest(connection)
            self._verify_snapshot(connection)

    def _dataset(self, connection):
        record = connection.execute(select(self.datasets).where(
            self.datasets.c.revision == REVISION)).mappings().one_or_none()
        if record is None:
            raise DataBlocked("客服订单批次尚未完成")
        manifest = record["manifest"]
        files = [item for item in manifest.get("files", []) if item.get("name") == "SalesOrderHeader.csv"]
        if (record["source_id"] != UPSTREAM_SOURCE_ID or manifest.get("source_id") != UPSTREAM_SOURCE_ID
                or manifest.get("revision") != REVISION or record["counts"] != EXPECTED_COUNTS
                or len(files) != 1 or files[0].get("sha256") != ORDER_FILE_SHA256
                or files[0].get("rows") != EXPECTED_COUNTS["orders"] or files[0].get("columns") != 26):
            raise DataBlocked("客服订单来源身份、摘要或完整批次无效")
        return record

    def _verify_record(self, record):
        line_and_row = self.source_rows.get(record["order_id"])
        if line_and_row is None:
            raise DataBlocked("数据库订单编号不存在于官方来源")
        _, row = line_and_row
        if (record["source_fields"] != row or record["source_revision"] != REVISION
                or record["order_date"] != datetime.fromisoformat(row[2])
                or record["status"] != int(row[5]) or record["online"] != bool(int(row[6]))
                or record["sales_person_id"] != (int(row[11]) if row[11] else None)
                or record["subtotal"] != Decimal(row[19])):
            raise DataBlocked("数据库订单字段与官方来源不一致")

    def _period_filter(self):
        return (self.orders.c.source_revision == REVISION,
                self.orders.c.order_date >= PERIOD_START, self.orders.c.order_date < PERIOD_END)

    def _quarter_digest(self, connection):
        columns = self.orders.c
        serialized = cast(func.jsonb_build_array(
            columns.order_id, columns.order_date, columns.status, columns.online,
            columns.sales_person_id, columns.subtotal, columns.source_fields), String)
        # PostgreSQL 在固定编号顺序下计算本季度全部字段摘要。
        ordered = select(serialized.label("record")).where(*self._period_filter()).order_by(
            columns.order_id).subquery()
        return connection.scalar(select(func.md5(func.string_agg(ordered.c.record, "\n"))))

    def _verify_snapshot(self, connection):
        if dict(self._dataset(connection)) != self.dataset:
            raise DataBlocked("订单批次身份已经变化")
        if hashlib.sha256(self.source_file.read_bytes()).hexdigest() != ORDER_FILE_SHA256:
            raise DataBlocked("官方订单文件摘要已经变化")
        if connection.scalar(select(func.count()).select_from(self.orders).where(
                self.orders.c.source_revision == REVISION)) != EXPECTED_COUNTS["orders"]:
            raise DataBlocked("订单批次数量不完整")
        for name, table in self.related_tables.items():
            if connection.scalar(select(func.count()).select_from(table).where(
                    table.c.source_revision == REVISION)) != EXPECTED_COUNTS[name]:
                raise DataBlocked("客服关联数据批次不完整")
        if self._quarter_digest(connection) != self.quarter_digest:
            raise DataBlocked("季度订单字段已经变化")
        columns = self.orders.c
        counts = connection.execute(select(
            func.count().label("total"),
            func.count().filter(columns.sales_person_id.is_not(None)).label("assigned"),
            func.count().filter(columns.sales_person_id.is_(None), columns.online.is_(True)).label("unassigned"),
            func.count().filter(columns.status == 5).label("shipped"),
        ).where(*self._period_filter())).one()
        if tuple(counts) != (1022, 317, 705, 1022):
            raise DataBlocked("固定季度数量、归属或状态无效")

    @contextmanager
    def _connection(self):
        with self.engine.connect() as connection, connection.begin():
            self._verify_snapshot(connection)
            yield connection

    @staticmethod
    def _check_period(period_start, period_end):
        if (period_start, period_end) != (PERIOD_START, PERIOD_END):
            raise DataBlocked("期间不支持：当前订单来源仅支持2023年第三季度")

    def snapshot(self, session):
        with self._connection():
            return OrderSnapshot(
                source_id=SOURCE_ID, upstream_source_id=UPSTREAM_SOURCE_ID, revision=REVISION,
                batch_id=f"{REVISION}:{self.dataset['imported_at'].isoformat()}",
                file_sha256=ORDER_FILE_SHA256, period_start=PERIOD_START, period_end=PERIOD_END,
                row_count=1022, assigned_count=317, unassigned_count=705, complete=True,
            )

    def aggregate(self, session, *, employee_ids, period_start, period_end, include_unassigned):
        self._check_period(period_start, period_end)
        columns = self.orders.c
        scope = columns.sales_person_id.in_(employee_ids)
        if include_unassigned:
            scope = or_(scope, columns.sales_person_id.is_(None))
        with self._connection() as connection:
            rows = connection.execute(select(
                columns.sales_person_id, func.sum(columns.subtotal), func.count(func.distinct(columns.order_id)),
            ).where(*self._period_filter(), columns.status == 5, scope).group_by(
                columns.sales_person_id)).all()
            return [SalesOrderAggregate(employee_id=row[0], sales_amount=row[1], order_count=row[2]) for row in rows]

    def evidence(self, session, *, employee_id, period_start, period_end):
        self._check_period(period_start, period_end)
        with self._connection() as connection:
            rows = connection.execute(select(self.orders).where(
                *self._period_filter(), self.orders.c.status == 5,
                self.orders.c.sales_person_id == employee_id,
            ).order_by(self.orders.c.order_id)).mappings().all()
            return [OrderEvidence(
                order_id=row["order_id"], employee_id=row["sales_person_id"], order_date=row["order_date"],
                status=row["status"], subtotal=row["subtotal"], source_line=self.source_rows[row["order_id"]][0],
            ) for row in rows]


def create_order_port(settings: Settings):
    return AdventureWorksOrderPort(settings)
