import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from shop.assessment_orders import configured_order_port
from shop.assessment_types import PERIOD_END, PERIOD_START, REVISION, SOURCE_ID
from shop.config import Settings

ORDER_FILE = Path("/Users/greensheep/Project/project-shop/.local/data/orders") / REVISION / "SalesOrderHeader.csv"
ORDER_DATABASE = "postgresql+psycopg://shop@127.0.0.1:55432/shop"


def test_official_order_adapter_reads_verified_snapshot():
    settings = Settings(
        assessment_order_adapter="shop.assessment_order_adapter:create_order_port",
        assessment_order_database_url=ORDER_DATABASE,
        assessment_order_source_file=ORDER_FILE,
    )
    port = configured_order_port(settings)
    with ORDER_FILE.open(encoding="utf-8-sig", newline="") as stream:
        rows = [(line, row) for line, row in enumerate(csv.reader(
            stream, delimiter="\t", quoting=csv.QUOTE_NONE), start=1)
            if PERIOD_START <= datetime.fromisoformat(row[2]) < PERIOD_END and row[5] == "5"]
    engine = create_engine(settings.database_url)
    with Session(engine) as session:
        snapshot = port.snapshot(session)
        assert snapshot.complete and snapshot.source_id == SOURCE_ID
        assert snapshot.upstream_source_id == "microsoft/adventureworks-oltp"
        assert snapshot.revision == REVISION
        assert (snapshot.row_count, snapshot.assigned_count, snapshot.unassigned_count) == (1022, 317, 705)
        selected = [row for _, row in rows if row[11] == "275"]
        aggregate = port.aggregate(session, employee_ids=(275,), period_start=PERIOD_START,
                                   period_end=PERIOD_END, include_unassigned=False)
        assert len(aggregate) == 1 and aggregate[0].employee_id == 275
        assert aggregate[0].order_count == len(selected)
        assert aggregate[0].sales_amount == sum((Decimal(row[19]) for row in selected), Decimal(0))
        evidence = port.evidence(session, employee_id=275, period_start=PERIOD_START, period_end=PERIOD_END)
        assert {item.order_id: item.source_line for item in evidence} == {
            int(row[0]): line for line, row in rows if row[11] == "275"}
    engine.dispose()


def test_order_adapter_connection_is_read_only():
    settings = Settings(
        assessment_order_adapter="shop.assessment_order_adapter:create_order_port",
        assessment_order_database_url=ORDER_DATABASE,
        assessment_order_source_file=ORDER_FILE,
    )
    port = configured_order_port(settings)
    with port.engine.connect() as connection, connection.begin():
        assert connection.scalar(text("SHOW transaction_read_only")) == "on"
