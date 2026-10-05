import csv
import json
import os
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import func, select, text

from shop.adventureworks import REVISION, SOURCE_ID, import_orders
from shop.auth import hash_password
from shop.customer_models import (
    CustomerBinding,
    CustomerOrder,
    CustomerProfile,
    OrderDataset,
    OrderLine,
    OrderProduct,
)
from shop.models import User
from shop.orders import OrderQueryService, bind_customer

PROJECT_DIR = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_DIR / ".local/data/orders/beaab06ef72831089ca80e5355d65e661fd19b26"
SOURCE_FILES = ("SalesOrderHeader", "SalesOrderDetail", "Product", "Customer", "Person", "Store")
EXPECTED_ROWS = {
    "SalesOrderHeader": 31465,
    "SalesOrderDetail": 121317,
    "Product": 504,
    "Customer": 19820,
    "Person": 19972,
    "Store": 701,
}
SOURCE_COLUMNS = {
    "SalesOrderHeader": (
        "SalesOrderID", "RevisionNumber", "OrderDate", "DueDate", "ShipDate", "Status",
        "OnlineOrderFlag", "SalesOrderNumber", "PurchaseOrderNumber", "AccountNumber",
        "CustomerID", "SalesPersonID", "TerritoryID", "BillToAddressID", "ShipToAddressID",
        "ShipMethodID", "CreditCardID", "CreditCardApprovalCode", "CurrencyRateID", "SubTotal",
        "TaxAmt", "Freight", "TotalDue", "Comment", "rowguid", "ModifiedDate",
    ),
    "SalesOrderDetail": (
        "SalesOrderID", "SalesOrderDetailID", "CarrierTrackingNumber", "OrderQty", "ProductID",
        "SpecialOfferID", "UnitPrice", "UnitPriceDiscount", "LineTotal", "rowguid", "ModifiedDate",
    ),
    "Product": (
        "ProductID", "Name", "ProductNumber", "MakeFlag", "FinishedGoodsFlag", "Color",
        "SafetyStockLevel", "ReorderPoint", "StandardCost", "ListPrice", "Size", "SizeUnitMeasureCode",
        "WeightUnitMeasureCode", "Weight", "DaysToManufacture", "ProductLine", "Class", "Style",
        "ProductSubcategoryID", "ProductModelID", "SellStartDate", "SellEndDate", "DiscontinuedDate",
        "rowguid", "ModifiedDate",
    ),
    "Customer": (
        "CustomerID", "PersonID", "StoreID", "TerritoryID", "AccountNumber", "rowguid", "ModifiedDate",
    ),
    "Person": (
        "BusinessEntityID", "PersonType", "NameStyle", "Title", "FirstName", "MiddleName", "LastName",
        "Suffix", "EmailPromotion", "AdditionalContactInfo", "Demographics", "rowguid", "ModifiedDate",
    ),
    "Store": (
        "BusinessEntityID", "Name", "SalesPersonID", "Demographics", "rowguid", "ModifiedDate",
    ),
}


def read_reference_data(directory: Path) -> dict[str, list[dict[str, str]]]:
    data = {}
    for name in SOURCE_FILES:
        path = directory / f"{name}.csv"
        if name in {"Person", "Store"}:
            frame = pd.read_csv(
                path,
                sep=r"\+\|",
                engine="python",
                header=None,
                dtype=str,
                keep_default_na=False,
                quoting=csv.QUOTE_NONE,
                on_bad_lines="error",
            )
            rows = frame.values.tolist()
        else:
            with path.open(encoding="utf-8-sig", newline="") as source_file:
                rows = list(csv.reader(source_file, delimiter="\t", quoting=csv.QUOTE_NONE))
        data[name] = [
            dict(zip(SOURCE_COLUMNS[name], row, strict=True))
            for row in rows
        ]
    return data


def decimal_value(value: str) -> Decimal:
    return Decimal(value)


def expected_order_rows(data: dict[str, list[dict[str, str]]], customer_id: int):
    products = {int(row["ProductID"]): row["Name"] for row in data["Product"]}
    lines_by_order = defaultdict(list)
    for row in data["SalesOrderDetail"]:
        lines_by_order[int(row["SalesOrderID"])].append(row)
    headers = [
        row for row in data["SalesOrderHeader"]
        if int(row["CustomerID"]) == customer_id
    ]
    headers.sort(
        key=lambda row: (row["OrderDate"], int(row["SalesOrderID"])),
        reverse=True,
    )
    return headers, lines_by_order, products


def expected_customer_name(data: dict[str, list[dict[str, str]]], customer_id: int) -> str:
    customer = next(row for row in data["Customer"] if int(row["CustomerID"]) == customer_id)
    if customer["PersonID"]:
        person = next(
            row for row in data["Person"]
            if int(row["BusinessEntityID"]) == int(customer["PersonID"])
        )
        return " ".join(value for value in (
            person["FirstName"], person["MiddleName"], person["LastName"]
        ) if value)
    store = next(
        row for row in data["Store"]
        if int(row["BusinessEntityID"]) == int(customer["StoreID"])
    )
    return store["Name"]


def assert_order_payload(payload, source_row, line_rows, products, detailed):
    order_id = int(source_row["SalesOrderID"])
    assert payload["order_id"] == order_id
    assert payload["order_number"] == source_row["SalesOrderNumber"]
    assert date.fromisoformat(payload["order_date"][:10]) == date.fromisoformat(
        source_row["OrderDate"][:10]
    )
    status = int(source_row["Status"])
    assert payload["status"] == status
    assert payload["status_label"] == f"来源状态代码{status}"
    assert Decimal(payload["subtotal"]) == decimal_value(source_row["SubTotal"])
    assert Decimal(payload["tax"]) == decimal_value(source_row["TaxAmt"])
    assert Decimal(payload["freight"]) == decimal_value(source_row["Freight"])
    assert Decimal(payload["total_due"]) == decimal_value(source_row["TotalDue"])
    assert payload["is_sample"] is True
    assert payload["source"] == {
        "source_id": SOURCE_ID,
        "revision": REVISION,
        "order_id": order_id,
    }
    ordered_lines = sorted(line_rows, key=lambda row: int(row["SalesOrderDetailID"]))
    visible_lines = ordered_lines if detailed else ordered_lines[:3]
    assert payload["item_count"] == len(ordered_lines)
    assert payload["remaining_item_count"] == len(ordered_lines) - len(visible_lines)
    assert len(payload["items"]) == len(visible_lines)
    for actual, expected in zip(payload["items"], visible_lines, strict=True):
        assert actual["line_id"] == int(expected["SalesOrderDetailID"])
        assert actual["product_id"] == int(expected["ProductID"])
        assert actual["name"] == products[int(expected["ProductID"])]
        assert actual["quantity"] == int(expected["OrderQty"])
        assert Decimal(actual["unit_price"]) == decimal_value(expected["UnitPrice"])
        assert Decimal(actual["discount"]) == decimal_value(expected["UnitPriceDiscount"])
        assert Decimal(actual["line_total"]) == decimal_value(expected["LineTotal"])


def test_real_adventureworks_import_and_customer_order_queries(db_session):
    assert os.environ["SHOP_TEST_DATABASE_URL"].rsplit("/", 1)[-1] == "shop_test"
    data = read_reference_data(SOURCE_DIR)
    for name, expected_count in EXPECTED_ROWS.items():
        assert len(data[name]) == expected_count

    first = import_orders(db_session, SOURCE_DIR)
    assert first["reused"] is False
    assert first["counts"] == {
        "customers": EXPECTED_ROWS["Customer"],
        "products": EXPECTED_ROWS["Product"],
        "orders": EXPECTED_ROWS["SalesOrderHeader"],
        "lines": EXPECTED_ROWS["SalesOrderDetail"],
    }
    assert db_session.scalar(select(func.count()).select_from(OrderDataset)) == 1
    assert db_session.scalar(select(func.count()).select_from(CustomerProfile)) == 19820
    assert db_session.scalar(select(func.count()).select_from(OrderProduct)) == 504
    assert db_session.scalar(select(func.count()).select_from(CustomerOrder)) == 31465
    assert db_session.scalar(select(func.count()).select_from(OrderLine)) == 121317

    people = {int(row["BusinessEntityID"]): row for row in data["Person"]}
    customers = {int(row["CustomerID"]): row for row in data["Customer"]}
    assert 11091 in customers and 11176 in customers
    for customer_id in (11091, 11176):
        row = customers[customer_id]
        if row["PersonID"]:
            assert int(row["PersonID"]) in people
        else:
            assert any(
                int(store["BusinessEntityID"]) == int(row["StoreID"])
                for store in data["Store"]
            )

    first_user = User(
        username=expected_customer_name(data, 11176),
        password_hash=hash_password("local-test-password"),
        role="customer",
    )
    second_user = User(
        username="customer-11176",
        password_hash=hash_password("local-test-password"),
        role="customer",
    )
    unbound_user = User(
        username="unbound",
        password_hash=hash_password("local-test-password"),
        role="customer",
    )
    db_session.add_all([first_user, second_user, unbound_user])
    db_session.flush()
    bind_customer(db_session, first_user.id, 11091)
    bind_customer(db_session, second_user.id, 11176)
    assert db_session.scalar(select(func.count()).select_from(CustomerBinding)) == 2

    first_orders, lines_by_order, products = expected_order_rows(data, 11091)
    second_orders, _, _ = expected_order_rows(data, 11176)
    assert first_orders and second_orders
    first_service = OrderQueryService(db_session, first_user.id)
    second_service = OrderQueryService(db_session, second_user.id)

    identity = first_service.identity()
    assert identity["source_id"] == SOURCE_ID
    assert identity["revision"] == REVISION
    assert identity["customer_id"] == 11091
    assert identity["name"] == expected_customer_name(data, 11091)
    assert identity["person_name"] == expected_customer_name(data, 11091)
    assert identity["store_name"] is None
    assert identity["name"] != first_user.username
    assert second_service.identity()["customer_id"] == 11176
    assert second_service.identity()["name"] == expected_customer_name(data, 11176)
    assert identity["is_sample"] is True

    with pytest.raises(PermissionError):
        OrderQueryService(db_session, unbound_user.id)

    summary = first_service.list_orders()
    assert summary["total"] == len(first_orders)
    assert summary["limit"] == 20
    assert summary["offset"] == 0
    assert summary["has_more"] is (len(first_orders) > 20)
    assert [row["order_id"] for row in summary["orders"]] == [
        int(row["SalesOrderID"]) for row in first_orders[:20]
    ]
    for payload, expected in zip(summary["orders"], first_orders[:20], strict=True):
        order_id = int(expected["SalesOrderID"])
        assert_order_payload(payload, expected, lines_by_order[order_id], products, detailed=False)

    page = first_service.list_orders(limit=5, offset=5)
    assert page["total"] == len(first_orders)
    assert page["limit"] == 5
    assert page["offset"] == 5
    assert page["has_more"] is (len(first_orders) > 10)
    assert [row["order_id"] for row in page["orders"]] == [
        int(row["SalesOrderID"]) for row in first_orders[5:10]
    ]

    source_order = first_orders[0]
    order_id = int(source_order["SalesOrderID"])
    order_number = source_order["SalesOrderNumber"]
    detail = first_service.get_order(order_number)
    assert_order_payload(detail, source_order, lines_by_order[order_id], products, detailed=True)
    assert_order_payload(
        first_service.get_order(str(order_id)),
        source_order,
        lines_by_order[order_id],
        products,
        detailed=True,
    )
    assert first_service.get_order("SO99999") is None
    with pytest.raises(ValueError):
        first_service.get_order("SO-not-a-complete-order-number")
    other_order = second_orders[0]
    assert first_service.get_order(other_order["SalesOrderNumber"]) is None
    assert first_service.get_order(str(other_order["SalesOrderID"])) is None

    with pytest.raises(ValueError):
        first_service.list_orders(limit=0)
    with pytest.raises(ValueError):
        first_service.list_orders(offset=-1)

    target_day = date.fromisoformat(source_order["OrderDate"][:10])
    by_day = first_service.list_orders(start_date=target_day, end_date=target_day)
    day_orders = [
        row for row in first_orders
        if date.fromisoformat(row["OrderDate"][:10]) == target_day
    ]
    assert by_day["total"] == len(day_orders)
    assert [row["order_id"] for row in by_day["orders"]] == [
        int(row["SalesOrderID"]) for row in day_orders[:20]
    ]

    sample_lines = lines_by_order[order_id]
    product_name = products[int(sample_lines[0]["ProductID"])]
    by_product = first_service.list_orders(product=product_name.upper())
    matching_orders = []
    for row in first_orders:
        matching_lines = lines_by_order[int(row["SalesOrderID"])]
        if any(product_name.casefold() in products[int(line["ProductID"])].casefold()
               for line in matching_lines):
            matching_orders.append(row)
    assert by_product["total"] == len(matching_orders)
    assert [row["order_id"] for row in by_product["orders"]] == [
        int(row["SalesOrderID"]) for row in matching_orders[:20]
    ]

    before_counts = {
        "customers": db_session.scalar(select(func.count()).select_from(CustomerProfile)),
        "products": db_session.scalar(select(func.count()).select_from(OrderProduct)),
        "orders": db_session.scalar(select(func.count()).select_from(CustomerOrder)),
        "lines": db_session.scalar(select(func.count()).select_from(OrderLine)),
    }
    second = import_orders(db_session, SOURCE_DIR)
    after_counts = {
        "customers": db_session.scalar(select(func.count()).select_from(CustomerProfile)),
        "products": db_session.scalar(select(func.count()).select_from(OrderProduct)),
        "orders": db_session.scalar(select(func.count()).select_from(CustomerOrder)),
        "lines": db_session.scalar(select(func.count()).select_from(OrderLine)),
    }
    assert second["reused"] is True
    assert second["counts"] == first["counts"]
    assert after_counts == before_counts

    database_identity = db_session.execute(text(
        "SELECT current_database() AS database, current_schema() AS schema, version() AS version"
    )).mappings().one()
    print("CUS01_DB_IDENTITY=" + json.dumps(dict(database_identity), ensure_ascii=False))
