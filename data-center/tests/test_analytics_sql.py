from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from data_center.analytics.engine import AnalyticsEngine, validate_metric_sql


def test_metric_sql_accepts_read_only_query_with_approved_relations():
    sql = "SELECT employee_id, SUM(subtotal) AS sales_amount FROM analytics.sales_orders GROUP BY employee_id"

    assert validate_metric_sql(sql) == sql


def test_metric_sql_rejects_write_statement():
    sql = "DELETE FROM analytics.sales_orders"

    try:
        validate_metric_sql(sql)
    except PermissionError as error:
        assert "read-only" in str(error)
    else:
        raise AssertionError("write statement was accepted")


def test_metric_sql_rejects_unapproved_relation():
    sql = "SELECT password_hash FROM public.users"

    try:
        validate_metric_sql(sql)
    except ValueError as error:
        assert "relation" in str(error)
    else:
        raise AssertionError("unapproved relation was accepted")


def test_metric_sql_rejects_unapproved_field():
    sql = "SELECT password_hash FROM analytics.sales_orders"

    try:
        validate_metric_sql(sql)
    except ValueError as error:
        assert "field" in str(error)
    else:
        raise AssertionError("unapproved field was accepted")


def test_metricflow_compiles_registered_metrics_with_employee_dimension():
    sql = AnalyticsEngine._compile_metricflow_query(
        {"sales_amount", "quota_completion"}, ["employee"],
        "analytics", "2023-07-01", "2023-10-01", 100,
    )

    assert "sales_orders" in sql
    assert "sales_quotas" in sql
    assert "employee" in sql
    assert validate_metric_sql(sql, schema="analytics", relations={"sales_orders", "sales_quotas"}) == sql


def test_metric_sql_validator_accepts_all_registered_metricflow_expressions():
    sql = AnalyticsEngine._compile_metricflow_query(
        {"sales_amount", "order_count", "average_order_value", "quota_completion"},
        ["employee"], "public", "2023-07-01", "2023-10-01", 100,
    )

    assert validate_metric_sql(sql, schema="public", relations={"sales_orders", "sales_quotas"}) == sql


def test_sync_page_digest_normalizes_typed_source_values():
    rows = [{"order_date": datetime(2023, 7, 1, tzinfo=UTC), "subtotal": Decimal("12.50")}]

    assert AnalyticsEngine._page_digest(rows) == AnalyticsEngine._page_digest(
        AnalyticsEngine._serialize_rows(rows),
    )


def test_dlt_database_url_contains_explicit_empty_password_for_passwordless_auth():
    database = create_engine("postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
    engine = AnalyticsEngine(database, Path("."), None, None)

    assert "data_center_writer:@127.0.0.1" in engine._destination_url()

    database.dispose()


def test_sync_run_directory_is_absolute_for_dbt_subprocess_paths():
    database = create_engine("postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
    engine = AnalyticsEngine(database, Path(".local/data-center"), None, None)

    assert engine._run_directory("task", "run").is_absolute()

    database.dispose()


def test_employee_filter_accepts_unique_positive_integer_ids():
    assert AnalyticsEngine._requested_employee_ids({"employee_ids": [275, 276]}) == [275, 276]


@pytest.mark.parametrize("filters", [
    {"employee_ids": []},
    {"employee_ids": [275, 275]},
    {"employee_ids": [True]},
    {"employee_ids": [0]},
    {"employee_ids": ["275"]},
    {"order_status": [5]},
])
def test_employee_filter_rejects_unregistered_or_invalid_values(filters):
    with pytest.raises(ValueError):
        AnalyticsEngine._requested_employee_ids(filters)
