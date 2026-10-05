import asyncio
import json
import os
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest
from data_center_protocol import DataQuery
from sqlalchemy import create_engine, text

from data_center.analytics.engine import AnalyticsEngine
from data_center.config import Settings
from data_center.database import authorized_connection
from data_center.ports import ShopPorts


@pytest.mark.skipif(os.environ.get("DC04_LIVE_ACCEPTANCE") != "1",
                    reason="需要显式启用真实 Shop HTTP 验收")
def test_shop_http_sync_metricflow_query_and_recovery():
    asyncio.run(_run_live_acceptance())


async def _run_live_acceptance():
    project_root = Path(__file__).resolve().parents[2]
    identity_path = Path(os.environ.get(
        "DC04_LIVE_IDENTITY_FILE",
        ".scratch/evidence/dc02-host-20261004-r1/identities.json",
    ))
    if not identity_path.is_absolute():
        identity_path = project_root / identity_path
    identities = json.loads(identity_path.read_text(encoding="utf-8"))["actors"]
    settings = Settings()
    database = create_engine(settings.database_url, pool_pre_ping=True)
    ports = ShopPorts(settings.shop_url)
    run_id = f"dc04-live-{uuid4()}"
    task_id = f"dc04-live-{uuid4()}"
    source_id = "microsoft/adventureworks"
    try:
        sync_identity = identities["sync"]
        sync_password_path = project_root / sync_identity["password_file"]
        sync_context = await ports.login(
            sync_identity["username"], sync_password_path.read_text(encoding="utf-8").strip(),
            f"{run_id}-sync",
        )
        analytics = AnalyticsEngine(database, settings.artifact_root, ports, ports)
        snapshot = await analytics.sync(source_id, sync_context, task_id=task_id, run_id=run_id)
        recovered = await analytics.sync(source_id, sync_context, task_id=task_id, run_id=run_id)
        assert snapshot.state == "published"
        assert recovered.snapshot_id == snapshot.snapshot_id

        query = DataQuery(
            question="核对2023年第三季度授权范围内的销售额与订单数",
            intent="metrics",
            source_ids=[source_id],
            snapshot_id=snapshot.snapshot_id,
            metric_ids=["sales_amount", "order_count", "quota_completion"],
            dimensions=["employee_id"],
            start_date=date(2023, 7, 1),
            end_date=date(2023, 10, 1),
            limit=100,
        )
        role_results = {}
        for identity_name in ("assessment", "assessment-manager", "assessment-admin"):
            identity = identities[identity_name]
            password_path = project_root / identity["password_file"]
            context = await ports.login(
                identity["username"], password_path.read_text(encoding="utf-8").strip(),
                f"{run_id}-{identity_name}",
            )
            scope = await ports.authorize(context, [source_id], "read")
            role_query = query
            if identity_name == "assessment":
                role_query = query.model_copy(update={"filters": {"employee_ids": [275]}})
            with authorized_connection(database, scope, snapshot.snapshot_id) as connection:
                reader_user = connection.scalar(text("SELECT current_user"))
            assert reader_user == "data_center_reader"
            if identity_name == "assessment-admin":
                with authorized_connection(database, scope, snapshot.snapshot_id) as connection:
                    assert connection.scalar(text("SELECT current_user")) == "data_center_reader"
                    admin_orders = connection.execute(text(
                        "SELECT employee_id, count(*) AS order_count "
                        "FROM public.sales_orders WHERE snapshot_id=:snapshot_id "
                        "GROUP BY employee_id"
                    ), {"snapshot_id": snapshot.snapshot_id}).mappings().all()
                assert sum(row["order_count"] for row in admin_orders) == 1022
                assert next(row["order_count"] for row in admin_orders
                            if row["employee_id"] is None) == 705
            result = await analytics.query(role_query, context)
            assert result.status == "completed"
            assert result.snapshots[0].snapshot_id == snapshot.snapshot_id
            assert result.metadata["currency_status"] == "未核实"
            returned_employee_ids = {
                row["employee_id"] for row in result.rows if row["employee_id"] is not None
            }
            assert {str(value) for value in returned_employee_ids} <= set(scope.employee_ids)
            role_results[identity_name] = {
                "policy_fingerprint": scope.fingerprint,
                "employee_ids": sorted(returned_employee_ids),
                "reader_current_user": reader_user,
                "rows": result.rows,
                "metadata": result.metadata,
            }
            if identity_name == "assessment":
                assert scope.employee_ids == ["275"]
                with pytest.raises(PermissionError):
                    await analytics.query(
                        query.model_copy(update={"filters": {"employee_ids": [276]}}), context,
                    )
                role_results[identity_name]["filter_check"] = {
                    "authorized_employee_ids": [275], "out_of_scope_employee_ids": [276],
                    "out_of_scope_status": "rejected",
                }
                detail = await analytics.read_evidence(result.evidence[0], context)
                assert detail["rows"]
                assert detail["aggregate_sha256"] == result.evidence[0].content_sha256
                assert all(row["employee_id"] == 275 for row in detail["rows"])
                assert all(row["source_line"] > 0 and len(row["source_sha256"]) == 64
                           for row in detail["rows"])
                role_results[identity_name]["evidence_detail"] = detail
                empty_identity = identities["assessment-empty"]
                empty_password = project_root / empty_identity["password_file"]
                empty_context = await ports.login(
                    empty_identity["username"], empty_password.read_text(encoding="utf-8").strip(),
                    f"{run_id}-assessment-empty",
                )
                with pytest.raises(PermissionError):
                    await analytics.read_evidence(result.evidence[0], empty_context)
                role_results[identity_name]["reduced_scope_evidence_check"] = {
                    "actor": "assessment-empty", "status": "rejected",
                }
            if identity_name == "assessment-admin":
                assert scope.organization
                assert any(row["employee_id"] is None for row in result.rows)
                detail = await analytics.read_evidence(result.evidence[0], context)
                order_rows = [row for row in detail["rows"] if row["record_type"] == "order"]
                quota_rows = [row for row in detail["rows"] if row["record_type"] == "quota"]
                assert len(order_rows) == 1022
                assert sum(row["employee_id"] is None for row in order_rows) == 705
                assert len(quota_rows) == 14
                assert all(row["quota_date"] and row["source_line"] > 0
                           and len(row["source_sha256"]) == 64
                           and row["source_revision"] == snapshot.source_revisions[source_id]
                           for row in quota_rows)
                assert [{name: value for name, value in original.items() if name != "identity_records"}
                        for original in detail["original_quotas"]] == [
                    {"record_id": f"quota:{quota_row['employee_id']}", **{
                        name: quota_row[name] for name in (
                        "record_id", "employee_id", "quota_date", "quota_amount", "source_line",
                        "source_sha256", "source_revision",
                        ) if name != "record_id"
                    }}
                    for quota_row in quota_rows
                ]
                employee_275_quota = next(row for row in quota_rows if row["employee_id"] == 275)
                assert employee_275_quota["quota_date"] == "2023-08-30"
                assert employee_275_quota["quota_amount"] == "1324000.0000"
                assert employee_275_quota["source_line"] == 18
                for original in detail["original_quotas"]:
                    identities = original["identity_records"]
                    assert {item["file_name"] for item in identities} == {"Employee.csv", "SalesPerson.csv"}
                    assert len(identities) == 2
                    assert all(item["BusinessEntityID"] == original["employee_id"]
                               and item["source_id"] == source_id
                               and item["revision"] == snapshot.source_revisions[source_id]
                               and item["source_line"] > 0 and len(item["source_sha256"]) == 64
                               for item in identities)
                original_275 = next(row for row in detail["original_quotas"] if row["employee_id"] == 275)
                assert {item["record_id"]: item["source_line"] for item in original_275["identity_records"]} == {
                    "employee:275": 275, "salesperson:275": 2,
                }
                role_results[identity_name]["evidence_detail"] = detail

        sync_evidence_path = analytics._run_directory(task_id, run_id) / "sync-evidence.json"
        query_evidence_paths = sorted(analytics.artifact_root.glob(f"dc04-query-{run_id}-*/query-evidence.json"))
        report_dir = project_root / ".scratch/evidence" / f"dc04-live-{run_id}"
        report_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "milestone": "DC-04",
            "task_id": task_id,
            "run_id": run_id,
            "source_snapshot_id": snapshot.quality["source_snapshot_id"],
            "published_snapshot": snapshot.model_dump(mode="json"),
            "recovery_snapshot_id": recovered.snapshot_id,
            "sync": json.loads(sync_evidence_path.read_text(encoding="utf-8")),
            "roles": role_results,
            "full_dlt_dbt_sync": json.loads(Path(os.environ["DC04_FULL_SYNC_EVIDENCE"]).read_text(
                encoding="utf-8",
            )) if os.environ.get("DC04_FULL_SYNC_EVIDENCE") else None,
            "metricflow_query_evidence": [
                json.loads(path.read_text(encoding="utf-8")) for path in query_evidence_paths
            ],
        }
        (report_dir / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8",
        )
    finally:
        await ports.close()
        database.dispose()
