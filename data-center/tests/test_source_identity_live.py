import asyncio
import json
import os
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest
from data_center_protocol import DataQuery
from sqlalchemy import create_engine

from data_center.analytics.engine import AnalyticsEngine
from data_center.config import Settings
from data_center.ports import ShopPorts


@pytest.mark.skipif(os.environ.get("DC_IDENTITY_LIVE") != "1",
                    reason="需要显式启用真实指标来源身份检查")
def test_metric_evidence_preserves_authorized_original_identity_records():
    asyncio.run(check_identity_records())


async def check_identity_records():
    root = Path(__file__).resolve().parents[2]
    identities = json.loads((root / ".scratch/evidence/dc02-host-20261004-r3/identities.json").read_text())
    identity = identities["actors"]["assessment"]
    settings = Settings()
    ports = ShopPorts(settings.shop_url)
    engine = create_engine(settings.database_url)
    try:
        context = await ports.login(identity["username"],
                                    (root / identity["password_file"]).read_text().strip(), str(uuid4()))
        analytics = AnalyticsEngine(engine, settings.artifact_root, ports, ports)
        query = DataQuery(question="核查员工275第三季度目标记录及来源身份", intent="metrics",
                          source_ids=["microsoft/adventureworks"], metric_ids=["quota_completion"],
                          filters={"employee_ids": [275]}, start_date=date(2023, 7, 1),
                          end_date=date(2023, 10, 1))
        result = await analytics.query(query, context)
        detail = await analytics.read_evidence(result.evidence[0], context)
        records = detail["original_quotas"][0].get("identity_records")
        assert records is not None
        assert [(record["file_name"], record["source_line"], record["BusinessEntityID"])
                for record in records] == [("Employee.csv", 275, 275), ("SalesPerson.csv", 2, 275)]
        assert all(record["source_id"] == "microsoft/adventureworks"
                   and record["revision"] == detail["source_revision"] for record in records)
        output = root / os.environ["DC_IDENTITY_LIVE_OUTPUT"]
        output.parent.mkdir(parents=True, exist_ok=True)
        assert not output.exists()
        output.write_text(json.dumps({"status": "passed", "metric_reference": result.evidence[0].model_dump(mode="json"),
                                      "original_quotas": detail["original_quotas"]}, ensure_ascii=False, indent=2) + "\n")
    finally:
        await ports.close()
        engine.dispose()
