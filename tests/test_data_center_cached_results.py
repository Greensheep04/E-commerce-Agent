import json
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import httpx
import pytest
from agent_cluster.mcp_client import MCPCommand, connect
from agent_cluster.types import TaskSpec
from integration_protocol import OperationResult
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.data_center_adapter import DataCenterAdapter
from shop.runtime_models import ProviderRequest


def saved_refusal():
    path = Path(".scratch/evidence/dc-formal-20261004-r2/attempts/REF-01-1.json")
    record = json.loads(path.read_text())
    return record, OperationResult.model_validate(record["result"])


def test_cached_refusal_retains_the_original_task_identifier():
    record, result = saved_refusal()
    assert result.status == "DATA_BLOCKED"
    assert result.data["result_id"] == record["task_id"]
    assert DataCenterAdapter.task_id_from_result(result) == record["task_id"]


def test_conflicting_task_identifiers_are_rejected():
    _, result = saved_refusal()
    conflicting = result.model_copy(update={"data": {**result.data, "task_id": "conflicting-task"}})
    with pytest.raises(ValueError, match="任务编号冲突"):
        DataCenterAdapter.task_id_from_result(conflicting)


@pytest.mark.skipif(os.environ.get("DC_CACHED_RESULT_LIVE") != "1",
                    reason="需要显式启用真实HTTP、MCP及费用检查")
@pytest.mark.asyncio
async def test_cached_refusal_reused_over_http_and_mcp_without_new_requests():
    record, expected = saved_refusal()
    root = Path(__file__).resolve().parents[1]
    output = Path(os.environ["DC_CACHED_RESULT_OUTPUT"])
    output.mkdir(parents=True, exist_ok=False)
    identities = json.loads((root / ".scratch/evidence/dc02-host-20261004-r3/identities.json").read_text())
    identity = identities["actors"][record["input"]["actor"]]
    database = create_engine(Settings().database_url)

    def requests():
        with Session(database) as session:
            values = session.scalars(select(ProviderRequest).where(
                ProviderRequest.run_id == expected.run_id
            ).order_by(ProviderRequest.id)).all()
            return [{"request_id": value.id, "status": value.status,
                     "charged_cny": str(value.charged_cny),
                     "reservation_cny": str(value.reservation_cny),
                     "usage": value.usage} for value in values]

    try:
        before = requests()
        assert before
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8002", timeout=60, trust_env=False) as client:
            response = await client.post("/api/login", json={
                "username": identity["username"],
                "password": Path(identity["password_file"]).read_text().strip(),
            })
            response.raise_for_status()
            response = await client.post("/api/modules/data_center/operations/ask", json={
                "request_id": record["input"]["request_id"], "version": "1",
                "payload": record["input"]["payload"], "upstream": [],
            })
            response.raise_for_status()
            http_result = OperationResult.model_validate(response.json())
        assert http_result == expected
        assert DataCenterAdapter.task_id_from_result(http_result) == record["task_id"]
        runtime = json.loads((root / ".local/data-center/runtime/cluster.json").read_text())
        command = MCPCommand.model_validate(runtime["actors"][identity["actor_id"]])
        async with connect(command) as client:
            spec = TaskSpec(task_id="cached-refusal-check", tool_name="shop_data_center_agent", action="ask")
            arguments = client.catalog.arguments(spec, record["input"]["payload"],
                                                  record["input"]["request_id"], [])
            mcp_result = await client.invoke(spec, arguments)
        assert mcp_result == expected
        assert DataCenterAdapter.task_id_from_result(mcp_result) == record["task_id"]
        after = requests()
        assert after == before
        sources = ["src/shop/data_center_adapter.py", "tests/test_data_center_cached_results.py"]
        report = {"status": "passed", "captured_at": datetime.now(UTC).isoformat(),
                  "task_id": record["task_id"], "run_id": expected.run_id,
                  "request_id": record["input"]["request_id"],
                  "checks": ["http_cached_result_identity", "mcp_cached_result_identity",
                             "original_refusal_unchanged", "scoped_requests_and_cost_unchanged"],
                  "source_sha256": {name: sha256((root / name).read_bytes()).hexdigest() for name in sources},
                  "model_requests_before": before, "model_requests_after": after,
                  "http_result": http_result.model_dump(mode="json"),
                  "mcp_result": mcp_result.model_dump(mode="json")}
        (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    finally:
        database.dispose()
