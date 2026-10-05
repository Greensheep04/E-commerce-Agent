import json
import os
from pathlib import Path

import httpx
import pytest

INPUTS = Path(".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json")


@pytest.mark.parametrize(("allowed", "action", "expected_status", "message"), [
    ({"shop_customer_agent": ["submit"]}, "submit", 403, "turn"),
    ({"shop_customer_agent": ["orders"]}, "turn", 403, "未授权"),
    ({"shop_customer_agent": ["unregistered_operation"]}, "unregistered_operation", 409, "未登记"),
])
def test_real_goal_api_rejects_invalid_operation_authority(allowed, action, expected_status, message):
    inputs = json.loads(INPUTS.read_text())
    customer = inputs["accounts"]["customer"]
    with httpx.Client(base_url=os.environ.get("SHOP_TEST_API_URL", "http://127.0.0.1:8001"),
                      trust_env=False, timeout=30) as client:
        login = client.post("/api/login", json={
            "username": customer["username"],
            "password": Path(customer["password_file"]).read_text().strip(),
        })
        login.raise_for_status()
        assert login.json()["id"] == customer["actor_id"]
        before = client.get("/api/cluster/goals")
        before.raise_for_status()
        response = client.post("/api/cluster/goals", json={
            "objective": "提交本人客服咨询并保存真实答复",
            "allowed_operations": allowed,
            "criteria": [{"tool_name": "shop_customer_agent", "action": action,
                          "path": ["data", "result"], "operator": "exists"}],
        })
        assert response.status_code == expected_status
        assert message in response.json()["detail"]
        after = client.get("/api/cluster/goals")
        after.raise_for_status()
        assert after.json() == before.json()
        client.post("/api/logout").raise_for_status()
