import json
import os
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from scripts.install_data_center_watch import install
from shop.config import Settings


@pytest.mark.skipif(os.environ.get("DC_WATCH_INSTALL_LIVE") != "1",
                    reason="需要显式启用真实来源监听安装检查")
@pytest.mark.asyncio
async def test_installation_identity_reuses_one_goal_and_creates_a_new_cancelled_replacement():
    root = Path(__file__).resolve().parents[1]
    output = Path(os.environ["DC_WATCH_INSTALL_LIVE_OUTPUT"])
    output.mkdir(parents=True, exist_ok=False)
    installation_id = uuid4()

    def arguments(identifier, name):
        return SimpleNamespace(
            identities=root / ".scratch/evidence/dc02-host-20261004-r3/identities.json",
            actor="sync", operation="sync", source_id="microsoft/adventureworks", payload=None,
            budget_cny=Decimal(5), interval_seconds=900, nightly_cron="0 2 * * *",
            timezone="Asia/Shanghai", shop_url="http://127.0.0.1:8002",
            notes_dir=Settings().cluster_notes_dir, paused=True, trigger=False,
            installation_id=identifier, output=output / name,
        )

    await install(arguments(installation_id, "initial.json"))
    await install(arguments(installation_id, "repeated.json"))
    initial = json.loads((output / "initial.json").read_text())
    repeated = json.loads((output / "repeated.json").read_text())
    assert initial == repeated
    assert initial["registration"]["installation_id"] == str(installation_id)
    identities = json.loads(arguments(installation_id, "unused.json").identities.read_text())
    identity = identities["actors"]["sync"]
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8002", trust_env=False) as client:
        response = await client.post("/api/login", json={
            "username": identity["username"], "password": Path(identity["password_file"]).read_text().strip(),
        })
        response.raise_for_status()
        response = await client.post(f"/api/cluster/goals/{initial['goal_id']}/control/cancel")
        response.raise_for_status()
        await install(arguments(uuid4(), "replacement.json"))
        replacement = json.loads((output / "replacement.json").read_text())
        assert replacement["goal_id"] != initial["goal_id"]
        assert replacement["state"] == "paused"
        response = await client.get(f"/api/cluster/goals/{initial['goal_id']}")
        response.raise_for_status()
        assert response.json()["state"] == "cancelled"
    source = "scripts/install_data_center_watch.py"
    (output / "report.json").write_text(json.dumps({
        "schema": "dc06.watch_installation_identity.v1", "status": "passed",
        "initial_goal_id": initial["goal_id"],
        "replacement_goal_id": replacement["goal_id"],
        "checks": ["same_installation_reuses_goal_note_and_schedules", "new_installation_has_new_goal",
                   "cancelled_goal_retains_terminal_state"],
        "source_sha256": {source: sha256((root / source).read_bytes()).hexdigest()},
    }, ensure_ascii=False, indent=2) + "\n")
