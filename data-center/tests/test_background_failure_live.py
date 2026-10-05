import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from data_center import api
from data_center.config import Settings


@pytest.mark.skipif(os.environ.get("DC_BACKGROUND_FAILURE_LIVE") != "1",
                    reason="需要启用真实失败任务及宿主HTTP回读检查")
@pytest.mark.asyncio
async def test_saved_model_schema_failure_remains_readable_after_background_execution():
    root = Path(__file__).resolve().parents[2]
    record = json.loads((root / (
        ".scratch/evidence/dc-formal-20261004-r3/attempts/MET-04-3.json"
    )).read_text())
    identities = json.loads((root / (
        ".scratch/evidence/dc02-host-20261004-r3/identities.json"
    )).read_text())
    identity = identities["actors"][record["input"]["actor"]]
    app = api.create_app(Settings())
    output = Path(os.environ["DC_BACKGROUND_FAILURE_OUTPUT"])
    output.mkdir(parents=True, exist_ok=False)
    try:
        context = await app.state.ports.login(
            identity["username"], Path(identity["password_file"]).read_text().strip(), str(uuid4()))
        task_id = record["task_id"]
        before = await app.state.ports.usage(task_id, context)
        await api.run_background(app.state.runtime, task_id, context)
        task = await app.state.runtime.show(task_id, context)
        after = await app.state.ports.usage(task_id, context)
        assert task.status == "failed"
        assert "answer" in task.error and "string" in task.error
        assert task.result is None
        assert before == after
        headers = {"Authorization": "Bearer " + context.credential.get_secret_value(),
                   "X-Shop-Actor": context.actor_id, "X-Request-ID": task.request_id}
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8100", trust_env=False,
                                     timeout=30, limits=httpx.Limits(max_connections=1)) as client:
            response = await client.post("/operations/ask", headers=headers,
                                         json={"payload": task.payload, "upstream": task.upstream})
            response.raise_for_status()
            submitted = response.json()
            assert submitted["data"]["task_id"] == task_id
            for _ in range(50):
                response = await client.post("/operations/show", headers={**headers, "X-Request-ID": str(uuid4())},
                                             json={"payload": {"task_id": task_id}, "upstream": []})
                response.raise_for_status()
                shown = response.json()
                if shown["status"] == "failed":
                    break
                await asyncio.sleep(0.1)
            assert shown["status"] == "failed"
            assert "answer" in shown["data"]["error"]
            health = await client.get("/health")
            assert health.json() == {"status": "ok"}
        assert await app.state.ports.usage(task_id, context) == before
        (output / "report.json").write_text(json.dumps({
            "task_id": task_id, "status": task.status, "error": task.error,
            "usage_before": before, "usage_after": after, "new_model_calls": 0,
            "http_submission": submitted, "http_readback": shown,
            "single_connection_health": health.json(),
        }, ensure_ascii=False, indent=2) + "\n")
    finally:
        await app.state.ports.close()
        app.state.runtime.observer.close()
        app.state.store.database.dispose()
