import argparse
import asyncio
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
from agent_cluster.mcp_client import MCPCommand, connect
from agent_cluster.types import TaskSpec

ROOT = Path(__file__).resolve().parents[1]


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")


async def invoke(client, action, payload, request_id=None):
    response = await client.post("/api/modules/data_center/operations/" + action,
                                 json={"request_id": request_id or str(uuid4()), "version": "1",
                                       "payload": payload, "upstream": []})
    response.raise_for_status()
    return response.json()


async def verify(args):
    args.output.mkdir(parents=True, exist_ok=True)
    identities = json.loads(args.identities.read_text())["actors"]
    identity = identities["sync"]
    state_path = args.output / "state.json"
    if state_path.exists():
        state = json.loads(state_path.read_text())
    else:
        state = {"run_id": str(uuid4()), "request_id": str(uuid4()), "created_at": datetime.now(UTC).isoformat()}
        save(state_path, state)
    async with httpx.AsyncClient(base_url=args.shop_url, timeout=300, trust_env=False) as client:
        login = await client.post("/api/login", json={"username": identity["username"],
                                "password": Path(identity["password_file"]).read_text().strip()})
        login.raise_for_status()
        modules = await client.get("/api/modules")
        modules.raise_for_status()
        module = next(item for item in modules.json() if item["module_id"] == "data_center")
        save(args.output / "module.json", module)
        initial = await invoke(client, "sync", {"source_id": "microsoft/adventureworks"}, state["request_id"])
        save(args.output / "initial.json", initial)
        task_id = initial["data"]["task_id"]
        state["task_id"] = task_id
        save(state_path, state)
        result = initial
        deadline = asyncio.get_running_loop().time() + 240
        while result["status"] in {"queued", "running"}:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("真实同步任务没有在240秒内完成")
            await asyncio.sleep(0.2)
            result = await invoke(client, "show", {"task_id": task_id})
        save(args.output / "sync-result.json", result)
        assert result["status"] == "completed"
        assert result["data"]["snapshot"]["record_count"] == 1036
        duplicate = await invoke(client, "sync", {"source_id": "microsoft/adventureworks"}, state["request_id"])
        assert duplicate == result
        conflict = await client.post("/api/modules/data_center/operations/sync", json={
            "request_id": state["request_id"], "payload": {"source_id": "shop/inventory"}, "version": "1", "upstream": []})
        assert conflict.status_code in {403, 409, 422}
        export = await client.post("/api/modules/data_center/operations/export", json={
            "request_id": str(uuid4()), "payload": {"task_id": task_id, "format": "csv"}, "version": "1", "upstream": []})
        assert export.status_code == 409
    command = MCPCommand(command=str(ROOT / "agent-hub/.venv/bin/python"),
                         args=["-m", "agent_hub", "--config", str((args.runtime / "sync.hub.json").resolve())],
                         cwd=ROOT, timeout_seconds=300)
    async with connect(command) as client:
        assert ("shop_data_center_agent", "show") in client.catalog.entries
        spec = TaskSpec(task_id="data-sync-read", tool_name="shop_data_center_agent", action="show")
        arguments = client.catalog.arguments(spec, {"task_id": task_id}, str(uuid4()), [])
        mcp_result = await client.invoke(spec, arguments)
        assert mcp_result.model_dump(mode="json") == result
        save(args.output / "mcp-result.json", mcp_result.model_dump(mode="json"))
    source_paths = [ROOT / "src/shop/data_center_adapter.py", ROOT / "data-center/src/data_center/api.py",
                    ROOT / "data-center/src/data_center/runtime.py", ROOT / "scripts/verify_data_center_integration.py"]
    save(args.output / "report.json", {"status": "passed", "run_id": state["run_id"], "task_id": task_id,
        "checks": ["real_http_module", "persistent_sync", "snapshot_1036", "same_request_reuse",
                   "request_conflict", "snapshot_export_refused", "real_mcp_stdio_show"],
        "model_calls": 0, "source_sha256": {str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
                                           for path in source_paths}})
    print(json.dumps({"status": "passed", "checks": 7, "task_id": task_id}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shop-url", default="http://127.0.0.1:8002")
    parser.add_argument("--identities", type=Path, default=ROOT / ".scratch/evidence/dc02-host-20261004-r1/identities.json")
    parser.add_argument("--runtime", type=Path, default=ROOT / ".local/data-center/runtime")
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(verify(parser.parse_args()))


if __name__ == "__main__":
    main()
