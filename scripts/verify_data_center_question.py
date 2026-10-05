import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from uuid import uuid4

import httpx
from run_data_center_acceptance import ROOT, invoke, save, source_inputs


async def verify(args):
    args.output.mkdir(parents=True, exist_ok=True)
    state_path = args.output / "state.json"
    if state_path.exists():
        state = json.loads(state_path.read_text())
    else:
        fixture = json.loads((ROOT / "tests/fixtures/data_center_cases.json").read_text())
        registry = json.loads(args.registry.read_text())
        entry = next(item for item in source_inputs(fixture, registry)
                     if item["case_id"] == args.case and item["attempt"] == 1)
        state = {"input": entry, "started_at": datetime.now(UTC).isoformat()}
        save(state_path, state)
    identities = json.loads(args.identities.read_text())
    if identities["window"] != "dc-live-20261004":
        raise ValueError("真实问答检查没有关联已批准窗口")
    identity = identities["actors"][state["input"]["actor"]]
    started = monotonic()
    async with httpx.AsyncClient(base_url=args.shop_url, trust_env=False, timeout=300) as client:
        response = await client.post("/api/login", json={"username": identity["username"],
            "password": Path(identity["password_file"]).read_text().strip()})
        response.raise_for_status()
        result = await invoke(client, "ask", state["input"]["payload"], state["input"]["request_id"])
        save(args.output / "initial.json", result)
        if "task_id" not in result.get("data", {}):
            raise ValueError("真实问答没有创建可恢复任务")
        state["task_id"] = result["data"]["task_id"]
        save(state_path, state)
        while result.get("status") in {"queued", "running"}:
            if monotonic() - started > 240:
                raise TimeoutError("真实问答检查超过240秒")
            await asyncio.sleep(0.5)
            result = await invoke(client, "show", {"task_id": state["task_id"]}, str(uuid4()))
        save(args.output / "result.json", result)
        save(args.output / "report.json", {"case": args.case, "task_id": state["task_id"],
            "status": result.get("status", "failed"), "elapsed_seconds": monotonic() - started,
            "expected": state["input"]["expected"]})
        print(json.dumps({"case": args.case, "task_id": state["task_id"],
                          "status": result.get("status", "failed")}), flush=True)
        assert result["status"] in {"completed", "partial", "DATA_BLOCKED"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="MET-01")
    parser.add_argument("--shop-url", default="http://127.0.0.1:8002")
    parser.add_argument("--identities", type=Path, default=ROOT / ".scratch/evidence/dc02-host-20261004-r2/identities.json")
    parser.add_argument("--registry", type=Path, default=ROOT / ".local/data-center/sources/r2/sources.json")
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(verify(parser.parse_args()))


if __name__ == "__main__":
    main()
