import argparse
import asyncio
import json
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
from agent_cluster.notes import NoteStore
from apscheduler.triggers.cron import CronTrigger
from data_center_protocol import DocumentIngest

from shop.config import Settings

ROOT = Path(__file__).resolve().parents[1]


async def login(client, identity):
    password_path = ROOT / identity["password_file"]
    response = await client.post("/api/login", json={
        "username": identity["username"], "password": password_path.read_text().strip(),
    })
    response.raise_for_status()


async def install(arguments):
    if arguments.paused and arguments.trigger:
        raise ValueError("暂停目标不能发起初始事件")
    if arguments.interval_seconds < 1:
        raise ValueError("来源检查间隔必须大于零")
    CronTrigger.from_crontab(arguments.nightly_cron, timezone=ZoneInfo(arguments.timezone))
    identities = json.loads(arguments.identities.read_text())["actors"]
    owner, administrator = identities[arguments.actor], identities["admin"]
    if arguments.operation == "sync":
        if arguments.source_id is None or arguments.payload is not None:
            raise ValueError("同步监听需要一个来源编号")
        payload = {"source_id": arguments.source_id}
        source_ids = [arguments.source_id]
    else:
        if arguments.payload is None or arguments.source_id is not None:
            raise ValueError("文档监听需要DocumentIngest输入文件")
        specification = DocumentIngest.model_validate_json(arguments.payload.read_text())
        payload = specification.model_dump(mode="json")
        source_ids = sorted({item.source_id for item in specification.documents})
    if not arguments.budget_cny.is_finite() or not Decimal(0) < arguments.budget_cny <= Decimal(30):
        raise ValueError("目标预算必须大于零且不超过30元")
    criteria = [
        {"tool_name": "shop_data_center_agent", "action": arguments.operation,
         "path": ["status"], "operator": "equals", "expected": "completed"},
        {"tool_name": "shop_data_center_agent", "action": arguments.operation,
         "path": ["data", "snapshot", "state"], "operator": "equals", "expected": "published"},
        {"tool_name": "shop_data_center_agent", "action": arguments.operation,
         "path": ["resources", 0], "operator": "resource", "module_id": "data_center",
         "resource_type": "snapshot"},
    ]
    registration = {"actor_id": owner["actor_id"], "operation": arguments.operation,
                    "payload": payload, "source_ids": source_ids,
                    "criteria": criteria,
                    "budget_cny": str(arguments.budget_cny),
                    "interval_seconds": arguments.interval_seconds,
                    "nightly_cron": arguments.nightly_cron, "timezone": arguments.timezone}
    if arguments.installation_id is not None:
        registration["installation_id"] = str(arguments.installation_id)
    signature = sha256(json.dumps(registration, sort_keys=True, ensure_ascii=False,
                                  separators=(",", ":")).encode()).hexdigest()
    objective = (
        f"数据中台来源监听 {signature}。读取主题为data-center-source-input的Note，"
        f"按原operation和payload完成{arguments.operation}，保持来源、版本及摘要。"
        "只使用此目标登记的操作和来源；完成结果必须提供published快照及快照引用。"
        "资料不足、权限变化、取消、unknown及费用不足时保留真实状态。"
    )
    async with (
        httpx.AsyncClient(base_url=arguments.shop_url, trust_env=False, timeout=300) as owner_client,
        httpx.AsyncClient(base_url=arguments.shop_url, trust_env=False, timeout=300) as admin_client,
    ):
        await login(owner_client, owner)
        await login(admin_client, administrator)
        response = await owner_client.get("/api/cluster/goals")
        response.raise_for_status()
        existing = [goal for goal in response.json() if goal["objective"] == objective]
        if len(existing) > 1:
            raise ValueError("同一来源监听存在多个目标")
        if existing:
            goal = existing[0]
        else:
            response = await owner_client.post("/api/cluster/goals", json={
                "objective": objective,
                "allowed_operations": {"shop_data_center_agent": [arguments.operation, "show"]},
                "criteria": criteria,
                "budget_cny": str(arguments.budget_cny), "max_decisions": 10, "max_tasks": 10,
            })
            response.raise_for_status()
            goal = response.json()
        goal_id, actor_id = goal["goal_id"], goal["actor_id"]
        if actor_id != owner["actor_id"]:
            raise PermissionError("来源监听目标不属于登记账号")
        notes = NoteStore(arguments.notes_dir)
        note_content = json.dumps(registration, ensure_ascii=False, sort_keys=True)
        note_id = "data-center-source-input-" + signature[:32]
        references = [item for item in notes.index(goal_id=goal_id, actor_id=actor_id)
                      if item.note_id == note_id]
        if references:
            reference = references[0]
            if notes.read(reference, goal_id=goal_id, actor_id=actor_id) != note_content:
                raise ValueError("来源监听Note与固定输入不一致")
        else:
            reference = notes.write(
                goal_id=goal_id, actor_id=actor_id, note_id=note_id,
                topic="data-center-source-input", content=note_content,
                source="data_center_watch_installation", summary="来源监听的完整操作输入及预算",
            )
        if arguments.paused:
            response = await owner_client.post(f"/api/cluster/goals/{goal_id}/control/pause")
            response.raise_for_status()
        response = await admin_client.post("/api/data-center/automation", json={
            "goal_id": goal_id, "actor_id": actor_id, "source_ids": source_ids,
            "operation": arguments.operation, "interval_seconds": arguments.interval_seconds,
            "nightly_cron": arguments.nightly_cron, "timezone": arguments.timezone,
        })
        response.raise_for_status()
        watch = response.json()
        triggered = None
        if arguments.trigger:
            response = await owner_client.post(f"/api/cluster/goals/{goal_id}/trigger", json={
                "event_id": "data-center-initial:" + signature,
                "payload": {"source_ids": source_ids, "registration_sha256": signature},
            })
            response.raise_for_status()
            triggered = response.json()
        response = await owner_client.get(f"/api/cluster/goals/{goal_id}")
        response.raise_for_status()
        state = response.json()["state"]
        report = {"registration": registration, "registration_sha256": signature,
                  "goal_id": goal_id, "watch": watch,
                  "note": reference.model_dump(mode="json"), "initial_run": triggered,
                  "state": state}
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({"goal_id": goal_id, "watch_id": watch["watch_id"],
                          "state": state}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--identities", type=Path, required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--installation-id", type=UUID)
    parser.add_argument("--operation", choices=("sync", "ingest"), required=True)
    parser.add_argument("--source-id")
    parser.add_argument("--payload", type=Path)
    parser.add_argument("--budget-cny", type=Decimal, default=Decimal(5))
    parser.add_argument("--interval-seconds", type=int, default=900)
    parser.add_argument("--nightly-cron", default="0 2 * * *")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--shop-url", default="http://127.0.0.1:8002")
    parser.add_argument("--notes-dir", type=Path, default=Settings().cluster_notes_dir)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--paused", action="store_true")
    parser.add_argument("--trigger", action="store_true")
    asyncio.run(install(parser.parse_args()))


if __name__ == "__main__":
    main()
