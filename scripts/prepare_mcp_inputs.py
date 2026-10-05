import argparse
import json
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from scripts.verify_integration_live import snapshot
from shop.config import Settings
from shop.integration_models import IntegrationIdentityMap
from shop.models import User
from shop.poster import PosterService
from shop.runtime_models import HarnessTask

OPERATIONS = {
    "harness": ["create", "list", "show", "run", "resume", "cancel"],
    "customer": ["identity", "orders", "order", "list", "create_conversation", "conversation",
                 "submit", "turn", "events", "cancel", "resume"],
    "assessment": ["create", "list", "show", "run", "resume", "cancel", "metrics", "evidence"],
    "poster": ["status", "prepare", "prepare_direct", "show", "run", "restore"],
}
DESCRIPTIONS = {
    "harness": "从来源素材生成并审核宣传内容", "customer": "查询本人订单并处理已确认的客服工单",
    "assessment": "在授权员工范围生成销售考核报告", "poster": "准备海报关键词并调用已登记业务角色",
}


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def write_secret(path, content):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(content)


def main():
    parser = argparse.ArgumentParser(description="固定MCP实际业务输入和受保护身份配置")
    parser.add_argument("--previous-inputs", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--assessment-root", required=True, type=Path)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    args = parser.parse_args()
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    config_dir = directory / "configs"
    config_dir.mkdir(mode=0o700)
    previous = json.loads(args.previous_inputs.read_text(encoding="utf-8"))
    settings = Settings()
    service = PosterService(settings)
    engine = create_engine(settings.database_url)
    try:
        with Session(engine) as session:
            accounts = dict(previous["accounts"])
            for name in ("assessment-manager", "assessment-manager2", "assessment-admin", "assessment-empty"):
                mapping = session.scalar(select(IntegrationIdentityMap).where(
                    IntegrationIdentityMap.source_username == name))
                assert mapping is not None, "考核历史身份映射必须存在"
                user = session.get(User, mapping.target_user_id)
                accounts[name] = {"username": user.username, "actor_id": user.id,
                                  "password_file": str(args.assessment_root / ".local/assessment/auth" / f"{name}.password")}
            selected = None
            for task in session.scalars(select(HarnessTask).where(
                    HarnessTask.status == "completed").order_by(HarnessTask.created_at.desc())):
                if not (task.result and task.result.get("status") == "materials_found"
                        and task.result.get("review", {}).get("approved") is True
                        and 1 <= len(task.result.get("recommendations", [])) <= 2):
                    continue
                poster_input = service.read_harness(task.id, accounts["admin"]["actor_id"])
                job = service.create_job(poster_input, accounts["admin"]["actor_id"])
                if not service.job_path(job.job_id).exists():
                    selected = (task, poster_input, job)
                    break
            assert selected is not None, "需要真实已审核且尚未准备关键词的Harness任务"
            task, poster_input, job = selected
            config_paths = {}
            for name, account in accounts.items():
                source = Path(account["password_file"])
                if not source.is_absolute():
                    source = args.source_root / source
                secret = config_dir / f"{name}.password"
                write_secret(secret, source.read_text(encoding="utf-8"))
                connector = {"connector_id": "shop", "base_url": args.base_url,
                             "username": account["username"], "expected_actor_id": account["actor_id"],
                             "password_file": str(secret)}
                agents = [{"connector_id": "shop", "module_id": module,
                           "agent_id": f"shop_{module}", "tool_name": f"shop_{module}_agent",
                           "description": DESCRIPTIONS[module], "allowed_operations": operations,
                           "expected_version": "1"} for module, operations in OPERATIONS.items()]
                shared = [{"connector_id": "shop", "module_id": "materials", "operation": operation,
                           "tool_name": f"shop_materials_{operation}", "description": description,
                           "expected_version": "1"}
                          for operation, description in (("search", "查询来源素材"), ("show", "读取来源素材及版本"))]
                path = config_dir / f"{name}.json"
                save(path, {"connectors": [connector], "agents": agents, "shared_tools": shared})
                config_paths[name] = str(path)
            run_id = "mcp-live-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
            inputs = {**previous, "run_id": run_id, "base_url": args.base_url, "accounts": accounts,
                      "hub_configs": config_paths, "harness_task_id": task.id, "harness_run_id": task.run_id,
                      "harness_result_sha256": sha256(json.dumps(task.result, ensure_ascii=False,
                          sort_keys=True, default=str).encode()).hexdigest(),
                      "poster_input": poster_input.model_dump(mode="json"), "poster_job_id": job.job_id,
                      "request_ids": {name: str(uuid4()) for name in previous["request_ids"]},
                      "previous_inputs_sha256": sha256(args.previous_inputs.read_bytes()).hexdigest()}
            save(directory / "inputs.json", inputs)
            save(directory / "baseline.json", snapshot(settings))
            print(json.dumps({"run_id": run_id, "inputs": str(directory / "inputs.json"),
                              "configs": config_paths}, ensure_ascii=False))
    finally:
        engine.dispose()
        service.engine.dispose()


if __name__ == "__main__":
    main()
