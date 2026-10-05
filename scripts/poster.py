import argparse
import importlib
import json
from pathlib import Path

from shop.config import Settings
from shop.poster import PosterService


def main():
    parser = argparse.ArgumentParser(description="自媒体海报框架接入")
    parser.add_argument("--agents", help="组员注册入口module:function，函数接收PosterService")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    prepare = commands.add_parser("prepare")
    prepare.add_argument("task_id")
    prepare.add_argument("--actor-id", required=True)
    direct = commands.add_parser("direct")
    direct.add_argument("--keyword", action="append", required=True)
    direct.add_argument("--version-id", action="append", required=True)
    direct.add_argument("--actor-id", required=True)
    direct.add_argument("--constraints", type=Path, help="JSON限制条件文件")
    for command in ("show", "run", "restore", "agent-run"):
        subparser = commands.add_parser(command)
        subparser.add_argument("job_id")
        subparser.add_argument("--actor-id", required=True)
        if command == "restore":
            subparser.add_argument("run_id")
        if command == "agent-run":
            subparser.add_argument("role")
            subparser.add_argument("--upstream-record")
    args = parser.parse_args()
    service = PosterService(Settings())
    if args.agents:
        module_name, function_name = args.agents.split(":", 1)
        builder = getattr(importlib.import_module(module_name), function_name)
        service.configure(builder(service))
    if args.command == "status":
        result = service.status()
    elif args.command == "prepare":
        result = service.prepare(args.task_id, args.actor_id).model_dump(mode="json")
    elif args.command == "direct":
        constraints = json.loads(args.constraints.read_text(encoding="utf-8")) if args.constraints else None
        result = service.prepare_direct(args.keyword, args.version_id, args.actor_id, constraints).model_dump(mode="json")
    elif args.command == "restore":
        session = service.restore(args.job_id, args.run_id, args.actor_id)
        result = {"run_id": session.run_id, "configuration": session.configuration,
                  "usage": session.usage.model_dump(), "final_result": session.final_result}
    elif args.command == "agent-run":
        result = service.agent_run(args.job_id, args.role, args.actor_id, args.upstream_record)
    else:
        result = getattr(service, args.command)(args.job_id, args.actor_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
