import argparse
import json
import os
from pathlib import Path
from secrets import token_urlsafe
from stat import S_IMODE

from shop.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def prepare_service_identity(path: Path):
    path = path.resolve()
    created = False
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(token_urlsafe(48) + "\n")
        created = True
    if S_IMODE(path.stat().st_mode) != 0o600:
        raise PermissionError("数据服务身份凭据文件需要0600权限")
    token = path.read_text(encoding="utf-8").strip()
    if len(token) < 32 or not token.isascii():
        raise ValueError("数据服务身份凭据需要至少32个ASCII字符")
    return {"path": str(path), "created": created}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--identities", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shop-url", default="http://127.0.0.1:8002")
    parser.add_argument("--service-token-file", type=Path,
                        default=Settings().data_center_service_token_file)
    args = parser.parse_args()
    service_identity = prepare_service_identity(args.service_token_file)
    identities = json.loads(args.identities.read_text())["actors"]
    template = json.loads((ROOT / "agent-hub/config.example.json").read_text())
    args.output.mkdir(parents=True, exist_ok=True)
    commands, configs = {}, {}
    for name, identity in identities.items():
        config = {**template, "connectors": [{**template["connectors"][0],
            "base_url": args.shop_url, "username": identity["username"],
            "expected_actor_id": identity["actor_id"],
            "password_file": str((ROOT / identity["password_file"]).resolve()), "read_timeout": 300}]}
        target = args.output / (name + ".hub.json")
        target.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n")
        target.chmod(0o600)
        configs[name] = str(target.resolve())
        commands[identity["actor_id"]] = {
            "command": str(ROOT / "agent-hub/.venv/bin/python"),
            "args": ["-m", "agent_hub", "--config", str(target.resolve())], "cwd": str(ROOT),
            "timeout_seconds": 300}
    cluster_path = args.output / "cluster.json"
    cluster_path.write_text(json.dumps({"actors": commands, "autorun": False, "data_center_autorun": True,
                                       "profile": {"model": "deepseek-flash", "reasoning_effort": "high",
                                                   "output_limit": 8192, "context_tokens": 1000000}},
                                      ensure_ascii=False, indent=2) + "\n")
    cluster_path.chmod(0o600)
    (args.output / "index.json").write_text(json.dumps({"hub_configs": configs,
                                                         "cluster_config": str(cluster_path.resolve())}, indent=2) + "\n")
    print(json.dumps({"actors": len(commands), "cluster_config": str(cluster_path),
                      "service_identity": service_identity}))


if __name__ == "__main__":
    main()
