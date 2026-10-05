import argparse
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

import httpx


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def parser():
    command = argparse.ArgumentParser(prog="data-center")
    command.add_argument("--shop-url", default=os.environ.get("DATA_CENTER_SHOP_URL", "http://127.0.0.1:8002"))
    command.add_argument("--username", required=True)
    command.add_argument("--password-file", type=Path, required=True)
    command.add_argument("--request-id", required=True)
    commands = command.add_subparsers(dest="command", required=True)

    for name in ("catalog", "metrics", "tasks", "manage-sources"):
        commands.add_parser(name)

    ask = commands.add_parser("ask")
    ask.add_argument("question")
    ask.add_argument("--source-id", action="append", required=True)
    ask.add_argument("--retrieval-mode", choices=("hybrid", "graph"), default="hybrid")

    for name in ("show", "run", "cancel"):
        item = commands.add_parser(name)
        item.add_argument("task_id")

    reference = commands.add_parser("reference")
    reference.add_argument("reference_file", type=Path)

    export = commands.add_parser("export")
    export.add_argument("task_id")
    export.add_argument("--format", choices=("markdown", "csv"), required=True)
    export.add_argument("--output", type=Path, required=True)

    register = commands.add_parser("register-source")
    register.add_argument("source_file", type=Path)

    grant = commands.add_parser("set-grant")
    grant.add_argument("grant_file", type=Path)

    withdraw = commands.add_parser("withdraw-source")
    withdraw.add_argument("source_id")

    invoke = commands.add_parser("invoke")
    invoke.add_argument("operation")
    invoke.add_argument("--input", type=Path, required=True,
                         help="JSON 文件路径；使用 - 时从标准输入读取")
    return command


def request_payload(args):
    if args.command in {"catalog", "metrics", "tasks", "manage-sources"}:
        operation = {"manage-sources": "manage_sources"}.get(args.command, args.command)
        return operation, {}
    if args.command == "ask":
        return "ask", {"question": args.question, "source_ids": args.source_id,
                        "retrieval_mode": args.retrieval_mode}
    if args.command in {"show", "run", "cancel"}:
        return args.command, {"task_id": args.task_id}
    if args.command == "reference":
        return "reference", read_json_input(args.reference_file)
    if args.command == "export":
        return "export", {"task_id": args.task_id, "format": args.format}
    if args.command == "register-source":
        return "register_source", read_json_input(args.source_file)
    if args.command == "set-grant":
        return "set_grant", read_json_input(args.grant_file)
    if args.command == "withdraw-source":
        return "withdraw_source", {"source_id": args.source_id}
    if args.command == "invoke":
        return args.operation, read_json_input(args.input)
    raise ValueError("未登记的数据中台命令")


def read_json_input(path: Path):
    if str(path) == "-":
        if sys.stdin.isatty():
            raise ValueError("JSON 标准输入必须通过管道或重定向提供")
        return json.loads(sys.stdin.read())
    return read_json(path)


def invoke(client: httpx.Client, operation: str, payload: dict, request_id: str):
    response = client.post(
        "/api/modules/data_center/operations/" + operation,
        json={"request_id": request_id, "version": "1", "payload": payload, "upstream": []},
    )
    if not response.is_success:
        try:
            detail = response.json().get("detail", "")
        except ValueError:
            detail = ""
        if isinstance(detail, dict):
            detail = detail.get("message", detail.get("code", ""))
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"数据中台操作 {operation} 返回 HTTP {response.status_code}{suffix}")
    return response.json()


def export_file(client: httpx.Client, result, output: Path):
    reference = next((item for item in result.get("resources", [])
                      if item.get("resource_type") in {"export_markdown", "export_csv"}), None)
    if reference is None:
        raise ValueError("导出结果缺少已登记的文件引用")
    relative_path = result["data"]["relative_path"]
    path = ("/api/modules/data_center/resources/" + reference["resource_id"]
            + "/files/" + relative_path)
    params = {"resource_type": reference["resource_type"]}
    for key in ("version", "sha256"):
        if reference.get(key) is not None:
            params[key] = reference[key]
    response = client.get(path, params=params)
    response.raise_for_status()
    actual = hashlib.sha256(response.content).hexdigest()
    expected = response.headers.get("x-content-sha256")
    if expected != actual or reference.get("sha256") not in {None, actual}:
        raise ValueError("导出文件摘要与服务端资源引用不一致")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as file:
        file.write(response.content)
    return {"path": str(output), "sha256": actual, "bytes": len(response.content)}


def main():
    args = parser().parse_args()
    password_mode = stat.S_IMODE(args.password_file.stat().st_mode)
    if password_mode & 0o077:
        raise PermissionError("密码文件权限过宽，请仅允许文件所有者读取")
    password = args.password_file.read_text(encoding="utf-8").strip()
    if not password:
        raise ValueError("密码文件为空")
    with httpx.Client(base_url=args.shop_url.rstrip("/"), timeout=300, trust_env=False) as client:
        login = client.post("/api/login", json={"username": args.username, "password": password})
        login.raise_for_status()
        identity = login.json()
        operation, payload = request_payload(args)
        result = invoke(client, operation, payload, args.request_id)
        if args.command == "export":
            result = {"result": result, "file": export_file(client, result, args.output)}
        result = {"actor_id": identity["id"], "operation": operation, "result": result}
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
