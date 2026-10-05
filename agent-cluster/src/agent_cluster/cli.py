import argparse
import json
import stat
import sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

import httpx


class CLIError(Exception):
    pass


@dataclass(frozen=True)
class ClientConfig:
    base_url: str
    username: str
    expected_actor_id: str
    password: str = field(repr=False)


def _required_string(value, field_name):
    if not isinstance(value, str) or not value.strip():
        raise CLIError(f"配置字段 {field_name} 必须是非空字符串")
    return value


def load_client_config(path):
    config_path = Path(path)
    try:
        value = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError) as exc:
        raise CLIError("配置文件读取失败") from exc
    except json.JSONDecodeError as exc:
        raise CLIError(f"配置文件 JSON 解析失败，位置 {exc.lineno}:{exc.colno}") from exc
    if not isinstance(value, dict):
        raise CLIError("配置文件必须是 JSON 对象")
    required = {"base_url", "username", "password_file", "expected_actor_id"}
    missing = required - value.keys()
    unknown = value.keys() - required
    if missing:
        raise CLIError("配置文件缺少字段: " + ", ".join(sorted(missing)))
    if unknown:
        raise CLIError("配置文件包含未知字段: " + ", ".join(sorted(unknown)))

    base_url = _required_string(value["base_url"], "base_url")
    try:
        parsed_url = httpx.URL(base_url)
    except httpx.InvalidURL as exc:
        raise CLIError("base_url 格式无效") from exc
    if (parsed_url.scheme not in {"http", "https"} or not parsed_url.host
            or parsed_url.username or parsed_url.password or parsed_url.query
            or parsed_url.fragment or parsed_url.path not in {"", "/"}):
        raise CLIError("base_url 必须是没有凭据和路径的 HTTP(S) 地址")

    username = _required_string(value["username"], "username")
    expected_actor_id = _required_string(value["expected_actor_id"], "expected_actor_id")
    password_reference = Path(_required_string(value["password_file"], "password_file"))
    password_path = (password_reference if password_reference.is_absolute()
                     else config_path.parent / password_reference)
    try:
        password_stat = password_path.stat()
        if not stat.S_ISREG(password_stat.st_mode):
            raise CLIError("密码文件必须是普通文件")
        if stat.S_IMODE(password_stat.st_mode) != 0o600:
            raise CLIError("密码文件权限必须为 0600")
        password = password_path.read_text(encoding="utf-8").rstrip("\r\n")
    except (OSError, UnicodeError) as exc:
        raise CLIError("密码文件读取失败") from exc
    if not password:
        raise CLIError("密码文件内容不能为空")
    return ClientConfig(
        base_url=str(parsed_url).rstrip("/"),
        username=username,
        expected_actor_id=expected_actor_id,
        password=password,
    )


def read_json_object(path, *, forbidden_fields=frozenset()):
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError) as exc:
        raise CLIError("请求 JSON 文件读取失败") from exc
    except json.JSONDecodeError as exc:
        raise CLIError(f"请求 JSON 解析失败，位置 {exc.lineno}:{exc.colno}") from exc
    if not isinstance(value, dict):
        raise CLIError("请求文件内容必须是 JSON 对象")
    forbidden = set(value) & set(forbidden_fields)
    if forbidden:
        raise CLIError("请求文件不能包含身份或服务端编号字段: " + ", ".join(sorted(forbidden)))
    return value


def _payload_argument(parser):
    parser.add_argument("--payload-file", type=Path)


def build_parser():
    parser = argparse.ArgumentParser(prog="agent-cluster")
    parser.add_argument("--config", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)

    goals = commands.add_parser("goals")
    goal_actions = goals.add_subparsers(dest="action", required=True)
    create_goal = goal_actions.add_parser("create")
    create_goal.add_argument("--file", required=True, type=Path)
    goal_actions.add_parser("list")
    show_goal = goal_actions.add_parser("show")
    show_goal.add_argument("goal_id")
    update_goal = goal_actions.add_parser("update")
    update_goal.add_argument("goal_id")
    update_goal.add_argument("--file", required=True, type=Path)
    update_goal.add_argument("--expected-revision", required=True, type=int)
    for action in ("pause", "resume", "cancel"):
        control_goal = goal_actions.add_parser(action)
        control_goal.add_argument("goal_id")
    start_goal = goal_actions.add_parser("start")
    start_goal.add_argument("goal_id")
    start_goal.add_argument("--event-id", required=True)
    _payload_argument(start_goal)

    runs = commands.add_parser("runs")
    run_actions = runs.add_subparsers(dest="action", required=True)
    show_run = run_actions.add_parser("show")
    show_run.add_argument("run_id")
    history_run = run_actions.add_parser("history")
    history_run.add_argument("run_id")

    events = commands.add_parser("events")
    event_actions = events.add_subparsers(dest="action", required=True)
    emit_event = event_actions.add_parser("emit")
    emit_event.add_argument("--goal-id", required=True)
    emit_event.add_argument("--event-id", required=True)
    _payload_argument(emit_event)

    schedules = commands.add_parser("schedules")
    schedule_actions = schedules.add_subparsers(dest="action", required=True)
    create_schedule = schedule_actions.add_parser("create")
    create_schedule.add_argument("--goal-id", required=True)
    trigger = create_schedule.add_mutually_exclusive_group(required=True)
    trigger.add_argument("--interval-seconds", type=int)
    trigger.add_argument("--cron")
    create_schedule.add_argument("--timezone", default="Asia/Shanghai")
    create_schedule.add_argument("--disabled", action="store_true")
    schedule_actions.add_parser("list")
    update_schedule = schedule_actions.add_parser("update")
    update_schedule.add_argument("schedule_id")
    enabled = update_schedule.add_mutually_exclusive_group(required=True)
    enabled.add_argument("--enabled", dest="enabled", action="store_true")
    enabled.add_argument("--disabled", dest="enabled", action="store_false")

    return parser


def _request_json(client, method, path, body=None):
    try:
        response = client.request(method, path, json=body)
    except httpx.HTTPError as exc:
        raise CLIError(f"{method} {path} 请求失败: {type(exc).__name__}") from exc
    if response.is_error:
        raise CLIError(f"{method} {path} 返回 HTTP {response.status_code}")
    if not response.content:
        return {"status": "ok"}
    try:
        return response.json()
    except ValueError as exc:
        raise CLIError(f"{method} {path} 返回内容不是有效 JSON") from exc


def _read_payload(path):
    return {} if path is None else read_json_object(path)


def _execute(client, arguments):
    if arguments.command == "goals":
        if arguments.action == "create":
            body = read_json_object(arguments.file, forbidden_fields={"actor_id", "goal_id"})
            return _request_json(client, "POST", "/api/cluster/goals", body)
        if arguments.action == "list":
            return _request_json(client, "GET", "/api/cluster/goals")
        if arguments.action == "show":
            goal_id = quote(arguments.goal_id, safe="")
            return _request_json(client, "GET", f"/api/cluster/goals/{goal_id}")
        if arguments.action == "update":
            goal_id = quote(arguments.goal_id, safe="")
            definition = read_json_object(
                arguments.file, forbidden_fields={"actor_id", "goal_id", "revision"})
            return _request_json(
                client, "PUT", f"/api/cluster/goals/{goal_id}",
                {"expected_revision": arguments.expected_revision, "definition": definition},
            )
        if arguments.action in {"pause", "resume", "cancel"}:
            goal_id = quote(arguments.goal_id, safe="")
            path = f"/api/cluster/goals/{goal_id}/control/{arguments.action}"
            return _request_json(client, "POST", path)
        goal_id = quote(arguments.goal_id, safe="")
        return _request_json(
            client, "POST", f"/api/cluster/goals/{goal_id}/trigger",
            {"event_id": arguments.event_id, "payload": _read_payload(arguments.payload_file)},
        )

    if arguments.command == "runs":
        run_id = quote(arguments.run_id, safe="")
        if arguments.action == "show":
            return _request_json(client, "GET", f"/api/cluster/runs/{run_id}")
        return _request_json(client, "GET", f"/api/cluster/runs/{run_id}/history")

    if arguments.command == "events":
        return _request_json(
            client, "POST", "/api/cluster/events",
            {"goal_id": arguments.goal_id, "event_id": arguments.event_id,
             "payload": _read_payload(arguments.payload_file)},
        )

    if arguments.command == "schedules":
        if arguments.action == "create":
            body = {
                "goal_id": arguments.goal_id,
                "interval_seconds": arguments.interval_seconds,
                "cron": arguments.cron,
                "timezone": arguments.timezone,
                "enabled": not arguments.disabled,
            }
            body = {key: value for key, value in body.items() if value is not None}
            return _request_json(client, "POST", "/api/cluster/schedules", body)
        if arguments.action == "list":
            return _request_json(client, "GET", "/api/cluster/schedules")
        schedules = _request_json(client, "GET", "/api/cluster/schedules")
        if not isinstance(schedules, list):
            raise CLIError("日程列表响应必须是 JSON 数组")
        matches = [item for item in schedules
                   if isinstance(item, dict) and item.get("schedule_id") == arguments.schedule_id]
        if len(matches) != 1:
            raise CLIError("当前身份下没有唯一匹配的日程")
        schedule = {**matches[0], "enabled": arguments.enabled}
        schedule_id = quote(arguments.schedule_id, safe="")
        return _request_json(client, "PUT", f"/api/cluster/schedules/{schedule_id}", schedule)
    raise CLIError("命令无效")


def main(argv=None):
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        config = load_client_config(arguments.config)
        with httpx.Client(base_url=config.base_url, trust_env=False, timeout=30) as client:
            login = _request_json(
                client, "POST", "/api/login",
                {"username": config.username, "password": config.password},
            )
            if (not isinstance(login, dict) or str(login.get("id", ""))
                    != config.expected_actor_id):
                raise CLIError("登录身份与 expected_actor_id 不匹配")
            result = _execute(client, arguments)
    except CLIError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0
