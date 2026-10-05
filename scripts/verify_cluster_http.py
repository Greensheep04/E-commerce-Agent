import argparse
import json
import os
import stat
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.runtime_models import BudgetAccount, ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
INPUTS_DEFAULT = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json"
BASE_URL_DEFAULT = "http://127.0.0.1:8001"
SENSITIVE_VALUES = []


def digest(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def save_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
                          encoding="utf-8")


def password_path_for(account):
    path = Path(account["password_file"])
    return path if path.is_absolute() else ROOT / path


def read_password(account, label):
    path = password_path_for(account)
    file_stat = path.stat()
    if not stat.S_ISREG(file_stat.st_mode) or stat.S_IMODE(file_stat.st_mode) != 0o600:
        raise ValueError(f"{label} 密码文件必须是普通文件且权限为 0600")
    password = path.read_text(encoding="utf-8").rstrip("\r\n")
    if not password:
        raise ValueError(f"{label} 密码文件内容为空")
    SENSITIVE_VALUES.append(password)
    return password


def load_fixed_inputs(path):
    inputs = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(inputs, dict) or not isinstance(inputs.get("accounts"), dict):
        raise TypeError("固定输入文件缺少账号对象")
    for label in ("customer", "other"):
        account = inputs["accounts"].get(label)
        if not isinstance(account, dict):
            raise TypeError(f"固定输入文件缺少 {label} 账号")
        for key in ("actor_id", "username", "password_file"):
            if not isinstance(account.get(key), str) or not account[key]:
                raise ValueError(f"固定输入文件的 {label} 账号缺少 {key}")
    inputs["accounts"]["customer"]["password"] = read_password(inputs["accounts"]["customer"], "customer")
    inputs["accounts"]["other"]["password"] = read_password(inputs["accounts"]["other"], "other")
    return inputs


def validate_cluster_config(path, actor_ids):
    config_path = Path(path).resolve(strict=True)
    raw = config_path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise TypeError("集群配置必须是 JSON 对象")
    if value.get("autorun") is not False:
        raise ValueError("真实 HTTP 检查要求集群配置 autorun=false")
    actors = value.get("actors")
    if not isinstance(actors, dict):
        raise TypeError("集群配置缺少 actors 对象")
    missing = set(actor_ids) - actors.keys()
    if missing:
        raise ValueError("集群配置缺少检查账号的 MCP 接入项")
    return {
        "path": str(config_path),
        "sha256": sha256(raw).hexdigest(),
        "autorun": value["autorun"],
        "actor_ids": sorted(actor_ids),
    }


def service_pids(port):
    result = subprocess.run(
        ["lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"端口 {port} 没有可读取的监听进程")
    pids = sorted({int(line) for line in result.stdout.splitlines() if line.strip()})
    if not pids:
        raise RuntimeError(f"端口 {port} 没有有效监听进程编号")
    return pids


def usage_snapshot():
    engine = create_engine(Settings().database_url)
    try:
        with Session(engine) as session:
            rows = session.scalars(select(ProviderRequest).order_by(ProviderRequest.id)).all()
            budget = session.get(BudgetAccount, "project")
            if budget is None:
                raise RuntimeError("项目费用账户不存在")
            return {
                "request_count": len(rows),
                "provider_requests": {
                    row.id: {
                        "status": row.status,
                        "reservation_cny": str(row.reservation_cny),
                        "charged_cny": (None if row.charged_cny is None
                                         else str(row.charged_cny)),
                        "cluster_goal_id": row.price_basis.get("cluster_goal_id"),
                        "cluster_run_id": row.price_basis.get("cluster_run_id"),
                        "verification_scope": (row.price_basis.get("verification_scope")
                                                or row.price_basis.get("scope")),
                    }
                    for row in rows
                },
                "spent_cny": str(budget.spent_cny),
                "reserved_cny": str(budget.reserved_cny),
            }
    finally:
        engine.dispose()


def usage_changes(before, after, goal_ids, run_ids):
    old_requests = before["provider_requests"]
    new_requests = after["provider_requests"]
    changed = []
    scoped = []
    for request_id in sorted(old_requests.keys() | new_requests.keys()):
        old = old_requests.get(request_id)
        new = new_requests.get(request_id)
        if old == new:
            continue
        entry = {"request_id": request_id, "before": old, "after": new}
        records = [record for record in (old, new) if record is not None]
        if any(record["cluster_goal_id"] in goal_ids
               or record["cluster_run_id"] in run_ids for record in records):
            scoped.append(entry)
        else:
            changed.append(entry)
    return {
        "test_scoped_changes": scoped,
        "shared_changes": changed,
        "budget_delta_cny": {
            "spent": str(Decimal(after["spent_cny"]) - Decimal(before["spent_cny"])),
            "reserved": str(Decimal(after["reserved_cny"])
                            - Decimal(before["reserved_cny"])),
        },
    }


def require_status(response, expected, label):
    if response.status_code != expected:
        raise AssertionError(f"{label}: 期望 HTTP {expected}，实际 HTTP {response.status_code}")
    return response.json() if response.content else {}


def require_success(response, label):
    if not 200 <= response.status_code < 300:
        raise AssertionError(f"{label}: HTTP {response.status_code}")
    return response.json() if response.content else {}


def goal_definition(objective):
    return {
        "objective": objective,
        "allowed_operations": {"shop_customer_agent": ["orders"]},
        "criteria": [{
            "tool_name": "shop_customer_agent",
            "action": "orders",
            "path": ["data", "orders"],
            "operator": "exists",
        }],
        "budget_cny": "1",
        "max_decisions": 5,
        "max_tasks": 5,
    }


def redact(value, secrets):
    for secret in secrets:
        if secret:
            value = value.replace(secret, "[REDACTED]")
    return value


def run_cli(arguments, output_dir, secrets, invocations):
    command = [sys.executable, "-m", "agent_cluster", *map(str, arguments)]
    result = subprocess.run(
        command,
        cwd=ROOT / "agent-cluster",
        env={**os.environ, "TMPDIR": str(output_dir / "tmp")},
        capture_output=True,
        text=True,
        check=False,
    )
    if any(secret and (secret in result.stdout or secret in result.stderr) for secret in secrets):
        raise AssertionError("CLI 输出包含密码")
    stdout = redact(result.stdout, secrets)
    stderr = redact(result.stderr, secrets)
    label = f"cli-{len(list(output_dir.glob('cli-*.stdout.txt'))) + 1}"
    (output_dir / f"{label}.stdout.txt").write_text(stdout, encoding="utf-8")
    (output_dir / f"{label}.stderr.txt").write_text(stderr, encoding="utf-8")
    invocations.append({"arguments": list(map(str, arguments[1:])),
                        "return_code": result.returncode,
                        "stdout_file": f"{label}.stdout.txt",
                        "stderr_file": f"{label}.stderr.txt"})
    if result.returncode != 0:
        raise AssertionError(f"CLI {arguments[1:3]} 退出码为 {result.returncode}")
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"CLI {arguments[1:3]} 输出不是 JSON") from exc


def run_checks(args, output_dir, run_id, report):
    inputs = load_fixed_inputs(args.inputs)
    report["inputs_sha256"] = digest(args.inputs)
    report["source_sha256"] = {
        str(path.relative_to(ROOT)): digest(path) for path in (
            Path(__file__).resolve(), ROOT / "agent-cluster/src/agent_cluster/cli.py",
            ROOT / "agent-cluster/src/agent_cluster/api.py", ROOT / "src/shop/cluster.py",
            ROOT / "src/shop/app.py",
        )
    }
    customer = inputs["accounts"]["customer"]
    other = inputs["accounts"]["other"]
    config = validate_cluster_config(args.cluster_config, {customer["actor_id"]})
    base_url = httpx.URL(args.base_url)
    if (base_url.scheme not in {"http", "https"} or not base_url.host
            or base_url.username or base_url.password or base_url.query or base_url.fragment):
        raise ValueError("base_url 必须是没有凭据的 HTTP(S) 地址")
    port = base_url.port or (443 if base_url.scheme == "https" else 80)
    pids = service_pids(port)
    before = usage_snapshot()
    report["service_pids"] = pids
    report["cluster_config"] = config
    report["provider_requests_and_budget"] = {"before": before}
    checks = report["checks"]
    request_log = report["http_requests"]
    cli_invocations = report["cli_invocations"]
    test_resource_ids = {"goal_ids": [], "run_ids": []}
    report["test_resource_ids"] = test_resource_ids
    secrets = [customer["password"], other["password"]]

    def request(client, method, path, body=None):
        report["current_http_request"] = {"method": method, "path": path}
        response = client.request(method, path, json=body)
        request_log.append({"method": method, "path": path, "status_code": response.status_code})
        report.pop("current_http_request", None)
        return response

    event_id = f"{run_id}-event"
    with (
        httpx.Client(base_url=str(base_url).rstrip("/"), trust_env=False, timeout=30) as owner,
        httpx.Client(base_url=str(base_url).rstrip("/"), trust_env=False, timeout=30) as other_client,
        httpx.Client(base_url=str(base_url).rstrip("/"), trust_env=False, timeout=30) as anonymous,
    ):
        require_status(request(anonymous, "GET", "/api/cluster/goals"), 401, "匿名目标列表")
        checks.append("anonymous_access_denied")

        owner_login = require_status(request(owner, "POST", "/api/login", {
            "username": customer["username"], "password": customer["password"],
        }), 200, "customer 登录")
        other_login = require_status(request(other_client, "POST", "/api/login", {
            "username": other["username"], "password": other["password"],
        }), 200, "other 登录")
        if owner_login.get("id") != customer["actor_id"]:
            raise AssertionError("customer 登录身份与固定 actor_id 不一致")
        if other_login.get("id") != other["actor_id"]:
            raise AssertionError("other 登录身份与固定 actor_id 不一致")
        checks.append("login_identity_matches_fixed_inputs")

        definition = goal_definition("查询本人最近五条订单，保存有来源的查询结果。")
        created = require_success(request(owner, "POST", "/api/cluster/goals", definition),
                                  "创建目标")
        goal_id = created["goal_id"]
        test_resource_ids["goal_ids"].append(goal_id)
        if created.get("actor_id") != customer["actor_id"]:
            raise AssertionError("创建目标身份没有来自当前登录会话")
        checks.append("goal_creation_uses_session_identity")

        spoofed = request(owner, "POST", "/api/cluster/goals", {
            **definition, "actor_id": other["actor_id"],
        })
        require_status(spoofed, 422, "伪造 actor_id")
        checks.append("goal_body_actor_id_rejected")

        listed = require_success(request(owner, "GET", "/api/cluster/goals"), "查询目标列表")
        if goal_id not in {item["goal_id"] for item in listed}:
            raise AssertionError("目标列表没有包含当前账号创建的目标")
        shown = require_success(request(owner, "GET", f"/api/cluster/goals/{goal_id}"), "读取目标")
        if shown["actor_id"] != customer["actor_id"] or shown["revision"] != 1:
            raise AssertionError("目标详情身份或初始版本不正确")
        checks.append("goal_list_and_detail")

        updated_definition = {**definition, "objective": "查询本人最近五条订单并保留来源记录。"}
        updated = require_success(request(owner, "PUT", f"/api/cluster/goals/{goal_id}", {
            "expected_revision": 1, "definition": updated_definition,
        }), "修改目标")
        if updated["revision"] != 2 or updated["objective"] != updated_definition["objective"]:
            raise AssertionError("目标版本更新结果不正确")
        checks.append("goal_update_revision")

        if request(other_client, "GET", f"/api/cluster/goals/{goal_id}").status_code != 403:
            raise AssertionError("其他账号读取目标没有返回 403")
        checks.append("cross_account_goal_access_denied")

        paused = require_success(request(owner, "POST", f"/api/cluster/goals/{goal_id}/control/pause"),
                                 "暂停目标")
        if paused.get("state") != "paused":
            raise AssertionError("目标暂停状态不正确")
        resumed = require_success(request(owner, "POST", f"/api/cluster/goals/{goal_id}/control/resume"),
                                  "恢复目标")
        if resumed.get("state") != "active":
            raise AssertionError("目标恢复状态不正确")
        checks.append("goal_pause_resume")

        event_body = {"goal_id": goal_id, "event_id": event_id,
                      "payload": {"source": "clu02-http", "sequence": 1}}
        first_run = require_success(request(owner, "POST", "/api/cluster/events", event_body), "发送事件")
        repeated_run = require_success(request(owner, "POST", "/api/cluster/events", event_body),
                                       "重复发送事件")
        if first_run["run_id"] != repeated_run["run_id"]:
            raise AssertionError("重复事件没有复用原运行")
        if first_run.get("status") != "queued":
            raise AssertionError("autorun=false 下事件运行没有保持 queued")
        conflict = request(owner, "POST", "/api/cluster/events", {
            **event_body, "payload": {"source": "clu02-http", "sequence": 2},
        })
        require_status(conflict, 409, "相同编号的不同事件内容")
        run_id_value = first_run["run_id"]
        test_resource_ids["run_ids"].append(run_id_value)
        run_detail = require_success(request(owner, "GET", f"/api/cluster/runs/{run_id_value}"), "读取运行")
        run_history = require_success(request(owner, "GET",
                                              f"/api/cluster/runs/{run_id_value}/history"), "读取运行历史")
        if run_detail["goal_id"] != goal_id or not isinstance(run_history, list):
            raise AssertionError("运行详情或历史关联不正确")
        if request(other_client, "GET", f"/api/cluster/runs/{run_id_value}").status_code != 403:
            raise AssertionError("其他账号读取运行没有返回 403")
        checks.append("event_deduplication_conflict_and_run_reads")
        checks.append("cross_account_run_access_denied")

        schedule_body = {
            "goal_id": goal_id,
            "interval_seconds": 3600,
            "timezone": "Asia/Shanghai",
            "enabled": False,
        }
        schedule = require_success(request(owner, "POST", "/api/cluster/schedules", schedule_body),
                                   "创建日程")
        schedule_id = schedule["schedule_id"]
        if schedule.get("actor_id") != customer["actor_id"] or schedule["enabled"] is not False:
            raise AssertionError("日程创建身份或启用状态不正确")
        schedules = require_success(request(owner, "GET", "/api/cluster/schedules"), "查询日程")
        if schedule_id not in {item["schedule_id"] for item in schedules}:
            raise AssertionError("日程列表没有包含新建日程")
        schedule_update = {**schedule, "enabled": True}
        saved_schedule = require_success(request(owner, "PUT",
                                                 f"/api/cluster/schedules/{schedule_id}",
                                                 schedule_update), "修改日程")
        if saved_schedule["enabled"] is not True:
            raise AssertionError("日程 HTTP 修改结果不正确")
        checks.append("schedule_http_create_list_update")

        cli_config_path = output_dir / "cli-customer-config.json"
        save_json(cli_config_path, {
            "base_url": str(base_url).rstrip("/"),
            "username": customer["username"],
            "password_file": str(password_path_for(customer).resolve(strict=True)),
            "expected_actor_id": customer["actor_id"],
        })
        cli_config_path.chmod(0o600)
        cli_definition_path = output_dir / "cli-goal.json"
        cli_definition = goal_definition("CLI 查询本人最近五条订单并保留来源记录。")
        save_json(cli_definition_path, cli_definition)
        cli_config = ["--config", cli_config_path]
        cli_created = run_cli([*cli_config, "goals", "create", "--file", cli_definition_path],
                              output_dir, secrets, cli_invocations)
        cli_goal_id = cli_created["goal_id"]
        test_resource_ids["goal_ids"].append(cli_goal_id)
        if cli_created.get("actor_id") != customer["actor_id"]:
            raise AssertionError("CLI 创建目标的身份与登录会话不一致")
        cli_list = run_cli([*cli_config, "goals", "list"], output_dir, secrets, cli_invocations)
        if cli_goal_id not in {item["goal_id"] for item in cli_list}:
            raise AssertionError("CLI 目标列表没有包含 CLI 创建的目标")
        updated_cli_definition_path = output_dir / "cli-goal-updated.json"
        updated_cli_definition = {
            **cli_definition, "objective": "CLI 修改后的本人订单查询目标。",
        }
        save_json(updated_cli_definition_path, updated_cli_definition)
        cli_updated = run_cli([
            *cli_config, "goals", "update", cli_goal_id, "--file", updated_cli_definition_path,
            "--expected-revision", "1",
        ], output_dir, secrets, cli_invocations)
        if cli_updated.get("revision") != 2:
            raise AssertionError("CLI 目标版本修改结果不正确")
        cli_shown = run_cli([*cli_config, "goals", "show", cli_goal_id],
                            output_dir, secrets, cli_invocations)
        if cli_shown.get("goal_id") != cli_goal_id or cli_shown.get("revision") != 2:
            raise AssertionError("CLI 目标查询编号不正确")
        cli_started = run_cli([
            *cli_config, "goals", "start", cli_goal_id, "--event-id", f"{run_id}-cli-manual",
        ], output_dir, secrets, cli_invocations)
        if cli_started.get("status") != "queued":
            raise AssertionError("autorun=false 下 CLI 启动目标没有保持 queued")
        cli_started_run_id = cli_started["run_id"]
        test_resource_ids["run_ids"].append(cli_started_run_id)
        cli_payload_path = output_dir / "cli-event-payload.json"
        save_json(cli_payload_path, {"source": "clu02-cli-http", "sequence": 1})
        cli_event = run_cli([
            *cli_config, "events", "emit", "--goal-id", cli_goal_id,
            "--event-id", f"{run_id}-cli-event", "--payload-file", cli_payload_path,
        ], output_dir, secrets, cli_invocations)
        if cli_event.get("status") != "queued":
            raise AssertionError("autorun=false 下 CLI 事件没有保持 queued")
        cli_event_run_id = cli_event["run_id"]
        test_resource_ids["run_ids"].append(cli_event_run_id)
        cli_run = run_cli([*cli_config, "runs", "show", cli_started_run_id],
                          output_dir, secrets, cli_invocations)
        cli_history = run_cli([*cli_config, "runs", "history", cli_started_run_id],
                              output_dir, secrets, cli_invocations)
        if cli_run.get("goal_id") != cli_goal_id or not isinstance(cli_history, list):
            raise AssertionError("CLI 运行详情或历史查询结果不正确")
        cli_schedule_list = run_cli([*cli_config, "schedules", "list"],
                                    output_dir, secrets, cli_invocations)
        if schedule_id not in {item["schedule_id"] for item in cli_schedule_list}:
            raise AssertionError("CLI 日程列表没有包含 HTTP 创建的日程")
        cli_schedule = run_cli([*cli_config, "schedules", "update", schedule_id, "--disabled"],
                               output_dir, secrets, cli_invocations)
        if cli_schedule.get("enabled") is not False:
            raise AssertionError("CLI 日程状态更新结果不正确")
        cli_controls = []
        for action, expected_state in (("pause", "paused"), ("resume", "active"),
                                       ("cancel", "cancelled")):
            result = run_cli([*cli_config, "goals", action, cli_goal_id],
                             output_dir, secrets, cli_invocations)
            if result.get("state") != expected_state:
                raise AssertionError(f"CLI 目标 {action} 状态不正确")
            cli_controls.append(action)
        final_cli_run = run_cli([*cli_config, "runs", "show", cli_started_run_id],
                                output_dir, secrets, cli_invocations)
        final_cli_event_run = run_cli([*cli_config, "runs", "show", cli_event_run_id],
                                      output_dir, secrets, cli_invocations)
        if final_cli_run.get("status") != "cancelled" or final_cli_event_run.get("status") != "cancelled":
            raise AssertionError("CLI 取消目标后没有取消活动运行")
        checks.append("cli_goal_create_update_list_show_start_pause_resume_cancel")
        checks.append("cli_event_emit_and_run_show_history")
        checks.append("cli_schedule_list_update")

        mismatch_config_path = output_dir / "cli-mismatch-config.json"
        save_json(mismatch_config_path, {
            "base_url": str(base_url).rstrip("/"),
            "username": customer["username"],
            "password_file": str(password_path_for(customer).resolve(strict=True)),
            "expected_actor_id": other["actor_id"],
        })
        mismatch_config_path.chmod(0o600)
        mismatch = subprocess.run(
            [sys.executable, "-m", "agent_cluster", "--config", str(mismatch_config_path),
             "goals", "list"],
            cwd=ROOT / "agent-cluster",
            env={**os.environ, "TMPDIR": str(output_dir / "tmp")},
            capture_output=True, text=True, check=False,
        )
        if any(secret and (secret in mismatch.stdout or secret in mismatch.stderr) for secret in secrets):
            raise AssertionError("CLI 身份拒绝输出包含密码")
        mismatch_stdout = redact(mismatch.stdout, secrets)
        mismatch_stderr = redact(mismatch.stderr, secrets)
        (output_dir / "cli-identity-mismatch.stdout.txt").write_text(mismatch_stdout, encoding="utf-8")
        (output_dir / "cli-identity-mismatch.stderr.txt").write_text(mismatch_stderr, encoding="utf-8")
        cli_invocations.append({"arguments": ["--config", str(mismatch_config_path), "goals", "list"],
                                "return_code": mismatch.returncode,
                                "stdout_file": "cli-identity-mismatch.stdout.txt",
                                "stderr_file": "cli-identity-mismatch.stderr.txt"})
        if mismatch.returncode != 2 or "expected_actor_id" not in mismatch_stderr:
            raise AssertionError("CLI 没有拒绝不匹配的 expected_actor_id")
        checks.append("cli_expected_actor_id_mismatch_rejected")

        cancelled = require_success(request(owner, "POST",
                                            f"/api/cluster/goals/{goal_id}/control/cancel"), "取消目标")
        if cancelled.get("state") != "cancelled":
            raise AssertionError("目标取消状态不正确")
        final_goal = require_success(request(owner, "GET", f"/api/cluster/goals/{goal_id}"), "读取取消目标")
        if final_goal.get("state") != "cancelled":
            raise AssertionError("取消目标没有持久保存取消状态")
        checks.append("goal_cancel_persisted")

        for client in (owner, other_client):
            require_success(request(client, "POST", "/api/logout"), "退出登录")
        if request(owner, "GET", "/api/cluster/goals").status_code != 401:
            raise AssertionError("退出登录后目标接口仍可访问")
        checks.append("logout_revokes_session_access")

    after = usage_snapshot()
    report["provider_requests_and_budget"]["after"] = after
    changes = usage_changes(before, after, set(test_resource_ids["goal_ids"]),
                            set(test_resource_ids["run_ids"]))
    report["provider_requests_and_budget"]["changes"] = changes
    if changes["test_scoped_changes"]:
        raise AssertionError("HTTP 与 CLI 检查创建的目标或运行产生了模型请求变化")
    checks.append("test_goal_and_run_provider_requests_unchanged")
    return {
        "status": "passed",
        "run_id": run_id,
        "entrypoint": str(base_url),
        "cli_python": sys.executable,
        "service_pids": pids,
        "cluster_config": config,
        "inputs_sha256": digest(args.inputs),
        "source_sha256": report["source_sha256"],
        "account_actor_ids": {"customer": customer["actor_id"], "other": other["actor_id"]},
        "checks": checks,
        "http_requests": request_log,
        "created_goal_id": goal_id,
        "event_run_id": run_id_value,
        "cli_goal_id": cli_goal_id,
        "cli_started_run_id": cli_started_run_id,
        "cli_event_run_id": cli_event_run_id,
        "cli_controls": cli_controls,
        "cli_invocations": cli_invocations,
        "schedule_id": schedule_id,
        "provider_requests_and_budget": {"before": before, "after": after,
                                          "changes": changes},
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="通过真实 HTTP 与 CLI 检查 Agent 集群接口")
    parser.add_argument("--inputs", type=Path, default=INPUTS_DEFAULT)
    parser.add_argument("--base-url", default=BASE_URL_DEFAULT)
    parser.add_argument("--cluster-config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)
    run_id = "clu02-http-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
    evidence_root = ROOT / ".scratch/evidence"
    output_dir = args.output_dir or evidence_root / run_id
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_dir = output_dir.resolve()
    if not output_dir.is_relative_to(evidence_root.resolve()) or not output_dir.name.startswith("clu02-http-"):
        parser.error("--output-dir 必须位于 .scratch/evidence/clu02-http-* 下")
    output_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    (output_dir / "tmp").mkdir(mode=0o700)
    report_path = output_dir / "report.json"
    report = {"run_id": run_id, "status": "failed", "output_dir": str(output_dir),
              "base_url": args.base_url,
              "inputs": str(args.inputs.resolve()),
              "cluster_config_path": str(args.cluster_config.resolve()),
              "checks": [], "http_requests": [], "cli_invocations": []}
    try:
        report.update(run_checks(args, output_dir, run_id, report))
    except (AssertionError, KeyError, OSError, RuntimeError, SQLAlchemyError,
            TypeError, ValueError, httpx.HTTPError, subprocess.SubprocessError) as error:
        report["status"] = "failed"
        report["script_return_code"] = 1
        report["error"] = {"type": type(error).__name__,
                            "message": redact(str(error), SENSITIVE_VALUES)}
        if "before" in report.get("provider_requests_and_budget", {}):
            try:
                after = usage_snapshot()
                report["provider_requests_and_budget"]["after"] = after
                resources = report.get("test_resource_ids", {})
                report["provider_requests_and_budget"]["changes"] = usage_changes(
                    report["provider_requests_and_budget"]["before"], after,
                    set(resources.get("goal_ids", [])), set(resources.get("run_ids", [])))
            except (OSError, RuntimeError, SQLAlchemyError, TypeError, ValueError) as snapshot_error:
                report["provider_requests_and_budget"]["snapshot_error_type"] = (
                    type(snapshot_error).__name__)
        save_json(report_path, report)
        print(json.dumps({"status": "failed", "run_id": run_id,
                          "report": str(report_path), "error_type": type(error).__name__},
                         ensure_ascii=False), file=sys.stderr)
        return 1
    save_json(report_path, report)
    report["script_return_code"] = 0
    save_json(report_path, report)
    print(json.dumps({"status": "passed", "run_id": run_id,
                      "report": str(report_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
