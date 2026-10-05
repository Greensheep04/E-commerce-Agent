import asyncio
import hashlib
import importlib.metadata
import json
import os
import re
import socket
import subprocess
import sys
import time
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

WORKTREE_ROOT = Path(__file__).resolve().parents[3]
HELPER = WORKTREE_ROOT / "agent-hub/tests/reliability/shop_test_admin.py"
INPUTS_DEFAULT = WORKTREE_ROOT / ".scratch/evidence/mcp-live-inputs-20261001-r1/inputs.json"
TASK_PROMPT = "推荐一件红色女士衬衫"


def digest(value):
    return hashlib.sha256(value).hexdigest()


def source_manifest():
    roots = [WORKTREE_ROOT / "agent-hub/src", WORKTREE_ROOT / "packages/integration-protocol/src",
             WORKTREE_ROOT / "packages/integration-host/src", WORKTREE_ROOT / "src/shop"]
    paths = [path for root in roots for path in root.rglob("*.py") if "__pycache__" not in path.parts]
    paths.extend([HELPER, Path(__file__)])
    return {str(path.relative_to(WORKTREE_ROOT)): digest(path.read_bytes()) for path in sorted(set(paths))}


def save_private(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    path.chmod(0o600)


def redact_diagnostic(value, runtime=None):
    secret_values = [
        secret
        for name, secret in os.environ.items()
        if secret and any(marker in name.upper() for marker in (
            "PASSWORD", "SECRET", "TOKEN", "API_KEY", "DATABASE_URL",
        ))
    ]
    if runtime is not None:
        secret_values.extend(
            runtime[name]
            for name in ("database_url", "service_database_url", "session_secret")
            if runtime.get(name)
        )
    for secret in sorted(set(secret_values), key=len, reverse=True):
        value = value.replace(secret, "<redacted>")
    return re.sub(
        r"(?i)\b(?:postgres(?:ql)?(?:\+[a-z0-9_]+)?|https?)://[^\s\"'<>]+",
        "<redacted-url>",
        value,
    )


def error_text(result):
    return "\n".join(item.text for item in result.content if getattr(item, "text", None))


def operation_value(result):
    assert not result.is_error, error_text(result)
    value = result.structured_content
    assert isinstance(value, dict), "MCP调用没有返回structuredContent"
    assert set(value) == {"data", "resources", "status", "run_id", "costs"}
    return value


def expect_error(result, message):
    assert result.is_error, "越权调用意外成功"
    assert message in error_text(result), error_text(result)


def expect_authorized(result):
    if result.is_error:
        assert "DATA_BLOCKED" in error_text(result), error_text(result)
        return {"status": "authorized_data_blocked", "error": error_text(result)}
    value = operation_value(result)
    return {"status": "authorized", "data": value["data"]}


def request_payload(action, payload, request_id=None):
    return {"action": action, "payload": payload,
            "request_id": request_id or str(uuid4()), "upstream": []}


async def invoke(session, module, action, payload, request_id=None):
    return await session.call_tool(f"shop_{module}_agent", request_payload(action, payload, request_id))


@asynccontextmanager
async def connected_session(config_path, stderr_path):
    config_path = Path(config_path).resolve(strict=True)
    stderr_path = Path(stderr_path)
    stderr_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "agent_hub", "--config", str(config_path)],
        env={"PYTHONPATH": os.pathsep.join((
            str(WORKTREE_ROOT / "agent-hub/src"),
            str(WORKTREE_ROOT / "packages/integration-protocol/src"),
        ))},
        cwd=WORKTREE_ROOT / "agent-hub",
    )
    with stderr_path.open("w", encoding="utf-8") as stderr:
        stderr_path.chmod(0o600)
        async with stdio_client(server, errlog=stderr) as streams:
            async with ClientSession(*streams) as session:
                await session.initialize()
                yield session


def _health(url):
    with urlopen(url, timeout=1) as response:
        return json.loads(response.read())


def hub_process_ids(config_path):
    result = subprocess.run(
        ["pgrep", "-f", re.escape(str(config_path))],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 1:
        return []
    if result.returncode != 0:
        raise RuntimeError(f"pgrep检查MCP进程失败，退出码={result.returncode}")
    return [int(value) for value in result.stdout.splitlines()]


async def wait_for_hub_process(config_path, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pids = await asyncio.to_thread(hub_process_ids, config_path)
        if len(pids) == 1:
            return pids[0]
        if len(pids) > 1:
            raise RuntimeError(f"同一MCP配置对应多个进程：{len(pids)}")
        await asyncio.sleep(0.05)
    raise TimeoutError("没有观察到配置对应的MCP服务进程")


async def wait_for_hub_exit(config_path, process_id, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pids = await asyncio.to_thread(hub_process_ids, config_path)
        if not pids:
            return {"pid": process_id, "process_absent_after_stdio_close": True}
        await asyncio.sleep(0.05)
    raise TimeoutError(f"stdio关闭后MCP服务进程仍存在：{process_id}")


async def wait_healthy(process, base_url):
    deadline = time.monotonic() + 30
    last_error = None
    while time.monotonic() < deadline:
        if process.returncode is not None:
            raise RuntimeError(f"隔离后端提前退出，退出码={process.returncode}")
        try:
            if await asyncio.to_thread(_health, base_url + "/health") == {"status": "ok"}:
                return
        except (URLError, TimeoutError, ConnectionError) as error:
            last_error = type(error).__name__
        await asyncio.sleep(0.1)
    raise TimeoutError(f"隔离后端未能就绪，最后错误类型={last_error}")


async def run_admin_helper(original_root, worktree_root, evidence_dir, runtime=None,
                           timeout=120, *arguments, report, report_path):
    python = original_root / ".venv/bin/python"
    command = [str(python), str(HELPER), *arguments]
    env = os.environ.copy()
    paths = [worktree_root / "src", worktree_root / "packages/integration-protocol/src",
             worktree_root / "packages/integration-host/src"]
    env["PYTHONPATH"] = os.pathsep.join(str(path) for path in paths)
    env["TMPDIR"] = str(worktree_root / ".scratch/runtime")
    Path(env["TMPDIR"]).mkdir(parents=True, exist_ok=True, mode=0o700)
    if runtime is not None:
        env["MCP04_DATABASE_URL"] = runtime["database_url"]
    result = await asyncio.to_thread(
        subprocess.run, command, cwd=original_root, env=env, capture_output=True,
        text=True, check=False, timeout=timeout,
    )
    label = arguments[0]
    stdout_path = evidence_dir / f"helper-{label}-{uuid4().hex[:8]}.stdout.txt"
    stdout_text = redact_diagnostic(result.stdout, runtime)
    stdout_path.write_text(stdout_text, encoding="utf-8")
    stdout_path.chmod(0o600)
    command_record = {
        "command": command,
        "exit_code": result.returncode,
        "stdout_path": str(stdout_path.relative_to(WORKTREE_ROOT)),
        "stdout_sha256": digest(stdout_text.encode("utf-8")),
    }
    if runtime is not None:
        command_record["database_url_sha256"] = digest(runtime["database_url"].encode("utf-8"))
    report["helper_commands"].append(command_record)
    save_private(report_path, report)
    if result.returncode != 0:
        stderr_path = evidence_dir / f"helper-{label}-{uuid4().hex[:8]}.stderr.txt"
        stderr_text = redact_diagnostic(result.stderr, runtime)
        stderr_path.write_text(stderr_text, encoding="utf-8")
        stderr_path.chmod(0o600)
        command_record["stderr_path"] = str(stderr_path.relative_to(WORKTREE_ROOT))
        command_record["stderr_sha256"] = digest(stderr_text.encode("utf-8"))
        command_record["stderr_bytes"] = len(stderr_text.encode("utf-8"))
        save_private(report_path, report)
        raise RuntimeError(
            f"数据库辅助命令{label}失败，退出码={result.returncode}，"
            f"安全日志保存在{stderr_path}"
        )
    return json.loads(result.stdout)


async def spawn_api(original_root, worktree_root, runtime, base_url, evidence_dir):
    port = int(base_url.rsplit(":", 1)[1])
    env = {
        "HOME": os.environ["HOME"],
        "PATH": os.environ["PATH"],
        "PYTHONPATH": os.pathsep.join((
            str(worktree_root / "src"),
            str(worktree_root / "packages/integration-protocol/src"),
            str(worktree_root / "packages/integration-host/src"),
        )),
        "TMPDIR": str(worktree_root / ".scratch/runtime"),
        "SHOP_DATABASE_URL": runtime["service_database_url"],
        "SHOP_SESSION_SECRET": runtime["session_secret"],
        "SHOP_DATA_DIR": str(original_root / ".local/data"),
        "SHOP_POSTER_DIR": str(original_root / ".local/poster"),
        "SHOP_HARNESS_TOKEN_LIMITS_FILE": str(original_root / ".local/harness/token-limits.json"),
    }
    Path(env["TMPDIR"]).mkdir(parents=True, exist_ok=True, mode=0o700)
    stdout_path = evidence_dir / "api.stdout.txt"
    stderr_path = evidence_dir / "api.stderr.txt"
    stdout = stdout_path.open("w", encoding="utf-8")
    stderr = stderr_path.open("w", encoding="utf-8")
    stdout_path.chmod(0o600)
    stderr_path.chmod(0o600)
    process = await asyncio.create_subprocess_exec(
        str(original_root / ".venv/bin/python"), "-m", "uvicorn", "shop.app:app",
        "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
        cwd=original_root, env=env, stdout=stdout, stderr=stderr,
    )
    return process, stdout, stderr


def ephemeral_base_url():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    return f"http://127.0.0.1:{port}"


def assessment_payload(scope, employee_ids=None):
    return {"scope": scope, "employee_ids": employee_ids or [],
            "period_start": "2023-07-01", "period_end": "2023-10-01"}


def allowed_employee_ids(access, salespeople, upper_employee_ids):
    if access["level"] == "admin":
        return set(salespeople)
    allowed = {access["employee_id"]}
    if access["level"] == "manager":
        allowed |= set(access["managed_employee_ids"]) & set(salespeople)
        allowed -= set(upper_employee_ids)
        allowed.add(access["employee_id"])
    return allowed


@pytest.mark.asyncio
async def test_real_mcp_identity_concurrency_and_process_closure():
    source_root = Path(os.environ.get(
        "AGENT_HUB_SOURCE_ROOT", "/Users/greensheep/Project/project-shop",
    )).resolve(strict=True)
    inputs_path = Path(os.environ.get("AGENT_HUB_TEST_INPUTS", str(INPUTS_DEFAULT))).resolve(strict=True)
    inputs = json.loads(inputs_path.read_text(encoding="utf-8"))
    run_id = "mcp04-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:8]
    evidence_dir = WORKTREE_ROOT / ".scratch/evidence" / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
    evidence_dir.chmod(0o700)
    base_url = ephemeral_base_url()
    config_dir = evidence_dir / "configs"
    report_path = evidence_dir / "report.json"
    report = {
        "milestone": "MCP-04",
        "check": "real_mcp_identity_concurrency_process_closure",
        "run_id": run_id,
        "status": "running",
        "inputs_sha256": digest(inputs_path.read_bytes()),
        "fixed_harness_prompt_sha256": digest(TASK_PROMPT.encode("utf-8")),
        "base_url": base_url,
        "source_manifest_before": source_manifest(),
        "checks": {},
        "records": {},
        "helper_commands": [],
    }
    save_private(report_path, report)
    api_process = None
    api_stdout = None
    api_stderr = None
    run_stack = None
    cancel_stack = None
    run_request = None
    cancel_request = None
    lock_process = None
    lock_released = False
    runtime = None
    try:
        setup_output = await run_admin_helper(
            source_root, WORKTREE_ROOT, evidence_dir, None, 300,
            "prepare", "--inputs", str(inputs_path), "--run-id", run_id,
            "--base-url", base_url, "--original-root", str(source_root),
            "--worktree-root", str(WORKTREE_ROOT), "--evidence-dir", str(evidence_dir),
            "--config-dir", str(config_dir),
            report=report, report_path=report_path,
        )
        runtime_path = Path(setup_output["runtime_file"]).resolve(strict=True)
        runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
        setup = runtime["report"]
        report["database"] = setup["database"]
        report["sources"] = setup["sources"]
        report["account_access"] = setup["accounts"]
        report["unassigned_account"] = setup["unassigned_account"]
        report["config_sha256"] = {
            name: item["sha256"] for name, item in setup["hub_configs"].items()
        }
        report["checks"]["isolated_schema_and_source_accounts"] = True
        save_private(report_path, report)

        api_process, api_stdout, api_stderr = await spawn_api(
            source_root, WORKTREE_ROOT, runtime, base_url, evidence_dir,
        )
        await wait_healthy(api_process, base_url)
        report["checks"]["isolated_http_service"] = True
        save_private(report_path, report)

        configs = {name: item["path"] for name, item in setup["hub_configs"].items()}
        access = {name: item["assessment_access"] for name, item in setup["accounts"].items()}
        salespeople = set(setup["sources"]["assessment"]["salesperson_ids"])
        upper_ids = {record["employee_id"] for record in access.values()
                     if record is not None and record["level"] in {"manager", "admin"}
                     and record["employee_id"] is not None}

        async with connected_session(configs["customer"], evidence_dir / "customer.stderr.txt") as customer:
            own_orders_result = await invoke(customer, "customer", "orders", {
                "order_number": inputs["order_number"], "limit": 1, "offset": 0,
            })
            own_orders = operation_value(own_orders_result)["data"]["orders"]
            assert any(row["order_number"] == inputs["order_number"] for row in own_orders)
            conversation_id = operation_value(await invoke(
                customer, "customer", "create_conversation", {"title": run_id},
            ))["data"]["id"]
            operation_value(await invoke(customer, "customer", "conversation", {
                "conversation_id": conversation_id,
            }))
            report["checks"]["customer_own_order_and_conversation"] = True
            report["records"]["customer_owner"] = {
                "actor_id": inputs["accounts"]["customer"]["actor_id"],
                "order_number": inputs["order_number"], "conversation_id": conversation_id,
            }
            save_private(report_path, report)

        async with connected_session(configs["other"], evidence_dir / "other.stderr.txt") as other:
            other_order = await invoke(other, "customer", "order", {
                "order_number": inputs["order_number"],
            })
            expect_error(other_order, "当前账号无法查询该订单")
            cross_conversation = await invoke(other, "customer", "conversation", {
                "conversation_id": conversation_id,
            })
            expect_error(cross_conversation, "当前账号无法访问该会话")
            report["checks"]["customer_cross_account_denied"] = True
            report["records"]["customer_denials"] = {
                "other_actor_id": inputs["accounts"]["other"]["actor_id"],
                "denials": 2,
            }
            save_private(report_path, report)

        async with connected_session(configs["customer"], evidence_dir / "customer-harness.stderr.txt") as customer:
            owned_task = operation_value(await invoke(customer, "harness", "create", {
                "prompt": TASK_PROMPT,
            }))["data"]
        async with connected_session(configs["other"], evidence_dir / "other-harness.stderr.txt") as other:
            expect_error(await invoke(other, "harness", "show", {
                "task_id": owned_task["task_id"],
            }), "无权访问该任务")
        async with connected_session(configs["admin"], evidence_dir / "admin-harness.stderr.txt") as admin:
            admin_task = operation_value(await invoke(admin, "harness", "show", {
                "task_id": owned_task["task_id"],
            }))["data"]
        assert admin_task["owner_id"] == inputs["accounts"]["customer"]["actor_id"]
        report["checks"]["harness_owner_and_admin_scope"] = True
        report["records"]["cross_account_task"] = {
            "task_id": owned_task["task_id"], "owner_id": admin_task["owner_id"],
            "other_account_denied": True, "administrator_read": True,
        }
        save_private(report_path, report)

        employee_id = access["assessment"]["employee_id"]
        async with connected_session(configs["assessment"], evidence_dir / "assessment.stderr.txt") as employee:
            employee_self = await invoke(employee, "assessment", "metrics", assessment_payload("self"))
            report["records"]["employee_self_query"] = expect_authorized(employee_self)
            employee_peer = await invoke(employee, "assessment", "metrics", assessment_payload(
                "employees", [sorted(salespeople - {employee_id})[0]],
            ))
            expect_error(employee_peer, "查询员工不在当前授权范围")

        manager_team_members = {}
        for manager_name in ("assessment-manager", "assessment-manager2"):
            manager_access = access[manager_name]
            assert manager_access["level"] == "manager"
            allowed = allowed_employee_ids(manager_access, salespeople, upper_ids)
            team = allowed - {manager_access["employee_id"]}
            assert team, f"{manager_name}没有固定来源中的有效团队成员"
            manager_team_members[manager_name] = sorted(team)
            async with connected_session(configs[manager_name], evidence_dir / f"{manager_name}.stderr.txt") as manager:
                team_query = await invoke(manager, "assessment", "metrics", assessment_payload(
                    "employees", [sorted(team)[0]],
                ))
                report["records"][f"{manager_name}_team_query"] = expect_authorized(team_query)
                peer_id = access["assessment-manager2" if manager_name == "assessment-manager"
                                 else "assessment-manager"]["employee_id"]
                peer_query = await invoke(manager, "assessment", "metrics", assessment_payload(
                    "employees", [peer_id],
                ))
                expect_error(peer_query, "查询员工不在当前授权范围")

        async with connected_session(configs["assessment-admin"], evidence_dir / "assessment-admin.stderr.txt") as admin:
            organization_query = await invoke(admin, "assessment", "metrics", assessment_payload("organization"))
            report["records"]["administrator_organization_query"] = expect_authorized(organization_query)
        async with connected_session(configs["assessment-unassigned"],
                                     evidence_dir / "assessment-unassigned.stderr.txt") as unassigned:
            expect_error(await invoke(unassigned, "assessment", "metrics", assessment_payload("self")),
                         "当前账号尚未绑定考核权限")
        report["checks"]["assessment_employee_manager_admin_and_unassigned_scope"] = True
        report["records"]["assessment_team_members"] = manager_team_members
        report["records"]["assessment_unassigned_actor_id"] = setup["unassigned_account"]["actor_id"]
        save_private(report_path, report)

        async with connected_session(configs["assessment"], evidence_dir / "assessment-create.stderr.txt") as employee:
            employee_run = operation_value(await invoke(employee, "assessment", "create", {
                "prompt": "查询本人已授权的销售考核范围",
            }))
        employee_run_id = employee_run["resources"][0]["resource_id"]
        employee_team_manager = next(
            name for name in ("assessment-manager", "assessment-manager2")
            if employee_id in allowed_employee_ids(access[name], salespeople, upper_ids)
        )
        peer_manager = "assessment-manager2" if employee_team_manager == "assessment-manager" else "assessment-manager"
        async with connected_session(configs[employee_team_manager],
                                     evidence_dir / f"{employee_team_manager}-assessment.stderr.txt") as manager:
            operation_value(await invoke(manager, "assessment", "show", {"run_id": employee_run_id}))
        async with connected_session(configs[peer_manager], evidence_dir / "assessment-peer.stderr.txt") as manager:
            expect_error(await invoke(manager, "assessment", "show", {"run_id": employee_run_id}),
                         "任务包含当前权限范围以外的员工资料")
        async with connected_session(configs["assessment-admin"], evidence_dir / "assessment-admin-run.stderr.txt") as admin:
            operation_value(await invoke(admin, "assessment", "show", {"run_id": employee_run_id}))
        report["checks"]["assessment_task_scope_and_hierarchy"] = True
        report["records"]["assessment_run"] = {
            "resource_id": employee_run_id, "owner_actor_id": inputs["accounts"]["assessment"]["actor_id"],
            "assigned_manager": employee_team_manager, "peer_manager_denied": peer_manager,
            "administrator_read": True,
        }
        save_private(report_path, report)

        async with connected_session(configs["admin"], evidence_dir / "run-admin.stderr.txt") as admin:
            lifecycle_task = operation_value(await invoke(admin, "harness", "create", {
                "prompt": TASK_PROMPT,
            }))["data"]
        task_id = lifecycle_task["task_id"]
        before = await run_admin_helper(
            source_root, WORKTREE_ROOT, evidence_dir, runtime, 60,
            "state", "--task-id", task_id,
            report=report, report_path=report_path,
        )
        assert before["task"]["status"] == "created"
        report["records"]["lifecycle_task_id"] = task_id

        reserved = await run_admin_helper(
            source_root, WORKTREE_ROOT, evidence_dir, runtime, 120,
            "prepare-request", "--task-id", task_id,
            "--original-root", str(source_root),
            report=report, report_path=report_path,
        )
        request_id = reserved["request_id"]
        lock_python = source_root / ".venv/bin/python"
        lock_process = await asyncio.create_subprocess_exec(
            str(lock_python), str(HELPER), "lock-request", "--request-id", request_id,
            cwd=source_root, env={**os.environ, "PYTHONPATH": os.pathsep.join((
                str(WORKTREE_ROOT / "src"), str(WORKTREE_ROOT / "packages/integration-protocol/src"),
                str(WORKTREE_ROOT / "packages/integration-host/src"),
            )), "MCP04_DATABASE_URL": runtime["database_url"],
                "TMPDIR": str(WORKTREE_ROOT / ".scratch/runtime")},
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        lock_line = await asyncio.wait_for(lock_process.stdout.readline(), timeout=10)
        lock_info = json.loads(lock_line)
        assert lock_info["lock_observed"] is True
        blocker_pid = lock_info["blocker_pid"]

        run_stack = AsyncExitStack()
        await run_stack.__aenter__()
        run_session = await run_stack.enter_async_context(connected_session(
            configs["admin"], evidence_dir / "running-hub.stderr.txt",
        ))
        hub_pid = await wait_for_hub_process(configs["admin"])
        report["checks"]["stdio_hub_process_observed"] = True
        report["records"]["stdio_hub_process"] = {"pid": hub_pid}
        save_private(report_path, report)
        run_request_id = str(uuid4())
        run_request = asyncio.create_task(invoke(
            run_session, "harness", "run", {"task_id": task_id}, run_request_id,
        ))
        running_state = await run_admin_helper(
            source_root, WORKTREE_ROOT, evidence_dir, runtime, 45,
            "wait-running", "--task-id", task_id,
            "--application-name", setup["service_application_name"],
            "--blocker-pid", str(blocker_pid),
            report=report, report_path=report_path,
        )
        active_wait = running_state["observed_provider_request_lock_wait"]
        assert len(active_wait) == 1
        assert active_wait[0]["wait_event_type"] == "Lock"
        assert "provider_requests" in active_wait[0]["query"].lower()
        assert running_state["provider_requests"][0]["id"] == request_id
        assert running_state["provider_requests"][0]["status"] == "reserved"

        concurrent_show = operation_value(await asyncio.wait_for(
            invoke(run_session, "harness", "show", {"task_id": task_id}), timeout=10,
        ))["data"]
        assert concurrent_show["status"] == "running"
        assert run_request.done() is False
        report["checks"]["concurrent_query_while_real_run_waits_on_provider_row"] = True
        report["records"]["running_lock_wait"] = running_state
        report["records"]["concurrent_task_status"] = concurrent_show["status"]
        report["records"]["concurrent_query_connection"] = "same_stdio_session"
        report["records"]["provider_request"] = reserved
        save_private(report_path, report)

        run_request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_request
        sdk_version = importlib.metadata.version("mcp")
        assert sdk_version == "2.2.0"
        report["checks"]["stdio_call_cancellation_notification"] = True
        report["records"]["stdio_call_cancellation"] = {
            "business_request_id": run_request_id,
            "client_sdk": "mcp",
            "client_sdk_version": sdk_version,
            "notification_method": "notifications/cancelled",
        }
        save_private(report_path, report)
        await run_stack.aclose()
        run_stack = None
        report["records"]["stdio_process_exit"] = await wait_for_hub_exit(
            configs["admin"], hub_pid,
        )
        report["checks"]["stdio_close_ended_hub_process"] = True
        save_private(report_path, report)
        after_process_close = await run_admin_helper(
            source_root, WORKTREE_ROOT, evidence_dir, runtime, 60,
            "state", "--task-id", task_id,
            report=report, report_path=report_path,
        )
        assert after_process_close["task"]["status"] == "running"
        assert after_process_close["provider_requests"][0]["status"] == "reserved"
        assert after_process_close["provider_requests"][0]["response_body_present"] is False
        async with connected_session(configs["admin"], evidence_dir / "restart-query.stderr.txt") as restarted:
            recovered = operation_value(await invoke(restarted, "harness", "show", {
                "task_id": task_id,
            }))["data"]
        assert recovered["task_id"] == task_id and recovered["status"] == "running"
        report["checks"]["stdio_process_closed_and_original_task_read_by_new_process"] = True
        report["records"]["after_stdio_close"] = after_process_close
        report["records"]["restarted_process_query"] = {
            "task_id": recovered["task_id"], "status": recovered["status"],
            "model_calls": recovered["model_calls"],
        }
        save_private(report_path, report)

        cancel_stack = AsyncExitStack()
        await cancel_stack.__aenter__()
        cancel_session = await cancel_stack.enter_async_context(connected_session(
            configs["admin"], evidence_dir / "business-cancel.stderr.txt",
        ))
        cancel_request_id = str(uuid4())
        cancel_request = asyncio.create_task(invoke(
            cancel_session, "harness", "cancel", {"task_id": task_id}, cancel_request_id,
        ))
        cancellation_started = await run_admin_helper(
            source_root, WORKTREE_ROOT, evidence_dir, runtime, 45,
            "wait-cancelled", "--task-id", task_id,
            report=report, report_path=report_path,
        )
        assert cancellation_started["task"]["cancel_requested"] is True
        assert cancellation_started["task"]["status"] == "cancelled"
        assert cancellation_started["provider_requests"][0]["status"] == "reserved"
        assert cancellation_started["provider_requests"][0]["response_body_present"] is False

        lock_process.stdin.write(b"release\n")
        await lock_process.stdin.drain()
        release_line = await asyncio.wait_for(lock_process.stdout.readline(), timeout=10)
        assert json.loads(release_line)["released"] is True
        lock_released = True
        assert await asyncio.wait_for(lock_process.wait(), timeout=10) == 0
        cancellation_result = await asyncio.wait_for(cancel_request, timeout=30)
        cancelled = operation_value(cancellation_result)["data"]
        assert cancelled["status"] == "cancelled"
        await cancel_stack.aclose()
        cancel_stack = None

        final_state = await run_admin_helper(
            source_root, WORKTREE_ROOT, evidence_dir, runtime, 60,
            "state", "--task-id", task_id,
            report=report, report_path=report_path,
        )
        request = final_state["provider_requests"]
        assert len(request) == 1
        assert request[0]["id"] == request_id
        assert request[0]["status"] == "cancelled"
        assert Decimal(request[0]["charged_cny"]) == Decimal("0")
        assert request[0]["response_body_present"] is False
        assert final_state["task"]["cancel_requested"] is True
        assert final_state["task"]["status"] == "cancelled"
        baseline_account = before["project_account"] or {"spent_cny": "0", "reserved_cny": "0"}
        assert Decimal(final_state["project_account"]["spent_cny"]) == Decimal(
            baseline_account["spent_cny"]
        )
        assert Decimal(final_state["project_account"]["reserved_cny"]) == Decimal(
            baseline_account["reserved_cny"]
        )
        report["checks"]["business_cancel_releases_request_without_charge_or_response"] = True
        report["records"]["cancel_requested_before_unlock"] = cancellation_started
        report["records"]["final_task_and_cost_state"] = final_state
        report["records"]["request_ids"] = {
            "run": run_request_id, "cancel": cancel_request_id,
            "provider_request": request_id,
        }
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = {
            "type": type(error).__name__,
            "message": redact_diagnostic(str(error), runtime),
        }
        raise
    finally:
        if lock_process is not None and not lock_released and lock_process.returncode is None:
            lock_process.stdin.write(b"release\n")
            await lock_process.stdin.drain()
            await asyncio.wait_for(lock_process.wait(), timeout=10)
            lock_released = True
        if run_request is not None and not run_request.done():
            run_request.cancel()
            await asyncio.gather(run_request, return_exceptions=True)
        if cancel_request is not None and not cancel_request.done():
            if lock_released:
                await asyncio.gather(cancel_request, return_exceptions=True)
            else:
                cancel_request.cancel()
                await asyncio.gather(cancel_request, return_exceptions=True)
        if run_stack is not None:
            await run_stack.aclose()
        if cancel_stack is not None:
            await cancel_stack.aclose()
        if api_process is not None and api_process.returncode is None:
            api_process.terminate()
            await asyncio.wait_for(api_process.wait(), timeout=15)
        if api_stdout is not None:
            api_stdout.close()
        if api_stderr is not None:
            api_stderr.close()
        report["source_manifest_after"] = source_manifest()
        report["source_unchanged"] = report["source_manifest_after"] == report["source_manifest_before"]
        assert report["source_unchanged"]
        save_private(report_path, report)
