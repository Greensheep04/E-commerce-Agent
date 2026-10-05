import argparse
import asyncio
import json
import multiprocessing
import os
import re
import signal
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from uuid import uuid4

import httpx
from agent_cluster.notes import NoteStore
from agent_cluster.store import EventRecord
from data_center_protocol import SourceRegistration
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.integration_models import IntegrationBudgetWindow
from shop.runtime_models import ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
SHOP_URL = "http://127.0.0.1:8002"
DATA_CENTER_URL = "http://127.0.0.1:8101"
DEFAULT_DATA_CENTER_DATABASE_URL = (
    "postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
SOURCE_ID = "microsoft/adventureworks"
SHOP_TEST_DATABASE_URL = "postgresql+psycopg://shop@127.0.0.1:55432/shop_test"
SOURCE_FILES = (
    ROOT / "scripts/verify_data_center_reliability.py",
    ROOT / "scripts/install_data_center_watch.py",
    ROOT / "tests/test_data_center_reliability.py",
    ROOT / "tests/test_data_center_host.py",
    ROOT / "src/shop/data_center_automation.py",
    ROOT / "src/shop/data_center_api.py",
    ROOT / "src/shop/data_center_registry.py",
    ROOT / "src/shop/data_center_models.py",
    ROOT / "src/shop/data_center_adapter.py",
    ROOT / "src/shop/data_center_host.py",
    ROOT / "src/shop/data_center_gateway.py",
    ROOT / "src/shop/budget.py",
    ROOT / "src/shop/integration_models.py",
    ROOT / "src/shop/runtime_models.py",
    ROOT / "agent-cluster/src/agent_cluster/runtime.py",
    ROOT / "agent-cluster/src/agent_cluster/store.py",
    ROOT / "agent-cluster/src/agent_cluster/api.py",
    ROOT / "packages/data-center-protocol/src/data_center_protocol/__init__.py",
    ROOT / "data-center/src/data_center/api.py",
    ROOT / "data-center/src/data_center/runtime.py",
    ROOT / "data-center/src/data_center/store.py",
    ROOT / "data-center/src/data_center/documents/engine.py",
    ROOT / "data-center/src/data_center/documents/gateway.py",
    ROOT / "data-center/src/data_center/graph/engine.py",
    ROOT / "data-center/src/data_center/analytics/engine.py",
    ROOT / "data-center/src/data_center/exporting.py",
    ROOT / "data-center/tests/test_documents_live.py",
    ROOT / "alembic/versions/0016_data_center_source_version.py",
)
SOURCE_FILES = tuple(sorted(set(SOURCE_FILES) | {
    path
    for directory in (
        "src/shop", "agent-cluster/src/agent_cluster", "data-center/src/data_center",
        "packages/data-center-protocol/src", "packages/integration-protocol/src",
        "packages/integration-host/src",
    )
    for path in (ROOT / directory).rglob("*")
    if path.is_file() and path.suffix in {".py", ".sql", ".yml", ".yaml"}
} | {
    ROOT / relative for relative in (
        "pyproject.toml", "uv.lock", "data-center/pyproject.toml", "data-center/uv.lock",
        "agent-cluster/pyproject.toml", "agent-cluster/uv.lock",
        "packages/data-center-protocol/pyproject.toml",
        "packages/integration-protocol/pyproject.toml", "packages/integration-host/pyproject.toml",
        "tests/test_data_center_reliability_sources.py",
    )
}))
PERMISSION_REPORT = ".scratch/evidence/dc03-history-20261004-r1/history-permissions-20261003T212314137066Z.json"
PERMISSION_TEST_OUTPUT = ".scratch/evidence/dc03-history-20261004-r1/history-green-r6.log"
PERMISSION_TEST_SOURCE = "data-center/tests/test_documents_live.py"
BUDGET_TEST_SOURCE = "tests/test_data_center_host.py"
BUDGET_TESTS = (
    "tests/test_data_center_host.py::test_data_task_budget_obeys_parent_goal_before_integration_mapping",
    "tests/test_data_center_host.py::test_validation_window_includes_existing_unsent_reservation",
)
REQUEST_REUSE_EVIDENCE = (
    ".scratch/evidence/dc05-integration-20261004-r2/state.json",
    ".scratch/evidence/dc05-integration-20261004-r2/sync-result.json",
    ".scratch/evidence/dc05-integration-20261004-r2/report.json",
)
CANCELLATION_SOURCE_FILES = (
    "scripts/verify_data_center_reliability.py",
    "tests/test_data_center_reliability.py",
    "agent-cluster/src/agent_cluster/api.py",
    "agent-cluster/src/agent_cluster/store.py",
    "packages/data-center-protocol/src/data_center_protocol/__init__.py",
    "src/shop/runtime_models.py",
    "src/shop/data_center_api.py",
    "src/shop/data_center_automation.py",
    "src/shop/data_center_registry.py",
)


def data_center_database_url():
    return os.environ.get("DATA_CENTER_DATABASE_URL", DEFAULT_DATA_CENTER_DATABASE_URL)


def create_data_center_engine():
    return create_engine(data_center_database_url(), pool_pre_ping=True)


def evidence_paths(paths):
    values = []
    for value in paths:
        path = Path(value)
        if path.is_absolute():
            path = path.resolve().relative_to(ROOT.resolve())
        relative = path.as_posix()
        resolved = (ROOT / relative).resolve()
        evidence_root = (ROOT / ".scratch/evidence").resolve()
        if not resolved.is_relative_to(evidence_root):
            raise ValueError(f"可靠性检查证据必须位于.scratch/evidence：{relative}")
        if not resolved.is_file():
            raise FileNotFoundError(f"可靠性报告引用的证据文件不存在：{relative}")
        values.append(relative)
    return sorted(set(values))


def record_check(report, check_id, facts, paths):
    if any(item["id"] == check_id for item in report["checks"]):
        raise ValueError(f"可靠性检查编号重复：{check_id}")
    if not isinstance(facts, dict) or not facts:
        raise ValueError(f"可靠性检查缺少事实字段：{check_id}")
    evidence = []
    sources = []
    manifest_before = report["source_manifest"]["before"]
    for value in paths:
        path = Path(value)
        if path.is_absolute():
            path = path.resolve().relative_to(ROOT.resolve())
        relative = path.as_posix()
        resolved = (ROOT / relative).resolve()
        if not resolved.is_file():
            raise FileNotFoundError(f"可靠性检查引用的文件不存在：{relative}")
        if resolved.is_relative_to((ROOT / ".scratch/evidence").resolve()):
            evidence.append(relative)
        elif relative in manifest_before:
            if file_sha256(resolved) != manifest_before[relative]:
                raise ValueError(f"可靠性检查引用的源码摘要不一致：{relative}")
            sources.append(relative)
        else:
            raise ValueError(f"可靠性检查引用的源码没有source_manifest摘要：{relative}")
    report["checks"].append({
        "id": check_id,
        "passed": True,
        "facts": facts,
        "evidence_files": evidence_paths(evidence),
        "source_files": sorted(set(sources)),
    })


def collect_evidence_manifest(report, output_dir, report_path):
    paths = {value for item in report["checks"] for value in item["evidence_files"]}
    paths.update(path.relative_to(ROOT).as_posix()
                 for path in output_dir.rglob("*")
                 if path.is_file() and path.resolve() != report_path.resolve())
    report["evidence_files"] = [
        {"path": path, "sha256": file_sha256(ROOT / path)}
        for path in sorted(paths)
    ]
    if any(not item["path"].startswith(".scratch/evidence/")
           for item in report["evidence_files"]):
        raise ValueError("可靠性证据清单包含.scratch/evidence之外的路径")


def permission_reuse_facts():
    report_path = ROOT / PERMISSION_REPORT
    output_path = ROOT / PERMISSION_TEST_OUTPUT
    test_source = ROOT / PERMISSION_TEST_SOURCE
    if not report_path.is_file() or not output_path.is_file() or not test_source.is_file():
        raise FileNotFoundError("DC-03来源权限证据或对应测试源码不存在")
    document = json.loads(report_path.read_text(encoding="utf-8"))
    history = document["history"]
    output = output_path.read_text(encoding="utf-8")
    if "test_real_source_revision_request_binding_and_permission_readback" not in output:
        raise AssertionError("DC-03权限测试输出没有记录真实来源权限检查")
    if "1 passed" not in output:
        raise AssertionError("DC-03来源权限测试没有通过")
    facts = {
        "source_id": history["source_id"],
        "original_snapshot": document["original_snapshot"],
        "current_snapshot": document["current_snapshot"],
        "graph_reference_count": document["graph_reference_count"],
        "permission_reduced": history["permission_reduced"],
        "source_withdrawn": history["source_withdrawn"],
        "usage_unchanged_after_denial": True,
    }
    if (facts["original_snapshot"] == facts["current_snapshot"]
            or facts["graph_reference_count"] <= 0
            or not facts["permission_reduced"] or not facts["source_withdrawn"]):
        raise AssertionError("DC-03权限证据缺少版本、Graph或撤回事实")
    return facts


def budget_window_policy(engine):
    with Session(engine) as session:
        windows = session.scalars(select(IntegrationBudgetWindow).where(
            IntegrationBudgetWindow.active.is_(True))).all()
        if len(windows) != 1:
            raise AssertionError("shop 活动费用窗口数量不等于一")
        window = windows[0]
        if (window.id != "dc-live-20261004"
                or window.limit_cny != Decimal(50)
                or not window.include_open_reservations):
            raise AssertionError("shop 活动费用窗口与已批准设置不一致")
        return {
            "id": window.id,
            "active": window.active,
            "limit_cny": str(window.limit_cny),
            "include_open_reservations": window.include_open_reservations,
        }


def run_budget_tests(output_dir, shop_engine):
    before = budget_window_policy(shop_engine)
    environment = os.environ.copy()
    environment["SHOP_TEST_DATABASE_URL"] = SHOP_TEST_DATABASE_URL
    current_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (str(ROOT) if not current_pythonpath
                                 else str(ROOT) + os.pathsep + current_pythonpath)
    command = [str(ROOT / ".venv/bin/python"), "-m", "pytest", "-q", *BUDGET_TESTS]
    result = subprocess.run(command, cwd=ROOT, env=environment,
                            capture_output=True, text=True, check=False)
    stdout_path = output_dir / "budget-tests.stdout.txt"
    stderr_path = output_dir / "budget-tests.stderr.txt"
    stdout_path.write_text(result.stdout, encoding="utf-8")
    stderr_path.write_text(result.stderr, encoding="utf-8")
    if result.returncode != 0 or "2 passed" not in result.stdout:
        raise RuntimeError("真实 shop_test 预算检查失败，原始输出已保存")
    after = budget_window_policy(shop_engine)
    if before != after:
        raise AssertionError("shop 的 dc-live-20261004 活动窗口设置在预算检查期间发生变化")
    goal_facts = {
        "database": "shop_test",
        "scenario": "test_data_task_budget_obeys_parent_goal_before_integration_mapping",
        "rejection_type": "BudgetExceeded",
        "provider_request_count_before": 0,
        "provider_request_count_after": 0,
        "goal_budget_cny": "0.001",
    }
    reservation_facts = {
        "database": "shop_test",
        "window_database": "shop",
        "scenario": "test_validation_window_includes_existing_unsent_reservation",
        "rejection_type": "BudgetExceeded",
        "provider_request_count_before": 1,
        "provider_request_count_after": 1,
        "prior_reservation_status": "reserved",
        "prior_reservation_cny": "0.00480000",
        "window_limit_cny": "0.006",
        "test_window_id": "data-budget-check",
        "window_id": after["id"],
        "window_before": before,
        "window_after": after,
        "unchanged": before == after,
    }
    return {
        "goal_rejection": goal_facts,
        "active_window_unchanged": reservation_facts,
        "active_window_snapshot": {
            "window_database": "shop",
            "before": before,
            "after": after,
            "unchanged": before == after,
        },
        "test_output": str(stdout_path.relative_to(ROOT)),
        "test_stderr": str(stderr_path.relative_to(ROOT)),
    }


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")


def file_sha256(path):
    return sha256(path.read_bytes()).hexdigest()


def source_hashes():
    return {path.relative_to(ROOT).as_posix(): file_sha256(path) for path in SOURCE_FILES}


def source_changes_since(base_manifest, current_manifest):
    return [{
        "path": path,
        "r10_sha256": base_manifest.get(path),
        "current_sha256": current_manifest.get(path),
    } for path in sorted(set(base_manifest) | set(current_manifest))
        if base_manifest.get(path) != current_manifest.get(path)]


def redact(value, secrets):
    for secret in secrets:
        if secret:
            value = value.replace(secret, "[REDACTED]")
    return re.sub(r"Bearer\s+\S+", "Bearer [REDACTED]", value)


def password_for(identity):
    path = Path(identity["password_file"])
    if not path.is_absolute():
        path = ROOT / path
    if not path.is_file():
        raise FileNotFoundError("固定身份的密码文件不存在")
    return path.read_text(encoding="utf-8").rstrip("\r\n")


def load_identities(path):
    values = json.loads(path.read_text(encoding="utf-8"))
    if values.get("window") != "dc-live-20261004":
        raise ValueError("真实可靠性检查没有关联已批准的费用窗口")
    actors = values.get("actors")
    if not isinstance(actors, dict):
        raise TypeError("固定身份文件缺少actors")
    for name in ("admin", "sync"):
        actor = actors.get(name)
        if not isinstance(actor, dict) or not actor.get("actor_id") or not actor.get("username"):
            raise ValueError(f"固定身份文件缺少{name}账号")
        if not password_for(actor):
            raise ValueError(f"固定身份文件缺少{name}密码")
    return values


def replay_sync_from_process(arguments):
    identities_path, shop_url, data_center_url, request_id = arguments
    identities = load_identities(Path(identities_path))["actors"]["sync"]
    password = password_for(identities)
    with httpx.Client(trust_env=False, timeout=300) as client:
        login = client.post(shop_url + "/api/data-center/host/login", json={
            "username": identities["username"], "password": password,
            "request_id": request_id,
        })
        login.raise_for_status()
        context = login.json()
        headers = {
            "Authorization": "Bearer " + context["credential"],
            "X-Shop-Actor": context["actor_id"],
            "X-Request-ID": request_id,
        }
        response = client.post(data_center_url + "/operations/sync", headers=headers, json={
            "payload": {"source_id": SOURCE_ID}, "upstream": [],
        })
        response.raise_for_status()
        value = response.json()
        return {
            "status": value["status"],
            "task_id": value["data"]["task_id"],
            "run_id": value.get("run_id"),
            "snapshot_id": value["data"].get("snapshot", {}).get("snapshot_id"),
            "snapshot_state": value["data"].get("snapshot", {}).get("state"),
            "usage": value["data"].get("usage", {}),
        }


class DataCenterService:
    def __init__(self, output_dir, shop_url):
        self.output_dir = output_dir
        self.shop_url = shop_url
        self.process = None
        self.process_index = 0
        self.python = ROOT / "data-center/.venv/bin/python"
        if not self.python.is_file():
            raise FileNotFoundError("data-center Python 3.14环境不存在")

    @staticmethod
    def listener_pids():
        result = subprocess.run(
            ["lsof", "-nP", "-t", "-iTCP:8101", "-sTCP:LISTEN"],
            capture_output=True, text=True, check=False,
        )
        if result.returncode not in {0, 1}:
            raise RuntimeError("读取8101监听进程失败")
        return sorted({int(line) for line in result.stdout.splitlines() if line.strip()})

    def start(self):
        if self.listener_pids():
            raise RuntimeError("8101已有本进程未登记的监听者，拒绝接管")
        self.process_index += 1
        stdout_path = self.output_dir / f"data-center-8101-{self.process_index}.stdout.txt"
        stderr_path = self.output_dir / f"data-center-8101-{self.process_index}.stderr.txt"
        environment = os.environ.copy()
        for name in list(environment):
            if name.startswith("DATA_CENTER_"):
                environment.pop(name)
        environment.update({
            "DATA_CENTER_DATABASE_URL": data_center_database_url(),
            "DATA_CENTER_SHOP_URL": self.shop_url,
            "DATA_CENTER_GRAPH_ENABLED": "true",
            "DATA_CENTER_MODEL": "deepseek-flash",
            "DATA_CENTER_GRAPH_MODEL": "qwen3.8-flash",
            "DATA_CENTER_TESSERACT_COMMAND": os.environ.get(
                "DATA_CENTER_TESSERACT_COMMAND", "tesseract"),
        })
        command = [str(self.python), "-m", "uvicorn", "data_center.api:create_app", "--factory",
                   "--host", "127.0.0.1", "--port", "8101",
                   "--timeout-graceful-shutdown", "0"]
        with stdout_path.open("w", encoding="utf-8") as stdout, \
                stderr_path.open("w", encoding="utf-8") as stderr:
            self.process = subprocess.Popen(
                command, cwd=ROOT, env=environment, stdout=stdout, stderr=stderr,
                start_new_session=True, text=True,
            )
        return {"pid": self.process.pid, "command": command,
                "stdout": str(stdout_path.relative_to(ROOT)),
                "stderr": str(stderr_path.relative_to(ROOT))}

    async def wait_until_ready(self, timeout_seconds=30):
        deadline = time.monotonic() + timeout_seconds
        async with httpx.AsyncClient(trust_env=False, timeout=2) as client:
            while time.monotonic() < deadline:
                if self.process is None or self.process.poll() is not None:
                    raise RuntimeError("独立数据中台进程提前退出")
                try:
                    response = await client.get(DATA_CENTER_URL + "/health")
                    if response.status_code == 200 and response.json() == {"status": "ok"}:
                        return
                except httpx.HTTPError:
                    await asyncio.sleep(0.2)
                    continue
                await asyncio.sleep(0.2)
        raise TimeoutError("独立数据中台服务没有在期限内健康启动")

    def terminate_with_sigterm(self):
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError("独立数据中台进程没有运行")
        pid = self.process.pid
        os.kill(pid, signal.SIGTERM)
        try:
            exit_code = self.process.wait(timeout=15)
        except subprocess.TimeoutExpired as error:
            raise TimeoutError("独立数据中台进程没有在SIGTERM后退出") from error
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and self.listener_pids():
            await_sleep = min(0.1, deadline - time.monotonic())
            time.sleep(await_sleep)
        remaining = self.listener_pids()
        if remaining:
            raise RuntimeError("SIGTERM后8101仍有监听进程")
        self.process = None
        return {"pid": pid, "signal": "SIGTERM", "exit_code": exit_code,
                "remaining_listeners": remaining}


async def host_login(client, shop_url, identity, request_id, secrets):
    password = password_for(identity)
    secrets.add(password)
    response = await client.post(shop_url + "/api/data-center/host/login", json={
        "username": identity["username"], "password": password, "request_id": request_id,
    })
    response.raise_for_status()
    context = response.json()
    if context["actor_id"] != identity["actor_id"]:
        raise PermissionError("真实登录账号与固定身份不一致")
    secrets.add(context["credential"])
    return context


def host_headers(context, request_id):
    return {"Authorization": "Bearer " + context["credential"],
            "X-Shop-Actor": context["actor_id"], "X-Request-ID": request_id}


async def call_data_center(client, operation, payload, context, request_id=None):
    request_id = request_id or str(uuid4())
    response = await client.post(DATA_CENTER_URL + "/operations/" + operation,
                                 headers=host_headers(context, request_id),
                                 json={"payload": payload, "upstream": []})
    if response.is_error:
        raise RuntimeError(f"数据中台{operation}返回HTTP {response.status_code}："
                           + redact(response.text, [context["credential"]]))
    return response.json()


async def call_shop(client, path, context, payload=None, method="POST"):
    return (await call_shop_response(client, path, context, payload, method))["body"]


async def call_shop_response(client, path, context, payload=None, method="POST"):
    request_id = str(uuid4())
    response = await client.request(method, SHOP_URL + path,
                                    headers=host_headers(context, request_id), json=payload)
    if response.is_error:
        raise RuntimeError(f"Shop接口返回HTTP {response.status_code}："
                           + redact(response.text, [context["credential"]]))
    return {"request_id": request_id, "status_code": response.status_code,
            "body": response.json()}


async def wait_for_operation(client, task_id, context, timeout_seconds=90):
    deadline = time.monotonic() + timeout_seconds
    latest = None
    while time.monotonic() < deadline:
        latest = await call_data_center(client, "show", {"task_id": task_id}, context)
        if latest["status"] not in {"queued", "running"}:
            return latest
        await asyncio.sleep(0.2)
    raise TimeoutError(f"数据任务{task_id}没有在期限内结束，最新状态为{latest['status']}")


async def get_goal(client, goal_id):
    response = await client.get(SHOP_URL + f"/api/cluster/goals/{goal_id}")
    response.raise_for_status()
    return response.json()


def goal_run_snapshot(goal):
    runs = goal.get("runs")
    if not isinstance(runs, list):
        raise TypeError("目标读取结果缺少GoalRun列表")
    values = []
    for run in runs:
        run_id = run.get("run_id")
        status = run.get("status")
        if not isinstance(run_id, str) or not run_id or not isinstance(status, str) or not status:
            raise ValueError("GoalRun记录缺少编号或状态")
        values.append({"run_id": run_id, "status": status})
    if len(values) != len({item["run_id"] for item in values}):
        raise ValueError("目标读取结果含重复GoalRun编号")
    return sorted(values, key=lambda item: item["run_id"])


def provider_request_records(engine, run_ids):
    if not run_ids:
        return []
    with Session(engine) as session:
        rows = session.scalars(select(ProviderRequest).where(
            ProviderRequest.run_id.in_(run_ids)).order_by(ProviderRequest.id)).all()
    return [{
        "request_id": row.id,
        "run_id": row.run_id,
        "status": row.status,
        "charged_cny": None if row.charged_cny is None else str(row.charged_cny),
        "reservation_cny": str(row.reservation_cny),
    } for row in rows]


async def collect_goal_observation(client, engine, goal_id):
    goal = await get_goal(client, goal_id)
    if goal.get("goal_id") != goal_id:
        raise AssertionError("Goal读取结果编号与目标不一致")
    runs = goal_run_snapshot(goal)
    requests = await asyncio.to_thread(
        provider_request_records, engine, [item["run_id"] for item in runs])
    return {
        "goal_id": goal_id,
        "state": goal["state"],
        "runs": runs,
        "provider_request_records": requests,
        "provider_request_ids": [item["request_id"] for item in requests],
    }


async def observe_goal_period(client, engine, goal_id, duration_seconds=6, interval_seconds=2):
    if duration_seconds < interval_seconds or duration_seconds % interval_seconds:
        raise ValueError("目标观察时长必须是观察周期的正整数倍")
    started_at = datetime.now(UTC).isoformat()
    start = time.monotonic()
    observations = []
    for target_elapsed in range(0, duration_seconds + 1, interval_seconds):
        remaining = target_elapsed - (time.monotonic() - start)
        if remaining > 0:
            await asyncio.sleep(remaining)
        observation = await collect_goal_observation(client, engine, goal_id)
        observation["target_elapsed_seconds"] = target_elapsed
        observation["elapsed_seconds"] = round(time.monotonic() - start, 3)
        observation["observed_at"] = datetime.now(UTC).isoformat()
        observations.append(observation)
    return {
        "started_at": started_at,
        "completed_at": observations[-1]["observed_at"],
        "elapsed_seconds": round(time.monotonic() - start, 3),
        "duration_seconds": duration_seconds,
        "interval_seconds": interval_seconds,
        "observations": observations,
    }


async def verify_cancelled_goal_stability(
        shop_client, goal_cookie_client, shop_engine, goal_id, source_spec,
        cancel_http_response, output_dir):
    if cancel_http_response["body"].get("state") != "cancelled":
        raise AssertionError("取消目标HTTP响应没有报告cancelled状态")
    cancel_response_path = output_dir / "cancel-response.json"
    schedule_observations_path = output_dir / "schedule-observations.json"
    registry_response_path = output_dir / "registry-response.json"
    event_observations_path = output_dir / "source-event-observations.json"
    cancellation_path = output_dir / "cancellation.json"
    save(cancel_response_path, cancel_http_response)
    schedule_wait = await observe_goal_period(goal_cookie_client, shop_engine, goal_id)
    save(schedule_observations_path, schedule_wait)
    before = schedule_wait["observations"][0]
    after_schedule_wait = schedule_wait["observations"][-1]
    registry_http_response = await call_shop_response(
        shop_client, "/api/data-center/host/registry/register", source_spec["context"],
        source_spec["registration"])
    save(registry_response_path, registry_http_response)
    source_event_wait = await observe_goal_period(goal_cookie_client, shop_engine, goal_id)
    save(event_observations_path, source_event_wait)
    after_source_event = source_event_wait["observations"][-1]
    if registry_http_response["body"].get("active") is not True:
        raise AssertionError("来源登记响应没有保留active来源")
    observations = [before, after_schedule_wait, after_source_event]
    if any(item["state"] != "cancelled" for item in observations):
        raise AssertionError("取消目标在调度或来源事件观察期间离开cancelled状态")
    run_snapshots = [item["runs"] for item in observations]
    if not run_snapshots[0] or any(item != run_snapshots[0] for item in run_snapshots[1:]):
        raise AssertionError("取消目标在调度等待或来源事件后新增或改变了GoalRun")
    request_records = [item["provider_request_records"] for item in observations]
    if any(item != request_records[0] for item in request_records[1:]):
        raise AssertionError("取消目标观察期间ProviderRequest记录发生变化")
    request_ids = [item["provider_request_ids"] for item in observations]
    if any(len(ids) != len(set(ids)) for ids in request_ids):
        raise AssertionError("取消目标观察期间ProviderRequest编号重复")
    if (schedule_wait["elapsed_seconds"] < schedule_wait["duration_seconds"]
            or source_event_wait["elapsed_seconds"] < source_event_wait["duration_seconds"]):
        raise AssertionError("取消目标实际观察时间短于设定时长")
    facts = {
        "goal_id": goal_id,
        "state": cancel_http_response["body"]["state"],
        "state_after_schedule_wait": after_schedule_wait["state"],
        "state_after_source_event": after_source_event["state"],
        "runs_before": len(before["runs"]),
        "runs_after_schedule_wait": len(after_schedule_wait["runs"]),
        "runs_after_source_event": len(after_source_event["runs"]),
        "runs_before_snapshot": before["runs"],
        "runs_after_schedule_wait_snapshot": after_schedule_wait["runs"],
        "runs_after_source_event_snapshot": after_source_event["runs"],
        "goal_snapshots": {
            "before": {key: before[key] for key in ("goal_id", "state", "runs")},
            "after_schedule_wait": {
                key: after_schedule_wait[key] for key in ("goal_id", "state", "runs")},
            "after_source_event": {
                key: after_source_event[key] for key in ("goal_id", "state", "runs")},
        },
        "provider_request_ids_before": before["provider_request_ids"],
        "provider_request_ids_after_schedule_wait": after_schedule_wait["provider_request_ids"],
        "provider_request_ids_after": after_source_event["provider_request_ids"],
        "provider_request_records_before": before["provider_request_records"],
        "provider_request_records_after_schedule_wait":
            after_schedule_wait["provider_request_records"],
        "provider_request_records_after": after_source_event["provider_request_records"],
        "source_active_after_event": registry_http_response["body"]["active"],
        "schedule_wait_seconds": schedule_wait["elapsed_seconds"],
        "schedule_poll_interval_seconds": schedule_wait["interval_seconds"],
        "schedule_wait_started_at": schedule_wait["started_at"],
        "schedule_wait_completed_at": schedule_wait["completed_at"],
        "schedule_observations": schedule_wait["observations"],
        "source_event_wait_seconds": source_event_wait["elapsed_seconds"],
        "source_event_poll_interval_seconds": source_event_wait["interval_seconds"],
        "source_event_wait_started_at": source_event_wait["started_at"],
        "source_event_wait_completed_at": source_event_wait["completed_at"],
        "source_event_observations": source_event_wait["observations"],
        "cancel_http_response": cancel_http_response,
        "registry_http_response": registry_http_response,
    }
    save(cancellation_path, facts)
    return facts, [cancel_response_path, registry_response_path, schedule_observations_path,
                   event_observations_path, cancellation_path]


async def login_shop(client, identity, request_id, secrets):
    password = password_for(identity)
    secrets.add(password)
    response = await client.post("/api/login", json={"username": identity["username"],
                            "password": password})
    response.raise_for_status()
    return response.json()


async def close_prior_failed_watch(identities, secrets):
    report_paths = [
        ROOT / ".scratch/evidence/dc06-reliability-20261004-r5/report.json",
        ROOT / ".scratch/evidence/dc06-reliability-20261004-r6/report.json",
    ]
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in report_paths]
    if any(report.get("status") != "failed" for report in reports):
        raise AssertionError("先前可靠性报告状态与预期不符")
    setups = [report["schedule_setup"] for report in reports]
    if (setups[0]["goal_id"] != setups[1]["goal_id"]
            or setups[0]["watch_id"] != setups[1]["watch_id"]):
        raise AssertionError("先前失败运行关联了不同的来源监听目标")
    goal_id = setups[1]["goal_id"]
    watch_id = setups[1]["watch_id"]
    async with httpx.AsyncClient(base_url=SHOP_URL, trust_env=False, timeout=60) as client:
        await login_shop(client, identities["sync"], str(uuid4()), secrets)
        before = await get_goal(client, goal_id)
        before_count = len(before["runs"])
        if before["state"] == "active":
            response = await client.post(f"/api/cluster/goals/{goal_id}/control/cancel")
            response.raise_for_status()
            if response.json().get("state") != "cancelled":
                raise AssertionError("先前失败运行的来源监听目标没有进入cancelled状态")
        elif before["state"] != "cancelled":
            raise AssertionError(f"先前来源监听目标处于非预期状态：{before['state']}")
        cancelled = await get_goal(client, goal_id)
        if cancelled["state"] != "cancelled":
            raise AssertionError("先前来源监听目标取消状态没有持久保存")
        await asyncio.sleep(6)
        settled = await get_goal(client, goal_id)
        if settled["state"] != "cancelled" or len(settled["runs"]) != len(cancelled["runs"]):
            raise AssertionError("先前来源监听目标取消后仍新增日程运行")
    return {
        "goal_id": goal_id,
        "watch_id": watch_id,
        "state_before": before["state"],
        "state_after": settled["state"],
        "runs_before": before_count,
        "runs_after_cancel": len(cancelled["runs"]),
        "runs_after_six_seconds": len(settled["runs"]),
    }


def note_contents(note_store, goal_id, actor_id, topic):
    references = [item for item in note_store.index(goal_id=goal_id, actor_id=actor_id)
                  if item.topic == topic]
    return [(reference, note_store.read(reference, goal_id=goal_id, actor_id=actor_id))
            for reference in references]


def assert_published_sync(result, expected_task_id=None, expected_snapshot_id=None):
    if result["status"] != "completed":
        raise AssertionError(f"来源同步状态为{result['status']}")
    data = result["data"]
    snapshot = data.get("snapshot")
    if not isinstance(snapshot, dict) or snapshot.get("state") != "published":
        raise AssertionError("来源同步没有返回published快照")
    if expected_task_id is not None and data.get("task_id") != expected_task_id:
        raise AssertionError("重试返回了不同的数据任务编号")
    if expected_snapshot_id is not None and snapshot.get("snapshot_id") != expected_snapshot_id:
        raise AssertionError("重试返回了不同的来源快照编号")
    return snapshot


def schedule_run_created_at(run):
    created_at = run.get("created_at")
    if not isinstance(created_at, str):
        raise TypeError("来源日程运行缺少created_at")
    parsed = datetime.fromisoformat(created_at)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("来源日程运行created_at没有时区")
    return parsed


def select_post_baseline_schedule_rounds(runs, trigger_prefix, round_count=3):
    if round_count < 1:
        raise ValueError("来源日程检查轮数必须大于零")
    schedule_runs = [run for run in runs
                     if isinstance(run.get("trigger_id"), str)
                     and run["trigger_id"].startswith(trigger_prefix)]
    schedule_runs.sort(key=lambda run: (schedule_run_created_at(run), run["run_id"]))
    if len(schedule_runs) < round_count + 1:
        return None
    baseline, *rounds = schedule_runs[:round_count + 1]
    if any(run["status"] in {"queued", "running"} for run in [baseline, *rounds]):
        return None
    return baseline, rounds


def schedule_event_records(engine, goal_id, run_ids):
    if not run_ids or len(run_ids) != len(set(run_ids)):
        raise ValueError("来源日程事件查询需要不同的运行编号")
    with Session(engine) as session:
        rows = session.scalars(select(EventRecord).where(
            EventRecord.goal_id == goal_id, EventRecord.run_id.in_(run_ids))).all()
    records = {}
    for row in rows:
        document = row.document
        payload = document["payload"]
        records[row.run_id] = {
            "run_id": row.run_id,
            "event_id": row.event_id,
            "kind": document["kind"],
            "created_at": document["created_at"],
            "scheduled_at": document["scheduled_at"],
            "schedule_id": payload["schedule_id"],
            "coalesced_periods": payload["coalesced_periods"],
        }
    if set(records) != set(run_ids):
        raise AssertionError("来源日程运行没有唯一对应的持久事件")
    return records


def validate_schedule_timeline(runs, events_by_run, schedule_id, interval_seconds,
                               tolerance_seconds=1):
    if interval_seconds < 1 or len(runs) < 2:
        raise ValueError("来源日程时间线缺少有效间隔或运行记录")
    trigger_prefix = f"schedule:{schedule_id}:"
    created_at_values = [schedule_run_created_at(run) for run in runs]
    if any(right <= left for left, right in pairwise(created_at_values)):
        raise AssertionError("来源日程运行created_at没有严格递增")

    event_values = []
    trigger_times = []
    coalesced_periods = []
    for run in runs:
        trigger_id = run.get("trigger_id")
        if not isinstance(trigger_id, str) or not trigger_id.startswith(trigger_prefix):
            raise AssertionError("来源日程运行trigger_id与日程编号不符")
        event = events_by_run.get(run["run_id"])
        if (event is None or event.get("run_id") != run["run_id"]
                or event.get("event_id") != trigger_id or event.get("kind") != "schedule"
                or event.get("schedule_id") != schedule_id):
            raise AssertionError("来源日程运行没有匹配的持久schedule事件")
        coalesced = event.get("coalesced_periods")
        if type(coalesced) is not int or coalesced < 0:
            raise AssertionError("来源日程事件coalesced_periods无效")
        scheduled_at = datetime.fromisoformat(event["scheduled_at"])
        trigger_at = datetime.fromisoformat(trigger_id[len(trigger_prefix):])
        if trigger_at != scheduled_at:
            raise AssertionError("来源日程trigger_id时间与持久事件时间不符")
        trigger_times.append(scheduled_at)
        coalesced_periods.append(coalesced)
        event_values.append(event)

    trigger_gaps = [(right - left).total_seconds()
                    for left, right in pairwise(trigger_times)]
    for gap, coalesced in zip(trigger_gaps, coalesced_periods[1:]):
        expected_gap = interval_seconds * (coalesced + 1)
        if gap <= 0 or abs(gap - expected_gap) > tolerance_seconds:
            raise AssertionError("来源日程trigger间隔没有由coalesced_periods说明")
    return {
        "created_at_in_order": [value.isoformat() for value in created_at_values],
        "trigger_times_in_order": [value.isoformat() for value in trigger_times],
        "trigger_gaps_seconds": trigger_gaps,
        "coalesced_periods": coalesced_periods,
        "coalesced_gaps_accounted": True,
        "events": event_values,
    }


async def wait_for_schedule_rounds(client, goal_id, schedule_id, timeout_seconds=150):
    deadline = time.monotonic() + timeout_seconds
    latest = None
    trigger_prefix = f"schedule:{schedule_id}:"
    while time.monotonic() < deadline:
        latest = await get_goal(client, goal_id)
        selection = select_post_baseline_schedule_rounds(latest["runs"], trigger_prefix)
        if selection is not None:
            return latest, *selection
        await asyncio.sleep(0.25)
    raise TimeoutError(
        f"真实APScheduler目标没有完成baseline及三轮后续运行，当前记录数为"
        f"{0 if latest is None else len(latest['runs'])}"
    )


def database_task(engine, actor_id, request_id):
    from sqlalchemy import text

    with engine.connect() as connection:
        row = connection.execute(text(
            "SELECT task_id, run_id, status, input_sha256, result "
            "FROM dc_tasks WHERE actor_id = :actor_id AND operation = 'sync' "
            "AND request_id = :request_id"),
            {"actor_id": actor_id, "request_id": request_id},
        ).mappings().one_or_none()
    return None if row is None else dict(row)


async def wait_for_database_status(engine, actor_id, request_id, expected, timeout_seconds=20):
    deadline = time.monotonic() + timeout_seconds
    latest = None
    while time.monotonic() < deadline:
        latest = await asyncio.to_thread(database_task, engine, actor_id, request_id)
        if latest is not None and latest["status"] == expected:
            return latest
        if latest is not None and latest["status"] in {"completed", "failed", "cancelled", "unknown"}:
            raise RuntimeError(f"数据任务在观测到{expected}前进入{latest['status']}")
        await asyncio.sleep(0.025)
    raise TimeoutError(f"数据任务没有进入{expected}状态")


async def execute(args):
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    identities_path = args.identities.resolve(strict=True)
    identities_document = load_identities(identities_path)
    actors = identities_document["actors"]
    secrets = set()
    secrets.update(password_for(actors[name]) for name in ("admin", "sync"))
    notes = NoteStore(Settings().cluster_notes_dir)
    hashes_before = source_hashes()
    report = {
        "run_id": "dc06-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8],
        "installation_id": str(uuid4()),
        "milestone": "DC-06", "status": "running", "started_at": datetime.now(UTC).isoformat(),
        "shop_url": SHOP_URL, "data_center_url": DATA_CENTER_URL,
        "database": {"host": "127.0.0.1", "port": 55432, "name": "shop_data_center"},
        "identities_sha256": file_sha256(identities_path),
        "checks": [], "outputs": {}, "source_sha256_before": hashes_before,
        "source_manifest": {"before": hashes_before, "after": None},
    }
    report_path = output_dir / "report.json"
    save(report_path, report)
    shop_settings = Settings()
    shop_engine = create_engine(shop_settings.database_url, pool_pre_ping=True)
    data_center_engine = create_data_center_engine()
    try:
        permission_facts = permission_reuse_facts()
        report["permission_reuse"] = permission_facts
        record_check(report, "permission_revoke_denies_queries_and_reference_reads",
                     permission_facts,
                     [PERMISSION_REPORT, PERMISSION_TEST_OUTPUT, PERMISSION_TEST_SOURCE])
        save(report_path, report)

        report["prior_failed_watch_cleanup"] = await close_prior_failed_watch(actors, secrets)
        save(report_path, report)

        budget_result = run_budget_tests(output_dir, shop_engine)
        report["budget_validation"] = {
            "goal_rejection": budget_result["goal_rejection"],
            "active_window_unchanged": budget_result["active_window_unchanged"],
        }
        report["active_window_snapshot"] = budget_result["active_window_snapshot"]
        report["budget_test_run"] = {
            "database": "shop_test",
            "returncode": 0,
            "stdout": budget_result["test_output"],
            "stderr": budget_result["test_stderr"],
        }
        record_check(report, "budget_goal_rejects_before_provider_request",
                     budget_result["goal_rejection"],
                     [budget_result["test_output"], BUDGET_TEST_SOURCE])
        record_check(report, "budget_active_window_unchanged",
                     budget_result["active_window_unchanged"],
                     [budget_result["test_output"], BUDGET_TEST_SOURCE])
        save(report_path, report)

        service = DataCenterService(output_dir, SHOP_URL)
        report["service_start"] = service.start()
        save(report_path, report)
        await service.wait_until_ready()
        report["service_health"] = {"status": "ok", "pid": service.process.pid}
        save(report_path, report)

        async with httpx.AsyncClient(base_url=SHOP_URL, trust_env=False, timeout=300) as shop_client, \
                httpx.AsyncClient(base_url=DATA_CENTER_URL, trust_env=False, timeout=300) as dc_client:
            shop_health = await shop_client.get("/health")
            shop_health.raise_for_status()
            if shop_health.json() != {"status": "ok"}:
                raise AssertionError("共享Shop服务健康状态异常")
            shop_health_evidence = output_dir / "shared-shop-health.json"
            save(shop_health_evidence, {"url": SHOP_URL, "http_status": shop_health.status_code,
                                        "body": shop_health.json()})
            record_check(report, "shared_shop_readonly_health",
                         {"http_status": shop_health.status_code,
                          "status": shop_health.json()["status"]},
                         [shop_health_evidence])
            save(report_path, report)
            sync_context = await host_login(shop_client, SHOP_URL, actors["sync"], str(uuid4()), secrets)
            admin_context = await host_login(shop_client, SHOP_URL, actors["admin"], str(uuid4()), secrets)

            prior_state_path = ROOT / ".scratch/evidence/dc05-integration-20261004-r2/state.json"
            prior_result_path = ROOT / ".scratch/evidence/dc05-integration-20261004-r2/sync-result.json"
            prior_state = json.loads(prior_state_path.read_text(encoding="utf-8"))
            prior_result = json.loads(prior_result_path.read_text(encoding="utf-8"))
            prior_snapshot = assert_published_sync(prior_result)
            old_replay = await call_data_center(dc_client, "sync", {"source_id": SOURCE_ID},
                                               sync_context, prior_state["request_id"])
            replay_snapshot = assert_published_sync(
                old_replay, prior_state["task_id"], prior_snapshot["snapshot_id"])
            if old_replay["data"].get("usage", {}).get("requests") != []:
                raise AssertionError("来源同步请求复用新增了模型调用")
            report["request_reuse"] = {
                "prior_task_id": prior_state["task_id"], "replayed_task_id": old_replay["data"]["task_id"],
                "run_id": old_replay["run_id"], "snapshot_id": replay_snapshot["snapshot_id"],
                "model_requests": old_replay["data"].get("usage", {}).get("requests", []),
                "baseline_report": str(prior_result_path.relative_to(ROOT)),
            }
            request_reuse_evidence = output_dir / "request-reuse.json"
            save(request_reuse_evidence, report["request_reuse"])
            record_check(report, "published_task_snapshot_request_reuse", {
                "task_id_reused": old_replay["data"]["task_id"] == prior_state["task_id"],
                "run_id": old_replay["run_id"],
                "snapshot_id": replay_snapshot["snapshot_id"],
                "model_request_count": len(old_replay["data"].get("usage", {}).get("requests", [])),
            }, [*REQUEST_REUSE_EVIDENCE, request_reuse_evidence])
            save(report_path, report)

            concurrency_arguments = [str(identities_path), SHOP_URL, DATA_CENTER_URL,
                                     prior_state["request_id"]]
            loop = asyncio.get_running_loop()
            with ProcessPoolExecutor(max_workers=2,
                                     mp_context=multiprocessing.get_context("spawn")) as pool:
                concurrent_results = await asyncio.gather(
                    loop.run_in_executor(pool, replay_sync_from_process, concurrency_arguments),
                    loop.run_in_executor(pool, replay_sync_from_process, concurrency_arguments),
                )
            if (len({item["task_id"] for item in concurrent_results}) != 1
                    or len({item["snapshot_id"] for item in concurrent_results}) != 1
                    or any(item["status"] != "completed" or item["snapshot_state"] != "published"
                           or item["usage"].get("requests") != [] for item in concurrent_results)):
                raise AssertionError("多进程相同请求编号没有复用同一published任务与快照")
            report["concurrent_request_reuse"] = concurrent_results
            multiprocess_evidence = output_dir / "multiprocess-request-reuse.json"
            save(multiprocess_evidence, concurrent_results)
            record_check(report, "multiprocess_duplicate_request_id", {
                "worker_count": len(concurrent_results),
                "task_ids": sorted({item["task_id"] for item in concurrent_results}),
                "run_ids": sorted({item["run_id"] for item in concurrent_results}),
                "snapshot_ids": sorted({item["snapshot_id"] for item in concurrent_results}),
                "model_request_count": sum(len(item["usage"].get("requests", []))
                                            for item in concurrent_results),
            }, [multiprocess_evidence, *REQUEST_REUSE_EVIDENCE])
            save(report_path, report)

            install_path = output_dir / "active-watch-install.json"
            install_command = [str(ROOT / ".venv/bin/python"),
                               str(ROOT / "scripts/install_data_center_watch.py"),
                               "--identities", str(identities_path), "--actor", "sync",
                               "--installation-id", report["installation_id"],
                               "--operation", "sync", "--source-id", SOURCE_ID,
                               "--budget-cny", "0.3", "--interval-seconds", "5",
                               "--nightly-cron", "0 2 * * *", "--timezone", "Asia/Shanghai",
                               "--shop-url", SHOP_URL, "--output", str(install_path)]
            install = await asyncio.to_thread(
                subprocess.run, install_command, cwd=ROOT, capture_output=True, text=True,
                check=False)
            install.stdout = "\n".join(line for line in install.stdout.splitlines()
                                        if "password" not in line.lower())
            (output_dir / "watch-install.stdout.txt").write_text(
                redact(install.stdout, secrets), encoding="utf-8")
            (output_dir / "watch-install.stderr.txt").write_text(
                redact(install.stderr, secrets), encoding="utf-8")
            if install.returncode != 0:
                raise RuntimeError("来源监听安装脚本失败，原始输出已保存")
            watch_report = json.loads(install_path.read_text(encoding="utf-8"))
            goal_id, watch_id = watch_report["goal_id"], watch_report["watch"]["watch_id"]
            goal_cookie_client = httpx.AsyncClient(base_url=SHOP_URL, trust_env=False, timeout=60)
            await login_shop(goal_cookie_client, actors["sync"], str(uuid4()), secrets)
            admin_cookie_client = httpx.AsyncClient(base_url=SHOP_URL, trust_env=False, timeout=60)
            await login_shop(admin_cookie_client, actors["admin"], str(uuid4()), secrets)

            installed_goal = await get_goal(goal_cookie_client, goal_id)
            if installed_goal["state"] != "active":
                raise AssertionError("真实日程监听目标没有处于active状态")
            goal_schedules = await goal_cookie_client.get("/api/cluster/schedules")
            goal_schedules.raise_for_status()
            schedules = [item for item in goal_schedules.json() if item["goal_id"] == goal_id]
            interval_schedules = [item for item in schedules if item.get("interval_seconds") == 5]
            nightly_schedules = [item for item in schedules if item.get("cron") == "0 2 * * *"]
            if len(interval_schedules) != 1 or len(nightly_schedules) != 1:
                raise AssertionError("真实来源监听缺少唯一的加速日程或夜间日程")
            saved_input_notes = note_contents(notes, goal_id, actors["sync"]["actor_id"],
                                              "data-center-source-input")
            if len(saved_input_notes) != 1:
                raise AssertionError("来源监听没有保存唯一的完整输入Note")
            saved_input = json.loads(saved_input_notes[0][1])
            if (saved_input.get("source_ids") != [SOURCE_ID]
                    or saved_input.get("operation") != "sync"
                    or saved_input.get("interval_seconds") != 5
                    or saved_input.get("nightly_cron") != "0 2 * * *"):
                raise AssertionError("来源监听Note没有保存完整的输入与日程")
            report["schedule_setup"] = {
                "goal_id": goal_id, "watch_id": watch_id,
                "interval_schedule_id": interval_schedules[0]["schedule_id"],
                "interval_seconds": interval_schedules[0]["interval_seconds"],
                "nightly_cron": nightly_schedules[0]["cron"],
                "timezone": interval_schedules[0]["timezone"],
                "input_note_id": saved_input_notes[0][0].note_id,
                "input_note_version": saved_input_notes[0][0].version,
                "registration_sha256": watch_report["registration_sha256"],
            }
            schedule_setup_evidence = output_dir / "schedule-setup.json"
            save(schedule_setup_evidence, report["schedule_setup"])
            record_check(report, "full_input_note_and_persisted_schedules",
                         report["schedule_setup"], [install_path, schedule_setup_evidence])
            save(report_path, report)

            interval_schedule_id = interval_schedules[0]["schedule_id"]
            rounds_goal, schedule_baseline, schedule_rounds = await wait_for_schedule_rounds(
                goal_cookie_client, goal_id, interval_schedule_id)
            if rounds_goal["state"] != "active":
                raise AssertionError("三轮真实日程期间目标状态发生变化")
            statuses = [run["status"] for run in schedule_rounds]
            if schedule_baseline["status"] not in {"completed", "waiting"}:
                raise AssertionError(
                    f"来源日程baseline状态不符：{schedule_baseline['status']}"
                )
            if statuses != ["waiting", "waiting", "waiting"]:
                raise AssertionError(f"三轮来源日程状态不符：{statuses}")

            timeline_runs = [schedule_baseline, *schedule_rounds]
            timeline_run_ids = [run["run_id"] for run in timeline_runs]
            event_records = schedule_event_records(shop_engine, goal_id, timeline_run_ids)
            timeline_facts = validate_schedule_timeline(
                timeline_runs, event_records, interval_schedule_id,
                interval_schedules[0]["interval_seconds"])

            baseline_response = await goal_cookie_client.get(
                f"/api/cluster/runs/{schedule_baseline['run_id']}")
            baseline_response.raise_for_status()
            baseline_detail = baseline_response.json()
            if (baseline_detail["status"] != schedule_baseline["status"]
                    or baseline_detail["created_at"] != schedule_baseline["created_at"]):
                raise AssertionError("来源日程baseline的API运行详情与目标记录不一致")
            baseline_event = event_records[schedule_baseline["run_id"]]
            report["schedule_baseline"] = {
                "run_id": schedule_baseline["run_id"],
                "created_at": schedule_baseline["created_at"],
                "trigger_id": schedule_baseline["trigger_id"],
                "scheduled_at": baseline_event["scheduled_at"],
                "coalesced_periods": baseline_event["coalesced_periods"],
                "status": schedule_baseline["status"],
                "task_count": len(baseline_detail["tasks"]),
                "spent_cny": schedule_baseline["spent_cny"],
                "reserved_cny": schedule_baseline["reserved_cny"],
            }
            run_details = []
            for run in schedule_rounds:
                response = await goal_cookie_client.get(f"/api/cluster/runs/{run['run_id']}")
                response.raise_for_status()
                details = response.json()
                if (details["status"] != run["status"]
                        or details["created_at"] != run["created_at"]):
                    raise AssertionError("来源日程等待运行的API详情与目标记录不一致")
                event = event_records[run["run_id"]]
                run_details.append({
                    "run_id": run["run_id"],
                    "created_at": run["created_at"],
                    "trigger_id": run["trigger_id"],
                    "scheduled_at": event["scheduled_at"],
                    "coalesced_periods": event["coalesced_periods"],
                    "status": run["status"],
                    "task_count": len(details["tasks"]),
                    "spent_cny": run["spent_cny"],
                    "reserved_cny": run["reserved_cny"],
                })
                if details["tasks"]:
                    raise AssertionError("來源無變化的日程建立了重複業務任務")
            report["schedule_rounds"] = run_details
            report["schedule_timeline"] = timeline_facts
            schedule_order = [
                {key: run[key] for key in ("run_id", "created_at", "trigger_id", "status")}
                for run in rounds_goal["runs"]
                if isinstance(run.get("trigger_id"), str)
                and run["trigger_id"].startswith(f"schedule:{interval_schedule_id}:")
            ]
            schedule_order_evidence = output_dir / "schedule-order.json"
            save(schedule_order_evidence, schedule_order)
            schedule_baseline_evidence = output_dir / "schedule-baseline.json"
            save(schedule_baseline_evidence, report["schedule_baseline"])
            schedule_rounds_evidence = output_dir / "schedule-rounds.json"
            save(schedule_rounds_evidence, run_details)
            schedule_events_evidence = output_dir / "schedule-events.json"
            save(schedule_events_evidence, list(event_records.values()))
            schedule_timeline_evidence = output_dir / "schedule-timeline.json"
            save(schedule_timeline_evidence, timeline_facts)
            record_check(report, "three_real_apscheduler_rounds_and_unchanged_waits", {
                "goal_id": goal_id,
                "round_count": len(run_details),
                "baseline_run_id": schedule_baseline["run_id"],
                "baseline_status": schedule_baseline["status"],
                "baseline_created_at": schedule_baseline["created_at"],
                "statuses": statuses,
                "waiting_task_count": sum(item["task_count"] for item in run_details
                                          if item["status"] == "waiting"),
                "completed_run_count": statuses.count("completed"),
                "trigger_times_in_order": timeline_facts["trigger_times_in_order"][1:],
                "trigger_gaps_seconds": timeline_facts["trigger_gaps_seconds"],
                "coalesced_periods": timeline_facts["coalesced_periods"][1:],
                "coalesced_gaps_accounted": timeline_facts["coalesced_gaps_accounted"],
            }, [schedule_order_evidence, schedule_baseline_evidence, schedule_rounds_evidence,
                schedule_events_evidence, schedule_timeline_evidence, install_path])
            save(report_path, report)

            registry_result = await call_shop(
                shop_client, "/api/data-center/host/registry/list", admin_context)
            source_specs = [item for item in registry_result["items"]
                            if item["source_id"] == SOURCE_ID]
            if len(source_specs) != 1 or source_specs[0]["kind"] != "analytics":
                raise AssertionError("来源目录没有唯一有效的AdventureWorks分析来源")
            source_spec = SourceRegistration.model_validate({
                field: source_specs[0][field] for field in SourceRegistration.model_fields
            }).model_dump(mode="json")
            event_results = []
            for _index in range(2):
                result = await call_shop(
                    shop_client, "/api/data-center/host/registry/register", admin_context, source_spec)
                event_results.append(result)
            event_observations = []
            event_observation_path = output_dir / "duplicate-source-event-observations.json"
            deadline = time.monotonic() + 30
            while True:
                after_events = await get_goal(goal_cookie_client, goal_id)
                source_event_runs = [run for run in after_events["runs"]
                                     if isinstance(run.get("trigger_id"), str)
                                     and run["trigger_id"].startswith("data-center-source:" + watch_id + ":")]
                event_observations.append({"observed_at": datetime.now(UTC).isoformat(),
                                           "runs": source_event_runs})
                save(event_observation_path, {"goal_id": goal_id, "watch_id": watch_id,
                     "registry_results": event_results, "observations": event_observations})
                if len(source_event_runs) > 1:
                    raise AssertionError("重复来源事件创建了多个运行")
                if source_event_runs and source_event_runs[0]["status"] not in {"queued", "running"}:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("重复来源事件没有在期限内进入终态")
                await asyncio.sleep(0.25)
            if source_event_runs[0]["status"] != "waiting":
                raise AssertionError("重复来源事件没有复用单个无变化等待运行")
            watch_notes = note_contents(notes, goal_id, actors["sync"]["actor_id"],
                                         "data-center-watch")
            event_note = next((item for item in watch_notes
                               if json.loads(item[1]).get("run_id") == source_event_runs[0]["run_id"]), None)
            if event_note is None or json.loads(event_note[1]).get("result") != "unchanged":
                raise AssertionError("重复来源事件没有保存无变化等待Note")
            report["duplicate_source_event"] = {
                "run_id": source_event_runs[0]["run_id"],
                "trigger_id": source_event_runs[0]["trigger_id"],
                "event_run_count": len(source_event_runs),
                "note_id": event_note[0].note_id, "note_version": event_note[0].version,
                "registry_calls": len(event_results),
            }
            event_evidence = output_dir / "duplicate-source-event.json"
            save(event_evidence, report["duplicate_source_event"])
            record_check(report, "duplicate_source_event_deduplication",
                         report["duplicate_source_event"],
                         [event_evidence, event_observation_path, install_path])
            save(report_path, report)

            before_cancel = await get_goal(goal_cookie_client, goal_id)
            cancel_response = await goal_cookie_client.post(f"/api/cluster/goals/{goal_id}/control/cancel")
            cancel_response.raise_for_status()
            cancel_http_response = {
                "status_code": cancel_response.status_code,
                "body": cancel_response.json(),
            }
            if cancel_http_response["body"].get("state") != "cancelled":
                raise AssertionError("来源监听目标没有进入cancelled状态")
            cancellation_source, cancellation_evidence = await verify_cancelled_goal_stability(
                shop_client, goal_cookie_client, shop_engine, goal_id,
                {"context": admin_context, "registration": source_spec},
                cancel_http_response, output_dir,
            )
            report["cancellation"] = {
                **cancellation_source,
                "state_before_cancel": before_cancel["state"],
                "runs_before_cancel_snapshot": goal_run_snapshot(before_cancel),
            }
            save(output_dir / "cancellation.json", report["cancellation"])
            record_check(report, "cancel_stops_new_scheduled_and_source_runs",
                         report["cancellation"], [*cancellation_evidence, install_path])
            save(report_path, report)

            recovery_request_id = str(uuid4())
            pending = asyncio.create_task(call_data_center(
                dc_client, "sync", {"source_id": SOURCE_ID}, sync_context, recovery_request_id))
            running_task = await wait_for_database_status(
                data_center_engine, sync_context["actor_id"], recovery_request_id, "running")
            try:
                await asyncio.wait_for(asyncio.shield(pending), timeout=0.05)
            except (TimeoutError, httpx.HTTPError):
                pass
            stop_evidence = service.terminate_with_sigterm()
            report["process_termination"] = {
                **stop_evidence,
                "request_id": recovery_request_id,
                "task_id": running_task["task_id"], "run_id": running_task["run_id"],
                "status_at_termination": running_task["status"],
                "input_sha256": running_task["input_sha256"],
            }
            termination_evidence = output_dir / "process-termination.json"
            save(termination_evidence, report["process_termination"])
            record_check(report, "sigterm_during_persisted_running_task",
                         report["process_termination"], [termination_evidence])
            service_restart = service.start()
            await service.wait_until_ready()
            report["process_restart"] = {**service_restart, "health_status": "ok"}
            restart_evidence = output_dir / "process-restart.json"
            save(restart_evidence, {
                "service_start": report["process_restart"],
                "health": {"status": "ok", "pid": service.process.pid},
            })
            record_check(report, "new_process_health_after_sigterm", {
                "old_pid": stop_evidence["pid"],
                "new_pid": service.process.pid,
                "health_status": "ok",
            }, [restart_evidence])
            save(report_path, report)
            resumed_submit = await call_data_center(
                dc_client, "sync", {"source_id": SOURCE_ID}, sync_context, recovery_request_id)
            if resumed_submit["data"]["task_id"] != running_task["task_id"]:
                raise AssertionError("新进程恢复时没有复用原任务编号")
            recovered = await wait_for_operation(dc_client, running_task["task_id"], sync_context)
            recovered_snapshot = assert_published_sync(
                recovered, running_task["task_id"])
            if recovered["run_id"] != running_task["run_id"]:
                raise AssertionError("新进程恢复时更换了原运行编号")
            if recovered["data"].get("usage", {}).get("requests") != []:
                raise AssertionError("零模型同步任务恢复时新增了模型调用")
            final_task = await asyncio.to_thread(
                database_task, data_center_engine, sync_context["actor_id"], recovery_request_id)
            if (final_task is None or final_task["task_id"] != running_task["task_id"]
                    or final_task["run_id"] != running_task["run_id"]
                    or final_task["status"] != "completed"):
                raise AssertionError("新进程恢复后数据库没有保存同一任务和运行的completed状态")
            report["process_recovery"] = {
                "old_pid": stop_evidence["pid"], "new_pid": service.process.pid,
                "actor_id": sync_context["actor_id"],
                "request_id": recovery_request_id,
                "task_id": recovered["data"]["task_id"], "run_id": recovered["run_id"],
                "status": recovered["status"], "snapshot_id": recovered_snapshot["snapshot_id"],
                "request_id_reused": final_task["task_id"] == running_task["task_id"],
                "model_requests": recovered["data"].get("usage", {}).get("requests", []),
            }
            recovery_evidence = output_dir / "process-recovery.json"
            save(recovery_evidence, report["process_recovery"])
            record_check(report, "new_process_resumes_same_task_request_and_snapshot",
                         report["process_recovery"], [recovery_evidence,
                                                       termination_evidence, restart_evidence])
            save(report_path, report)

            original_notes = note_contents(notes, watch_report["goal_id"],
                                           actors["sync"]["actor_id"], "data-center-source-input")
            if len(original_notes) != 1 or original_notes[0][0].version != 1:
                raise AssertionError("恢复后早期来源输入Note丢失或被覆盖")
            if original_notes[0][1] != saved_input_notes[0][1]:
                raise AssertionError("恢复后早期来源输入Note内容发生变化")
            report["early_note_recovery"] = {
                "goal_id": watch_report["goal_id"],
                "note_id": original_notes[0][0].note_id,
                "version_before_recovery": saved_input_notes[0][0].version,
                "version_after_recovery": original_notes[0][0].version,
                "content_sha256": sha256(original_notes[0][1].encode("utf-8")).hexdigest(),
                "content_unchanged": True,
            }
            early_note_evidence = output_dir / "early-note-recovery.json"
            save(early_note_evidence, report["early_note_recovery"])
            record_check(report, "early_input_note_preserved_after_recovery",
                         report["early_note_recovery"], [early_note_evidence,
                                                         schedule_setup_evidence])

            hashes_after = source_hashes()
            if hashes_before != hashes_after:
                raise AssertionError("真实可靠性检查期间源码发生变化")
            report["source_sha256_after"] = hashes_after
            report["source_manifest"]["after"] = hashes_after
            report["source_unchanged"] = True
            report["service_left_running"] = {"pid": service.process.pid,
                                               "listener_pids": service.listener_pids()}
            if report["service_left_running"]["listener_pids"] != [service.process.pid]:
                raise AssertionError("8101监听者与本脚本持有的服务进程不一致")
            await goal_cookie_client.aclose()
            await admin_cookie_client.aclose()
    finally:
        shop_engine.dispose()
        data_center_engine.dispose()

    report["status"] = "passed"
    report["completed_at"] = datetime.now(UTC).isoformat()
    if len(report["checks"]) != 14:
        raise AssertionError(f"可靠性检查数量不为14：{len(report['checks'])}")
    collect_evidence_manifest(report, output_dir, report_path)
    save(report_path, report)
    print(json.dumps({"run_id": report["run_id"], "status": report["status"],
                      "checks": len(report["checks"]), "report": str(report_path.relative_to(ROOT))},
                     ensure_ascii=False))


async def execute_cancellation_supplement(args):
    output_dir = args.output_dir.resolve()
    evidence_root = (ROOT / ".scratch/evidence").resolve()
    if not output_dir.is_relative_to(evidence_root):
        raise ValueError("取消补充报告目录必须位于.scratch/evidence")
    output_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    identities_path = args.identities.resolve(strict=True)
    base_report_path = args.base_report.resolve(strict=True)
    base_report_relative = base_report_path.relative_to(ROOT.resolve()).as_posix()
    base_report = json.loads(base_report_path.read_text(encoding="utf-8"))
    goal_id = args.cancelled_goal_id
    base_cancellation = base_report.get("cancellation", {})
    if (base_report.get("milestone") != "DC-06" or base_report.get("status") != "passed"
            or base_report.get("source_unchanged") is not True
            or base_cancellation.get("state") != "cancelled"
            or base_cancellation.get("goal_id") != goal_id):
        raise ValueError("补充核验目标没有绑定到已通过且已取消的DC-06报告")
    identities_document = load_identities(identities_path)
    actors = identities_document["actors"]
    hashes_before = source_hashes()
    report = {
        "run_id": "dc06-cancellation-supplement-"
        + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8],
        "milestone": "DC-06",
        "report_type": "cancellation_supplement",
        "status": "running",
        "started_at": datetime.now(UTC).isoformat(),
        "goal_id": goal_id,
        "shop_url": SHOP_URL,
        "data_center_url": DATA_CENTER_URL,
        "database": {"host": "127.0.0.1", "port": 55432, "name": "shop"},
        "identities_sha256": file_sha256(identities_path),
        "original_r10": {
            "path": base_report_relative,
            "report_sha256": file_sha256(base_report_path),
            "run_id": base_report["run_id"],
            "source_sha256_before": base_report["source_sha256_before"],
            "source_sha256_after": base_report["source_sha256_after"],
        },
        "source_manifest": {"before": hashes_before, "after": None},
        "source_applicability": {
            "scope": "cancelled_goal_http_and_postgres_observation",
            "original_r10_run_id": base_report["run_id"],
            "changed_since_r10": source_changes_since(
                base_report["source_sha256_after"], hashes_before),
        },
        "checks": [],
    }
    report_path = output_dir / "report.json"
    save(report_path, report)
    shop_engine = create_engine(Settings().database_url, pool_pre_ping=True)
    secrets = set()
    try:
        async with httpx.AsyncClient(base_url=SHOP_URL, trust_env=False, timeout=60) as client:
            await login_shop(client, actors["sync"], str(uuid4()), secrets)
            goal_before_cancel = await get_goal(client, goal_id)
            if (goal_before_cancel.get("state") != "cancelled"
                    or goal_before_cancel.get("actor_id") != actors["sync"]["actor_id"]):
                raise AssertionError("补充核验目标当前状态或归属与原已取消目标不符")
            before_cancel_runs = goal_run_snapshot(goal_before_cancel)
            if len(before_cancel_runs) != base_cancellation.get("runs_after"):
                raise AssertionError("原DC-06报告与当前目标的GoalRun数量不一致")
            before_cancel_requests = provider_request_records(
                shop_engine, [item["run_id"] for item in before_cancel_runs])
            admin_context = await host_login(
                client, SHOP_URL, actors["admin"], str(uuid4()), secrets)
            registry_result = await call_shop(
                client, "/api/data-center/host/registry/list", admin_context)
            source_specs = [item for item in registry_result["items"]
                            if item["source_id"] == SOURCE_ID]
            if len(source_specs) != 1 or source_specs[0]["kind"] != "analytics":
                raise AssertionError("来源目录没有唯一有效的AdventureWorks分析来源")
            source_registration = SourceRegistration.model_validate({
                field: source_specs[0][field] for field in SourceRegistration.model_fields
            }).model_dump(mode="json")
            cancel_response = await client.post(
                f"/api/cluster/goals/{goal_id}/control/cancel")
            cancel_response.raise_for_status()
            cancel_http_response = {
                "status_code": cancel_response.status_code,
                "body": cancel_response.json(),
            }
            cancellation_facts, cancellation_evidence = await verify_cancelled_goal_stability(
                client, client, shop_engine, goal_id,
                {"context": admin_context, "registration": source_registration},
                cancel_http_response, output_dir,
            )
            cancellation_facts["state_before_cancel"] = goal_before_cancel["state"]
            cancellation_facts["runs_before_cancel_snapshot"] = before_cancel_runs
            cancellation_facts["provider_request_ids_before_cancel"] = [
                item["request_id"] for item in before_cancel_requests]
            cancellation_facts["provider_request_records_before_cancel"] = before_cancel_requests
            report["cancellation"] = cancellation_facts
            save(output_dir / "cancellation.json", cancellation_facts)
            record_check(
                report, "cancel_stops_new_scheduled_and_source_runs", cancellation_facts,
                [base_report_path, *cancellation_evidence],
            )
            save(report_path, report)
    finally:
        shop_engine.dispose()
    hashes_after = source_hashes()
    if hashes_before != hashes_after:
        raise AssertionError("取消补充核验期间源码发生变化")
    report["source_manifest"]["after"] = hashes_after
    report["status"] = "passed"
    report["completed_at"] = datetime.now(UTC).isoformat()
    collect_evidence_manifest(report, output_dir, report_path)
    save(report_path, report)
    print(json.dumps({"run_id": report["run_id"], "status": report["status"],
                      "checks": len(report["checks"]),
                      "report": str(report_path.relative_to(ROOT))}, ensure_ascii=False))


async def run(args):
    try:
        if args.cancelled_goal_id:
            await execute_cancellation_supplement(args)
        else:
            await execute(args)
    except Exception as error:
        report_path = args.output_dir.resolve() / "report.json"
        if report_path.is_file():
            report = json.loads(report_path.read_text(encoding="utf-8"))
            identities_path = args.identities.resolve()
            secrets = []
            if identities_path.is_file():
                actors = json.loads(identities_path.read_text(encoding="utf-8")).get("actors", {})
                secrets.extend(password_for(actor) for actor in actors.values()
                                if isinstance(actor, dict) and actor.get("password_file"))
            database_url = os.environ.get("DATA_CENTER_DATABASE_URL")
            if database_url:
                secrets.append(database_url)
            report["status"] = "failed"
            report["failed_at"] = datetime.now(UTC).isoformat()
            report["failure"] = {
                "type": type(error).__name__,
                "message": redact(str(error), secrets),
                "last_check": report["checks"][-1] if report.get("checks") else None,
            }
            save(report_path, report)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--identities", type=Path,
                        default=ROOT / ".scratch/evidence/dc02-host-20261004-r2/identities.json")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--cancelled-goal-id")
    parser.add_argument("--base-report", type=Path,
                        default=ROOT / ".scratch/evidence/dc06-reliability-20261004T011619Z-5abb75ca/report.json")
    args = parser.parse_args()
    if args.output_dir is None:
        args.output_dir = ROOT / ".scratch/evidence" / (
            "dc06-reliability-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8])
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
