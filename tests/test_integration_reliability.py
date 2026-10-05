import asyncio
import hashlib
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.orm import sessionmaker

from shop.auth import hash_password
from shop.budget import BudgetExceeded, BudgetService
from shop.config import Settings
from shop.integration import ShopIntegration
from shop.integration_models import IntegrationBudgetWindow, IntegrationCall
from shop.models import User
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESS_HELPER = Path(__file__).with_name("integration_process.py")
TEST_PASSWORD = "if05-local-process-password"
SESSION_SECRET = "if05-local-process-session-secret"
PROMPT_CONCURRENT = "IF-05 两个进程提交同一 Harness 创建请求"
PROMPT_RESTART = "IF-05 进程重启后的 Harness 创建请求"


@pytest.fixture(scope="session")
def reliability_evidence_dir(tmp_path_factory):
    path = tmp_path_factory.getbasetemp().parent
    path.mkdir(parents=True, exist_ok=True)
    return path


def _user_and_settings(db_session, data_dir):
    data_dir.mkdir(parents=True, exist_ok=True)
    user = User(username="if05-reliability-owner",
                password_hash=hash_password(TEST_PASSWORD), role="admin")
    db_session.add(user)
    db_session.commit()
    settings = Settings(database_url=db_session.info["test_database_url"],
                        data_dir=data_dir, poster_dir=data_dir.parent / "poster",
                        session_secret=SESSION_SECRET)
    return user, settings


def _child_environment(db_session, user, data_dir, request_id, prompt):
    environment = os.environ.copy()
    environment.update({
        "IF05_DATABASE_URL": db_session.info["test_database_url"],
        "IF05_DATA_DIR": str(data_dir),
        "IF05_POSTER_DIR": str(data_dir.parent / "poster"),
        "IF05_SESSION_SECRET": SESSION_SECRET,
        "IF05_USERNAME": user.username,
        "IF05_PASSWORD": TEST_PASSWORD,
        "IF05_REQUEST_ID": request_id,
        "IF05_PROMPT": prompt,
    })
    pythonpath = [str(PROJECT_ROOT)]
    if environment.get("PYTHONPATH"):
        pythonpath.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(pythonpath)
    return environment


def _collect_processes(processes, evidence_dir, name):
    records = []
    for index, process in enumerate(processes):
        timed_out = False
        try:
            stdout, stderr = process.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            stdout, stderr = process.communicate()
        stdout_path = evidence_dir / f"{name}-{index}.stdout.txt"
        stderr_path = evidence_dir / f"{name}-{index}.stderr.txt"
        stdout_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
        records.append({
            "returncode": process.returncode,
            "timed_out": timed_out,
            "stdout": stdout,
            "stderr": stderr,
            "stdout_path": stdout_path.name,
            "stderr_path": stderr_path.name,
        })
    return records


def _database_conditions(db_session):
    database, schema, version = db_session.execute(text(
        "SELECT current_database(), current_schema(), current_setting('server_version')"
    )).one()
    return {"database": database, "schema": schema, "postgresql_version": version}


def _record_evidence(evidence_dir, name, db_session, inputs, observations, status):
    source_paths = (
        "src/shop/integration.py",
        "src/shop/integration_adapters.py",
        "src/shop/integration_models.py",
        "src/shop/integration_store.py",
        "src/shop/budget.py",
        "src/shop/harness.py",
        "src/shop/runtime_models.py",
        "tests/integration_process.py",
        "tests/test_integration_reliability.py",
        "uv.lock",
    )
    sources = {
        path: hashlib.sha256((PROJECT_ROOT / path).read_bytes()).hexdigest()
        for path in source_paths
    }
    input_text = json.dumps(inputs, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    report = {
        "milestone": "IF-05",
        "check": name,
        "status": status,
        "run_id": evidence_dir.name,
        "input_sha256": hashlib.sha256(input_text.encode()).hexdigest(),
        "inputs": inputs,
        "database": _database_conditions(db_session),
        "data_version": "if05-fixed-input-v1",
        "run_conditions": {
            "source_data": "使用固定输入与本次检查建立的独立测试数据",
            "python_version": sys.version.split()[0],
            "provider_requests_expected": 0,
            "cloud_requests": 0,
            "dependencies": "真实 PostgreSQL 与项目安装依赖",
        },
        "source_sha256": sources,
        "observations": observations,
    }
    (evidence_dir / f"{name}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )


def _process_result(record):
    assert record["returncode"] == 0, record["stderr"] or record["stdout"]
    assert not record["timed_out"], "真实进程运行超过90秒"
    return json.loads(record["stdout"])


def test_if05_harness_create_is_single_resource_across_real_processes(
    db_session, data_dir, reliability_evidence_dir
):
    user, _ = _user_and_settings(db_session, data_dir)
    request_id = "if05-two-process-harness-create-v1"
    barrier_directory = reliability_evidence_dir / "two-process-barrier"
    barrier_directory.mkdir(parents=True, exist_ok=True)
    environment = _child_environment(db_session, user, data_dir, request_id, PROMPT_CONCURRENT)
    command = [sys.executable, str(PROCESS_HELPER), "--barrier-directory", str(barrier_directory)]
    processes = [
        subprocess.Popen(command + ["--slot", str(slot)], cwd=PROJECT_ROOT,
                         env=environment, text=True, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
        for slot in range(2)
    ]
    records = _collect_processes(processes, reliability_evidence_dir, "two-process")
    results = [_process_result(record) for record in records]
    observations = {"processes": results, "command": command}
    inputs = {
        "identity": {"actor_id": user.id, "username": user.username},
        "module_id": "harness",
        "operation": "create",
        "request_id": request_id,
        "payload": {"prompt": PROMPT_CONCURRENT},
    }
    _record_evidence(reliability_evidence_dir, "if05-two-process-harness-create",
                     db_session, inputs, observations, "observed")

    assert results[0]["pid"] != results[1]["pid"]
    assert results[0]["actor_id"] == results[1]["actor_id"] == user.id
    assert results[0]["task_id"] == results[1]["task_id"]
    assert all(item["provider_request_count_before"] == 0 for item in results)
    assert all(item["provider_request_count_after"] == 0 for item in results)
    db_session.expire_all()
    assert db_session.scalar(select(func.count()).select_from(HarnessTask)) == 1
    assert db_session.scalar(select(func.count()).select_from(IntegrationCall)) == 1
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
    observations["persisted_harness_task_count"] = 1
    observations["persisted_request_binding_count"] = 1
    observations["provider_request_count"] = 0
    _record_evidence(reliability_evidence_dir, "if05-two-process-harness-create",
                     db_session, inputs, observations, "passed")


def test_if05_committed_create_survives_process_restart_and_rejects_changed_input(
    db_session, data_dir, reliability_evidence_dir
):
    user, settings = _user_and_settings(db_session, data_dir)
    request_id = "if05-process-restart-harness-create-v1"
    environment = _child_environment(db_session, user, data_dir, request_id, PROMPT_RESTART)
    command = [sys.executable, str(PROCESS_HELPER)]
    first_process = subprocess.Popen(command + ["--abrupt-exit"], cwd=PROJECT_ROOT,
                                     env=environment, text=True, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
    first_record = _collect_processes([first_process], reliability_evidence_dir,
                                      "restart-before-termination")[0]
    first_result = _process_result(first_record)

    second_process = subprocess.Popen(command, cwd=PROJECT_ROOT, env=environment,
                                      text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    second_record = _collect_processes([second_process], reliability_evidence_dir,
                                       "restart-after-termination")[0]
    second_result = _process_result(second_record)

    async def reject_changed_input():
        gateway = ShopIntegration(settings)
        await gateway.startup()
        try:
            context = gateway.login(user.username, TEST_PASSWORD, request_id)
            with pytest.raises(ValueError, match="相同请求编号的内容不一致"):
                await gateway.invoke(
                    "harness", "create", {"prompt": "IF-05 同编号但输入内容已经改变"}, context
                )
            return True
        finally:
            await gateway.shutdown()

    conflict_rejected = asyncio.run(reject_changed_input())
    observations = {
        "first_process": first_result,
        "restarted_process": second_result,
        "conflicting_input_rejected": conflict_rejected,
        "process_exit_codes": [first_record["returncode"], second_record["returncode"]],
        "command": command + ["--abrupt-exit"],
    }
    inputs = {
        "identity": {"actor_id": user.id, "username": user.username},
        "module_id": "harness",
        "operation": "create",
        "request_id": request_id,
        "payload": {"prompt": PROMPT_RESTART},
        "conflicting_payload": {"prompt": "IF-05 同编号但输入内容已经改变"},
    }
    _record_evidence(reliability_evidence_dir, "if05-process-restart",
                     db_session, inputs, observations, "observed")

    assert first_record["returncode"] == second_record["returncode"] == 0
    assert first_process.pid != second_process.pid
    assert first_result["actor_id"] == second_result["actor_id"] == user.id
    assert first_result["task_id"] == second_result["task_id"]
    assert first_result["provider_request_count_before"] == 0
    assert first_result["provider_request_count_after"] == 0
    assert second_result["provider_request_count_before"] == 0
    assert second_result["provider_request_count_after"] == 0
    assert conflict_rejected is True
    db_session.expire_all()
    assert db_session.scalar(select(func.count()).select_from(HarnessTask)) == 1
    assert db_session.scalar(select(func.count()).select_from(IntegrationCall)) == 1
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
    observations["persisted_harness_task_count"] = 1
    observations["persisted_request_binding_count"] = 1
    observations["provider_request_count"] = 0
    _record_evidence(reliability_evidence_dir, "if05-process-restart",
                     db_session, inputs, observations, "passed")


def test_if05_concurrent_budget_reservations_respect_window_and_keep_unknown(
    db_session, reliability_evidence_dir
):
    window = IntegrationBudgetWindow(id="if05-budget-window-v1",
                                     limit_cny=Decimal("0.00030000"), active=True)
    db_session.add(window)
    db_session.commit()
    maker = sessionmaker(db_session.get_bind(), expire_on_commit=False)
    barrier = threading.Barrier(2)

    def reserve(slot):
        barrier.wait(timeout=30)
        budget = BudgetService(maker)
        try:
            request = budget.reserve(
                task_id=None, run_id="if05-budget-window-v1", step_key=f"if05-concurrent-{slot}",
                service="deepseek", model="deepseek-flash",
                request_body={"model": "deepseek-flash", "messages": [
                    {"role": "user", "content": "IF-05 并发费用预留"}
                ]},
                input_limit=100, output_limit=1,
            )
        except BudgetExceeded as error:
            return {"status": "rejected", "reason": str(error)}
        return {"status": "reserved", "request_id": request.id,
                "reservation_cny": str(request.reservation_cny)}

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, range(2)))
    accepted = [item for item in results if item["status"] == "reserved"]
    rejected = [item for item in results if item["status"] == "rejected"]
    observations = {"concurrent_reservations": results}
    inputs = {
        "budget_window_id": window.id,
        "limit_cny": str(window.limit_cny),
        "service": "deepseek",
        "model": "deepseek-flash",
        "input_limit": 100,
        "output_limit": 1,
        "requests_started_concurrently": 2,
    }
    _record_evidence(reliability_evidence_dir, "if05-concurrent-budget",
                     db_session, inputs, observations, "observed")

    assert len(accepted) == 1
    assert len(rejected) == 1
    assert rejected[0]["reason"] == "接口集成验证预算不足"
    assert accepted[0]["reservation_cny"] == "0.00020800"

    budget = BudgetService(maker)
    budget.mark_sent(accepted[0]["request_id"])
    budget.mark_unknown(accepted[0]["request_id"], "IF-05 保留 unknown 费用")
    db_session.expire_all()
    request = db_session.get(ProviderRequest, accepted[0]["request_id"])
    account = db_session.get(BudgetAccount, "project")
    current_window = db_session.get(IntegrationBudgetWindow, window.id)
    window_total = db_session.scalar(select(func.sum(ProviderRequest.reservation_cny)).where(
        ProviderRequest.price_basis["integration_run_id"].astext == window.id
    ))
    assert request.status == "unknown"
    assert request.charged_cny is None
    assert account.reserved_cny == Decimal("0.00020800")
    assert window_total == Decimal("0.00020800")

    try:
        extra = budget.reserve(
            task_id=None, run_id="if05-budget-window-v1", step_key="if05-after-unknown",
            service="deepseek", model="deepseek-flash",
            request_body={"model": "deepseek-flash", "messages": [
                {"role": "user", "content": "IF-05 未知费用后的新增预留"}
            ]},
            input_limit=50, output_limit=0,
        )
    except BudgetExceeded as error:
        after_unknown = {"status": "rejected", "reason": str(error)}
    else:
        after_unknown = {"status": "reserved", "request_id": extra.id,
                         "reservation_cny": str(extra.reservation_cny)}
    observations.update({
        "unknown_request_id": request.id,
        "unknown_status": request.status,
        "account_reserved_cny": str(account.reserved_cny),
        "window_total_cny": str(window_total),
        "window_limit_cny": str(current_window.limit_cny),
        "reservation_after_unknown": after_unknown,
    })
    _record_evidence(reliability_evidence_dir, "if05-concurrent-budget",
                     db_session, inputs, observations, "observed")

    assert current_window.active is True
    assert window_total <= current_window.limit_cny
    assert after_unknown == {"status": "rejected", "reason": "接口集成验证预算不足"}
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 1
    _record_evidence(reliability_evidence_dir, "if05-concurrent-budget",
                     db_session, inputs, observations, "passed")
