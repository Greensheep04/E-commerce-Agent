import argparse
import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic
from uuid import uuid4

import httpx
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.assessment import PROFILE
from shop.assessment_models import AssessmentAccess, AssessmentSource
from shop.config import Settings
from shop.customer_runtime_models import CustomerTicket
from shop.integration import ShopIntegration
from shop.integration_models import IntegrationBudgetWindow, IntegrationCall
from shop.models import User
from shop.orders import OrderQueryService
from shop.poster import POSTER_PROFILE, PosterService
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
ASSESSMENT_SOURCE = Path("/Users/greensheep/.codex/worktrees/7a43/project-shop")


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def digest(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             default=str).encode()).hexdigest()


def source_manifest():
    paths = [ROOT / name for name in ("pyproject.toml", "uv.lock", ".env.example")]
    for name in ("src", "packages", "vendor", "alembic", "frontend/src"):
        paths.extend(path for path in (ROOT / name).rglob("*")
                     if path.is_file() and path.suffix in {".py", ".toml", ".tsx", ".ts", ".css"}
                     and "__pycache__" not in path.parts)
    return {str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
            for path in sorted(set(paths))}


def snapshot(settings):
    engine = create_engine(settings.database_url)
    try:
        with Session(engine) as session:
            requests = [{"id": row.id, "status": row.status, "model": row.model,
                         "charged_cny": str(row.charged_cny) if row.charged_cny is not None else None,
                         "reservation_cny": str(row.reservation_cny),
                         "integration_run_id": row.price_basis.get("integration_run_id"),
                         "stage": row.price_basis.get("verification_scope"),
                         "request_sha256": digest(row.request_body),
                         "response_sha256": digest(row.response_body),
                         "run_id": row.run_id, "assessment_id": row.assessment_id,
                         "usage": row.usage}
                        for row in session.scalars(select(ProviderRequest).order_by(ProviderRequest.id))]
            account = session.get(BudgetAccount, "project")
            return {"requests": requests, "spent_cny": str(account.spent_cny),
                    "reserved_cny": str(account.reserved_cny),
                    "ticket_ids": list(session.scalars(select(CustomerTicket.id).order_by(CustomerTicket.id)))}
    finally:
        engine.dispose()


def freeze(directory, base_url):
    settings = Settings()
    engine = create_engine(settings.database_url)
    service = PosterService(settings)
    try:
        with Session(engine) as session:
            admin = session.scalar(select(User).where(User.username == "admin"))
            customer = session.scalar(select(User).where(User.username == "customer-11091"))
            other = session.scalar(select(User).where(User.username == "customer-11176"))
            access = session.scalar(select(AssessmentAccess).where(
                AssessmentAccess.level == "employee", AssessmentAccess.employee_id == 275))
            employee = session.get(User, access.user_id)
            assert admin and customer and other and employee
            orders = OrderQueryService(session, customer.id).list_orders(limit=20)
            order_number = orders["orders"][0]["order_number"]
            tasks = session.scalars(select(HarnessTask).where(
                HarnessTask.status == "completed").order_by(HarnessTask.created_at.desc())).all()
            selected = None
            for task in tasks:
                if not (task.result and task.result.get("status") == "materials_found"
                        and task.result.get("review", {}).get("approved") is True
                        and 1 <= len(task.result.get("recommendations", [])) <= 2):
                    continue
                inputs = service.read_harness(task.id, admin.id)
                candidate = service.create_job(inputs, admin.id)
                if not service.job_path(candidate.job_id).exists():
                    selected = (task, inputs, candidate)
                    break
            assert selected is not None, "需要尚未准备关键词的真实已审核Harness任务"
            task, poster_input, job = selected
            run_id = "if-live-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
            accounts = {
                "admin": {"username": admin.username, "actor_id": admin.id,
                          "password_file": str(settings.admin_password_file)},
                "customer": {"username": customer.username, "actor_id": customer.id,
                             "password_file": ".local/auth/customer-11091.password"},
                "other": {"username": other.username, "actor_id": other.id,
                          "password_file": ".local/auth/customer-11176.password"},
                "assessment": {"username": employee.username, "actor_id": employee.id,
                               "password_file": str(ASSESSMENT_SOURCE / ".local/assessment/auth/assessment-employee.password")},
            }
            for account in accounts.values():
                assert Path(account["password_file"]).is_file()
            payload = {"run_id": run_id, "base_url": base_url, "accounts": accounts,
                       "harness_task_id": task.id, "harness_run_id": task.run_id,
                       "harness_result_sha256": digest(task.result),
                       "poster_job_id": job.job_id, "poster_input": poster_input.model_dump(mode="json"),
                       "order_number": order_number, "orders": orders,
                       "assessment_employee_id": access.employee_id,
                       "assessment_prompt": "请列出我在2023年第三季度的销售额、订单数、平均订单金额及目标完成率，并给出订单依据。",
                       "order_prompt": f"查询订单{order_number}的完整明细",
                       "ticket_prompt": f"可以为订单{order_number}创建人工工单核查发货信息吗？请提供待确认内容",
                       "request_ids": {name: str(uuid4()) for name in (
                           "conversation", "order", "ticket", "confirm", "assessment_create",
                           "assessment_run", "poster_prepare", "poster_run")},
                       "profiles": {"assessment": PROFILE.model_dump(), "poster": POSTER_PROFILE.model_dump()},
                       "data_sources": {"assessment": [
                           {"id": row.id, "source_id": row.source_id, "revision": row.revision,
                            "fingerprint": row.fingerprint, "manifest": row.manifest}
                           for row in session.scalars(select(AssessmentSource))],
                           "order_source_sha256": sha256(settings.assessment_order_source_file.read_bytes()).hexdigest()},
                       "source_sha256": source_manifest(),
                       "database_config_sha256": sha256(settings.database_url.encode()).hexdigest()}
            save(directory / "inputs.json", payload)
            save(directory / "baseline.json", snapshot(settings))
            print(directory / "inputs.json")
    finally:
        service.engine.dispose()
        engine.dispose()


def activate_window(settings, run_id):
    engine = create_engine(settings.database_url)
    try:
        with Session(engine) as session, session.begin():
            active = session.scalar(select(IntegrationBudgetWindow).where(IntegrationBudgetWindow.active))
            assert active is None or active.id == run_id, "其他接口验证预算窗口仍在使用"
            window = session.get(IntegrationBudgetWindow, run_id)
            if window is None:
                session.add(IntegrationBudgetWindow(id=run_id, limit_cny=Decimal(5), active=True))
            else:
                assert window.limit_cny == Decimal(5) and window.active
    finally:
        engine.dispose()


async def verify(inputs_path, directory, worker_evidence=None):
    inputs = json.loads(inputs_path.read_text(encoding="utf-8"))
    settings = Settings()
    before = snapshot(settings)
    sources = source_manifest()
    changed = {name for name in set(inputs["source_sha256"]) | set(sources)
               if inputs["source_sha256"].get(name) != sources.get(name)}
    reuse = None
    if changed:
        assert changed == {"src/shop/customer_worker.py"} and worker_evidence is not None
        proof = json.loads(worker_evidence.read_text(encoding="utf-8"))
        assert proof["status"] == "passed"
        assert proof["source_sha256"]["src/shop/customer_worker.py"] == sources["src/shop/customer_worker.py"]
        reuse = {"changed_sources": sorted(changed), "evidence": str(worker_evidence),
                 "evidence_sha256": sha256(worker_evidence.read_bytes()).hexdigest(),
                 "scope": "客服关闭及锁等待路径已补充实际数据库验证；固定业务输入与请求编号保持"}
    activate_window(settings, inputs["run_id"])
    report = {"run_id": inputs["run_id"], "milestones": ["IF-02", "IF-04", "IF-05"],
              "inputs": str(inputs_path), "inputs_sha256": sha256(inputs_path.read_bytes()).hexdigest(),
              "source_sha256": sources, "checks": {}, "records": {}, "status": "running"}
    report["source_reuse"] = reuse
    save(directory / "before.json", before)
    save(directory / "report.json", report)
    gateway = ShopIntegration(settings)
    clients = {}

    def record(name, value):
        report["checks"][name] = True
        report["records"][name] = value
        save(directory / "report.json", report)

    async def call(account, module, operation, payload, request_id=None, upstream=None, expected=200):
        body = {"version": "1", "request_id": request_id or str(uuid4()), "payload": payload,
                "upstream": upstream or []}
        response = await clients[account].post(f"/api/modules/{module}/operations/{operation}", json=body)
        value = response.json()
        assert response.status_code == expected, (module, operation, response.status_code, value)
        return value

    async def python(account, module, operation, payload, request_id=None, upstream=None):
        identity = inputs["accounts"][account]
        context = gateway.login(identity["username"], Path(identity["password_file"]).read_text().strip(),
                                request_id=request_id, upstream=upstream)
        result = await gateway.invoke(module, operation, payload, context)
        return result.model_dump(mode="json")

    async def turn(payload, request_id):
        first = await call("customer", "customer", "submit", payload, request_id)
        repeated = await call("customer", "customer", "submit", payload, request_id)
        assert first["data"]["id"] == repeated["data"]["id"]
        turn_id = first["data"]["id"]
        deadline = monotonic() + 130
        while monotonic() < deadline:
            result = await call("customer", "customer", "turn", {"turn_id": turn_id})
            if result["data"]["status"] not in {"queued", "running"}:
                assert result["data"]["status"] == "completed", result
                return result
            await asyncio.sleep(0.25)
        raise TimeoutError("真实客服业务没有在累计时间内完成")

    try:
        await gateway.startup()
        for name, account in inputs["accounts"].items():
            client = httpx.AsyncClient(base_url=inputs["base_url"], timeout=220)
            clients[name] = client
            response = await client.post("/api/login", json={"username": account["username"],
                "password": Path(account["password_file"]).read_text().strip()})
            assert response.status_code == 200 and response.json()["id"] == account["actor_id"]
        async with httpx.AsyncClient(base_url=inputs["base_url"]) as anonymous:
            assert (await anonymous.get("/api/modules")).status_code == 401
            assert (await anonymous.get("/api/poster/status")).status_code == 401
        modules = (await clients["admin"].get("/api/modules")).json()
        assert {row["module_id"] for row in modules} == set(gateway.adapters)
        for row in modules:
            assert all(op["input_schema"] and op["output_schema"] for op in row["operations"] if op["supported"])
        record("authenticated_catalog", modules)
        identity_http = await call("customer", "customer", "identity", {})
        identity_python = await python("customer", "customer", "identity", {})
        assert identity_http["data"] == identity_python["data"]
        order = await call("customer", "customer", "order", {"order_number": inputs["order_number"]})
        assert order["data"] == (await python("customer", "customer", "order",
                                             {"order_number": inputs["order_number"]}))["data"]
        await call("other", "customer", "order", {"order_number": inputs["order_number"]}, expected=404)
        record("customer_identity_and_order", {"identity": identity_http, "order": order})
        harness = await call("admin", "harness", "show", {"task_id": inputs["harness_task_id"]})
        assert digest(harness["data"]["result"]) == inputs["harness_result_sha256"]
        stable = snapshot(settings)
        await python("admin", "harness", "run", {"task_id": inputs["harness_task_id"]})
        assert snapshot(settings) == stable
        page = await clients["admin"].get(
            f'/api/modules/harness/resources/{inputs["harness_task_id"]}/files/page.html')
        assert page.status_code == 200 and sha256(page.content).hexdigest() == page.headers["X-Content-SHA256"]
        material = inputs["poster_input"]["materials"][0]
        asset = await call("admin", "materials", "show", {"asset_id": material["asset_id"]})
        image = await clients["admin"].get(
            f'/api/modules/materials/resources/{material["version_id"]}/files/content')
        assert image.status_code == 200 and sha256(image.content).hexdigest() == material["file_sha256"]
        record("upstream_and_resource_files", {"harness": harness, "asset": asset,
               "html_sha256": sha256(page.content).hexdigest(), "image_sha256": sha256(image.content).hexdigest()})
        conversation = await call("customer", "customer", "create_conversation",
            {"title": inputs["run_id"]}, inputs["request_ids"]["conversation"])
        conversation_id = conversation["data"]["id"]
        order_turn = await turn({"conversation_id": conversation_id, "message": inputs["order_prompt"]},
                                inputs["request_ids"]["order"])
        result = order_turn["data"]["result"]
        assert any(row["order_number"] == inputs["order_number"] for row in result["orders"])
        assert result["sources"] and result["text"]
        record("customer_order_workflow", order_turn)
        pending_turn = await turn({"conversation_id": conversation_id, "message": inputs["ticket_prompt"]},
                                  inputs["request_ids"]["ticket"])
        pending = pending_turn["data"]["result"]["pending_ticket"]
        assert pending and pending_turn["data"]["result"]["ticket"] is None
        confirmation = {"conversation_id": conversation_id, "message": "确认", "confirm_pending": pending["id"]}
        confirmed = await turn(confirmation, inputs["request_ids"]["confirm"])
        ticket = confirmed["data"]["result"]["ticket"]
        assert ticket and ticket["order_number"] == inputs["order_number"]
        stable = snapshot(settings)
        reused = await call("customer", "customer", "submit", confirmation, inputs["request_ids"]["confirm"])
        assert reused["data"]["id"] == confirmed["data"]["id"] and snapshot(settings) == stable
        record("customer_confirmed_ticket_and_reuse", {"pending": pending_turn, "confirmed": confirmed})
        assessment = await call("assessment", "assessment", "create",
            {"prompt": inputs["assessment_prompt"]}, inputs["request_ids"]["assessment_create"])
        assessment_id = assessment["data"]["id"]
        assessment = await call("assessment", "assessment", "run", {"run_id": assessment_id},
                                inputs["request_ids"]["assessment_run"])
        assert assessment["data"]["status"] == "completed", assessment
        assert assessment["data"]["result"]["observations"] and assessment["data"]["result"]["evidence"]
        assert {row["employee_id"] for row in assessment["data"]["metrics"]["employees"]} == {275}
        stable = snapshot(settings)
        restored = await python("assessment", "assessment", "resume", {"run_id": assessment_id},
                                inputs["request_ids"]["assessment_run"])
        assert restored["data"]["result"] == assessment["data"]["result"] and snapshot(settings) == stable
        record("assessment_report_and_reuse", assessment)
        upstream = harness["resources"] + [item for item in asset["resources"] if item["resource_type"] == "asset_version"
                                          and item["resource_id"] == material["version_id"]]
        prepared = await call("admin", "poster", "prepare", {"task_id": inputs["harness_task_id"]},
                              inputs["request_ids"]["poster_prepare"], upstream)
        assert prepared["data"]["job"]["job_id"] == inputs["poster_job_id"]
        assert 2 <= len(prepared["data"]["job"]["keyword_plan"]["keywords"]) <= 5
        stable = snapshot(settings)
        reused = await call("admin", "poster", "prepare", {"task_id": inputs["harness_task_id"]},
                            inputs["request_ids"]["poster_prepare"], upstream)
        assert reused["data"]["job"]["keyword_plan"] == prepared["data"]["job"]["keyword_plan"]
        assert snapshot(settings) == stable
        waiting = await python("admin", "poster", "run", {"job_id": inputs["poster_job_id"]},
                               inputs["request_ids"]["poster_run"])
        assert waiting["status"] == "waiting_agents" and snapshot(settings) == stable
        unsupported = await call("admin", "poster", "resume",
                                 {"job_id": inputs["poster_job_id"]}, expected=422)
        assert unsupported["detail"]["code"] == "unsupported_operation"
        record("poster_keywords_upstream_and_waiting_agents", {"prepared": prepared, "waiting": waiting})
        report["browser_inputs"] = {"base_url": inputs["base_url"], "accounts": inputs["accounts"],
                                    "harness_task_id": inputs["harness_task_id"],
                                    "conversation_id": conversation_id, "assessment_run_id": assessment_id}
        save(directory / "browser-inputs.json", report["browser_inputs"])
        record("all_live_checks", True)
        report["status"] = "checking"
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = f"{type(error).__name__}: {error}"
        save(directory / "report.json", report)
        raise
    finally:
        for client in clients.values():
            await client.aclose()
        await gateway.shutdown()
        after = snapshot(settings)
        save(directory / "after.json", after)
        save(directory / "report.json", report)
        original = {row["id"]: row for row in before["requests"]}
        assert all(row == original[row["id"]] for row in after["requests"] if row["id"] in original)
        added = [row for row in after["requests"] if row["id"] not in original]
        assert all(row["integration_run_id"] == inputs["run_id"] for row in added)
        amount = sum((Decimal(row["charged_cny"] or row["reservation_cny"]) for row in added), Decimal(0))
        assert amount <= Decimal(5)
        report["request_increment"] = added
        report["new_spent_and_reserved_cny"] = str(amount)
        report["ticket_increment"] = sorted(set(after["ticket_ids"]) - set(before["ticket_ids"]))
        baseline = json.loads((inputs_path.parent / "baseline.json").read_text(encoding="utf-8"))
        baseline_ids = {row["id"] for row in baseline["requests"]}
        cumulative = [row for row in after["requests"] if row["id"] not in baseline_ids]
        assert all(row["integration_run_id"] == inputs["run_id"] for row in cumulative)
        report["window_requests"] = cumulative
        report["window_spent_and_reserved_cny"] = str(sum((
            Decimal(row["charged_cny"] or row["reservation_cny"]) for row in cumulative), Decimal(0)))
        assert Decimal(report["window_spent_and_reserved_cny"]) <= Decimal(5)
        if report["status"] == "checking":
            assert all(row["status"] == "completed" for row in cumulative)
            assert {row["stage"] for row in cumulative} == {"customer", "assessment", "m3"}
            assert len(set(after["ticket_ids"]) - set(baseline["ticket_ids"])) == 1
        engine = create_engine(settings.database_url)
        try:
            with Session(engine) as session:
                report["integration_calls"] = [{"actor_id": row.actor_id, "module_id": row.module_id,
                    "operation": row.operation, "request_id": row.request_id, "resource_id": row.resource_id,
                    "payload_sha256": row.payload_sha256, "upstream": row.upstream}
                    for row in session.scalars(select(IntegrationCall).where(
                        IntegrationCall.request_id.in_(list(inputs["request_ids"].values()))))]
        finally:
            engine.dispose()
        report["source_after_sha256"] = source_manifest()
        assert report["source_after_sha256"] == sources
        if report["status"] == "checking":
            report["status"] = "passed"
        save(directory / "report.json", report)
        print(directory / "report.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["freeze", "run"])
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--worker-evidence", type=Path)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=False)
    if args.mode == "freeze":
        freeze(args.directory, args.base_url)
    else:
        assert args.inputs is not None
        asyncio.run(verify(args.inputs, args.directory, args.worker_evidence))


if __name__ == "__main__":
    main()
