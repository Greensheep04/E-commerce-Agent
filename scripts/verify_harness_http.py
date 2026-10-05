import argparse
import json
import secrets
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from shop.auth import hash_password
from shop.config import Settings
from shop.models import User
from shop.runtime_models import BudgetAccount, ProviderRequest


def main() -> None:
    parser = argparse.ArgumentParser(description="通过实际 HTTP 服务验证 Harness 权限与完成结果")
    parser.add_argument("completed_task_id", nargs="?")
    parser.add_argument("--permissions-only", action="store_true")
    args = parser.parse_args()
    if not args.permissions_only and not args.completed_task_id:
        parser.error("完整检查需要已完成的黑色男士T恤任务编号")
    settings = Settings()
    run_id = f"m2-http-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_dir = Path(".scratch/evidence") / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    engine = create_engine(settings.database_url)
    maker = sessionmaker(engine, expire_on_commit=False)
    accounts = []
    with maker.begin() as session:
        requests_before = session.scalar(select(func.count()).select_from(ProviderRequest))
        account = session.get(BudgetAccount, "project")
        budget_before = (str(account.spent_cny), str(account.reserved_cny))
        for label in ("owner", "other"):
            password = secrets.token_urlsafe(32)
            user = User(id=str(uuid4()), username=f"{run_id}-{label}",
                        password_hash=hash_password(password), role="user")
            session.add(user)
            accounts.append((user, password))
    checks = []
    responses = {}
    with (
        httpx.Client(base_url="http://127.0.0.1:8000", timeout=30) as owner,
        httpx.Client(base_url="http://127.0.0.1:8000", timeout=30) as other,
        httpx.Client(base_url="http://127.0.0.1:8000", timeout=30) as admin,
    ):
        for client, (user, password) in zip((owner, other), accounts, strict=True):
            client.post("/api/login", json={
                "username": user.username, "password": password,
            }).raise_for_status()
        created_response = owner.post("/api/harness/tasks", json={
            "prompt": "HTTP 权限边界验证，创建后直接取消",
        })
        created_response.raise_for_status()
        created = created_response.json()
        assert created["owner_id"] == accounts[0][0].id
        owner_endpoint = f"/api/harness/tasks/{created['task_id']}"
        for suffix in ("", "/preview", "/export"):
            assert admin.get(owner_endpoint + suffix).status_code == 401
        for suffix in ("run", "resume", "cancel"):
            assert admin.post(f"{owner_endpoint}/{suffix}").status_code == 401
        checks.append("anonymous_read_preview_export_and_actions_denied")
        admin.post("/api/login", json={
            "username": "admin",
            "password": settings.admin_password_file.read_text(encoding="utf-8").strip(),
        }).raise_for_status()
        assert other.get(owner_endpoint).status_code == 403
        for suffix in ("preview", "export"):
            assert other.get(f"{owner_endpoint}/{suffix}").status_code == 403
        for suffix in ("run", "resume", "cancel"):
            assert other.post(f"{owner_endpoint}/{suffix}").status_code == 403
        assert owner.get(owner_endpoint).json()["status"] == "created"
        assert admin.get(owner_endpoint).status_code == 200
        assert created["task_id"] in {item["task_id"] for item in owner.get("/api/harness/tasks").json()}
        assert created["task_id"] not in {item["task_id"] for item in other.get("/api/harness/tasks").json()}
        checks.append("task_owner_and_cross_account_actions")
        cancelled = owner.post(f"{owner_endpoint}/cancel")
        cancelled.raise_for_status()
        assert cancelled.json()["status"] == "cancelled"
        for suffix in ("run", "resume"):
            response = owner.post(f"{owner_endpoint}/{suffix}")
            response.raise_for_status()
            value = response.json()
            assert value["status"] == "cancelled"
            assert value["model_calls"] == value["tool_calls"] == 0
            assert value["requests"] == value["tools"] == []
        checks.append("cancelled_task_has_no_new_calls")
        for suffix in ("preview", "export"):
            assert owner.get(f"{owner_endpoint}/{suffix}").status_code == 409
        checks.append("cancelled_task_has_no_preview_or_export")
        if not args.permissions_only:
            endpoint = f"/api/harness/tasks/{args.completed_task_id}"
            completed_response = admin.get(endpoint)
            completed_response.raise_for_status()
            completed = completed_response.json()
            assert completed["status"] == "completed"
            assert completed["result"]["status"] == "materials_found"
            assert len(completed["result"]["recommendations"]) == 2
            for item in completed["result"]["recommendations"]:
                detail_response = admin.get(f"/api/assets/{item['asset_id']}")
                detail_response.raise_for_status()
                detail = detail_response.json()
                version = next(value for value in detail["versions"]
                               if value["version"] == item["version"])
                assert detail["record_id"] == item["record_id"]
                assert detail["source_id"] == item["source_id"]
                assert detail["source_revision"] == item["source_revision"]
                for field, expected in (("gender", "Men"), ("articleType", "Tshirts"),
                                        ("baseColour", "Black")):
                    assert version["original_fields"][field] == expected
                file_response = admin.get(
                    f"/api/assets/{item['asset_id']}/versions/{item['version']}/file"
                )
                file_response.raise_for_status()
                assert sha256(file_response.content).hexdigest() == item["file_sha256"]
            checks.append("completed_recommendations_resolve_to_actual_files")
            for suffix in ("run", "resume"):
                response = admin.post(f"{endpoint}/{suffix}")
                response.raise_for_status()
                assert response.json() == completed
            checks.append("completed_task_keeps_result_requests_and_counters")
            responses["completed"] = completed
        responses["cancelled"] = cancelled.json()
        for client in (owner, other, admin):
            client.post("/api/logout").raise_for_status()
            assert client.get(owner_endpoint).status_code == 401
        checks.append("logout_revokes_task_access")
    with maker() as session:
        account = session.get(BudgetAccount, "project")
        assert session.scalar(select(func.count()).select_from(ProviderRequest)) == requests_before
        assert (str(account.spent_cny), str(account.reserved_cny)) == budget_before
    engine.dispose()
    report = {
        "run_id": run_id, "entrypoint": "http://127.0.0.1:8000",
        "status": "passed", "checks": checks,
        "milestone": "HAR-04", "permissions_only": args.permissions_only,
        "completed_result_verified": not args.permissions_only,
        "request_count_unchanged": requests_before,
        "budget_cny_unchanged": {"spent": budget_before[0], "reserved": budget_before[1]},
        "source_sha256": {str(path): sha256(path.read_bytes()).hexdigest() for path in (
            Path("src/shop/harness_api.py"), Path("src/shop/campaign.py"),
            Path("src/shop/harness.py"), Path("scripts/verify_harness_http.py"))},
        "test_user_ids": [account[0].id for account in accounts],
        "responses": responses,
    }
    report_path = evidence_dir / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "passed", "run_id": run_id,
                      "report": str(report_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
