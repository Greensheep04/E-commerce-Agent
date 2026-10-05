from fastapi.testclient import TestClient
from sqlalchemy import func, select

from shop.app import create_app
from shop.config import Settings
from shop.runtime_models import ProviderRequest


def login(client, name):
    response = client.post("/api/login", json={
        "username": name, "password": "assessment-test-password",
    })
    assert response.status_code == 200


def test_assessment_requires_login(db_session, data_dir):
    settings = Settings(
        database_url=db_session.info["test_database_url"],
        data_dir=data_dir,
        session_secret="assessment-test-session",
        assessment_order_adapter=None,
    )
    with TestClient(create_app(settings)) as client:
        response = client.get("/api/assessment/me")
        assert response.status_code == 401


def test_task_data_blocked_is_persisted_without_paid_calls(db_session, data_dir, assessment_accounts):
    settings = Settings(database_url=db_session.info["test_database_url"],
                        data_dir=data_dir, session_secret="assessment-test-session",
                        assessment_order_adapter=None)
    with TestClient(create_app(settings)) as client:
        login(client, "employee")
        created = client.post("/api/assessment/runs", json={"prompt": "查看我的季度销售额"})
        assert created.status_code == 200
        run = created.json()
        endpoint = f"/api/assessment/runs/{run['id']}"
        blocked = client.post(endpoint + "/run")
        assert blocked.status_code == 200
        assert blocked.json()["stop_reason"] == "DATA_BLOCKED"
        assert blocked.json()["metrics"] is None
        assert blocked.json()["result"] is None
        assert blocked.json()["model_calls"] == 0
        assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
        login(client, "other")
        assert client.get(endpoint).status_code == 403
        assert client.get(endpoint + "/export?format=html").status_code == 403
        assert client.get("/api/assessment/runs").json() == []


def test_metric_api_uses_domain_permissions(db_session, data_dir, assessment_accounts):
    settings = Settings(database_url=db_session.info["test_database_url"],
                        data_dir=data_dir, session_secret="assessment-test-session",
                        assessment_order_adapter=None)
    with TestClient(create_app(settings)) as client:
        login(client, "employee")
        assert client.get("/api/assessment/me").json()["allowed_employee_ids"] == [275]
        assert len(client.get("/api/assessment/employees").json()) == 1
        assert client.get("/api/assessment/status").json()["status"] == "DATA_BLOCKED"
        assert client.get("/api/assessment/metrics?scope=employees&employee_ids=276").status_code == 403
        assert client.get("/api/assessment/access").status_code == 403
        login(client, "admin")
        assert len(client.get("/api/assessment/employees").json()) == 17
        assert client.get("/api/assessment/access").status_code == 200
