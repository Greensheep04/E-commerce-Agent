from pathlib import Path
from secrets import token_urlsafe
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from shop.app import create_app
from shop.auth import hash_password
from shop.config import Settings
from shop.data_center_models import DataCenterTaskBinding
from shop.models import User
from shop.runtime_models import ProviderRequest
from tests.test_data_center_host import document_source


def service_application(db_session, tmp_path: Path):
    user = User(username="service-identity-reader", password_hash=hash_password("local-test"),
                role="viewer")
    db_session.add(user)
    db_session.commit()
    document_source(db_session, user.id)
    token_file = tmp_path / "service.key"
    token_file.write_text(token_urlsafe(48), encoding="utf-8")
    token_file.chmod(0o600)
    settings = Settings(
        database_url=db_session.info["test_database_url"],
        session_secret="service-identity-session",
        cluster_config_file=None,
        data_center_url=None,
        data_center_service_token_file=token_file,
    )
    return create_app(settings), token_file


async def credentials(client):
    request_id = str(uuid4())
    response = await client.post("/api/data-center/host/login", json={
        "username": "service-identity-reader", "password": "local-test", "request_id": request_id,
    })
    assert response.status_code == 200
    identity = response.json()
    return {"Authorization": "Bearer " + identity["credential"],
            "X-Shop-Actor": identity["actor_id"], "X-Request-ID": request_id}, identity


def model_body(task_id, run_id):
    return {"task_id": task_id, "run_id": run_id, "step_key": "answer", "kind": "chat",
            "model": "deepseek-flash", "source_ids": ["project-shop/interfaces"],
            "messages": [{"role": "user", "content": "说明公共接口。"}]}


@pytest.mark.asyncio
@pytest.mark.parametrize("service_header", ["missing", "unrelated", "user_credential"])
async def test_user_credentials_cannot_access_internal_task_and_model_endpoints(
    db_session, tmp_path, service_header,
):
    app, _ = service_application(db_session, tmp_path)
    task_id, run_id = str(uuid4()), str(uuid4())
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://local-shop") as client:
            headers, identity = await credentials(client)
            if service_header == "unrelated":
                headers["X-Data-Center-Service-Token"] = token_urlsafe(48)
            elif service_header == "user_credential":
                headers["X-Data-Center-Service-Token"] = identity["credential"]
            bodies = {
                "tasks/bind": {"task_id": task_id, "run_id": run_id, "operation": "query",
                               "source_ids": ["project-shop/interfaces"],
                               "payload": {"question": "说明公共接口。"}},
                "models/invoke": model_body(task_id, run_id),
                "models/reuse": {"invocation": model_body(task_id, run_id),
                                 "original_request_id": str(uuid4())},
                "models/lookup": {"task_id": task_id, "step_key": "answer"},
                "tasks/usage": {"task_id": task_id},
                "tasks/cancel": {"task_id": task_id},
            }
            responses = [await client.post("/api/data-center/host/" + path, json=body,
                                           headers=headers) for path, body in bodies.items()]
            assert [response.status_code for response in responses] == [401] * 6
        db_session.expire_all()
        assert db_session.get(DataCenterTaskBinding, task_id) is None
        assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
    finally:
        await app.state.integration.shutdown()
        app.state.customer.engine.dispose()
        app.state.engine.dispose()


@pytest.mark.asyncio
async def test_service_identity_preserves_user_authorization_and_cancellation(db_session, tmp_path):
    app, token_file = service_application(db_session, tmp_path)
    task_id, run_id = str(uuid4()), str(uuid4())
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://local-shop") as client:
            headers, _ = await credentials(client)
            headers["X-Data-Center-Service-Token"] = token_file.read_text(encoding="utf-8")
            body = {"task_id": task_id, "run_id": run_id, "operation": "query",
                    "source_ids": ["project-shop/interfaces"],
                    "payload": {"question": "说明公共接口。"}}
            bound = await client.post("/api/data-center/host/tasks/bind", json=body, headers=headers)
            assert bound.status_code == 200
            lookup = await client.post("/api/data-center/host/models/lookup",
                                       json={"task_id": task_id, "step_key": "answer"}, headers=headers)
            assert lookup.status_code == 200 and lookup.json() is None
            usage = await client.post("/api/data-center/host/tasks/usage",
                                      json={"task_id": task_id}, headers=headers)
            assert usage.status_code == 200 and usage.json()["requests"] == []
            unauthorized = await client.post("/api/data-center/host/tasks/bind", json={
                **body, "task_id": str(uuid4()), "run_id": str(uuid4()), "source_ids": ["unregistered"],
            }, headers=headers)
            assert unauthorized.status_code == 403
            cancelled = await client.post("/api/data-center/host/tasks/cancel",
                                          json={"task_id": task_id}, headers=headers)
            assert cancelled.status_code == 200
            rejected = await client.post("/api/data-center/host/models/invoke",
                                         json=model_body(task_id, run_id), headers=headers)
            assert rejected.status_code == 403
            reuse_rejected = await client.post("/api/data-center/host/models/reuse", json={
                "invocation": model_body(task_id, run_id), "original_request_id": str(uuid4()),
            }, headers=headers)
            assert reuse_rejected.status_code == 403
            identity_rejected = await client.post("/api/data-center/host/tasks/usage",
                json={"task_id": task_id},
                headers={"X-Data-Center-Service-Token": headers["X-Data-Center-Service-Token"]})
            assert identity_rejected.status_code == 401
        db_session.expire_all()
        assert db_session.get(DataCenterTaskBinding, task_id).cancel_requested is True
        assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
    finally:
        await app.state.integration.shutdown()
        app.state.customer.engine.dispose()
        app.state.engine.dispose()
