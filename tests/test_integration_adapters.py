import asyncio
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from shop.app import create_app
from shop.auth import hash_password
from shop.config import Settings
from shop.integration import ShopIntegration
from shop.models import User
from shop.runtime_models import HarnessTask


def users_and_settings(db_session, data_dir):
    users = [User(username=name, password_hash=hash_password("integration-local-password"),
                  role=role) for name, role in (("integration-admin", "admin"),
                                               ("integration-member", "customer"))]
    db_session.add_all(users)
    db_session.commit()
    settings = Settings(database_url=db_session.info["test_database_url"], data_dir=data_dir,
                        poster_dir=data_dir.parent / "poster",
                        session_secret="integration-authentication-test")
    return users, settings


def test_python_context_requires_authentication_and_checks_task_ownership(db_session, data_dir):
    users, settings = users_and_settings(db_session, data_dir)

    async def check():
        gateway = ShopIntegration(settings)
        await gateway.startup()
        try:
            assert {item.module_id for item in gateway.host.describe()} == {
                "materials", "harness", "customer", "assessment", "poster",
            }
            owner = gateway.login(users[0].username, "integration-local-password", str(uuid4()))
            created = await gateway.invoke("harness", "create", {"prompt": "查询素材"}, owner)
            task_id = created.data["task_id"]
            other = gateway.login(users[1].username, "integration-local-password", str(uuid4()))
            with pytest.raises(PermissionError):
                await gateway.invoke("harness", "show", {"task_id": task_id}, other)
            forged = owner.model_copy(update={"actor_id": users[1].id})
            with pytest.raises(PermissionError):
                await gateway.invoke("harness", "show", {"task_id": task_id}, forged)
            result = await gateway.invoke("harness", "show", {"task_id": task_id}, owner)
            assert result.data["owner_id"] == users[0].id
        finally:
            await gateway.shutdown()

    asyncio.run(check())


def test_concurrent_creation_reuses_task_and_conflicting_payload_is_rejected(db_session, data_dir):
    users, settings = users_and_settings(db_session, data_dir)
    request_id = str(uuid4())

    def create():
        async def call():
            gateway = ShopIntegration(settings)
            await gateway.startup()
            try:
                context = gateway.login(users[0].username, "integration-local-password", request_id)
                result = await gateway.invoke("harness", "create", {"prompt": "查找蓝色上衣"}, context)
                return result.data["task_id"]
            finally:
                await gateway.shutdown()
        return asyncio.run(call())

    with ThreadPoolExecutor(max_workers=2) as pool:
        ids = list(pool.map(lambda _: create(), range(2)))
    assert ids[0] == ids[1]
    assert db_session.scalar(select(func.count()).select_from(HarnessTask)) == 1

    async def conflict():
        gateway = ShopIntegration(settings)
        await gateway.startup()
        try:
            context = gateway.login(users[0].username, "integration-local-password", request_id)
            with pytest.raises(ValueError):
                await gateway.invoke("harness", "create", {"prompt": "改变原始需求"}, context)
        finally:
            await gateway.shutdown()
    asyncio.run(conflict())


def test_http_module_directory_and_identity_input_refusal(db_session, data_dir):
    users, settings = users_and_settings(db_session, data_dir)
    with TestClient(create_app(settings)) as client:
        assert client.get("/api/modules").status_code == 401
        assert client.post("/api/login", json={"username": users[0].username,
                                               "password": "integration-local-password"}).status_code == 200
        response = client.get("/api/modules")
        assert response.status_code == 200
        assert len(response.json()) == 5
        path = "/api/modules/harness/operations/create"
        body = {"version": "1", "request_id": str(uuid4()), "payload": {"prompt": "查询素材"}}
        created = client.post(path, json=body)
        repeated = client.post(path, json=body)
        assert created.status_code == repeated.status_code == 200
        assert created.json()["data"]["task_id"] == repeated.json()["data"]["task_id"]
        assert "credential" not in created.json()
        body["payload"]["owner_id"] = users[1].id
        assert client.post(path, json=body).status_code == 422
