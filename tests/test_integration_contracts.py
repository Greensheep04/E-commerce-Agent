import asyncio
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from integration_host import (
    DuplicateModule,
    InvalidInput,
    InvalidOutput,
    MissingCapability,
    ModuleHost,
    UnknownOperation,
    UnsupportedOperation,
    UnsupportedVersion,
)
from integration_protocol import CallContext, HostCapabilities, HostCapability
from sqlalchemy import func, select

from shop.app import create_app
from shop.auth import hash_password
from shop.config import Settings
from shop.integration import ShopIntegration
from shop.integration_adapters import AssetOutput, MaterialsAdapter
from shop.integration_models import IntegrationCall
from shop.models import User
from shop.runtime_models import ProviderRequest

PASSWORD = "if01-local-contract-password"
REQUEST_ID = "90000000-0000-0000-0000-000000000001"
SECOND_REQUEST_ID = "90000000-0000-0000-0000-000000000002"


@pytest.fixture
def contract_inputs(db_session, data_dir):
    users = [
        User(id="80000000-0000-0000-0000-000000000001", username="if01-contract-admin",
             password_hash=hash_password(PASSWORD), role="admin"),
        User(id="80000000-0000-0000-0000-000000000002", username="if01-contract-member",
             password_hash=hash_password(PASSWORD), role="customer"),
    ]
    db_session.add_all(users)
    db_session.commit()
    settings = Settings(
        database_url=db_session.info["test_database_url"], data_dir=data_dir,
        poster_dir=data_dir.parent / "poster-contract",
        session_secret="if01-local-contract-authentication",
        assessment_order_adapter=None, _env_file=None,
    )
    return users, settings


def test_real_modules_register_and_publish_declared_schemas(contract_inputs):
    _, settings = contract_inputs
    gateway = ShopIntegration(settings)
    try:
        descriptors = gateway.host.describe()
        assert {item.module_id for item in descriptors} == {
            "materials", "harness", "customer", "assessment", "poster",
        }
        for descriptor in descriptors:
            assert descriptor.version == "1"
            assert HostCapability.AUTHENTICATION in descriptor.required_capabilities
            adapter = gateway.adapters[descriptor.module_id]
            for operation in descriptor.operations:
                if operation.supported:
                    assert operation.input_schema == adapter.input_models[operation.name].model_json_schema()
                    assert operation.input_schema["additionalProperties"] is False
                    assert operation.output_schema == adapter.output_models[operation.name].model_json_schema()
        descriptors[0].operations.clear()
        assert gateway.host.describe()[0].operations
        with pytest.raises(DuplicateModule):
            gateway.host.register(gateway.adapters["materials"])
    finally:
        asyncio.run(gateway.shutdown())


@pytest.mark.parametrize("missing", list(HostCapability))
def test_real_modules_require_each_host_capability(contract_inputs, missing):
    _, settings = contract_inputs

    async def exercise():
        gateway = ShopIntegration(settings)
        services = {
            "authentication": gateway.host.capabilities.authentication,
            "persistence": gateway.host.capabilities.persistence,
            "budget": gateway.host.capabilities.budget,
        }
        del services[missing.value]
        host = ModuleHost(HostCapabilities(**services))
        for adapter in gateway.adapters.values():
            host.register(adapter)
        try:
            with pytest.raises(MissingCapability) as failure:
                await host.startup()
            assert missing.value in str(failure.value)
            assert gateway.worker.tasks == {}
        finally:
            await host.shutdown()
            await gateway.shutdown()

    asyncio.run(exercise())


def test_real_host_rejects_versions_operations_and_illegal_input(contract_inputs, db_session):
    users, settings = contract_inputs

    async def exercise():
        gateway = ShopIntegration(settings)
        await gateway.startup()
        try:
            context = gateway.login(users[0].username, PASSWORD, REQUEST_ID)
            with pytest.raises(UnsupportedVersion):
                await gateway.invoke("materials", "search", {}, context, version="2")
            with pytest.raises(UnknownOperation):
                await gateway.invoke("materials", "unknown_operation", {}, context)
            with pytest.raises(UnsupportedOperation):
                await gateway.invoke("poster", "cancel", {}, context)
            for payload in ({"limit": 0}, {"limit": "1"}, {"actor_id": users[1].id}):
                with pytest.raises(InvalidInput):
                    await gateway.invoke("materials", "search", payload, context)
            result = await gateway.invoke("materials", "search", {}, context)
            assert result.data == {"items": []}
        finally:
            await gateway.shutdown()

    asyncio.run(exercise())
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0


def test_real_credentials_are_private_and_bound_to_authenticated_actor(contract_inputs):
    users, settings = contract_inputs

    async def exercise():
        gateway = ShopIntegration(settings)
        await gateway.startup()
        try:
            context = gateway.login(users[0].username, PASSWORD, REQUEST_ID)
            assert context.credential is not None
            assert "credential" not in context.model_dump(mode="json")
            assert "credential" not in repr(context)
            assert context.credential.get_secret_value() not in context.model_dump_json()
            unsigned = CallContext(actor_id=users[0].id, request_id=REQUEST_ID)
            with pytest.raises(PermissionError):
                await gateway.invoke("materials", "search", {}, unsigned)
            forged = context.model_copy(update={"actor_id": users[1].id})
            with pytest.raises(PermissionError):
                await gateway.invoke("materials", "search", {}, forged)
            result = await gateway.invoke("materials", "search", {}, context)
            assert result.status == "completed"
        finally:
            await gateway.shutdown()

    asyncio.run(exercise())


def test_real_query_output_must_match_registered_model(contract_inputs):
    users, settings = contract_inputs

    async def exercise():
        gateway = ShopIntegration(settings)
        adapter = MaterialsAdapter(gateway)
        # 此实例登记真实AssetOutput，以核查真实查询输出与登记模型不一致时的拒绝行为。
        adapter.output_models["search"] = AssetOutput
        host = ModuleHost(gateway.host.capabilities)
        host.register(adapter)
        await host.startup()
        try:
            context = gateway.login(users[0].username, PASSWORD, REQUEST_ID)
            with pytest.raises(InvalidOutput):
                await host.invoke("materials", "search", {}, context)
        finally:
            await host.shutdown()
            await gateway.shutdown()

    asyncio.run(exercise())


def test_actual_http_preserves_request_id_upstream_and_private_credential(contract_inputs, db_session):
    users, settings = contract_inputs
    path = "/api/modules/harness/operations/create"
    first_body = {"version": "1", "request_id": REQUEST_ID, "payload": {"prompt": "查询素材"}}
    with TestClient(create_app(settings)) as client:
        assert client.get("/api/modules").status_code == 401
        assert client.post(path, json=first_body).status_code == 401
        assert client.post("/api/login", json={
            "username": users[0].username, "password": PASSWORD,
        }).status_code == 200
        directory = client.get("/api/modules")
        assert directory.status_code == 200
        assert {item["module_id"] for item in directory.json()} == {
            "materials", "harness", "customer", "assessment", "poster",
        }
        first = client.post(path, json=first_body)
        assert first.status_code == 200
        first_result = first.json()
        assert "credential" not in first_result
        assert "credential" not in first_result["data"]
        upstream = first_result["resources"]
        assert len(upstream) == 1
        second = client.post(path, json={
            "version": "1", "request_id": SECOND_REQUEST_ID,
            "payload": {"prompt": "根据上游任务查询素材"}, "upstream": upstream,
        })
        assert second.status_code == 200
        saved = db_session.get(IntegrationCall, (
            users[0].id, "harness", "create", SECOND_REQUEST_ID,
        ))
        assert saved is not None
        assert saved.upstream == upstream
        assert saved.resource_id == second.json()["data"]["task_id"]
        assert str(UUID(saved.request_id)) == SECOND_REQUEST_ID
        for identity_field in ("actor_id", "role", "customer_id", "credential"):
            body = {**first_body, identity_field: "body-identity"}
            assert client.post(path, json=body).status_code == 422
        forbidden_payload = {**first_body, "payload": {"prompt": "查询素材", "owner_id": users[1].id}}
        assert client.post(path, json=forbidden_payload).status_code == 422
        unsupported = client.post("/api/modules/poster/operations/cancel", json={
            "request_id": SECOND_REQUEST_ID, "payload": {},
        })
        assert unsupported.status_code == 422
        assert unsupported.json()["detail"]["code"] == "unsupported_operation"
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
