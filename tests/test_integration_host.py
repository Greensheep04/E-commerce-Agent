import asyncio
import importlib
import importlib.util
import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError


def host_api():
    assert importlib.util.find_spec("integration_host") is not None
    return importlib.import_module("integration_host")


def test_public_packages_have_independent_build_metadata():
    root = Path(__file__).resolve().parents[1]
    for package in ("integration-protocol", "integration-host"):
        path = root / "packages" / package / "pyproject.toml"
        assert path.is_file()
        metadata = tomllib.loads(path.read_text())
        assert metadata["project"]["name"] == package
        assert not any("shop" in dependency for dependency in metadata["project"]["dependencies"])
        assert metadata["build-system"]["build-backend"] == "hatchling.build"


def test_host_rejects_invalid_adapter_registration():
    api = host_api()
    host = api.ModuleHost()
    with pytest.raises(api.InvalidAdapter):
        host.register(object())


def test_host_requires_startup_and_rejects_unknown_module():
    api = host_api()
    protocol = importlib.import_module("integration_protocol")
    host = api.ModuleHost()
    context = protocol.CallContext(actor_id="actor-1", request_id="request-1")

    async def exercise():
        with pytest.raises(api.HostNotStarted):
            await host.invoke("materials", "list", {}, context)
        await host.startup()
        with pytest.raises(api.UnknownModule):
            await host.invoke("materials", "list", {}, context)
        await host.shutdown()
        with pytest.raises(api.HostNotStarted):
            await host.invoke("materials", "list", {}, context)

    asyncio.run(exercise())


def test_http_router_requires_explicit_server_context_dependency():
    api = host_api()
    host = api.ModuleHost()
    with pytest.raises(TypeError):
        api.create_router(host)


@pytest.mark.parametrize("identity_field", ["actor_id", "role", "customer_id"])
def test_http_envelope_rejects_body_identity(identity_field):
    api = host_api()
    with pytest.raises(ValidationError):
        api.OperationRequest.model_validate(
            {"version": "1", "request_id": "request-1", "payload": {},
             identity_field: "expanded-identity"}
        )
