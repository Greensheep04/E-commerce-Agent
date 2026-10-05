import importlib
import importlib.util
from math import nan

import pytest
from pydantic import SecretStr, ValidationError


def protocol():
    assert importlib.util.find_spec("integration_protocol") is not None
    return importlib.import_module("integration_protocol")


def test_call_context_rejects_identity_expansion():
    api = protocol()
    with pytest.raises(ValidationError):
        api.CallContext(actor_id="actor-1", request_id="request-1", role="admin")
    with pytest.raises(ValidationError):
        api.CallContext(actor_id="", request_id="request-1")
    with pytest.raises(ValidationError):
        api.CallContext(actor_id="actor-1", request_id=123)


def test_credential_stays_private_when_context_is_serialized():
    api = protocol()
    context = api.CallContext(
        actor_id="actor-1", request_id="request-1", credential=SecretStr("signed-credential")
    )
    updated = context.model_copy(update={"request_id": "request-2", "upstream": []})
    assert updated.credential.get_secret_value() == "signed-credential"
    assert updated.model_dump(mode="json") == {
        "actor_id": "actor-1", "request_id": "request-2", "upstream": []
    }
    assert "signed-credential" not in repr(updated)
    assert "credential" not in repr(updated)


def test_resource_reference_preserves_source_identity():
    api = protocol()
    reference = api.ResourceRef(
        module_id="materials", resource_type="asset", resource_id="asset-1",
        version="3", sha256="a" * 64,
    )
    assert reference.model_dump(mode="json") == {
        "module_id": "materials", "resource_type": "asset", "resource_id": "asset-1",
        "version": "3", "sha256": "a" * 64,
    }
    with pytest.raises(ValidationError):
        api.ResourceRef(module_id="materials", resource_type="asset", resource_id="asset-1",
                        sha256="invalid")


def test_operation_result_rejects_non_json_and_unknown_fields():
    api = protocol()
    result = api.OperationResult(
        data={"job_id": "job-1", "values": [1, "saved", None]}, status="completed",
        costs={"spent_cny": "0.01000000"},
    )
    assert result.model_dump(mode="json")["data"] == {
        "job_id": "job-1", "values": [1, "saved", None]
    }
    with pytest.raises(ValidationError):
        api.OperationResult(data={"value": object()}, status="completed")
    with pytest.raises(ValidationError):
        api.OperationResult(data={"value": nan}, status="completed")
    with pytest.raises(ValidationError):
        api.OperationResult(data={}, status="completed", actor_id="actor-1")


def test_module_descriptor_rejects_duplicate_operations():
    api = protocol()
    with pytest.raises(ValidationError):
        api.ModuleDescriptor(
            module_id="materials", operations=[
                api.OperationDescriptor(name="list"), api.OperationDescriptor(name="list")
            ],
        )


def test_module_descriptor_has_explicit_required_capabilities():
    api = protocol()
    descriptor = api.ModuleDescriptor(
        module_id="materials", operations=[api.OperationDescriptor(name="list")],
        required_capabilities=[api.HostCapability.AUTHENTICATION],
    )
    assert descriptor.model_dump(mode="json")["required_capabilities"] == ["authentication"]
    with pytest.raises(ValidationError):
        api.ModuleDescriptor(
            module_id="materials", operations=[api.OperationDescriptor(name="list")],
            required_capabilities=["undefined"],
        )
