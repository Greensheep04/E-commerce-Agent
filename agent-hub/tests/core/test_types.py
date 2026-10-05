from hashlib import sha256

import pytest
from pydantic import ValidationError

from agent_hub.core import AgentRegistration, ResourceContent, SharedToolRegistration


def test_resource_content_rejects_digest_mismatch():
    with pytest.raises(ValidationError):
        ResourceContent(
            content=b"resource",
            mime_type="application/octet-stream",
            sha256="0" * 64,
        )


def test_resource_content_preserves_verified_bytes():
    value = ResourceContent(
        content=b"resource",
        mime_type="application/octet-stream",
        sha256=sha256(b"resource").hexdigest(),
    )
    assert value.content == b"resource"


def test_agent_registration_rejects_repeated_operation():
    with pytest.raises(ValidationError):
        AgentRegistration(
            agent_id="assessment",
            connector_id="shop",
            module_id="assessment",
            tool_name="assessment_agent",
            description="查询考核任务",
            allowed_operations=["show", "show"],
        )


def test_agent_registration_rejects_identity_field():
    with pytest.raises(ValidationError):
        AgentRegistration(
            agent_id="customer",
            connector_id="shop",
            module_id="customer",
            tool_name="customer_agent",
            description="查询客户资料",
            allowed_operations=["identity"],
            actor_id="supplied-identity",
        )


def test_shared_tool_registration_rejects_empty_operation():
    with pytest.raises(ValidationError):
        SharedToolRegistration(
            connector_id="shop",
            module_id="materials",
            operation="",
            tool_name="materials_search",
            description="查询素材",
        )
