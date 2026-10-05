import json
import os
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from integration_protocol import ResourceRef

from agent_hub.config import ConnectorConfig
from agent_hub.connectors.http import HttpConnector
from agent_hub.core import HubError


@pytest.fixture
def inputs():
    return json.loads(Path(os.environ["AGENT_HUB_TEST_INPUTS"]).read_text(encoding="utf-8"))


@pytest.fixture
def connector_config():
    data = json.loads(Path(os.environ["AGENT_HUB_TEST_CONFIG"]).read_text(encoding="utf-8"))
    return ConnectorConfig.model_validate(data["connectors"][0])


async def test_authenticated_directory_and_result_preserve_original_schema(connector_config):
    connector = HttpConnector(connector_config)
    try:
        modules = await connector.describe()
        assert {row.module_id for row in modules} == {"materials", "harness", "customer", "assessment", "poster"}
        result = await connector.invoke("materials", "search", {"limit": 1}, version="1",
                                        request_id=str(uuid4()), upstream=[])
        assert len(result.data["items"]) == 1
        assert set(result.model_dump()) == {"data", "resources", "status", "run_id", "costs"}
        assert all(op.input_schema and op.output_schema for row in modules for op in row.operations if op.supported)
    finally:
        await connector.close()


async def test_wrong_bound_identity_fails_before_directory(connector_config):
    connector = HttpConnector(connector_config.model_copy(update={"expected_actor_id": str(uuid4())}))
    try:
        with pytest.raises(HubError, match="身份"):
            await connector.describe()
    finally:
        await connector.close()


async def test_backend_rejection_and_file_reference_constraints(connector_config, inputs):
    connector = HttpConnector(connector_config)
    material = inputs["poster_input"]["materials"][0]
    try:
        await connector.describe()
        with pytest.raises(HubError):
            await connector.invoke("poster", "resume", {"job_id": inputs["poster_job_id"]},
                                   version="1", request_id=str(uuid4()), upstream=[])
        reference = ResourceRef(module_id="materials", resource_type="asset_version",
                                resource_id=material["version_id"], sha256=material["file_sha256"])
        content = await connector.read_file(reference, "content")
        assert sha256(content.content).hexdigest() == reference.sha256
        assert content.mime_type.startswith("image/")
        for update in ({"version": "999"}, {"sha256": "0" * 64}, {"resource_type": "task"}):
            with pytest.raises(HubError):
                await connector.read_file(reference.model_copy(update=update), "content")
    finally:
        await connector.close()


async def test_backend_not_available_reports_transport_failure(connector_config):
    connector = HttpConnector(connector_config.model_copy(update={"base_url": "http://127.0.0.1:1"}))
    try:
        with pytest.raises(HubError) as failure:
            await connector.describe()
        assert failure.value.code == "backend_unavailable"
    finally:
        await connector.close()


async def test_credential_file_permissions_fail_before_authentication(connector_config, tmp_path):
    path = tmp_path / "exposed.password"
    path.write_bytes(connector_config.password_file.read_bytes())
    path.chmod(0o644)
    connector = HttpConnector(connector_config.model_copy(update={"password_file": path}))
    try:
        with pytest.raises(HubError) as failure:
            await connector.describe()
        assert failure.value.code == "unsafe_credentials"
    finally:
        path.chmod(0o600)
        await connector.close()
