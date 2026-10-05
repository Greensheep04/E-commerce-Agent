from hashlib import sha256
from uuid import uuid4

import httpx
import pytest

import scripts.run_data_center_acceptance as acceptance


@pytest.mark.asyncio
async def test_real_unauthenticated_operation_preserves_http_error_body():
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8002", trust_env=False, timeout=30
    ) as client:
        result = await acceptance.invoke(
            client, "show", {"task_id": str(uuid4())}, str(uuid4())
        )

    assert result["http_status"] == 401
    error = result["error"]
    assert error["content_type"].startswith("application/json")
    assert error["body_excerpt"]
    assert error["body_sha256"] == sha256(error["body_excerpt"].encode()).hexdigest()
    assert error["body_truncated"] is False
    assert "detail" in error["body_excerpt"]


@pytest.mark.asyncio
async def test_real_head_error_preserves_empty_body_without_json_decoding():
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8002", trust_env=False, timeout=30
    ) as client:
        response = await client.head("/health")

    assert response.status_code == 405
    assert response.content == b""
    result = acceptance.operation_response(response)
    assert result["http_status"] == 405
    assert result["error"]["body_excerpt"] == ""
    assert result["error"]["body_sha256"] == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    assert result["error"]["body_truncated"] is False


@pytest.mark.asyncio
async def test_real_successful_response_keeps_original_json():
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8002", trust_env=False, timeout=30
    ) as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert acceptance.operation_response(response) == {"status": "ok"}
