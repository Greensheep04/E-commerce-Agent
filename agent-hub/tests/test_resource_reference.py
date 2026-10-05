import json
import os
from pathlib import Path

import httpx

from agent_hub.config import load_config


async def test_resource_reference_query_checks_type_version_and_sha256():
    inputs = json.loads(Path(os.environ["AGENT_HUB_TEST_INPUTS"]).read_text(encoding="utf-8"))
    config = load_config(Path(os.environ["AGENT_HUB_TEST_CONFIG"])).connectors[0]
    material = inputs["poster_input"]["materials"][0]
    async with httpx.AsyncClient(base_url=config.base_url, trust_env=False) as client:
        login = await client.post("/api/login", json={
            "username": config.username, "password": config.password_file.read_text(encoding="utf-8").strip(),
        })
        assert login.status_code == 200
        path = f'/api/modules/materials/resources/{material["version_id"]}/files/content'
        valid = await client.get(path, params={"resource_type": "asset_version", "sha256": material["file_sha256"]})
        assert valid.status_code == 200
        legacy = await client.get(path)
        assert legacy.status_code == 200
        assert legacy.content == valid.content
        assert legacy.headers["X-Content-SHA256"] == material["file_sha256"]
        for params in ({"resource_type": "asset_version", "version": "999"},
                       {"resource_type": "asset_version", "sha256": "0" * 64},
                       {"resource_type": "task"}, {"version": "999"}):
            assert (await client.get(path, params=params)).status_code in {409, 422}
