import json
import os
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from data_center_protocol import ModelInvocation
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from shop.config import Settings
from shop.data_center_gateway import ShopDataModelGateway
from shop.data_center_host import ShopDataAuthorization
from shop.integration import ShopAuthentication


@pytest.mark.skipif(os.environ.get("DC_MODEL_TIMEOUT_LIVE") != "1",
                    reason="需要显式启用真实模型超时及持久费用检查")
@pytest.mark.asyncio
async def test_real_model_timeout_returns_unknown_and_preserves_request_identity():
    identities = json.loads(Path(
        ".scratch/evidence/dc02-host-20261004-r3/identities.json"
    ).read_text())
    registry = json.loads(Path(".local/data-center/sources/r3/sources.json").read_text())
    source_id = next(source["source_id"] for source in registry["sources"]
                     if source["source_key"] == "markitdown:readme-v2")
    settings = Settings()
    database = create_engine(settings.database_url)
    maker = sessionmaker(database, expire_on_commit=False)
    authentication = ShopAuthentication(settings, maker)
    authorization = ShopDataAuthorization(authentication, maker)
    gateway = ShopDataModelGateway(settings, maker, authorization)
    task_id, run_id = str(uuid4()), str(uuid4())
    context = authentication.issue(identities["actors"]["admin"]["actor_id"], str(uuid4()))
    output = Path(os.environ["DC_MODEL_TIMEOUT_OUTPUT"])
    output.mkdir(parents=True, exist_ok=False)
    request = ModelInvocation(
        task_id=task_id, run_id=run_id, step_key="actual-timeout",
        kind="chat", model="qwen3.8-flash", source_ids=[source_id],
        messages=[{"role": "user", "content": "请返回OK。"}],
        max_output_tokens=32, timeout_seconds=0.001,
    )
    try:
        await gateway.bind(task_id, run_id, "query", [source_id], {
            "question": "检查真实模型超时后的持久unknown状态",
        }, context)
        reply, error = None, None
        try:
            reply = await gateway.invoke(request, context)
        except (TimeoutError, httpx.TimeoutException) as exception:
            error = {"type": type(exception).__name__, "message": str(exception)}
        lookup = await gateway.lookup(task_id, request.step_key, context)
        usage = await gateway.usage(task_id, context)
        report = {"task_id": task_id, "run_id": run_id,
                  "timeout_seconds": request.timeout_seconds, "error": error,
                  "reply": reply.model_dump(mode="json") if reply else None,
                  "lookup": lookup.model_dump(mode="json") if lookup else None,
                  "usage_before_reuse": usage}
        (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        assert reply is not None and reply.status == "unknown"
        assert lookup is not None and lookup.request_id == reply.request_id
        assert lookup.reserved_cny > 0 and lookup.spent_cny == 0
        assert len(usage["requests"]) == 1 and usage["requests"][0]["status"] == "unknown"
        reused = await gateway.invoke(request, context)
        assert reused == reply
        assert await gateway.usage(task_id, context) == usage
        report["reused"] = reused.model_dump(mode="json")
        report["status"] = "passed"
        (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    finally:
        database.dispose()
