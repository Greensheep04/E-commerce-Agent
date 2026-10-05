import asyncio
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from data_center_protocol import DataQuestion, SourceRegistration
from integration_protocol import ResourceRef
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

from data_center.analytics import AnalyticsEngine
from data_center.config import Settings
from data_center.documents import ConversionPolicy, DocumentEngine
from data_center.graph.engine import GraphConfig
from data_center.ports import ShopPorts
from data_center.runtime import DataRuntime
from data_center.store import DataStore

ROOT = Path(__file__).resolve().parents[2]
IDENTITY_PATH = ROOT / ".scratch/evidence/dc02-host-20261004-r3/identities.json"
COMPLETED_ATTEMPT_PATH = ROOT / ".scratch/evidence/dc-formal-20261004-r5/attempts/DOC-01-1.json"
SOURCE_MANIFEST_PATH = ROOT / ".local/data-center/sources/r4/sources.json"
SOURCE_ROOT = ROOT / ".local/data-center/sources"


@pytest.mark.skipif(os.environ.get("DC_SCOPE_LIVE") != "1",
                    reason="需要显式启用真实 Shop HTTP 与 PostgreSQL 检查")
@pytest.mark.parametrize("change", ["source_revision", "grant"])
@pytest.mark.asyncio
async def test_incomplete_ask_rejects_resume_after_policy_version_change(change):
    state = await live_runtime()
    context = None
    source_id = f"dc-scope/{uuid4().hex}"
    registered = False
    observations = {"change": change, "source_id": source_id,
                    "configuration": state["configuration"], "status": "started"}
    try:
        context = await admin_context(state, f"{source_id}/login")
        await register_document_source(state, source_id, "source-r1", "initial", context)
        registered = True
        question, task = await incomplete_ask(state, source_id, context)
        assert task.status == "queued"
        assert sorted(task.checkpoints) == ["authorization"]
        before = await state["ports"].authorize(
            context, question.source_ids, "read")
        usage_before = await state["ports"].usage(task.task_id, context)
        observations.update({
            "actor_id": context.actor_id,
            "request_id": context.request_id,
            "task_id": task.task_id,
            "run_id": task.run_id,
            "checkpoint_names": sorted(task.checkpoints),
            "policy_version_before": before.policy_version,
            "usage_before": usage_before,
        })

        if change == "source_revision":
            changed = await register_document_source(
                state, source_id, "source-r2", "updated", new_request_context(context))
            observations["source_revision_after"] = changed["revision"]
        else:
            await state["ports"].manage("set_grant", {
                "actor_id": context.actor_id,
                "source_id": source_id,
                "permissions": ["read"],
                "asset_ids": ["markitdown-readme"],
                "source_wide": False,
            }, new_request_context(context))
            observations["grant_changed"] = True

        after = await state["ports"].authorize(context, question.source_ids, "read")
        observations["policy_version_after"] = after.policy_version
        assert before.policy_version != after.policy_version
        with pytest.raises(PermissionError):
            await state["runtime"]._active(task, context)

        usage_after = await state["ports"].usage(task.task_id, context)
        assert usage_after == usage_before
        assert usage_after["requests"] == []
        observations.update({"status": "passed", "usage_after": usage_after,
                             "model_requests_added": 0})
    except Exception as error:
        observations["status"] = "failed"
        observations["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if context is not None and registered:
            if observations.get("grant_changed"):
                await state["ports"].manage("set_grant", {
                    "actor_id": context.actor_id,
                    "source_id": source_id,
                    "permissions": [],
                    "asset_ids": [],
                    "source_wide": False,
                }, new_request_context(context))
                observations["grant_restored"] = True
            await withdraw_source(state, source_id, context)
        save_observation(observations)
        await close_runtime(state)


@pytest.mark.skipif(os.environ.get("DC_SCOPE_LIVE") != "1",
                    reason="需要显式启用真实 Shop HTTP 与 PostgreSQL 检查")
@pytest.mark.asyncio
async def test_completed_result_reads_historical_references_without_new_model_requests():
    state = await live_runtime()
    observations = {"attempt_path": str(COMPLETED_ATTEMPT_PATH.relative_to(ROOT)),
                    "configuration": state["configuration"], "status": "started"}
    try:
        context = await admin_context(state, f"dc-scope/history/{uuid4()}")
        attempt_record = json.loads(COMPLETED_ATTEMPT_PATH.read_text(encoding="utf-8"))
        assert attempt_record["input"]["case_id"] == "DOC-01"
        assert attempt_record["input"]["attempt"] == 1
        assert attempt_record["input"]["actor"] == "admin"
        task_id = attempt_record["observations"][0]["data"]["task_id"]
        task = await asyncio.to_thread(state["runtime"].store.get, task_id, context.actor_id)
        assert task.status == "completed"
        assert task.result is not None
        assert task.run_id == attempt_record["observations"][1]["run_id"]
        original_result = task.result
        usage_before = await state["ports"].usage(task.task_id, context)
        returned = await state["runtime"].run(task.task_id, context)
        assert returned.model_dump(mode="json") == original_result
        current = await asyncio.to_thread(state["runtime"].store.get, task_id, context.actor_id)
        assert current.task_id == task.task_id
        assert current.run_id == task.run_id
        assert current.result == original_result
        usage_after = await state["ports"].usage(task.task_id, context)
        assert usage_after == usage_before
        observations.update({"actor_id": context.actor_id, "request_id": task.request_id,
                             "task_id": task.task_id, "run_id": task.run_id,
                             "historical_evidence_count": len(task.result["data"]["evidence"]),
                             "usage_before": usage_before, "usage_after": usage_after,
                             "model_requests_added": 0, "status": "passed"})
    except Exception as error:
        observations["status"] = "failed"
        observations["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save_observation(observations)
        await close_runtime(state)


async def live_runtime():
    settings = Settings()
    database = create_engine(settings.database_url, pool_pre_ping=True)
    ports = ShopPorts(settings.shop_url)
    documents = DocumentEngine(
        database, settings.artifact_root, ports, ports, ports,
        conversion_policy=ConversionPolicy(
            artifacts_path=settings.docling_models,
            tokenizer_cache=settings.tokenizer_cache,
            tesseract_command=settings.tesseract_command,
        ),
        graph_config=GraphConfig(enabled=settings.graph_enabled, chat_model=settings.graph_model),
    )
    analytics = AnalyticsEngine(database, settings.artifact_root, ports, ports)
    store = DataStore(database)
    return {"database": database, "ports": ports, "runtime": DataRuntime(
        store, ports, documents, analytics, model=settings.model),
        "configuration": {"shop_url": settings.shop_url,
                          "database": {"driver": make_url(settings.database_url).drivername,
                                       "host": make_url(settings.database_url).host,
                                       "port": make_url(settings.database_url).port,
                                       "database": make_url(settings.database_url).database}}}


async def close_runtime(state):
    await state["ports"].close()
    state["database"].dispose()


async def admin_context(state, request_id):
    identity = json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))["actors"]["admin"]
    password = (ROOT / identity["password_file"]).read_text(encoding="utf-8").strip()
    return await state["ports"].login(identity["username"], password, request_id)


def new_request_context(context):
    return context.model_copy(update={"request_id": str(uuid4())})


async def register_document_source(state, source_id, revision, marker, context):
    manifest = json.loads(SOURCE_MANIFEST_PATH.read_text(encoding="utf-8"))
    original = next(item for item in manifest["sources"] if item["document_key"] == "readme-v1")
    source_path = SOURCE_ROOT / original["path"]
    source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    registration = SourceRegistration(
        source_id=source_id,
        kind="document",
        title=original["title"],
        revision=revision,
        nature=original["nature"],
        license_id=original["license_id"],
        sha256=source_sha256,
        properties={"record_id": original["record_id"], "test_marker": marker},
        relative_path=original["path"],
        resource=ResourceRef(
            module_id="data_center", resource_type="source",
            resource_id=f"{source_id}/{revision}/README.md", version=revision,
            sha256=source_sha256,
        ),
    )
    return await state["ports"].manage(
        "register_source", registration.model_dump(mode="json"), context)


async def incomplete_ask(state, source_id, context):
    question = DataQuestion(
        question="读取 markitdown README 的 Python 版本要求",
        source_ids=[source_id],
        retrieval_mode="hybrid",
    )
    task = await state["runtime"].submit(
        "ask", question.model_dump(mode="json"), new_request_context(context))
    task = await asyncio.to_thread(state["runtime"].store.get, task.task_id, context.actor_id)
    return question, task


async def withdraw_source(state, source_id, context):
    await state["ports"].manage("withdraw_source", {"source_id": source_id},
                                new_request_context(context))


def save_observation(observations):
    directory = ROOT / ".scratch/evidence" / f"dc-scope-{uuid4().hex}"
    directory.mkdir(parents=True, exist_ok=False)
    source_files = [
        Path(__file__),
        ROOT / "data-center/src/data_center/runtime.py",
        ROOT / "data-center/src/data_center/store.py",
        ROOT / "src/shop/data_center_host.py",
        ROOT / "src/shop/data_center_registry.py",
    ]
    observations["source_sha256"] = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source_files
    }
    (directory / "report.json").write_text(
        json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
