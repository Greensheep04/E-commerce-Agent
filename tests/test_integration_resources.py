import asyncio
import base64
import json
import os
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from shop.app import create_app
from shop.auth import hash_password
from shop.config import Settings
from shop.fashion import REVISION, SOURCE_ID
from shop.integration import ShopIntegration
from shop.integration_models import IntegrationCall
from shop.materials import MaterialInput, MaterialRepository
from shop.models import Asset, AssetVersion, Source, User
from shop.poster import PosterService
from shop.runtime_models import ProviderRequest

PASSWORD = "if-resource-local-password"
REQUEST_ID = "93000000-0000-0000-0000-000000000001"
SECOND_REQUEST_ID = "93000000-0000-0000-0000-000000000002"
REJECTED_REQUEST_ID = "93000000-0000-0000-0000-000000000003"
RUN_ID = "run-95000000000000000000000000000001"
SECOND_RUN_ID = "run-95000000000000000000000000000002"
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def source_image():
    engine = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop")
    try:
        with Session(engine) as session:
            session.execute(text("SET TRANSACTION READ ONLY"))
            asset = session.scalars(select(Asset).where(
                Asset.source_id == SOURCE_ID, Asset.source_revision == REVISION,
                Asset.kind == "image",
            ).order_by(Asset.source_order, Asset.record_id).limit(1)).one()
            version = asset.versions[0]
            source = session.get(Source, (SOURCE_ID, REVISION))
            assert source is not None
            content = MaterialRepository(session, ROOT / ".local" / "data").read_file(version)
            assert sha256(content).hexdigest() == version.file_sha256
            values = {
                "source_id": asset.source_id, "source_revision": asset.source_revision,
                "record_id": asset.record_id, "title": version.title, "kind": "image",
                "source_order": asset.source_order, "original_fields": version.original_fields,
                "filename": version.filename, "mime_type": version.mime_type, "tags": version.tags,
            }
            source_values = {
                "source_id": source.source_id, "revision": source.revision,
                "license": source.license, "license_note": source.license_note, "url": source.url,
            }
            identity = {
                "source_id": SOURCE_ID, "revision": REVISION, "record_id": asset.record_id,
                "source_order": asset.source_order, "source_asset_id": asset.id,
                "source_version_id": version.id, "file_sha256": version.file_sha256,
                "original_fields": version.original_fields,
            }
    finally:
        engine.dispose()
    configured = os.environ.get("IF_RESOURCES_EVIDENCE_DIR")
    evidence = Path(configured) if configured else ROOT / ".scratch" / "evidence" / (
        "if-resources-inputs-" + uuid4().hex
    )
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "inputs.json").write_text(json.dumps({
        "source": identity, "material_input": values, "request_ids": [
            REQUEST_ID, SECOND_REQUEST_ID, REJECTED_REQUEST_ID,
        ], "poster_keywords": ["商品", "宣传"],
        "milestones": {"materials": "IF-02", "upstream": "IF-04", "files": "IF-04"},
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return source_values, values, content


@pytest.fixture
def resource_inputs(db_session, data_dir, source_image):
    source_values, values, content = source_image
    users = [
        User(id="83000000-0000-0000-0000-000000000001", username="if-resource-admin",
             password_hash=hash_password(PASSWORD), role="admin"),
        User(id="83000000-0000-0000-0000-000000000002", username="if-resource-other",
             password_hash=hash_password(PASSWORD), role="customer"),
    ]
    db_session.add_all([Source(**source_values), *users])
    db_session.commit()
    settings = Settings(
        database_url=db_session.info["test_database_url"], data_dir=data_dir,
        poster_dir=data_dir.parent / "poster-resources",
        session_secret="if-resource-local-authentication", assessment_order_adapter=None,
        _env_file=None,
    )
    yield users, settings, values, content
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0


def material_payload(values, content):
    return {**values, "file_base64": base64.b64encode(content).decode("ascii")}


def save_source(db_session, settings, user, values, content):
    asset = MaterialRepository(db_session, settings.data_dir).save(MaterialInput(
        **values, owner_id=user.id, file_bytes=content,
    ))
    return asset.versions[-1].id


def login(client, user):
    assert client.post("/api/login", json={
        "username": user.username, "password": PASSWORD,
    }).status_code == 200


def prepare_direct(client, version_id, *, request_id=REQUEST_ID, constraints=None):
    body = {
        "request_id": request_id,
        "payload": {"keywords": ["商品", "宣传"], "version_ids": [version_id],
                    "constraints": {} if constraints is None else constraints},
    }
    response = client.post("/api/modules/poster/operations/prepare_direct", json=body)
    assert response.status_code == 200
    return response.json()


def register_source_session(settings, job_id, actor_id, run_id):
    service = PosterService(settings)
    try:
        job = service.load_job(job_id, actor_id)
        session = service.new_session(job, run_id)
        references = session.file_store.artifacts
        assert len(references) == 1
        reference = references[0]
        assert session.final_result is None
        relative_path = "framework/" + reference.uri
        store = session.file_store
        physical_path = store._safe_relative_path(
            store._relative_from_uri(reference.uri), must_exist=True
        )
        return relative_path, physical_path, reference.sha256
    finally:
        service.engine.dispose()


def test_material_admin_save_reuses_version_and_persists_request_mapping(resource_inputs, db_session):
    users, settings, values, content = resource_inputs

    async def exercise():
        gateway = ShopIntegration(settings)
        await gateway.startup()
        try:
            context = gateway.login(users[0].username, PASSWORD, REQUEST_ID)
            payload = material_payload(values, content)
            first = await gateway.invoke("materials", "save", payload, context)
            repeated = await gateway.invoke("materials", "save", payload, context)
            assert first.data == repeated.data
            assert first.data["source_id"] == SOURCE_ID
            assert first.data["source_revision"] == REVISION
            assert len(first.data["versions"]) == 1
            references = [item for item in first.resources if item.resource_type == "asset_version"]
            assert len(references) == 1
            assert references[0].sha256 == sha256(content).hexdigest()
            other = gateway.login(users[1].username, PASSWORD, SECOND_REQUEST_ID)
            with pytest.raises(PermissionError):
                await gateway.invoke("materials", "save", payload, other)
        finally:
            await gateway.shutdown()

    asyncio.run(exercise())
    assert db_session.scalar(select(func.count()).select_from(Asset)) == 1
    assert db_session.scalar(select(func.count()).select_from(AssetVersion)) == 1
    assert db_session.scalar(select(func.count()).select_from(IntegrationCall)) == 1
    saved = db_session.get(IntegrationCall, (users[0].id, "materials", "save", REQUEST_ID))
    assert saved.resource_id == db_session.scalar(select(Asset.id))


def test_material_save_and_mapping_share_database_transaction(resource_inputs, db_session):
    users, settings, values, content = resource_inputs
    version_id = save_source(db_session, settings, users[0], values, content)
    db_session.execute(text(
        "ALTER TABLE integration_calls ADD CONSTRAINT if_resource_reject_request "
        "CHECK (request_id <> '93000000-0000-0000-0000-000000000003')"
    ))
    db_session.commit()

    async def exercise():
        gateway = ShopIntegration(settings)
        await gateway.startup()
        try:
            context = gateway.login(users[0].username, PASSWORD, REJECTED_REQUEST_ID)
            payload = material_payload(values, content)
            payload["title"] = values["title"] + " 事务验证"
            with pytest.raises(IntegrityError):
                await gateway.invoke("materials", "save", payload, context)
        finally:
            await gateway.shutdown()

    asyncio.run(exercise())
    db_session.expire_all()
    assert db_session.scalar(select(func.count()).select_from(AssetVersion)) == 1
    assert db_session.get(AssetVersion, version_id).asset.title == values["title"]
    assert db_session.scalar(select(func.count()).select_from(IntegrationCall)) == 0


def test_returned_material_version_upstream_checks_version_and_sha(resource_inputs, db_session):
    users, settings, values, content = resource_inputs
    save_source(db_session, settings, users[0], values, content)
    asset_id = db_session.scalar(select(Asset.id))

    async def exercise():
        gateway = ShopIntegration(settings)
        await gateway.startup()
        try:
            context = gateway.login(users[0].username, PASSWORD, REQUEST_ID)
            shown = await gateway.invoke("materials", "show", {"asset_id": asset_id}, context)
            references = [item for item in shown.resources if item.resource_type == "asset_version"]
            assert len(references) == 1
            context = context.model_copy(update={"upstream": shown.resources})
            valid = await gateway.invoke("materials", "search", {}, context)
            assert valid.data["items"][0]["id"] == asset_id
            wrong_version = references[0].model_copy(update={"version": "999"})
            wrong_sha = references[0].model_copy(update={"sha256": "0" * 64})
            for reference in (wrong_version, wrong_sha):
                invalid = context.model_copy(update={"upstream": [reference]})
                with pytest.raises(ValueError):
                    await gateway.invoke("materials", "search", {}, invalid)
        finally:
            await gateway.shutdown()

    asyncio.run(exercise())


def test_poster_direct_prepare_show_and_run_wait_for_agents_without_model_requests(resource_inputs, db_session):
    users, settings, values, content = resource_inputs
    version_id = save_source(db_session, settings, users[0], values, content)
    with TestClient(create_app(settings)) as client:
        login(client, users[0])
        prepared = prepare_direct(client, version_id)
        assert prepared["status"] == prepared["data"]["job"]["status"] == "prepared"
        job_id = prepared["data"]["job"]["job_id"]
        repeated = prepare_direct(client, version_id)
        assert repeated["data"]["job"]["job_id"] == job_id
        shown = client.get("/api/poster/jobs/" + job_id)
        assert shown.status_code == 200
        assert shown.json()["data"]["job"]["job_id"] == job_id
        run_body = {"request_id": SECOND_REQUEST_ID, "payload": {"job_id": job_id}}
        waiting = client.post("/api/modules/poster/operations/run", json=run_body)
        assert waiting.status_code == 200
        assert waiting.json()["status"] == "waiting_agents"
        assert waiting.json()["data"]["job"]["status"] == "waiting_agents"
        assert waiting.json()["data"]["cost"]["requests"] == []
        assert client.get("/api/poster/jobs/" + job_id).json()["status"] == "waiting_agents"
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0


def test_registered_poster_file_checks_login_owner_task_and_sha(resource_inputs, db_session):
    users, settings, values, content = resource_inputs
    version_id = save_source(db_session, settings, users[0], values, content)
    with TestClient(create_app(settings)) as client:
        login(client, users[0])
        first = prepare_direct(client, version_id)
        job_id = first["data"]["job"]["job_id"]
        relative, _, digest = register_source_session(settings, job_id, users[0].id, RUN_ID)
        second = prepare_direct(client, version_id, request_id=SECOND_REQUEST_ID,
                                constraints={"resource_boundary": "second_job"})
        second_id = second["data"]["job"]["job_id"]
        assert second_id != job_id
        other_relative, _, _ = register_source_session(settings, second_id, users[0].id, SECOND_RUN_ID)
        generic = f"/api/modules/poster/resources/{job_id}/files/{relative}"
        poster = f"/api/poster/jobs/{job_id}/files/{relative}"
        for path in (generic, poster):
            response = client.get(path)
            assert response.status_code == 200
            assert response.content == content
            assert response.headers["X-Content-SHA256"] == digest == sha256(response.content).hexdigest()
            assert response.headers["X-Content-Type-Options"] == "nosniff"
        cross_task = client.get(f"/api/poster/jobs/{job_id}/files/{other_relative}")
        assert cross_task.status_code == 404
        assert client.get(f"/api/poster/jobs/{job_id}/files/framework/unregistered.image").status_code == 404
        traversal = client.get(f"/api/poster/jobs/{job_id}/files/framework/%2E%2E/%2E%2E/job.json")
        assert traversal.status_code == 409
        assert client.post("/api/logout").status_code == 200
        assert client.get(generic).status_code == 401
        assert client.get(poster).status_code == 401
        login(client, users[1])
        assert client.get(generic).status_code == 403
        assert client.get(poster).status_code == 403


@pytest.mark.parametrize("damage", ["missing", "damaged"])
def test_registered_poster_file_rejects_missing_or_changed_content(resource_inputs, db_session, damage):
    users, settings, values, content = resource_inputs
    version_id = save_source(db_session, settings, users[0], values, content)
    with TestClient(create_app(settings), raise_server_exceptions=False) as client:
        login(client, users[0])
        prepared = prepare_direct(client, version_id)
        job_id = prepared["data"]["job"]["job_id"]
        relative, physical, _ = register_source_session(settings, job_id, users[0].id, RUN_ID)
        path = f"/api/poster/jobs/{job_id}/files/{relative}"
        assert client.get(path).status_code == 200
        if damage == "missing":
            physical.unlink()
            expected = 404
        else:
            physical.write_bytes(content + b"changed-content")
            expected = 409
        rejected = client.get(path)
        assert rejected.status_code == expected
        assert rejected.headers["content-type"].startswith("application/json")
        assert "detail" in rejected.json()


def test_unreviewed_harness_does_not_expose_page_file(resource_inputs):
    users, settings, _, _ = resource_inputs
    with TestClient(create_app(settings)) as client:
        login(client, users[0])
        created = client.post("/api/modules/harness/operations/create", json={
            "request_id": REQUEST_ID, "payload": {"prompt": "引用已有素材资料"},
        })
        assert created.status_code == 200
        data = created.json()["data"]
        assert data["result"] is None
        response = client.get(f"/api/modules/harness/resources/{data['task_id']}/files/page.html")
        assert response.status_code == 409
        assert response.headers["content-type"].startswith("application/json")
