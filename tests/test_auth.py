from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from scripts.manage import init_admin
from shop.app import create_app
from shop.auth import hash_password
from shop.config import Settings
from shop.fashion import REVISION, SOURCE_ID, row_to_material
from shop.materials import MaterialInput, MaterialRepository
from shop.models import Source, User


def test_api_access_and_file_path(db_session, data_dir: Path, source_row):
    user = User(username="reader", password_hash=hash_password("reader-pass"), role="user")
    admin = User(username="admin", password_hash=hash_password("admin-pass"), role="admin")
    db_session.add_all([user, admin])
    db_session.add(Source(source_id=SOURCE_ID, revision=REVISION))
    db_session.commit()
    asset = MaterialRepository(db_session, data_dir).save(row_to_material(source_row, 0, admin.id))
    text_asset = MaterialRepository(db_session, data_dir).save(MaterialInput(
        source_id=SOURCE_ID, source_revision=REVISION, record_id="test-input-text",
        title="测试输入：文本", kind="text", owner_id=admin.id, source_order=1,
        original_fields={"test_input": True}, filename="test.html", mime_type="text/html",
        file_bytes=b"<script>alert(1)</script>",
    ))
    settings = Settings(
        database_url=db_session.info["test_database_url"], data_dir=data_dir,
        session_secret="test-secret", upload_max_bytes=10,
    )
    app = create_app(settings)
    app.state.sessionmaker = sessionmaker(bind=db_session.bind, expire_on_commit=False)
    with TestClient(app) as client:
        assert client.get("/api/assets").status_code == 401
        assert client.post("/api/imports/fashion").status_code == 401
        assert client.post("/api/login", json={"username": "reader", "password": "reader-pass"}).status_code == 200
        assert client.post("/api/imports/fashion").status_code == 403
        assert client.get(f"/manage/materials/{asset.id}/edit").status_code == 403
        assert client.get("/admin/asset/list", follow_redirects=False).status_code == 302
        assert client.get("/api/assets", params={"source_id": SOURCE_ID, "tag": asset.tags[0]}).json()[0]["id"] == asset.id
        file_response = client.get(f"/api/assets/{asset.id}/versions/1/file")
        assert file_response.status_code == 200
        assert file_response.headers["x-content-type-options"] == "nosniff"
        assert file_response.content == row_to_material(source_row, 0, admin.id).file_bytes
        text_response = client.get(f"/api/assets/{text_asset.id}/versions/1/file")
        assert text_response.status_code == 200
        assert text_response.headers["content-type"].startswith("text/plain")
        assert text_response.headers["x-content-type-options"] == "nosniff"
        assert client.post("/api/logout").status_code == 200
        assert client.post("/admin/login", data={"username": "admin"}).status_code == 400
        assert client.post("/api/login", json={"username": "admin", "password": "admin-pass"}).status_code == 200
        assert client.get("/api/assets").status_code == 200
        assert client.get("/admin/").status_code == 200
        assert f"/manage/materials/{asset.id}/edit" in client.get("/admin/asset/list").text
        upload_data = {
            "source_id": SOURCE_ID, "source_revision": REVISION,
            "record_id": "test-upload", "title": "测试输入", "kind": "image",
        }
        assert client.post(
            "/api/assets", data=upload_data,
            files={"file": ("test.png", b"", "image/png")},
        ).status_code == 422
        assert client.post(
            "/api/assets", data={**upload_data, "kind": "invalid"},
            files={"file": ("test.png", b"abc", "image/png")},
        ).status_code == 422
        assert client.post(
            "/api/assets", data=upload_data,
            files={"file": ("test.png", b"more than ten bytes", "image/png")},
        ).status_code == 413
        editor_response = client.get(f"/manage/materials/{asset.id}/edit")
        assert editor_response.status_code == 200
        assert f"/api/assets/{asset.id}/versions/1/file" in editor_response.text
        update = client.put(
            f"/api/assets/{asset.id}",
            json={"title": "测试输入：修订名称", "tags": ["测试输入"], "owner_id": admin.id},
        )
        assert update.status_code == 200
        assert update.json()["current_version"] == 2
        assert client.get(f"/api/assets/{asset.id}/versions/2/file").content == file_response.content
        form_update = client.post(
            f"/manage/materials/{asset.id}/edit",
            data={"title": "测试输入：页面修订", "tags": "测试输入", "owner_id": admin.id},
            follow_redirects=False,
        )
        assert form_update.status_code == 303
        assert client.get(f"/api/assets/{asset.id}").json()["current_version"] == 3
        db_session.expire_all()
        asset = db_session.get(type(asset), asset.id)
        asset.versions[-1].file_path = "../../outside.txt"
        db_session.commit()
        assert client.get(f"/api/assets/{asset.id}/versions/3/file").status_code == 400


def test_init_admin_file_conflict_keeps_database_empty(db_session, data_dir: Path):
    password_file = data_dir / "auth" / "admin.password"
    password_file.parent.mkdir(parents=True)
    password_file.write_text("test-input", encoding="utf-8")
    settings = Settings(
        database_url=db_session.info["test_database_url"], data_dir=data_dir,
        session_secret_file=data_dir / "auth" / "session.key",
        admin_password_file=password_file,
    )
    with pytest.raises(FileExistsError):
        init_admin("admin", settings)
    assert db_session.scalar(select(User).where(User.username == "admin")) is None
