from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from threading import Barrier

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from shop.app import create_app
from shop.auth import hash_password
from shop.config import Settings
from shop.materials import MaterialInput, MaterialRepository
from shop.models import Asset, AssetVersion, Source, User

OPENCV_REVISION = "d21a05f1afee9521f0a700dc4f22dd7dc5f7ac72"
TEXT_SOURCE_ID = "material-delivery-test-text"
TEXT_SOURCE_REVISION = "local-test-input-v1"
VIDEO_SOURCE_ID = "material-delivery-opencv-vtest"
VIDEO_SAMPLE_PATH = (
    Path(__file__).resolve().parents[1]
    / ".scratch/evidence/mat-luna-audit/sources/opencv-vtest.avi"
)
VIDEO_SAMPLE_SHA256 = "45cddc9490be69345cbdab64ca583be65987e864ca408038e648db99e10516cf"
TEXT_PASSWORD = "test-reader-password"
ADMIN_PASSWORD = "test-admin-password"


@pytest.fixture
def material_api(db_session, data_dir):
    admin = User(
        username="delivery-admin",
        password_hash=hash_password(ADMIN_PASSWORD),
        role="admin",
    )
    reader = User(
        username="delivery-reader",
        password_hash=hash_password(TEXT_PASSWORD),
        role="user",
    )
    db_session.add_all(
        [
            admin,
            reader,
            Source(
                source_id=TEXT_SOURCE_ID,
                revision=TEXT_SOURCE_REVISION,
                license_note="本地编写的 UTF-8 测试输入。",
            ),
            Source(
                source_id=VIDEO_SOURCE_ID,
                revision=OPENCV_REVISION,
                url=f"https://github.com/opencv/opencv/blob/{OPENCV_REVISION}/samples/data/vtest.avi",
                license_note=(
                    "OpenCV 根目录 LICENSE 写明 Apache License 2.0；"
                    "固定提交的样本路径没有单独的许可说明。"
                ),
            ),
        ]
    )
    db_session.commit()

    settings = Settings(
        database_url=db_session.info["test_database_url"],
        data_dir=data_dir,
        session_secret="material-delivery-test-session",
        upload_max_bytes=12 * 1024 * 1024,
    )
    app = create_app(settings)
    app.state.sessionmaker = sessionmaker(bind=db_session.bind, expire_on_commit=False)
    try:
        with TestClient(app) as client:
            yield {
                "client": client,
                "session": db_session,
                "data_dir": data_dir,
                "admin": admin,
                "reader": reader,
            }
    finally:
        app.state.engine.dispose()


def upload(
    client,
    *,
    source_id,
    source_revision,
    record_id,
    title,
    kind,
    filename,
    content,
    content_type,
):
    return client.post(
        "/api/assets",
        data={
            "source_id": source_id,
            "source_revision": source_revision,
            "record_id": record_id,
            "title": title,
            "kind": kind,
        },
        files={"file": (filename, content, content_type)},
    )


def test_admin_uploads_utf8_text_and_real_opencv_video(material_api):
    client = material_api["client"]
    admin = material_api["admin"]
    text_content = "素材中心文本附件\nUTF-8 boundary input.\n".encode()
    video_content = VIDEO_SAMPLE_PATH.read_bytes()
    assert sha256(video_content).hexdigest() == VIDEO_SAMPLE_SHA256

    login = client.post(
        "/api/login",
        json={"username": admin.username, "password": ADMIN_PASSWORD},
    )
    assert login.status_code == 200
    assert login.json()["role"] == "admin"

    text_upload = upload(
        client,
        source_id=TEXT_SOURCE_ID,
        source_revision=TEXT_SOURCE_REVISION,
        record_id="utf8-text",
        title="UTF-8 文本附件",
        kind="text",
        filename="source-note.txt",
        content=text_content,
        content_type="text/plain",
    )
    assert text_upload.status_code == 200
    text_asset = text_upload.json()
    assert text_asset["kind"] == "text"
    assert text_asset["owner_id"] == admin.id
    assert text_asset["current_version"] == 1
    text_detail = client.get(f"/api/assets/{text_asset['id']}")
    assert text_detail.status_code == 200
    text_version = text_detail.json()["versions"][0]
    assert text_version["filename"] == "source-note.txt"
    assert text_version["file_size"] == len(text_content)
    assert text_version["file_sha256"] == sha256(text_content).hexdigest()
    assert text_version["mime_type"] == "text/plain; charset=utf-8"
    text_file = client.get(f"/api/assets/{text_asset['id']}/versions/1/file")
    assert text_file.status_code == 200
    assert text_file.content == text_content
    assert text_file.headers["content-type"].startswith("text/plain")
    assert text_file.headers["content-disposition"].startswith("inline")
    assert text_file.headers["x-content-type-options"] == "nosniff"

    video_upload = upload(
        client,
        source_id=VIDEO_SOURCE_ID,
        source_revision=OPENCV_REVISION,
        record_id="opencv-vtest",
        title="OpenCV vtest AVI attachment",
        kind="video",
        filename="vtest.avi",
        content=video_content,
        content_type="video/x-msvideo",
    )
    assert video_upload.status_code == 200
    video_asset = video_upload.json()
    assert video_asset["kind"] == "video"
    assert video_asset["owner_id"] == admin.id
    assert video_asset["current_version"] == 1
    video_detail = client.get(f"/api/assets/{video_asset['id']}")
    assert video_detail.status_code == 200
    video_version = video_detail.json()["versions"][0]
    assert video_version["filename"] == "vtest.avi"
    assert video_version["file_size"] == 8_131_690
    assert video_version["file_sha256"] == VIDEO_SAMPLE_SHA256
    assert video_version["mime_type"] == "application/octet-stream"
    video_file = client.get(f"/api/assets/{video_asset['id']}/versions/1/file")
    assert video_file.status_code == 200
    assert video_file.content == video_content
    assert video_file.headers["content-type"].startswith("application/octet-stream")
    assert video_file.headers["content-disposition"].startswith("attachment")
    assert video_file.headers["x-content-type-options"] == "nosniff"


def test_reader_cannot_upload_or_change_material_metadata(material_api):
    client = material_api["client"]
    admin = material_api["admin"]
    reader = material_api["reader"]
    content = b"permission boundary input\n"
    upload_fields = {
        "source_id": TEXT_SOURCE_ID,
        "source_revision": TEXT_SOURCE_REVISION,
        "record_id": "permission-boundary",
        "title": "Permission boundary",
        "kind": "text",
    }

    anonymous_upload = client.post(
        "/api/assets",
        data=upload_fields,
        files={"file": ("permission.txt", content, "text/plain")},
    )
    assert anonymous_upload.status_code == 401

    reader_login = client.post(
        "/api/login",
        json={"username": reader.username, "password": TEXT_PASSWORD},
    )
    assert reader_login.status_code == 200
    denied_upload = client.post(
        "/api/assets",
        data=upload_fields,
        files={"file": ("permission.txt", content, "text/plain")},
    )
    assert denied_upload.status_code == 403
    assert client.post("/api/logout").status_code == 200

    admin_login = client.post(
        "/api/login",
        json={"username": admin.username, "password": ADMIN_PASSWORD},
    )
    assert admin_login.status_code == 200
    created = upload(
        client,
        source_id=TEXT_SOURCE_ID,
        source_revision=TEXT_SOURCE_REVISION,
        record_id="permission-boundary",
        title="Permission boundary",
        kind="text",
        filename="permission.txt",
        content=content,
        content_type="text/plain",
    )
    assert created.status_code == 200
    asset_id = created.json()["id"]
    assert client.post("/api/logout").status_code == 200

    reader_login = client.post(
        "/api/login",
        json={"username": reader.username, "password": TEXT_PASSWORD},
    )
    assert reader_login.status_code == 200
    denied_update = client.put(
        f"/api/assets/{asset_id}",
        json={"title": "Unauthorized edit", "tags": [], "owner_id": reader.id},
    )
    assert denied_update.status_code == 403
    assert client.get(f"/manage/materials/{asset_id}/edit").status_code == 403
    detail = client.get(f"/api/assets/{asset_id}")
    assert detail.status_code == 200
    assert detail.json()["current_version"] == 1
    assert detail.json()["title"] == "Permission boundary"

    records = material_api["session"].scalars(
        select(Asset).where(Asset.source_id == TEXT_SOURCE_ID)
    ).all()
    assert len(records) == 1


def test_file_replacement_keeps_prior_file_and_owner_versions(material_api):
    client = material_api["client"]
    admin = material_api["admin"]
    reader = material_api["reader"]
    assert client.post(
        "/api/login",
        json={"username": admin.username, "password": ADMIN_PASSWORD},
    ).status_code == 200

    first_content = b"first uploaded text\n"
    replacement_content = b"replacement file content\n"
    first_upload = upload(
        client,
        source_id=TEXT_SOURCE_ID,
        source_revision=TEXT_SOURCE_REVISION,
        record_id="version-history",
        title="First upload",
        kind="text",
        filename="version.txt",
        content=first_content,
        content_type="text/plain",
    )
    assert first_upload.status_code == 200
    asset_id = first_upload.json()["id"]

    metadata_update = client.put(
        f"/api/assets/{asset_id}",
        json={
            "title": "Metadata revision",
            "tags": ["reviewed"],
            "owner_id": reader.id,
        },
    )
    assert metadata_update.status_code == 200
    assert metadata_update.json()["current_version"] == 2

    file_replacement = upload(
        client,
        source_id=TEXT_SOURCE_ID,
        source_revision=TEXT_SOURCE_REVISION,
        record_id="version-history",
        title="Replacement upload",
        kind="text",
        filename="version.txt",
        content=replacement_content,
        content_type="text/plain",
    )
    assert file_replacement.status_code == 200
    assert file_replacement.json()["id"] == asset_id
    assert file_replacement.json()["current_version"] == 3

    detail = client.get(f"/api/assets/{asset_id}")
    assert detail.status_code == 200
    asset = detail.json()
    versions = asset["versions"]
    assert [version["version"] for version in versions] == [1, 2, 3]
    assert asset["owner_id"] == admin.id
    assert [
        (version["title"], version["owner_id"], version["file_sha256"])
        for version in versions
    ] == [
        ("First upload", admin.id, sha256(first_content).hexdigest()),
        ("Metadata revision", reader.id, sha256(first_content).hexdigest()),
        ("Replacement upload", admin.id, sha256(replacement_content).hexdigest()),
    ]
    assert versions[1]["tags"] == ["reviewed"]
    assert versions[0]["tags"] == []

    for number, expected_content in (
        (1, first_content),
        (2, first_content),
        (3, replacement_content),
    ):
        response = client.get(f"/api/assets/{asset_id}/versions/{number}/file")
        assert response.status_code == 200
        assert response.content == expected_content


def test_changed_material_file_is_rejected_by_checksum(material_api):
    client = material_api["client"]
    admin = material_api["admin"]
    assert client.post(
        "/api/login",
        json={"username": admin.username, "password": ADMIN_PASSWORD},
    ).status_code == 200
    content = b"checksum source file\n"
    created = upload(
        client,
        source_id=TEXT_SOURCE_ID,
        source_revision=TEXT_SOURCE_REVISION,
        record_id="changed-file",
        title="Checksum source",
        kind="text",
        filename="checksum.txt",
        content=content,
        content_type="text/plain",
    )
    assert created.status_code == 200
    asset_id = created.json()["id"]
    version = material_api["session"].scalar(
        select(AssetVersion).where(
            AssetVersion.asset_id == asset_id,
            AssetVersion.version == 1,
        )
    )
    stored_path = material_api["data_dir"] / version.file_path
    stored_path.write_bytes(b"changed bytes after upload\n")

    response = client.get(f"/api/assets/{asset_id}/versions/1/file")
    assert response.status_code == 400
    assert response.json()["detail"] == "素材文件校验失败"


def test_missing_material_file_returns_not_found(material_api):
    client = material_api["client"]
    admin = material_api["admin"]
    assert client.post(
        "/api/login",
        json={"username": admin.username, "password": ADMIN_PASSWORD},
    ).status_code == 200
    created = upload(
        client,
        source_id=TEXT_SOURCE_ID,
        source_revision=TEXT_SOURCE_REVISION,
        record_id="missing-file",
        title="Missing file",
        kind="text",
        filename="missing.txt",
        content=b"file removed for boundary test\n",
        content_type="text/plain",
    )
    assert created.status_code == 200
    asset_id = created.json()["id"]
    version = material_api["session"].scalar(
        select(AssetVersion).where(
            AssetVersion.asset_id == asset_id,
            AssetVersion.version == 1,
        )
    )
    (material_api["data_dir"] / version.file_path).unlink()

    response = client.get(f"/api/assets/{asset_id}/versions/1/file")
    assert response.status_code == 404
    assert response.json()["detail"] == "素材文件不存在"


def test_text_upload_rejects_invalid_utf8(material_api):
    client = material_api["client"]
    admin = material_api["admin"]
    assert client.post(
        "/api/login",
        json={"username": admin.username, "password": ADMIN_PASSWORD},
    ).status_code == 200
    response = upload(
        client,
        source_id=TEXT_SOURCE_ID,
        source_revision=TEXT_SOURCE_REVISION,
        record_id="invalid-utf8",
        title="Invalid UTF-8",
        kind="text",
        filename="invalid.txt",
        content=b"\xff",
        content_type="text/plain",
    )
    assert response.status_code == 400
    assert material_api["session"].scalar(
        select(Asset).where(
            Asset.source_id == TEXT_SOURCE_ID,
            Asset.record_id == "invalid-utf8",
        )
    ) is None


def test_concurrent_saves_keep_sequential_material_versions(material_api):
    session = material_api["session"]
    engine = session.get_bind()
    data_dir = material_api["data_dir"]
    admin = material_api["admin"]
    initial = MaterialInput(
        source_id=TEXT_SOURCE_ID,
        source_revision=TEXT_SOURCE_REVISION,
        record_id="concurrent-version",
        title="Initial version",
        kind="text",
        owner_id=admin.id,
        source_order=5,
        original_fields={},
        filename="concurrent.txt",
        mime_type="text/plain",
        file_bytes=b"initial version\n",
    )
    initial_asset = MaterialRepository(session, data_dir).save(initial)
    asset_id = initial_asset.id
    write_barrier = Barrier(2)
    makers = sessionmaker(bind=engine, expire_on_commit=False)

    def save_version(record_suffix, content):
        item = MaterialInput(
            source_id=TEXT_SOURCE_ID,
            source_revision=TEXT_SOURCE_REVISION,
            record_id="concurrent-version",
            title=f"Concurrent {record_suffix}",
            kind="text",
            owner_id=admin.id,
            source_order=5,
            original_fields={"writer": record_suffix},
            filename="concurrent.txt",
            mime_type="text/plain",
            file_bytes=content,
        )
        with makers() as writer_session:
            cached_asset = writer_session.get(Asset, asset_id)
            assert cached_asset is not None
            observed_version = cached_asset.versions[-1].version
            write_barrier.wait(timeout=10)
            saved_asset = MaterialRepository(writer_session, data_dir).save(item)
            return observed_version, [version.version for version in saved_asset.versions]

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="version-writer") as executor:
        futures = [
            executor.submit(save_version, "A", b"concurrent writer A\n"),
            executor.submit(save_version, "B", b"concurrent writer B\n"),
        ]
        failures = [future.exception(timeout=20) for future in futures]

    failure_details = [
        f"{type(error).__name__}: {getattr(error, 'orig', error)}"
        for error in failures
        if error is not None
    ]
    assert failure_details == []
    results = [future.result(timeout=20) for future in futures]
    assert [observed_version for observed_version, _ in results] == [1, 1]
    assert sorted(len(versions) for _, versions in results) == [2, 3]
    with makers() as read_session:
        saved_asset = read_session.get(Asset, asset_id)
        assert [version.version for version in saved_asset.versions] == [1, 2, 3]
        assert {version.file_sha256 for version in saved_asset.versions} == {
            sha256(b"initial version\n").hexdigest(),
            sha256(b"concurrent writer A\n").hexdigest(),
            sha256(b"concurrent writer B\n").hexdigest(),
        }
