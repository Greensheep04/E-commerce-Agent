import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from subprocess import run
from urllib.request import urlopen
from uuid import uuid4

import httpx
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import Session

from shop.auth import hash_password
from shop.config import Settings
from shop.fashion import REVISION, SOURCE_ID
from shop.models import Asset, AssetVersion, Source, User

OPENCV_REVISION = "d21a05f1afee9521f0a700dc4f22dd7dc5f7ac72"
OPENCV_VIDEO_URL = (
    f"https://raw.githubusercontent.com/opencv/opencv/{OPENCV_REVISION}/"
    "samples/data/vtest.avi"
)
OPENCV_VIDEO_PAGE = (
    f"https://github.com/opencv/opencv/blob/{OPENCV_REVISION}/samples/data/vtest.avi"
)
OPENCV_LICENSE_PAGE = (
    f"https://github.com/opencv/opencv/blob/{OPENCV_REVISION}/LICENSE"
)
OPENCV_VIDEO_PATH = Path(
    ".scratch/evidence/mat-luna-audit/sources/opencv-vtest.avi"
)
OPENCV_VIDEO_SIZE = 8_131_690
OPENCV_VIDEO_SHA256 = (
    "45cddc9490be69345cbdab64ca583be65987e864ca408038e648db99e10516cf"
)
OPENCV_VIDEO_BLOB_SHA1 = "965ab12bdb5e498297cefb476e4efb8204c6a29a"
OPENCV_LICENSE_BLOB_SHA1 = "d645695673349e3947e8e5ae42332d0ac3164cd7"


def source_identity(run_id: str) -> dict:
    return {
        "run_id": run_id,
        "text_source_id": f"mat-luna-http-text-{run_id}",
        "text_revision": f"local-input-{run_id}",
        "video_source_id": f"mat-luna-http-opencv-{run_id}",
        "video_revision": OPENCV_REVISION,
        "image_source_id": f"mat-luna-http-image-{run_id}",
        "image_revision": REVISION,
        "reader_username": f"mat-luna-reader-{run_id}",
        "reader_id": str(uuid4()),
        "reader_password": uuid4().hex,
    }


def check_video_sample() -> bytes:
    content = OPENCV_VIDEO_PATH.read_bytes()
    actual_sha256 = sha256(content).hexdigest()
    if len(content) != OPENCV_VIDEO_SIZE:
        raise ValueError(
            f"OpenCV 视频大小不符：预期 {OPENCV_VIDEO_SIZE}，实际 {len(content)}"
        )
    if actual_sha256 != OPENCV_VIDEO_SHA256:
        raise ValueError(
            f"OpenCV 视频 SHA-256 不符：预期 {OPENCV_VIDEO_SHA256}，实际 {actual_sha256}"
        )
    return content


def prepare_video_sample() -> dict:
    if not OPENCV_VIDEO_PATH.exists():
        OPENCV_VIDEO_PATH.parent.mkdir(parents=True, exist_ok=True)
        with urlopen(OPENCV_VIDEO_URL, timeout=60) as response:
            content = response.read(OPENCV_VIDEO_SIZE + 1)
        if len(content) != OPENCV_VIDEO_SIZE:
            raise ValueError(
                f"OpenCV 视频大小不符：预期 {OPENCV_VIDEO_SIZE}，实际 {len(content)}"
            )
        if sha256(content).hexdigest() != OPENCV_VIDEO_SHA256:
            raise ValueError("OpenCV 视频 SHA-256 与固定来源不符")
        OPENCV_VIDEO_PATH.write_bytes(content)

    content = check_video_sample()
    media_result = run(
        ["file", str(OPENCV_VIDEO_PATH)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    run_id = f"source-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_dir = Path(".scratch/evidence/mat-luna-audit") / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    report = {
        "run_id": run_id,
        "path": str(OPENCV_VIDEO_PATH),
        "source_url": OPENCV_VIDEO_PAGE,
        "source_revision": OPENCV_REVISION,
        "git_blob_sha1": OPENCV_VIDEO_BLOB_SHA1,
        "sha256": sha256(content).hexdigest(),
        "size": len(content),
        "license_url": OPENCV_LICENSE_PAGE,
        "license_git_blob_sha1": OPENCV_LICENSE_BLOB_SHA1,
        "repository_license": "Apache License 2.0",
        "sample_specific_license_notice": None,
        "license_evidence_limit": (
            "完整阅读的仓库 LICENSE 写明 Apache License 2.0；固定样本路径没有单独的许可或署名说明。"
        ),
        "media_identification": {
            "command": "file",
            "result": media_result,
            "decode_check": "未运行",
        },
        "status": "已核实",
    }
    report_path = evidence_dir / "source.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "status": "已核实",
        "report": str(report_path),
        "sha256": report["sha256"],
        "size": len(content),
    }


def create_audit_rows(engine, identity: dict) -> dict:
    with Session(engine) as session:
        admin = session.scalar(select(User).where(User.username == "admin"))
        if admin is None or admin.role != "admin":
            raise RuntimeError("项目数据库中没有可用的管理员账户")
        original_source = session.get(Source, (SOURCE_ID, REVISION))
        if original_source is None:
            raise RuntimeError("项目数据库中没有固定 Hugging Face 来源记录")
        reader = User(
            id=identity["reader_id"],
            username=identity["reader_username"],
            password_hash=hash_password(identity["reader_password"]),
            role="user",
        )
        session.add_all(
            [
                reader,
                Source(
                    source_id=identity["text_source_id"],
                    revision=identity["text_revision"],
                    license_note="本地编写的 UTF-8 上传检查输入。",
                ),
                Source(
                    source_id=identity["video_source_id"],
                    revision=identity["video_revision"],
                    url=OPENCV_VIDEO_PAGE,
                    license_note=(
                        "OpenCV 仓库 LICENSE 写明 Apache License 2.0；"
                        "固定样本路径没有单独的许可或署名说明。"
                    ),
                ),
                Source(
                    source_id=identity["image_source_id"],
                    revision=identity["image_revision"],
                    license=original_source.license,
                    license_note=original_source.license_note,
                    url=original_source.url,
                ),
            ]
        )
        session.commit()
        identity["admin_id"] = admin.id
        identity["image_origin"] = {
            "source_id": SOURCE_ID,
            "revision": REVISION,
            "license": original_source.license,
            "license_note": original_source.license_note,
            "url": original_source.url,
        }
    return identity


def post_asset(
    client,
    *,
    source_id: str,
    source_revision: str,
    record_id: str,
    title: str,
    kind: str,
    filename: str,
    content: bytes,
    content_type: str,
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


def verify_http(
    settings: Settings,
    identity: dict,
    video_content: bytes,
) -> dict:
    reader_password = identity["reader_password"]
    admin_password = settings.admin_password_file.read_text(encoding="utf-8").strip()
    text_content = f"MAT-03 本地 UTF-8 上传输入 {identity['run_id']}\n".encode()
    replacement_content = (
        f"MAT-03 替换上传输入 {identity['run_id']}\n".encode()
    )
    checks = []
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=60) as client:
        health = client.get("/health")
        health.raise_for_status()
        assert health.json() == {"status": "ok"}
        checks.append("live_health")

        assert client.get("/api/assets").status_code == 401
        anonymous_upload = post_asset(
            client,
            source_id=identity["text_source_id"],
            source_revision=identity["text_revision"],
            record_id="anonymous-upload",
            title="Anonymous upload",
            kind="text",
            filename="anonymous.txt",
            content=b"anonymous request\n",
            content_type="text/plain",
        )
        assert anonymous_upload.status_code == 401
        checks.append("anonymous_upload_rejected")

        reader_login = client.post(
            "/api/login",
            json={
                "username": identity["reader_username"],
                "password": reader_password,
            },
        )
        reader_login.raise_for_status()
        assert reader_login.json()["role"] == "user"
        reader_upload = post_asset(
            client,
            source_id=identity["text_source_id"],
            source_revision=identity["text_revision"],
            record_id="reader-upload",
            title="Reader upload",
            kind="text",
            filename="reader.txt",
            content=b"reader request\n",
            content_type="text/plain",
        )
        assert reader_upload.status_code == 403
        assert client.get(
            "/api/assets",
            params={"source_id": identity["text_source_id"]},
        ).json() == []
        client.post("/api/logout").raise_for_status()
        checks.append("reader_upload_rejected_without_record")

        admin_login = client.post(
            "/api/login",
            json={"username": "admin", "password": admin_password},
        )
        admin_login.raise_for_status()
        assert admin_login.json()["role"] == "admin"
        assert admin_login.json()["id"] == identity["admin_id"]

        original_image_list = client.get(
            "/api/assets",
            params={
                "source_id": SOURCE_ID,
                "source_revision": REVISION,
                "kind": "image",
                "limit": 1,
            },
        )
        original_image_list.raise_for_status()
        original_images = original_image_list.json()
        assert len(original_images) == 1
        original_image = client.get(f"/api/assets/{original_images[0]['id']}")
        original_image.raise_for_status()
        original_image_detail = original_image.json()
        original_image_version = original_image_detail["versions"][-1]
        original_image_file = client.get(
            f"/api/assets/{original_image_detail['id']}/versions/"
            f"{original_image_version['version']}/file"
        )
        original_image_file.raise_for_status()
        original_image_bytes = original_image_file.content
        original_image_sha256 = sha256(original_image_bytes).hexdigest()
        assert original_image_sha256 == original_image_version["file_sha256"]
        image_upload = post_asset(
            client,
            source_id=identity["image_source_id"],
            source_revision=identity["image_revision"],
            record_id=f"copy-{identity['run_id']}",
            title=original_image_detail["title"],
            kind="image",
            filename=original_image_version["filename"],
            content=original_image_bytes,
            content_type=original_image_version["mime_type"],
        )
        image_upload.raise_for_status()
        image_asset = image_upload.json()
        image_detail = client.get(f"/api/assets/{image_asset['id']}")
        image_detail.raise_for_status()
        image_version = image_detail.json()["versions"][0]
        assert image_version["file_sha256"] == original_image_sha256
        copied_image_file = client.get(
            f"/api/assets/{image_asset['id']}/versions/1/file"
        )
        copied_image_file.raise_for_status()
        assert copied_image_file.content == original_image_bytes
        assert image_asset["source_id"] == identity["image_source_id"]
        original_image_after_upload = client.get(
            f"/api/assets/{original_image_detail['id']}"
        )
        original_image_after_upload.raise_for_status()
        assert (
            original_image_after_upload.json()["versions"][-1]["file_sha256"]
            == original_image_sha256
        )
        checks.append("official_source_image_read_and_isolated_upload")

        text_upload = post_asset(
            client,
            source_id=identity["text_source_id"],
            source_revision=identity["text_revision"],
            record_id="text-history",
            title="Text upload",
            kind="text",
            filename="input.txt",
            content=text_content,
            content_type="text/plain",
        )
        text_upload.raise_for_status()
        text_asset = text_upload.json()
        video_upload = post_asset(
            client,
            source_id=identity["video_source_id"],
            source_revision=identity["video_revision"],
            record_id="opencv-vtest",
            title="OpenCV vtest AVI attachment",
            kind="video",
            filename="vtest.avi",
            content=video_content,
            content_type="video/x-msvideo",
        )
        video_upload.raise_for_status()
        video_asset = video_upload.json()
        checks.append("admin_text_and_video_upload")
        assert len({text_asset["id"], video_asset["id"], image_asset["id"]}) == 3

        client.post("/api/logout").raise_for_status()
        reader_login = client.post(
            "/api/login",
            json={
                "username": identity["reader_username"],
                "password": reader_password,
            },
        )
        reader_login.raise_for_status()
        denied_update = client.put(
            f"/api/assets/{text_asset['id']}",
            json={
                "title": "Unauthorized edit",
                "tags": [],
                "owner_id": identity["reader_id"],
            },
        )
        assert denied_update.status_code == 403
        assert client.get(
            f"/manage/materials/{text_asset['id']}/edit"
        ).status_code == 403
        untouched = client.get(f"/api/assets/{text_asset['id']}")
        untouched.raise_for_status()
        assert untouched.json()["current_version"] == 1
        assert untouched.json()["title"] == "Text upload"
        client.post("/api/logout").raise_for_status()
        checks.append("reader_metadata_edit_rejected")

        admin_login = client.post(
            "/api/login",
            json={"username": "admin", "password": admin_password},
        )
        admin_login.raise_for_status()
        metadata_update = client.put(
            f"/api/assets/{text_asset['id']}",
            json={
                "title": "Metadata revision",
                "tags": ["reviewed"],
                "owner_id": identity["reader_id"],
            },
        )
        metadata_update.raise_for_status()
        assert metadata_update.json()["current_version"] == 2
        matching_filters = {
            "q": "Metadata revision",
            "kind": "text",
            "tag": "reviewed",
            "owner_id": identity["reader_id"],
            "source_id": identity["text_source_id"],
            "source_revision": identity["text_revision"],
        }
        filtered_assets = client.get("/api/assets", params=matching_filters)
        filtered_assets.raise_for_status()
        assert [item["id"] for item in filtered_assets.json()] == [text_asset["id"]]
        filter_mismatches = {
            "q": f"absent-{identity['run_id']}",
            "kind": "image",
            "tag": f"absent-{identity['run_id']}",
            "owner_id": f"absent-{identity['run_id']}",
            "source_id": f"absent-{identity['run_id']}",
            "source_revision": f"absent-{identity['run_id']}",
        }
        for name, value in filter_mismatches.items():
            mismatch_query = {**matching_filters, name: value}
            mismatch_response = client.get("/api/assets", params=mismatch_query)
            mismatch_response.raise_for_status()
            assert mismatch_response.json() == [], name
        checks.append("combined_filters_and_six_nonmatching_filters")

        replacement = post_asset(
            client,
            source_id=identity["text_source_id"],
            source_revision=identity["text_revision"],
            record_id="text-history",
            title="Replacement text",
            kind="text",
            filename="input.txt",
            content=replacement_content,
            content_type="text/plain",
        )
        replacement.raise_for_status()
        assert replacement.json()["id"] == text_asset["id"]
        assert replacement.json()["current_version"] == 3

        text_detail = client.get(f"/api/assets/{text_asset['id']}")
        text_detail.raise_for_status()
        text_versions = text_detail.json()["versions"]
        assert [item["version"] for item in text_versions] == [1, 2, 3]
        assert [
            (item["title"], item["owner_id"], item["file_sha256"])
            for item in text_versions
        ] == [
            (
                "Text upload",
                identity["admin_id"],
                sha256(text_content).hexdigest(),
            ),
            (
                "Metadata revision",
                identity["reader_id"],
                sha256(text_content).hexdigest(),
            ),
            (
                "Replacement text",
                identity["admin_id"],
                sha256(replacement_content).hexdigest(),
            ),
        ]
        assert text_versions[1]["tags"] == ["reviewed"]
        for number, expected_content in (
            (1, text_content),
            (2, text_content),
            (3, replacement_content),
        ):
            file_response = client.get(
                f"/api/assets/{text_asset['id']}/versions/{number}/file"
            )
            file_response.raise_for_status()
            assert file_response.content == expected_content
            assert file_response.headers["x-content-type-options"] == "nosniff"
        checks.append("text_file_replacement_and_version_history")

        video_detail = client.get(f"/api/assets/{video_asset['id']}")
        video_detail.raise_for_status()
        video_version = video_detail.json()["versions"][0]
        assert video_asset["kind"] == "video"
        assert video_version["filename"] == "vtest.avi"
        assert video_version["file_size"] == OPENCV_VIDEO_SIZE
        assert video_version["file_sha256"] == OPENCV_VIDEO_SHA256
        assert video_version["mime_type"] == "application/octet-stream"
        video_file = client.get(
            f"/api/assets/{video_asset['id']}/versions/1/file"
        )
        video_file.raise_for_status()
        assert video_file.content == video_content
        assert video_file.headers["content-disposition"].startswith("attachment")
        assert video_file.headers["x-content-type-options"] == "nosniff"
        checks.append("video_attachment_content_and_digest")

    return {
        "run_id": identity["run_id"],
        "entrypoint": "http://127.0.0.1:8000",
        "text_source_id": identity["text_source_id"],
        "text_revision": identity["text_revision"],
        "video_source_id": identity["video_source_id"],
        "video_revision": identity["video_revision"],
        "image_source_id": identity["image_source_id"],
        "image_revision": identity["image_revision"],
        "image_source_license": identity["image_origin"]["license"],
        "image_source_license_note": identity["image_origin"]["license_note"],
        "image_origin_source_id": identity["image_origin"]["source_id"],
        "image_origin_revision": identity["image_origin"]["revision"],
        "image_origin_asset_id": original_image_detail["id"],
        "image_origin_file_sha256": original_image_sha256,
        "image_asset_id": image_asset["id"],
        "video_sample": {
            "source_url": OPENCV_VIDEO_PAGE,
            "size": len(video_content),
            "sha256": sha256(video_content).hexdigest(),
            "git_blob_sha1": OPENCV_VIDEO_BLOB_SHA1,
            "repository_license": "Apache License 2.0",
            "sample_specific_license_notice": None,
        },
        "text_asset_id": text_asset["id"],
        "video_asset_id": video_asset["id"],
        "text_version_sha256": [item["file_sha256"] for item in text_versions],
        "checks": checks,
        "status": "通过",
    }


def cleanup_audit_rows(engine, identity: dict):
    settings = Settings()
    source_ids = [
        identity["text_source_id"],
        identity["video_source_id"],
        identity["image_source_id"],
    ]
    with Session(engine) as session, session.begin():
        assets = session.scalars(
            select(Asset).where(Asset.source_id.in_(source_ids))
        ).all()
        asset_ids = [asset.id for asset in assets]
        file_paths = [
            version.file_path
            for asset in assets
            for version in asset.versions
        ]
        if asset_ids:
            session.execute(
                delete(AssetVersion).where(AssetVersion.asset_id.in_(asset_ids))
            )
            session.execute(delete(Asset).where(Asset.id.in_(asset_ids)))
        session.execute(delete(Source).where(Source.source_id.in_(source_ids)))
        session.execute(delete(User).where(User.id == identity["reader_id"]))

    data_dir = settings.data_dir.resolve()
    for relative_path in set(file_paths):
        stored_path = (data_dir / relative_path).resolve()
        if not stored_path.is_relative_to(data_dir):
            raise ValueError("上传检查文件路径超出项目数据目录")
        with Session(engine) as session:
            referenced = session.scalar(
                select(AssetVersion.id)
                .where(AssetVersion.file_path == relative_path)
                .limit(1)
            )
        if referenced is None:
            stored_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="检查素材附件上传与文件版本。")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--prepare-video",
        action="store_true",
        help="获取或核实固定提交中的 OpenCV 视频样例，并保存来源记录。",
    )
    mode.add_argument(
        "--http",
        action="store_true",
        help="通过运行中的 HTTP 服务检查上传、版本、文件与权限。",
    )
    arguments = parser.parse_args()

    if arguments.prepare_video:
        print(json.dumps(prepare_video_sample(), ensure_ascii=False))
        return

    video_content = check_video_sample()
    settings = Settings()
    run_id = f"matupload-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_dir = Path(".scratch/evidence/mat-luna-audit") / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    identity = source_identity(run_id)
    report = None
    try:
        create_audit_rows(engine, identity)
        report = verify_http(settings, identity, video_content)
    finally:
        if "admin_id" in identity:
            cleanup_audit_rows(engine, identity)
        engine.dispose()

    report["cleanup"] = "已删除本次运行创建的 Source、User、Asset 与 AssetVersion 数据。"
    report_path = evidence_dir / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": report["status"],
                "run_id": report["run_id"],
                "report": str(report_path),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
