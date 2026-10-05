import argparse
import os
import secrets
from io import BytesIO
from pathlib import Path

from PIL import Image
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from shop.auth import hash_password
from shop.config import Settings
from shop.fashion import EXPECTED_COUNT, REVISION, SOURCE_ID
from shop.materials import MaterialRepository
from shop.models import Asset, AssetVersion, ImportBatch, Source, User


def write_secret(path: Path, value: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(value)


def init_admin(username: str, settings: Settings | None = None):
    settings = settings or Settings()
    if settings.admin_password_file.exists():
        raise FileExistsError("管理员密码文件已存在")
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with Session(engine) as session:
        existing = session.scalar(select(User).where(User.username == username))
        if existing is not None:
            raise ValueError("管理员账号已存在")
        password = secrets.token_urlsafe(32)
        user = User(username=username, password_hash=hash_password(password), role="admin")
        session.add(user)
        session.flush()
        created_secret = False
        created_password = False
        try:
            if not settings.session_secret_file.exists():
                write_secret(settings.session_secret_file, secrets.token_urlsafe(48))
                created_secret = True
            write_secret(settings.admin_password_file, password)
            created_password = True
            session.commit()
        except Exception:
            session.rollback()
            if created_password:
                settings.admin_password_file.unlink()
            if created_secret:
                settings.session_secret_file.unlink()
            raise
    print(f"管理员账号已创建：{username}；密码保存于 {settings.admin_password_file}")


def verify_materials():
    settings = Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with Session(engine) as session:
        source = session.get(Source, (SOURCE_ID, REVISION))
        if source is None:
            raise ValueError("来源版本不存在")
        assets = session.scalars(
            select(Asset).where(Asset.source_id == SOURCE_ID, Asset.source_revision == REVISION)
            .order_by(Asset.source_order)
        ).all()
        if len(assets) != EXPECTED_COUNT:
            raise ValueError(f"素材数量异常：{len(assets)}")
        repository = MaterialRepository(session, settings.data_dir)
        for order, asset in enumerate(assets):
            if asset.source_order != order or asset.record_id != str(asset.versions[0].original_fields["id"]):
                raise ValueError(f"来源顺序或记录 ID 异常：{order}")
            if session.get(User, asset.owner_id) is None:
                raise ValueError(f"负责人不存在：{asset.id}")
            content = repository.read_file(asset.versions[-1])
            with Image.open(BytesIO(content)) as image:
                image.verify()
        batches = session.scalars(
            select(ImportBatch).where(
                ImportBatch.source_id == SOURCE_ID, ImportBatch.source_revision == REVISION
            ).order_by(ImportBatch.started_at)
        ).all()
        for batch in batches:
            if batch.status != "completed" or batch.imported_count != EXPECTED_COUNT or len(batch.manifest) != EXPECTED_COUNT:
                raise ValueError(f"导入批次未完成：{batch.id}")
            if [row["order"] for row in batch.manifest] != list(range(EXPECTED_COUNT)):
                raise ValueError(f"导入清单顺序异常：{batch.id}")
            for row in batch.manifest:
                asset = assets[row["order"]]
                if (
                    row["source_id"] != SOURCE_ID or row["revision"] != REVISION
                    or row["record_id"] != asset.record_id or row["asset_id"] != asset.id
                    or row["sha256"] not in {version.file_sha256 for version in asset.versions}
                ):
                    raise ValueError(f"导入清单与素材不一致：{batch.id} 第 {row['order']} 条")
            print(f"批次={batch.id} 状态={batch.status} 记录={batch.imported_count}")
        version_count = session.scalar(
            select(func.count(AssetVersion.id)).join(Asset).where(
                Asset.source_id == SOURCE_ID, Asset.source_revision == REVISION
            )
        )
        print(
            f"来源={SOURCE_ID}@{REVISION} 素材={len(assets)} "
            f"版本={version_count} 图片校验={len(assets)} 批次={len(batches)}"
        )


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    init_parser = commands.add_parser("init-admin")
    init_parser.add_argument("--username", default="admin")
    commands.add_parser("verify-materials")
    arguments = parser.parse_args()
    if arguments.command == "init-admin":
        init_admin(arguments.username)
    elif arguments.command == "verify-materials":
        verify_materials()


if __name__ == "__main__":
    main()
