from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path

from datasets import Image, load_dataset
from PIL import Image as PillowImage
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.materials import MaterialInput, MaterialRepository
from shop.models import ImportBatch, Source

SOURCE_ID = "ashraq/fashion-product-images-small"
REVISION = "3859c76db2f6f3d3b9a3863345e3ccdbff75879d"
SOURCE_URL = f"https://huggingface.co/datasets/{SOURCE_ID}/tree/{REVISION}"
EXPECTED_COUNT = 300


def row_to_material(row: dict, order: int, owner_id: str) -> MaterialInput:
    image_field = row["image"]
    image_content = image_field.get("bytes")
    image_path = image_field.get("path")
    if image_content is None:
        if image_path is None:
            raise ValueError("来源图片没有文件内容或路径")
        image_content = Path(image_path).read_bytes()
    with PillowImage.open(BytesIO(image_content)) as image:
        image.verify()
        image_format = image.format
    if image_format is None:
        raise ValueError("来源图片格式无法识别")
    mime_type = PillowImage.MIME[image_format]
    image_digest = sha256(image_content).hexdigest()
    original_fields = dict(row)
    original_fields["image"] = {
        "path": image_path,
        "sha256": image_digest,
        "size": len(image_content),
    }
    record_id = str(row["id"])
    title = row["productDisplayName"]
    tags = [
        str(row[key]) for key in ("gender", "masterCategory", "subCategory", "articleType")
        if row.get(key)
    ]
    suffix = image_format.lower() if image_format.lower() != "jpeg" else "jpg"
    return MaterialInput(
        source_id=SOURCE_ID, source_revision=REVISION, record_id=record_id,
        title=title, kind="image", tags=tags, owner_id=owner_id, source_order=order,
        original_fields=original_fields, filename=Path(image_path).name if image_path else f"{record_id}.{suffix}",
        mime_type=mime_type, file_bytes=image_content,
    )


def import_rows(settings: Settings, owner_id: str, rows) -> ImportBatch:
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with engine.connect() as lock_connection:
        locked = lock_connection.execute(
            text("SELECT pg_try_advisory_lock(hashtextextended(:identity, 0))"),
            {"identity": f"{SOURCE_ID}@{REVISION}"},
        ).scalar_one()
        if not locked:
            raise RuntimeError("相同来源版本的导入正在运行")
        try:
            return _import_rows_locked(engine, settings, owner_id, rows)
        finally:
            lock_connection.execute(
                text("SELECT pg_advisory_unlock(hashtextextended(:identity, 0))"),
                {"identity": f"{SOURCE_ID}@{REVISION}"},
            )


def _import_rows_locked(engine, settings: Settings, owner_id: str, rows) -> ImportBatch:
    with Session(engine, expire_on_commit=False) as session:
        source = session.get(Source, (SOURCE_ID, REVISION))
        if source is None:
            session.add(Source(
                source_id=SOURCE_ID, revision=REVISION, license=None, url=SOURCE_URL,
                license_note=(
                    "HF 数据卡未声明许可；上游 Kaggle 数据页标注 MIT："
                    "https://www.kaggle.com/datasets/paramaggarwal/fashion-product-images-small"
                ),
            ))
            session.commit()
        batch = ImportBatch(
            source_id=SOURCE_ID, source_revision=REVISION, owner_id=owner_id,
            status="running", expected_count=EXPECTED_COUNT, imported_count=0, manifest=[],
        )
        session.add(batch)
        session.commit()
        repository = MaterialRepository(session, settings.data_dir)
        try:
            for order, row in enumerate(rows):
                item = row_to_material(row, order, owner_id)
                asset = repository.save(item)
                batch.manifest = [
                    *batch.manifest,
                    {"order": order, "source_id": SOURCE_ID, "revision": REVISION,
                     "record_id": item.record_id, "asset_id": asset.id,
                     "sha256": asset.versions[-1].file_sha256},
                ]
                batch.imported_count = order + 1
                session.commit()
            if batch.imported_count != EXPECTED_COUNT:
                raise ValueError(f"来源条数不足：收到 {batch.imported_count}，预期 {EXPECTED_COUNT}")
            batch.status = "completed"
            batch.completed_at = datetime.now(UTC)
            session.commit()
            return batch
        except Exception as error:
            session.rollback()
            batch.status = "failed"
            batch.error = f"第 {batch.imported_count + 1} 条：{type(error).__name__}: {error}"
            batch.completed_at = datetime.now(UTC)
            session.commit()
            raise


def run_import(settings: Settings, owner_id: str) -> ImportBatch:
    dataset = load_dataset(SOURCE_ID, revision=REVISION, split="train", streaming=True)
    dataset = dataset.cast_column("image", Image(decode=False))
    return import_rows(settings, owner_id, dataset.take(EXPECTED_COUNT))
