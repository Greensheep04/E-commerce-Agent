from hashlib import sha256
from io import BytesIO
from pathlib import Path

from datasets import Image as DatasetImage
from datasets import get_dataset_config_names, get_dataset_split_names, load_dataset
from PIL import Image as PillowImage

from shop.materials import MaterialInput

SOURCE_ID = "Marqo/deepfashion-multimodal"
SOURCE_REVISION = "21601c432a281c8a7deaa2afdda952b4b9ef3fd7"
SOURCE_CONFIG = "default"
SOURCE_SPLIT = "data"
SOURCE_URL = f"https://huggingface.co/datasets/{SOURCE_ID}/tree/{SOURCE_REVISION}"
EXPECTED_COUNT = 50
EXPECTED_SOURCE_RECORDS = 42537
DATASET_FIELDS = ("category1", "category2", "category3", "item_ID")
EXPECTED_ROW_FIELDS = {"image", "category1", "category2", "category3", "text", "item_ID"}


def verify_source_metadata() -> dict:
    configs = get_dataset_config_names(SOURCE_ID, revision=SOURCE_REVISION)
    if configs != [SOURCE_CONFIG]:
        raise ValueError(f"来源 config 与固定元数据不符：{configs}")
    splits = get_dataset_split_names(
        SOURCE_ID,
        config_name=SOURCE_CONFIG,
        revision=SOURCE_REVISION,
    )
    if splits != [SOURCE_SPLIT]:
        raise ValueError(f"来源 split 与固定元数据不符：{splits}")
    return {
        "source_id": SOURCE_ID,
        "revision": SOURCE_REVISION,
        "config": SOURCE_CONFIG,
        "split": SOURCE_SPLIT,
        "metadata_library": "datasets",
    }


def load_source_rows() -> list[dict]:
    dataset = load_dataset(
        SOURCE_ID,
        revision=SOURCE_REVISION,
        split=SOURCE_SPLIT,
    )
    dataset = dataset.cast_column("image", DatasetImage(decode=False))
    if len(dataset) != EXPECTED_SOURCE_RECORDS:
        raise ValueError(
            f"来源 split 记录数与固定元数据不符：{len(dataset)}"
        )
    rows = list(dataset.select(range(EXPECTED_COUNT)))
    if len(rows) != EXPECTED_COUNT:
        raise ValueError(f"来源记录数不足：收到 {len(rows)}，预期 {EXPECTED_COUNT}")
    return rows


def _json_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "item"):
        return _json_value(value.item())
    raise TypeError(f"来源字段值无法写入 JSON：{type(value).__name__}")


def row_to_material(row: dict, source_order: int, owner_id: str) -> tuple[MaterialInput, str, int]:
    if set(row) != EXPECTED_ROW_FIELDS:
        raise ValueError(f"来源字段与固定结构不符：{sorted(row)}")
    record_id = row["item_ID"]
    if not isinstance(record_id, str) or not record_id:
        raise ValueError(f"source_order={source_order} 缺少有效 item_ID")
    text = row["text"]
    if not isinstance(text, str) or not text.strip():
        raise ValueError(f"item_ID={record_id} 缺少参考 text")
    image_field = row["image"]
    if not isinstance(image_field, dict):
        raise TypeError(f"item_ID={record_id} 的 image 字段结构无效")
    image_bytes = image_field.get("bytes")
    image_path = image_field.get("path")
    if image_bytes is None:
        if not image_path:
            raise ValueError(f"item_ID={record_id} 没有图片字节或缓存路径")
        image_bytes = Path(image_path).read_bytes()
    if not isinstance(image_bytes, bytes):
        image_bytes = bytes(image_bytes)
    if not image_bytes:
        raise ValueError(f"item_ID={record_id} 图片字节为空")
    with PillowImage.open(BytesIO(image_bytes)) as image:
        image.verify()
        image_format = image.format
    if image_format is None:
        raise ValueError(f"item_ID={record_id} 图片格式无法识别")
    suffix = "jpg" if image_format == "JPEG" else image_format.lower()
    image_sha256 = sha256(image_bytes).hexdigest()
    original_fields = {
        field: _json_value(row[field])
        for field in DATASET_FIELDS
    }
    tags = [
        str(row[field])
        for field in ("category1", "category2", "category3")
        if row[field] is not None and str(row[field])
    ]
    filename = Path(image_path).name if image_path else f"{record_id}.{suffix}"
    material = MaterialInput(
        source_id=SOURCE_ID,
        source_revision=SOURCE_REVISION,
        record_id=record_id,
        title=record_id,
        kind="image",
        tags=tags,
        owner_id=owner_id,
        source_order=source_order,
        original_fields=original_fields,
        filename=filename,
        mime_type=PillowImage.MIME[image_format],
        file_bytes=image_bytes,
    )
    return material, image_sha256, len(image_bytes)
