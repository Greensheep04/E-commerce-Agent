import json
import os
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.materials import MaterialRepository
from shop.models import Asset, AssetVersion, Source, User

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EVALUATION_ROOT = PROJECT_ROOT / ".local" / "harness-evaluation"
FIXTURE_ROOT = PROJECT_ROOT / "tests" / "fixtures"
MAIN_SOURCE_ID = "ashraq/fashion-product-images-small"
MAIN_SOURCE_REVISION = "3859c76db2f6f3d3b9a3863345e3ccdbff75879d"
CASE_COUNT = 20
QUERY_COUNT = 30
MAIN_FILTER_FIELDS = ("gender", "articleType", "baseColour")
MISSING_FACT_FIELDS = ("price", "stock", "material", "waterproof")
SOURCE_LICENSE = "Non-commercial research; redistribution prohibited"
SOURCE_LICENSE_NOTE = (
    "仅用于非商业研究评价，禁止再发布或二次分发；辅助素材不得进入宣传导出。"
    "HF 数据卡未提供独立 license 字段。"
)


def _dataset_helpers():
    hf_cache_dir = PROJECT_ROOT / ".scratch" / "hf"
    os.environ["HF_HOME"] = str(hf_cache_dir)
    os.environ["HF_DATASETS_CACHE"] = str(hf_cache_dir / "datasets")
    os.environ["HF_HUB_CACHE"] = str(hf_cache_dir / "hub")
    token_path = Path.home() / ".cache" / "huggingface" / "token"
    if "HF_TOKEN_PATH" not in os.environ and token_path.is_file():
        os.environ["HF_TOKEN_PATH"] = str(token_path)
    from shop.harness_dataset import (
        DATASET_FIELDS,
        EXPECTED_COUNT,
        EXPECTED_SOURCE_RECORDS,
        SOURCE_ID,
        SOURCE_REVISION,
        SOURCE_SPLIT,
        SOURCE_URL,
        load_source_rows,
        row_to_material,
        verify_source_metadata,
    )

    return {
        "dataset_fields": DATASET_FIELDS,
        "expected_count": EXPECTED_COUNT,
        "expected_source_records": EXPECTED_SOURCE_RECORDS,
        "source_id": SOURCE_ID,
        "source_revision": SOURCE_REVISION,
        "source_split": SOURCE_SPLIT,
        "source_url": SOURCE_URL,
        "load_source_rows": load_source_rows,
        "row_to_material": row_to_material,
        "verify_source_metadata": verify_source_metadata,
    }


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.part")
    temporary_path.write_text(
        json.dumps(content, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(path)


def validate_queries(queries_fixture: dict, rows: list[dict], source: dict) -> dict:
    if queries_fixture["source"] != {
        "source_id": source["source_id"],
        "revision": source["revision"],
        "split": source["split"],
        "selection": "该 revision 唯一 split 的 source_order 0 至 49",
    }:
        raise ValueError("固定查询文件的来源标识与下载来源不符")
    queries = queries_fixture["queries"]
    if queries_fixture["record_count"] != QUERY_COUNT or len(queries) != QUERY_COUNT:
        raise ValueError(f"固定查询数量不符：{len(queries)}")
    if len({query["id"] for query in queries}) != QUERY_COUNT:
        raise ValueError("固定查询 id 重复")
    by_id = {row["item_ID"]: (order, row) for order, row in enumerate(rows)}
    for query in queries:
        expected_ids = query["expected_record_ids"]
        if not expected_ids or len(set(expected_ids)) != len(expected_ids):
            raise ValueError(f"{query['id']} 的预期记录编号为空或重复")
        evidence = query["evidence"]
        evidence_ids = [record["item_ID"] for record in evidence]
        if set(evidence_ids) != set(expected_ids) or len(evidence_ids) != len(expected_ids):
            raise ValueError(f"{query['id']} 的 evidence 未覆盖且仅覆盖预期记录")
        if expected_ids != sorted(expected_ids, key=lambda item_id: by_id[item_id][0]):
            raise ValueError(f"{query['id']} 的预期记录没有按来源顺序排列")
        if any(term in query["query"].lower() for term in ("cotton", "fabric", "material")):
            raise ValueError(f"{query['id']} 将不可见材质写入查询")
        for record in evidence:
            source_order, row = by_id[record["item_ID"]]
            if source_order != record["source_order"]:
                raise ValueError(f"{query['id']} 的 source_order 不符")
            if record["category1"] != row["category1"]:
                raise ValueError(f"{query['id']} 的 category1 证据不符")
            if any(
                sentence not in row["text"]
                for sentence in record["reference_sentences"]
            ):
                raise ValueError(f"{query['id']} 的参考原句无法在来源 text 中找到")
    return {
        "count": len(queries),
        "unique_ids": len({query["id"] for query in queries}),
        "expected_record_links": sum(
            len(query["expected_record_ids"]) for query in queries
        ),
        "evidence_sentences_checked": sum(
            len(record["reference_sentences"])
            for query in queries
            for record in query["evidence"]
        ),
    }


def query_main_source_fields(session: Session) -> list[dict]:
    latest = (
        select(
            AssetVersion.asset_id,
            func.max(AssetVersion.version).label("version"),
        )
        .group_by(AssetVersion.asset_id)
        .subquery()
    )
    source_fields = AssetVersion.original_fields
    selected = [
        source_fields[field].astext.label(field)
        for field in MAIN_FILTER_FIELDS
    ]
    selected.extend(
        source_fields.has_key(field).label(f"has_{field}")
        for field in MISSING_FACT_FIELDS
    )
    statement = (
        select(*selected)
        .join(
            latest,
            (latest.c.asset_id == AssetVersion.asset_id)
            & (latest.c.version == AssetVersion.version),
        )
        .join(Asset, Asset.id == AssetVersion.asset_id)
        .where(
            Asset.source_id == MAIN_SOURCE_ID,
            Asset.source_revision == MAIN_SOURCE_REVISION,
        )
        .order_by(Asset.source_order, Asset.id)
    )
    return [dict(row._mapping) for row in session.execute(statement).all()]


def validate_cases(cases_fixture: dict, source_rows: list[dict]) -> dict:
    if cases_fixture["main_source"] != {
        "source_id": MAIN_SOURCE_ID,
        "revision": MAIN_SOURCE_REVISION,
    }:
        raise ValueError("固定需求文件的主来源版本不符")
    cases = cases_fixture["cases"]
    if cases_fixture["record_count"] != CASE_COUNT or len(cases) != CASE_COUNT:
        raise ValueError(f"固定需求数量不符：{len(cases)}")
    if len({case["id"] for case in cases}) != CASE_COUNT:
        raise ValueError("固定需求 id 重复")
    actual_classes = Counter(case["notes"]["scenario"] for case in cases)
    expected_classes = {
        "source_match": 12,
        "source_no_match": 4,
        "required_source_fact_missing": 4,
    }
    if dict(actual_classes) != expected_classes:
        raise ValueError(f"固定需求类别数量不符：{dict(actual_classes)}")
    if cases_fixture["class_counts"] != {
        "materials_found": 12,
        "no_match": 4,
        "required_source_fact_missing": 4,
    }:
        raise ValueError("固定需求文件的类别计数不符")
    for case in cases:
        attributes = case["attributes"]
        if not attributes or not set(attributes).issubset(
            {"gender", "masterCategory", "subCategory", "articleType", "baseColour", "season", "usage"}
        ):
            raise ValueError(f"{case['id']} 使用了不支持的数据库属性")
        matching_count = sum(
            all(row.get(key) == value for key, value in attributes.items())
            for row in source_rows
        )
        recorded_count = case["notes"]["source_match_count"]
        if matching_count != recorded_count:
            raise ValueError(
                f"{case['id']} 的 source_fields 实际匹配数为 {matching_count}，"
                f"记录值为 {recorded_count}"
            )
        scenario = case["notes"]["scenario"]
        if scenario == "source_match":
            if case["expected_status"] != "materials_found" or matching_count < 1:
                raise ValueError(f"{case['id']} 的匹配预期与真实 source_fields 不符")
            if case["notes"]["requested_max_items"] not in (1, 2):
                raise ValueError(f"{case['id']} 请求数量超出上限")
        elif scenario == "source_no_match":
            if case["expected_status"] != "insufficient" or matching_count != 0:
                raise ValueError(f"{case['id']} 的无匹配预期与真实 source_fields 不符")
        elif scenario == "required_source_fact_missing":
            missing_fields = case["notes"]["missing_source_fields"]
            required_fields = {fact["field"] for fact in case["required_facts"]}
            if case["expected_status"] != "insufficient" or matching_count < 1:
                raise ValueError(f"{case['id']} 的资料缺失预期与真实 source_fields 不符")
            if set(missing_fields) != required_fields:
                raise ValueError(f"{case['id']} 的缺失字段与 required_facts 不符")
        else:
            raise ValueError(f"{case['id']} 的场景类型无效：{scenario}")
    return {
        "count": len(cases),
        "class_counts": {
            "materials_found": 12,
            "no_match": 4,
            "required_source_fact_missing": 4,
        },
        "source_match_counts": {
            case["id"]: case["notes"]["source_match_count"] for case in cases
        },
    }


def validate_main_fields(cases_fixture: dict, source_rows: list[dict]) -> dict:
    if len(source_rows) != 300:
        raise ValueError(f"主图库来源记录数不符：收到 {len(source_rows)}，预期 300")
    if any(
        field not in row
        for row in source_rows
        for field in MAIN_FILTER_FIELDS
    ):
        raise ValueError("主图库 source_fields 未返回固定筛选字段")
    for field in MISSING_FACT_FIELDS:
        if not all(f"has_{field}" in row for row in source_rows):
            raise ValueError(f"主图库 source_fields 未返回字段存在性：{field}")
    absent_fields = [
        field
        for field in MISSING_FACT_FIELDS
        if any(row[f"has_{field}"] for row in source_rows)
    ]
    if absent_fields:
        raise ValueError(f"固定缺失字段在主图库 source_fields 中存在：{absent_fields}")
    return validate_cases(cases_fixture, source_rows)


def ensure_source(session: Session, source_id: str, revision: str, url: str) -> None:
    source = session.get(Source, (source_id, revision))
    if source is None:
        session.add(
            Source(
                source_id=source_id,
                revision=revision,
                license=SOURCE_LICENSE,
                license_note=SOURCE_LICENSE_NOTE,
                url=url,
            )
        )
        session.commit()
        return
    if (
        source.license != SOURCE_LICENSE
        or source.license_note != SOURCE_LICENSE_NOTE
        or source.url != url
    ):
        raise ValueError("已有辅助来源记录与固定来源标注不符")


def import_auxiliary_rows(
    session: Session,
    settings: Settings,
    source: dict,
    rows: list[dict],
    owner_id: str,
    dataset_fields: tuple[str, ...],
    row_to_material,
) -> tuple[list[dict], list[dict], dict]:
    repository = MaterialRepository(session, settings.data_dir)
    manifest_records = []
    reference_records = []
    previous_assets = session.scalars(
        select(Asset).where(
            Asset.source_id == source["source_id"],
            Asset.source_revision == source["revision"],
        )
    ).all()
    before_count = len(previous_assets)
    if before_count not in (0, len(rows)):
        raise ValueError(f"辅助来源已有 {before_count} 条记录，预期为 0 或 {len(rows)}")
    seen_ids = set()
    for source_order, row in enumerate(rows):
        item, expected_sha256, expected_size = row_to_material(row, source_order, owner_id)
        if item.record_id in seen_ids:
            raise ValueError(f"来源记录编号重复：{item.record_id}")
        seen_ids.add(item.record_id)
        if set(item.original_fields) != set(dataset_fields):
            raise ValueError(f"{item.record_id} 的入库字段超出白名单")
        if "text" in item.original_fields:
            raise ValueError(f"{item.record_id} 的原始 text 进入了素材字段")
        existing = session.scalar(
            select(Asset).where(
                Asset.source_id == source["source_id"],
                Asset.source_revision == source["revision"],
                Asset.record_id == item.record_id,
            )
        )
        if existing is not None and existing.versions:
            version = existing.versions[-1]
            if len(existing.versions) != 1:
                raise ValueError(f"{item.record_id} 已有多条版本记录，停止覆盖")
            matches_existing = all(
                (
                    existing.source_order == source_order,
                    existing.kind == item.kind,
                    existing.title == item.title,
                    existing.tags == item.tags,
                    existing.owner_id == owner_id,
                    version.file_sha256 == expected_sha256,
                    version.file_size == expected_size,
                    version.original_fields == item.original_fields,
                    version.filename == item.filename,
                    version.mime_type == item.mime_type,
                )
            )
            if not matches_existing:
                raise ValueError(f"{item.record_id} 已有素材内容与固定来源不符")
        asset = repository.save(item)
        if len(asset.versions) != 1:
            raise ValueError(f"{item.record_id} 重复处理后产生了多余版本")
        version = asset.versions[-1]
        content = repository.read_file(version)
        if sha256(content).hexdigest() != expected_sha256 or len(content) != expected_size:
            raise ValueError(f"{item.record_id} 的存储图片与来源字节不符")
        manifest_records.append(
            {
                "source_order": source_order,
                "item_ID": item.record_id,
                "asset_id": asset.id,
                "version": version.version,
                "original_fields": version.original_fields,
                "image": {
                    "sha256": version.file_sha256,
                    "size": version.file_size,
                    "filename": version.filename,
                    "mime_type": version.mime_type,
                    "file_path": version.file_path,
                },
            }
        )
        reference_records.append(
            {
                "source_order": source_order,
                "item_ID": item.record_id,
                "text": row["text"],
            }
        )
    if len(manifest_records) != len(rows):
        raise ValueError(f"辅助来源清单数量不符：{len(manifest_records)}")
    if [record["source_order"] for record in manifest_records] != list(range(len(rows))):
        raise ValueError("辅助来源顺序未保持为 0 至 49")
    if len({record["item_ID"] for record in manifest_records}) != len(rows):
        raise ValueError("辅助来源清单 item_ID 重复")
    if any(record["version"] != 1 for record in manifest_records):
        raise ValueError("辅助素材版本号不是 1")
    if any("text" in record["original_fields"] for record in manifest_records):
        raise ValueError("辅助素材 original_fields 中包含原始 text")
    final_assets = session.scalars(
        select(Asset)
        .where(
            Asset.source_id == source["source_id"],
            Asset.source_revision == source["revision"],
        )
        .order_by(Asset.source_order, Asset.id)
        .execution_options(populate_existing=True)
    ).all()
    expected_asset_order = [
        (source_order, row["item_ID"])
        for source_order, row in enumerate(rows)
    ]
    actual_asset_order = [
        (asset.source_order, asset.record_id)
        for asset in final_assets
    ]
    if actual_asset_order != expected_asset_order:
        raise ValueError("辅助来源数据库记录与固定来源顺序或编号不符")
    if any(len(asset.versions) != 1 for asset in final_assets):
        raise ValueError("辅助来源存在重复素材版本")
    if any(asset.owner_id != owner_id for asset in final_assets):
        raise ValueError("辅助素材负责人不是既有管理员")
    for record, row in zip(manifest_records, rows, strict=True):
        expected_fields = {
            field: row[field]
            for field in dataset_fields
        }
        if record["original_fields"] != expected_fields:
            raise ValueError(f"{record['item_ID']} 的原字段与来源记录不符")
    return manifest_records, reference_records, {
        "count_before": before_count,
        "count_after": len(manifest_records),
        "version_counts": dict(Counter(str(record["version"]) for record in manifest_records)),
        "all_images_sha256_verified": True,
        "all_image_bytes_read_back": True,
    }


def code_hashes() -> dict:
    paths = (
        PROJECT_ROOT / "src" / "shop" / "harness_dataset.py",
        PROJECT_ROOT / "scripts" / "prepare_harness_evaluation.py",
        FIXTURE_ROOT / "harness_queries.json",
        FIXTURE_ROOT / "harness_cases.json",
    )
    return {
        str(path.relative_to(PROJECT_ROOT)): sha256(path.read_bytes()).hexdigest()
        for path in paths
    }


def main() -> None:
    helpers = _dataset_helpers()
    source = helpers["verify_source_metadata"]()
    source_rows = helpers["load_source_rows"]()
    if len(source_rows) != helpers["expected_count"]:
        raise ValueError("辅助来源行数与固定样本数不符")
    item_ids = [row["item_ID"] for row in source_rows]
    if len(set(item_ids)) != len(item_ids):
        raise ValueError("固定前50条存在重复 item_ID")

    queries_fixture = read_json(FIXTURE_ROOT / "harness_queries.json")
    cases_fixture = read_json(FIXTURE_ROOT / "harness_cases.json")
    query_report = validate_queries(queries_fixture, source_rows, source)

    settings = Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    main_query_report = {}
    with Session(engine, expire_on_commit=False) as session:
        main_source_rows = query_main_source_fields(session)
        main_query_report = validate_main_fields(cases_fixture, main_source_rows)
        main_owner = session.scalar(
            select(User).where(User.username == "admin", User.role == "admin")
        )
        if main_owner is None:
            raise ValueError("项目数据库中没有可用的管理员账户")
        ensure_source(
            session,
            source["source_id"],
            source["revision"],
            helpers["source_url"],
        )
        manifest_records, reference_records, import_report = import_auxiliary_rows(
            session,
            settings,
            source,
            source_rows,
            main_owner.id,
            helpers["dataset_fields"],
            helpers["row_to_material"],
        )
        import_report["owner_username"] = "admin"

    manifest_path = EVALUATION_ROOT / "manifest.json"
    reference_path = EVALUATION_ROOT / "reference_text.json"
    manifest = {
        "source_id": source["source_id"],
        "revision": source["revision"],
        "config": source["config"],
        "split": source["split"],
        "selection": "source_order 0 至 49",
        "metadata_record_count": helpers["expected_source_records"],
        "record_count": len(manifest_records),
        "source_url": helpers["source_url"],
        "license": SOURCE_LICENSE,
        "license_note": SOURCE_LICENSE_NOTE,
        "material_data_dir": str(settings.data_dir),
        "original_fields": list(helpers["dataset_fields"]),
        "records": manifest_records,
    }
    reference_text = {
        "source_id": source["source_id"],
        "revision": source["revision"],
        "split": source["split"],
        "record_count": len(reference_records),
        "records": reference_records,
    }
    prior_manifest = read_json(manifest_path) if manifest_path.is_file() else None
    prior_reference = read_json(reference_path) if reference_path.is_file() else None
    if prior_manifest is not None and prior_manifest != manifest:
        raise ValueError("既有辅助素材清单与固定来源内容不符")
    if prior_reference is not None and prior_reference != reference_text:
        raise ValueError("既有评价参考 text 与固定来源内容不符")
    write_json(manifest_path, manifest)
    write_json(reference_path, reference_text)

    run_id = f"har-data-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_dir = PROJECT_ROOT / ".scratch" / "evidence" / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    database_url = make_url(settings.database_url)
    report = {
        "run_id": run_id,
        "status": "verified",
        "source": {
            **source,
            "url": helpers["source_url"],
            "license": SOURCE_LICENSE,
            "license_note": SOURCE_LICENSE_NOTE,
            "metadata_config_count": 1,
            "metadata_split_count": 1,
            "selected_record_count": len(manifest_records),
            "source_orders": [record["source_order"] for record in manifest_records],
            "item_ids_unique": len({record["item_ID"] for record in manifest_records}) == len(manifest_records),
            "image_sha256": {
                record["item_ID"]: record["image"]["sha256"]
                for record in manifest_records
            },
        },
        "material_import": import_report,
        "fixtures": {
            "queries": query_report,
            "cases": main_query_report,
        },
        "main_source": {
            "source_id": MAIN_SOURCE_ID,
            "revision": MAIN_SOURCE_REVISION,
            "record_count": len(main_source_rows),
            "queried_source_fields": [
                *MAIN_FILTER_FIELDS,
                *MISSING_FACT_FIELDS,
            ],
            "missing_required_fields": list(MISSING_FACT_FIELDS),
        },
        "outputs": {
            "manifest": str(manifest_path.relative_to(PROJECT_ROOT)),
            "manifest_sha256": sha256(manifest_path.read_bytes()).hexdigest(),
            "reference_text": str(reference_path.relative_to(PROJECT_ROOT)),
            "reference_text_sha256": sha256(reference_path.read_bytes()).hexdigest(),
            "hf_cache": str((PROJECT_ROOT / ".scratch" / "hf").relative_to(PROJECT_ROOT)),
        },
        "execution": {
            "database_driver": database_url.drivername,
            "database_host": database_url.host,
            "database_port": database_url.port,
            "database_name": database_url.database,
            "material_data_dir": str(settings.data_dir),
            "model_requests": 0,
            "code_sha256": code_hashes(),
            "prior_manifest_verified": prior_manifest is not None,
            "prior_reference_text_verified": prior_reference is not None,
        },
    }
    report_path = evidence_dir / "report.json"
    write_json(report_path, report)
    print(
        json.dumps(
            {
                "run_id": run_id,
                "report": str(report_path.relative_to(PROJECT_ROOT)),
                "manifest": str(manifest_path.relative_to(PROJECT_ROOT)),
                "reference_text": str(reference_path.relative_to(PROJECT_ROOT)),
                "auxiliary_records": len(manifest_records),
                "query_count": query_report["count"],
                "case_count": main_query_report["count"],
                "model_requests": 0,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
