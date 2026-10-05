import argparse
import csv
import hashlib
import json
import shutil
from collections import Counter
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INDUSTRY_ROOT = Path("/Users/greensheep/.codex/worktrees/f4dc/industry-RAG")
FIXTURE_PATH = PROJECT_ROOT / "tests/fixtures/data_center_cases.json"
SOURCE_ROOT = PROJECT_ROOT / ".local/data-center/sources"
SOURCE_REGISTRY_PATH = SOURCE_ROOT / "r2/sources.json"
EVIDENCE_ROOT = PROJECT_ROOT / ".scratch/evidence/dc01-inputs-20261004-r2"
RUN_ID = "dc01-inputs-20261004-r2"
SOURCE_RUN_ID = RUN_ID
ORDER_REVISION = "beaab06ef72831089ca80e5355d65e661fd19b26"
ORDER_SHA256 = "cf637c7d58006ebbc13daf0b8ed6cf7f738165d298ae0b5479a59237429c1882"
ASSESSMENT_REVISION = ORDER_REVISION
ASSESSMENT_SOURCE_ID = "microsoft/adventureworks"
ORDER_SOURCE_ID = "microsoft/adventureworks-oltp"
SCHEMA_SHA256 = "d979f036750f9c7b4fb7be3d58cdbb596a74f864a6325998501b3a96c6ad53f9"
DOCUMENT_TARGETS = {
    "readme-v1": "markitdown/readme-v1.md",
    "readme-v2": "markitdown/readme-v2.md",
    "pdf": "markitdown/invoice.pdf",
    "docx": "markitdown/equations.docx",
    "xlsx": "markitdown/test.xlsx",
    "pptx": "markitdown/test.pptx",
    "image": "tesseract/phototest.tif",
    "scan": "tesseract/phototest-scan-v04.pdf",
}
DOCUMENT_SOURCE_KEYS = {
    "readme-v1": "markitdown:readme-v1",
    "readme-v2": "markitdown:readme-v2",
    "pdf": "markitdown:invoice",
    "docx": "markitdown:equations",
    "xlsx": "markitdown:workbook",
    "pptx": "markitdown:slides",
    "image": "tesseract:phototest-image",
    "scan": "tesseract:phototest-scan",
}
DOCUMENT_RESOURCE_MODULE = "data_center"
DATA_SOURCES = (
    {
        "source_key": "adventureworks-oltp:sales-order-header",
        "source_id": ORDER_SOURCE_ID,
        "record_id": "sales-order-header",
        "resource_id": "SalesOrderHeader.csv",
        "upstream_path": f".local/data/orders/{ORDER_REVISION}/SalesOrderHeader.csv",
        "path": "adventureworks-oltp/SalesOrderHeader.csv",
        "revision": ORDER_REVISION,
        "license": None,
        "license_status": "not_recorded_in_existing_source_manifest",
        "nature": "public_business_sample",
        "sha256": ORDER_SHA256,
        "rows": 31465,
        "columns": 26,
    },
    {
        "source_key": "adventureworks:employee",
        "source_id": ASSESSMENT_SOURCE_ID,
        "record_id": "employee",
        "resource_id": "Employee.csv",
        "upstream_path": ".local/assessment/source/Employee.csv",
        "path": "adventureworks/Employee.csv",
        "revision": ASSESSMENT_REVISION,
        "license": None,
        "license_status": "not_recorded_in_existing_source_manifest",
        "nature": "public_business_sample",
        "sha256": "111b1ba4a51bf982c7f8841cdb14c14374fc4552a22e7ba067ef728e7ae72258",
        "rows": 290,
        "columns": 16,
    },
    {
        "source_key": "adventureworks:salesperson",
        "source_id": ASSESSMENT_SOURCE_ID,
        "record_id": "salesperson",
        "resource_id": "SalesPerson.csv",
        "upstream_path": ".local/assessment/source/SalesPerson.csv",
        "path": "adventureworks/SalesPerson.csv",
        "revision": ASSESSMENT_REVISION,
        "license": None,
        "license_status": "not_recorded_in_existing_source_manifest",
        "nature": "public_business_sample",
        "sha256": "92ed21c0e22bf0060a746d53c1f8f0ed68a1ece4b0d5b9c60dbc233d52499206",
        "rows": 17,
        "columns": 9,
    },
    {
        "source_key": "adventureworks:salesperson-quota-history",
        "source_id": ASSESSMENT_SOURCE_ID,
        "record_id": "salesperson-quota-history",
        "resource_id": "SalesPersonQuotaHistory.csv",
        "upstream_path": ".local/assessment/source/SalesPersonQuotaHistory.csv",
        "path": "adventureworks/SalesPersonQuotaHistory.csv",
        "revision": ASSESSMENT_REVISION,
        "license": None,
        "license_status": "not_recorded_in_existing_source_manifest",
        "nature": "public_business_sample",
        "sha256": "769cfb3d44c1f422dd23eb24bc0d310f82739eb6f49598520a3eb47e68161064",
        "rows": 163,
        "columns": 5,
    },
    {
        "source_key": "adventureworks:schema-reference",
        "source_id": ASSESSMENT_SOURCE_ID,
        "record_id": "adventureworks-oltp-schema",
        "resource_id": "instawdb.sql",
        "upstream_path": ".local/assessment/source/instawdb.sql",
        "path": "adventureworks/schema/instawdb.sql",
        "revision": ASSESSMENT_REVISION,
        "license": None,
        "license_status": "not_recorded_in_existing_source_manifest",
        "nature": "public_business_sample",
        "sha256": SCHEMA_SHA256,
    },
)
EXPECTED_CATEGORY_COUNTS = {
    "document": 10,
    "cross_document": 6,
    "analytics": 8,
    "refusal": 6,
}
EXPECTED_THRESHOLDS = {
    "document": {"attempts": 30, "passed_minimum": 28},
    "cross_document": {"attempts": 18, "passed_minimum": 17},
    "analytics": {"attempts": 24, "passed_minimum": 23},
    "refusal": {"attempts": 18, "passed_minimum": 18},
    "overall": {"attempts": 90, "passed_minimum": 86},
}
PERIOD_START = date(2023, 7, 1)
PERIOD_END = date(2023, 10, 1)
FORMULAS = {
    "sales_amount": "SUM(SubTotal) where Status=5",
    "order_count": "COUNT(DISTINCT SalesOrderID) where Status=5",
    "average_order_value": "sales_amount / order_count",
    "quota_completion": "sales_amount / quota_amount",
}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_tsv(path: Path, expected_rows: int, expected_columns: int, quote_none: bool) -> list[list[str]]:
    options = {"delimiter": "\t", "strict": True}
    if quote_none:
        options["quoting"] = csv.QUOTE_NONE
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.reader(stream, **options))
    require(len(rows) == expected_rows, f"{path.name} 行数不匹配：{len(rows)}")
    for line_number, row in enumerate(rows, start=1):
        require(len(row) == expected_columns,
                f"{path.name} 第 {line_number} 行列数不匹配：{len(row)}")
    return rows


def source_path(source: dict) -> Path:
    return PROJECT_ROOT / source["upstream_path"]


def original_source_path(source: dict) -> Path:
    if source["source_key"].startswith("adventureworks"):
        return source_path(source)
    return Path(source["upstream_root"]) / source["upstream_path"]


def target_path(source: dict) -> Path:
    return SOURCE_ROOT / source["path"]


def build_document_sources(industry_root: Path, reference: dict, manifest: dict) -> list[dict]:
    entries = []
    for document_key, relative_path in DOCUMENT_TARGETS.items():
        document = reference["documents"][document_key]
        manifest_document = manifest["files"].get(document["raw_path"])
        original_path = (manifest_document["path"] if manifest_document is not None
                         else document["raw_path"])
        if manifest_document is None:
            generated = manifest["generated"][document_key]
            require(generated["sha256"] == document["source_sha256"],
                    f"{document_key} 生成原件摘要与固定参考不匹配")
        source_key = DOCUMENT_SOURCE_KEYS[document_key]
        source_revision = document["source_revision"]
        resource_id = f"{source_key}/{source_revision}/{original_path}"
        entries.append({
            "source_key": source_key,
            "document_key": document_key,
            "source_id": f"industry-rag/{source_key}/{document['entity_id']}/{source_revision}",
            "upstream_source_id": document["source_id"],
            "record_id": document["entity_id"],
            "resource_id": resource_id,
            "original_filename": Path(original_path).name,
            "original_path": original_path,
            "title": Path(document["raw_path"]).name,
            "language": manifest["documents"][document_key]["language"],
            "resource": {
                "module_id": DOCUMENT_RESOURCE_MODULE,
                "resource_type": "source",
                "resource_id": resource_id,
                "version": source_revision,
                "sha256": document["source_sha256"],
            },
            "path": relative_path,
            "revision": source_revision,
            "source_revision": source_revision,
            "license": document["license_id"],
            "license_id": document["license_id"],
            "nature": document["source_nature"],
            "source_nature": document["source_nature"],
            "sha256": document["source_sha256"],
            "source_sha256": document["source_sha256"],
            "upstream_path": document["raw_path"],
            "upstream_root": str(industry_root / "artifacts/raw/documents-v04"),
        })
    for document_key, generated in manifest.get("generated", {}).items():
        output = next(item for item in entries if item["document_key"] == document_key)
        inputs = [item for item in entries
                  if item["source_sha256"] == generated["image_sha256"]]
        require(len(inputs) == 1, "生成文档没有唯一的原始输入资源")
        require(output["source_sha256"] == generated["sha256"], "生成文档输出摘要不匹配")
        original = inputs[0]
        output["generation"] = {
            "program": generated["program"], "source_id": original["source_id"],
            "source_record_id": original["record_id"], "source_resource": original["resource"],
            "source_sha256": original["source_sha256"], "output_sha256": output["source_sha256"],
            "manifest_sha256": file_sha256(industry_root / "artifacts/raw/documents-v04/manifest.json"),
        }
    entries.extend(build_supplemental_document_sources(FIXTURE))
    return entries


def build_supplemental_document_sources(fixture: dict) -> list[dict]:
    entries = []
    for item in fixture["supplemental_document_sources"]:
        revision = item["revision"]
        nature = item.get("nature", "technical_sample")
        language = item.get("language", "zho")
        resource_path = item.get("resource_path", item["original_path"])
        resource_id = f"{item['source_key']}/{revision}/{resource_path}"
        entries.append({
            "source_key": item["source_key"],
            "document_key": item["record_id"],
            "source_id": f"{item['upstream_source_id']}/{item['record_id']}/{revision}",
            "upstream_source_id": item["upstream_source_id"],
            "record_id": item["record_id"],
            "resource_id": resource_id,
            "original_filename": item["original_filename"],
            "original_path": item["original_path"],
            "title": item["title"],
            "language": language,
            "resource": {
                "module_id": DOCUMENT_RESOURCE_MODULE,
                "resource_type": "source",
                "resource_id": resource_id,
                "version": revision,
                "sha256": item["sha256"],
            },
            "path": item["path"],
            "revision": revision,
            "source_revision": revision,
            "license": item["license_id"],
            "license_id": item["license_id"],
            "nature": nature,
            "source_nature": nature,
            "sha256": item["sha256"],
            "source_sha256": item["sha256"],
            "upstream_path": item["original_path"],
            "upstream_root": str(PROJECT_ROOT),
        })
    return entries


def build_sources(industry_root: Path, reference: dict, manifest: dict) -> list[dict]:
    documents = build_document_sources(industry_root, reference, manifest)
    data_sources = []
    for item in DATA_SOURCES:
        source = dict(item)
        source["source_revision"] = source["revision"]
        source["license_id"] = source["license"]
        source["source_nature"] = source["nature"]
        source["source_sha256"] = source["sha256"]
        source["upstream_path"] = str(PROJECT_ROOT / source["upstream_path"])
        source["original_path"] = source["upstream_path"]
        source["original_filename"] = Path(source["upstream_path"]).name
        source["upstream_source_id"] = source["source_id"]
        data_sources.append(source)
    return documents + data_sources


def verify_control(path: Path, expected_sha256: str, label: str) -> str:
    require(path.is_file(), f"缺少固定来源：{path}")
    digest = file_sha256(path)
    require(digest == expected_sha256,
            f"{label} SHA-256 不匹配：期望 {expected_sha256}，实际 {digest}")
    return digest


def verify_source_files(sources: list[dict]) -> list[dict]:
    results = []
    for source in sources:
        original = original_source_path(source)
        digest = verify_control(original, source["sha256"], source["source_key"])
        if "rows" in source:
            columns = read_tsv(original, source["rows"], source["columns"],
                               source["source_key"] == "adventureworks-oltp:sales-order-header")
        else:
            columns = None
        target = target_path(source)
        if target.exists():
            require(target.is_file() and file_sha256(target) == digest,
                    f"已存在来源副本内容不符：{target}")
            copy_state = "existing_copy_verified"
        else:
            copy_state = "copy_required"
        results.append({
            "source_key": source["source_key"],
            "source_id": source["source_id"],
            "original_path": source["original_path"],
            "original_filename": source["original_filename"],
            "path": source["path"],
            "revision": source["source_revision"],
            "license_id": source["license_id"],
            "sha256": digest,
            "rows": source.get("rows"),
            "columns": len(columns[0]) if columns else source.get("columns"),
            "copy_state": copy_state,
        })
    return results


def verify_documents(manifest: dict, reference: dict, manifest_sha256: str,
                     reference_sha256: str, source_map: dict[str, dict]) -> dict:
    require(reference.get("validation", {}).get("status") == "PASSED",
            "固定文档参考状态未通过")
    require(reference.get("source_manifest_sha256") == manifest_sha256,
            "固定文档参考绑定的来源清单摘要不匹配")
    require(set(reference["documents"]) == set(DOCUMENT_TARGETS),
            "固定文档数量或身份与登记列表不符")
    require(len(reference["reference_entries"]) == 14,
            "固定文档参考条目数量不为 14")
    manifest_files = manifest["files"]
    require(len(manifest_files) == 10,
            "Industry RAG 原始文档清单文件数与固定版本不符")
    reference_entries = {item["reference_id"]: item for item in reference["reference_entries"]}
    used_reference_ids = set()
    for case in FIXTURE["cases"]:
        if case["category"] != "document":
            continue
        case_reference_ids = set(case["reference_ids"])
        used_reference_ids |= case_reference_ids
        for reference_id in case_reference_ids:
            expected = reference_entries[reference_id]["must_answer"]
            require(expected == [claim for claim in case["expected_claims"]
                                  if claim in expected],
                    f"{case['id']} 的独立预期与固定参考不一致：{reference_id}")
            evidence = [item for item in case["source_evidence"]
                        if item["reference_id"] == reference_id]
            require(evidence, f"{case['id']} 缺少参考条目的来源位置：{reference_id}")
            citations = reference_entries[reference_id]["citations"]
            for locator in evidence:
                valid = any(
                    citation["document_key"] == locator["document_key"]
                    and citation["start_line"] == locator["line_start"]
                    and citation["end_line"] == locator["line_end"]
                    for citation in citations
                )
                require(valid, f"{case['id']} 来源行与固定引用不匹配：{reference_id}")
                source = source_map[locator["source_key"]]
                require(source["sha256"] == reference["documents"][locator["document_key"]]["source_sha256"],
                        f"{case['id']} 来源摘要与固定引用不匹配：{reference_id}")
    require(used_reference_ids == set(reference_entries),
            "30 场景没有完整覆盖全部 14 个固定文档参考条目")
    return {
        "raw_manifest_sha256": manifest_sha256,
        "reference_sha256": reference_sha256,
        "documents": len(reference["documents"]),
        "reference_entries": len(reference_entries),
        "covered_reference_entries": len(used_reference_ids),
        "reference_validation_status": reference["validation"]["status"],
        "upstream_model_calls": reference["validation"].get("model_calls", 0),
    }


def validate_fixture(reference: dict, sources: list[dict]) -> dict:
    counts = Counter(case["category"] for case in FIXTURE["cases"])
    require(len(FIXTURE["cases"]) == 30, "固定场景数量不为 30")
    require(dict(counts) == EXPECTED_CATEGORY_COUNTS,
            f"固定场景类别数量不符：{dict(counts)}")
    require(FIXTURE["acceptance_thresholds"] == EXPECTED_THRESHOLDS,
            "固定验收门槛与 DC-01 里程碑不一致")
    require(FIXTURE["execution_policy"]["attempts_per_scenario"] == 3,
            "每个场景执行次数不为 3")
    require(FIXTURE["execution_policy"]["planned_attempts"] == 90,
            "计划执行次数不为 90")
    require(sum(FIXTURE["acceptance_thresholds"][category]["attempts"]
                for category in EXPECTED_CATEGORY_COUNTS) == 90,
            "分类执行次数总和不为 90")
    case_ids = [case["id"] for case in FIXTURE["cases"]]
    require(len(case_ids) == len(set(case_ids)), "固定场景编号重复")
    source_map = {source["source_key"]: source for source in sources}
    require(len(source_map) == len(sources), "来源 source_key 重复")
    document_sources = [source for source in sources if source["source_key"] in FIXTURE["document_ingest_source_keys"]]
    expected_document_count = len(DOCUMENT_TARGETS) + len(FIXTURE["supplemental_document_sources"])
    require(len(document_sources) == len(FIXTURE["document_ingest_source_keys"]) == expected_document_count,
            "固定文档导入来源数量与来源登记不符")
    document_source_ids = [source["source_id"] for source in document_sources]
    require(len(document_source_ids) == len(set(document_source_ids)), "文档来源 source_id 重复")
    document_resource_ids = []
    for source in document_sources:
        resource = source["resource"]
        require(resource["module_id"] == DOCUMENT_RESOURCE_MODULE
                and resource["resource_type"] == "source"
                and resource["resource_id"] == source["resource_id"]
                and resource["version"] == source["source_revision"]
                and resource["sha256"] == source["source_sha256"],
                f"{source['source_key']} ResourceRef 与原件登记不一致")
        if source["source_key"] in DOCUMENT_SOURCE_KEYS.values():
            require(source["record_id"] == reference["documents"][source["document_key"]]["entity_id"],
                    f"{source['source_key']} record_id 与固定参考不一致")
        else:
            item = next((value for value in FIXTURE["supplemental_document_sources"]
                         if value["source_key"] == source["source_key"]), None)
            require(item is not None and source["record_id"] == item["record_id"]
                    and source["original_path"] == item["original_path"]
                    and source["path"] == item["path"]
                    and source["upstream_source_id"] == item["upstream_source_id"]
                    and source["sha256"] == item["sha256"],
                    f"{source['source_key']} 补充文档来源身份不一致")
        document_resource_ids.append(resource["resource_id"])
    require(len(document_resource_ids) == len(set(document_resource_ids)),
            "固定文档 ResourceRef.resource_id 不唯一")
    require(set(FIXTURE["document_ingest_source_keys"]) == set(source_map)
            & set(FIXTURE["document_ingest_source_keys"]),
            "DocumentIngest 引用了未登记来源")
    for case in FIXTURE["cases"]:
        require(case["attempts"] == 3, f"{case['id']} 执行次数不为 3")
        for evidence in case.get("source_evidence", []):
            if "source_key" in evidence:
                require(evidence["source_key"] in source_map,
                        f"{case['id']} 引用了未登记来源：{evidence['source_key']}")
                if "document_key" in evidence:
                    require(evidence["source_key"] in FIXTURE["document_ingest_source_keys"]
                            and source_map[evidence["source_key"]]["document_key"] == evidence["document_key"],
                            f"{case['id']} 文档证据编号与来源身份不一致")
    reference_ids = {item["reference_id"] for item in reference["reference_entries"]}
    require({reference_id for case in FIXTURE["cases"]
             for reference_id in case.get("reference_ids", [])} == reference_ids,
            "固定文档参考条目覆盖不完整")
    search_case_ids = [item["case_id"] for item in FIXTURE["document_search_cases"]]
    require(len(search_case_ids) == len(set(search_case_ids)), "文档检索输入编号重复")
    require({f"REL-{number:02d}" for number in range(1, 7)}.issubset(search_case_ids),
            "REL-01 至 REL-06 缺少 hybrid/graph 文档检索输入")
    for item in FIXTURE["document_search_cases"]:
        case = next((value for value in FIXTURE["cases"] if value["id"] == item["case_id"]), None)
        require(case is not None and case["category"] in {"document", "cross_document"},
                f"{item['case_id']} 文档检索输入没有对应文档场景")
        expected_source_keys = {entry["source_key"] for entry in case.get("source_evidence", [])
                                if entry.get("source_key") in FIXTURE["document_ingest_source_keys"]}
        require(set(item["source_keys"]) == expected_source_keys,
                f"{item['case_id']} 检索来源与文档来源证据不一致")
        require(item["source_keys"] and all(key in source_map for key in item["source_keys"]),
                f"{item['case_id']} 检索输入包含未登记来源")
        require(item["expected_text"] and all(isinstance(value, str) and value for value in item["expected_text"]),
                f"{item['case_id']} 检索输入缺少固定预期文本")
        require(1 <= item["limit"] <= 100, f"{item['case_id']} 检索条数限制无效")
    return {"scenario_count": len(case_ids), "category_counts": dict(counts),
            "attempts_per_scenario": 3, "planned_attempts": 90,
            "case_ids": case_ids, "document_ingest_sources": len(document_sources),
            "document_search_cases": search_case_ids}


def verify_supplemental_document_references(sources: list[dict]) -> dict:
    source_map = {source["source_key"]: source for source in sources}
    supplemental_keys = {item["source_key"] for item in FIXTURE["supplemental_document_sources"]}
    require(len(supplemental_keys) == len(FIXTURE["supplemental_document_sources"]),
            "补充文档 source_key 重复")
    source_lines = {}
    for item in FIXTURE["supplemental_document_sources"]:
        source = source_map[item["source_key"]]
        original = original_source_path(source)
        require(file_sha256(original) == item["sha256"],
                f"{item['source_key']} 原始来源摘要与固定输入不一致")
        require(Path(item["path"]).suffix == ".md",
                f"{item['source_key']} 文档副本必须使用 .md 扩展名")
        source_lines[item["source_key"]] = original.read_text(encoding="utf-8").splitlines()
    citations = []
    for case in FIXTURE["cases"]:
        for evidence in case.get("source_evidence", []):
            source_key = evidence.get("source_key")
            if source_key not in supplemental_keys or "document_key" not in evidence:
                continue
            lines = source_lines[source_key]
            start = evidence["line_start"]
            end = evidence["line_end"]
            require(1 <= start <= end <= len(lines),
                    f"{case['id']} {source_key} 固定引用行号超出来源范围")
            citations.append({"case_id": case["id"], "source_key": source_key,
                              "line_start": start, "line_end": end})
    relation_case = next(case for case in FIXTURE["cases"] if case["id"] == "REL-06")
    schema_key = "adventureworks:schema-document"
    schema_citations = [item for item in relation_case["source_evidence"]
                        if item.get("source_key") == schema_key]
    schema_excerpt = "\n".join(
        "\n".join(source_lines[schema_key][item["line_start"] - 1:item["line_end"]])
        for item in schema_citations
    )
    for value in (
        "SalesOrderHeader",
        "SalesPersonID",
        "Foreign key to SalesPerson.BusinessEntityID",
        "SalesPerson",
        "Foreign key to Employee.BusinessEntityID",
        "SalesPersonQuotaHistory",
    ):
        require(value in schema_excerpt,
                f"REL-06 AdventureWorks schema 引用没有字段关系依据：{value}")
    search_case = next(item for item in FIXTURE["document_search_cases"]
                       if item["case_id"] == "REL-06")
    relation_document_text = "\n".join(
        "\n".join(source_lines[key]) for key in search_case["source_keys"]
        if key in source_lines
    )
    for expected_text in search_case["expected_text"]:
        require(expected_text in relation_document_text,
                f"REL-06 文档检索预期没有出现在固定来源中：{expected_text}")
    schema_source = source_map[schema_key]
    require(schema_source["original_filename"] == "instawdb.sql"
            and schema_source["revision"] == ORDER_REVISION
            and schema_source["license_id"] == "MIT"
            and schema_source["sha256"] == SCHEMA_SHA256
            and schema_source["path"].endswith("instawdb.sql.md"),
            "AdventureWorks schema 文档来源没有保留原文件身份")
    return {"supplemental_document_count": len(supplemental_keys),
            "citation_count": len(citations),
            "schema_source_key": schema_key,
            "schema_original_filename": schema_source["original_filename"],
            "schema_revision": schema_source["revision"],
            "schema_license_id": schema_source["license_id"],
            "schema_sha256": schema_source["sha256"],
            "schema_field_relation_citations": len(schema_citations)}


def build_document_ingest(fixture: dict, sources: list[dict]) -> dict:
    source_map = {source["source_key"]: source for source in sources}
    documents = []
    for source_key in fixture["document_ingest_source_keys"]:
        source = source_map[source_key]
        documents.append({
            "source_id": source["source_id"],
            "record_id": source["record_id"],
            "source_revision": source["source_revision"],
            "source_nature": source["source_nature"],
            "license_id": source["license_id"],
            "resource": source["resource"],
            "title": source["title"],
            "language": source["language"],
        })
    return {"documents": documents, "graph_enabled": True}


def build_document_queries(fixture: dict, sources: list[dict], retrieval_mode: str) -> list[dict]:
    source_map = {source["source_key"]: source for source in sources}
    case_map = {case["id"]: case for case in fixture["cases"]}
    queries = []
    for item in fixture["document_search_cases"]:
        case = case_map[item["case_id"]]
        queries.append({
            "case_id": item["case_id"],
            "query": {
                "question": case["query"],
                "intent": "documents",
                "source_ids": [source_map[key]["source_id"] for key in item["source_keys"]],
                "retrieval_mode": retrieval_mode,
                "limit": item["limit"],
            },
            "expected_text": item["expected_text"],
        })
    return queries


def build_host_sources(fixture: dict, sources: list[dict]) -> dict:
    source_map = {source["source_key"]: source for source in sources}
    registrations = []
    for source_key in fixture["document_ingest_source_keys"]:
        source = source_map[source_key]
        properties = {
            "record_id": source["record_id"],
            "upstream_source_id": source["upstream_source_id"],
            "source_key": source["source_key"],
            "original_filename": source["original_filename"],
            "original_path": source["original_path"],
        }
        if "generation" in source:
            properties["generation"] = source["generation"]
        registrations.append({
            "source_id": source["source_id"],
            "kind": "document",
            "title": source["title"],
            "revision": source["source_revision"],
            "nature": source["source_nature"],
            "license_id": source["license_id"],
            "sha256": source["source_sha256"],
            "properties": properties,
            "relative_path": source["path"],
            "resource": source["resource"],
        })
    data_source_keys = {item["source_key"] for item in DATA_SOURCES}
    require(len(registrations) == len(fixture["document_ingest_source_keys"]),
            "宿主来源登记数量与文档导入来源不符")
    require(all(item["kind"] == "document" for item in registrations),
            "宿主来源输入包含非文档来源")
    require(not data_source_keys.intersection(fixture["document_ingest_source_keys"]),
            "AdventureWorks 数据来源不得重复进入文档宿主登记")
    require(all({"record_id", "upstream_source_id", "source_key", "original_filename"}
                .issubset(item["properties"]) for item in registrations),
            "宿主来源登记缺少原始来源属性")
    return {"schema_version": 1, "sources": registrations}


def query_comparison_signature(item: dict) -> dict:
    query = dict(item["query"])
    query.pop("retrieval_mode")
    return {"case_id": item["case_id"], "query": query, "expected_text": item["expected_text"]}


def read_analytics_sources(source_map: dict[str, dict]) -> tuple[dict, dict]:
    order_source = source_map["adventureworks-oltp:sales-order-header"]
    quota_source = source_map["adventureworks:salesperson-quota-history"]
    orders = read_tsv(source_path(order_source), order_source["rows"], order_source["columns"], True)
    quotas = read_tsv(source_path(quota_source), quota_source["rows"], quota_source["columns"], False)
    quarter_orders = []
    for line_number, row in enumerate(orders, start=1):
        order_date = datetime.fromisoformat(row[2]).date()
        if PERIOD_START <= order_date < PERIOD_END and row[5] == "5":
            quarter_orders.append((line_number, row))
    quarter_quotas = []
    for line_number, row in enumerate(quotas, start=1):
        quota_date = datetime.fromisoformat(row[1]).date()
        if PERIOD_START <= quota_date < PERIOD_END:
            quarter_quotas.append((line_number, row))
    require(len(quarter_orders) == 1022,
            f"固定期间已发货订单数不符：{len(quarter_orders)}")
    require(len(quarter_quotas) == 14,
            f"固定期间季度目标记录数不符：{len(quarter_quotas)}")
    return {"all": quarter_orders}, {"all": quarter_quotas}


def display_value(value: Decimal, percentage: bool = False) -> str:
    shown = value * 100 if percentage else value
    rendered = str(shown.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    return f"{rendered}%" if percentage else rendered


def calculate_scope(employee_ids: list[int], quarter_orders: list[tuple[int, list[str]]],
                    quarter_quotas: list[tuple[int, list[str]]]) -> dict:
    selected_orders = [(line, row) for line, row in quarter_orders
                       if row[11] and int(row[11]) in employee_ids]
    order_ids = [row[0] for _, row in selected_orders]
    require(len(order_ids) == len(set(order_ids)), "固定订单范围内存在重复 SalesOrderID")
    amount = sum((Decimal(row[19]) for _, row in selected_orders), Decimal(0))
    order_count = len(set(order_ids))
    selected_quotas = [(line, row) for line, row in quarter_quotas
                       if int(row[0]) in employee_ids]
    quota_amounts = [Decimal(row[2]) for _, row in selected_quotas]
    quota_total = sum(quota_amounts, Decimal(0)) if len(selected_quotas) == len(employee_ids) else None
    require(len(selected_quotas) <= len(employee_ids), "同一员工在固定期间存在重复目标记录")
    with localcontext() as context:
        context.prec = 38
        average = amount / Decimal(order_count) if order_count else None
        completion = amount / quota_total if quota_total is not None else None
    result = {
        "sales_amount": str(amount),
        "order_count": order_count,
        "matching_order_rows": len(selected_orders),
        "average_order_value": str(average) if average is not None else None,
        "average_order_value_display": display_value(average) if average is not None else None,
        "quota_amount": str(quota_total) if quota_total is not None else None,
        "quota_completion": str(completion) if completion is not None else None,
        "quota_completion_display": display_value(completion, percentage=True)
        if completion is not None else None,
        "quota_records": [
            {"source_line": line, "employee_id": int(row[0]), "date": row[1], "amount": row[2]}
            for line, row in selected_quotas
        ],
        "order_evidence_first": {
            "line": selected_orders[0][0], "SalesOrderID": selected_orders[0][1][0]
        } if selected_orders else None,
        "order_evidence_last": {
            "line": selected_orders[-1][0], "SalesOrderID": selected_orders[-1][1][0]
        } if selected_orders else None,
    }
    return result


def verify_analytics_cases(quarter_orders: list[tuple[int, list[str]]],
                           quarter_quotas: list[tuple[int, list[str]]]) -> dict:
    computed = {}
    for case in FIXTURE["cases"]:
        if case["category"] != "analytics":
            continue
        expected = case["expected_metrics"]
        if case["id"] == "MET-06":
            employee_results = {
                employee_id: calculate_scope([employee_id], quarter_orders, quarter_quotas)
                for employee_id in case["employee_ids"]
            }
            for metric_name in ("sales_amount", "order_count"):
                expected_rows = expected[metric_name]
                values = {str(employee_id): employee_results[employee_id][metric_name]
                          for employee_id in (275, 277)}
                highest = max((275, 277), key=lambda employee_id:
                              employee_results[employee_id][metric_name])
                require(expected_rows["275"] == values["275"]
                        and expected_rows["277"] == values["277"]
                        and expected_rows["highest_employee_id"] == highest,
                        f"{case['id']} {metric_name} 独立比较结果不符")
            computed[case["id"]] = employee_results
            continue
        actual = calculate_scope(case["employee_ids"], quarter_orders, quarter_quotas)
        for key, value in expected.items():
            if key in {"formula", "display"}:
                continue
            if key == "quota_date":
                actual_value = actual["quota_records"][0]["date"]
            elif key == "quota_source_line":
                actual_value = actual["quota_records"][0]["source_line"]
            elif key == "first_evidence":
                actual_value = actual["order_evidence_first"]
            elif key == "last_evidence":
                actual_value = actual["order_evidence_last"]
            else:
                actual_value = actual.get(key)
            require(actual_value == value,
                    f"{case['id']} {key} 独立核算不符：{actual_value!r} != {value!r}")
        if "display" in expected:
            metric_key = "quota_completion_display" if case["id"] in {"MET-05", "MET-08"} else (
                "average_order_value_display" if case["id"] == "MET-04" else None
            )
            require(metric_key is not None and actual[metric_key] == expected["display"],
                    f"{case['id']} 百分数或金额显示值不符")
        computed[case["id"]] = actual
    return computed


def verify_rel06_csv_evidence(source_map: dict[str, dict],
                              quarter_orders: list[tuple[int, list[str]]],
                              quarter_quotas: list[tuple[int, list[str]]]) -> dict:
    case = next(item for item in FIXTURE["cases"] if item["id"] == "REL-06")
    expected = case["expected_relations"]
    csv_expected = {
        key: value for key, value in expected.items()
        if key in {"employee_id", "employee_source_line", "salesperson_source_line",
                   "salesperson_order_lines_first_last", "quota_source_line",
                   "q3_sales_amount", "q3_quota_amount", "q3_quota_date"}
    }
    employee_source = source_map["adventureworks:employee"]
    salesperson_source = source_map["adventureworks:salesperson"]
    employee_rows = read_tsv(source_path(employee_source), employee_source["rows"],
                             employee_source["columns"], False)
    salesperson_rows = read_tsv(source_path(salesperson_source), salesperson_source["rows"],
                                salesperson_source["columns"], False)
    employee_line = csv_expected["employee_source_line"]
    salesperson_line = csv_expected["salesperson_source_line"]
    employee_id_index = FIXTURE["fixed_analytics_period"]["field_map"]["Employee.csv"]["BusinessEntityID"]
    salesperson_id_index = FIXTURE["fixed_analytics_period"]["field_map"]["SalesPerson.csv"]["BusinessEntityID"]
    employee_id = int(employee_rows[employee_line - 1][employee_id_index])
    salesperson_id = int(salesperson_rows[salesperson_line - 1][salesperson_id_index])
    require(employee_id == csv_expected["employee_id"] == 275,
            "REL-06 employee 来源行与固定员工编号不符")
    require(salesperson_id == employee_id,
            "REL-06 salesperson 来源行与员工编号不一致")
    selected_order_lines = [line for line, row in quarter_orders
                            if row[11] and int(row[11]) == employee_id]
    order_line_bounds = [selected_order_lines[0], selected_order_lines[-1]]
    require(order_line_bounds == csv_expected["salesperson_order_lines_first_last"],
            "REL-06 已发货订单首末来源行不符")
    actual = calculate_scope([employee_id], quarter_orders, quarter_quotas)
    require(actual["sales_amount"] == csv_expected["q3_sales_amount"],
            "REL-06 销售额与 AdventureWorks CSV 独立核算不符")
    require(len(actual["quota_records"]) == 1
            and actual["quota_records"][0]["source_line"] == csv_expected["quota_source_line"]
            and actual["quota_records"][0]["amount"] == csv_expected["q3_quota_amount"]
            and actual["quota_records"][0]["date"] == csv_expected["q3_quota_date"],
            "REL-06 季度目标来源行、日期或金额不符")
    return {
        "case_id": case["id"],
        "employee_source_row": {"line": employee_line, "employee_id": employee_id},
        "salesperson_source_row": {"line": salesperson_line, "business_entity_id": salesperson_id},
        "shipped_order_source_lines": order_line_bounds,
        "sales_amount": actual["sales_amount"],
        "quota_record": actual["quota_records"][0],
    }


def json_bytes(payload: dict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True,
                        allow_nan=False) + "\n").encode("utf-8")


def write_immutable(path: Path, content: bytes) -> bool:
    if path.exists():
        require(path.is_file() and path.read_bytes() == content,
                f"已存在冻结文件与本次结果不同：{path}")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return True


def evidence_artifact_reference(evidence_root: Path, relative_path: Path, content: bytes) -> dict:
    path = evidence_root / relative_path
    require(path.is_relative_to(PROJECT_ROOT), "证据文件路径必须位于项目目录内")
    if path.exists():
        require(path.is_file() and path.read_bytes() == content,
                "冻结文件内容与报告输入不匹配")
    return {"path": path.relative_to(PROJECT_ROOT).as_posix(),
            "sha256": hashlib.sha256(content).hexdigest()}


def copy_source_files(sources: list[dict]) -> list[dict]:
    copied = []
    for source in sources:
        original = original_source_path(source)
        target = target_path(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            require(target.is_file() and file_sha256(target) == source["sha256"],
                    f"已存在来源副本内容不符：{target}")
        else:
            shutil.copyfile(original, target)
        require(file_sha256(target) == source["sha256"],
                f"来源副本 SHA-256 不匹配：{target}")
        copied.append({"source_key": source["source_key"], "path": source["path"],
                       "sha256": file_sha256(target)})
    return copied


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--industry-rag-root", type=Path, default=DEFAULT_INDUSTRY_ROOT)
    parser.add_argument("--revision", choices=("r2", "r3", "r4"), default="r2")
    parser.add_argument("--report-revision", choices=("r2", "r3", "r4", "r5"))
    parser.add_argument("--check-only", action="store_true")
    arguments = parser.parse_args()
    industry_root = arguments.industry_rag_root.resolve()
    global EVIDENCE_ROOT, FIXTURE, RUN_ID, SOURCE_REGISTRY_PATH, SOURCE_RUN_ID
    report_revision = arguments.report_revision or arguments.revision
    require(report_revision != "r4" or arguments.revision == "r3",
            "r4 报告必须引用 r3 来源登记")
    require(report_revision != "r5" or arguments.revision == "r4",
            "r5 报告必须引用 r4 来源登记")
    RUN_ID = f"dc01-inputs-20261004-{report_revision}"
    SOURCE_RUN_ID = f"dc01-inputs-20261004-{arguments.revision}"
    SOURCE_REGISTRY_PATH = SOURCE_ROOT / arguments.revision / "sources.json"
    EVIDENCE_ROOT = PROJECT_ROOT / ".scratch/evidence" / RUN_ID
    FIXTURE = read_json(FIXTURE_PATH)
    if arguments.revision in {"r3", "r4"}:
        schema_document = next(
            item for item in FIXTURE["supplemental_document_sources"]
            if item["source_key"] == "adventureworks:schema-document"
        )
        original_path = Path(schema_document["original_path"])
        schema_document["resource_path"] = original_path.with_name(
            original_path.name + ".md"
        ).as_posix()
    controls = FIXTURE["source_controls"]
    raw_manifest_path = industry_root / "artifacts/raw/documents-v04/manifest.json"
    reference_path = industry_root / "artifacts/verification/v04/fixed/reference_documents.json"
    assessment_manifest_path = PROJECT_ROOT / ".local/assessment/source/manifest.json"
    schema_path = PROJECT_ROOT / ".local/assessment/source/instawdb.sql"
    source_manifest_sha = verify_control(
        raw_manifest_path, controls["raw_document_manifest"]["sha256"], "Industry RAG 原始来源清单"
    )
    reference_sha = verify_control(
        reference_path, controls["reference_documents"]["sha256"], "Industry RAG 固定参考"
    )
    require(str(reference_path) == controls["reference_documents"]["path"],
            "Industry RAG 固定参考路径与输入登记不符")
    verify_control(assessment_manifest_path,
                   controls["assessment_source_manifest"]["sha256"], "AdventureWorks 考核来源清单")
    verify_control(schema_path, controls["assessment_schema_reference"]["sha256"],
                   "AdventureWorks schema reference")
    verify_control(PROJECT_ROOT / controls["assessment_access_profiles"]["path"],
                   controls["assessment_access_profiles"]["sha256"], "固定权限配置")
    verify_control(PROJECT_ROOT / controls["project_boundary"]["path"],
                   controls["project_boundary"]["sha256"], "数据中台边界说明")
    manifest = read_json(raw_manifest_path)
    reference = read_json(reference_path)
    assessment_manifest = read_json(assessment_manifest_path)
    require(assessment_manifest.get("source_id") == ASSESSMENT_SOURCE_ID
            and assessment_manifest.get("revision") == ASSESSMENT_REVISION,
            "AdventureWorks 考核来源身份或修订不匹配")
    manifest_sources = {item["name"]: item for item in assessment_manifest["files"]}
    for item in DATA_SOURCES:
        if item["source_key"].startswith("adventureworks:") and "rows" in item:
            metadata = manifest_sources[item["resource_id"]]
            require(metadata["sha256"] == item["sha256"]
                    and metadata["rows"] == item["rows"],
                    f"{item['resource_id']} 与 AdventureWorks 来源清单不一致")
    global_sources = build_sources(industry_root, reference, manifest)
    source_map = {item["source_key"]: item for item in global_sources}
    if arguments.revision in {"r3", "r4"}:
        schema_resource_id = source_map["adventureworks:schema-document"]["resource"]["resource_id"]
        require(schema_resource_id.endswith("/.local/assessment/source/instawdb.sql.md"),
                "r3 AdventureWorks schema ResourceRef 必须使用载体文件后缀")
    source_results = verify_source_files(global_sources)
    document_summary = verify_documents(manifest, reference, source_manifest_sha,
                                        reference_sha, source_map)
    fixture_summary = validate_fixture(reference, global_sources)
    supplemental_document_check = verify_supplemental_document_references(global_sources)
    order_groups, quota_groups = read_analytics_sources(source_map)
    analytics_results = verify_analytics_cases(order_groups["all"], quota_groups["all"])
    rel06_csv_check = verify_rel06_csv_evidence(source_map, order_groups["all"], quota_groups["all"])
    document_ingest = build_document_ingest(FIXTURE, global_sources)
    hybrid_queries = build_document_queries(FIXTURE, global_sources, "hybrid")
    graph_queries = build_document_queries(FIXTURE, global_sources, "graph")
    same_query_inputs = (
        len(hybrid_queries) == len(graph_queries)
        and [query_comparison_signature(item) for item in hybrid_queries]
        == [query_comparison_signature(item) for item in graph_queries]
    )
    require(same_query_inputs, "hybrid 与 graph 查询的问题或来源集合不一致")
    host_sources = build_host_sources(FIXTURE, global_sources)
    document_ingest_reference = evidence_artifact_reference(
        EVIDENCE_ROOT, Path("ingest/document_ingest.json"), json_bytes(document_ingest)
    )
    hybrid_queries_reference = evidence_artifact_reference(
        EVIDENCE_ROOT, Path("queries/hybrid_queries.json"), json_bytes(hybrid_queries)
    )
    graph_queries_reference = evidence_artifact_reference(
        EVIDENCE_ROOT, Path("queries/graph_queries.json"), json_bytes(graph_queries)
    )
    host_sources_reference = evidence_artifact_reference(
        EVIDENCE_ROOT, Path("host_sources.json"), json_bytes(host_sources)
    )
    profile_fixture = read_json(PROJECT_ROOT / controls["assessment_access_profiles"]["path"])
    profiles = {profile["username"]: profile for profile in profile_fixture["test_profiles"]}
    for case in FIXTURE["cases"]:
        actor_profile = case.get("actor_profile")
        if actor_profile and "actor_employee_id" in case:
            require(profiles[actor_profile]["employee_id"] == case["actor_employee_id"],
                    f"{case['id']} 身份员工编号与固定权限配置不符")
        if actor_profile and "actor_managed_employee_ids" in case:
            require(profiles[actor_profile]["managed_employee_ids"]
                    == case["actor_managed_employee_ids"],
                    f"{case['id']} 团队范围与固定权限配置不符")
    sources_payload = {
        "schema_version": 1,
        "registry_id": SOURCE_RUN_ID,
        "source_root": ".local/data-center/sources",
        "provenance": {
            "industry_rag_manifest": {"path": str(raw_manifest_path), "sha256": source_manifest_sha},
            "industry_rag_reference": {"path": str(reference_path), "sha256": reference_sha},
        "assessment_manifest": {"path": str(assessment_manifest_path),
                                    "sha256": file_sha256(assessment_manifest_path)},
            "adventureworks_schema": {
                "path": str(schema_path), "revision": ORDER_REVISION,
                "license_id": "MIT", "sha256": SCHEMA_SHA256,
            },
        },
        "sources": global_sources,
    }
    schedule = [
        {"case_id": case["id"], "category": case["category"], "attempt": attempt,
         "state": "planned"}
        for case in FIXTURE["cases"]
        for attempt in range(1, FIXTURE["execution_policy"]["attempts_per_scenario"] + 1)
    ]
    require(len(schedule) == 90, "生成的固定执行计划数量不为 90")
    report = {
        "milestone": "DC-01",
        "revision": arguments.revision,
        "report_revision": report_revision,
        "run_id": RUN_ID,
        "status": "frozen_inputs_ready",
        "prepared_date": FIXTURE["prepared_date"],
        "preparation_script_sha256": file_sha256(Path(__file__)),
        "source_fixture": "tests/fixtures/data_center_cases.json",
        "source_fixture_sha256": file_sha256(FIXTURE_PATH),
        "source_registry": str(SOURCE_REGISTRY_PATH.relative_to(PROJECT_ROOT)),
        "source_registry_sha256": hashlib.sha256(json_bytes(sources_payload)).hexdigest(),
        "document_ingest": {
            **document_ingest_reference,
            "document_count": len(document_ingest["documents"]),
            "graph_enabled": document_ingest["graph_enabled"],
        },
        "document_queries": {
            "hybrid_path": hybrid_queries_reference["path"],
            "graph_path": graph_queries_reference["path"],
            "hybrid_sha256": hybrid_queries_reference["sha256"],
            "graph_sha256": graph_queries_reference["sha256"],
            "query_count": len(hybrid_queries),
            "relation_case_ids": [f"REL-{number:02d}" for number in range(1, 7)],
            "same_query_inputs_for_comparison": same_query_inputs,
        },
        "host_sources": {
            **host_sources_reference,
            "document_source_count": len(host_sources["sources"]),
            "business_source_count": 0,
        },
        "source_files": source_results,
        "source_file_count": len(source_results),
        "document_reference_check": document_summary,
        "supplemental_document_check": supplemental_document_check,
        "fixed_inputs": fixture_summary,
        "assessment_data_check": {
            "source_id": ASSESSMENT_SOURCE_ID,
            "revision": ASSESSMENT_REVISION,
            "shipped_q3_order_rows": len(order_groups["all"]),
            "q3_quota_rows": len(quota_groups["all"]),
            "currency_status": FIXTURE["fixed_analytics_period"]["currency_status"],
        },
        "independent_analytics": analytics_results,
        "independent_rel06_csv_evidence": rel06_csv_check,
        "execution_plan": schedule,
        "execution_policy": FIXTURE["execution_policy"],
        "acceptance_thresholds": FIXTURE["acceptance_thresholds"],
        "preparation_calls": {"model_calls": 0, "provider_requests": 0, "new_cost_cny": "0.00"},
        "frozen": not arguments.check_only,
    }
    output = {"status": "verified", "run_id": report["run_id"],
              "source_count": len(global_sources), "planned_attempts": len(schedule),
              "categories": fixture_summary["category_counts"],
              "source_registry": str(SOURCE_REGISTRY_PATH),
              "document_ingest": str(EVIDENCE_ROOT / "ingest/document_ingest.json"),
              "hybrid_queries": str(EVIDENCE_ROOT / "queries/hybrid_queries.json"),
              "graph_queries": str(EVIDENCE_ROOT / "queries/graph_queries.json"),
              "host_sources": str(EVIDENCE_ROOT / "host_sources.json"),
              "evidence_report": str(EVIDENCE_ROOT / "report.json"),
              "check_only": arguments.check_only}
    if not arguments.check_only:
        copied = copy_source_files(global_sources)
        sources_payload["copies_verified"] = copied
        report["source_copies"] = copied
        sources_bytes = json_bytes(sources_payload)
        report["source_registry_sha256"] = hashlib.sha256(sources_bytes).hexdigest()
        evidence_files = (
            (EVIDENCE_ROOT / "upstream_document_manifest.json", raw_manifest_path),
            (EVIDENCE_ROOT / "reference_documents.json", reference_path),
            (EVIDENCE_ROOT / "assessment_source_manifest.json", assessment_manifest_path),
        )
        for target, original in evidence_files:
            write_immutable(target, original.read_bytes())
        write_immutable(SOURCE_REGISTRY_PATH, sources_bytes)
        write_immutable(EVIDENCE_ROOT / "ingest/document_ingest.json", json_bytes(document_ingest))
        write_immutable(EVIDENCE_ROOT / "queries/hybrid_queries.json", json_bytes(hybrid_queries))
        write_immutable(EVIDENCE_ROOT / "queries/graph_queries.json", json_bytes(graph_queries))
        write_immutable(EVIDENCE_ROOT / "host_sources.json", json_bytes(host_sources))
        write_immutable(EVIDENCE_ROOT / "report.json", json_bytes(report))
    print(json.dumps(output, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
