import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

from shop.assessment_dataset import (
    MANIFEST_NAME,
    REVISION,
    SOURCE_FILES,
    load_assessment_source,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIRECTORY = PROJECT_ROOT / ".local" / "assessment" / "source"
SOURCE_URL_BASE = (
    "https://raw.githubusercontent.com/microsoft/sql-server-samples/"
    f"{REVISION}/samples/databases/adventure-works/oltp-install-script"
)
SCHEMA_REFERENCE = {
    "name": "instawdb.sql",
    "url": f"{SOURCE_URL_BASE}/instawdb.sql",
    "sha256": "d979f036750f9c7b4fb7be3d58cdbb596a74f864a6325998501b3a96c6ad53f9",
    "field_terminator": "tab",
    "header": False,
    "column_index_base": 0,
    "column_counts": {
        "Employee.csv": 16,
        "SalesPerson.csv": 9,
        "SalesPersonQuotaHistory.csv": 5,
    },
    "employee_columns": {
        "BusinessEntityID": 0,
        "OrganizationNode": 3,
        "OrganizationLevel_computed": 4,
        "JobTitle": 5,
        "CurrentFlag": 13,
    },
}


def _write_json(path: Path, content: dict) -> None:
    path.write_text(
        json.dumps(content, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def prepare_source(directory: Path) -> dict:
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    download_count = 0
    for source_file in SOURCE_FILES:
        path = directory / source_file["name"]
        if not path.exists():
            with urlopen(f"{SOURCE_URL_BASE}/{source_file['name']}") as response:
                path.write_bytes(response.read())
            download_count += 1

    dataset = load_assessment_source(directory)
    manifest_path = directory / MANIFEST_NAME
    if manifest_path.exists():
        existing_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing_manifest != dataset.manifest:
            raise ValueError(f"{MANIFEST_NAME} 与固定来源文件摘要不符")
    else:
        _write_json(manifest_path, dataset.manifest)

    verified_dataset = load_assessment_source(directory)
    run_id = f"assessment-data-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_directory = PROJECT_ROOT / ".scratch" / "evidence" / run_id
    evidence_directory.mkdir(parents=True, exist_ok=False)
    source_hashes = {
        "src/shop/assessment_dataset.py": sha256(
            (PROJECT_ROOT / "src" / "shop" / "assessment_dataset.py").read_bytes()
        ).hexdigest(),
        "scripts/prepare_assessment_data.py": sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    report = {
        "run_id": run_id,
        "status": "verified",
        "source": verified_dataset.manifest,
        "schema_reference": SCHEMA_REFERENCE,
        "record_counts": {
            "employees": len(verified_dataset.employees),
            "salespeople": len(verified_dataset.salespeople),
            "quotas_in_period": len(verified_dataset.quotas),
        },
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256(manifest_path.read_bytes()).hexdigest(),
        "code_sha256": source_hashes,
        "execution": {
            "source_downloads": download_count,
            "source_rows_validated": sum(item["rows"] for item in SOURCE_FILES),
            "period_quota_rows_returned": len(verified_dataset.quotas),
        },
    }
    report_path = evidence_directory / "report.json"
    _write_json(report_path, report)
    return {
        "run_id": run_id,
        "report": str(report_path.relative_to(PROJECT_ROOT)),
        "manifest": str(manifest_path),
        "employee_count": len(verified_dataset.employees),
        "salesperson_count": len(verified_dataset.salespeople),
        "quota_count": len(verified_dataset.quotas),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    arguments = parser.parse_args()
    print(json.dumps(prepare_source(arguments.directory), ensure_ascii=False))


if __name__ == "__main__":
    main()
