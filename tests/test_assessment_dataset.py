import csv
import json
import shutil
import subprocess
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from shop.assessment_dataset import (
    PERIOD_END,
    PERIOD_START,
    REVISION,
    SOURCE_ID,
    load_assessment_source,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OFFICIAL_SOURCE_DIRECTORY = PROJECT_ROOT / ".local" / "assessment" / "source"


def test_loads_fixed_microsoft_source_rows_and_period_targets():
    dataset = load_assessment_source(OFFICIAL_SOURCE_DIRECTORY)

    assert SOURCE_ID == "microsoft/adventureworks"
    assert REVISION == "beaab06ef72831089ca80e5355d65e661fd19b26"
    assert PERIOD_START == datetime(2023, 7, 1)  # noqa: DTZ001
    assert PERIOD_END == datetime(2023, 10, 1)  # noqa: DTZ001
    assert len(dataset.employees) == 290
    assert len(dataset.salespeople) == 17
    assert len(dataset.quotas) == 14
    assert len({record.employee_id for record in dataset.quotas}) == 14
    assert all(PERIOD_START <= record.quota_date < PERIOD_END for record in dataset.quotas)
    assert all(isinstance(record.amount, Decimal) for record in dataset.quotas)
    assert all(record.amount.is_finite() and record.amount > 0 for record in dataset.quotas)
    assert all(isinstance(record.source_line, int) and record.source_line > 0 for record in dataset.employees)
    assert all(isinstance(record.source_line, int) and record.source_line > 0 for record in dataset.salespeople)
    assert all(isinstance(record.source_line, int) and record.source_line > 0 for record in dataset.quotas)
    assert dataset.manifest["source_id"] == SOURCE_ID
    assert dataset.manifest["revision"] == REVISION
    assert dataset.manifest["files"] == [
        {"name": "Employee.csv", "sha256": "111b1ba4a51bf982c7f8841cdb14c14374fc4552a22e7ba067ef728e7ae72258", "rows": 290},
        {"name": "SalesPerson.csv", "sha256": "92ed21c0e22bf0060a746d53c1f8f0ed68a1ece4b0d5b9c60dbc233d52499206", "rows": 17},
        {"name": "SalesPersonQuotaHistory.csv", "sha256": "769cfb3d44c1f422dd23eb24bc0d310f82739eb6f49598520a3eb47e68161064", "rows": 163},
    ]


def test_rejects_changed_official_source_copy(tmp_path: Path):
    for name in ("Employee.csv", "SalesPerson.csv", "SalesPersonQuotaHistory.csv"):
        shutil.copyfile(OFFICIAL_SOURCE_DIRECTORY / name, tmp_path / name)

    with (tmp_path / "Employee.csv").open("a", encoding="utf-8", newline="") as source_file:
        source_file.write("\n")

    with pytest.raises(ValueError, match="SHA-256"):
        load_assessment_source(tmp_path)


def test_repeated_preparation_keeps_source_manifest_and_counts():
    manifest_path = OFFICIAL_SOURCE_DIRECTORY / "manifest.json"
    original_manifest = manifest_path.read_bytes()

    first = subprocess.run(
        [
            sys.executable,
            "scripts/prepare_assessment_data.py",
            "--directory",
            str(OFFICIAL_SOURCE_DIRECTORY),
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    second = subprocess.run(
        [
            sys.executable,
            "scripts/prepare_assessment_data.py",
            "--directory",
            str(OFFICIAL_SOURCE_DIRECTORY),
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    first_result = json.loads(first.stdout)
    second_result = json.loads(second.stdout)

    assert first_result["employee_count"] == second_result["employee_count"] == 290
    assert first_result["salesperson_count"] == second_result["salesperson_count"] == 17
    assert first_result["quota_count"] == second_result["quota_count"] == 14
    assert manifest_path.read_bytes() == original_manifest


def test_employee_csv_shape_matches_official_column_order():
    with (OFFICIAL_SOURCE_DIRECTORY / "Employee.csv").open(
        encoding="utf-8-sig", newline=""
    ) as source_file:
        employee_row = next(csv.reader(source_file, delimiter="\t"))

    assert len(employee_row) == 16
    assert employee_row[0] == "1"
    assert employee_row[5] == "Chief Executive Officer"
    assert employee_row[13] == "1"
