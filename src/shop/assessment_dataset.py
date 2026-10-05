import csv
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

SOURCE_ID = "microsoft/adventureworks"
REVISION = "beaab06ef72831089ca80e5355d65e661fd19b26"
# 来源日期没有时区字段，保留为无时区 datetime。
PERIOD_START = datetime(2023, 7, 1)  # noqa: DTZ001
PERIOD_END = datetime(2023, 10, 1)  # noqa: DTZ001
MANIFEST_NAME = "manifest.json"

SOURCE_FILES = (
    {
        "name": "Employee.csv",
        "sha256": "111b1ba4a51bf982c7f8841cdb14c14374fc4552a22e7ba067ef728e7ae72258",
        "rows": 290,
        "columns": 16,
    },
    {
        "name": "SalesPerson.csv",
        "sha256": "92ed21c0e22bf0060a746d53c1f8f0ed68a1ece4b0d5b9c60dbc233d52499206",
        "rows": 17,
        "columns": 9,
    },
    {
        "name": "SalesPersonQuotaHistory.csv",
        "sha256": "769cfb3d44c1f422dd23eb24bc0d310f82739eb6f49598520a3eb47e68161064",
        "rows": 163,
        "columns": 5,
    },
)


@dataclass(frozen=True)
class EmployeeRecord:
    employee_id: int
    job_title: str
    current_flag: bool
    source_line: int


@dataclass(frozen=True)
class SalespersonRecord:
    employee_id: int
    territory_id: int | None
    source_line: int


@dataclass(frozen=True)
class QuotaRecord:
    employee_id: int
    quota_date: datetime
    amount: Decimal
    source_line: int


@dataclass(frozen=True)
class AssessmentDataset:
    employees: list[EmployeeRecord]
    salespeople: list[SalespersonRecord]
    quotas: list[QuotaRecord]
    manifest: dict


def _read_file_rows(directory: Path, source_file: dict) -> tuple[list[tuple[int, list[str]]], dict]:
    path = directory / source_file["name"]
    content = path.read_bytes()
    actual_sha256 = sha256(content).hexdigest()
    if actual_sha256 != source_file["sha256"]:
        raise ValueError(
            f"{source_file['name']} SHA-256 与固定来源不符：{actual_sha256}"
        )
    with path.open(encoding="utf-8-sig", newline="") as file_handle:
        rows = [
            (source_line, row)
            for source_line, row in enumerate(
                csv.reader(file_handle, delimiter="\t", strict=True),
                start=1,
            )
        ]
    if len(rows) != source_file["rows"]:
        raise ValueError(
            f"{source_file['name']} 来源行数为 {len(rows)}，"
            f"固定值为 {source_file['rows']}"
        )
    for source_line, row in rows:
        if len(row) != source_file["columns"]:
            raise ValueError(
                f"{source_file['name']} 第 {source_line} 行列数为 {len(row)}，"
                f"固定值为 {source_file['columns']}"
            )
    return rows, {
        "name": source_file["name"],
        "sha256": actual_sha256,
        "rows": len(rows),
    }


def _positive_integer(value: str, filename: str, source_line: int, field: str) -> int:
    if not value.isdecimal() or int(value) <= 0:
        raise ValueError(
            f"{filename} 第 {source_line} 行字段 {field} 不是正整数：{value!r}"
        )
    return int(value)


def _parse_employee_rows(rows: list[tuple[int, list[str]]]) -> list[EmployeeRecord]:
    employees = []
    for source_line, row in rows:
        employee_id = _positive_integer(row[0], "Employee.csv", source_line, "BusinessEntityID")
        job_title = row[5]
        if not job_title.strip():
            raise ValueError(f"Employee.csv 第 {source_line} 行 JobTitle 为空")
        current_value = row[13]
        if current_value not in {"0", "1"}:
            raise ValueError(
                f"Employee.csv 第 {source_line} 行 CurrentFlag 不是 0 或 1：{current_value!r}"
            )
        employees.append(
            EmployeeRecord(
                employee_id=employee_id,
                job_title=job_title,
                current_flag=current_value == "1",
                source_line=source_line,
            )
        )
    return employees


def _parse_salesperson_rows(rows: list[tuple[int, list[str]]]) -> list[SalespersonRecord]:
    salespeople = []
    for source_line, row in rows:
        employee_id = _positive_integer(
            row[0], "SalesPerson.csv", source_line, "BusinessEntityID"
        )
        territory_id = (
            None
            if row[1] == ""
            else _positive_integer(row[1], "SalesPerson.csv", source_line, "TerritoryID")
        )
        salespeople.append(
            SalespersonRecord(
                employee_id=employee_id,
                territory_id=territory_id,
                source_line=source_line,
            )
        )
    return salespeople


def _parse_quota_rows(rows: list[tuple[int, list[str]]]) -> list[QuotaRecord]:
    quotas = []
    for source_line, row in rows:
        employee_id = _positive_integer(
            row[0], "SalesPersonQuotaHistory.csv", source_line, "BusinessEntityID"
        )
        quota_date = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")  # noqa: DTZ007
        amount = Decimal(row[2])
        if not amount.is_finite() or amount <= 0:
            raise ValueError(
                f"SalesPersonQuotaHistory.csv 第 {source_line} 行 SalesQuota "
                f"必须是有限正数：{row[2]!r}"
            )
        quotas.append(
            QuotaRecord(
                employee_id=employee_id,
                quota_date=quota_date,
                amount=amount,
                source_line=source_line,
            )
        )
    return quotas


def _validate_relationships(
    employees: list[EmployeeRecord],
    salespeople: list[SalespersonRecord],
    quotas: list[QuotaRecord],
) -> None:
    employee_ids = [record.employee_id for record in employees]
    if len(employee_ids) != len(set(employee_ids)):
        raise ValueError("Employee.csv 存在重复 BusinessEntityID")
    employee_id_set = set(employee_ids)

    salesperson_ids = [record.employee_id for record in salespeople]
    if len(salesperson_ids) != len(set(salesperson_ids)):
        raise ValueError("SalesPerson.csv 存在重复 BusinessEntityID")
    salesperson_id_set = set(salesperson_ids)
    for record in salespeople:
        if record.employee_id not in employee_id_set:
            raise ValueError(
                f"SalesPerson.csv 第 {record.source_line} 行员工编号 "
                f"{record.employee_id} 在 Employee.csv 中不存在"
            )

    quota_keys = set()
    for record in quotas:
        if record.employee_id not in salesperson_id_set:
            raise ValueError(
                f"SalesPersonQuotaHistory.csv 第 {record.source_line} 行员工编号 "
                f"{record.employee_id} 在 SalesPerson.csv 中不存在"
            )
        key = (record.employee_id, record.quota_date)
        if key in quota_keys:
            raise ValueError(
                f"SalesPersonQuotaHistory.csv 第 {record.source_line} 行季度目标重复："
                f"employee_id={record.employee_id}, quota_date={record.quota_date.isoformat()}"
            )
        quota_keys.add(key)


def load_assessment_source(directory: Path) -> AssessmentDataset:
    directory = Path(directory)
    file_rows = []
    file_manifest = []
    for source_file in SOURCE_FILES:
        rows, metadata = _read_file_rows(directory, source_file)
        file_rows.append(rows)
        file_manifest.append(metadata)

    employees = _parse_employee_rows(file_rows[0])
    salespeople = _parse_salesperson_rows(file_rows[1])
    all_quotas = _parse_quota_rows(file_rows[2])
    _validate_relationships(employees, salespeople, all_quotas)
    quotas = [
        record
        for record in all_quotas
        if PERIOD_START <= record.quota_date < PERIOD_END
    ]

    manifest = {
        "source_id": SOURCE_ID,
        "revision": REVISION,
        "files": file_manifest,
    }
    manifest_path = directory / MANIFEST_NAME
    if manifest_path.is_file():
        stored_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if stored_manifest != manifest:
            raise ValueError(f"{MANIFEST_NAME} 与固定来源文件摘要不符")

    return AssessmentDataset(
        employees=employees,
        salespeople=salespeople,
        quotas=quotas,
        manifest=manifest,
    )
