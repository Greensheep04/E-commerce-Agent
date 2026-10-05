import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from shop.budget import VERIFICATION_LIMIT
from shop.config import Settings
from shop.fashion import REVISION, SOURCE_ID
from shop.model_profiles import profiles
from shop.runtime_models import BudgetAccount, ProviderRequest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHECK_TO_TEST = {
    "cancel_no_new_request_and_reservation_release": (
        "test_cancel_after_reservation_releases_cost_and_blocks_replay"
    ),
    "concurrent_run_exclusion": "test_concurrent_campaign_run_is_rejected_by_postgres_lock",
    "six_model_call_limit": "test_six_model_calls_stop_before_creating_a_seventh_request",
    "ten_tool_call_limit": "test_tenth_campaign_tool_is_allowed_and_eleventh_is_rejected",
    "cumulative_120_second_limit": ("test_cumulative_execution_time_stops_before_request_creation"),
    "unknown_request_stop": (
        "test_unknown_external_request_stops_campaign_without_releasing_reservation"
    ),
    "saved_real_response_reuse_and_parameter_guard": (
        "test_saved_real_understand_response_replays_without_parameter_drift"
    ),
    "atomic_task_profile_snapshot": "test_campaign_profile_snapshot_is_saved_with_task_atomically",
}
SOURCE_FILES = (
    "src/shop/campaign.py",
    "src/shop/harness.py",
    "src/shop/paid_models.py",
    "src/shop/budget.py",
    "src/shop/runtime_models.py",
    "src/shop/model_profiles.py",
    "src/shop/config.py",
    "src/shop/fashion.py",
    "tests/conftest.py",
    "tests/test_campaign_runtime.py",
    "scripts/verify_campaign_boundaries.py",
)


def file_hashes() -> dict[str, str]:
    return {path: sha256((PROJECT_ROOT / path).read_bytes()).hexdigest() for path in SOURCE_FILES}


def database_snapshot(database_url: str) -> dict:
    engine = create_engine(database_url)
    try:
        with Session(engine) as session:
            account = session.get(BudgetAccount, "project")
            verification_requests = session.scalars(
                select(ProviderRequest).where(
                    ProviderRequest.price_basis["verification"].as_boolean()
                )
            ).all()
            spent = account.spent_cny if account is not None else 0
            reserved = account.reserved_cny if account is not None else 0
            verification_total = sum(
                (item.charged_cny if item.charged_cny is not None else item.reservation_cny)
                for item in verification_requests
            )
            return {
                "spent_cny": str(spent),
                "reserved_cny": str(reserved),
                "verification_total_cny": str(verification_total),
                "verification_limit_cny": str(VERIFICATION_LIMIT),
                "verification_requests": len(verification_requests),
            }
    finally:
        engine.dispose()


def case_results(junit_path: Path) -> dict[str, str]:
    if not junit_path.is_file():
        return {name: "not_executed" for name in CHECK_TO_TEST}
    root = ET.parse(junit_path).getroot()
    by_test = {}
    for case in root.findall(".//testcase"):
        name = case.attrib["name"]
        if case.find("failure") is not None:
            by_test[name] = "failed"
        elif case.find("error") is not None:
            by_test[name] = "error"
        elif case.find("skipped") is not None:
            by_test[name] = "skipped"
        else:
            by_test[name] = "passed"
    return {
        check: by_test.get(test_name, "not_executed") for check, test_name in CHECK_TO_TEST.items()
    }


def main() -> None:
    settings = Settings()
    main_url = make_url(settings.database_url)
    test_url = main_url.set(database="shop_test")
    if test_url.database == main_url.database:
        raise ValueError("边界测试必须连接独立 shop_test 数据库")

    run_id = f"har-boundaries-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_dir = PROJECT_ROOT / ".scratch" / "evidence" / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    junit_path = evidence_dir / "pytest.xml"
    test_command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:cacheprovider",
        f"--basetemp={evidence_dir / 'pytest-temp'}",
        f"--junitxml={junit_path}",
        "tests/test_campaign_runtime.py",
    ]
    child_environment = dict(os.environ)
    child_environment["SHOP_TEST_DATABASE_URL"] = test_url.render_as_string(hide_password=False)
    result = subprocess.run(
        test_command,
        cwd=PROJECT_ROOT,
        env=child_environment,
        capture_output=True,
        text=True,
        check=False,
    )
    stdout_path = evidence_dir / "pytest.stdout.txt"
    stderr_path = evidence_dir / "pytest.stderr.txt"
    stdout_path.write_text(result.stdout, encoding="utf-8")
    stderr_path.write_text(result.stderr, encoding="utf-8")

    source_hashes = file_hashes()
    outcomes = case_results(junit_path)
    passed = result.returncode == 0 and all(value == "passed" for value in outcomes.values())
    report = {
        "run_id": run_id,
        "status": "passed_with_checkpoint_pending" if passed else "failed",
        "milestone": "HAR-01",
        "command": test_command,
        "exit_code": result.returncode,
        "checks": outcomes,
        "campaign_checkpoint_new_process_recovery": {
            "status": "not_executed",
            "reason": "需要一次 Campaign understand 真实请求；本地边界运行未发起外部请求。",
        },
        "input": {
            "main_source_id": SOURCE_ID,
            "main_source_revision": REVISION,
            "saved_real_response_run_id": "0c5fd569-c140-40a8-ab0d-325b22dbf5dc",
            "saved_real_response_report": (
                ".scratch/evidence/har-models-20260930T071204-0c5fd569/report.json"
            ),
            "model_profiles": {
                role: profile.model_dump()
                for role, profile in profiles(settings.harness_token_limits_file).items()
            },
        },
        "database": {
            "main": {
                "driver": main_url.drivername,
                "host": main_url.host,
                "port": main_url.port,
                "database": main_url.database,
                **database_snapshot(settings.database_url),
            },
            "tests": {
                "driver": test_url.drivername,
                "host": test_url.host,
                "port": test_url.port,
                "database": test_url.database,
            },
        },
        "execution": {
            "new_model_requests_from_boundary_suite": 0,
            "pytest_stdout": str(stdout_path.relative_to(PROJECT_ROOT)),
            "pytest_stderr": str(stderr_path.relative_to(PROJECT_ROOT)),
            "junit_xml": str(junit_path.relative_to(PROJECT_ROOT)),
            "source_sha256": source_hashes,
        },
    }
    report_path = evidence_dir / "report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "run_id": run_id,
                "status": report["status"],
                "report": str(report_path.relative_to(PROJECT_ROOT)),
                "checks": outcomes,
                "exit_code": result.returncode,
            },
            ensure_ascii=False,
        )
    )
    if result.returncode != 0:
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
