import json
import os
import subprocess
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from sqlalchemy.engine import make_url

PROJECT_DIR = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_DIR / ".local/data/orders/beaab06ef72831089ca80e5355d65e661fd19b26"
DATABASE_URL = "postgresql+psycopg://shop@127.0.0.1:55432/shop_test"
EXPECTED_SOURCE_HASHES = {
    "SalesOrderHeader.csv": "cf637c7d58006ebbc13daf0b8ed6cf7f738165d298ae0b5479a59237429c1882",
    "SalesOrderDetail.csv": "220129977aa63e34fa4a1d66c2604bd2184373151efb09c98b343371ba77ab8a",
    "Product.csv": "df0379a7cba8b11b97ea5d104cf1313ef9f683137b6426f2fac6bb8c74c2a2ea",
    "Customer.csv": "4704aa5b46aed326599014524c821aca50e938587e6a1133c0f6d9aae5c5efc6",
    "Person.csv": "e22b399865b5f322eda6b34c4c4532165a1ee62cb2362d3f6bb8c1eebbecd896",
    "Store.csv": "5f9d31ef5c8a4dfa2243c861654d9962a7e151ed0a5f0f7834d55184952b0520",
}
CODE_PATHS = (
    "tests/test_orders.py",
    "scripts/verify_customer_orders.py",
    "src/shop/adventureworks.py",
    "src/shop/customer_models.py",
    "src/shop/orders.py",
    "src/shop/models.py",
)


def file_hash(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def code_hashes() -> dict[str, str | None]:
    return {
        name: file_hash(PROJECT_DIR / name) if (PROJECT_DIR / name).is_file() else None
        for name in CODE_PATHS
    }


def source_manifest() -> dict[str, dict[str, str | int | None]]:
    result = {}
    for name, expected_hash in EXPECTED_SOURCE_HASHES.items():
        path = SOURCE_DIR / name
        result[name] = {
            "sha256": file_hash(path) if path.is_file() else None,
            "expected_sha256": expected_hash,
            "bytes": path.stat().st_size if path.is_file() else None,
        }
    return result


def database_identity(database_url: str) -> dict[str, str | int | None]:
    url = make_url(database_url)
    return {
        "driver": f"{url.get_backend_name()}+{url.get_driver_name()}",
        "host": url.host,
        "port": url.port,
        "database": url.database,
    }


def write_json(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    run_id = f"cus01-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    evidence_dir = PROJECT_DIR / ".scratch/evidence" / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    code_before = code_hashes()
    files = source_manifest()
    environment = os.environ.copy()
    environment["SHOP_TEST_DATABASE_URL"] = DATABASE_URL
    environment["HF_HOME"] = str(PROJECT_DIR / ".scratch/huggingface")
    environment["UV_CACHE_DIR"] = str(PROJECT_DIR / ".scratch/uv-cache")
    pytest_base = evidence_dir / "pytest-data"
    command = [
        str(PROJECT_DIR / ".venv/bin/python"),
        "-m", "pytest", "-q", "-s", "tests/test_orders.py",
        f"--basetemp={pytest_base}",
    ]
    inputs = {
        "run_id": run_id,
        "scope": "CUS-01",
        "source_id": "microsoft/adventureworks-oltp",
        "revision": "beaab06ef72831089ca80e5355d65e661fd19b26",
        "source_directory": str(SOURCE_DIR.relative_to(PROJECT_DIR)),
        "source_files": files,
        "database": database_identity(DATABASE_URL),
        "configuration": {
            "database": "shop_test",
            "schema": "db_session fixture creates and removes a unique PostgreSQL schema",
            "model_calls": 0,
            "api_coverage": "anonymous access and API ownership are checked by the main agent",
        },
        "code_sha256_before": code_before,
        "command": command,
    }
    write_json(evidence_dir / "inputs.json", inputs)
    source_errors = [
        name for name, item in files.items()
        if item["sha256"] != item["expected_sha256"]
    ]
    started = perf_counter()
    if source_errors:
        result_code = 2
        stdout = ""
        stderr = "来源文件缺失或 SHA-256 与固定版本不符：" + ", ".join(source_errors)
    elif not (PROJECT_DIR / ".venv/bin/python").is_file():
        result_code = 2
        stdout = ""
        stderr = "项目 .venv/bin/python 不存在"
    else:
        result = subprocess.run(
            command,
            cwd=PROJECT_DIR,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        result_code = result.returncode
        stdout = result.stdout
        stderr = result.stderr
    elapsed = round(perf_counter() - started, 3)
    (evidence_dir / "orders.stdout.txt").write_text(stdout, encoding="utf-8")
    (evidence_dir / "orders.stderr.txt").write_text(stderr, encoding="utf-8")
    code_after = code_hashes()
    db_identity_line = next(
        (line.removeprefix("CUS01_DB_IDENTITY=") for line in stdout.splitlines()
         if line.startswith("CUS01_DB_IDENTITY=")),
        None,
    )
    try:
        observed_database = json.loads(db_identity_line) if db_identity_line else None
    except json.JSONDecodeError:
        observed_database = None
    report = {
        "run_id": run_id,
        "status": "passed" if result_code == 0 and code_before == code_after else "failed",
        "scope": "CUS-01 data import, bindings, order summaries, details, pagination and filters",
        "check_count": 1,
        "failed_check_count": 0 if result_code == 0 and code_before == code_after else 1,
        "exit_code": result_code if code_before == code_after else 3,
        "elapsed_seconds": elapsed,
        "source_files": files,
        "code_sha256_before": code_before,
        "code_sha256_after": code_after,
        "database_configuration": inputs["database"],
        "observed_database": observed_database,
        "pytest_output": {
            "stdout_file": "orders.stdout.txt",
            "stderr_file": "orders.stderr.txt",
        },
        "api_boundary": "Anonymous access and API ownership are checked by the main agent.",
    }
    write_json(evidence_dir / "report.json", report)
    print(json.dumps({"run_id": run_id, **report}, ensure_ascii=False), flush=True)
    if code_before != code_after:
        return 3
    return result_code


if __name__ == "__main__":
    raise SystemExit(main())
