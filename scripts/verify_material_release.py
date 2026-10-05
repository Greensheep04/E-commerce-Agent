import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.fashion import REVISION, SOURCE_ID
from shop.models import Asset, ImportBatch, Source

PROJECT_DIR = Path(__file__).resolve().parents[1]


def source_manifest():
    paths = [
        PROJECT_DIR / name
        for name in ("pyproject.toml", "uv.lock", ".env.example", "alembic.ini")
    ]
    for directory in ("src", "scripts", "tests", "alembic"):
        paths.extend((PROJECT_DIR / directory).rglob("*.py"))
    paths.extend((PROJECT_DIR / "scripts").glob("*.sh"))
    return {
        str(path.relative_to(PROJECT_DIR)): sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
    }


def write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_command(evidence_dir, name, arguments, environment):
    started = perf_counter()
    result = subprocess.run(
        arguments, cwd=PROJECT_DIR, env=environment, capture_output=True, text=True, check=False,
    )
    (evidence_dir / f"{name}.stdout.txt").write_text(result.stdout, encoding="utf-8")
    (evidence_dir / f"{name}.stderr.txt").write_text(result.stderr, encoding="utf-8")
    record = {
        "command": arguments,
        "exit_code": result.returncode,
        "elapsed_seconds": round(perf_counter() - started, 3),
    }
    write_json(evidence_dir / f"{name}.json", record)
    print(json.dumps({"check": name, **record}, ensure_ascii=False), flush=True)
    result.check_returncode()
    return record


def data_snapshot(settings):
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with Session(engine) as session:
        source = session.get(Source, (SOURCE_ID, REVISION))
        if source is None:
            raise ValueError("固定来源不存在")
        assets = session.scalars(
            select(Asset).where(
                Asset.source_id == SOURCE_ID, Asset.source_revision == REVISION
            ).order_by(Asset.source_order)
        ).all()
        batches = session.scalars(
            select(ImportBatch).where(
                ImportBatch.source_id == SOURCE_ID, ImportBatch.source_revision == REVISION
            ).order_by(ImportBatch.started_at)
        ).all()
        snapshot = {
            "database": session.execute(text("SELECT current_database()")).scalar_one(),
            "postgresql": session.execute(text("SELECT version()")).scalar_one(),
            "source_id": source.source_id,
            "revision": source.revision,
            "license": source.license,
            "license_note": source.license_note,
            "records": [
                {
                    "asset_id": asset.id, "record_id": asset.record_id,
                    "order": asset.source_order, "owner_id": asset.owner_id,
                    "versions": [
                        {"version": row.version, "sha256": row.file_sha256}
                        for row in asset.versions
                    ],
                }
                for asset in assets
            ],
            "batches": [
                {
                    "id": batch.id, "status": batch.status,
                    "expected_count": batch.expected_count,
                    "imported_count": batch.imported_count,
                    "manifest_sha256": sha256(
                        json.dumps(batch.manifest, sort_keys=True).encode("utf-8")
                    ).hexdigest(),
                }
                for batch in batches
            ],
        }
    engine.dispose()
    return snapshot


def main():
    settings = Settings()
    run_id = f"mat-release-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_dir = PROJECT_DIR / ".scratch/evidence" / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    print(f"运行编号：{run_id}；证据目录：{evidence_dir}", flush=True)
    before = source_manifest()
    write_json(evidence_dir / "source-before.json", before)
    write_json(evidence_dir / "inputs.json", {
        "python": sys.version,
        "dependencies": {
            name: version(name)
            for name in ("fastapi", "sqladmin", "sqlalchemy", "psycopg", "datasets", "pillow")
        },
        "source_id": SOURCE_ID,
        "revision": REVISION,
        "data_dir": str(settings.data_dir.resolve()),
        "upload_max_bytes": settings.upload_max_bytes,
        "test_database": "shop_test",
        "entrypoint": "http://127.0.0.1:8000",
    })
    environment = os.environ.copy()
    environment["SHOP_TEST_DATABASE_URL"] = (
        "postgresql+psycopg://shop@127.0.0.1:55432/shop_test"
    )
    environment["HF_HOME"] = str(PROJECT_DIR / ".scratch/huggingface")
    environment["UV_CACHE_DIR"] = str(PROJECT_DIR / ".scratch/uv-cache")
    python = str(PROJECT_DIR / ".venv/bin/python")
    commands = [
        ("dependencies", ["uv", "sync", "--frozen", "--extra", "dev"]),
        ("postgresql", ["bash", "scripts/postgres.sh", "status"]),
        ("shell", ["bash", "-n", "scripts/postgres.sh", "scripts/run_api.sh"]),
        ("migration-current", [python, "-m", "alembic", "current"]),
        ("migration-check", [python, "-m", "alembic", "check"]),
        ("materials", [python, "scripts/manage.py", "verify-materials"]),
        ("video-source", [python, "scripts/verify_material_uploads.py", "--prepare-video"]),
        ("pytest", [
            python, "-m", "pytest", "tests/test_materials.py", "tests/test_auth.py",
            "tests/test_material_delivery.py", f"--basetemp={evidence_dir / 'pytest-data'}",
        ]),
        ("ruff", [python, "-m", "ruff", "check", "src/shop/materials.py", "src/shop/app.py",
                  "src/shop/auth.py", "src/shop/models.py", "src/shop/config.py",
                  "src/shop/fashion.py", "scripts/manage.py", "scripts/import_fashion.py",
                  "scripts/verify_material_http.py", "scripts/verify_material_uploads.py",
                  "scripts/verify_material_release.py", "tests/conftest.py",
                  "tests/test_materials.py", "tests/test_auth.py", "tests/test_material_delivery.py",
                  "alembic/env.py"]),
        ("http", [python, "scripts/verify_material_http.py"]),
        ("uploads-http", [python, "scripts/verify_material_uploads.py", "--http"]),
    ]
    checks = {
        name: run_command(evidence_dir, name, arguments, environment)
        for name, arguments in commands
    }
    snapshot = data_snapshot(settings)
    write_json(evidence_dir / "data-snapshot.json", snapshot)
    after = source_manifest()
    write_json(evidence_dir / "source-after.json", after)
    if before != after:
        raise ValueError("验证期间源码发生变化，请检查受影响证据")
    write_json(evidence_dir / "report.json", {
        "run_id": run_id, "status": "passed", "checks": checks,
        "scope": ["MAT-01", "MAT-02", "MAT-03", "MAT-04", "MAT-05"],
        "asset_count": len(snapshot["records"]),
        "version_count": sum(len(row["versions"]) for row in snapshot["records"]),
        "batch_count": len(snapshot["batches"]),
        "source_manifest_sha256": sha256(
            json.dumps(after, sort_keys=True).encode("utf-8")
        ).hexdigest(),
    })
    print(f"验证通过，报告：{evidence_dir / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
