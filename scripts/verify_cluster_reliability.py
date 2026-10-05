import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
INPUTS = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50"
SOURCE_FILES = [
    ROOT / "agent-cluster/src/agent_cluster/store.py",
    ROOT / "agent-cluster/src/agent_cluster/types.py",
    ROOT / "agent-cluster/src/agent_cluster/engine.py",
    ROOT / "agent-cluster/src/agent_cluster/mcp_client.py",
    ROOT / "agent-cluster/src/agent_cluster/notes/store.py",
    ROOT / "agent-cluster/tests/reliability/conftest.py",
    ROOT / "agent-cluster/tests/reliability/clu05_workers.py",
    ROOT / "agent-cluster/tests/reliability/test_store_processes.py",
    ROOT / "agent-cluster/tests/reliability/test_live_reliability.py",
    ROOT / "src/shop/app.py",
    ROOT / "src/shop/budget.py",
    ROOT / "src/shop/cluster.py",
    ROOT / "src/shop/integration_adapters.py",
    ROOT / "agent-hub/src/agent_hub/config.py",
    ROOT / "agent-hub/src/agent_hub/connectors/http.py",
    ROOT / "agent-hub/src/agent_hub/mcp/server.py",
    ROOT / "scripts/verify_cluster_reliability.py",
]
CLUSTER_CONFIG = ROOT / ".scratch/cluster-live-config.json"
HUB_BASE_URL = "http://127.0.0.1:8001"


def file_sha256(path):
    return sha256(path.read_bytes()).hexdigest()


def source_hashes():
    return {path.relative_to(ROOT).as_posix(): file_sha256(path) for path in SOURCE_FILES}


def main():
    run_id = "clu05-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    schema = "clu05_" + uuid4().hex
    evidence_dir = ROOT / ".scratch/evidence" / run_id
    runtime_dir = ROOT / ".scratch/runtime"
    pytest_parent = ROOT / ".scratch/pytest"
    pytest_base = pytest_parent / run_id
    python = ROOT / ".venv/bin/python"
    cluster_config = CLUSTER_CONFIG.resolve(strict=True)
    evidence_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    pytest_parent.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["TMPDIR"] = str(runtime_dir)
    environment["TMP"] = str(runtime_dir)
    environment["TEMP"] = str(runtime_dir)
    environment["CLU05_TEST_SCHEMA"] = schema
    environment["CLU05_EVIDENCE_DIR"] = str(evidence_dir)
    environment["CLU05_CLUSTER_CONFIG"] = str(cluster_config)
    environment["CLU05_HUB_BASE_URL"] = HUB_BASE_URL
    source_path = os.pathsep.join((
        str(ROOT / "src"),
        str(ROOT / "agent-cluster/src"),
        str(ROOT / "agent-cluster/tests/reliability"),
    ))
    environment["PYTHONPATH"] = os.pathsep.join(
        item for item in (source_path, environment.get("PYTHONPATH", "")) if item
    )
    command = [
        str(python),
        "-m",
        "pytest",
        "-q",
        "--import-mode=importlib",
        "--basetemp",
        str(pytest_base),
        str(ROOT / "agent-cluster/tests/reliability"),
    ]
    before = source_hashes()
    started_at = datetime.now(UTC)
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    completed_at = datetime.now(UTC)
    after = source_hashes()
    exit_code = result.returncode if result.returncode else int(before != after)
    (evidence_dir / "pytest.stdout.txt").write_text(result.stdout, encoding="utf-8")
    (evidence_dir / "pytest.stderr.txt").write_text(result.stderr, encoding="utf-8")
    configuration_path = evidence_dir / "configuration-and-health.json"
    report = {
        "run_id": run_id,
        "milestone": "CLU-05",
        "status": "passed" if exit_code == 0 else "failed",
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": (completed_at - started_at).total_seconds(),
        "database": {"host": "127.0.0.1", "port": 55432, "database": "shop_test", "schema": schema},
        "fixed_inputs": {
            "directory": str(INPUTS.relative_to(ROOT)),
            "inputs_sha256": file_sha256(INPUTS / "inputs.json"),
            "sources_sha256": file_sha256(INPUTS / "sources.json"),
            "baseline_sha256": file_sha256(INPUTS / "baseline.json"),
        },
        "environment": {
            "python": sys.version,
            "pytest": version("pytest"),
            "sqlalchemy": version("SQLAlchemy"),
            "psycopg": version("psycopg"),
            "apscheduler": version("APScheduler"),
            "pydantic": version("pydantic"),
            "pydantic_settings": version("pydantic-settings"),
            "pgvector": version("pgvector"),
            "tmpdir": str(runtime_dir.relative_to(ROOT)),
            "pytest_base": str(pytest_base.relative_to(ROOT)),
            "python_executable": str(python.relative_to(ROOT)),
        },
        "command": [
            ".venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "--import-mode=importlib",
            "--basetemp",
            str(pytest_base.relative_to(ROOT)),
            "agent-cluster/tests/reliability",
        ],
        "live_service": {
            "hub_base_url": HUB_BASE_URL,
            "health_path": "/health",
            "cluster_config": {
                "name": cluster_config.name,
                "sha256": file_sha256(cluster_config),
            },
        },
        "exit_code": exit_code,
        "source_sha256_before": before,
        "source_sha256_after": after,
        "source_unchanged": before == after,
        "outputs": {
            "stdout": str((evidence_dir / "pytest.stdout.txt").relative_to(ROOT)),
            "stderr": str((evidence_dir / "pytest.stderr.txt").relative_to(ROOT)),
            "provider_response_source": str((evidence_dir / "provider_response_source.json").relative_to(ROOT)),
            "configuration_and_health": str(configuration_path.relative_to(ROOT)),
        },
        "paid_model_calls": 0,
    }
    (evidence_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps({
        "run_id": run_id,
        "status": report["status"],
        "exit_code": exit_code,
        "schema": schema,
        "report": str((evidence_dir / "report.json").relative_to(ROOT)),
    }, ensure_ascii=False))
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
