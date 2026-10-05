from hashlib import sha256
from pathlib import Path

from scripts.verify_data_center_release import DC06_SOURCE_PATHS
from scripts.verify_data_center_reliability import source_hashes

ROOT = Path(__file__).resolve().parents[1]


def assert_recorded(paths):
    recorded = source_hashes()
    for relative in paths:
        assert relative in recorded, f"可靠性记录缺少运行依赖：{relative}"
        assert recorded[relative] == sha256((ROOT / relative).read_bytes()).hexdigest()


def test_note_recovery_records_note_store_and_public_types():
    assert_recorded([
        "agent-cluster/src/agent_cluster/notes/store.py",
        "agent-cluster/src/agent_cluster/types.py",
        "agent-cluster/src/agent_cluster/failures.py",
    ])


def test_real_http_sync_records_configuration_authorization_and_service_components():
    assert_recorded([
        "src/shop/app.py",
        "src/shop/auth.py",
        "src/shop/config.py",
        "src/shop/paid_models.py",
        "data-center/src/data_center/config.py",
        "data-center/src/data_center/database.py",
        "data-center/src/data_center/ports.py",
        "data-center/src/data_center/observability.py",
        "packages/integration-host/src/integration_host/http.py",
        "packages/integration-protocol/src/integration_protocol/__init__.py",
    ])


def test_sync_and_metrics_record_actual_sql_and_yaml_resources():
    resources = sorted(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "data-center/src/data_center/analytics").rglob("*")
        if path.is_file() and path.suffix in {".sql", ".yaml", ".yml"}
    )
    assert len(resources) == 10
    assert_recorded(resources)


def test_dependency_identity_records_all_runtime_manifests_and_locks():
    assert_recorded([
        "pyproject.toml",
        "uv.lock",
        "data-center/pyproject.toml",
        "data-center/uv.lock",
        "agent-cluster/pyproject.toml",
        "agent-cluster/uv.lock",
        "packages/data-center-protocol/pyproject.toml",
        "packages/integration-protocol/pyproject.toml",
        "packages/integration-host/pyproject.toml",
    ])


def test_producer_and_release_auditor_require_same_recorded_files():
    assert set(source_hashes()) == set(DC06_SOURCE_PATHS)
