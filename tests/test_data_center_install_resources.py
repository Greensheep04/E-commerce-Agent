import hashlib
import os
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

from scripts.check_data_center_install import source_manifest
from scripts.run_data_center_acceptance import source_digests
from scripts.verify_data_center_release import (
    current_source_manifest,
    quality_source_digests,
    verify_data_center_wheel,
)

ROOT = Path(__file__).resolve().parents[1]
RESOURCE_PATHS = (
    "data_center/analytics/dbt/dbt_project.yml",
    "data_center/analytics/dbt/models/sales_orders.sql",
    "data_center/analytics/dbt/models/sales_quotas.sql",
    "data_center/analytics/dbt/models/schema.yml",
    "data_center/analytics/metricflow/metrics.yaml",
    "data_center/analytics/metricflow/semantic_models.yaml",
    "data_center/analytics/metricflow/project_configuration.yaml",
)


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory):
    directory = tmp_path_factory.mktemp("data_center_wheel")
    uv = shutil.which("uv")
    if uv is None:
        raise FileNotFoundError("wheel检查需要uv")
    environment = {**os.environ, "UV_CACHE_DIR": str(ROOT / ".scratch/uv-cache")}
    subprocess.run(
        [uv, "build", "--offline", "--wheel", str(ROOT / "data-center"),
         "--out-dir", str(directory)],
        cwd=ROOT, env=environment, check=True, capture_output=True, text=True,
    )
    return directory / "data_center-0.1.0-py3-none-any.whl"


@pytest.mark.parametrize("collector", [source_manifest, lambda: current_source_manifest()[0]])
def test_install_source_identity_includes_runtime_analytics_resources(collector):
    files = collector()["data-center"]["files"]
    for resource in RESOURCE_PATHS:
        path = "data-center/src/" + resource
        assert path in files
        assert files[path] == hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


@pytest.mark.parametrize("collector", [source_digests, quality_source_digests])
def test_quality_source_identity_includes_runtime_analytics_resources(collector):
    files = collector()
    for resource in RESOURCE_PATHS:
        path = "data-center/src/" + resource
        assert path in files
        assert files[path] == hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


@pytest.mark.parametrize("resource", [RESOURCE_PATHS[1], RESOURCE_PATHS[4]])
@pytest.mark.parametrize("change", ["missing", "modified"])
def test_wheel_validation_rejects_changed_runtime_analytics_resources(
    tmp_path, built_wheel, resource, change,
):
    manifest, _ = current_source_manifest()
    verify_data_center_wheel(built_wheel, manifest)
    changed_wheel = tmp_path / "changed.whl"
    with zipfile.ZipFile(built_wheel) as source, zipfile.ZipFile(changed_wheel, "w") as target:
        assert resource in source.namelist()
        for entry in source.infolist():
            if entry.filename == resource and change == "missing":
                continue
            content = source.read(entry.filename)
            if entry.filename == resource:
                content += b"\n"
            target.writestr(entry, content)
    with pytest.raises(ValueError, match="源码|配置"):
        verify_data_center_wheel(changed_wheel, manifest)
