import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


@pytest.mark.parametrize("previous", [
    "base", "0008_customer_tool_attempts", "0005_assessment", "0016_data_center_source_version",
])
def test_if03_merge_migration_preserves_each_existing_branch(previous, data_dir):
    database_url = os.environ["SHOP_TEST_DATABASE_URL"]
    assert make_url(database_url).database != "shop"
    schema = "migration_" + uuid4().hex
    admin = create_engine(database_url)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    url = make_url(database_url).set(query={"options": f"-csearch_path={schema},public"})
    env = {**os.environ, "SHOP_DATABASE_URL": url.render_as_string(hide_password=False)}
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        commands = [] if previous == "base" else [["upgrade", previous]]
        commands.extend([["upgrade", "head"], ["check"]])
        for index, args in enumerate(commands):
            result = subprocess.run([str(Path(".venv/bin/alembic").resolve()), *args], env=env,
                                    capture_output=True, text=True, timeout=60, check=False)
            (data_dir / f"migration-{index}.txt").write_text(result.stdout + result.stderr, encoding="utf-8")
            assert result.returncode == 0, result.stdout + result.stderr
        engine = create_engine(url)
        try:
            with engine.connect() as connection:
                assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0017_data_center_model_reuse"
                assert connection.scalar(text("SELECT count(*) FROM integration_identity_maps")) == 0
                assert connection.scalar(text("SELECT count(*) FROM integration_calls")) == 0
                assert connection.scalar(text("SELECT count(*) FROM assessment_runs")) == 0
                for table in ("cluster_goals", "cluster_runs", "cluster_events", "cluster_tasks",
                              "cluster_messages", "cluster_schedules", "cluster_decisions",
                              "data_center_task_bindings", "data_center_model_reuses"):
                    assert connection.scalar(text(f"SELECT count(*) FROM {table}")) == 0
        finally:
            engine.dispose()
    finally:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()
