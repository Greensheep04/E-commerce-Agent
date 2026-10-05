import json
import os
import subprocess
from pathlib import Path

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parent
PYTHON = PACKAGE_ROOT / ".venv" / "bin" / "python"


def run_cli(*arguments):
    return subprocess.run(
        [str(PYTHON), "-m", "agent_cluster", *arguments],
        cwd=PACKAGE_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "TMPDIR": str(WORKSPACE_ROOT / ".scratch" / "clu02-cli-tmp")},
    )


def test_module_help_lists_supported_commands():
    result = run_cli("--help")

    assert result.returncode == 0
    assert "goals" in result.stdout
    assert "runs" in result.stdout
    assert "events" in result.stdout
    assert "schedules" in result.stdout


def test_invalid_config_json_fails_without_exposing_password(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{", encoding="utf-8")

    result = run_cli("--config", str(config), "goals", "list")

    assert result.returncode == 2
    assert "JSON" in result.stderr
    assert "password" not in result.stdout.lower()


def test_config_loads_relative_password_file_and_hides_password(tmp_path):
    from agent_cluster.cli import load_client_config

    password_file = tmp_path / "auth" / "password.txt"
    password_file.parent.mkdir()
    password_file.write_text("safe-secret\n", encoding="utf-8")
    password_file.chmod(0o600)
    config_path = tmp_path / "client.json"
    config_path.write_text(json.dumps({
        "base_url": "http://127.0.0.1:8001",
        "username": "operator",
        "password_file": "auth/password.txt",
        "expected_actor_id": "operator-id",
    }), encoding="utf-8")

    config = load_client_config(config_path)

    assert config.base_url == "http://127.0.0.1:8001"
    assert config.username == "operator"
    assert config.password == "safe-secret"
    assert config.expected_actor_id == "operator-id"
    assert "safe-secret" not in repr(config)


def test_config_rejects_password_file_without_exact_permissions(tmp_path):
    from agent_cluster.cli import CLIError, load_client_config

    password_file = tmp_path / "password.txt"
    password_file.write_text("safe-secret", encoding="utf-8")
    password_file.chmod(0o640)
    config_path = tmp_path / "client.json"
    config_path.write_text(json.dumps({
        "base_url": "http://127.0.0.1:8001",
        "username": "operator",
        "password_file": "password.txt",
        "expected_actor_id": "operator-id",
    }), encoding="utf-8")

    with pytest.raises(CLIError, match="0600"):
        load_client_config(config_path)


def test_config_reports_invalid_password_encoding(tmp_path):
    from agent_cluster.cli import CLIError, load_client_config

    password_file = tmp_path / "password.txt"
    password_file.write_bytes(b"\xff")
    password_file.chmod(0o600)
    config_path = tmp_path / "client.json"
    config_path.write_text(json.dumps({
        "base_url": "http://127.0.0.1:8001",
        "username": "operator",
        "password_file": "password.txt",
        "expected_actor_id": "operator-id",
    }), encoding="utf-8")

    with pytest.raises(CLIError, match="密码文件读取失败"):
        load_client_config(config_path)


def test_request_json_reader_requires_object_and_rejects_identity_fields(tmp_path):
    from agent_cluster.cli import CLIError, read_json_object

    request_file = tmp_path / "goal.json"
    request_file.write_text(json.dumps({"objective": "统计订单", "criteria": []},
                                       ensure_ascii=False), encoding="utf-8")

    assert read_json_object(request_file) == {"objective": "统计订单", "criteria": []}
    request_file.write_text("[]", encoding="utf-8")
    with pytest.raises(CLIError, match="JSON 对象"):
        read_json_object(request_file)
    request_file.write_text('{"actor_id":"forged"}', encoding="utf-8")
    with pytest.raises(CLIError, match="actor_id"):
        read_json_object(request_file, forbidden_fields={"actor_id", "goal_id"})
    request_file.write_bytes(b"\xff")
    with pytest.raises(CLIError, match="请求 JSON 文件读取失败"):
        read_json_object(request_file)


def test_parser_reads_goal_revision_and_schedule_trigger_arguments():
    from agent_cluster.cli import build_parser

    parser = build_parser()
    update = parser.parse_args([
        "--config", "client.json", "goals", "update", "goal-7",
        "--file", "goal.json", "--expected-revision", "3",
    ])
    schedule = parser.parse_args([
        "--config", "client.json", "schedules", "create", "--goal-id", "goal-7",
        "--cron", "0 9 * * *", "--timezone", "Asia/Shanghai", "--disabled",
    ])

    assert (update.command, update.action, update.goal_id, update.expected_revision) == (
        "goals", "update", "goal-7", 3)
    assert (schedule.command, schedule.action, schedule.goal_id, schedule.cron,
            schedule.timezone, schedule.disabled) == (
        "schedules", "create", "goal-7", "0 9 * * *", "Asia/Shanghai", True)
