import argparse
import json
import os
import subprocess
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def sources():
    paths = [ROOT / "agent-hub/pyproject.toml", ROOT / "agent-hub/uv.lock",
             ROOT / "src/shop/integration_api.py"]
    for name in ("agent-hub/src", "agent-hub/tests", "src/shop", "packages/integration-protocol/src"):
        paths.extend(path for path in (ROOT / name).rglob("*.py") if "__pycache__" not in path.parts)
    paths.extend(ROOT / "scripts" / name for name in (
        "prepare_mcp_inputs.py", "mcp_runtime_evidence.py", "verify_mcp_live.py", "verify_mcp_release.py"))
    return {str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest() for path in sorted(set(paths))}


def main():
    parser = argparse.ArgumentParser(description="验证独立MCP项目、真实stdio及业务证据")
    parser.add_argument("--inputs", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--live-report", required=True, type=Path)
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    inputs_path = args.inputs.resolve(strict=True)
    inputs = json.loads(inputs_path.read_text(encoding="utf-8"))
    run_id = "mcp-release-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    directory = args.directory.resolve() if args.directory else ROOT / ".scratch/evidence" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    env = {**os.environ, "UV_CACHE_DIR": str(ROOT / ".scratch/uv-cache"),
           "TMPDIR": str(ROOT / ".scratch/runtime"), "PYTHONPATH": f"{ROOT}:{ROOT / 'src'}",
           "AGENT_HUB_TEST_INPUTS": str(inputs_path), "AGENT_HUB_TEST_CONFIG": inputs["hub_configs"]["admin"],
           "AGENT_HUB_SOURCE_ROOT": str(args.source_root.resolve())}
    hub_python = ROOT / "agent-hub/.venv/bin/python"
    report = {"run_id": run_id, "milestone": "MCP-06", "status": "running", "commands": [],
              "source_sha256": sources(), "inputs_sha256": sha256(inputs_path.read_bytes()).hexdigest(),
              "config_sha256": {name: sha256(Path(path).read_bytes()).hexdigest()
                                for name, path in inputs["hub_configs"].items()}}

    def command(name, arguments, cwd=ROOT, command_env=None):
        stdout = directory / f"{name}.stdout.txt"
        stderr = directory / f"{name}.stderr.txt"
        with stdout.open("w", encoding="utf-8") as out, stderr.open("w", encoding="utf-8") as err:
            result = subprocess.run([str(argument) for argument in arguments], cwd=cwd,
                                    env=command_env or env, stdout=out, stderr=err, check=False)
        record = {"name": name, "arguments": [str(argument) for argument in arguments],
                  "cwd": str(cwd), "exit_code": result.returncode,
                  "stdout": str(stdout), "stderr": str(stderr),
                  "stdout_sha256": sha256(stdout.read_bytes()).hexdigest(),
                  "stderr_sha256": sha256(stderr.read_bytes()).hexdigest()}
        report["commands"].append(record)
        save(directory / "report.json", report)
        assert result.returncode == 0, f"{name}检查失败，见{stdout}与{stderr}"

    try:
        command("hub-tests", [hub_python, "-m", "pytest", ROOT / "agent-hub/tests", "-q",
                              "--basetemp", directory / "pytest-runtime"])
        command("hub-ruff", [ROOT / "agent-hub/.venv/bin/ruff", "check", "--config",
                             ROOT / "agent-hub/pyproject.toml", ROOT / "agent-hub/src", ROOT / "agent-hub/tests"])
        command("integration-ruff", [ROOT / "agent-hub/.venv/bin/ruff", "check",
            ROOT / "src/shop/integration_api.py", *(ROOT / "scripts" / name for name in (
                "prepare_mcp_inputs.py", "mcp_runtime_evidence.py", "verify_mcp_live.py", "verify_mcp_release.py"))])
        wheels = directory / "wheels"
        command("protocol-wheel", ["uv", "build", "--wheel", ROOT / "packages/integration-protocol", "--out-dir", wheels])
        command("hub-wheel", ["uv", "build", "--wheel", ROOT / "agent-hub", "--out-dir", wheels])
        independent = directory / "independent-env"
        command("independent-environment", ["uv", "venv", independent, "--python", "3.11"])
        command("independent-install", ["uv", "pip", "install", "--python", independent / "bin/python",
                                         *sorted(wheels.glob("*.whl"))])
        clean_env = {key: value for key, value in env.items() if key != "PYTHONPATH"}
        command("independent-import", [independent / "bin/python", "-c",
            ("import importlib.util; from agent_hub.cli import build_registry; from agent_hub.config import load_config; "
            "import sys; assert importlib.util.find_spec('shop') is None; assert 'sqlalchemy' not in sys.modules; "
            "from importlib.metadata import version; print({name: version(name) for name in "
            "('agent-hub','integration-protocol','fastmcp','mcp','httpx','jsonschema')})")], command_env=clean_env)
        command("independent-help", [independent / "bin/agent-hub", "--help"], command_env=clean_env)
        command("independent-stdio-live-reuse", [independent / "bin/python", ROOT / "scripts/verify_mcp_live.py",
            "--inputs", inputs_path, "--directory", directory / "independent-live",
            "--source-root", args.source_root.resolve()], command_env=clean_env)
        prior = json.loads(args.live_report.read_text(encoding="utf-8"))
        current = json.loads((directory / "independent-live/report.json").read_text(encoding="utf-8"))
        assert prior["status"] == current["status"] == "passed"
        assert prior["inputs_sha256"] == current["inputs_sha256"] == report["inputs_sha256"]
        assert prior["window_requests"] == current["window_requests"]
        report["real_business"] = {"prior": str(args.live_report),
            "prior_sha256": sha256(args.live_report.read_bytes()).hexdigest(),
            "current": str(directory / "independent-live/report.json"),
            "request_increment_during_reuse": 0,
            "window_spent_and_reserved_cny": current["window_spent_and_reserved_cny"]}
        report["source_after_sha256"] = sources()
        assert report["source_after_sha256"] == report["source_sha256"]
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save(directory / "report.json", report)
        print(directory / "report.json")


if __name__ == "__main__":
    main()
