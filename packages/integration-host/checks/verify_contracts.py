import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
from xml.etree import ElementTree


def source_digests(root):
    paths = [root / "pyproject.toml", root / "uv.lock"]
    for directory in (
        root / "packages" / "integration-protocol",
        root / "packages" / "integration-host",
        root / "src" / "shop",
    ):
        paths.extend(directory.rglob("*.py"))
        paths.extend(directory.rglob("pyproject.toml"))
    paths.extend(root / "tests" / name for name in (
        "test_integration_protocol.py", "test_integration_host.py", "test_integration_contracts.py",
    ))
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(set(paths))
    }


def run(command, root, output):
    with output.open("w", encoding="utf-8") as stream:
        completed = subprocess.run(
            command, cwd=root, stdout=stream, stderr=subprocess.STDOUT, check=False
        )
    return {"command": command, "exit_code": completed.returncode, "output": str(output)}


def main():
    root = Path(__file__).resolve().parents[3]
    run_id = "if01-contracts-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    evidence = root / ".scratch" / "evidence" / run_id
    evidence.mkdir(parents=True)
    database = urlsplit(os.environ["SHOP_TEST_DATABASE_URL"])
    assert database.path.rsplit("/", 1)[-1] == "shop_test"
    before = source_digests(root)
    tests = ["tests/test_integration_protocol.py", "tests/test_integration_host.py",
             "tests/test_integration_contracts.py"]
    pytest_command = [
        sys.executable, "-m", "pytest", "-q", "--basetemp=.scratch/pytest-" + run_id,
        "--junitxml=" + str(evidence / "pytest.xml"), *tests,
    ]
    pytest_result = run(pytest_command, root, evidence / "pytest.txt")
    ruff_command = [
        str(root / ".venv" / "bin" / "ruff"), "check", "packages/integration-protocol",
        "packages/integration-host", *tests,
    ]
    ruff_result = run(ruff_command, root, evidence / "ruff.txt")
    after = source_digests(root)
    suites = ElementTree.parse(evidence / "pytest.xml").getroot().findall("testsuite")
    counts = {
        name: sum(int(suite.attrib[name]) for suite in suites)
        for name in ("tests", "failures", "errors", "skipped")
    }
    report = {
        "run_id": run_id, "milestone": "IF-01", "python": sys.version,
        "database": {"host": database.hostname, "port": database.port, "name": "shop_test",
                     "isolation": "每项真实数据库检查建立独立schema，结束后删除"},
        "fixed_inputs": {"definition": "tests/test_integration_contracts.py:contract_inputs及常量",
                         "fixture_sha256": before["tests/test_integration_contracts.py"]},
        "versions": {name: importlib.metadata.version(name) for name in (
            "integration-protocol", "integration-host", "pydantic", "fastapi", "sqlalchemy",
            "poster-framework",
        )},
        "checks": [pytest_result, ruff_result], "pytest_counts": counts,
        "sources_before": before, "sources_after": after, "sources_unchanged": before == after,
        "cloud_request_evidence": "指定真实调用检查断言ProviderRequest数量为零",
    }
    passed = (
        pytest_result["exit_code"] == 0 and ruff_result["exit_code"] == 0
        and counts["failures"] == counts["errors"] == counts["skipped"] == 0 and before == after
    )
    report["status"] = "passed" if passed else "failed"
    (evidence / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"report": str(evidence / "report.json"), "status": report["status"],
                      "pytest_counts": counts, "sources_unchanged": before == after},
                     ensure_ascii=False, indent=2))
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
