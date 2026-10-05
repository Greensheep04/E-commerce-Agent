import argparse
import json
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

from agent_cluster.types import GoalDefinition, GoalRun

from scripts.cluster_quality import verify_result
from scripts.verify_cluster_live import execution_manifest, verify_configuration
from scripts.verify_integration_live import save, snapshot
from shop.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def digest_file(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def reusable_test_evidence(previous, name, current_sources, current_versions):
    report_path = previous / "report.json"
    report = read(report_path)
    changes = {path for path in report["source_sha256"].keys() | current_sources.keys()
               if report["source_sha256"].get(path) != current_sources.get(path)}
    assert changes <= {"scripts/verify_cluster_release.py", "scripts/check_cluster_install.py"}, (
        "复用检查的业务、公共包或测试源码发生变化")
    assert report["versions"] == current_versions, "复用检查的依赖版本发生变化"
    assert len(report["runtime_directory"].encode()) <= 70, "复用检查的IPC目录条件不适用"
    matches = [entry for entry in report["commands"] if entry["name"] == name]
    assert len(matches) == 1 and matches[0]["exit_code"] == 0, "复用检查没有独立通过记录"
    entry = dict(matches[0])
    python, target = {
        "root-tests": (ROOT / ".venv/bin/python", "tests"),
        "cluster-tests": (ROOT / ".venv/bin/python", "agent-cluster/tests"),
        "hub-stdio": (ROOT / "agent-hub/.venv/bin/python", "agent-hub/tests/stdio"),
    }[name]
    expected_arguments = [str(python), "-m", "pytest", target, "-q", "--basetemp",
                          str(previous.resolve() / (name + "-runtime")), "--junitxml",
                          str(previous.resolve() / (name + ".xml"))]
    assert entry["arguments"] == expected_arguments, "复用检查的pytest命令或测试目标不一致"
    assert entry["output_sha256"] == digest_file(entry["output"]), "复用检查的原始输出摘要不一致"
    xml_path = previous / (name + ".xml")
    xml = ET.parse(xml_path)
    assert not any(xml.findall(".//" + state) for state in ("failure", "error", "skipped"))
    assert type(report[name + "-count"]) is int and report[name + "-count"] > 0, "复用检查没有测试用例"
    assert len(xml.findall(".//testcase")) == report[name + "-count"]
    entry.update({"reused_from": str(previous), "original_report_sha256": digest_file(report_path),
                  "xml_sha256": digest_file(xml_path), "validation_source_changes": sorted(changes)})
    return entry, xml_path


def sources():
    result = execution_manifest()
    for name in ("agent-cluster", "tests", "scripts"):
        for path in (ROOT / name).rglob("*"):
            if path.is_file() and path.suffix in {".py", ".toml", ".json", ".md"} and not any(
                part in {".venv", "__pycache__", ".pytest_cache", ".ruff_cache"} for part in path.parts
            ):
                result[str(path.relative_to(ROOT))] = digest_file(path)
    return dict(sorted(result.items()))


def verify_formal(args, directory, settings):
    verify_configuration(args.cluster_config, args.formal_directory / "configuration.json")
    formal = read(args.formal_directory / "report.json")
    definitions = read(args.formal_directory / "definitions.json")
    inputs = read(args.inputs)
    assert formal["kind"] == "formal" and formal["executed_runs"] == 60
    assert formal["passed_runs"] == sum(item["quality"]["passed"] for item in formal["records"])
    assert formal["failed_runs"] == 60 - formal["passed_runs"]
    original_sources = definitions["execution_source_sha256"]
    current_sources = execution_manifest()
    source_changes = {name: {"before": original_sources.get(name), "after": current_sources.get(name)}
                      for name in sorted(original_sources.keys() | current_sources.keys())
                      if original_sources.get(name) != current_sources.get(name)}
    quality_directory = getattr(args, "formal_quality_directory", None)
    permitted_changes = {"scripts/cluster_quality.py"} if quality_directory else set()
    assert set(source_changes) <= permitted_changes, "正式业务源码与当前版本不一致"
    assert definitions["fixture_sha256"] == inputs["fixture_sha256"]
    assert definitions["source_sha256"] == inputs["source_sha256"]
    assert read(args.formal_directory / "source.json") == original_sources
    quality_records = None
    quality_summary = None
    if quality_directory:
        quality_summary = read(quality_directory / "report.json")
        assert quality_summary["kind"] == "quality_recheck" and quality_summary["executed_runs"] == 60
        assert quality_summary["original_report_sha256"] == digest_file(
            args.formal_directory / "report.json"), "正式原报告摘要不一致"
        assert quality_summary["evaluator_sha256"] == digest_file(
            ROOT / "scripts/cluster_quality.py"), "独立评分源码摘要不一致"
        assert quality_summary["quality_report_sha256"] == digest_file(quality_directory / "quality.json")
        quality_report = read(quality_directory / "quality.json")
        assert quality_report["status"] == "complete" and quality_report["unevaluated_repetitions"] == 0
        assert quality_report["evaluator_sha256"] == quality_summary["evaluator_sha256"]
        assert quality_report["test_sha256"] == digest_file(
            ROOT / "agent-cluster/tests/quality/test_cluster_quality.py")
        assert quality_report["inputs_sha256"] == digest_file(args.inputs)
        assert quality_report["fixture_sha256"] == inputs["fixture_sha256"] == digest_file(
            ROOT / "agent-cluster/tests/fixtures/goals_v1.json")
        quality_records = {item["run_id"]: item for item in quality_report["evaluated_records"]}
        assert len(quality_records) == len(quality_report["evaluated_records"]) == 60
        assert quality_summary["original_passed_runs"] == formal["passed_runs"]
    else:
        assert formal["passed"] and formal["passed_runs"] >= 57 and not formal["safety_issues"]
    cases = {case["id"]: case for case in definitions["cases"]}
    identifiers, goals, runs, rechecked = set(), set(), set(), []
    baseline = read(args.formal_directory / "baseline.json")
    for record in formal["records"]:
        identity = (record["case_id"], record["repetition"])
        assert identity not in identifiers and record["goal_id"] not in goals and record["run_id"] not in runs
        identifiers.add(identity)
        goals.add(record["goal_id"])
        runs.add(record["run_id"])
        item = args.formal_directory / f"{identity[0]}-{identity[1]}"
        goal = GoalDefinition.model_validate_json((item / "goal.json").read_text())
        run = GoalRun.model_validate_json((item / "run.json").read_text())
        assert run.goal_id == goal.goal_id == record["goal_id"] and run.run_id == record["run_id"]
        assert run.status == record["actual_status"]
        quality = verify_result(cases[identity[0]], run, read(item / "tasks.json"), settings, inputs, baseline)
        if quality_records is not None:
            prior = quality_records[run.run_id]
            assert prior["case_id"] == identity[0]
            assert Path(prior["artifact_directory"]).resolve() == item.resolve()
            assert prior["run_sha256"] == digest_file(item / "run.json")
            assert prior["tasks_sha256"] == digest_file(item / "tasks.json")
            assert prior["source_manifest_sha256"] == digest_file(args.formal_directory / "source.json")
            assert prior["legacy_quality_passed"] == record["quality"]["passed"]
            assert quality == prior["quality"], f"独立核验结果与记录不一致：{identity[0]}-{identity[1]}"
        else:
            assert quality == record["quality"], f"独立核验结果与原报告不一致：{identity[0]}-{identity[1]}"
        rechecked.append({"case_id": identity[0], "repetition": identity[1], "quality": quality,
                          "original_quality": record["quality"]})
    assert identifiers == {(name, repetition) for name in cases for repetition in (1, 2, 3)}
    passed = sum(item["quality"]["passed"] for item in rechecked)
    safety = [issue for item in rechecked for issue in item["quality"]["safety_issues"]]
    assert passed >= 57 and not safety
    if quality_summary is not None:
        assert quality_summary["passed_runs"] == quality_report["passed_records"] == passed
        assert quality_summary["failed_runs"] == quality_report["failed_records"] == 60 - passed
        assert quality_summary["safety_issues"] == safety
    save(directory / "formal-independent-recheck.json", rechecked)
    return {"directory": str(args.formal_directory), "report_sha256": digest_file(args.formal_directory / "report.json"),
            "executed": 60, "passed": passed, "failed": 60 - passed,
            "original_passed": formal["passed_runs"], "original_failed": formal["failed_runs"],
            "source_changes": source_changes,
            "quality_recheck": {"directory": str(quality_directory),
                                "report_sha256": digest_file(quality_directory / "report.json")}
            if quality_directory else None,
            "safety_issues": safety, "goal_count": len(goals), "run_count": len(runs)}


def usage_audit(args, directory, settings):
    current = snapshot(settings)
    baseline = read(args.inputs.parent / "baseline.json")
    by_id = {row["id"]: row for row in current["requests"]}
    for original in baseline["requests"]:
        assert original["id"] in by_id
        if original["status"] == "unknown":
            assert by_id[original["id"]] == original
    spent = sum((Decimal(row["charged_cny"]) for row in current["requests"]
                 if row["charged_cny"] is not None), Decimal(0))
    reserved = sum((Decimal(row["reservation_cny"]) for row in current["requests"]
                    if row["charged_cny"] is None), Decimal(0))
    assert spent == Decimal(current["spent_cny"]) and reserved == Decimal(current["reserved_cny"])
    window = [row for row in current["requests"] if row["integration_run_id"] == "clu-live-20261003"]
    window_total = sum((Decimal(row["charged_cny"] if row["charged_cny"] is not None
                               else row["reservation_cny"]) for row in window), Decimal(0))
    assert window_total <= 30 and spent + reserved <= 300
    assert not any(row["status"] == "sent" for row in current["requests"])
    save(directory / "final-usage.json", current)
    return {"spent_cny": str(spent), "reserved_cny": str(reserved), "window_total_cny": str(window_total),
            "window_requests": len(window), "historical_unknown_preserved": True}


def main():
    parser = argparse.ArgumentParser(description="Agent集群固定验收及最终源码综合验证")
    parser.add_argument("--inputs", required=True, type=Path)
    parser.add_argument("--cluster-config", required=True, type=Path)
    parser.add_argument("--hub-inputs", required=True, type=Path)
    parser.add_argument("--formal-directory", required=True, type=Path)
    parser.add_argument("--formal-quality-directory", type=Path)
    parser.add_argument("--reuse-tests-from", type=Path)
    parser.add_argument("--evidence-index", required=True, type=Path)
    parser.add_argument("--directory", required=True, type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    settings = Settings()
    hub_inputs = read(args.hub_inputs)
    run_id = directory.name
    environment = {**os.environ, "TMPDIR": str(ROOT / ".scratch" / ("c-" + uuid4().hex[:8])),
                   "HF_HOME": str(ROOT / ".scratch/huggingface"),
                   "UV_CACHE_DIR": str(ROOT / ".scratch/uv-cache"),
                   "SHOP_TEST_DATABASE_URL": "postgresql+psycopg://shop@127.0.0.1:55432/shop_test",
                   "PYTHONPATH": os.pathsep.join((str(ROOT), str(ROOT / "src"),
                                                  str(ROOT / "agent-cluster/tests/reliability"))),
                   "CLU05_TEST_SCHEMA": "clu05_" + uuid4().hex, "CLU05_EVIDENCE_DIR": str(directory),
                   "CLU05_CLUSTER_CONFIG": str(args.cluster_config.resolve()), "CLU05_HUB_BASE_URL": "http://127.0.0.1:8001",
                   "AGENT_HUB_TEST_INPUTS": str(args.hub_inputs.resolve()),
                   "AGENT_HUB_TEST_CONFIG": hub_inputs["hub_configs"]["admin"]}
    Path(environment["TMPDIR"]).mkdir()
    report = {"run_id": run_id, "milestone": "CLU-06", "status": "running", "commands": [],
              "started_at": datetime.now(UTC).isoformat(), "source_sha256": sources(),
              "runtime_directory": environment["TMPDIR"],
              "versions": {name: version(name) for name in (
                  "agent-cluster", "deepagents", "langgraph", "mcp", "APScheduler", "SQLAlchemy", "psycopg", "tokenizers")}}
    before = snapshot(settings)
    save(directory / "before.json", before)

    def command(name, arguments, *, cwd=ROOT, env=None):
        if args.reuse_tests_from and name in {"root-tests", "cluster-tests", "hub-stdio"}:
            entry, xml_path = reusable_test_evidence(
                args.reuse_tests_from, name, report["source_sha256"], report["versions"])
            shutil.copyfile(xml_path, directory / (name + ".xml"))
            report["commands"].append(entry)
            save(directory / "report.json", report)
            print(f"{name}: reused exit=0", flush=True)
            return
        output = directory / f"{name}.txt"
        started = datetime.now(UTC)
        with output.open("w") as stream:
            process = subprocess.run([str(arg) for arg in arguments], cwd=cwd, env=env or environment,
                                     stdout=stream, stderr=subprocess.STDOUT, check=False)
        report["commands"].append({"name": name, "arguments": [str(arg) for arg in arguments],
                                   "exit_code": process.returncode, "output": str(output),
                                   "output_sha256": digest_file(output),
                                   "duration_seconds": (datetime.now(UTC) - started).total_seconds()})
        save(directory / "report.json", report)
        print(f"{name}: exit={process.returncode}", flush=True)
        assert process.returncode == 0, f"检查失败：{name}，见{output}"

    try:
        report["formal"] = verify_formal(args, directory, settings)
        evidence = read(args.evidence_index)
        assert {entry["milestone"] for entry in evidence} >= {"CLU-02", "CLU-03", "CLU-04", "CLU-05"}
        for entry in evidence:
            data = read(entry["path"])
            assert data.get("status") == "passed" or data.get("passed") is True
            entry["sha256"] = digest_file(entry["path"])
        report["reused_evidence"] = evidence
        for name, target in (("root-tests", "tests"), ("cluster-tests", "agent-cluster/tests")):
            command(name, [sys.executable, "-m", "pytest", target, "-q",
                           "--basetemp", directory / (name + "-runtime"),
                           "--junitxml", directory / (name + ".xml")])
            xml = ET.parse(directory / (name + ".xml"))
            assert not any(xml.findall(".//" + state) for state in ("failure", "error", "skipped"))
            report[name + "-count"] = len(xml.findall(".//testcase"))
        command("root-ruff", [ROOT / ".venv/bin/ruff", "check", "src", "scripts", "tests", "alembic", "packages"])
        command("cluster-ruff", [ROOT / ".venv/bin/ruff", "check", "agent-cluster/src", "agent-cluster/tests"])
        command("hub-stdio", [ROOT / "agent-hub/.venv/bin/python", "-m", "pytest", "agent-hub/tests/stdio", "-q",
                              "--basetemp", directory / "hub-stdio-runtime",
                              "--junitxml", directory / "hub-stdio.xml"])
        hub_xml = ET.parse(directory / "hub-stdio.xml")
        assert not any(hub_xml.findall(".//" + state) for state in ("failure", "error", "skipped"))
        report["hub-stdio-count"] = len(hub_xml.findall(".//testcase"))
        command("hub-ruff", [ROOT / ".venv/bin/ruff", "check", "agent-hub/src/agent_hub/mcp/server.py"])
        command("alembic", [ROOT / ".venv/bin/alembic", "check"])
        command("cluster-http", [sys.executable, "-m", "scripts.verify_cluster_http", "--inputs", args.inputs,
                                 "--cluster-config", args.cluster_config, "--output-dir",
                                 directory / ("clu02-http-" + run_id)])
        wheels = directory / "wheels"
        for package in ("packages/integration-protocol", "agent-cluster"):
            command(Path(package).name + "-wheel", ["uv", "build", "--wheel", ROOT / package, "--out-dir", wheels])
        independent = directory / "independent-env"
        command("independent-environment", ["uv", "venv", independent, "--python", "3.11"])
        command("independent-install", ["uv", "pip", "install", "--python", independent / "bin/python",
                                        *sorted(wheels.glob("*.whl"))])
        clean = {key: value for key, value in environment.items() if key != "PYTHONPATH"}
        command("independent-help", [independent / "bin/agent-cluster", "--help"], env=clean)
        command("independent-real-checks", [independent / "bin/python", ROOT / "scripts/check_cluster_install.py",
                                           "--inputs", args.inputs.resolve(), "--cluster-config", args.cluster_config.resolve(),
                                           "--output", directory / "independent-checks.json"], env=clean)
        assert before == snapshot(settings), "最终程序验证改变了主库模型请求、工单或费用"
        report["usage"] = usage_audit(args, directory, settings)
        report["source_after_sha256"] = sources()
        assert report["source_sha256"] == report["source_after_sha256"]
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        report["completed_at"] = datetime.now(UTC).isoformat()
        save(directory / "report.json", report)
        print(directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
