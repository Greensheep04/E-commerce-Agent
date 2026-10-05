import argparse
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from scripts.inspect_integration_runtime import inspect_database
from scripts.verify_integration_history import verify as verify_history
from scripts.verify_integration_live import ROOT, save, snapshot, source_manifest
from shop.config import Settings
from shop.integration_models import IntegrationBudgetWindow

TEST_FILES = (
    "test_integration_protocol", "test_integration_host", "test_integration_contracts",
    "test_integration_adapters", "test_integration_budget", "test_integration_resources",
    "test_integration_permissions", "test_integration_reliability", "test_integration_worker_shutdown",
    "test_integration_worker_waiter",
    "test_integration_history", "test_integration_migrations",
    "test_auth", "test_customer_orders_api", "test_customer_runtime",
    "test_campaign_runtime", "test_campaign_cancel_race", "test_campaign_inputs",
    "test_campaign_source_schema", "test_harness", "test_harness_budget_rates",
    "test_harness_profiles", "test_harness_schema",
    "test_assessment_api", "test_assessment_budget", "test_assessment_controls",
    "test_assessment_order_adapter", "test_assessment_query_validation",
    "test_assessment_service", "test_assessment_comparisons", "test_assessment_dataset",
    "test_poster", "test_poster_budget", "test_poster_replay", "test_material_delivery",
    "test_materials",
)
HISTORY = ROOT / ".scratch/evidence/if-history-production-20261001"


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def file_digest(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def release_sources():
    files = source_manifest()
    for path in (ROOT / "src").rglob("*.html"):
        files[str(path.relative_to(ROOT))] = file_digest(path)
    for name in ("tests", "scripts"):
        for path in (ROOT / name).rglob("*"):
            if path.is_file() and path.suffix in {".py", ".mjs", ".json"} and "__pycache__" not in path.parts:
                files[str(path.relative_to(ROOT))] = file_digest(path)
    for name in ("AGENTS.md", "docs/02-项目规划/接口补充里程碑.md",
                 "docs/03-技术设计/总体接口.md", "docs/03-技术设计/接口接入说明.md"):
        files[name] = file_digest(ROOT / name)
    return dict(sorted(files.items()))


def verify_sources(recorded):
    for name, expected in recorded.items():
        assert file_digest(ROOT / name) == expected, f"验证证据源码发生变化：{name}"


def verify_independent():
    path = ROOT / ".scratch/if01-independent-20261001-r2/report.json"
    report = read(path)
    assert report["shop_installed"] is False and report["sqlalchemy_installed"] is False
    assert report["host_startup_shutdown"] == report["unknown_module_rejected"] == "passed"
    verify_sources({"packages/" + name: value for name, value in report["sources_sha256"].items()})
    contracts = read(ROOT / ".scratch/evidence/if01-contracts-20261001T085046Z-607d7adc/report.json")
    verify_sources({name: value for name, value in contracts["sources_before"].items()
                    if name.startswith("packages/")})
    return {"report": str(path.relative_to(ROOT)), "sha256": file_digest(path),
            "scope": "独立wheel安装与公共包依赖；当前五模块接入由本轮程序集合检查"}


def verify_history_evidence(directory, settings):
    first = read(HISTORY / "import-r1.json")
    repeated = read(HISTORY / "import-r2.json")
    assert first["database"]["preserved_original_rows"] is True
    assert repeated["database"]["source_sha256"] == first["database"]["source_sha256"]
    assert all(amount == 0 for amount in repeated["database"]["increments"].values())
    assert Decimal(repeated["database"]["added_spent_cny"]) == 0
    assert Decimal(repeated["database"]["added_reserved_cny"]) == 0
    backups = {
        "main-before.dump": "d71eb5297603b57a027996a1140aa43a72a695bb44e785b4c727a03240495fbb",
        "assessment-before.dump": "da4c6c038fb9822f23512b047f6f7dba3cebd52153e24f3bf4b505765cfcd9ee",
    }
    for name, expected in backups.items():
        assert file_digest(HISTORY / name) == expected
    for section in ("poster_files", "evidence_files"):
        assert first[section]["source"] == first[section]["after"]
        for name, identity in first[section]["after"].items():
            base = settings.poster_dir if section == "poster_files" else ROOT / ".scratch/evidence/sm03-poster-paid-20261001T1"
            assert file_digest(base / name) == identity["sha256"]
    preserved = verify_history(settings.database_url, read(HISTORY / "main-original.json"))
    source = verify_history("postgresql+psycopg://shop@127.0.0.1:55436/shop",
                            read(HISTORY / "assessment-original.json"))
    save(directory / "main-preserved.json", preserved)
    save(directory / "assessment-source-preserved.json", source)
    return {"production_import": str(HISTORY.relative_to(ROOT)),
            "backups_sha256": backups,
            "reports_sha256": {path.name: file_digest(path) for path in HISTORY.glob("*.json")},
            "source_ledger": source["ledger"], "main_ledger": preserved["ledger"]}


def verify_live(path):
    report = read(path)
    assert report["status"] == "passed" and all(report["checks"].values())
    assert set(report["checks"]) == {
        "authenticated_catalog", "customer_identity_and_order", "upstream_and_resource_files",
        "customer_order_workflow", "customer_confirmed_ticket_and_reuse",
        "assessment_report_and_reuse", "poster_keywords_upstream_and_waiting_agents", "all_live_checks"}
    verify_sources(report["source_sha256"])
    assert report["source_sha256"] == report["source_after_sha256"]
    assert file_digest(report["inputs"]) == report["inputs_sha256"]
    assert Decimal(report["window_spent_and_reserved_cny"]) <= Decimal(5)
    assert all(row["status"] == "completed" for row in report["window_requests"])
    assert {row["stage"] for row in report["window_requests"]} == {"customer", "assessment", "m3"}
    return {"path": str(path), "sha256": file_digest(path), "run_id": report["run_id"],
            "fixed_inputs": report["inputs"], "fixed_inputs_sha256": report["inputs_sha256"],
            "requests": len(report["window_requests"]), "spent_cny": report["window_spent_and_reserved_cny"]}


def verify_browser(path, live_path):
    report = read(path)
    assert report["status"] == "passed" and report["checks"]
    assert all(row["passed"] for row in report["checks"])
    names = [row["name"] for row in report["checks"]]
    assert len(names) == len(set(names)) == 13
    assert set(names) == {
        "Harness任务DOM与服务端记录一致", "Harness审核预览与服务端结果一致",
        "Harness独立HTML导出与服务端结果一致", "Harness退出后服务端拒绝读取",
        "客服订单列表与真实客户服务端记录一致", "客服已完成会话与服务端记录一致",
        "客服已完成工单详情与服务端记录一致", "客服会话与订单reload后保持服务端结果",
        "客服退出后服务端拒绝读取", "考核员工账号、指标、来源与历史报告DOM匹配服务端",
        "考核HTML报告导出与服务端报告一致", "考核退出后服务端拒绝读取", "所有页面pageerror为空",
    }
    assert report["page_errors"] == []
    verify_sources(report["source_hashes"])
    assert report["source_report_sha256"] == file_digest(live_path)
    assert report["inputs_sha256"] == file_digest(report["inputs_path"])
    return {"path": str(path), "sha256": file_digest(path), "run_id": report["run_id"],
            "checks": [row["name"] for row in report["checks"]]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live-report", type=Path, required=True)
    parser.add_argument("--browser-report", type=Path, required=True)
    args = parser.parse_args()
    run_id = "if-release-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    directory = ROOT / ".scratch/evidence" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    settings = Settings()
    before = snapshot(settings)
    report = {"run_id": run_id, "status": "running", "source_sha256": release_sources(),
              "commands": [], "outcomes": {}, "versions": {name: version(name) for name in (
                  "integration-protocol", "integration-host", "poster-framework", "pydantic",
                  "fastapi", "sqlalchemy", "psycopg", "langgraph")}}
    environment = dict(os.environ, SHOP_TEST_DATABASE_URL="postgresql+psycopg://shop@127.0.0.1:55432/shop_test",
                       HF_HOME=str(ROOT / ".scratch/huggingface"), TMPDIR=str(directory / "tmp"))
    Path(environment["TMPDIR"]).mkdir()

    def command(label, arguments, cwd=ROOT):
        path = directory / f"{label}.txt"
        with path.open("w", encoding="utf-8") as output:
            result = subprocess.run(arguments, cwd=cwd, env=environment, stdout=output,
                                    stderr=subprocess.STDOUT, check=False)
        report["commands"].append({"label": label, "arguments": arguments,
                                   "exit_code": result.returncode, "output": str(path)})
        save(directory / "report.json", report)
        assert result.returncode == 0, f"检查失败：{label}，原始输出：{path}"

    save(directory / "before.json", before)
    save(directory / "report.json", report)
    try:
        report["outcomes"]["IF-01"] = verify_independent()
        report["live"] = verify_live(args.live_report)
        report["browser"] = verify_browser(args.browser_report, args.live_report)
        report["code_review"] = {"model": "gpt-6.1-sol", "reasoning_effort": "xhigh",
            "scope": ["integration", "budget", "paid_models", "customer_worker", "migrations", "verification"],
            "worker_shutdown_regressions": ["tests/test_integration_worker_shutdown.py",
                                            "tests/test_integration_worker_waiter.py"]}
        for name in ("assessment", "prepare_assessment_data", "setup_assessment_sample", "poster"):
            command(name + "-help", [sys.executable, "-m", "scripts." + name, "--help"])
        command("pytest", [sys.executable, "-m", "pytest", "-q",
            "--basetemp=" + str(directory / "pytest-tmp"), "--junitxml=" + str(directory / "pytest.xml"),
            *["tests/" + name + ".py" for name in TEST_FILES]])
        xml = ET.parse(directory / "pytest.xml")
        tests = xml.findall(".//testcase")
        identifiers = [(row.get("classname"), row.get("name")) for row in tests]
        assert len(tests) == len(set(identifiers)) and tests
        assert not xml.findall(".//failure") and not xml.findall(".//error") and not xml.findall(".//skipped")
        assert {row.get("classname") for row in tests} == {"tests." + name for name in TEST_FILES}
        report["program_checks"] = len(tests)
        command("ruff", [str(ROOT / ".venv/bin/ruff"), "check", "src", "scripts", "tests", "alembic", "packages"])
        command("alembic", [str(ROOT / ".venv/bin/alembic"), "check"])
        command("frontend", ["npm", "run", "build"], ROOT / "frontend")
        report["outcomes"]["IF-03"] = verify_history_evidence(directory, settings)
        runtime = inspect_database(settings.database_url)
        assert runtime["sent_requests"] == 0 and not any(runtime["running"].values())
        report["runtime"] = runtime
        report["source_after_sha256"] = release_sources()
        assert report["source_sha256"] == report["source_after_sha256"]
        assert snapshot(settings) == before, "最终程序验证新增或修改了主库请求、工单或账目"
        engine = create_engine(settings.database_url)
        try:
            with Session(engine) as session, session.begin():
                active = session.scalar(select(IntegrationBudgetWindow).where(IntegrationBudgetWindow.active))
                assert active is not None and active.id == report["live"]["run_id"]
                active.active = False
        finally:
            engine.dispose()
        report["outcomes"].update({
            "IF-02": "真实权限、调度、原子任务、请求去重与工单重复确认检查通过",
            "IF-04": "当前HTTP、来源文件与三个业务页面DOM检查通过",
            "IF-05": "真实进程、预算、取消恢复、少量云业务与完成结果复用检查通过",
            "IF-06": "受影响回归、源码及数据关联、迁移、构建、接入文档与费用检查通过"})
        report["budget_window_closed"] = True
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save(directory / "after.json", snapshot(settings))
        save(directory / "report.json", report)
        print(directory / "report.json")


if __name__ == "__main__":
    main()
