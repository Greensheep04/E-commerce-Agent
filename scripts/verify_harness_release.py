import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.model_profiles import profiles
from shop.runtime_models import BudgetAccount, ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = {
    "recovery": ".scratch/evidence/har-resume-20260930T105010-e83f8ea1/report.json",
    "queries": ".scratch/evidence/har-queries-image-audit-v1-20260930T1220/report.json",
    "query_freeze": ".scratch/evidence/har-query-fixture-freeze-20260930T1215/report.json",
    "cases": ".scratch/evidence/har-cases-v2-20260930T1100/report.json",
    "facts": ".scratch/evidence/har-fact-audit-v2-20260930/report.json",
    "review": ".scratch/evidence/har-review-20260930T115013-e30e81af/report.json",
    "http": ".scratch/evidence/m2-http-20260930T112511-38c8df82/report.json",
    "browser": ".scratch/evidence/har-completed-20260930T114713Z-6f3a541b/report.json",
}
SOURCE_REUSE_CHANGES = {
    "src/shop/paid_models.py": {
        "previous": "8495e087cbcfd150894e8c104a57707b1264b13299b298d78ea75a12db052673",
        "current": "1cb2233ce7e5cfa2308e486bf4a92ab895e72fd1ef8bad112612e105cf343454",
        "scope": "发送前取消转换为RequestLimit(cancelled)，保存参数、响应及计费流程保持一致。",
        "affected_check": "tests/test_campaign_cancel_race.py，实际PostgreSQL取消顺序覆盖文字及embedding。",
    },
    "src/shop/asset_index.py": {
        "previous": "990e5d48bfa016e5bf1f4a230f30dbdda414e1927529a48a6e0f802af130c79a",
        "current": "dd6e7cedff34b7bcfa87875f2dc43cf54a58d14499718b306c9357c57eae2cc4",
        "scope": "attribute_values通过dict.items遍历相同字段与集合，查询、索引与返回值保持一致。",
        "affected_check": "tests/test_campaign_source_schema.py，当前来源属性检查。",
    },
    "src/shop/runtime_models.py": {
        "previous": "70333038036d3d6c6f57c922cbaedd8ee69a6f60c0392bee87d6bb63b3c421ff",
        "current": "6fd7cd5414d18b07f6ee93669a6dc971b6da4a7e5f902b123b576aa9238d635d",
        "scope": "ORM声明数据库已有的run_id/step_key唯一约束与AssetVersion索引，主数据库结构及业务流程保持一致。",
        "affected_check": "alembic check与tests/test_harness_schema.py，实际PostgreSQL唯一约束及索引检查。",
    },
}


def save(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read(path: str | Path) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def digest(path: str | Path) -> str:
    return sha256((ROOT / path).read_bytes()).hexdigest()


def source_manifest() -> dict:
    paths = [ROOT / name for name in ("pyproject.toml", "uv.lock", ".env.example", "README.md")]
    for root in ("src", "scripts", "tests", "frontend", "alembic"):
        paths.extend(path for path in (ROOT / root).rglob("*")
                     if path.is_file() and path.suffix in {".py", ".json", ".html", ".ts", ".tsx", ".css", ".mjs", ".sh"}
                     and "node_modules" not in path.parts and "dist" not in path.parts
                     and "__pycache__" not in path.parts)
    return {str(path.relative_to(ROOT)): digest(path) for path in sorted(set(paths))}


def assert_sources(recorded: dict, selected: set[str] | None = None) -> None:
    required = selected if selected is not None else set(recorded)
    assert required <= set(recorded), required - set(recorded)
    for path in required:
        current = digest(path)
        if current != recorded[path]:
            change = SOURCE_REUSE_CHANGES.get(path)
            assert change is not None, f"证据源码已变化：{path}"
            assert recorded[path] == change["previous"] and current == change["current"], path


def verify_reused_evidence() -> dict:
    data = {label: read(path) for label, path in EVIDENCE.items()}
    recovery = data["recovery"]
    assert recovery["status"] == "passed" and all(recovery["checks"].values())
    assert_sources(recovery["source_sha256"], {
        "src/shop/harness.py", "src/shop/paid_models.py", "src/shop/budget.py",
        "src/shop/runtime_models.py", "src/shop/model_profiles.py", "src/shop/config.py",
    })
    queries = data["queries"]
    assert queries["passed"] and queries["total"] == 30 and queries["correct"] >= 27
    assert queries["fixture_sha256"] == digest(queries["fixture"])
    freeze = data["query_freeze"]
    assert freeze["passed"] and not freeze["ranking_read"] and freeze["model_calls"] == 0
    assert freeze["fixture_sha256"] == queries["fixture_sha256"]
    query_started = read(Path(EVIDENCE["queries"]).parent / "started.json")
    assert datetime.fromisoformat(freeze["verified_at"]) < datetime.fromisoformat(query_started["started_at"])
    assert_sources(query_started["code_sha256"], {
        "src/shop/asset_index.py", "src/shop/paid_models.py", "scripts/evaluate_harness.py",
    })
    cases = data["cases"]
    assert cases["passed"] and cases["total"] == 20 and cases["correct"] >= 18
    assert cases["insufficient_correct"] == cases["insufficient_total"] == 8
    assert not any(row["violations"] for row in cases["records"])
    assert cases["fixture_sha256"] == digest("tests/fixtures/harness_cases.json")
    assert_sources(read(Path(EVIDENCE["cases"]).parent / "started.json")["code_sha256"], {
        "src/shop/campaign.py", "src/shop/asset_index.py", "src/shop/paid_models.py",
        "src/shop/budget.py", "src/shop/campaign_types.py", "src/shop/model_profiles.py",
    })
    assert data["facts"]["source_report"] == EVIDENCE["cases"]
    assert data["facts"]["scope"]["fact_checks"]["unsupported_product_fact_statements_found"] == 0
    assert data["review"]["status"] == "passed"
    assert data["review"]["source_sha256"] == digest("src/shop/campaign.py")
    assert all(not row["review"]["approved"] for row in data["review"]["records"])
    unsupported = read(".scratch/har-review-reuse.json")[0]
    assert unsupported["same_instruction"] and not json.loads(unsupported["content"])["approved"]
    assert data["http"]["status"] == "passed"
    assert_sources(data["http"]["source_sha256"])
    browser = data["browser"]
    assert browser["status"] == "passed" and browser["escaping_check_requested"]
    assert len(browser["checks"]) == 10 and not browser["page_errors"]
    assert_sources(browser["sources_sha256"])
    return {label: {"path": path, "sha256": digest(path)} for label, path in EVIDENCE.items()}


def run_check(directory: Path, name: str, command: list[str], environment: dict) -> dict:
    result = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True,
                            text=True, check=False)
    (directory / f"{name}.stdout.txt").write_text(result.stdout, encoding="utf-8")
    (directory / f"{name}.stderr.txt").write_text(result.stderr, encoding="utf-8")
    record = {"command": command, "exit_code": result.returncode}
    save(directory / f"{name}.json", record)
    print(json.dumps({"check": name, **record}, ensure_ascii=False), flush=True)
    result.check_returncode()
    return record


def verify_entrypoint(directory: Path, settings: Settings) -> dict:
    prompt = "请推荐一件黑色男士T恤，写中文商品宣传标题、说明和文案。不要补充没有来源依据的价格、库存、材质或性能信息。"
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=150, trust_env=False) as client:
        client.post("/api/login", json={
            "username": "admin",
            "password": settings.admin_password_file.read_text(encoding="utf-8").strip(),
        }).raise_for_status()
        response = client.post("/api/harness/tasks", json={"prompt": prompt})
        response.raise_for_status()
        created = response.json()
        save(directory / "entrypoint-created.json", created)
        assert created["model_profile"]["prompt_version"] == "campaign-v2"
        endpoint = f"/api/harness/tasks/{created['task_id']}"
        response = client.post(endpoint + "/run")
        response.raise_for_status()
        task = response.json()
        save(directory / "entrypoint-result.json", task)
        assert task["status"] == "completed", task["stop_reason"]
        assert task["result"]["status"] == "materials_found"
        recommendations = task["result"]["recommendations"]
        assert len(recommendations) == 1 and task["result"]["review"]["approved"]
        for item in recommendations:
            assert item["source_id"] == "ashraq/fashion-product-images-small"
            assert item["source_revision"] == "3859c76db2f6f3d3b9a3863345e3ccdbff75879d"
            assert all(item["original_fields"][key] == value for key, value in (
                ("gender", "Men"), ("articleType", "Tshirts"), ("baseColour", "Black")))
            file = client.get(f"/api/assets/{item['asset_id']}/versions/{item['version']}/file")
            file.raise_for_status()
            assert sha256(file.content).hexdigest() == item["file_sha256"]
        preview = client.get(endpoint + "/preview")
        exported = client.get(endpoint + "/export")
        preview.raise_for_status()
        exported.raise_for_status()
        assert preview.content == exported.content
        assert "sandbox" in preview.headers["content-security-policy"]
        assert "data:image/" in exported.text
        (directory / "entrypoint.html").write_bytes(exported.content)
        for action in ("run", "resume"):
            response = client.post(endpoint + "/" + action)
            response.raise_for_status()
            assert response.json() == task
        client.post("/api/logout").raise_for_status()
        assert client.get(endpoint).status_code == 401
    return {"entrypoint": "http://127.0.0.1:8000", "task_id": task["task_id"],
            "status": "passed", "model_calls": task["model_calls"],
            "tool_calls": task["tool_calls"], "elapsed_seconds": task["elapsed_seconds"],
            "export_sha256": sha256(exported.content).hexdigest(),
            "checks": ["api_create_run_campaign_v2", "actual_source_file_and_conditions",
                       "approved_image_review", "preview_and_independent_html_export",
                       "completed_task_request_reuse", "logout_revokes_access"]}


def main() -> None:
    parser = argparse.ArgumentParser(description="运行最终Harness集合并复用已验证的付费与浏览器证据")
    parser.add_argument("--node", type=Path, required=True)
    args = parser.parse_args()
    settings = Settings()
    run_id = f"har-release-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    directory = ROOT / ".scratch/evidence" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    before = source_manifest()
    save(directory / "source-before.json", before)
    reused = verify_reused_evidence()
    save(directory / "reused-evidence.json", reused)
    environment = dict(os.environ)
    environment["SHOP_TEST_DATABASE_URL"] = "postgresql+psycopg://shop@127.0.0.1:55432/shop_test"
    environment["HF_HOME"] = str(ROOT / ".scratch/huggingface")
    environment["UV_CACHE_DIR"] = str(ROOT / ".scratch/uv-cache")
    environment["TMPDIR"] = str(ROOT / ".scratch/process-tmp")
    environment["PATH"] = str(args.node.resolve().parent) + os.pathsep + environment["PATH"]
    Path(environment["TMPDIR"]).mkdir(parents=True, exist_ok=True)
    save(directory / "inputs.json", {
        "python": sys.version,
        "dependencies": {name: version(name) for name in (
            "fastapi", "sqlalchemy", "psycopg", "langgraph", "langgraph-checkpoint-postgres",
            "httpx", "pgvector", "jinja2", "numpy")},
        "model_profiles": {role: profile.model_dump() for role, profile in
                           profiles(settings.harness_token_limits_file).items()},
        "main_database": "shop", "test_database": "shop_test",
        "node": str(args.node.resolve()), "reused_evidence": reused,
    })
    python = sys.executable
    tests = ["tests/" + name for name in (
        "test_harness.py", "test_harness_budget_rates.py", "test_harness_profiles.py", "test_harness_schema.py",
        "test_index_recovery.py", "test_index_versions.py", "test_campaign_inputs.py",
        "test_campaign_runtime.py", "test_campaign_source_schema.py", "test_campaign_cancel_race.py")]
    commands = [
        ("migration", [python, "-m", "alembic", "check"]),
        ("pytest", [python, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                    f"--basetemp={directory / 'pytest-data'}", f"--junitxml={directory / 'pytest.xml'}", *tests]),
        ("ruff", [python, "-m", "ruff", "check", "src", "scripts", "tests"]),
        ("typescript", [str(args.node), "frontend/node_modules/typescript/bin/tsc", "--noEmit", "-p", "frontend/tsconfig.json"]),
        ("frontend", [str(args.node), "frontend/node_modules/vite/bin/vite.js", "build", "frontend"]),
        ("indexes", [python, "scripts/verify_harness_indexes.py"]),
        ("models", [python, "scripts/verify_harness_models.py"]),
    ]
    checks = {name: run_check(directory, name, command, environment)
              for name, command in commands}
    models_path = json.loads((directory / "models.stdout.txt").read_text(encoding="utf-8"))["report"]
    models = read(models_path)
    assert models["checks"] == {
        "text_thinking_json": True, "image_thinking_json": True, "embedding_1024": True,
    }
    assert_sources(models["source_hashes"])
    entrypoint = verify_entrypoint(directory, settings)
    save(directory / "entrypoint-report.json", entrypoint)
    checks["usage-all"] = run_check(directory, "usage-all", [python, "scripts/report_harness_usage.py",
        "--directory", str(directory / "usage-all")], environment)
    checks["usage-business"] = run_check(directory, "usage-business", [python, "scripts/report_harness_usage.py",
        "--evaluation-report", EVIDENCE["cases"], "--directory", str(directory / "usage-business")], environment)
    engine = create_engine(settings.database_url)
    with Session(engine) as session:
        account = session.get(BudgetAccount, "project")
        request_count = session.scalar(select(func.count()).select_from(ProviderRequest))
        assert account.spent_cny + account.reserved_cny < Decimal(10)
        budget = {"spent_cny": str(account.spent_cny), "reserved_cny": str(account.reserved_cny),
                  "request_count": request_count, "verification_limit_cny": 10}
    engine.dispose()
    after = source_manifest()
    save(directory / "source-after.json", after)
    assert before == after, "整体验证期间源码发生变化"
    save(directory / "report.json", {
        "run_id": run_id, "status": "passed", "milestones": [f"HAR-0{number}" for number in range(1, 6)],
        "checks": checks, "entrypoint": entrypoint, "budget": budget,
        "current_models_report": {"path": models_path, "sha256": digest(models_path)},
        "reused_evidence": reused, "source_reuse_changes": SOURCE_REUSE_CHANGES,
        "source_manifest_sha256": sha256(
            json.dumps(after, sort_keys=True).encode()).hexdigest(),
        "limits": {"queries": "28/30；原始25/30保留", "cases": "18/20；两条超时计失败",
                   "annotation": "独立原图模型标注，未经人工视觉核查",
                   "fact_review": "来源字段与保存描述的独立文字审查"},
    })
    print(json.dumps({"status": "passed", "report": str(directory / "report.json")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
