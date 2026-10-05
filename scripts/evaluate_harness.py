import argparse
import json
import logging
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from shop.asset_index import AUX_SOURCE, MAIN_SOURCE, IndexService
from shop.campaign import CampaignService
from shop.campaign_types import RequirementPlan
from shop.config import Settings
from shop.paid_models import PaidModels


def save(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def code_hashes() -> dict:
    return {str(path): sha256(path.read_bytes()).hexdigest()
            for root in ("src/shop", "scripts", "tests/fixtures")
            for path in sorted(Path(root).rglob("*")) if path.is_file() and path.suffix in {".py", ".json", ".html"}}


def evaluate_queries(
    service: CampaignService, directory: Path, fixture_path: Path,
    reuse_report_path: Path | None,
) -> dict:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    if len(fixture["queries"]) != 30 or len({item["id"] for item in fixture["queries"]}) != 30:
        raise ValueError("检索评价需要30条具有不同编号的固定查询")
    if any(not item["expected_record_ids"] for item in fixture["queries"]):
        raise ValueError("固定查询必须包含有依据的预期记录")
    reusable = {}
    if reuse_report_path:
        previous = json.loads(reuse_report_path.read_text(encoding="utf-8"))
        if previous["milestone"] != "HAR-02":
            raise ValueError("向量复用需要HAR-02检索评价报告")
        reusable = {item["query"]: item["run_id"] for item in previous["records"]}
    index = IndexService(service.settings, service.maker)
    if index.coverage(AUX_SOURCE) != 50:
        raise ValueError("辅助图库索引尚未齐备")
    records = []
    client = PaidModels(service.settings, service.budget)
    for query in fixture["queries"]:
        path = directory / f"{query['id']}.json"
        if path.exists():
            saved = json.loads(path.read_text(encoding="utf-8"))
            if saved["query"] != query["query"] or saved["expected_record_ids"] != query["expected_record_ids"]:
                raise ValueError("保存的检索结果与当前固定查询不一致，请使用新的运行目录")
            records.append(saved)
            continue
        identity_path = directory / f"{query['id']}-identity.json"
        if identity_path.exists():
            identity = json.loads(identity_path.read_text(encoding="utf-8"))
            if identity["input"] != query:
                raise ValueError("保存的检索身份与当前固定查询不一致")
            run_id = identity["run_id"]
        else:
            run_id = reusable.get(query["query"]) or str(uuid4())
            save(identity_path, {"run_id": run_id, "input": query})
        response = client.embed([query["query"]], run_id=run_id,
                                step_key="query_embedding", timeout_seconds=60)
        plan = RequirementPlan(count=2, queries=[query["query"]])
        found = index.search(plan, index.embedding_vector(response).tolist(), AUX_SOURCE)
        ids = [item["record_id"] for item in found]
        row = {"id": query["id"], "run_id": run_id, "query": query["query"],
               "embedding_reused": query["query"] in reusable,
               "expected_record_ids": query["expected_record_ids"], "candidate_record_ids": ids,
               "correct": bool(set(ids) & set(query["expected_record_ids"])), "candidates": found}
        save(path, row)
        records.append(row)
    correct = sum(item["correct"] for item in records)
    return {"milestone": "HAR-02", "correct": correct, "total": len(records),
            "recall_at_12": correct / len(records), "passed": correct >= 27,
            "fixture": str(fixture_path), "reuse_query_report": str(reuse_report_path) if reuse_report_path else None,
            "fixture_sha256": sha256(fixture_path.read_bytes()).hexdigest(),
            "records": records}


def evaluate_cases(service: CampaignService, directory: Path, selected: set[str]) -> dict:
    if IndexService(service.settings, service.maker).coverage(MAIN_SOURCE) != 300:
        raise ValueError("主图库索引尚未齐备")
    fixture_path = Path("tests/fixtures/harness_cases.json")
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    records = []
    for case in fixture["cases"]:
        if selected and case["id"] not in selected:
            continue
        path = directory / f"{case['id']}.json"
        if path.exists():
            records.append(json.loads(path.read_text(encoding="utf-8")))
            continue
        identity_path = directory / f"{case['id']}-identity.json"
        if identity_path.exists():
            identity = json.loads(identity_path.read_text(encoding="utf-8"))
        else:
            identity = service.create(case["prompt"])
            save(identity_path, identity)
        execution_error = None
        try:
            task = service.run(identity["task_id"])
        except Exception as error:
            logging.getLogger(__name__).exception("宣传需求执行失败，case_id=%s", case["id"])
            execution_error = f"{type(error).__name__}: {error}"
            task = service.show(identity["task_id"])
        result = task["result"]
        correct = task["status"] == "completed" and result["status"] == case["expected_status"]
        violations = []
        if result and result["status"] == "materials_found":
            if not 1 <= len(result["recommendations"]) <= 2:
                violations.append("推荐数量超出范围")
            for item in result["recommendations"]:
                for key, value in case["attributes"].items():
                    if item["original_fields"].get(key) != value:
                        violations.append(f"{item['asset_id']}不符合{key}={value}")
                if item["source_id"] != MAIN_SOURCE:
                    violations.append("业务结果引用了非主来源")
            if not result["review"]["approved"]:
                violations.append("业务结果没有通过原图审核")
        elif result:
            if result["recommendations"] or not result["missing_info"]:
                violations.append("信息不足结果缺少原因或仍包含推荐")
        correct = bool(correct and not violations)
        row = {"id": case["id"], "expected": case, "correct": correct,
               "violations": violations, "execution_error": execution_error, "task": task}
        if correct and case["expected_status"] == "materials_found":
            (directory / f"{case['id']}.html").write_text(service.render(task["task_id"]), encoding="utf-8")
        save(path, row)
        records.append(row)
        print(json.dumps({"case": case["id"], "correct": correct, "status": task["status"],
                          "task_id": task["task_id"]}), flush=True)
    correct = sum(item["correct"] for item in records)
    insufficient = [item for item in records if item["expected"]["expected_status"] == "insufficient"]
    return {"milestone": "HAR-03", "correct": correct, "total": len(records),
            "insufficient_correct": sum(item["correct"] for item in insufficient),
            "insufficient_total": len(insufficient),
            "passed": len(records) == 20 and correct >= 18 and len(insufficient) == 8
                      and all(item["correct"] for item in insufficient)
                      and not any(item["violations"] for item in records),
            "unsupported_claims": "需要结合保存的来源、文案及原图审核逐项核查",
            "fixture_sha256": sha256(fixture_path.read_bytes()).hexdigest(), "records": records}


def main() -> None:
    parser = argparse.ArgumentParser(description="执行固定检索或宣传需求评价并保存逐条证据")
    parser.add_argument("mode", choices=["queries", "cases"])
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--query-fixture", type=Path, default=Path("tests/fixtures/harness_queries.json"))
    parser.add_argument("--reuse-query-report", type=Path)
    args = parser.parse_args()
    directory = args.directory or Path(".scratch/evidence") / (
        f"har-{args.mode}-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}")
    directory.mkdir(parents=True, exist_ok=True)
    service = CampaignService(Settings())
    start_path = directory / "started.json"
    if not start_path.exists():
        save(start_path, {"code_sha256": code_hashes(), "mode": args.mode,
                          "started_at": datetime.now(UTC).isoformat()})
    report = (evaluate_queries(service, directory, args.query_fixture, args.reuse_query_report) if args.mode == "queries"
              else evaluate_cases(service, directory, set(args.case)))
    save(directory / "report.json", report)
    print(json.dumps({"report": str(directory / "report.json"), "correct": report["correct"],
                      "total": report["total"], "passed": report["passed"]}))


if __name__ == "__main__":
    main()
