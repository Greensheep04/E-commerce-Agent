import argparse
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import numpy as np
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.campaign_types import Draft, ImageDescription, RequirementPlan, Review
from shop.config import Settings
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest


def main() -> None:
    parser = argparse.ArgumentParser(description="根据实际请求记录统计思考用量、费用及失败状态")
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--evaluation-report", type=Path)
    args = parser.parse_args()
    evaluation = None
    task_ids = None
    if args.evaluation_report:
        evaluation = json.loads(args.evaluation_report.read_text(encoding="utf-8"))
        if evaluation["milestone"] != "HAR-03" or evaluation["total"] != 20:
            raise ValueError("业务用量统计需要完整的20需求HAR-03评价报告")
        task_ids = [row["task"]["task_id"] for row in evaluation["records"]]
        if len(task_ids) != 20 or len(set(task_ids)) != 20:
            raise ValueError("评价报告必须关联20个不同任务")
    directory = args.directory or Path(".scratch/evidence") / (
        f"har-usage-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}")
    directory.mkdir(parents=True, exist_ok=True)
    engine = create_engine(Settings().database_url)
    with Session(engine) as session:
        query = select(ProviderRequest).order_by(ProviderRequest.created_at)
        task_query = select(HarnessTask).where(HarnessTask.workflow_version == "campaign-v1")
        if task_ids is not None:
            query = query.where(ProviderRequest.task_id.in_(task_ids))
            task_query = task_query.where(HarnessTask.id.in_(task_ids))
        requests = session.scalars(query).all()
        tasks = session.scalars(task_query).all()
        if task_ids is not None and {item.id for item in tasks} != set(task_ids):
            raise ValueError("评价报告关联的任务不完整")
        account = session.get(BudgetAccount, "project")
        groups = defaultdict(list)
        for request in requests:
            role = "description" if request.step_key.startswith("description") else request.step_key.removesuffix("-retry1")
            if request.model == "qwen3.7-text-embedding":
                role = "query_embedding" if request.task_id or request.step_key == "query_embedding" else "index_embedding"
            groups[role].append(request)
        modules = {}
        for role, items in groups.items():
            budget = 4096 if role == "description" else 25000
            tokens, durations, outputs = [], [], []
            unknown_reasoning = []
            truncated = []
            invalid_json = []
            for item in items:
                if item.response_body:
                    duration = item.response_body.get("client_duration_seconds")
                    if duration is not None:
                        durations.append(duration)
                if not item.usage or item.model == "qwen3.7-text-embedding":
                    continue
                details = item.usage.get("completion_tokens_details", {})
                reasoning = details.get("reasoning_tokens")
                if reasoning is None:
                    unknown_reasoning.append(item.id)
                else:
                    tokens.append(reasoning)
                    outputs.append(item.usage["completion_tokens"] - reasoning)
                if item.response_body:
                    if item.response_body["choices"][0]["finish_reason"] == "length":
                        truncated.append(item.id)
                    schema = {"description": ImageDescription, "understand": RequirementPlan,
                              "compose": Draft, "revise": Draft, "review": Review,
                              "rereview": Review}.get(role)
                    if schema is not None and item.response_body["choices"][0]["finish_reason"] == "stop":
                        try:
                            schema.model_validate_json(item.response_body["choices"][0]["message"]["content"])
                        except ValidationError:
                            invalid_json.append(item.id)
            summary = {
                "requests": len(items), "statuses": dict(Counter(item.status for item in items)),
                "known_cost_cny": str(sum((item.charged_cny or Decimal(0) for item in items), Decimal(0))),
                "unknown_or_unsent_reserved_cny": str(sum(
                    (item.reservation_cny for item in items if item.charged_cny is None), Decimal(0))),
                "truncated_request_ids": truncated, "reasoning_usage_unknown_ids": unknown_reasoning,
                "json_validation_failed_request_ids": invalid_json,
                "request_ids": [item.id for item in items],
                "usage_unknown_request_ids": [item.id for item in items if not item.usage],
            }
            if tokens:
                mean = float(np.mean(tokens))
                summary["reasoning"] = {
                    "sample_count": len(tokens), "mean": mean,
                    "p50": float(np.percentile(tokens, 50)), "p95": float(np.percentile(tokens, 95)),
                    "maximum": max(tokens), "reference_budget": budget,
                    "mean_gap": budget - mean, "mean_ratio": mean / budget,
                    "reference_only": any(item.service == "deepseek" for item in items),
                    "formal_output_mean": float(np.mean(outputs)),
                }
                summary["formal_output"] = {"sample_count": len(outputs), "mean": float(np.mean(outputs)),
                                            "p50": float(np.percentile(outputs, 50)),
                                            "p95": float(np.percentile(outputs, 95)), "maximum": max(outputs)}
            if durations:
                summary["duration_seconds"] = {"sample_count": len(durations), "mean": float(np.mean(durations)),
                                                "p50": float(np.percentile(durations, 50)),
                                                "p95": float(np.percentile(durations, 95)),
                                                "maximum": max(durations)}
            modules[role] = summary
        report = {
            "measured_at": datetime.now(UTC).isoformat(), "modules": modules,
            "account": {"spent_cny": str(account.spent_cny), "reserved_cny": str(account.reserved_cny)},
            "limits_cny": {"project": 300, "new_request_threshold": 270, "verification": 10},
            "evidence_limit": "思考Token仅按提供商实际usage统计；无usage的请求单列。接口样本与业务评价由运行报告分别关联。",
            "scope": {"type": "fixed_business_evaluation" if evaluation else "all_historical_requests",
                      "task_ids": task_ids,
                      "evaluation_report": str(args.evaluation_report) if evaluation else None,
                      "evaluation_sha256": sha256(args.evaluation_report.read_bytes()).hexdigest() if evaluation else None},
            "sources_sha256": {str(path): sha256(path.read_bytes()).hexdigest() for path in (
                Path(__file__), Path("src/shop/model_profiles.py"), Path("src/shop/paid_models.py"))},
            "campaign_task_statuses": dict(Counter(item.status for item in tasks)),
            "campaign_result_statuses": dict(Counter(item.result["status"] for item in tasks if item.result)),
            "campaign_stop_reasons": dict(Counter(item.stop_reason for item in tasks if item.stop_reason)),
        }
        if tasks:
            durations = [float(item.elapsed_seconds) for item in tasks]
            report["task_duration_seconds"] = {
                "sample_count": len(durations), "mean": float(np.mean(durations)),
                "p50": float(np.percentile(durations, 50)), "p95": float(np.percentile(durations, 95)),
                "maximum": max(durations)}
        if evaluation:
            report["quality"] = {key: evaluation[key] for key in (
                "correct", "total", "insufficient_correct", "insufficient_total", "passed", "fixture_sha256")}
    path = directory / "report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(path), "account": report["account"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
