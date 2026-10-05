import argparse
import asyncio
import copy
import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic
from uuid import uuid4

import httpx
from data_center_protocol import DataQuestion
from integration_protocol import OperationResult
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.orm import Session

from scripts.run_data_center_acceptance import invoke, save, source_digests, source_inputs
from scripts.verify_data_center_release import (
    audit_source_applicability,
    source_manifest_is_applicable,
)
from shop.config import Settings
from shop.data_center_adapter import DataCenterAdapter
from shop.runtime_models import ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
CASE_IDS = ("REL-01", "REL-02", "REL-03", "REL-04", "REL-05", "REL-06")
PROFILE = {"model": "deepseek-flash", "thinking": "high", "output_tokens": 16384}


def digest(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":")).encode()).hexdigest()


def build_pairs(fixture, registry, frozen):
    if (frozen["config"].get("model") != PROFILE["model"]
            or frozen["config"].get("thinking") != PROFILE["thinking"]
            or frozen["config"].get("output_tokens") not in {8192, 16384}):
        raise ValueError("Graph批次的回答配置与比较要求不一致")
    expected = {item["case_id"]: item for item in source_inputs(fixture, registry)
                if item["case_id"] in CASE_IDS and item["attempt"] == 1}
    pairs = []
    for case_id in CASE_IDS:
        candidates = [item for item in frozen["attempts"]
                      if item["case_id"] == case_id and item["attempt"] == 1]
        if len(candidates) != 1:
            raise ValueError(f"Graph缺少唯一的第一次记录：{case_id}")
        graph = copy.deepcopy(candidates[0])
        if any(graph[name] != expected[case_id][name]
               for name in ("category", "actor", "expected", "payload")):
            raise ValueError(f"Graph固定输入与来源登记不一致：{case_id}")
        hybrid = {**copy.deepcopy(graph), "request_id": str(uuid4()),
                  "payload": {**graph["payload"], "retrieval_mode": "hybrid"}}
        pairs.append({"case_id": case_id, "graph": graph, "hybrid": hybrid})
    return pairs


def validate_graph_record(pair, record):
    if record["input"] != pair["graph"]:
        raise ValueError("Graph回答没有绑定固定的第一次记录")
    if (not record.get("finished_at") or not record.get("source_stable")
            or record.get("timeout") or record.get("error")
            or record.get("result", {}).get("status") in {None, "queued", "running"}):
        raise ValueError("Graph第一次记录缺少稳定源码及完整终态")


def answer_profile(requests):
    answers = [item for item in requests if item["step_key"] == "answer"]
    if len(answers) != 1:
        return None
    request = answers[0]
    body = request["request_body"]
    profile = {"model": request["model"], "thinking": body.get("reasoning_effort"),
               "output_tokens": body.get("max_tokens")}
    if (profile["model"] != PROFILE["model"] or profile["thinking"] != PROFILE["thinking"]
            or profile["output_tokens"] not in {8192, 16384}
            or body.get("thinking") != {"type": "enabled"}):
        raise ValueError("实际模型回答配置与比较要求不一致")
    return {**profile, "system_prompt_sha256": sha256(
        body["messages"][0]["content"].encode()).hexdigest()}


def read_execution(shop_database, data_database, task_id):
    tasks = Table("dc_tasks", MetaData(), autoload_with=data_database)
    with data_database.connect() as connection:
        task = connection.execute(select(tasks.c.task_id, tasks.c.run_id, tasks.c.actor_id,
            tasks.c.request_id, tasks.c.payload, tasks.c.status, tasks.c.checkpoints)
            .where(tasks.c.task_id == task_id)).mappings().one()
    checkpoints = task["checkpoints"]
    with Session(shop_database) as session:
        rows = session.scalars(select(ProviderRequest).where(
            ProviderRequest.run_id == task["run_id"]).order_by(ProviderRequest.created_at)).all()
        requests = [{"request_id": row.id, "step_key": row.step_key, "model": row.model,
                     "status": row.status, "request_body": row.request_body,
                     "response_body": row.response_body, "usage": row.usage,
                     "spent_cny": str(row.charged_cny or Decimal(0)),
                     "reserved_cny": str(row.reservation_cny
                                         if row.charged_cny is None else Decimal(0)),
                     "created_at": row.created_at.isoformat(),
                     "completed_at": row.completed_at.isoformat() if row.completed_at else None}
                    for row in rows]
    return {name: task[name] for name in ("task_id", "run_id", "actor_id", "request_id",
                                         "payload", "status")} | {
        "authorization": checkpoints["authorization"],
        "plan": checkpoints.get("query_plan"), "plan_reason": checkpoints.get("query_plan_reason"),
        "requests": requests,
        "answer_profile": answer_profile(requests),
        "spent_cny": str(sum((Decimal(item["spent_cny"]) for item in requests), Decimal(0))),
        "reserved_cny": str(sum((Decimal(item["reserved_cny"]) for item in requests), Decimal(0))),
        "token_totals": {name: sum((item["usage"] or {}).get(name, 0) for item in requests)
                         for name in ("prompt_tokens", "completion_tokens", "total_tokens")}}


def pair_conditions(graph_record, hybrid_record, graph_execution, hybrid_execution):
    def corpus(record):
        return sorted(digest({name: snapshot[name] for name in (
            "snapshot_id", "source_ids", "schema_sha256", "content_sha256", "source_revisions")})
            for snapshot in record["result"].get("data", {}).get("snapshots", []))

    def plan_sources(execution):
        return sorted(digest({**{name: value for name, value in query.items()
                                 if name != "retrieval_mode"},
                              "source_ids": sorted(query["source_ids"])})
            for query in execution["plan"] or [])

    return {"same_actor": graph_execution["actor_id"] == hybrid_execution["actor_id"],
            "same_question_and_allowed_sources": graph_execution["payload"] == {
                **hybrid_execution["payload"], "retrieval_mode": "graph"},
            "same_authorization": graph_execution["authorization"] == hybrid_execution["authorization"],
            "same_actual_planned_sources": plan_sources(graph_execution) == plan_sources(hybrid_execution),
            "same_actual_snapshots": corpus(graph_record) == corpus(hybrid_record),
            "same_answer_profile_and_prompt": graph_execution["answer_profile"] is not None
                and graph_execution["answer_profile"] == hybrid_execution["answer_profile"],
            "independent_request": graph_execution["request_id"] != hybrid_execution["request_id"],
            "independent_task_run": graph_execution["task_id"] != hybrid_execution["task_id"]
                and graph_execution["run_id"] != hybrid_execution["run_id"]}


def prepare(args):
    fixture_path = args.fixture.resolve()
    registry_path = args.registry.resolve()
    graph_inputs_path = args.graph_run.resolve() / "inputs.json"
    fixture, registry, graph_inputs = (json.loads(path.read_text())
                                      for path in (fixture_path, registry_path, graph_inputs_path))
    if any(graph_inputs["config"].get(name) != value for name, value in PROFILE.items()):
        raise ValueError("执行比较需要当前16384输出tokens的Graph回答配置")
    fixed = {str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
             for path in (fixture_path, registry_path)}
    if graph_inputs["input_sha256"] != fixed:
        raise ValueError("比较固定输入与Graph批次的fixture/registry摘要不一致")
    binding = {"input_sha256": fixed, "graph_inputs_path": str(graph_inputs_path),
               "graph_inputs_sha256": sha256(graph_inputs_path.read_bytes()).hexdigest(),
               "graph_run_id": graph_inputs["run_id"], "profile": PROFILE}
    path = args.output / "inputs.json"
    if path.exists():
        frozen = json.loads(path.read_text())
        if frozen["binding"] != binding:
            raise ValueError("比较恢复时固定输入或Graph批次发生变化")
        return frozen
    frozen = {"run_id": str(uuid4()), "created_at": datetime.now(UTC).isoformat(),
              "binding": binding, "pairs": build_pairs(fixture, registry, graph_inputs),
              "selection_rule": "REL01至REL06固定顺序，各取正式Graph第1次记录，各生成一次hybrid回答",
              "independent_quality_review": "pending"}
    save(path, frozen)
    return frozen


async def login(client, identity):
    response = await client.post("/api/login", json={"username": identity["username"],
        "password": Path(identity["password_file"]).read_text().strip()})
    response.raise_for_status()
    return datetime.now(UTC).isoformat()


async def references(client, result):
    return [await invoke(client, "reference", reference, str(uuid4()))
            for reference in result.get("data", {}).get("evidence", [])]


async def hybrid_answer(args, client, pair, path):
    if path.exists():
        prior = json.loads(path.read_text())
        if prior["input"] != pair["hybrid"]:
            raise ValueError("hybrid恢复请求内容发生变化")
        if prior.get("finished_at"):
            return prior
    observation = {"input": pair["hybrid"], "started_at": datetime.now(UTC).isoformat(),
                   "source_before": source_digests(), "observations": []}
    started = monotonic()
    save(path, observation)
    try:
        result = await invoke(client, "ask", pair["hybrid"]["payload"], pair["hybrid"]["request_id"])
        observation["observations"].append(result)
        save(path, observation)
        if "http_status" in result:
            raise ValueError(f"hybrid提交HTTP失败：{result['http_status']}")
        task_id = DataCenterAdapter.task_id_from_result(OperationResult.model_validate(result))
        observation["task_id"] = task_id
        while result.get("status") in {"queued", "running"}:
            if monotonic() - started > args.timeout:
                observation["timeout"] = True
                break
            await asyncio.sleep(0.5)
            result = await invoke(client, "show", {"task_id": task_id}, str(uuid4()))
            observation["observations"].append(result)
            save(path, observation)
        observation["result"] = result
        observation["references"] = await references(client, result)
        return observation
    except BaseException as error:
        observation["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        observation["finished_at"] = datetime.now(UTC).isoformat()
        observation["elapsed_seconds"] = monotonic() - started
        observation["source_after"] = source_digests()
        observation["source_stable"] = observation["source_before"] == observation["source_after"]
        save(path, observation)


async def execute(args, frozen):
    identities = json.loads(args.identities.read_text())
    if identities["window"] != "dc-live-20261004":
        raise ValueError("比较问答缺少已批准的费用窗口")
    current_sources = source_digests()
    applicability = audit_source_applicability(args.source_applicability, current_sources)
    graph_records = {}
    for pair in frozen["pairs"]:
        path = args.graph_run / "attempts" / f"{pair['case_id']}-1.json"
        record = json.loads(path.read_text())
        validate_graph_record(pair, record)
        if not source_manifest_is_applicable(record, "cross_document", current_sources, applicability):
            raise ValueError("Graph第一次回答与当前生产源码摘要不一致")
        graph_records[pair["case_id"]] = (path, record)
    shop_database = create_engine(Settings().database_url)
    data_database = create_engine(args.data_database_url)
    report = {"schema": "dc03.final_retrieval_comparison.v1", "milestone": "DC-03",
              "run_id": frozen["run_id"], "started_at": datetime.now(UTC).isoformat(),
              "binding": frozen["binding"], "source_before": current_sources,
              "denominator": 6, "pairs": [], "independent_quality_review": "pending",
              "source_applicability": applicability}
    save(args.output / "report.json", report)
    try:
        for pair in frozen["pairs"]:
            if source_digests() != current_sources:
                raise ValueError("生产源码在比较执行期间发生变化")
            graph_path, graph_record = graph_records[pair["case_id"]]
            graph_before = read_execution(shop_database, data_database, graph_record["task_id"])
            if (graph_before["payload"] != DataQuestion.model_validate(
                    pair["graph"]["payload"]).model_dump(mode="json")
                    or graph_before["request_id"] != pair["graph"]["request_id"]
                    or graph_before["run_id"] != graph_record["result"]["run_id"]):
                raise ValueError("Graph真实任务没有绑定第一次输入身份")
            identity = identities["actors"][pair["graph"]["actor"]]
            if graph_before["actor_id"] != identity["actor_id"]:
                raise ValueError("Graph实际账号与比较账号不一致")
            async with httpx.AsyncClient(base_url=args.shop_url, timeout=300, trust_env=False) as client:
                logged_at = await login(client, identity)
                saved_graph = await invoke(client, "show", {"task_id": graph_record["task_id"]}, str(uuid4()))
                if saved_graph != graph_record["result"]:
                    raise ValueError("Graph原始回答回读内容发生变化")
                graph_refs = await references(client, saved_graph)
                hybrid_path = args.output / "hybrid" / f"{pair['case_id']}-1.json"
                hybrid_record = await hybrid_answer(args, client, pair, hybrid_path)
            hybrid_execution = read_execution(shop_database, data_database, hybrid_record["task_id"])
            graph_after = read_execution(shop_database, data_database, graph_record["task_id"])
            if graph_after != graph_before:
                raise ValueError("原Graph任务或费用在比较期间发生变化")
            comparison = {"case_id": pair["case_id"], "login_at": logged_at,
                "graph_record_path": str(graph_path.resolve()),
                "graph_record_sha256": sha256(graph_path.read_bytes()).hexdigest(),
                "hybrid_record_path": str(hybrid_path.resolve()),
                "hybrid_record_sha256": sha256(hybrid_path.read_bytes()).hexdigest(),
                "graph": {"record": graph_record, "execution": graph_before, "references_readback": graph_refs},
                "hybrid": {"record": hybrid_record, "execution": hybrid_execution},
                "conditions": pair_conditions(graph_record, hybrid_record, graph_before, hybrid_execution),
                "original_graph_unchanged": True, "independent_quality_review": "pending"}
            pair_path = args.output / "pairs" / f"{pair['case_id']}.json"
            save(pair_path, comparison)
            report["pairs"].append({"case_id": pair["case_id"], "path": str(pair_path.resolve()),
                "sha256": sha256(pair_path.read_bytes()).hexdigest(), "conditions": comparison["conditions"],
                "graph_status": graph_record["result"]["status"],
                "hybrid_status": hybrid_record["result"]["status"],
                "graph_elapsed_seconds": graph_record["elapsed_seconds"],
                "hybrid_elapsed_seconds": hybrid_record["elapsed_seconds"],
                "graph_spent_cny": graph_before["spent_cny"],
                "hybrid_spent_cny": hybrid_execution["spent_cny"],
                "graph_token_totals": graph_before["token_totals"],
                "hybrid_token_totals": hybrid_execution["token_totals"]})
            save(args.output / "report.json", report)
            print(json.dumps(report["pairs"][-1], ensure_ascii=False), flush=True)
            if hybrid_record.get("timeout") or hybrid_record["result"]["status"] in {
                    "unknown", "budget_exhausted", "failed", "cancelled"}:
                break
    finally:
        shop_database.dispose()
        data_database.dispose()
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["source_after"] = source_digests()
        report["source_stable"] = report["source_before"] == report["source_after"]
        report["executed_pairs"] = len(report["pairs"])
        report["status"] = "review_pending" if len(report["pairs"]) == 6 else "incomplete"
        report["script_sha256"] = sha256(Path(__file__).read_bytes()).hexdigest()
        save(args.output / "report.json", report)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, default=ROOT / "tests/fixtures/data_center_cases.json")
    parser.add_argument("--registry", type=Path, default=ROOT / ".local/data-center/sources/r4/sources.json")
    parser.add_argument("--graph-run", type=Path, required=True)
    parser.add_argument("--identities", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shop-url", default="http://127.0.0.1:8002")
    parser.add_argument("--data-database-url", default="postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--source-applicability", type=Path)
    args = parser.parse_args()
    frozen = prepare(args)
    if args.execute:
        asyncio.run(execute(args, frozen))
    else:
        print(json.dumps({"run_id": frozen["run_id"], "planned_pairs": 6,
                          "new_model_calls": 0, "status": "prepared"}))


if __name__ == "__main__":
    main()
