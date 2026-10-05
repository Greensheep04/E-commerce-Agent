import argparse
import asyncio
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import monotonic
from uuid import uuid4

import httpx
from integration_protocol import OperationResult

from shop.data_center_adapter import DataCenterAdapter

ROOT = Path(__file__).resolve().parents[1]


def source_digests():
    directories = (
        ROOT / "data-center/src", ROOT / "packages/data-center-protocol/src",
        ROOT / "src/shop", ROOT / "agent-cluster/src", ROOT / "agent-hub/src",
    )
    paths = {path for directory in directories for path in directory.rglob("*")
             if path.is_file() and path.suffix in {".py", ".sql", ".yml", ".yaml"}}
    paths.update(ROOT / name for name in (
        "pyproject.toml", "uv.lock", "data-center/pyproject.toml", "data-center/uv.lock",
        "packages/data-center-protocol/pyproject.toml",
    ))
    return {str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
            for path in sorted(paths)}


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def source_inputs(fixture, registry):
    sources = {item["source_key"]: item["source_id"] for item in registry["sources"]}
    searches = {item["case_id"]: item["source_keys"] for item in fixture["document_search_cases"]}
    prepared = []
    for case in fixture["cases"]:
        keys = searches.get(case["id"], [item["source_key"] for item in case.get("source_evidence", [])
                                       if "source_key" in item])
        selected = [sources[key] for key in keys if key in fixture["document_ingest_source_keys"]]
        if case["category"] in {"analytics", "refusal"} or case["id"] == "REL-06":
            selected.append("microsoft/adventureworks")
        if case["id"] == "REF-06":
            selected.append(sources["project-shop:data-center-milestone"])
        if not selected:
            raise ValueError(f"固定场景缺少登记来源：{case['id']}")
        profile = case.get("actor_profile", "admin")
        actor = "assessment" if profile == "assessment-employee" else profile
        if actor == "assessment-manager":
            actor = "assessment-manager"
        for attempt in range(1, 4):
            prepared.append({"case_id": case["id"], "category": case["category"],
                             "attempt": attempt, "actor": actor, "expected": case,
                             "request_id": str(uuid4()), "payload": {
                                 "question": case["query"], "source_ids": sorted(set(selected)),
                                 "retrieval_mode": "graph" if case["category"] == "cross_document" else "hybrid"}})
    return prepared


def operation_response(response):
    if response.is_error:
        text = response.text
        return {"http_status": response.status_code, "error": {
            "content_type": response.headers.get("content-type", ""),
            "body_sha256": sha256(response.content).hexdigest(),
            "body_excerpt": text[:2048], "body_truncated": len(text) > 2048,
        }}
    return response.json()


async def invoke(client, action, payload, request_id):
    response = await client.post("/api/modules/data_center/operations/" + action, json={
        "request_id": request_id, "version": "1", "payload": payload, "upstream": []})
    return operation_response(response)


async def execute(args):
    fixture_path = ROOT / "tests/fixtures/data_center_cases.json"
    registry_path = args.registry.resolve()
    fixture, registry = (json.loads(path.read_text()) for path in (fixture_path, registry_path))
    if args.case_ids and not set(args.case_ids).issubset({case["id"] for case in fixture["cases"]}):
        raise ValueError("选中的评价场景没有在固定输入中登记")
    identities = json.loads(args.identities.read_text())
    if identities["window"] != "dc-live-20261004":
        raise ValueError("正式评价需要已批准的活动费用窗口")
    args.output.mkdir(parents=True, exist_ok=True)
    inputs_path = args.output / "inputs.json"
    digests = {str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
               for path in (fixture_path, registry_path)}
    if inputs_path.exists():
        frozen = json.loads(inputs_path.read_text())
        if frozen["input_sha256"] != digests:
            raise ValueError("正式评价恢复时固定来源或输入发生变化")
    else:
        attempts = source_inputs(fixture, registry)
        reused_inputs = None
        if args.reuse_from is not None:
            previous_path = args.reuse_from / "inputs.json"
            previous = json.loads(previous_path.read_text())
            if previous["input_sha256"] != digests:
                raise ValueError("复用证据的固定输入及来源摘要与当前批次不一致")
            selected = {(item["case_id"], item["attempt"]): item for item in previous["attempts"]}
            for item in attempts:
                if item["category"] not in args.reuse_categories:
                    continue
                original = selected[item["case_id"], item["attempt"]]
                if any(item[key] != original[key] for key in ("actor", "expected", "payload", "category")):
                    raise ValueError("复用证据的身份、预期或请求内容不一致")
                record_path = args.reuse_from / "attempts" / f"{item['case_id']}-{item['attempt']}.json"
                record = json.loads(record_path.read_text())
                if (not record.get("finished_at") or not record.get("source_stable")
                        or record.get("timeout") or record.get("error")
                        or record.get("result", {}).get("status") not in {"completed", "partial", "DATA_BLOCKED"}):
                    raise ValueError("复用证据缺少稳定源码及完整终态")
                item["request_id"] = original["request_id"]
                item["generation_origin"] = {"run_id": previous["run_id"],
                    "record_path": str(record_path.resolve()),
                    "record_sha256": sha256(record_path.read_bytes()).hexdigest(),
                    "task_id": record["task_id"], "result": record["result"]}
            reused_inputs = {"path": str(previous_path.resolve()),
                             "sha256": sha256(previous_path.read_bytes()).hexdigest(),
                             "categories": args.reuse_categories}
        frozen = {"run_id": str(uuid4()), "created_at": datetime.now(UTC).isoformat(),
                  "input_sha256": digests, "attempts": attempts, "reused_inputs": reused_inputs,
                  "config": {"model": "deepseek-flash", "thinking": "high", "output_tokens": 16384,
                             "shop_url": args.shop_url, "timeout_seconds": args.timeout}}
        save(inputs_path, frozen)
    clients = {}
    segment_id = str(uuid4())
    segment = {"segment_id": segment_id, "started_at": datetime.now(UTC).isoformat(),
               "source_before": source_digests(), "categories": args.categories}
    save(args.output / "segments" / f"{segment_id}.json", segment)
    try:
        for name in sorted({item["actor"] for item in frozen["attempts"]}):
            identity = identities["actors"][name]
            client = httpx.AsyncClient(base_url=args.shop_url, timeout=300, trust_env=False)
            clients[name] = client
            response = await client.post("/api/login", json={"username": identity["username"],
                "password": Path(identity["password_file"]).read_text().strip()})
            response.raise_for_status()
        for item in frozen["attempts"]:
            if args.categories and item["category"] not in args.categories:
                continue
            if args.case_ids and item["case_id"] not in args.case_ids:
                continue
            path = args.output / "attempts" / f"{item['case_id']}-{item['attempt']}.json"
            if path.exists() and json.loads(path.read_text()).get("finished_at"):
                continue
            client = clients[item["actor"]]
            observation = {"input": item, "segment_id": segment_id,
                           "source_before": source_digests(),
                           "started_at": datetime.now(UTC).isoformat(), "observations": []}
            started = monotonic()
            save(path, observation)
            try:
                result = await invoke(client, "ask", item["payload"], item["request_id"])
                observation["observations"].append(result)
                save(path, observation)
                task_id = (DataCenterAdapter.task_id_from_result(OperationResult.model_validate(result))
                           if "http_status" not in result else None)
                if task_id is not None:
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
                if "generation_origin" in item:
                    origin = item["generation_origin"]
                    observation["reused_generation"] = (task_id == origin["task_id"]
                                                        and result == origin["result"])
                    if not observation["reused_generation"]:
                        raise ValueError("复用检查的任务编号或原始回答发生变化")
                if result.get("status") in {"completed", "partial"}:
                    references = []
                    for reference in result["data"]["evidence"]:
                        references.append(await invoke(client, "reference", reference, str(uuid4())))
                    observation["references"] = references
                if result.get("status") == "budget_exhausted":
                    observation["budget_exhausted"] = True
            except (httpx.HTTPError, ValueError, KeyError, TypeError, TimeoutError) as error:
                observation["error"] = {"type": type(error).__name__, "message": str(error)}
            observation["finished_at"] = datetime.now(UTC).isoformat()
            observation["elapsed_seconds"] = monotonic() - started
            observation["source_after"] = source_digests()
            observation["source_stable"] = observation["source_before"] == observation["source_after"]
            save(path, observation)
            print(json.dumps({"case": item["case_id"], "attempt": item["attempt"],
                              "status": observation.get("result", {}).get("status", "failed"),
                              "elapsed_seconds": observation["elapsed_seconds"]}), flush=True)
            if observation.get("budget_exhausted"):
                break
    finally:
        for client in clients.values():
            await client.aclose()
        segment["finished_at"] = datetime.now(UTC).isoformat()
        segment["source_after"] = source_digests()
        segment["source_stable"] = segment["source_before"] == segment["source_after"]
        save(args.output / "segments" / f"{segment_id}.json", segment)
        paths = sorted((args.output / "attempts").glob("*.json"))
        save(args.output / "execution-report.json", {"run_id": frozen["run_id"],
            "planned": 90, "executed": len(paths), "attempts": [str(path) for path in paths],
            "independent_quality_review": "pending", "input_sha256": digests,
            "source_sha256": source_digests(), "segments": [str(path) for path in
                sorted((args.output / "segments").glob("*.json"))]})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shop-url", default="http://127.0.0.1:8002")
    parser.add_argument("--identities", type=Path, required=True)
    parser.add_argument("--registry", type=Path, default=ROOT / ".local/data-center/sources/r3/sources.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--categories", nargs="*")
    parser.add_argument("--case-ids", nargs="*")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--reuse-from", type=Path)
    parser.add_argument("--reuse-categories", nargs="*", default=[])
    args = parser.parse_args()
    if bool(args.reuse_from) != bool(args.reuse_categories):
        parser.error("复用证据必须同时指定来源批次和类别")
    asyncio.run(execute(args))


if __name__ == "__main__":
    main()
