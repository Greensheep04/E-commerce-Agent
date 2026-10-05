import argparse
import asyncio
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from integration_protocol import OperationResult
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.integration import ShopIntegration
from shop.runtime_models import ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
ATTEMPT = ROOT / ".scratch/evidence/dc-formal-20261004-r6/attempts/DOC-01-1.json"
IDENTITIES = ROOT / ".scratch/evidence/dc02-host-20261004-r3/identities.json"


class TransportTrace:
    def __init__(self):
        self.events = []

    async def __call__(self, name, info):
        parts = name.split(".")
        if len(parts) < 3 or parts[-2] not in {"connect_tcp", "close"}:
            return
        address = None
        if "host" in info and "port" in info:
            host = info["host"]
            address = {"host": host.decode() if isinstance(host, bytes) else host,
                       "port": int(info["port"])}
        self.events.append({
            "at": datetime.now(UTC).isoformat(),
            "event": name,
            "address": address,
        })


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def digest(value):
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return sha256(encoded).hexdigest()


def provider_requests(engine, run_id):
    with Session(engine) as session:
        rows = session.scalars(select(ProviderRequest).where(
            ProviderRequest.run_id == run_id).order_by(ProviderRequest.id)).all()
        return [{"id": row.id, "status": row.status, "usage": row.usage,
                 "charged_cny": None if row.charged_cny is None else str(row.charged_cny),
                 "reservation_cny": str(row.reservation_cny)} for row in rows]


def source_sha256():
    paths = ("src/shop/data_center_adapter.py", "src/shop/integration.py",
             "scripts/verify_data_center_transport.py")
    return {name: sha256((ROOT / name).read_bytes()).hexdigest() for name in paths}


def task_reference_summary(result):
    return [{"module_id": ref.module_id, "resource_type": ref.resource_type,
             "resource_id": ref.resource_id, "version": ref.version, "sha256": ref.sha256}
            for ref in result.resources]


def opened_tcp_connection(events):
    return any(event["event"].endswith(".connect_tcp.started") for event in events)


async def verify(output, include_service_expiry):
    record = json.loads(ATTEMPT.read_text())
    expected = OperationResult.model_validate(record["result"])
    task_id = record["task_id"]
    if expected.status != "completed" or expected.data.get("task_id") != task_id:
        raise ValueError("固定完成任务的状态或编号与原始记录不一致")
    run_id = expected.run_id
    if not run_id or expected.data.get("run_id") != run_id:
        raise ValueError("固定任务的外层与结果 run_id 不一致")
    identities = json.loads(IDENTITIES.read_text())
    identity = identities["actors"]["admin"]
    password = Path(identity["password_file"]).read_text().strip()
    settings = Settings(data_center_url="http://127.0.0.1:8100")
    source_before = source_sha256()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    integration = ShopIntegration(settings)
    adapter = integration.adapters["data_center"]
    trace = TransportTrace()

    async def add_trace(request):
        request.extensions["trace"] = trace

    adapter.client.event_hooks["request"].append(add_trace)
    observations = []
    failure = None
    try:
        context = integration.login(identity["username"], password, str(uuid4()))
        before = provider_requests(engine, run_id)
        if not before:
            raise ValueError("固定 run_id 没有可核对的 ProviderRequest 记录")

        async def show(label):
            start = len(trace.events)
            result = await adapter.invoke("show", adapter.input_models["show"](task_id=task_id),
                                          context.model_copy(update={"request_id": str(uuid4())}))
            value = result.model_dump(mode="json")
            if value != record["result"]:
                raise AssertionError(f"{label} 返回结果与固定原始结果不同")
            if result.run_id != run_id or result.data.get("task_id") != task_id:
                raise AssertionError(f"{label} 改变了原 task_id 或 run_id")
            refs = task_reference_summary(result)
            observations.append({"label": label, "at": datetime.now(UTC).isoformat(),
                                 "result_sha256": digest(value), "status": result.status,
                                 "task_id": result.data["task_id"], "run_id": result.run_id,
                                 "references": refs,
                                 "trace_events": trace.events[start:]})

        await show("initial")
        await asyncio.sleep(0.2)
        await show("short_interval")
        if opened_tcp_connection(observations[1]["trace_events"]):
            failure = "短间隔 show 建立了新的 TCP 连接"

        await asyncio.sleep(2.2)
        await show("after_2_2_seconds")
        if not opened_tcp_connection(observations[2]["trace_events"]):
            failure = "等待2.2秒后没有建立新的 TCP 连接"

        if include_service_expiry:
            await asyncio.sleep(5.2)
            await show("after_5_2_seconds")
            if not opened_tcp_connection(observations[3]["trace_events"]):
                failure = "等待5.2秒后没有建立新的 TCP 连接"

        after = provider_requests(engine, run_id)
        if after != before:
            failure = "show 检查改变了固定 run_id 的 ProviderRequest 编号、状态、用量或费用"
        source_after = source_sha256()
        source_stable = source_before == source_after
        if not source_stable:
            failure = "检查期间相关源码摘要发生变化"
        report = {"status": "failed" if failure else "passed",
                  "failure": failure,
                  "captured_at": datetime.now(UTC).isoformat(),
                  "attempt_path": str(ATTEMPT.relative_to(ROOT)),
                  "attempt_sha256": sha256(ATTEMPT.read_bytes()).hexdigest(),
                  "task_id": task_id, "run_id": run_id,
                  "result_sha256": digest(record["result"]),
                  "checks": ["real_adapter_show", "short_interval_tcp_reuse",
                             "2.2_second_connection_expiry", "fixed_result_and_reference_identity",
                             "provider_request_no_change"],
                  "include_service_expiry": include_service_expiry,
                  "service_expiry_5_2_seconds_executed": include_service_expiry,
                  "timing_seconds": {"short_interval": 0.2, "client_expiry": 2.0,
                                     "service_idle_expiry": 5.0, "client_probe": 2.2,
                                     "service_probe": 5.2},
                  "close_trace_callback_available": False,
                  "close_trace_callback_note":
                      "httpcore close trace uses request=None, so HTTPX request extensions cannot receive it",
                  "model_requests": 0,
                  "source_sha256_before": source_before,
                  "source_sha256_after": source_after,
                  "source_stable": source_stable,
                  "provider_requests_before": before, "provider_requests_after": after,
                  "observations": observations,
                  "transport_events": trace.events}
        save(output / "report.json", report)
        print(json.dumps({"status": report["status"], "failure": failure,
                          "task_id": task_id, "run_id": run_id,
                          "transport_events": len(trace.events)}, ensure_ascii=False))
        if failure:
            raise AssertionError(failure)
    finally:
        await integration.shutdown()
        engine.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-service-expiry", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("原始证据目录已经存在")
    asyncio.run(verify(args.output, args.include_service_expiry))


if __name__ == "__main__":
    main()
