import asyncio
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from data_center_protocol import ModelInvocation, ModelReply
from sqlalchemy import create_engine, text

from shop.config import Settings

OUTPUT = Path(".scratch/evidence/dc-model-reuse-http-20261004-r1")
SPEC_DEFAULT = Path(".scratch/evidence/dc03-live-20261004-r2/spec.json")
BASELINE_DEFAULT = Path(".scratch/evidence/dc-persistence-audit-20261004-r1/baseline-20261004T144528Z.json")
GRAPH_TASK_ID = "52688e12-15bb-4c71-a8bd-ba4852d2e2d3"
GRAPH_RUN_ID = "1436a6f2-41fc-4f16-bf8b-658e4d3c7aaf"


def canonical_sha256(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def provider_state(database):
    with database.connect() as connection, connection.begin():
        connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        rows = connection.execute(text(
            "SELECT id, md5(row_to_json(p)::text) AS record_md5 FROM provider_requests AS p ORDER BY id"
        )).all()
        material = "\n".join(f"{row.id}:{row.record_md5}" for row in rows)
        status_rows = connection.execute(text(
            "SELECT status, count(*) AS count, sum(charged_cny) AS paid_cny, "
            "sum(CASE WHEN charged_cny IS NULL THEN reservation_cny ELSE 0 END) AS reserved_cny "
            "FROM provider_requests GROUP BY status ORDER BY status"
        )).mappings().all()
        windows = connection.execute(text(
            "SELECT id, limit_cny, active, include_open_reservations, "
            "md5(row_to_json(w)::text) AS record_md5 FROM integration_budget_windows AS w "
            "WHERE active IS TRUE ORDER BY id"
        )).mappings().all()
        if len(windows) != 1:
            raise AssertionError(f"活动费用窗口数量为{len(windows)}")
        window = windows[0]
        totals = connection.execute(text(
            "SELECT coalesce(sum(charged_cny) FILTER "
            "(WHERE price_basis->>'integration_run_id'=:window_id), 0) AS paid_cny, "
            "coalesce(sum(reservation_cny) FILTER (WHERE charged_cny IS NULL), 0) AS reserved_cny, "
            "count(*) FILTER (WHERE charged_cny IS NULL) AS open_count "
            "FROM provider_requests"
        ), {"window_id": window["id"]}).mappings().one()
    return {
        "provider_request_count": len(rows),
        "provider_records_sha256": hashlib.sha256(material.encode()).hexdigest(),
        "status_rows": [{key: str(value) if value is not None else None for key, value in row.items()}
                        for row in status_rows],
        "active_window": {"id": window["id"], "limit_cny": str(window["limit_cny"]),
                          "record_md5": window["record_md5"]},
        "window_paid_cny": str(totals["paid_cny"]),
        "all_open_reservations_cny": str(totals["reserved_cny"]),
        "open_request_count": totals["open_count"],
    }


def read_live_rows(database, actor_id):
    with database.connect() as connection, connection.begin():
        connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        original = connection.execute(text(
            "SELECT p.id, p.run_id, p.step_key, p.status, p.model, p.request_body, "
            "p.response_body, p.usage, p.charged_cny, b.task_id, b.actor_id, b.source_ids "
            "FROM provider_requests AS p JOIN data_center_task_bindings AS b ON b.run_id=p.run_id "
            "WHERE p.run_id=:run_id AND p.status='completed' "
            "AND p.step_key LIKE 'graph-embedding:%' AND b.actor_id=:actor_id "
            "ORDER BY p.created_at, p.id LIMIT 1"
        ), {"run_id": GRAPH_RUN_ID, "actor_id": actor_id}).mappings().one()
        wrong_completed = connection.execute(text(
            "SELECT p.id, p.run_id, p.step_key, p.status, p.model, p.request_body, "
            "p.response_body, p.usage, p.charged_cny, b.task_id, b.actor_id, b.source_ids "
            "FROM provider_requests AS p JOIN data_center_task_bindings AS b ON b.run_id=p.run_id "
            "WHERE p.run_id=:run_id AND p.status='completed' "
            "AND p.step_key LIKE 'graph-embedding:%' AND p.id<>:original_id "
            "AND b.actor_id=:actor_id ORDER BY p.created_at, p.id LIMIT 1"
        ), {"run_id": GRAPH_RUN_ID, "original_id": original["id"],
            "actor_id": actor_id}).mappings().one()
        reserved = connection.execute(text(
            "SELECT p.id, p.run_id, p.step_key, p.status, p.model, p.request_body, "
            "p.response_body, p.usage, p.charged_cny, b.task_id, b.actor_id, b.source_ids "
            "FROM provider_requests AS p JOIN data_center_task_bindings AS b ON b.run_id=p.run_id "
            "WHERE p.run_id=:run_id AND p.status='reserved' "
            "AND p.step_key LIKE 'graph-embedding:%' AND b.actor_id=:actor_id "
            "ORDER BY p.created_at, p.id LIMIT 1"
        ), {"run_id": GRAPH_RUN_ID, "actor_id": actor_id}).mappings().one()
        unknown = connection.execute(text(
            "SELECT p.id, p.run_id, p.step_key, p.status, p.model, b.actor_id, b.source_ids "
            "FROM provider_requests AS p JOIN data_center_task_bindings AS b ON b.run_id=p.run_id "
            "WHERE p.status='unknown' AND p.price_basis->>'verification_scope'='data_center' "
            "AND b.actor_id=:actor_id ORDER BY p.created_at, p.id LIMIT 1"
        ), {"actor_id": actor_id}).mappings().one()
    return tuple(dict(row) for row in (original, wrong_completed, reserved, unknown))


def invocation_for(row, task_id, run_id, source_ids, *, step_key=None, texts=None):
    return ModelInvocation(
        task_id=task_id,
        run_id=run_id,
        step_key=step_key or row["step_key"],
        kind="embedding",
        model=row["model"],
        texts=list(row["request_body"]["input"] if texts is None else texts),
        source_ids=list(source_ids),
    )


class LiveHostClient:
    def __init__(self, host_url):
        settings = Settings()
        token = settings.get_data_center_service_token()
        self.client = httpx.AsyncClient(
            base_url=host_url.rstrip("/") + "/api/data-center/host/",
            headers={"X-Data-Center-Service-Token": token},
            timeout=100,
            trust_env=False,
        )

    async def login(self, identities, role, request_id=None):
        actor = identities["actors"][role]
        request_id = request_id or str(uuid4())
        response = await self.client.post("login", json={
            "username": actor["username"],
            "password": Path(actor["password_file"]).read_text().strip(),
            "request_id": request_id,
        })
        assert response.status_code == 200, response.status_code
        result = response.json()
        assert result["actor_id"] == actor["actor_id"]
        return {"actor_id": result["actor_id"], "credential": result["credential"],
                "request_id": request_id}

    @staticmethod
    def headers(context):
        return {
            "Authorization": "Bearer " + context["credential"],
            "X-Shop-Actor": context["actor_id"],
            "X-Request-ID": context["request_id"],
        }

    async def post(self, path, body, context):
        return await self.client.post(path, json=body, headers=self.headers(context))

    async def identity(self, context):
        response = await self.post("identity", {}, context)
        assert response.status_code == 200 and response.json()["actor_id"] == context["actor_id"]

    async def close(self):
        await self.client.aclose()


async def bind_task(host, context, source_ids, case_name):
    task_id, run_id = str(uuid4()), str(uuid4())
    response = await host.post("tasks/bind", {
        "task_id": task_id,
        "run_id": run_id,
        "operation": "build_graph",
        "source_ids": source_ids,
        "payload": {"live_check": case_name, "task_id": task_id, "run_id": run_id},
    }, context)
    assert response.status_code == 200, response.status_code
    return {"task_id": task_id, "run_id": run_id}


async def independent_readback(definition, output_path):
    spec = json.loads(Path(definition["spec_path"]).read_text())
    identities = json.loads(Path(spec["identities_path"]).read_text())
    host = LiveHostClient(spec["host_url"])
    try:
        context = await host.login(identities, definition["actor_role"], definition["request_id"])
        lookup = await host.post("models/lookup", {
            "task_id": definition["task_id"], "step_key": definition["step_key"],
        }, context)
        usage = await host.post("tasks/usage", {"task_id": definition["task_id"]}, context)
        assert lookup.status_code == usage.status_code == 200
        if lookup.json() is None:
            raise AssertionError("独立进程没有读取到已保存的模型复用结果")
        reply = ModelReply.model_validate_json(lookup.content)
        output = {
            "task_id": definition["task_id"],
            "run_id": definition["run_id"],
            "lookup_http_status": lookup.status_code,
            "usage_http_status": usage.status_code,
            "reply": {"request_id": reply.request_id, "status": reply.status,
                      "response_sha256": canonical_sha256(reply.response)},
            "usage": usage.json(),
        }
        Path(output_path).write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    finally:
        await host.close()


def run_readback_process(definition, input_path, output_path):
    input_path.write_text(json.dumps(definition, ensure_ascii=False, indent=2) + "\n")
    completed = subprocess.run(
        [sys.executable, "-P", str(Path(__file__).resolve()), "--readback-worker",
         str(input_path.resolve()), str(output_path.resolve())],
        cwd=Path.cwd(), capture_output=True, text=True, timeout=60, check=False,
    )
    return {"returncode": completed.returncode, "stdout_sha256": hashlib.sha256(
        completed.stdout.encode()).hexdigest(), "stderr_sha256": hashlib.sha256(
        completed.stderr.encode()).hexdigest()}


@pytest.mark.asyncio
async def test_real_http_model_reuse_is_parallel_persistent_authorized_and_zero_cost():
    spec_path = Path(os.environ.get("DC_MODEL_REUSE_HTTP_SPEC", SPEC_DEFAULT))
    baseline_path = Path(os.environ.get("DC_MODEL_REUSE_HTTP_BASELINE", BASELINE_DEFAULT))
    if not spec_path.is_file() or not baseline_path.is_file():
        pytest.skip("需要指定真实数据中台配置与只读基线")

    spec = json.loads(spec_path.read_text())
    baseline = json.loads(baseline_path.read_text())
    identities = json.loads(Path(spec["identities_path"]).read_text())
    actor_id = baseline["identity"]["actor_id"]
    assert baseline["identity"]["task_id"] == GRAPH_TASK_ID
    assert baseline["identity"]["run_id"] == GRAPH_RUN_ID
    assert actor_id == identities["actors"]["admin"]["actor_id"]

    database = create_engine(Settings().database_url, pool_pre_ping=True)
    before = provider_state(database)
    original, wrong_completed, reserved, unknown = read_live_rows(database, actor_id)
    assert original["status"] == "completed" and original["response_body"] is not None
    assert wrong_completed["step_key"] != original["step_key"]
    assert wrong_completed["request_body"] != original["request_body"]
    assert reserved["status"] == "reserved" and reserved["response_body"] is None
    assert unknown["status"] == "unknown"
    source_ids = list(original["source_ids"])
    assert len(source_ids) > 1 and len(set(source_ids)) == len(source_ids)
    host = LiveHostClient(spec["host_url"])
    report = {
        "schema": "dc_model_reuse_http_live.v1",
        "started_at": datetime.now(UTC).isoformat(),
        "spec_path": str(spec_path.resolve()),
        "baseline_path": str(baseline_path.resolve()),
        "host_url": spec["host_url"],
        "actor_id": actor_id,
        "source_ids": source_ids,
        "original": {"request_id": original["id"], "task_id": original["task_id"],
                     "run_id": original["run_id"], "step_key": original["step_key"],
                     "model": original["model"], "input_count": len(original["request_body"]["input"]),
                     "request_body_sha256": canonical_sha256(original["request_body"]),
                     "response_body_sha256": canonical_sha256(original["response_body"]),
                     "charged_cny": str(original["charged_cny"])},
        "negative_source_requests": {
            "wrong_completed_id": {"request_id": wrong_completed["id"],
                                    "step_key": wrong_completed["step_key"],
                                    "status": wrong_completed["status"]},
            "reserved_id": {"request_id": reserved["id"], "step_key": reserved["step_key"],
                             "status": reserved["status"]},
            "unknown_id": {"request_id": unknown["id"], "step_key": unknown["step_key"],
                            "status": unknown["status"], "run_id": unknown["run_id"]},
        },
        "provider_before": before,
        "checks": {},
    }
    actor_context = await host.login(identities, "admin")
    success = False
    try:
        await host.identity(actor_context)
        bound_one, bound_two = await asyncio.gather(
            bind_task(host, actor_context, source_ids, "parallel-run-one"),
            bind_task(host, actor_context, source_ids, "parallel-run-two"),
        )
        invocation_one = invocation_for(original, bound_one["task_id"], bound_one["run_id"], source_ids)
        invocation_two = invocation_for(original, bound_two["task_id"], bound_two["run_id"], source_ids)
        bodies = [
            {"invocation": invocation_one.model_dump(mode="json"), "original_request_id": original["id"]},
            {"invocation": invocation_two.model_dump(mode="json"), "original_request_id": original["id"]},
        ]
        reuse_responses = await asyncio.gather(
            host.post("models/reuse", bodies[0], actor_context),
            host.post("models/reuse", bodies[1], actor_context),
        )
        assert [response.status_code for response in reuse_responses] == [200, 200]
        replies = [ModelReply.model_validate_json(response.content) for response in reuse_responses]
        for reply in replies:
            assert reply.status == "completed" and reply.request_id == original["id"]
            assert canonical_sha256(reply.response) == canonical_sha256(original["response_body"])
            assert reply.reserved_cny == 0
        usages = []
        for binding in (bound_one, bound_two):
            response = await host.post("tasks/usage", {"task_id": binding["task_id"]}, actor_context)
            assert response.status_code == 200
            usage = response.json()
            assert usage["run_id"] == binding["run_id"] and usage["requests"] == []
            assert usage["spent_cny"] == usage["reserved_cny"] == "0"
            assert len(usage["reused_requests"]) == 1
            assert usage["reused_requests"][0]["request_id"] == original["id"]
            assert usage["reused_requests"][0]["original_run_id"] == original["run_id"]
            usages.append(usage)
        report["checks"]["parallel_runs"] = {
            "bindings": [bound_one, bound_two],
            "http_statuses": [response.status_code for response in reuse_responses],
            "reply_request_ids": [reply.request_id for reply in replies],
            "usage": usages,
        }

        readback_input = OUTPUT / f"readback-input-{bound_one['task_id']}.json"
        readback_output = OUTPUT / f"readback-output-{bound_one['task_id']}.json"
        OUTPUT.mkdir(parents=True, exist_ok=True)
        child = run_readback_process({
            "spec_path": str(spec_path.resolve()), "actor_role": "admin",
            "request_id": actor_context["request_id"], "task_id": bound_one["task_id"],
            "run_id": bound_one["run_id"], "step_key": invocation_one.step_key,
        }, readback_input, readback_output)
        assert child["returncode"] == 0
        readback = json.loads(readback_output.read_text())
        assert readback["task_id"] == bound_one["task_id"]
        assert readback["run_id"] == bound_one["run_id"]
        assert readback["reply"]["request_id"] == original["id"]
        assert readback["reply"]["response_sha256"] == canonical_sha256(original["response_body"])
        assert readback["usage"]["requests"] == []
        assert readback["usage"]["spent_cny"] == readback["usage"]["reserved_cny"] == "0"
        report["checks"]["independent_process_readback"] = {"process": child, "result": readback}

        missing_id_binding = await bind_task(host, actor_context, source_ids, "missing-request-id")
        missing_id_invocation = invocation_for(original, missing_id_binding["task_id"],
                                               missing_id_binding["run_id"], source_ids)
        missing_response = await host.post("models/reuse", {
            "invocation": missing_id_invocation.model_dump(mode="json"),
        }, actor_context)
        assert missing_response.status_code == 422

        error_id_binding = await bind_task(host, actor_context, source_ids, "invalid-request-id")
        error_id_invocation = invocation_for(original, error_id_binding["task_id"],
                                             error_id_binding["run_id"], source_ids)
        malformed_response = await host.post("models/reuse", {
            "invocation": error_id_invocation.model_dump(mode="json"),
            "original_request_id": "not-a-uuid",
        }, actor_context)
        assert malformed_response.status_code == 422
        wrong_id_response = await host.post("models/reuse", {
            "invocation": error_id_invocation.model_dump(mode="json"),
            "original_request_id": wrong_completed["id"],
        }, actor_context)
        assert wrong_id_response.status_code == 422
        unknown_response = await host.post("models/reuse", {
            "invocation": error_id_invocation.model_dump(mode="json"),
            "original_request_id": unknown["id"],
        }, actor_context)
        assert unknown_response.status_code == 422
        report["checks"]["request_id_rejections"] = {
            "missing_id_http_status": missing_response.status_code,
            "malformed_id_http_status": malformed_response.status_code,
            "wrong_completed_id_http_status": wrong_id_response.status_code,
            "unknown_id_http_status": unknown_response.status_code,
        }

        reserved_binding = await bind_task(host, actor_context, source_ids, "reserved-request-id")
        reserved_invocation = invocation_for(reserved, reserved_binding["task_id"],
                                             reserved_binding["run_id"], source_ids)
        reserved_response = await host.post("models/reuse", {
            "invocation": reserved_invocation.model_dump(mode="json"),
            "original_request_id": reserved["id"],
        }, actor_context)
        assert reserved_response.status_code == 422
        report["checks"]["reserved_id_rejection"] = {"http_status": reserved_response.status_code}

        changed_source_ids = source_ids[:-1]
        source_binding = await bind_task(host, actor_context, changed_source_ids, "changed-source-set")
        source_invocation = invocation_for(original, source_binding["task_id"],
                                           source_binding["run_id"], changed_source_ids)
        source_response = await host.post("models/reuse", {
            "invocation": source_invocation.model_dump(mode="json"),
            "original_request_id": original["id"],
        }, actor_context)
        assert source_response.status_code == 422

        changed_texts = list(original["request_body"]["input"])
        changed_texts[0] += " "
        content_binding = await bind_task(host, actor_context, source_ids, "changed-request-body")
        content_invocation = invocation_for(original, content_binding["task_id"],
                                            content_binding["run_id"], source_ids, texts=changed_texts)
        content_response = await host.post("models/reuse", {
            "invocation": content_invocation.model_dump(mode="json"),
            "original_request_id": original["id"],
        }, actor_context)
        assert content_response.status_code == 422

        changed_step_binding = await bind_task(host, actor_context, source_ids, "changed-step-key")
        changed_step_invocation = invocation_for(original, changed_step_binding["task_id"],
            changed_step_binding["run_id"], source_ids, step_key=original["step_key"] + ":changed")
        step_response = await host.post("models/reuse", {
            "invocation": changed_step_invocation.model_dump(mode="json"),
            "original_request_id": original["id"],
        }, actor_context)
        assert step_response.status_code == 422
        report["checks"]["scope_and_request_identity_rejections"] = {
            "changed_source_set_http_status": source_response.status_code,
            "changed_request_body_http_status": content_response.status_code,
            "changed_step_key_http_status": step_response.status_code,
        }

        other_context = await host.login(identities, "other")
        cross_actor_response = await host.post("models/reuse", bodies[0], other_context)
        assert cross_actor_response.status_code == 403
        report["checks"]["cross_actor_rejection"] = {
            "bound_actor_id": actor_id,
            "request_actor_id": other_context["actor_id"],
            "http_status": cross_actor_response.status_code,
        }

        cancel_binding = await bind_task(host, actor_context, source_ids, "cancel-after-reuse")
        cancel_invocation = invocation_for(original, cancel_binding["task_id"],
                                           cancel_binding["run_id"], source_ids)
        cancel_reuse = await host.post("models/reuse", {
            "invocation": cancel_invocation.model_dump(mode="json"),
            "original_request_id": original["id"],
        }, actor_context)
        assert cancel_reuse.status_code == 200
        cancel_response = await host.post("tasks/cancel", {"task_id": cancel_binding["task_id"]}, actor_context)
        assert cancel_response.status_code == 200
        cancelled_lookup = await host.post("models/lookup", {
            "task_id": cancel_binding["task_id"], "step_key": cancel_invocation.step_key,
        }, actor_context)
        cancelled_reuse = await host.post("models/reuse", {
            "invocation": cancel_invocation.model_dump(mode="json"),
            "original_request_id": original["id"],
        }, actor_context)
        assert cancelled_lookup.status_code == cancelled_reuse.status_code == 403
        report["checks"]["cancelled_task_rejections"] = {
            "reuse_before_cancel_http_status": cancel_reuse.status_code,
            "cancel_http_status": cancel_response.status_code,
            "lookup_after_cancel_http_status": cancelled_lookup.status_code,
            "reuse_after_cancel_http_status": cancelled_reuse.status_code,
        }
        success = True
    finally:
        after = provider_state(database)
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["provider_after"] = after
        report["provider_records_unchanged"] = (
            before["provider_request_count"] == after["provider_request_count"]
            and before["provider_records_sha256"] == after["provider_records_sha256"]
        )
        report["fees_unchanged"] = (
            before["active_window"] == after["active_window"]
            and before["window_paid_cny"] == after["window_paid_cny"]
            and before["all_open_reservations_cny"] == after["all_open_reservations_cny"]
            and before["open_request_count"] == after["open_request_count"]
        )
        report["status"] = "passed" if success and report["provider_records_unchanged"] and report["fees_unchanged"] else "incomplete"
        OUTPUT.mkdir(parents=True, exist_ok=True)
        report_path = OUTPUT / f"report-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid4()}.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
        await host.close()
        database.dispose()
    assert report["provider_records_unchanged"] and report["fees_unchanged"]
    assert report["status"] == "passed"
    print(f"evidence_report={report_path.resolve()}")


if __name__ == "__main__" and len(sys.argv) == 4 and sys.argv[1] == "--readback-worker":
    asyncio.run(independent_readback(json.loads(Path(sys.argv[2]).read_text()), sys.argv[3]))
