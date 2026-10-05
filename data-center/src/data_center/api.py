import json
import logging
from contextlib import asynccontextmanager
from hashlib import sha256
from typing import Annotated

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from integration_protocol import CallContext, OperationResult, ProtocolModel, ResourceRef
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from pydantic import Field, SecretStr
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from data_center.analytics import AnalyticsEngine
from data_center.config import Settings
from data_center.documents import ConversionPolicy, DocumentEngine
from data_center.exporting import render_export
from data_center.graph.engine import GraphConfig
from data_center.observability import RunObserver
from data_center.ports import ShopPorts
from data_center.runtime import DataRuntime
from data_center.store import DataStore, digest


class OperationRequest(ProtocolModel):
    payload: dict
    upstream: list[ResourceRef] = Field(default_factory=list)


async def run_background(runtime, task_id, context):
    try:
        await runtime.run(task_id, context)
    except Exception:
        # HTTP响应已经发送，任务错误通过持久状态和日志报告。
        logging.getLogger(__name__).exception("数据任务后台执行失败，task_id=%s", task_id)


def task_view(task):
    return {"task_id": task.task_id, "run_id": task.run_id, "operation": task.operation,
            "status": task.status, "cancel_requested": task.cancel_requested,
            "input_sha256": task.input_sha256,
            "created_at": task.created_at.isoformat(), "updated_at": task.updated_at.isoformat(),
            "checkpoints": list(task.checkpoints), "result": task.result, "error": task.error}


async def caller(request: Request):
    header = request.headers.get("authorization", "")
    actor_id = request.headers.get("x-shop-actor")
    request_id = request.headers.get("x-request-id")
    if not header.startswith("Bearer ") or not actor_id or not request_id:
        raise HTTPException(401, "需要已认证调用凭据")
    context = CallContext(actor_id=actor_id, request_id=request_id, credential=SecretStr(header[7:]))
    await request.app.state.ports.identity(context)
    return context


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    database = create_engine(settings.database_url, pool_pre_ping=True)
    ports = ShopPorts(settings.shop_url, service_token_file=settings.service_token_file)
    documents = DocumentEngine(database, settings.artifact_root, ports, ports, ports,
                               conversion_policy=ConversionPolicy(
                                   artifacts_path=settings.docling_models,
                                   tokenizer_cache=settings.tokenizer_cache,
                                   tesseract_command=settings.tesseract_command),
                               graph_config=GraphConfig(enabled=settings.graph_enabled,
                                                        chat_model=settings.graph_model))
    analytics = AnalyticsEngine(database, settings.artifact_root, ports, ports)
    store = DataStore(database)
    observer = RunObserver(settings.artifact_root / "observability")
    runtime = DataRuntime(store, ports, documents, analytics, model=settings.model, observer=observer)

    @asynccontextmanager
    async def lifespan(app):
        yield
        await ports.close()
        observer.close()
        database.dispose()

    app = FastAPI(title="数据中台", lifespan=lifespan)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=observer.provider)
    HTTPXClientInstrumentor.instrument_client(ports.client, tracer_provider=observer.provider)
    SQLAlchemyInstrumentor().instrument(engine=database, tracer_provider=observer.provider)
    app.state.runtime, app.state.store = runtime, store
    app.state.ports = ports
    app.state.documents, app.state.analytics = documents, analytics

    @app.exception_handler(PermissionError)
    async def denied(request, error):
        return JSONResponse({"detail": str(error)}, status_code=403)

    @app.exception_handler(ValueError)
    async def invalid(request, error):
        return JSONResponse({"detail": str(error)}, status_code=422)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/operations/{operation}")
    async def operation(operation: str, body: OperationRequest,
                        context: Annotated[CallContext, Depends(caller)], background: BackgroundTasks):
        context = context.model_copy(update={"upstream": body.upstream})
        if operation == "catalog":
            return OperationResult(status="completed", data=await ports.catalog(context))
        if operation in {"manage_sources", "register_source", "set_grant", "withdraw_source"}:
            return OperationResult(status="completed", data=await ports.manage(operation, body.payload, context))
        if operation == "metrics":
            await ports.authorize(context, ["microsoft/adventureworks"], "read")
            return OperationResult(status="completed", data={"items": [item.model_dump(mode="json")
                                                                         for item in analytics.metric_definitions()]})
        if operation == "tasks":
            items = []
            for task in store.list(context.actor_id):
                try:
                    items.append(task_view(await runtime.show(task.task_id, context)))
                except PermissionError:
                    continue
            return OperationResult(status="completed", data={"items": items})
        if operation in {"query", "ask", "ingest", "sync", "build_graph"}:
            task = await runtime.submit(operation, body.payload, context)
            if task.result is not None:
                task = await runtime.show(task.task_id, context)
                return OperationResult.model_validate_json(json.dumps(task.result))
            background.add_task(run_background, runtime, task.task_id, context)
            return OperationResult(status=task.status, run_id=task.run_id,
                                   data={"task_id": task.task_id}, resources=[ResourceRef(
                                       module_id="data_center", resource_type="task", resource_id=task.task_id,
                                       version="1", sha256=task.input_sha256)])
        if operation == "show":
            task = await runtime.show(body.payload["task_id"], context)
            if task.result is not None:
                return OperationResult.model_validate_json(json.dumps(task.result))
            return OperationResult(status=task.status, run_id=task.run_id, data=task_view(task))
        if operation == "run":
            return await runtime.run(body.payload["task_id"], context)
        if operation == "cancel":
            task = await runtime.cancel(body.payload["task_id"], context)
            return OperationResult(status=task.status, run_id=task.run_id, data=task_view(task))
        if operation == "graph_status":
            from data_center_protocol import DataQuery

            query = DataQuery.model_validate_json(json.dumps(body.payload))
            result = await documents.graph_status(query, context)
            return OperationResult(status=result["state"], data=result)
        if operation == "reference":
            from data_center_protocol import DataEvidenceRef

            reference = DataEvidenceRef.model_validate_json(json.dumps(body.payload))
            if reference.kind == "document":
                chunk = await documents.read_evidence(reference, context)
                content = chunk.model_dump(mode="json")
            elif reference.kind == "metric":
                content = await analytics.read_evidence(reference, context)
            else:
                await ports.authorize(context, [reference.source_id], "read")
                content = await ports.read_current(reference.source_id, reference.record_id, context)
                if digest(content) != reference.content_sha256:
                    raise ValueError("当前业务记录与引用内容摘要不一致")
            return OperationResult(status="completed", data=content)
        if operation == "export":
            task = await runtime.show(body.payload["task_id"], context)
            raw, mime, filename, checksum = render_export(task, body.payload["format"])
            directory = settings.artifact_root / "exports" / context.actor_id / task.task_id
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / filename
            path.write_bytes(raw)
            reference = ResourceRef(module_id="data_center", resource_type="export_" + body.payload["format"],
                                    resource_id=task.task_id, version="1", sha256=checksum)
            return OperationResult(status="completed", data={"task_id": task.task_id,
                                                               "relative_path": filename, "mime_type": mime},
                                   resources=[reference])
        if operation == "resolve":
            resource_type, resource_id = body.payload["resource_type"], body.payload["resource_id"]
            if resource_type == "snapshot":
                from data_center.analytics.models import AnalyticsSnapshot
                from data_center.documents.models import DocumentSnapshotRow

                with Session(database) as session:
                    doc = session.get(DocumentSnapshotRow, resource_id)
                    analytic = session.get(AnalyticsSnapshot, resource_id)
                    if doc is not None:
                        source_ids, checksum, state = doc.payload["source_ids"], doc.payload["content_sha256"], doc.state
                    elif analytic is not None:
                        source_ids, checksum, state = [analytic.source_id], analytic.content_sha256, analytic.state
                    else:
                        raise LookupError("来源快照不存在")
                own_sync = any(task.operation == "sync" and task.result is not None
                               and task.result["data"].get("snapshot", {}).get("snapshot_id") == resource_id
                               for task in store.list(context.actor_id))
                await ports.authorize(context, source_ids, "sync" if own_sync else "read")
                if state != "published":
                    raise PermissionError("来源快照没有处于已发布状态")
            else:
                task = await runtime.show(resource_id, context)
                if resource_type == "task":
                    checksum = task.input_sha256
                elif resource_type == "result" and task.result is not None:
                    checksum = digest(task.result["data"])
                elif resource_type in {"export_markdown", "export_csv"}:
                    raw, _, _, checksum = render_export(task, resource_type.removeprefix("export_"))
                else:
                    raise ValueError("资源类型或完成状态无效")
            reference = ResourceRef(module_id="data_center", resource_type=resource_type,
                                    resource_id=resource_id, version="1", sha256=checksum)
            return OperationResult(status="completed", data={"reference": reference.model_dump(mode="json")})
        raise HTTPException(422, "未登记的数据中台操作")

    @app.post("/files")
    async def files(body: OperationRequest, context: Annotated[CallContext, Depends(caller)]):
        task = await runtime.show(body.payload["task_id"], context)
        format = {"answer.md": "markdown", "result.csv": "csv"}.get(body.payload["relative_path"])
        if format is None:
            raise ValueError("请求文件没有登记为可导出资料")
        raw, mime, _, _ = render_export(task, format)
        return Response(raw, media_type=mime, headers={"X-Content-SHA256": sha256(raw).hexdigest(),
                                                       "Cache-Control": "no-store"})

    return app
