from hmac import compare_digest
from typing import Literal

from data_center_protocol import GrantRegistration, ModelInvocation, SourceRegistration
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from integration_host import IntegrationError, OperationRequest
from integration_protocol import CallContext, ProtocolModel, ResourceRef
from pydantic import Field, SecretStr
from sqlalchemy import select

from shop.assessment_orders import DataBlocked
from shop.auth import current_user
from shop.budget import BudgetExceeded
from shop.data_center_models import DataCenterSource, DataCenterWatch
from shop.data_center_registry import DataCenterRegistry

router = APIRouter(prefix="/api/data-center/host", tags=["data-center-host"])
ui_router = APIRouter(prefix="/api/data-center", tags=["data-center"])


class Login(ProtocolModel):
    username: str
    password: SecretStr
    request_id: str


class SourceRequest(ProtocolModel):
    source_id: str
    snapshot_id: str | None = None
    cursor: str | None = None
    limit: int = Field(default=500, ge=1, le=1000)
    record_id: str | None = None


class AuthorizationRequest(ProtocolModel):
    source_ids: list[str]
    action: Literal["read", "sync", "manage", "ingest", "model"]


class TaskBindingRequest(ProtocolModel):
    task_id: str
    run_id: str
    operation: Literal["query", "ask", "ingest", "sync", "build_graph"]
    source_ids: list[str]
    payload: dict


class LookupRequest(ProtocolModel):
    task_id: str
    step_key: str


class ModelReuseRequest(ProtocolModel):
    invocation: ModelInvocation
    original_request_id: str


class TaskRequest(ProtocolModel):
    task_id: str


class WatchRequest(ProtocolModel):
    goal_id: str
    actor_id: str
    source_ids: list[str] = Field(min_length=1)
    operation: Literal["sync", "ingest"]
    interval_seconds: int = Field(default=900, ge=1)
    nightly_cron: str = "0 2 * * *"
    timezone: str = "Asia/Shanghai"


def caller(request: Request):
    header = request.headers.get("authorization", "")
    actor = request.headers.get("x-shop-actor")
    request_id = request.headers.get("x-request-id")
    if not header.startswith("Bearer ") or not actor or not request_id:
        raise HTTPException(401, "需要已认证调用凭据")
    context = CallContext(actor_id=actor, request_id=request_id, credential=SecretStr(header[7:]))
    try:
        request.app.state.integration.auth.verify(context)
    except PermissionError as error:
        raise HTTPException(401, str(error)) from error
    return context


def service_caller(request: Request, context=Depends(caller)):
    provided = request.headers.get("x-data-center-service-token", "")
    if not provided:
        raise HTTPException(401, "需要数据服务身份凭据")
    expected = request.app.state.integration.settings.get_data_center_service_token()
    if not compare_digest(provided.encode(), expected.encode()):
        raise HTTPException(401, "数据服务身份凭据无效")
    return context


async def checked(operation):
    try:
        return await operation
    except PermissionError as error:
        raise HTTPException(403, str(error)) from error
    except DataBlocked as error:
        raise HTTPException(424, {"code": "DATA_BLOCKED", "message": str(error)}) from error
    except BudgetExceeded as error:
        raise HTTPException(429, {"code": "budget_exhausted", "message": str(error)}) from error
    except LookupError as error:
        raise HTTPException(404, str(error)) from error
    except ValueError as error:
        raise HTTPException(422, str(error)) from error


@router.post("/login")
async def login(body: Login, request: Request):
    try:
        context = request.app.state.integration.login(body.username, body.password.get_secret_value(),
                                                     body.request_id)
    except PermissionError as error:
        raise HTTPException(401, str(error)) from error
    return {"actor_id": context.actor_id, "credential": context.credential.get_secret_value(),
            "expires_in": 3600}


@router.post("/identity")
async def identity(request: Request, context=Depends(caller)):
    return {"actor_id": context.actor_id}


@router.post("/catalog")
async def catalog(request: Request, context=Depends(caller)):
    with request.app.state.sessionmaker() as session:
        sources = session.scalars(select(DataCenterSource).where(DataCenterSource.active.is_(True))).all()
        items = []
        for source in sources:
            try:
                scope = await request.app.state.data_authorization.authorize(context, [source.source_id], "read")
            except PermissionError:
                continue
            record_id = source.properties["record_id"] if source.kind == "document" else source.source_id
            if source.kind == "document" and source.source_id not in scope.source_wide and f"{source.source_id}::{record_id}" not in scope.asset_ids:
                continue
            items.append({"asset_id": f"{source.source_id}::{record_id}", "source_id": source.source_id,
                          "record_id": record_id, "title": source.title, "kind": source.kind,
                          "source_revision": source.revision, "source_nature": source.nature,
                          "license_id": source.license_id, "sha256": source.sha256,
                          "properties": source.properties})
        return {"items": items}


@router.post("/authorize")
async def authorize(body: AuthorizationRequest, request: Request, context=Depends(caller)):
    return await checked(request.app.state.data_authorization.authorize(
        context, body.source_ids, body.action))


@router.post("/snapshot")
async def snapshot(body: SourceRequest, request: Request, context=Depends(caller)):
    return await checked(request.app.state.data_sources.snapshot(body.source_id, context))


@router.post("/page")
async def page(body: SourceRequest, request: Request, context=Depends(caller)):
    if body.snapshot_id is None:
        raise HTTPException(422, "分页必须绑定来源快照")
    return await checked(request.app.state.data_sources.read_page(
        body.source_id, body.snapshot_id, body.cursor, body.limit, context))


@router.post("/current")
async def current(body: SourceRequest, request: Request, context=Depends(caller)):
    if body.record_id is None:
        raise HTTPException(422, "当前业务查询必须提供记录编号")
    return await checked(request.app.state.data_sources.read_current(
        body.source_id, body.record_id, context))


@router.post("/file")
async def file(body: ResourceRef, request: Request, context=Depends(caller)):
    content = await checked(request.app.state.data_sources.read_file(body, context))
    return Response(content, media_type="application/octet-stream", headers={
        "X-Content-SHA256": body.sha256, "Cache-Control": "no-store"})


@router.post("/tasks/bind")
async def bind(body: TaskBindingRequest, request: Request, context=Depends(service_caller)):
    await checked(request.app.state.data_models.bind(
        body.task_id, body.run_id, body.operation, body.source_ids, body.payload, context))
    return {"status": "bound", "task_id": body.task_id}


@router.post("/tasks/cancel")
async def cancel(body: TaskRequest, request: Request, context=Depends(service_caller)):
    await checked(request.app.state.data_models.cancel(body.task_id, context))
    return {"status": "cancelled", "task_id": body.task_id}


@router.post("/models/invoke")
async def invoke(body: ModelInvocation, request: Request, context=Depends(service_caller)):
    return await checked(request.app.state.data_models.invoke(body, context))


@router.post("/models/lookup")
async def lookup(body: LookupRequest, request: Request, context=Depends(service_caller)):
    return await checked(request.app.state.data_models.lookup(body.task_id, body.step_key, context))


@router.post("/models/reuse")
async def reuse(body: ModelReuseRequest, request: Request, context=Depends(service_caller)):
    return await checked(request.app.state.data_models.reuse(
        body.invocation, body.original_request_id, context))


@router.post("/tasks/usage")
async def usage(body: TaskRequest, request: Request, context=Depends(service_caller)):
    return await checked(request.app.state.data_models.usage(body.task_id, context))


def registry(request):
    integration = request.app.state.integration
    return DataCenterRegistry(integration.settings, integration.maker, integration.auth)


@router.post("/registry/list")
async def registered_sources(request: Request, context=Depends(caller)):
    return {"items": registry(request).list(context)}


@router.post("/registry/register")
async def register_source(body: SourceRegistration, request: Request, context=Depends(caller)):
    result = registry(request).register(body, context)
    cluster = request.app.state.cluster
    if cluster is not None and cluster.automation is not None:
        with request.app.state.sessionmaker() as session:
            watched = session.scalar(select(DataCenterWatch.watch_id).where(
                DataCenterWatch.active.is_(True), DataCenterWatch.source_ids.contains([body.source_id])))
        if watched is not None:
            await cluster.automation.trigger_source_event(body.source_id)
    return result


@router.post("/registry/grant")
async def set_grant(body: GrantRegistration, request: Request, context=Depends(caller)):
    return registry(request).grant(body, context)


@router.post("/registry/withdraw")
async def withdraw_source(body: SourceRequest, request: Request, context=Depends(caller)):
    return registry(request).withdraw(body.source_id, context)


@ui_router.post("/operations/{operation}")
async def browser_operation(operation: str, body: OperationRequest, request: Request,
                            user=Depends(current_user)):
    context = request.app.state.integration.auth.issue(user.id, body.request_id, body.upstream)
    try:
        return await request.app.state.integration.invoke("data_center", operation, body.payload, context, body.version)
    except IntegrationError as error:
        raise HTTPException(error.status_code, {"code": error.code, "message": error.message}) from error


@ui_router.post("/automation")
async def install_watch(body: WatchRequest, request: Request, user=Depends(current_user)):
    if user.role != "admin":
        raise PermissionError("来源日程管理需要管理员权限")
    cluster = request.app.state.cluster
    if cluster is None or cluster.automation is None:
        raise ValueError("当前服务没有配置Agent集群自动运行")
    watch = cluster.automation.install_watch(**body.model_dump())
    cluster.runtime.notify()
    return {"watch_id": watch.watch_id, "goal_id": watch.goal_id,
            "source_ids": watch.source_ids, "operation": watch.operation, "active": watch.active}


@ui_router.get("/automation")
def watches(request: Request, user=Depends(current_user)):
    if user.role != "admin":
        raise PermissionError("来源日程管理需要管理员权限")
    with request.app.state.sessionmaker() as session:
        rows = session.scalars(select(DataCenterWatch).order_by(DataCenterWatch.watch_id)).all()
        return {"items": [{"watch_id": row.watch_id, "goal_id": row.goal_id,
                           "actor_id": row.actor_id, "source_ids": row.source_ids,
                           "operation": row.operation, "active": row.active,
                           "committed_signature": row.committed_signature} for row in rows]}
