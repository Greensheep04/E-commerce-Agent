from contextlib import asynccontextmanager
from html import escape

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from integration_host import create_router
from markupsafe import Markup
from pydantic import BaseModel, Field, ValidationError
from sqladmin import Admin, ModelView
from sqladmin.authentication import AuthenticationBackend
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles

from shop.assessment_api import router as assessment_router
from shop.assessment_orders import DataBlocked
from shop.auth import admin_user, authenticate, current_user
from shop.cluster import ShopCluster
from shop.config import Settings
from shop.customer_api import router as customer_router
from shop.customer_runtime import CustomerService
from shop.data_center_api import router as data_center_host_router
from shop.data_center_api import ui_router as data_center_ui_router
from shop.data_center_gateway import ShopDataModelGateway
from shop.data_center_host import ShopDataAuthorization, ShopSourceReadPort
from shop.harness_api import router as harness_router
from shop.integration import ShopIntegration
from shop.integration_api import integration_context, resource_router
from shop.integration_api import router as poster_router
from shop.materials import MaterialInput, MaterialRepository
from shop.models import Asset, AssetVersion, ImportBatch, Source, User


class AdminAuth(AuthenticationBackend):
    def __init__(self, secret_key: str, maker: sessionmaker):
        super().__init__(secret_key)
        self.maker = maker

    async def login(self, request: Request) -> bool:
        form = await request.form()
        with self.maker() as session:
            user = authenticate(session, str(form.get("username", "")), str(form.get("password", "")))
            if user is None or user.role != "admin":
                return False
            request.session["user_id"] = user.id
            return True

    async def logout(self, request: Request) -> bool:
        request.session.clear()
        return True

    async def authenticate(self, request: Request) -> bool:
        user_id = request.session.get("user_id")
        if user_id is None:
            return False
        with self.maker() as session:
            user = session.get(User, user_id)
            return user is not None and user.role == "admin"


class SourceAdmin(ModelView, model=Source):
    column_list = [Source.source_id, Source.revision, Source.license, Source.license_note, Source.url]
    can_delete = False


class BatchAdmin(ModelView, model=ImportBatch):
    column_list = [
        ImportBatch.id, ImportBatch.source_id, ImportBatch.source_revision,
        ImportBatch.status, ImportBatch.imported_count, ImportBatch.expected_count,
    ]
    can_create = False
    can_edit = False
    can_delete = False


class AssetAdmin(ModelView, model=Asset):
    column_list = [
        Asset.id, Asset.title, Asset.kind, Asset.source_id, Asset.source_revision,
        Asset.record_id, Asset.owner_id,
    ]
    column_formatters = {
        Asset.id: lambda model, attribute: Markup(
            f'<a href="/manage/materials/{escape(model.id)}/edit">{escape(model.id)}</a>'
        )
    }
    can_create = False
    can_edit = False
    can_delete = False


class VersionAdmin(ModelView, model=AssetVersion):
    column_list = [
        AssetVersion.id, AssetVersion.asset_id, AssetVersion.version,
        AssetVersion.file_sha256, AssetVersion.owner_id,
    ]
    can_create = False
    can_edit = False
    can_delete = False


def get_session(request: Request):
    with request.app.state.sessionmaker() as session:
        yield session


class LoginInput(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1)


class MetadataUpdate(BaseModel):
    title: str = Field(min_length=1, max_length=500)
    tags: list[str]
    owner_id: str


def asset_payload(asset: Asset) -> dict:
    return {
        "id": asset.id,
        "source_id": asset.source_id,
        "source_revision": asset.source_revision,
        "record_id": asset.record_id,
        "source_order": asset.source_order,
        "title": asset.title,
        "kind": asset.kind,
        "tags": asset.tags,
        "owner_id": asset.owner_id,
        "current_version": asset.versions[-1].version,
        "edit_url": f"/manage/materials/{asset.id}/edit",
    }


def version_payload(version: AssetVersion) -> dict:
    return {
        "id": version.id,
        "asset_id": version.asset_id,
        "version": version.version,
        "file_sha256": version.file_sha256,
        "file_size": version.file_size,
        "filename": version.filename,
        "mime_type": version.mime_type,
        "original_fields": version.original_fields,
        "title": version.title,
        "tags": version.tags,
        "owner_id": version.owner_id,
    }


@asynccontextmanager
async def customer_lifespan(app: FastAPI):
    app.state.customer_worker = app.state.integration.worker
    await app.state.integration.startup()
    if app.state.cluster is not None:
        await app.state.cluster.startup()
    try:
        yield
    finally:
        if app.state.cluster is not None:
            await app.state.cluster.shutdown()
        await app.state.integration.shutdown()
        app.state.customer.engine.dispose()
        app.state.engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    if settings.data_center_url is not None:
        settings.get_data_center_service_token()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    app = FastAPI(title="项目模块接入服务", lifespan=customer_lifespan)
    app.include_router(harness_router)
    app.include_router(customer_router)
    app.include_router(assessment_router)
    app.include_router(poster_router)
    app.include_router(resource_router)
    app.include_router(data_center_host_router)
    app.include_router(data_center_ui_router)
    app.add_middleware(SessionMiddleware, secret_key=settings.get_session_secret(), same_site="lax")
    app.state.settings = settings
    app.state.engine = engine
    app.state.sessionmaker = sessionmaker(engine, expire_on_commit=False)
    app.state.customer = CustomerService(settings)
    app.state.integration = ShopIntegration(settings, customer=app.state.customer)
    app.state.data_authorization = ShopDataAuthorization(app.state.integration.auth,
                                                        app.state.sessionmaker)
    app.state.data_sources = ShopSourceReadPort(settings, app.state.sessionmaker,
                                                app.state.data_authorization)
    app.state.data_models = ShopDataModelGateway(settings, app.state.sessionmaker,
                                                app.state.data_authorization)
    app.state.assessment_metrics = app.state.integration.metrics
    app.include_router(create_router(app.state.integration.host, integration_context))
    app.state.cluster = None
    if settings.cluster_config_file is not None:
        app.state.cluster = ShopCluster(settings, engine, app.state.sessionmaker,
                                        integration=app.state.integration)
        app.include_router(app.state.cluster.router())

    @app.exception_handler(PermissionError)
    async def permission_error(request: Request, error: PermissionError):
        return JSONResponse({"detail": str(error)}, status_code=403)

    @app.exception_handler(ValueError)
    async def operation_error(request: Request, error: ValueError):
        return JSONResponse({"detail": str(error)}, status_code=409)

    @app.exception_handler(LookupError)
    async def missing_resource(request: Request, error: LookupError):
        if type(error) is not LookupError:
            raise error
        return JSONResponse({"detail": str(error)}, status_code=404)

    @app.exception_handler(DataBlocked)
    async def blocked_data(request: Request, error: DataBlocked):
        return JSONResponse({"detail": f"DATA_BLOCKED: {error}"}, status_code=409)
    admin = Admin(
        app, engine,
        authentication_backend=AdminAuth(settings.get_session_secret(), app.state.sessionmaker),
    )
    for view in (SourceAdmin, BatchAdmin, AssetAdmin, VersionAdmin):
        admin.add_view(view)

    @app.get("/health")
    def health(session: Session = Depends(get_session)):
        session.execute(select(User.id).limit(1)).first()
        return {"status": "ok"}

    @app.post("/api/login")
    def login(body: LoginInput, request: Request, session: Session = Depends(get_session)):
        user = authenticate(session, body.username, body.password)
        if user is None:
            raise HTTPException(status_code=401, detail="账号或密码错误")
        request.session["user_id"] = user.id
        return {"id": user.id, "username": user.username, "role": user.role}

    @app.post("/api/logout")
    def logout(request: Request):
        request.session.clear()
        return {"status": "ok"}

    @app.get("/api/assets")
    def list_assets(
        source_id: str | None = None, source_revision: str | None = None,
        kind: str | None = None, owner_id: str | None = None, tag: str | None = None,
        q: str | None = None, limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
        user: User = Depends(current_user), session: Session = Depends(get_session),
    ):
        repository = MaterialRepository(session, settings.data_dir)
        assets = repository.search(
            title=q, source_id=source_id, source_revision=source_revision,
            kind=kind, owner_id=owner_id, tag=tag, limit=limit, offset=offset,
        )
        return [asset_payload(asset) for asset in assets]

    @app.get("/api/assets/{asset_id}")
    def get_asset(asset_id: str, user: User = Depends(current_user), session: Session = Depends(get_session)):
        asset = MaterialRepository(session, settings.data_dir).get(asset_id)
        if asset is None:
            raise HTTPException(status_code=404, detail="素材不存在")
        return {**asset_payload(asset), "versions": [version_payload(v) for v in asset.versions]}

    @app.get("/api/assets/{asset_id}/versions/{number}/file")
    def get_file(
        asset_id: str, number: int, user: User = Depends(current_user),
        session: Session = Depends(get_session),
    ):
        version = session.scalar(
            select(AssetVersion).where(
                AssetVersion.asset_id == asset_id, AssetVersion.version == number
            )
        )
        if version is None:
            raise HTTPException(status_code=404, detail="素材版本不存在")
        repository = MaterialRepository(session, settings.data_dir)
        try:
            content = repository.read_file(version)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        except FileNotFoundError as error:
            raise HTTPException(status_code=404, detail="素材文件不存在") from error
        safe_inline = version.mime_type.startswith("image/") or version.mime_type.startswith("text/plain")
        media_type = version.mime_type if safe_inline else "application/octet-stream"
        disposition = "inline" if safe_inline else "attachment"
        return Response(
            content=content, media_type=media_type,
            headers={"X-Content-Type-Options": "nosniff", "Content-Disposition": f'{disposition}; filename="material"'},
        )

    @app.put("/api/assets/{asset_id}")
    def update_asset(
        asset_id: str, body: MetadataUpdate, user: User = Depends(admin_user),
        session: Session = Depends(get_session),
    ):
        repository = MaterialRepository(session, settings.data_dir)
        try:
            asset = repository.update_metadata(
                asset_id, title=body.title, tags=body.tags, owner_id=body.owner_id
            )
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return asset_payload(asset)

    @app.get("/manage/materials/{asset_id}/edit", response_class=HTMLResponse)
    def edit_page(
        asset_id: str, user: User = Depends(admin_user), session: Session = Depends(get_session)
    ):
        asset = MaterialRepository(session, settings.data_dir).get(asset_id)
        if asset is None:
            raise HTTPException(status_code=404, detail="素材不存在")
        file_url = f"/api/assets/{escape(asset.id)}/versions/{asset.versions[-1].version}/file"
        preview = (
            f'<img alt="素材预览" src="{file_url}">'
            if asset.kind == "image"
            else f'<a href="{file_url}">下载素材文件</a>'
        )
        return HTMLResponse(
            f'<form method="post" action="/manage/materials/{escape(asset.id)}/edit">'
            f'<label>名称 <input name="title" value="{escape(asset.title)}"></label>'
            f'<label>标签 <input name="tags" value="{escape(", ".join(asset.tags))}"></label>'
            f'<label>负责人 ID <input name="owner_id" value="{escape(asset.owner_id)}"></label>'
            '<button type="submit">保存新版本</button></form>'
            f"{preview}"
        )

    @app.post("/manage/materials/{asset_id}/edit")
    def save_edit_page(
        asset_id: str, title: str = Form(...), tags: str = Form(""),
        owner_id: str = Form(...), user: User = Depends(admin_user),
        session: Session = Depends(get_session),
    ):
        repository = MaterialRepository(session, settings.data_dir)
        try:
            repository.update_metadata(
                asset_id, title=title, tags=[tag.strip() for tag in tags.split(",") if tag.strip()],
                owner_id=owner_id,
            )
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return RedirectResponse(url=f"/manage/materials/{asset_id}/edit", status_code=303)

    @app.get("/api/imports")
    def list_batches(user: User = Depends(current_user), session: Session = Depends(get_session)):
        batches = session.scalars(select(ImportBatch).order_by(ImportBatch.started_at.desc())).all()
        return [
            {"id": batch.id, "status": batch.status, "source_id": batch.source_id,
             "source_revision": batch.source_revision, "owner_id": batch.owner_id,
             "expected_count": batch.expected_count, "imported_count": batch.imported_count,
             "error": batch.error, "manifest": batch.manifest}
            for batch in batches
        ]

    @app.post("/api/assets")
    async def upload_asset(
        source_id: str = Form(...), source_revision: str = Form(...),
        record_id: str = Form(...), title: str = Form(...), kind: str = Form(...),
        file: UploadFile = File(...), user: User = Depends(admin_user),
        session: Session = Depends(get_session),
    ):
        content = bytearray()
        while chunk := await file.read(1024 * 1024):
            content.extend(chunk)
            if len(content) > settings.upload_max_bytes:
                raise HTTPException(status_code=413, detail="文件超过上传大小上限")
        try:
            material = MaterialInput(
                source_id=source_id, source_revision=source_revision, record_id=record_id,
                title=title, kind=kind, owner_id=user.id, source_order=0,
                original_fields={}, filename=file.filename or "upload",
                mime_type=file.content_type or "application/octet-stream", file_bytes=bytes(content),
            )
        except ValidationError as error:
            detail = [
                {"field": ".".join(map(str, item["loc"])), "message": item["msg"]}
                for item in error.errors()
            ]
            raise HTTPException(status_code=422, detail=detail) from error
        repository = MaterialRepository(session, settings.data_dir)
        try:
            asset = repository.save(material)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return asset_payload(asset)

    @app.post("/api/imports/fashion")
    def import_fashion(user: User = Depends(admin_user)):
        from shop.fashion import run_import

        batch = run_import(settings, user.id)
        return {"id": batch.id, "status": batch.status, "imported_count": batch.imported_count}

    app.mount("/harness", StaticFiles(directory=settings.harness_frontend_dir, html=True, check_dir=False))
    return app


app = create_app()
