from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from integration_host import IntegrationError, OperationRequest
from integration_protocol import ResourceRef
from pydantic import ValidationError

from shop.auth import current_user
from shop.models import User

router = APIRouter(prefix="/api/poster", tags=["poster"])
resource_router = APIRouter(prefix="/api/modules", tags=["module-resources"])


def integration_context(request: Request, user: User = Depends(current_user)):
    return request.app.state.integration.auth.issue(user.id, str(uuid4()))


async def invoke(request, operation, payload, context, version="1"):
    try:
        return await request.app.state.integration.invoke("poster", operation, payload, context, version)
    except IntegrationError as error:
        raise HTTPException(error.status_code, {"code": error.code, "message": error.message}) from error


@router.get("/status")
async def status(request: Request, context=Depends(integration_context)):
    return await invoke(request, "status", {}, context)


@router.get("/jobs/{job_id}")
async def job(job_id: str, request: Request, context=Depends(integration_context)):
    return await invoke(request, "show", {"job_id": job_id}, context)


@router.post("/operations/{operation}")
async def operation(operation: str, body: OperationRequest, request: Request,
                    context=Depends(integration_context)):
    context = context.model_copy(update={"request_id": body.request_id, "upstream": body.upstream})
    return await invoke(request, operation, body.payload, context, body.version)


async def file_response(request, module_id, resource_id, relative_path, context):
    try:
        content, media_type, digest = await request.app.state.integration.file(
            module_id, resource_id, relative_path, context,
        )
    except IntegrationError as error:
        raise HTTPException(error.status_code, {"code": error.code, "message": error.message}) from error
    return Response(content, media_type=media_type, headers={
        "X-Content-SHA256": digest, "X-Content-Type-Options": "nosniff", "Cache-Control": "no-store",
        "Content-Security-Policy": "sandbox; default-src 'none'; img-src data:; style-src 'unsafe-inline'",
    })


@resource_router.get("/{module_id}/resources/{resource_id}/files/{relative_path:path}")
async def resource_file(module_id: str, resource_id: str, relative_path: str, request: Request,
                        resource_type: str | None = None, version: str | None = None,
                        sha256: str | None = None, context=Depends(integration_context)):
    if resource_type is not None or version is not None or sha256 is not None:
        if resource_type is None:
            raise HTTPException(422, "指定版本或摘要时必须提供资源类型")
        try:
            reference = ResourceRef(module_id=module_id, resource_type=resource_type,
                                    resource_id=resource_id, version=version, sha256=sha256)
        except ValidationError as error:
            raise HTTPException(422, "资源引用格式无效") from error
        context = context.model_copy(update={"upstream": [reference]})
    return await file_response(request, module_id, resource_id, relative_path, context)


@router.get("/jobs/{job_id}/files/{relative_path:path}")
async def poster_file(job_id: str, relative_path: str, request: Request,
                      context=Depends(integration_context)):
    return await file_response(request, "poster", job_id, relative_path, context)
