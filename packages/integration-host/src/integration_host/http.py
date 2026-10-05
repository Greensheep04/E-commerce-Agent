from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from integration_protocol import (
    CallContext,
    JSONObject,
    ModuleDescriptor,
    NonEmptyString,
    OperationResult,
    ProtocolModel,
    ResourceRef,
)
from pydantic import Field

from .errors import IntegrationError
from .host import ModuleHost


class OperationRequest(ProtocolModel):
    version: NonEmptyString = "1"
    request_id: NonEmptyString
    payload: JSONObject
    upstream: list[ResourceRef] = Field(default_factory=list)


def create_router(host: ModuleHost, context_dependency: Callable) -> APIRouter:
    if not callable(context_dependency):
        raise TypeError("context_dependency必须提供服务端身份验证依赖")
    router = APIRouter(prefix="/api/modules", tags=["modules"])

    @router.get("", response_model=list[ModuleDescriptor])
    async def modules(context: Annotated[CallContext, Depends(context_dependency)]):
        if not isinstance(context, CallContext):
            raise TypeError("身份验证依赖必须返回CallContext")
        return host.describe()

    @router.post("/{module_id}/operations/{operation}", response_model=OperationResult)
    async def invoke(
        module_id: str, operation: str, body: OperationRequest,
        context: Annotated[CallContext, Depends(context_dependency)],
    ):
        if not isinstance(context, CallContext):
            raise TypeError("身份验证依赖必须返回CallContext")
        context = context.model_copy(update={
            "request_id": body.request_id, "upstream": body.upstream,
        })
        try:
            return await host.invoke(module_id, operation, body.payload, context, body.version)
        except IntegrationError as error:
            raise HTTPException(
                status_code=error.status_code,
                detail={"code": error.code, "message": error.message},
            ) from error

    return router
