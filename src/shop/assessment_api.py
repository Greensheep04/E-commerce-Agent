from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from shop.assessment import AssessmentService
from shop.assessment_models import AssessmentAccess, AssessmentEmployee
from shop.assessment_orders import DataBlocked
from shop.assessment_service import (
    AccessDenied,
    SalesMetricService,
    configure_access,
    principal_for,
)
from shop.assessment_types import REVISION, SAMPLE_NOTICE, SOURCE_ID
from shop.auth import current_user
from shop.models import User

router = APIRouter(prefix="/api/assessment", tags=["assessment"])


def access_context(request: Request, user: User = Depends(current_user)):
    with request.app.state.sessionmaker() as session:
        try:
            return principal_for(session, user.id)
        except AccessDenied as error:
            raise HTTPException(403, str(error)) from error


def metric_service(request: Request) -> SalesMetricService:
    return request.app.state.assessment_metrics


def run_service(request: Request):
    service = AssessmentService(request.app.state.settings, metric_service(request))
    try:
        yield service
    except AccessDenied as error:
        raise HTTPException(403, str(error)) from error
    except LookupError as error:
        raise HTTPException(404, str(error)) from error
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    finally:
        service.close()


@router.get("/me")
def me(principal=Depends(access_context)):
    return {**principal.__dict__, "notice": SAMPLE_NOTICE}


@router.get("/employees")
def employees(request: Request, principal=Depends(access_context)):
    with request.app.state.sessionmaker() as session:
        return [{"employee_id": row.employee_id, "job_title": row.job_title,
                 "current_flag": row.current_flag}
                for row in session.scalars(select(AssessmentEmployee).where(
                    AssessmentEmployee.employee_id.in_(principal.allowed_employee_ids),
                ).order_by(AssessmentEmployee.employee_id)).all()]


@router.get("/status")
def status(request: Request, principal=Depends(access_context)):
    with request.app.state.sessionmaker() as session:
        try:
            metric_service(request).snapshot(session)
            state, reason = "ready", None
        except DataBlocked as error:
            state, reason = "DATA_BLOCKED", str(error)
    return {"status": state, "reason": reason, "notice": SAMPLE_NOTICE,
            "source_id": SOURCE_ID, "revision": REVISION}


@router.get("/metrics")
def metrics(
    request: Request, scope: Literal["self", "team", "employees", "organization"] = "self",
    employee_ids: list[int] = Query(default=[]),
    period_start: str = "2023-07-01", period_end: str = "2023-10-01",
    principal=Depends(access_context),
):
    with request.app.state.sessionmaker() as session:
        try:
            return metric_service(request).query(
                session, principal, scope=scope, employee_ids=employee_ids,
                period_start=period_start, period_end=period_end,
            )
        except AccessDenied as error:
            raise HTTPException(403, str(error)) from error
        except DataBlocked as error:
            raise HTTPException(409, f"DATA_BLOCKED: {error}") from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error


@router.get("/evidence/{employee_id}")
def evidence(employee_id: int, request: Request, principal=Depends(access_context)):
    with request.app.state.sessionmaker() as session:
        try:
            return metric_service(request).evidence(session, principal, employee_id)
        except AccessDenied as error:
            raise HTTPException(403, str(error)) from error
        except DataBlocked as error:
            raise HTTPException(409, f"DATA_BLOCKED: {error}") from error


class AccessInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: str = Field(min_length=1, max_length=36)
    level: Literal["employee", "manager", "admin"]
    employee_id: int | None = None
    managed_employee_ids: list[int] = Field(default_factory=list, max_length=17)


@router.get("/access")
def list_access(request: Request, principal=Depends(access_context)):
    if principal.level != "admin":
        raise HTTPException(403, "权限配置需要考核管理员权限")
    with request.app.state.sessionmaker() as session:
        return [{"user_id": user.id, "username": user.username,
                 "level": access.level if access else None,
                 "employee_id": access.employee_id if access else None,
                 "managed_employee_ids": access.managed_employee_ids if access else []}
                for user, access in session.execute(select(User, AssessmentAccess).outerjoin(
                    AssessmentAccess, AssessmentAccess.user_id == User.id,
                ).order_by(User.username)).all()]


@router.post("/access")
def set_access(body: AccessInput, request: Request, principal=Depends(access_context)):
    if principal.level != "admin":
        raise HTTPException(403, "权限配置需要考核管理员权限")
    with request.app.state.sessionmaker.begin() as session:
        try:
            access = configure_access(session, **body.model_dump())
            return {"user_id": access.user_id, "level": access.level,
                    "employee_id": access.employee_id,
                    "managed_employee_ids": access.managed_employee_ids}
        except ValueError as error:
            raise HTTPException(422, str(error)) from error


class RunInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    prompt: str = Field(min_length=1, max_length=4000)
    parent_id: str | None = Field(default=None, min_length=36, max_length=36)


@router.post("/runs")
def create_run(body: RunInput, principal=Depends(access_context), service=Depends(run_service)):
    return service.create(body.prompt, principal.user_id, body.parent_id)


@router.get("/runs")
def list_runs(principal=Depends(access_context), service=Depends(run_service)):
    return service.listing(principal.user_id)


@router.get("/runs/{run_id}")
def get_run(run_id: str, principal=Depends(access_context), service=Depends(run_service)):
    return service.show(run_id, principal.user_id)


@router.post("/runs/{run_id}/run")
@router.post("/runs/{run_id}/resume")
def execute_run(run_id: str, principal=Depends(access_context), service=Depends(run_service)):
    return service.execute(run_id, principal.user_id)


@router.post("/runs/{run_id}/cancel")
def cancel_run(run_id: str, principal=Depends(access_context), service=Depends(run_service)):
    return service.cancel(run_id, principal.user_id)


@router.get("/runs/{run_id}/export")
def export_run(run_id: str, format: Literal["html", "csv"] = "html",
               principal=Depends(access_context), service=Depends(run_service)):
    content = service.export(run_id, principal.user_id, format)
    return Response(content, media_type="text/html" if format == "html" else "text/csv",
                    headers={"Content-Disposition": f'attachment; filename="assessment-{run_id}.{format}"',
                             "X-Content-Type-Options": "nosniff", "Cache-Control": "no-store"})
