from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field

from shop.auth import current_user
from shop.campaign import CampaignService
from shop.harness import HarnessService
from shop.models import User

router = APIRouter(prefix="/api/harness", tags=["harness"])


class CreateTask(BaseModel):
    prompt: str = Field(min_length=1, max_length=4000)


def service(request: Request) -> HarnessService:
    return CampaignService(request.app.state.settings)


def authorized(task_id: str, user: User, harness: HarnessService) -> dict:
    try:
        task = harness.show(task_id)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    if user.role != "admin" and task["owner_id"] != user.id:
        raise HTTPException(status_code=403, detail="无权访问该任务")
    return task


@router.post("/tasks")
def create(body: CreateTask, user: User = Depends(current_user), harness: HarnessService = Depends(service)):
    return harness.create(body.prompt, owner_id=user.id)


@router.get("/tasks/{task_id}")
def show(task_id: str, user: User = Depends(current_user), harness: HarnessService = Depends(service)):
    return authorized(task_id, user, harness)


@router.get("/tasks")
def listing(user: User = Depends(current_user), harness: CampaignService = Depends(service)):
    return harness.listing(None if user.role == "admin" else user.id)


def page(task_id: str, user: User, harness: CampaignService) -> str:
    authorized(task_id, user, harness)
    try:
        return harness.render(task_id)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.get("/tasks/{task_id}/preview", response_class=HTMLResponse)
def preview(task_id: str, user: User = Depends(current_user), harness: CampaignService = Depends(service)):
    return HTMLResponse(page(task_id, user, harness), headers={
        "Content-Security-Policy": "sandbox; default-src 'none'; img-src data:; style-src 'unsafe-inline'",
        "X-Content-Type-Options": "nosniff", "Cache-Control": "no-store",
    })


@router.get("/tasks/{task_id}/export")
def export(task_id: str, user: User = Depends(current_user), harness: CampaignService = Depends(service)):
    return Response(page(task_id, user, harness), media_type="text/html", headers={
        "Content-Disposition": f'attachment; filename="campaign-{task_id}.html"',
        "X-Content-Type-Options": "nosniff", "Cache-Control": "no-store",
    })


@router.post("/tasks/{task_id}/run")
def run(task_id: str, user: User = Depends(current_user), harness: HarnessService = Depends(service)):
    authorized(task_id, user, harness)
    try:
        return harness.run(task_id)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/tasks/{task_id}/resume")
def resume(task_id: str, user: User = Depends(current_user), harness: HarnessService = Depends(service)):
    authorized(task_id, user, harness)
    try:
        return harness.resume(task_id)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/tasks/{task_id}/cancel")
def cancel(task_id: str, user: User = Depends(current_user), harness: HarnessService = Depends(service)):
    authorized(task_id, user, harness)
    return harness.cancel(task_id)
