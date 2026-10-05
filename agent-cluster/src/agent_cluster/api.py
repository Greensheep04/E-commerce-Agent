from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import Field

from agent_cluster.runtime import schedule_trigger
from agent_cluster.types import (
    ClusterModel,
    CompletionCriterion,
    GoalDefinition,
    ScheduleDefinition,
    TriggerEvent,
)


class GoalInput(ClusterModel):
    objective: str
    allowed_operations: dict[str, list[str]]
    criteria: list[CompletionCriterion]
    budget_cny: Decimal = Decimal("30")
    max_decisions: int = 40
    max_tasks: int = 80
    required_confirmations: list[str] = Field(default_factory=list)
    approved_confirmations: list[str] = Field(default_factory=list)


class UpdateInput(ClusterModel):
    expected_revision: int
    definition: GoalInput


class EventInput(ClusterModel):
    event_id: str
    payload: dict = Field(default_factory=dict)


class ExternalEventInput(EventInput):
    goal_id: str


class ScheduleInput(ClusterModel):
    goal_id: str
    interval_seconds: int | None = None
    cron: str | None = None
    timezone: str = "Asia/Shanghai"
    enabled: bool = True


def create_cluster_router(store, runtime, actor_dependency, validate_goal):
    router = APIRouter(prefix="/api/cluster", tags=["Agent集群"])

    @router.post("/goals")
    async def create_goal(body: GoalInput, actor_id: str = Depends(actor_dependency)):
        goal = GoalDefinition(actor_id=actor_id, **body.model_dump())
        await validate_goal(goal)
        return store.create_goal(goal)

    @router.get("/goals")
    def list_goals(actor_id: str = Depends(actor_dependency)):
        return store.list_goals(actor_id)

    @router.get("/goals/{goal_id}")
    def get_goal(goal_id: str, actor_id: str = Depends(actor_dependency)):
        goal = store.get_goal(goal_id, actor_id)
        return {**goal.model_dump(mode="json"), "state": store.goal_state(goal_id, actor_id),
                "runs": [run.model_dump(mode="json") for run in store.list_runs(goal_id, actor_id)]}

    @router.put("/goals/{goal_id}")
    async def update_goal(goal_id: str, body: UpdateInput, actor_id: str = Depends(actor_dependency)):
        previous = store.get_goal(goal_id, actor_id)
        goal = GoalDefinition(goal_id=goal_id, actor_id=actor_id, revision=previous.revision,
                              **body.definition.model_dump())
        await validate_goal(goal)
        return store.update_goal(goal, expected_revision=body.expected_revision)

    @router.post("/goals/{goal_id}/control/{action}")
    async def control_goal(goal_id: str, action: str, actor_id: str = Depends(actor_dependency)):
        store.control_goal(goal_id, actor_id, action)
        runtime.notify()
        if action == "cancel":
            await runtime.engine.cancel_goal_tasks(goal_id, actor_id)
        return {"goal_id": goal_id, "state": store.goal_state(goal_id, actor_id)}

    @router.post("/goals/{goal_id}/trigger")
    def trigger_goal(goal_id: str, body: EventInput, actor_id: str = Depends(actor_dependency)):
        result = store.trigger(TriggerEvent(goal_id=goal_id, actor_id=actor_id, kind="manual", **body.model_dump()))
        runtime.notify()
        return result

    @router.post("/events")
    def emit_event(body: ExternalEventInput, actor_id: str = Depends(actor_dependency)):
        result = store.trigger(TriggerEvent(actor_id=actor_id, kind="event", **body.model_dump()))
        runtime.notify()
        return result

    @router.get("/runs/{run_id}")
    def get_run(run_id: str, actor_id: str = Depends(actor_dependency)):
        run = store.get_run(run_id, actor_id)
        return {**run.model_dump(mode="json"), "tasks": store.tasks(run_id, actor_id)}

    @router.get("/runs/{run_id}/history")
    def get_history(run_id: str, actor_id: str = Depends(actor_dependency)):
        return store.history(run_id, actor_id)

    @router.post("/schedules")
    def create_schedule(body: ScheduleInput, actor_id: str = Depends(actor_dependency)):
        schedule = ScheduleDefinition(actor_id=actor_id, **body.model_dump())
        schedule_trigger(schedule)
        runtime.notify()
        return store.save_schedule(schedule)

    @router.put("/schedules/{schedule_id}")
    def update_schedule(schedule_id: str, body: ScheduleDefinition, actor_id: str = Depends(actor_dependency)):
        if body.actor_id != actor_id or body.schedule_id != schedule_id:
            raise PermissionError("日程身份或编号不一致")
        schedule_trigger(body)
        return store.save_schedule(body)

    @router.get("/schedules")
    def list_schedules(actor_id: str = Depends(actor_dependency)):
        return store.schedules(actor_id)

    return router
