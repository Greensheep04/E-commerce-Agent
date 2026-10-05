import json
from contextlib import contextmanager
from hashlib import sha256
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from agent_cluster.types import (
    GoalDefinition,
    GoalRun,
    ScheduleDefinition,
    TaskPlan,
    TriggerEvent,
    now,
)

TERMINAL = {"completed", "failed", "cancelled", "unknown", "budget_exhausted"}


class PlanRejected(ValueError):
    pass


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


class Base(DeclarativeBase):
    pass


class GoalRecord(Base):
    __tablename__ = "cluster_goals"
    goal_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    actor_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    state: Mapped[str] = mapped_column(String(30), nullable=False, default="active")
    document: Mapped[dict] = mapped_column(JSONB, nullable=False)


class RunRecord(Base):
    __tablename__ = "cluster_runs"
    run_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    goal_id: Mapped[str] = mapped_column(ForeignKey("cluster_goals.goal_id"), nullable=False, index=True)
    actor_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    document: Mapped[dict] = mapped_column(JSONB, nullable=False)


class EventRecord(Base):
    __tablename__ = "cluster_events"
    goal_id: Mapped[str] = mapped_column(ForeignKey("cluster_goals.goal_id"), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    run_id: Mapped[str] = mapped_column(ForeignKey("cluster_runs.run_id"), nullable=False)
    document: Mapped[dict] = mapped_column(JSONB, nullable=False)


class TaskRecord(Base):
    __tablename__ = "cluster_tasks"
    __table_args__ = (UniqueConstraint("request_id"),)
    run_id: Mapped[str] = mapped_column(ForeignKey("cluster_runs.run_id"), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="queued")
    spec: Mapped[dict] = mapped_column(JSONB, nullable=False)
    arguments: Mapped[dict | None] = mapped_column(JSONB)
    result: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(String(2000))


class MessageRecord(Base):
    __tablename__ = "cluster_messages"
    run_id: Mapped[str] = mapped_column(ForeignKey("cluster_runs.run_id"), primary_key=True)
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    document: Mapped[dict] = mapped_column(JSONB, nullable=False)


class ScheduleRecord(Base):
    __tablename__ = "cluster_schedules"
    schedule_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    goal_id: Mapped[str] = mapped_column(ForeignKey("cluster_goals.goal_id"), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(100), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    document: Mapped[dict] = mapped_column(JSONB, nullable=False)
    next_fire_at: Mapped[object | None] = mapped_column(DateTime(timezone=True))


class DecisionRecord(Base):
    __tablename__ = "cluster_decisions"
    run_id: Mapped[str] = mapped_column(ForeignKey("cluster_runs.run_id"), primary_key=True)
    step_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    request: Mapped[dict] = mapped_column(JSONB, nullable=False)
    response: Mapped[dict | None] = mapped_column(JSONB)


class ClusterStore:
    def __init__(self, engine):
        if engine.dialect.name != "postgresql":
            raise ValueError("目标存储需要PostgreSQL")
        self.engine = engine
        self.maker = sessionmaker(engine, expire_on_commit=False)

    def initialize(self):
        Base.metadata.create_all(self.engine)

    @staticmethod
    def _goal(session, goal_id, actor_id, *, lock=False):
        record = session.get(GoalRecord, goal_id, with_for_update=lock)
        if record is None:
            raise LookupError("目标不存在")
        if record.actor_id != actor_id:
            raise PermissionError("无权访问该目标")
        return record

    def _run(self, session, run_id, actor_id, *, lock=False):
        row = session.get(RunRecord, run_id)
        if row is None:
            raise LookupError("运行不存在")
        self._goal(session, row.goal_id, actor_id, lock=lock)
        if row.actor_id != actor_id:
            raise PermissionError("无权访问该运行")
        if lock:
            session.refresh(row, with_for_update=True)
        return row

    @staticmethod
    def _save_run(row, **changes):
        value = GoalRun.model_validate({**row.document, **changes, "updated_at": now()})
        row.status = value.status
        row.document = value.model_dump(mode="json")
        return value

    def create_goal(self, goal: GoalDefinition):
        goal = GoalDefinition.model_validate(goal)
        with self.maker.begin() as session:
            if session.get(GoalRecord, goal.goal_id) is not None:
                raise ValueError("目标编号已存在")
            session.add(GoalRecord(goal_id=goal.goal_id, actor_id=goal.actor_id,
                                   document=goal.model_dump(mode="json")))
        return goal

    def get_goal(self, goal_id, actor_id):
        with self.maker() as session:
            return GoalDefinition.model_validate(self._goal(session, goal_id, actor_id).document)

    def goal_state(self, goal_id, actor_id):
        with self.maker() as session:
            return self._goal(session, goal_id, actor_id).state

    def list_goals(self, actor_id):
        with self.maker() as session:
            return [GoalDefinition.model_validate(row.document) for row in session.scalars(
                select(GoalRecord).where(GoalRecord.actor_id == actor_id).order_by(GoalRecord.goal_id))]

    def update_goal(self, goal: GoalDefinition, *, expected_revision: int):
        with self.maker.begin() as session:
            row = self._goal(session, goal.goal_id, goal.actor_id, lock=True)
            if row.document["revision"] != expected_revision:
                raise ValueError("目标版本已改变")
            active = session.scalar(select(RunRecord).where(
                RunRecord.goal_id == goal.goal_id, RunRecord.status.not_in(TERMINAL | {"paused"})))
            if active is not None:
                raise ValueError("修改目标前需要暂停活动运行")
            value = GoalDefinition.model_validate({**goal.model_dump(), "revision": expected_revision + 1,
                                                  "created_at": row.document["created_at"]})
            row.document = value.model_dump(mode="json")
        return value

    def trigger(self, event: TriggerEvent, *, only_active: bool = False):
        value = event.model_dump(mode="json", exclude={"created_at"})
        signature = digest(value)
        with self.maker.begin() as session:
            goal = self._goal(session, event.goal_id, event.actor_id, lock=True)
            if only_active and goal.state != "active":
                return None
            prior = session.get(EventRecord, (event.goal_id, event.event_id))
            if prior is not None:
                if prior.content_sha256 != signature:
                    raise ValueError("事件编号相同但内容不同")
                return GoalRun.model_validate(session.get(RunRecord, prior.run_id).document)
            if goal.state == "paused":
                raise ValueError("目标已暂停")
            if goal.state == "cancelled":
                raise ValueError("目标已取消")
            run = GoalRun(goal_id=event.goal_id, actor_id=event.actor_id,
                          goal_revision=goal.document["revision"], trigger_id=event.event_id)
            session.add(RunRecord(run_id=run.run_id, goal_id=run.goal_id,
                                  actor_id=run.actor_id, status=run.status,
                                  document=run.model_dump(mode="json")))
            session.flush()
            session.add(EventRecord(goal_id=event.goal_id, event_id=event.event_id,
                                    run_id=run.run_id, content_sha256=signature,
                                    document=event.model_dump(mode="json")))
        return run

    def get_run(self, run_id, actor_id):
        with self.maker() as session:
            return GoalRun.model_validate(self._run(session, run_id, actor_id).document)

    def list_runs(self, goal_id, actor_id):
        with self.maker() as session:
            self._goal(session, goal_id, actor_id)
            return [GoalRun.model_validate(row.document) for row in session.scalars(
                select(RunRecord).where(RunRecord.goal_id == goal_id).order_by(RunRecord.run_id))]

    def runnable(self):
        with self.maker() as session:
            return [GoalRun.model_validate(row.document) for row in session.scalars(select(RunRecord).where(
                RunRecord.status.in_(["queued", "running"])).order_by(RunRecord.run_id))]

    def set_run(self, run_id, actor_id, **changes):
        allowed = {"status", "summary", "error", "spent_cny", "reserved_cny", "decision_count"}
        if set(changes) - allowed:
            raise ValueError("运行更新字段无效")
        with self.maker.begin() as session:
            row = self._run(session, run_id, actor_id, lock=True)
            if row.status == "cancelled" and changes.get("status", "cancelled") != "cancelled":
                raise ValueError("已取消运行不能继续调度")
            if row.status == "paused" and changes.get("status", "paused") not in {"paused", "cancelled"}:
                raise ValueError("暂停运行需要通过目标控制恢复")
            return self._save_run(row, **changes)

    def control_goal(self, goal_id, actor_id, action):
        if action not in {"pause", "resume", "cancel"}:
            raise ValueError("目标控制操作无效")
        with self.maker.begin() as session:
            goal = self._goal(session, goal_id, actor_id, lock=True)
            if goal.state == "cancelled":
                if action != "cancel":
                    raise ValueError("已取消目标不能恢复")
                return
            goal.state = {"pause": "paused", "resume": "active", "cancel": "cancelled"}[action]
            for run in session.scalars(select(RunRecord).where(RunRecord.goal_id == goal_id).with_for_update()):
                if run.status in TERMINAL:
                    continue
                if action == "resume":
                    if run.status in {"paused", "waiting", "waiting_confirmation", "waiting_dependencies"}:
                        self._save_run(run, status="queued")
                else:
                    self._save_run(run, status=goal.state)

    def save_plan(self, run_id, actor_id, plan: TaskPlan, *, expected_version: int):
        plan = TaskPlan.model_validate(plan)
        with self.maker.begin() as session:
            row = self._run(session, run_id, actor_id, lock=True)
            goal = self._goal(session, row.goal_id, actor_id)
            if row.document["plan_version"] != expected_version:
                raise ValueError("运行计划版本已改变")
            if row.document["goal_revision"] != goal.document["revision"]:
                raise ValueError("运行目标版本已改变")
            if goal.state != "active" or row.status in TERMINAL or row.status == "paused":
                raise ValueError("当前运行不允许新增调度")
            definition = GoalDefinition.model_validate(goal.document)
            existing = {task.task_id: task for task in session.scalars(select(TaskRecord).where(
                TaskRecord.run_id == run_id))}
            if len(set(existing) | {task.task_id for task in plan.tasks}) > definition.max_tasks:
                raise PlanRejected("目标任务数量达到上限")
            for spec in plan.tasks:
                allowed = definition.allowed_operations.get(spec.tool_name, [])
                if spec.action not in allowed:
                    raise PermissionError("目标没有授权该工具操作")
                value = spec.model_dump(mode="json")
                prior = existing.get(spec.task_id)
                if prior is not None:
                    if prior.spec != value:
                        raise PlanRejected(f"原任务编号{spec.task_id}不能改变调用内容；"
                                           "请保留原定义，使用新的task_id登记修正任务及其依赖")
                    continue
                session.add(TaskRecord(run_id=run_id, task_id=spec.task_id,
                                       request_id=str(uuid4()), spec=value))
            return self._save_run(row, plan_version=expected_version + 1)

    def tasks(self, run_id, actor_id):
        with self.maker() as session:
            self._run(session, run_id, actor_id)
            return [{"task_id": task.task_id, "request_id": task.request_id,
                     "status": task.status, "spec": task.spec, "arguments": task.arguments,
                     "result": task.result, "error": task.error}
                    for task in session.scalars(select(TaskRecord).where(
                        TaskRecord.run_id == run_id).order_by(TaskRecord.task_id))]

    def set_task(self, run_id, actor_id, task_id, *, status, arguments=None, result=None, error=None):
        with self.maker.begin() as session:
            row = self._run(session, run_id, actor_id, lock=True)
            task = session.get(TaskRecord, (run_id, task_id), with_for_update=True)
            if task is None:
                raise LookupError("任务不存在")
            if status == "sent" and row.status not in {"queued", "running"}:
                raise ValueError("运行停止后禁止新增调用")
            if arguments is not None:
                if task.arguments is not None and task.arguments != arguments:
                    raise ValueError("保存的请求内容不能改变")
                task.arguments = arguments
            task.status = status
            if result is not None:
                task.result = result
            task.error = error
            if status == "unknown" and row.status in {"queued", "running"}:
                self._save_run(row, status="unknown", error=error)

    def append_message(self, run_id, actor_id, message):
        with self.maker.begin() as session:
            self._run(session, run_id, actor_id, lock=True)
            latest = session.scalar(select(MessageRecord.sequence).where(
                MessageRecord.run_id == run_id).order_by(MessageRecord.sequence.desc()).limit(1))
            sequence = 0 if latest is None else latest + 1
            session.add(MessageRecord(run_id=run_id, sequence=sequence, document=message))
        return sequence

    def append_tool_result(self, run_id, actor_id, call_id, result):
        with self.maker.begin() as session:
            self._run(session, run_id, actor_id, lock=True)
            self._append_tool(session, run_id, call_id, result)

    @staticmethod
    def _append_tool(session, run_id, call_id, result):
        messages = list(session.scalars(select(MessageRecord).where(MessageRecord.run_id == run_id)))
        previous = next((item.document for item in messages if item.document.get("role") == "tool"
                         and item.document.get("tool_call_id") == call_id), None)
        document = {"role": "tool", "tool_call_id": call_id,
                    "content": json.dumps(result, ensure_ascii=False)}
        if previous is not None:
            if previous != document:
                raise ValueError("工具调用编号对应结果已经改变")
            return
        sequence = max((item.sequence for item in messages), default=-1) + 1
        session.add(MessageRecord(run_id=run_id, sequence=sequence, document=document))

    def finalize_tool(self, run_id, actor_id, call_id, result, **changes):
        with self.maker.begin() as session:
            run = self._run(session, run_id, actor_id, lock=True)
            existing = session.scalar(select(MessageRecord).where(
                MessageRecord.run_id == run_id,
                MessageRecord.document["role"].astext == "tool",
                MessageRecord.document["tool_call_id"].astext == call_id))
            if existing is not None:
                return json.loads(existing.document["content"])
            if changes and run.status in {"running", "queued"}:
                self._save_run(run, **changes)
            self._append_tool(session, run_id, call_id, result)
            return result

    def prepare_decision(self, run_id, actor_id, step_key, request):
        with self.maker.begin() as session:
            self._run(session, run_id, actor_id, lock=True)
            row = session.get(DecisionRecord, (run_id, step_key))
            if row is not None:
                return row.request, row.response
            session.add(DecisionRecord(run_id=run_id, step_key=step_key, request=request))
            return request, None

    def decision(self, run_id, actor_id, step_key):
        with self.maker() as session:
            self._run(session, run_id, actor_id)
            row = session.get(DecisionRecord, (run_id, step_key))
            return None if row is None else (row.request, row.response)

    def commit_decision(self, run_id, actor_id, step_key, response):
        with self.maker.begin() as session:
            run = self._run(session, run_id, actor_id, lock=True)
            row = session.get(DecisionRecord, (run_id, step_key), with_for_update=True)
            if row is None:
                raise LookupError("协调请求尚未登记")
            if row.response is not None:
                if row.response != response:
                    raise ValueError("同一协调请求不能改变响应")
                return
            row.response = response
            latest = session.scalar(select(MessageRecord.sequence).where(
                MessageRecord.run_id == run_id).order_by(MessageRecord.sequence.desc()).limit(1))
            session.add(MessageRecord(run_id=run_id, sequence=0 if latest is None else latest + 1,
                                      document=response["choices"][0]["message"]))
            self._save_run(run, decision_count=run.document["decision_count"] + 1)

    def history(self, run_id, actor_id):
        with self.maker() as session:
            self._run(session, run_id, actor_id)
            return [row.document for row in session.scalars(select(MessageRecord).where(
                MessageRecord.run_id == run_id).order_by(MessageRecord.sequence))]

    def save_schedule(self, schedule: ScheduleDefinition):
        with self.maker.begin() as session:
            self._goal(session, schedule.goal_id, schedule.actor_id, lock=True)
            prior = session.get(ScheduleRecord, schedule.schedule_id, with_for_update=True)
            if prior is not None and (prior.actor_id != schedule.actor_id or prior.goal_id != schedule.goal_id):
                raise PermissionError("日程编号不属于当前目标")
            if prior is None:
                prior = ScheduleRecord(schedule_id=schedule.schedule_id, goal_id=schedule.goal_id,
                                       actor_id=schedule.actor_id, document={})
                session.add(prior)
            changed = prior.document != schedule.model_dump(mode="json")
            prior.enabled = schedule.enabled
            prior.document = schedule.model_dump(mode="json")
            if changed:
                prior.next_fire_at = None
        return schedule

    def schedule_due(self, schedule_id, trigger, *, expected_definition=None):
        with self.maker.begin() as session:
            row = session.get(ScheduleRecord, schedule_id)
            if row is None or not row.enabled:
                return None
            goal = self._goal(session, row.goal_id, row.actor_id, lock=True)
            session.refresh(row, with_for_update=True)
            if not row.enabled:
                return None
            if expected_definition is not None and row.document != expected_definition:
                return None
            if goal.state != "active":
                return None
            current = now()
            due = row.next_fire_at
            if due is None:
                row.next_fire_at = trigger.get_next_fire_time(None, current)
                return None
            if due > current:
                return None
            missed = 0
            latest = due
            following = trigger.get_next_fire_time(due, due)
            while following is not None and following <= current:
                latest = following
                following = trigger.get_next_fire_time(following, following)
                missed += 1
            row.next_fire_at = following
            event = TriggerEvent(event_id=f"schedule:{schedule_id}:{latest.isoformat()}",
                                 goal_id=row.goal_id, actor_id=row.actor_id, kind="schedule",
                                 scheduled_at=latest, payload={"schedule_id": schedule_id,
                                                              "coalesced_periods": missed})
            # 日程推进和事件创建属于同一事务。
            signature = digest(event.model_dump(mode="json", exclude={"created_at"}))
            run = GoalRun(goal_id=goal.goal_id, actor_id=goal.actor_id,
                          goal_revision=goal.document["revision"], trigger_id=event.event_id)
            session.add(RunRecord(run_id=run.run_id, goal_id=run.goal_id, actor_id=run.actor_id,
                                  status=run.status, document=run.model_dump(mode="json")))
            session.flush()
            session.add(EventRecord(goal_id=goal.goal_id, event_id=event.event_id,
                                    content_sha256=signature, run_id=run.run_id,
                                    document=event.model_dump(mode="json")))
            return run

    def schedules(self, actor_id=None):
        with self.maker() as session:
            statement = select(ScheduleRecord)
            if actor_id is not None:
                statement = statement.where(ScheduleRecord.actor_id == actor_id)
            return [ScheduleDefinition.model_validate(row.document) for row in session.scalars(statement)]

    @contextmanager
    def lease(self, key):
        with self.engine.connect() as connection:
            acquired = connection.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                                         {"key": "agent-cluster:" + key})
            connection.commit()
            try:
                yield bool(acquired)
            finally:
                if acquired:
                    connection.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                                       {"key": "agent-cluster:" + key})
                    connection.commit()
