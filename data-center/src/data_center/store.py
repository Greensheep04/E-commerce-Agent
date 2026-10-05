import json
from datetime import UTC, datetime
from hashlib import sha256
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, String, Text, UniqueConstraint, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker

from data_center.database import Base

TERMINAL_TASK_STATUSES = frozenset({
    "completed", "partial", "DATA_BLOCKED", "unknown", "failed", "budget_exhausted", "cancelled",
})


class DataTask(Base):
    __tablename__ = "dc_tasks"
    __table_args__ = (UniqueConstraint("actor_id", "operation", "request_id"),)

    task_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), unique=True)
    actor_id: Mapped[str] = mapped_column(Text)
    operation: Mapped[str] = mapped_column(String(30))
    request_id: Mapped[str] = mapped_column(Text)
    input_sha256: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSONB)
    upstream: Mapped[list] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(30))
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    checkpoints: Mapped[dict] = mapped_column(JSONB, default=dict)
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


def digest(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":")).encode()).hexdigest()


class DataStore:
    def __init__(self, database):
        self.database = database
        self.maker = sessionmaker(database, expire_on_commit=False)

    def create(self, operation, payload, context):
        upstream = [ref.model_dump(mode="json") for ref in context.upstream]
        fingerprint = digest({"payload": payload, "upstream": upstream})
        with self.maker.begin() as session:
            session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
                            {"key": f"dc-task:{context.actor_id}:{operation}:{context.request_id}"})
            prior = session.scalar(select(DataTask).where(
                DataTask.actor_id == context.actor_id, DataTask.operation == operation,
                DataTask.request_id == context.request_id,
            ))
            if prior is not None:
                if prior.input_sha256 != fingerprint:
                    raise ValueError("相同请求编号的输入或上游引用冲突")
                return prior
            now = datetime.now(UTC)
            task = DataTask(task_id=str(uuid4()), run_id=str(uuid4()), actor_id=context.actor_id,
                            operation=operation, request_id=context.request_id,
                            input_sha256=fingerprint, payload=payload, upstream=upstream,
                            status="queued", created_at=now, updated_at=now)
            session.add(task)
            session.flush()
            return task

    def get(self, task_id, actor_id):
        with self.maker() as session:
            task = session.get(DataTask, task_id)
            if task is None or task.actor_id != actor_id:
                raise PermissionError("任务不属于当前账号")
            session.expunge(task)
            return task

    def update(self, task_id, **changes):
        with self.maker.begin() as session:
            task = session.get(DataTask, task_id, with_for_update=True)
            if task is None:
                raise LookupError("数据任务不存在")
            if changes.get("cancel_requested") is True:
                if task.status in TERMINAL_TASK_STATUSES:
                    changes.pop("status", None)
                else:
                    changes["status"] = "cancelled"
            if task.cancel_requested and (
                    changes.get("status") not in {None, "cancelled"}
                    or task.status not in {"queued", "running", "cancelled"}):
                return task
            for key, value in changes.items():
                setattr(task, key, value)
            task.updated_at = datetime.now(UTC)
            session.flush()
            return task

    def checkpoint(self, task_id, step, value):
        with self.maker.begin() as session:
            task = session.get(DataTask, task_id, with_for_update=True)
            if task is None:
                raise LookupError("数据任务不存在")
            task.checkpoints = {**task.checkpoints, step: value}
            task.updated_at = datetime.now(UTC)

    def list(self, actor_id, limit=100):
        with self.maker() as session:
            tasks = session.scalars(select(DataTask).where(DataTask.actor_id == actor_id)
                                   .order_by(DataTask.created_at.desc()).limit(limit)).all()
            for task in tasks:
                session.expunge(task)
            return tasks
