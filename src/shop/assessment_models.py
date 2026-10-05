from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shop.models import Base, new_id, now


class AssessmentSource(Base):
    __tablename__ = "assessment_sources"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    revision: Mapped[str] = mapped_column(String(40), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest: Mapped[dict] = mapped_column(JSONB, nullable=False)
    imported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class AssessmentEmployee(Base):
    __tablename__ = "assessment_employees"

    employee_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_key: Mapped[str] = mapped_column(ForeignKey("assessment_sources.id"), nullable=False)
    job_title: Mapped[str] = mapped_column(String(200), nullable=False)
    current_flag: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_salesperson: Mapped[bool] = mapped_column(Boolean, nullable=False)
    territory_id: Mapped[int | None] = mapped_column(Integer)
    source_line: Mapped[int] = mapped_column(Integer, nullable=False)
    salesperson_line: Mapped[int | None] = mapped_column(Integer)


class AssessmentQuota(Base):
    __tablename__ = "assessment_quotas"
    __table_args__ = (CheckConstraint("amount > 0", name="ck_assessment_quota_positive"),)

    employee_id: Mapped[int] = mapped_column(ForeignKey("assessment_employees.employee_id"), primary_key=True)
    quota_date: Mapped[datetime] = mapped_column(DateTime(), primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(38, 4), nullable=False)
    source_line: Mapped[int] = mapped_column(Integer, nullable=False)


class AssessmentAccess(Base):
    __tablename__ = "assessment_access"
    __table_args__ = (
        CheckConstraint("level IN ('employee', 'manager', 'admin')", name="ck_assessment_access_level"),
        CheckConstraint("level = 'admin' OR employee_id IS NOT NULL", name="ck_assessment_access_identity"),
    )

    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), primary_key=True)
    level: Mapped[str] = mapped_column(String(20), nullable=False)
    employee_id: Mapped[int | None] = mapped_column(
        ForeignKey("assessment_employees.employee_id"), unique=True,
    )
    managed_employee_ids: Mapped[list[int]] = mapped_column(JSONB, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class AssessmentRun(Base):
    __tablename__ = "assessment_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    run_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False, default=new_id)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    parent_id: Mapped[str | None] = mapped_column(ForeignKey("assessment_runs.id"))
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="created")
    scope_employee_ids: Mapped[list[int]] = mapped_column(JSONB, nullable=False)
    owner_level: Mapped[str] = mapped_column(String(20), nullable=False)
    model_profile: Mapped[dict] = mapped_column(JSONB, nullable=False)
    query: Mapped[dict | None] = mapped_column(JSONB)
    metrics: Mapped[dict | None] = mapped_column(JSONB)
    result: Mapped[dict | None] = mapped_column(JSONB)
    model_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tool_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    elapsed_seconds: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False, default=0)
    active_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancel_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stop_reason: Mapped[str | None] = mapped_column(String(60))
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class AssessmentToolCall(Base):
    __tablename__ = "assessment_tool_calls"
    __table_args__ = (UniqueConstraint("assessment_id", "step_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    assessment_id: Mapped[str] = mapped_column(ForeignKey("assessment_runs.id"), nullable=False)
    step_key: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    arguments: Mapped[dict] = mapped_column(JSONB, nullable=False)
    result: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
