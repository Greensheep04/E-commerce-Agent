from datetime import datetime
from decimal import Decimal

from pgvector.sqlalchemy import Vector
from sqlalchemy import DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shop.models import Base, now


class BudgetAccount(Base):
    __tablename__ = "budget_accounts"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    spent_cny: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False, default=0)
    reserved_cny: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False, default=0)


class HarnessTask(Base):
    __tablename__ = "harness_tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    owner_id: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    run_id: Mapped[str] = mapped_column(String(36), nullable=False)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    workflow_version: Mapped[str] = mapped_column(String(30), nullable=False, default="legacy")
    model_profile: Mapped[dict | None] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    model_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tool_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    elapsed_seconds: Mapped[Decimal] = mapped_column(Numeric(10, 3), nullable=False, default=0)
    cancel_requested: Mapped[bool] = mapped_column(nullable=False, default=False)
    result: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    stop_reason: Mapped[str | None] = mapped_column(String(60))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now)


class ProviderRequest(Base):
    __tablename__ = "provider_requests"
    __table_args__ = (
        UniqueConstraint("task_id", "step_key"),
        UniqueConstraint("run_id", "step_key", name="uq_provider_run_step"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str | None] = mapped_column(ForeignKey("harness_tasks.id"))
    assessment_id: Mapped[str | None] = mapped_column(ForeignKey("assessment_runs.id"))
    run_id: Mapped[str | None] = mapped_column(String(36))
    step_key: Mapped[str] = mapped_column(String(100), nullable=False)
    service: Mapped[str] = mapped_column(String(80), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    reservation_cny: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    charged_cny: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    price_basis: Mapped[dict] = mapped_column(JSONB, nullable=False)
    request_body: Mapped[dict] = mapped_column(JSONB, nullable=False)
    response_body: Mapped[dict | None] = mapped_column(JSONB)
    usage: Mapped[dict | None] = mapped_column(JSONB)
    provider_request_id: Mapped[str | None] = mapped_column(String(200))
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ToolCall(Base):
    __tablename__ = "harness_tool_calls"
    __table_args__ = (UniqueConstraint("task_id", "call_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("harness_tasks.id"), nullable=False)
    call_id: Mapped[str] = mapped_column(String(200), nullable=False)
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    arguments: Mapped[dict] = mapped_column(JSONB, nullable=False)
    result: Mapped[dict | None] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now)


class AssetIndex(Base):
    __tablename__ = "asset_indexes"
    __table_args__ = (
        UniqueConstraint("fingerprint"),
        Index("ix_asset_indexes_asset_version", "asset_version_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    asset_version_id: Mapped[str] = mapped_column(ForeignKey("asset_versions.id"), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    file_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    fields_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration: Mapped[dict] = mapped_column(JSONB, nullable=False)
    description: Mapped[dict | None] = mapped_column(JSONB)
    search_text: Mapped[str | None] = mapped_column(Text)
    embedding: Mapped[list | None] = mapped_column(Vector(1024))
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now, nullable=False)
