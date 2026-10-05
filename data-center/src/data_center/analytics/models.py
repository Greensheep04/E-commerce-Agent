from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Date,
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

from data_center.database import Base


class AnalyticsSnapshot(Base):
    __tablename__ = "data_center_analytics_snapshot"

    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_snapshot_id: Mapped[str] = mapped_column(String(64), nullable=False)
    source_revision: Mapped[str] = mapped_column(String(200), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    task_id: Mapped[str] = mapped_column(String(200), nullable=False)
    run_id: Mapped[str] = mapped_column(String(200), nullable=False)
    quality: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class AnalyticsSyncRun(Base):
    __tablename__ = "data_center_analytics_sync_run"
    __table_args__ = (UniqueConstraint("task_id", "run_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[str] = mapped_column(String(200), nullable=False)
    run_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_snapshot_id: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    next_cursor: Mapped[str | None] = mapped_column(Text)
    pages_read: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    records_read: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AnalyticsSyncPage(Base):
    __tablename__ = "data_center_analytics_sync_page"
    __table_args__ = (UniqueConstraint("task_id", "run_id", "page_number"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[str] = mapped_column(String(200), nullable=False)
    run_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_snapshot_id: Mapped[str] = mapped_column(String(64), nullable=False)
    page_number: Mapped[int] = mapped_column(Integer, nullable=False)
    cursor: Mapped[str | None] = mapped_column(Text)
    next_cursor: Mapped[str | None] = mapped_column(Text)
    rows: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)


class AnalyticsRecord(Base):
    __tablename__ = "data_center_analytics_record"
    __table_args__ = (UniqueConstraint("snapshot_id", "record_type", "record_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[str] = mapped_column(
        ForeignKey("data_center_analytics_snapshot.snapshot_id"), nullable=False, index=True,
    )
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_revision: Mapped[str] = mapped_column(String(200), nullable=False)
    record_type: Mapped[str] = mapped_column(String(16), nullable=False)
    record_id: Mapped[str] = mapped_column(String(200), nullable=False)
    order_id: Mapped[int | None] = mapped_column(Integer)
    employee_id: Mapped[int | None] = mapped_column(Integer, index=True)
    order_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), index=True)
    status: Mapped[int | None] = mapped_column(Integer)
    subtotal: Mapped[Decimal | None] = mapped_column(Numeric(38, 6))
    period_start: Mapped[date | None] = mapped_column(Date)
    period_end: Mapped[date | None] = mapped_column(Date)
    quota_amount: Mapped[Decimal | None] = mapped_column(Numeric(38, 6))
    source_line: Mapped[int] = mapped_column(Integer, nullable=False)
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
