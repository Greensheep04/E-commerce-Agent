from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Numeric, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shop.models import Base, now


class IntegrationCall(Base):
    __tablename__ = "integration_calls"

    actor_id: Mapped[str] = mapped_column(ForeignKey("users.id"), primary_key=True)
    module_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    operation: Mapped[str] = mapped_column(String(80), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(80), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(100), nullable=False)
    upstream: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class IntegrationBudgetWindow(Base):
    __tablename__ = "integration_budget_windows"
    __table_args__ = (Index("uq_integration_active_budget", "active", unique=True,
                           postgresql_where=text("active")),)

    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    limit_cny: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    include_open_reservations: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class IntegrationIdentityMap(Base):
    __tablename__ = "integration_identity_maps"

    source_name: Mapped[str] = mapped_column(String(80), primary_key=True)
    source_user_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    target_user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    source_username: Mapped[str] = mapped_column(String(100), nullable=False)
    target_username: Mapped[str] = mapped_column(String(100), nullable=False)


class IntegrationHistoryImport(Base):
    __tablename__ = "integration_history_imports"

    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    report: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
