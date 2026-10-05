from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shop.models import Base, now


class DataCenterSource(Base):
    __tablename__ = "data_center_sources"

    source_id: Mapped[str] = mapped_column(String(300), primary_key=True)
    kind: Mapped[str] = mapped_column(String(30), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    revision: Mapped[str] = mapped_column(String(200), nullable=False)
    nature: Mapped[str] = mapped_column(String(40), nullable=False)
    license_id: Mapped[str] = mapped_column(String(200), nullable=False)
    resource: Mapped[dict | None] = mapped_column(JSONB)
    relative_path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    properties: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class DataCenterGrant(Base):
    __tablename__ = "data_center_grants"

    actor_id: Mapped[str] = mapped_column(ForeignKey("users.id"), primary_key=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("data_center_sources.source_id"), primary_key=True)
    permissions: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    asset_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    source_wide: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class DataCenterSourceVersion(Base):
    __tablename__ = "data_center_source_versions"

    source_id: Mapped[str] = mapped_column(ForeignKey("data_center_sources.source_id"), primary_key=True)
    revision: Mapped[str] = mapped_column(String(200), primary_key=True)
    document: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now)


class DataCenterTaskBinding(Base):
    __tablename__ = "data_center_task_bindings"

    task_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    actor_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    request_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    operation: Mapped[str] = mapped_column(String(30), nullable=False)
    source_ids: Mapped[list] = mapped_column(JSONB, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class DataCenterWatch(Base):
    __tablename__ = "data_center_watches"

    watch_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    goal_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    actor_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    source_ids: Mapped[list] = mapped_column(JSONB, nullable=False)
    operation: Mapped[str] = mapped_column(String(30), nullable=False)
    committed_signature: Mapped[str | None] = mapped_column(String(64))
    observed_signatures: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class DataCenterModelReuse(Base):
    __tablename__ = "data_center_model_reuses"

    task_id: Mapped[str] = mapped_column(ForeignKey("data_center_task_bindings.task_id"), primary_key=True)
    step_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    provider_request_id: Mapped[str] = mapped_column(ForeignKey("provider_requests.id"), nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    scope_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now)
