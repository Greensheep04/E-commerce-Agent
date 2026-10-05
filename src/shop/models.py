from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def new_id() -> str:
    return str(uuid4())


def now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    username: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class Source(Base):
    __tablename__ = "sources"

    source_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    revision: Mapped[str] = mapped_column(String(200), primary_key=True)
    license: Mapped[str | None] = mapped_column(String(200))
    license_note: Mapped[str | None] = mapped_column(Text)
    url: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class ImportBatch(Base):
    __tablename__ = "import_batches"
    __table_args__ = (
        ForeignKeyConstraint(["source_id", "source_revision"], ["sources.source_id", "sources.revision"]),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_revision: Mapped[str] = mapped_column(String(200), nullable=False)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    expected_count: Mapped[int] = mapped_column(Integer, nullable=False)
    imported_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    manifest: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Asset(Base):
    __tablename__ = "assets"
    __table_args__ = (
        UniqueConstraint("source_id", "source_revision", "record_id"),
        ForeignKeyConstraint(["source_id", "source_revision"], ["sources.source_id", "sources.revision"]),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_revision: Mapped[str] = mapped_column(String(200), nullable=False)
    record_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_order: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    tags: Mapped[list[str]] = mapped_column(JSONB, default=list, nullable=False)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    versions: Mapped[list["AssetVersion"]] = relationship(
        back_populates="asset", order_by="AssetVersion.version", lazy="selectin"
    )


class AssetVersion(Base):
    __tablename__ = "asset_versions"
    __table_args__ = (UniqueConstraint("asset_id", "version"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    asset_id: Mapped[str] = mapped_column(ForeignKey("assets.id"), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    file_size: Mapped[int] = mapped_column(Integer, nullable=False)
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(200), nullable=False)
    original_fields: Mapped[dict] = mapped_column(JSONB, nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    tags: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    asset: Mapped[Asset] = relationship(back_populates="versions")
