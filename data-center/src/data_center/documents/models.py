from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from data_center.database import Base


class DocumentSnapshotRow(Base):
    __tablename__ = "dc_document_snapshots"
    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    state: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict] = mapped_column(JSONB)
    graph_enabled: Mapped[bool] = mapped_column(Boolean)


class DocumentRow(Base):
    __tablename__ = "dc_documents"
    snapshot_id: Mapped[str] = mapped_column(ForeignKey("dc_document_snapshots.snapshot_id"), primary_key=True)
    source_id: Mapped[str] = mapped_column(Text, primary_key=True)
    record_id: Mapped[str] = mapped_column(Text, primary_key=True)
    asset_id: Mapped[str] = mapped_column(Text, index=True)
    source_revision: Mapped[str] = mapped_column(Text)
    source_sha256: Mapped[str] = mapped_column(String(64))
    content_sha256: Mapped[str] = mapped_column(String(64))
    converted: Mapped[dict] = mapped_column(JSONB)


class DocumentChunkRow(Base):
    __tablename__ = "dc_document_chunks"
    snapshot_id: Mapped[str] = mapped_column(ForeignKey("dc_document_snapshots.snapshot_id"), primary_key=True)
    chunk_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_id: Mapped[str] = mapped_column(Text, index=True)
    record_id: Mapped[str] = mapped_column(Text)
    asset_id: Mapped[str] = mapped_column(Text, index=True)
    text: Mapped[str] = mapped_column(Text)
    text_sha256: Mapped[str] = mapped_column(String(64))
    evidence: Mapped[dict] = mapped_column(JSONB)
    metadata_payload: Mapped[dict] = mapped_column(JSONB)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
    embedding_call_id: Mapped[str | None] = mapped_column(Text, nullable=True)


class DocumentRequestRow(Base):
    __tablename__ = "dc_document_requests"
    actor_id: Mapped[str] = mapped_column(Text, primary_key=True)
    request_id: Mapped[str] = mapped_column(Text, primary_key=True)
    operation: Mapped[str] = mapped_column(String(32), primary_key=True)
    input_sha256: Mapped[str] = mapped_column(String(64))
    task_id: Mapped[str] = mapped_column(Text)
    snapshot_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)


class GraphIndexRow(Base):
    __tablename__ = "dc_graph_indexes"
    fingerprint: Mapped[str] = mapped_column(String(64), primary_key=True)
    policy_version: Mapped[str] = mapped_column(String(64), index=True)
    scope_fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    state: Mapped[str] = mapped_column(String(32))
    snapshot_ids: Mapped[list] = mapped_column(JSONB)
    allowed_chunk_ids: Mapped[list] = mapped_column(JSONB)
    config_sha256: Mapped[str] = mapped_column(String(64))
    call_ids: Mapped[list] = mapped_column(JSONB)
    chunk_count: Mapped[int] = mapped_column(Integer)


class GraphChunkRefRow(Base):
    __tablename__ = "dc_graph_chunk_refs"
    fingerprint: Mapped[str] = mapped_column(ForeignKey("dc_graph_indexes.fingerprint"), primary_key=True)
    graph_chunk_id: Mapped[str] = mapped_column(Text, primary_key=True)
    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    chunk_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_id: Mapped[str] = mapped_column(Text, index=True)
    record_id: Mapped[str] = mapped_column(Text)
    asset_id: Mapped[str] = mapped_column(Text, index=True)
    text_sha256: Mapped[str] = mapped_column(String(64))


class GraphScopeAliasRow(Base):
    __tablename__ = "dc_graph_scope_aliases"

    alias_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    actor_id: Mapped[str] = mapped_column(Text, index=True)
    policy_version: Mapped[str] = mapped_column(String(64), index=True)
    scope_fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    graph_fingerprint: Mapped[str] = mapped_column(ForeignKey("dc_graph_indexes.fingerprint"), index=True)
    proof: Mapped[dict] = mapped_column(JSONB)
