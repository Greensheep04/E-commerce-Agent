from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shop.models import Base, now


class CustomerConversation(Base):
    __tablename__ = 'customer_conversations'
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    owner_id: Mapped[str] = mapped_column(ForeignKey('users.id'), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    orders: Mapped[list] = mapped_column(JSONB, nullable=False)
    selected_order: Mapped[str | None] = mapped_column(String(100))
    context: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    pending_ticket: Mapped[dict | None] = mapped_column(JSONB)
    next_event: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class CustomerTurn(Base):
    __tablename__ = 'customer_turns'
    __table_args__ = (
        UniqueConstraint('conversation_id', 'request_id', name='uq_customer_turn_request'),
        Index('ix_customer_turn_history', 'conversation_id', 'received_at'),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    conversation_id: Mapped[str] = mapped_column(ForeignKey('customer_conversations.id'), nullable=False)
    request_id: Mapped[str] = mapped_column(String(36), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default='queued')
    model_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tool_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cancel_requested: Mapped[bool] = mapped_column(nullable=False, default=False)
    authorization: Mapped[dict | None] = mapped_column(JSONB)
    result: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    config: Mapped[dict] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    elapsed_seconds: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CustomerEvent(Base):
    __tablename__ = 'customer_events'
    conversation_id: Mapped[str] = mapped_column(ForeignKey('customer_conversations.id'), primary_key=True)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    turn_id: Mapped[str | None] = mapped_column(ForeignKey('customer_turns.id'))
    type: Mapped[str] = mapped_column(String(40), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class CustomerToolCall(Base):
    __tablename__ = 'customer_tool_calls'
    __table_args__ = (UniqueConstraint('turn_id', 'call_id', name='uq_customer_tool_call'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    turn_id: Mapped[str] = mapped_column(ForeignKey('customer_turns.id'), nullable=False)
    call_id: Mapped[str] = mapped_column(String(200), nullable=False)
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    arguments: Mapped[dict] = mapped_column(JSONB, nullable=False)
    result: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    duration_seconds: Mapped[Decimal | None] = mapped_column(Numeric(12, 6))
    attempts: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)


class CustomerTicket(Base):
    __tablename__ = 'customer_tickets'
    __table_args__ = (UniqueConstraint('conversation_id', 'authorization_id', name='uq_customer_ticket_authorization'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    number: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    conversation_id: Mapped[str] = mapped_column(ForeignKey('customer_conversations.id'), nullable=False)
    owner_id: Mapped[str] = mapped_column(ForeignKey('users.id'), nullable=False)
    turn_id: Mapped[str] = mapped_column(ForeignKey('customer_turns.id'), nullable=False)
    authorization_id: Mapped[str] = mapped_column(String(36), nullable=False)
    order_number: Mapped[str | None] = mapped_column(String(100))
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default='open')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
