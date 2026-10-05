from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shop.models import Base, now


class OrderDataset(Base):
    __tablename__ = "order_datasets"
    __table_args__ = (
        ForeignKeyConstraint(["source_id", "revision"], ["sources.source_id", "sources.revision"]),
    )
    revision: Mapped[str] = mapped_column(String(200), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    manifest: Mapped[dict] = mapped_column(JSONB, nullable=False)
    counts: Mapped[dict] = mapped_column(JSONB, nullable=False)
    imported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class CustomerProfile(Base):
    __tablename__ = "customer_profiles"
    source_revision: Mapped[str] = mapped_column(ForeignKey("order_datasets.revision"), primary_key=True)
    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int | None] = mapped_column(Integer)
    store_id: Mapped[int | None] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    person_name: Mapped[str | None] = mapped_column(String(500))
    store_name: Mapped[str | None] = mapped_column(String(500))
    source_fields: Mapped[list] = mapped_column(JSONB, nullable=False)


class CustomerBinding(Base):
    __tablename__ = "customer_bindings"
    __table_args__ = (
        ForeignKeyConstraint(["source_revision", "customer_id"],
                             ["customer_profiles.source_revision", "customer_profiles.customer_id"]),
    )
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), primary_key=True)
    source_revision: Mapped[str] = mapped_column(String(200), nullable=False)
    customer_id: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class OrderProduct(Base):
    __tablename__ = "order_products"
    source_revision: Mapped[str] = mapped_column(ForeignKey("order_datasets.revision"), primary_key=True)
    product_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    product_number: Mapped[str] = mapped_column(String(100), nullable=False)
    color: Mapped[str | None] = mapped_column(String(100))
    size: Mapped[str | None] = mapped_column(String(100))
    list_price: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    source_fields: Mapped[list] = mapped_column(JSONB, nullable=False)


class CustomerOrder(Base):
    __tablename__ = "customer_orders"
    __table_args__ = (
        ForeignKeyConstraint(["source_revision", "customer_id"],
                             ["customer_profiles.source_revision", "customer_profiles.customer_id"]),
        UniqueConstraint("source_revision", "order_number", name="uq_customer_order_number"),
        Index("ix_customer_order_scope_date", "source_revision", "customer_id", "order_date", "order_id"),
    )
    source_revision: Mapped[str] = mapped_column(ForeignKey("order_datasets.revision"), primary_key=True)
    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_number: Mapped[str] = mapped_column(String(100), nullable=False)
    customer_id: Mapped[int] = mapped_column(Integer, nullable=False)
    order_date: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    due_date: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    ship_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    status: Mapped[int] = mapped_column(Integer, nullable=False)
    online: Mapped[bool] = mapped_column(nullable=False)
    sales_person_id: Mapped[int | None] = mapped_column(Integer)
    subtotal: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    tax: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    freight: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    total_due: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    source_fields: Mapped[list] = mapped_column(JSONB, nullable=False)


class OrderLine(Base):
    __tablename__ = "order_lines"
    __table_args__ = (
        ForeignKeyConstraint(["source_revision", "order_id"],
                             ["customer_orders.source_revision", "customer_orders.order_id"]),
        ForeignKeyConstraint(["source_revision", "product_id"],
                             ["order_products.source_revision", "order_products.product_id"]),
        Index("ix_order_line_order", "source_revision", "order_id", "line_id"),
        Index("ix_order_line_product", "source_revision", "product_id", "order_id"),
    )
    source_revision: Mapped[str] = mapped_column(String(200), primary_key=True)
    line_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(Integer, nullable=False)
    product_id: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    discount: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    line_total: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    source_fields: Mapped[list] = mapped_column(JSONB, nullable=False)
