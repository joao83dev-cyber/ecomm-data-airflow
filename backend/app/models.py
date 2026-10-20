from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

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
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

SCHEMA = "ecommerce"


class Client(Base):
    __tablename__ = "clients"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    external_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Address(Base):
    __tablename__ = "address"
    __table_args__ = (
        UniqueConstraint("client_id", "external_id", "version", name="uq_address_client_external_version"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    client_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey(f"{SCHEMA}.clients.id", ondelete="RESTRICT"), nullable=False)
    external_id: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    line1: Mapped[str] = mapped_column(String(250), nullable=False)
    line2: Mapped[str | None] = mapped_column(String(250))
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    region: Mapped[str] = mapped_column(String(120), nullable=False)
    postal_code: Mapped[str] = mapped_column(String(32), nullable=False)
    country: Mapped[str] = mapped_column(String(2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("status IN ('PENDING', 'APPROVED', 'REJECTED', 'CANCELLED')", name="ck_orders_status"),
        CheckConstraint("total_amount >= 0", name="ck_orders_total_nonnegative"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    external_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    client_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey(f"{SCHEMA}.clients.id", ondelete="RESTRICT"), nullable=False)
    address_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey(f"{SCHEMA}.address.id", ondelete="RESTRICT"), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="PENDING")
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    customer_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)
    address_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)
    items_snapshot: Mapped[list] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        UniqueConstraint("order_id", "line_number", name="uq_order_items_line"),
        CheckConstraint("quantity > 0", name="ck_order_items_quantity_positive"),
        CheckConstraint("unit_price >= 0", name="ck_order_items_price_nonnegative"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey(f"{SCHEMA}.orders.id", ondelete="RESTRICT"), nullable=False)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    sku: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)


class Payment(Base):
    __tablename__ = "payment"
    __table_args__ = (
        CheckConstraint("amount >= 0", name="ck_payment_amount_nonnegative"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey(f"{SCHEMA}.orders.id", ondelete="RESTRICT"), unique=True, nullable=False)
    provider_reference: Mapped[str | None] = mapped_column(String(200), unique=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Invoice(Base):
    __tablename__ = "invoices"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey(f"{SCHEMA}.orders.id", ondelete="RESTRICT"), unique=True, nullable=False)
    invoice_number: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    __table_args__ = {"schema": SCHEMA}

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OrderEventHistory(Base):
    __tablename__ = "order_event_history"
    __table_args__ = {"schema": SCHEMA}

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    order_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey(f"{SCHEMA}.orders.id", ondelete="RESTRICT"), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("attempts >= 0", name="ck_outbox_attempts_nonnegative"),
        UniqueConstraint("order_id", "topic", name="uq_outbox_order_topic"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey(f"{SCHEMA}.orders.id", ondelete="RESTRICT"), nullable=False)
    topic: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    delivered: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
