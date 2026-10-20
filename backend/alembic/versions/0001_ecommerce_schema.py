"""Create ecommerce schema with immutable order snapshots and delivery outbox."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_ecommerce_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS ecommerce")
    op.create_table(
        "clients",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("external_id", sa.String(128), nullable=False, unique=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        schema="ecommerce",
    )
    op.create_table(
        "address",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ecommerce.clients.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("external_id", sa.String(128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("line1", sa.String(250), nullable=False),
        sa.Column("line2", sa.String(250)),
        sa.Column("city", sa.String(120), nullable=False),
        sa.Column("region", sa.String(120), nullable=False),
        sa.Column("postal_code", sa.String(32), nullable=False),
        sa.Column("country", sa.String(2), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("client_id", "external_id", "version", name="uq_address_client_external_version"),
        schema="ecommerce",
    )
    op.create_table(
        "orders",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("external_id", sa.String(128), nullable=False, unique=True),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ecommerce.clients.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("address_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ecommerce.address.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="PENDING"),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("total_amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("customer_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("address_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("items_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("status IN ('PENDING', 'APPROVED', 'REJECTED', 'CANCELLED')", name="ck_orders_status"),
        sa.CheckConstraint("total_amount >= 0", name="ck_orders_total_nonnegative"),
        schema="ecommerce",
    )
    op.create_table(
        "order_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ecommerce.orders.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("line_number", sa.Integer(), nullable=False),
        sa.Column("sku", sa.String(128), nullable=False),
        sa.Column("description", sa.String(500), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("unit_price", sa.Numeric(14, 2), nullable=False),
        sa.UniqueConstraint("order_id", "line_number", name="uq_order_items_line"),
        sa.CheckConstraint("quantity > 0", name="ck_order_items_quantity_positive"),
        sa.CheckConstraint("unit_price >= 0", name="ck_order_items_price_nonnegative"),
        schema="ecommerce",
    )
    op.create_table(
        "order_event_history",
        sa.Column("event_id", sa.String(128), primary_key=True),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ecommerce.orders.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        schema="ecommerce",
    )
    op.create_table(
        "payment",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ecommerce.orders.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("provider_reference", sa.String(200), unique=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("amount >= 0", name="ck_payment_amount_nonnegative"),
        schema="ecommerce",
    )
    op.create_table(
        "invoices",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ecommerce.orders.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("invoice_number", sa.String(80), nullable=False, unique=True),
        sa.Column("amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        schema="ecommerce",
    )
    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.String(128), primary_key=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        schema="ecommerce",
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ecommerce.orders.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("topic", sa.String(100), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("delivered", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("attempts >= 0", name="ck_outbox_attempts_nonnegative"),
        sa.UniqueConstraint("order_id", "topic", name="uq_outbox_order_topic"),
        schema="ecommerce",
    )
    op.create_index("ix_outbox_delivery", "outbox_events", ["delivered", "next_attempt_at"], schema="ecommerce")
    op.execute("""
        CREATE FUNCTION ecommerce.prevent_closed_order_mutation() RETURNS trigger AS $$
        BEGIN
            IF OLD.closed_at IS NOT NULL THEN
                RAISE EXCEPTION 'closed order % is immutable', OLD.id USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_orders_immutable_closed
        BEFORE UPDATE OR DELETE ON ecommerce.orders
        FOR EACH ROW EXECUTE FUNCTION ecommerce.prevent_closed_order_mutation();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_orders_immutable_closed ON ecommerce.orders")
    op.execute("DROP FUNCTION IF EXISTS ecommerce.prevent_closed_order_mutation()")
    op.drop_index("ix_outbox_delivery", table_name="outbox_events", schema="ecommerce")
    for table in ("outbox_events", "processed_events", "order_event_history", "invoices", "payment", "order_items", "orders", "address", "clients"):
        op.drop_table(table, schema="ecommerce")
    op.execute("DROP SCHEMA IF EXISTS ecommerce")
