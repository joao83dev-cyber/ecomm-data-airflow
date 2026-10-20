"""Prevent retroactive mutation of historical addresses and closed order records."""
from alembic import op

revision = "0002_related_immutable"
down_revision = "0001_ecommerce_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE FUNCTION ecommerce.prevent_address_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'address versions are immutable; insert a new version' USING ERRCODE = '55000';
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_address_versions_immutable
        BEFORE UPDATE OR DELETE ON ecommerce.address
        FOR EACH ROW EXECUTE FUNCTION ecommerce.prevent_address_mutation();
    """)
    op.execute("""
        CREATE FUNCTION ecommerce.prevent_closed_order_item_mutation() RETURNS trigger AS $$
        DECLARE order_closed_at TIMESTAMPTZ;
        BEGIN
            SELECT closed_at INTO order_closed_at FROM ecommerce.orders WHERE id = OLD.order_id;
            IF order_closed_at IS NOT NULL THEN
                RAISE EXCEPTION 'items of closed order % are immutable', OLD.order_id USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_order_items_closed_immutable
        BEFORE UPDATE OR DELETE ON ecommerce.order_items
        FOR EACH ROW EXECUTE FUNCTION ecommerce.prevent_closed_order_item_mutation();
    """)
    op.execute("""
        CREATE FUNCTION ecommerce.prevent_final_payment_mutation() RETURNS trigger AS $$
        BEGIN
            IF OLD.status <> 'PENDING' THEN
                RAISE EXCEPTION 'final payment % is immutable', OLD.id USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_final_payment_immutable
        BEFORE UPDATE OR DELETE ON ecommerce.payment
        FOR EACH ROW EXECUTE FUNCTION ecommerce.prevent_final_payment_mutation();
    """)
    op.execute("""
        CREATE FUNCTION ecommerce.prevent_invoice_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'invoices are immutable' USING ERRCODE = '55000';
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_invoices_immutable
        BEFORE UPDATE OR DELETE ON ecommerce.invoices
        FOR EACH ROW EXECUTE FUNCTION ecommerce.prevent_invoice_mutation();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_address_versions_immutable ON ecommerce.address")
    op.execute("DROP FUNCTION IF EXISTS ecommerce.prevent_address_mutation()")
    op.execute("DROP TRIGGER IF EXISTS trg_order_items_closed_immutable ON ecommerce.order_items")
    op.execute("DROP FUNCTION IF EXISTS ecommerce.prevent_closed_order_item_mutation()")
    op.execute("DROP TRIGGER IF EXISTS trg_final_payment_immutable ON ecommerce.payment")
    op.execute("DROP FUNCTION IF EXISTS ecommerce.prevent_final_payment_mutation()")
    op.execute("DROP TRIGGER IF EXISTS trg_invoices_immutable ON ecommerce.invoices")
    op.execute("DROP FUNCTION IF EXISTS ecommerce.prevent_invoice_mutation()")
