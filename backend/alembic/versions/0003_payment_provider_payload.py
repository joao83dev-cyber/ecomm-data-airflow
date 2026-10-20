"""Persist the payment provider event snapshot."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0003_payment_payload"
down_revision = "0002_related_immutable"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "payment",
        sa.Column("provider_payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        schema="ecommerce",
    )


def downgrade() -> None:
    op.drop_column("payment", "provider_payload", schema="ecommerce")
