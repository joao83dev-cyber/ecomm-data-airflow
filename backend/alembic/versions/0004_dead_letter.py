"""Add an operator-inspectable dead letter table for invalid Kafka events."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0004_dead_letter"
down_revision = "0003_payment_payload"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dead_letter_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        schema="ecommerce",
    )


def downgrade() -> None:
    op.drop_table("dead_letter_events", schema="ecommerce")
