"""Create the mock connection accounts table."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0005_connections"
down_revision = "0004_dead_letter"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connections",
        sa.Column("userid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("login", sa.String(128), nullable=False, unique=True),
        sa.Column("hashpass", sa.Text(), nullable=False),
        sa.Column("date_access", sa.DateTime(timezone=True), nullable=False),
        schema="ecommerce",
    )


def downgrade() -> None:
    op.drop_table("connections", schema="ecommerce")
