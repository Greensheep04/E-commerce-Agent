import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0011_integration_history"
down_revision = "0010_integration_calls"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "integration_identity_maps",
        sa.Column("source_name", sa.String(80), primary_key=True),
        sa.Column("source_user_id", sa.String(36), primary_key=True),
        sa.Column("target_user_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("source_username", sa.String(100), nullable=False),
        sa.Column("target_username", sa.String(100), nullable=False),
    )
    op.create_table(
        "integration_history_imports",
        sa.Column("id", sa.String(80), primary_key=True),
        sa.Column("source_name", sa.String(80), nullable=False),
        sa.Column("source_sha256", sa.String(64), nullable=False),
        sa.Column("report", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade():
    op.drop_table("integration_history_imports")
    op.drop_table("integration_identity_maps")
