import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0010_integration_calls"
down_revision = "0009_merge_module_branches"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "integration_calls",
        sa.Column("actor_id", sa.String(36), sa.ForeignKey("users.id"), primary_key=True),
        sa.Column("module_id", sa.String(80), primary_key=True),
        sa.Column("operation", sa.String(80), primary_key=True),
        sa.Column("request_id", sa.String(100), primary_key=True),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(80), nullable=False),
        sa.Column("resource_id", sa.String(100), nullable=False),
        sa.Column("upstream", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "integration_budget_windows",
        sa.Column("id", sa.String(80), primary_key=True),
        sa.Column("limit_cny", sa.Numeric(18, 8), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("uq_integration_active_budget", "integration_budget_windows", ["active"],
                    unique=True, postgresql_where=sa.text("active"))


def downgrade():
    op.drop_table("integration_budget_windows")
    op.drop_table("integration_calls")
