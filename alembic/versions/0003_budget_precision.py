import sqlalchemy as sa

from alembic import op

revision = "0003_budget_precision"
down_revision = "0002_harness_budget"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("harness_tasks", sa.Column("started_at", sa.DateTime(timezone=True)))
    op.alter_column("budget_accounts", "spent_cny", type_=sa.Numeric(18, 8))
    op.alter_column("budget_accounts", "reserved_cny", type_=sa.Numeric(18, 8))
    op.alter_column("provider_requests", "reservation_cny", type_=sa.Numeric(18, 8))
    op.alter_column("provider_requests", "charged_cny", type_=sa.Numeric(18, 8))


def downgrade():
    op.drop_column("harness_tasks", "started_at")
    op.alter_column("provider_requests", "charged_cny", type_=sa.Numeric(12, 6))
    op.alter_column("provider_requests", "reservation_cny", type_=sa.Numeric(12, 6))
    op.alter_column("budget_accounts", "reserved_cny", type_=sa.Numeric(12, 6))
    op.alter_column("budget_accounts", "spent_cny", type_=sa.Numeric(12, 6))
