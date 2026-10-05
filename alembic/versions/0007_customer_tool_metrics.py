from sqlalchemy import Column, Numeric

from alembic import op

revision = '0007_customer_tool_metrics'
down_revision = '0006_customer_runtime'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('customer_tool_calls', Column('duration_seconds', Numeric(12, 6)))


def downgrade():
    raise RuntimeError('数据库迁移需要明确的数据保留与恢复方案')
