from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = '0008_customer_tool_attempts'
down_revision = '0007_customer_tool_metrics'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('customer_tool_calls', Column('attempts', JSONB, nullable=False, server_default='[]'))


def downgrade():
    raise RuntimeError('数据库迁移需要明确的数据保留与恢复方案')
