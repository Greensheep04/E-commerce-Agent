import sqlalchemy as sa

from alembic import op

revision = "0013_data_center"
down_revision = "0012_agent_cluster"
branch_labels = None
depends_on = None


def upgrade():
    from shop.data_center_models import DataCenterGrant, DataCenterSource, DataCenterTaskBinding

    for table in (DataCenterSource.__table__, DataCenterGrant.__table__, DataCenterTaskBinding.__table__):
        table.create(op.get_bind())
    op.add_column("integration_budget_windows", sa.Column(
        "include_open_reservations", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.alter_column("integration_budget_windows", "include_open_reservations", server_default=None)


def downgrade():
    raise RuntimeError("删除数据中台持久记录需要用户批准")
