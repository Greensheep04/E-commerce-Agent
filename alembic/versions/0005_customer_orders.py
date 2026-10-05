from alembic import op
from shop.customer_models import (
    CustomerBinding,
    CustomerOrder,
    CustomerProfile,
    OrderDataset,
    OrderLine,
    OrderProduct,
)

revision = "0005_customer_orders"
down_revision = "0004_campaign_indexes"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    for model in (OrderDataset, CustomerProfile, OrderProduct, CustomerOrder, OrderLine, CustomerBinding):
        model.__table__.create(connection)


def downgrade():
    raise RuntimeError("数据库迁移需要明确的数据保留与恢复方案")
