import sqlalchemy as sa

from alembic import op

revision = "0014_data_center_request"
down_revision = "0013_data_center"
branch_labels = None
depends_on = None


def upgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("data_center_task_bindings")}
    if "request_id" not in columns:
        op.add_column("data_center_task_bindings", sa.Column("request_id", sa.String(200), nullable=True))


def downgrade():
    raise RuntimeError("删除数据中台持久记录需要用户批准")
