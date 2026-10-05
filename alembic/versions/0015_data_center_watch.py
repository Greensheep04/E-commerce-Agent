import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0015_data_center_watch"
down_revision = "0014_data_center_request"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("data_center_watches",
        sa.Column("watch_id", sa.String(36), primary_key=True),
        sa.Column("goal_id", sa.String(200), nullable=False, unique=True),
        sa.Column("actor_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("source_ids", postgresql.JSONB(), nullable=False),
        sa.Column("operation", sa.String(30), nullable=False),
        sa.Column("committed_signature", sa.String(64), nullable=True),
        sa.Column("observed_signatures", postgresql.JSONB(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False))


def downgrade():
    raise RuntimeError("删除数据中台日程记录需要用户批准")
