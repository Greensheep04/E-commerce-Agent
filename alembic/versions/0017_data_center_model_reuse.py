import sqlalchemy as sa

from alembic import op

revision = "0017_data_center_model_reuse"
down_revision = "0016_data_center_source_version"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("data_center_model_reuses",
        sa.Column("task_id", sa.String(36), sa.ForeignKey("data_center_task_bindings.task_id"), primary_key=True),
        sa.Column("step_key", sa.String(100), primary_key=True),
        sa.Column("provider_request_id", sa.String(36), sa.ForeignKey("provider_requests.id"), nullable=False),
        sa.Column("request_sha256", sa.String(64), nullable=False),
        sa.Column("scope_fingerprint", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))


def downgrade():
    raise RuntimeError("删除模型复用记录需要用户批准")
