import sqlalchemy as sa
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0004_campaign_indexes"
down_revision = "0003_budget_precision"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
    op.add_column("harness_tasks", sa.Column("workflow_version", sa.String(30),
                                          server_default="legacy", nullable=False))
    op.add_column("harness_tasks", sa.Column("model_profile", postgresql.JSONB()))
    op.create_table(
        "asset_indexes",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("asset_version_id", sa.String(36), sa.ForeignKey("asset_versions.id"), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False, unique=True),
        sa.Column("file_sha256", sa.String(64), nullable=False),
        sa.Column("fields_sha256", sa.String(64), nullable=False),
        sa.Column("configuration", postgresql.JSONB(), nullable=False),
        sa.Column("description", postgresql.JSONB()),
        sa.Column("search_text", sa.Text()),
        sa.Column("embedding", Vector(1024)),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_asset_indexes_asset_version", "asset_indexes", ["asset_version_id"])
    op.create_unique_constraint("uq_provider_run_step", "provider_requests", ["run_id", "step_key"])


def downgrade():
    raise RuntimeError("数据库迁移需要明确的数据保留与恢复方案")
