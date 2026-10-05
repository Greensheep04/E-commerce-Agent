import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0016_data_center_source_version"
down_revision = "0015_data_center_watch"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("data_center_source_versions",
        sa.Column("source_id", sa.String(300), sa.ForeignKey("data_center_sources.source_id"), primary_key=True),
        sa.Column("revision", sa.String(200), primary_key=True),
        sa.Column("document", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.execute(sa.text("""
        INSERT INTO data_center_source_versions (source_id, revision, document, created_at)
        SELECT source_id, revision, jsonb_build_object(
            'source_id', source_id, 'kind', kind, 'title', title, 'revision', revision,
            'nature', nature, 'license_id', license_id, 'sha256', sha256,
            'properties', properties, 'relative_path', relative_path, 'resource', resource
        ), updated_at FROM data_center_sources
    """))


def downgrade():
    raise RuntimeError("删除来源版本记录需要用户批准")
