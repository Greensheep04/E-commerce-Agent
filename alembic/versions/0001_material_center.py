import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "0001_material_center"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "users",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("username", sa.String(100), nullable=False, unique=True),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("role", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "sources",
        sa.Column("source_id", sa.String(200), primary_key=True),
        sa.Column("revision", sa.String(200), primary_key=True),
        sa.Column("license", sa.String(200)),
        sa.Column("license_note", sa.Text()),
        sa.Column("url", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "import_batches",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("source_id", sa.String(200), nullable=False),
        sa.Column("source_revision", sa.String(200), nullable=False),
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("expected_count", sa.Integer(), nullable=False),
        sa.Column("imported_count", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("manifest", JSONB(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["source_id", "source_revision"], ["sources.source_id", "sources.revision"]),
    )
    op.create_table(
        "assets",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("source_id", sa.String(200), nullable=False),
        sa.Column("source_revision", sa.String(200), nullable=False),
        sa.Column("record_id", sa.String(200), nullable=False),
        sa.Column("source_order", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("kind", sa.String(20), nullable=False),
        sa.Column("tags", JSONB(), nullable=False),
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_id", "source_revision"], ["sources.source_id", "sources.revision"]),
        sa.UniqueConstraint("source_id", "source_revision", "record_id"),
    )
    op.create_table(
        "asset_versions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("asset_id", sa.String(36), sa.ForeignKey("assets.id"), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("file_sha256", sa.String(64), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=False),
        sa.Column("filename", sa.String(500), nullable=False),
        sa.Column("mime_type", sa.String(200), nullable=False),
        sa.Column("original_fields", JSONB(), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("tags", JSONB(), nullable=False),
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("asset_id", "version"),
    )


def downgrade():
    op.drop_table("asset_versions")
    op.drop_table("assets")
    op.drop_table("import_batches")
    op.drop_table("sources")
    op.drop_table("users")
