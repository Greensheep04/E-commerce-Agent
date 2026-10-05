import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "0005_assessment"
down_revision = "0004_campaign_indexes"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "assessment_sources",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("source_id", sa.String(200), nullable=False),
        sa.Column("revision", sa.String(40), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("manifest", JSONB(), nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "assessment_employees",
        sa.Column("employee_id", sa.Integer(), primary_key=True),
        sa.Column("source_key", sa.String(40), sa.ForeignKey("assessment_sources.id"), nullable=False),
        sa.Column("job_title", sa.String(200), nullable=False),
        sa.Column("current_flag", sa.Boolean(), nullable=False),
        sa.Column("is_salesperson", sa.Boolean(), nullable=False),
        sa.Column("territory_id", sa.Integer()),
        sa.Column("source_line", sa.Integer(), nullable=False),
        sa.Column("salesperson_line", sa.Integer()),
    )
    op.create_table(
        "assessment_quotas",
        sa.Column("employee_id", sa.Integer(), sa.ForeignKey("assessment_employees.employee_id"), primary_key=True),
        sa.Column("quota_date", sa.DateTime(), primary_key=True),
        sa.Column("amount", sa.Numeric(38, 4), nullable=False),
        sa.Column("source_line", sa.Integer(), nullable=False),
        sa.CheckConstraint("amount > 0", name="ck_assessment_quota_positive"),
    )
    op.create_table(
        "assessment_access",
        sa.Column("user_id", sa.String(36), sa.ForeignKey("users.id"), primary_key=True),
        sa.Column("level", sa.String(20), nullable=False),
        sa.Column("employee_id", sa.Integer(), sa.ForeignKey("assessment_employees.employee_id"), unique=True),
        sa.Column("managed_employee_ids", JSONB(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("level IN ('employee', 'manager', 'admin')", name="ck_assessment_access_level"),
        sa.CheckConstraint("level = 'admin' OR employee_id IS NOT NULL", name="ck_assessment_access_identity"),
    )
    op.create_table(
        "assessment_runs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("run_id", sa.String(36), unique=True, nullable=False),
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("parent_id", sa.String(36), sa.ForeignKey("assessment_runs.id")),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("scope_employee_ids", JSONB(), nullable=False),
        sa.Column("owner_level", sa.String(20), nullable=False),
        sa.Column("model_profile", JSONB(), nullable=False),
        sa.Column("query", JSONB()),
        sa.Column("metrics", JSONB()),
        sa.Column("result", JSONB()),
        sa.Column("model_calls", sa.Integer(), nullable=False),
        sa.Column("tool_calls", sa.Integer(), nullable=False),
        sa.Column("elapsed_seconds", sa.Numeric(12, 3), nullable=False),
        sa.Column("active_since", sa.DateTime(timezone=True)),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False),
        sa.Column("stop_reason", sa.String(60)),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "assessment_tool_calls",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("assessment_id", sa.String(36), sa.ForeignKey("assessment_runs.id"), nullable=False),
        sa.Column("step_key", sa.String(100), nullable=False),
        sa.Column("name", sa.String(80), nullable=False),
        sa.Column("arguments", JSONB(), nullable=False),
        sa.Column("result", JSONB()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("assessment_id", "step_key"),
    )
    op.add_column("provider_requests", sa.Column("assessment_id", sa.String(36)))
    op.create_foreign_key("fk_provider_assessment", "provider_requests", "assessment_runs", ["assessment_id"], ["id"])


def downgrade():
    op.drop_constraint("fk_provider_assessment", "provider_requests", type_="foreignkey")
    op.drop_column("provider_requests", "assessment_id")
    for table in ("assessment_tool_calls", "assessment_runs", "assessment_access",
                  "assessment_quotas", "assessment_employees", "assessment_sources"):
        op.drop_table(table)
