import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "0002_harness_budget"
down_revision = "0001_material_center"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "budget_accounts",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("spent_cny", sa.Numeric(12, 6), nullable=False),
        sa.Column("reserved_cny", sa.Numeric(12, 6), nullable=False),
    )
    op.execute("INSERT INTO budget_accounts (id, spent_cny, reserved_cny) VALUES ('project', 0, 0)")
    op.create_table(
        "harness_tasks",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("model_calls", sa.Integer(), nullable=False),
        sa.Column("tool_calls", sa.Integer(), nullable=False),
        sa.Column("elapsed_seconds", sa.Numeric(10, 3), nullable=False),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False),
        sa.Column("result", JSONB()),
        sa.Column("error", sa.Text()),
        sa.Column("stop_reason", sa.String(60)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "provider_requests",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("harness_tasks.id")),
        sa.Column("run_id", sa.String(36)),
        sa.Column("step_key", sa.String(100), nullable=False),
        sa.Column("service", sa.String(80), nullable=False),
        sa.Column("model", sa.String(100), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("reservation_cny", sa.Numeric(12, 6), nullable=False),
        sa.Column("charged_cny", sa.Numeric(12, 6)),
        sa.Column("price_basis", JSONB(), nullable=False),
        sa.Column("request_body", JSONB(), nullable=False),
        sa.Column("response_body", JSONB()),
        sa.Column("usage", JSONB()),
        sa.Column("provider_request_id", sa.String(200)),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("task_id", "step_key"),
    )
    op.create_table(
        "harness_tool_calls",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("harness_tasks.id"), nullable=False),
        sa.Column("call_id", sa.String(200), nullable=False),
        sa.Column("name", sa.String(80), nullable=False),
        sa.Column("arguments", JSONB(), nullable=False),
        sa.Column("result", JSONB()),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task_id", "call_id"),
    )


def downgrade():
    op.drop_table("harness_tool_calls")
    op.drop_table("provider_requests")
    op.drop_table("harness_tasks")
    op.drop_table("budget_accounts")
