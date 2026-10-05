from alembic import op

revision = "0012_agent_cluster"
down_revision = "0011_integration_history"
branch_labels = None
depends_on = None


def upgrade():
    from agent_cluster.store import Base

    Base.metadata.create_all(op.get_bind())


def downgrade():
    raise RuntimeError("删除Agent集群持久记录需要用户批准")
