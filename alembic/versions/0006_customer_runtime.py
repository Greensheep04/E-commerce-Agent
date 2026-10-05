import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = '0006_customer_runtime'
down_revision = '0005_customer_orders'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'customer_conversations',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('owner_id', sa.String(36), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('title', sa.String(200), nullable=False),
        sa.Column('orders', JSONB(), nullable=False),
        sa.Column('selected_order', sa.String(100)),
        sa.Column('context', JSONB(), nullable=False),
        sa.Column('pending_ticket', JSONB()),
        sa.Column('next_event', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index('ix_customer_conversations_owner_id', 'customer_conversations', ['owner_id'])
    op.create_table(
        'customer_turns',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('conversation_id', sa.String(36), sa.ForeignKey('customer_conversations.id'), nullable=False),
        sa.Column('request_id', sa.String(36), nullable=False),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('status', sa.String(30), nullable=False),
        sa.Column('model_calls', sa.Integer(), nullable=False),
        sa.Column('tool_calls', sa.Integer(), nullable=False),
        sa.Column('cancel_requested', sa.Boolean(), nullable=False),
        sa.Column('authorization', JSONB()),
        sa.Column('result', JSONB()),
        sa.Column('error', sa.Text()),
        sa.Column('config', JSONB(), nullable=False),
        sa.Column('received_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('elapsed_seconds', sa.Numeric(12, 3), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True)),
        sa.Column('finished_at', sa.DateTime(timezone=True)),
        sa.UniqueConstraint('conversation_id', 'request_id', name='uq_customer_turn_request'),
    )
    op.create_index('ix_customer_turn_history', 'customer_turns', ['conversation_id', 'received_at'])
    op.create_table(
        'customer_events',
        sa.Column('conversation_id', sa.String(36), sa.ForeignKey('customer_conversations.id'), primary_key=True),
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('turn_id', sa.String(36), sa.ForeignKey('customer_turns.id')),
        sa.Column('type', sa.String(40), nullable=False),
        sa.Column('payload', JSONB(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        'customer_tool_calls',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('turn_id', sa.String(36), sa.ForeignKey('customer_turns.id'), nullable=False),
        sa.Column('call_id', sa.String(200), nullable=False),
        sa.Column('name', sa.String(80), nullable=False),
        sa.Column('arguments', JSONB(), nullable=False),
        sa.Column('result', JSONB()),
        sa.Column('error', sa.Text()),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('turn_id', 'call_id', name='uq_customer_tool_call'),
    )
    op.create_table(
        'customer_tickets',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('number', sa.String(60), unique=True, nullable=False),
        sa.Column('conversation_id', sa.String(36), sa.ForeignKey('customer_conversations.id'), nullable=False),
        sa.Column('owner_id', sa.String(36), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('turn_id', sa.String(36), sa.ForeignKey('customer_turns.id'), nullable=False),
        sa.Column('authorization_id', sa.String(36), nullable=False),
        sa.Column('order_number', sa.String(100)),
        sa.Column('reason', sa.Text(), nullable=False),
        sa.Column('status', sa.String(30), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('conversation_id', 'authorization_id', name='uq_customer_ticket_authorization'),
    )


def downgrade():
    raise RuntimeError('数据库迁移需要明确的数据保留与恢复方案')
