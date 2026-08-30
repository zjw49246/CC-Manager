"""add durable Discussion provider leases

Revision ID: d1a9c4e7b260
Revises: c2f8a6d4e1b9
"""

from alembic import op
import sqlalchemy as sa


revision = "d1a9c4e7b260"
down_revision = "c2f8a6d4e1b9"
branch_labels = None
depends_on = None

_STATUS_CONSTRAINT = "ck_discussions_status"
_PROJECT_LEASE_INDEX = "ix_discussions_project_status_id"
_ALLOWED_STATUSES = ("active", "closing", "closed")


def _assert_historical_statuses_are_supported() -> None:
    bind = op.get_bind()
    unsupported = bind.execute(
        sa.text(
            "SELECT id, status FROM discussions "
            "WHERE status IS NULL "
            "OR status NOT IN ('active', 'closing', 'closed') "
            "ORDER BY id LIMIT 1"
        )
    ).first()
    if unsupported is not None:
        raise RuntimeError(
            "Discussion provider lease migration refused unsupported historical "
            f"status for Discussion #{unsupported[0]}: {unsupported[1]!r}"
        )


def upgrade() -> None:
    _assert_historical_statuses_are_supported()
    with op.batch_alter_table("discussions", schema=None) as batch_op:
        batch_op.create_check_constraint(
            _STATUS_CONSTRAINT,
            "status IN ('active', 'closing', 'closed')",
        )
        batch_op.create_index(
            _PROJECT_LEASE_INDEX,
            ["project_id", "status", "id"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("discussions", schema=None) as batch_op:
        batch_op.drop_index(_PROJECT_LEASE_INDEX)
        batch_op.drop_constraint(_STATUS_CONSTRAINT, type_="check")
