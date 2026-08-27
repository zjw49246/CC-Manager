"""add direct PR merge actions

Revision ID: b9d4e6f1a2c7
Revises: a8c4e2f6b0d1
"""

from alembic import op
import sqlalchemy as sa


revision = "b9d4e6f1a2c7"
down_revision = "a8c4e2f6b0d1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "pr_merge_queue_actions",
        sa.Column(
            "effect_kind",
            sa.String(length=20),
            nullable=False,
            server_default="queue",
        ),
    )
    op.add_column(
        "pr_merge_queue_actions",
        sa.Column("publishing_actor", sa.String(length=200), nullable=True),
    )
    op.add_column(
        "pr_merge_queue_actions",
        sa.Column("publishing_started_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "pr_merge_queue_actions",
        sa.Column("merge_method", sa.String(length=16), nullable=True),
    )
    op.add_column(
        "pr_merge_queue_actions",
        sa.Column(
            "wait_for_ci",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "pr_merge_queue_actions",
        sa.Column(
            "required_checks",
            sa.JSON(),
            nullable=False,
            server_default="[]",
        ),
    )
    # Queue is no longer admitted by policy. Existing action rows retain their
    # effect kind and are reconciled by the legacy recovery path.
    op.execute(
        sa.text(
            "UPDATE monitored_repos SET merge_queue_mode = 'manual' "
            "WHERE merge_queue_mode <> 'manual'"
        )
    )


def downgrade() -> None:
    op.drop_column("pr_merge_queue_actions", "required_checks")
    op.drop_column("pr_merge_queue_actions", "wait_for_ci")
    op.drop_column("pr_merge_queue_actions", "merge_method")
    op.drop_column("pr_merge_queue_actions", "publishing_started_at")
    op.drop_column("pr_merge_queue_actions", "publishing_actor")
    op.drop_column("pr_merge_queue_actions", "effect_kind")
