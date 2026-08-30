"""freeze isolated Browser child launch profiles

Revision ID: 2a6c8e0f4b1d
Revises: f1c4e6a8b0d2, d3c8a7f1e620
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "2a6c8e0f4b1d"
down_revision: Union[str, Sequence[str], None] = (
    "f1c4e6a8b0d2",
    "d3c8a7f1e620",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Existing bindings are intentionally left untrusted (NULL). Startup
    # recovery terminalizes every non-terminal pre-profile child; only a new
    # atomic reservation may publish a complete version-1 launch profile.
    for column in (
        sa.Column("launch_profile_version", sa.Integer(), nullable=True),
        sa.Column("provider", sa.String(length=24), nullable=True),
        sa.Column("model", sa.String(length=100), nullable=True),
        sa.Column("reasoning_effort", sa.String(length=20), nullable=True),
        sa.Column("codex_service_tier", sa.String(length=20), nullable=True),
        sa.Column("task_mode", sa.String(length=20), nullable=True),
        sa.Column("launch_config_digest", sa.String(length=64), nullable=True),
        sa.Column("owner_task_incarnation_id", sa.String(length=32), nullable=True),
        sa.Column("owner_task_retry_count", sa.Integer(), nullable=True),
        sa.Column("owner_task_turn_generation", sa.BigInteger(), nullable=True),
        sa.Column("owner_task_status", sa.String(length=24), nullable=True),
        sa.Column("child_task_incarnation_id", sa.String(length=32), nullable=True),
    ):
        op.add_column("test_harness_child_bindings", column)
    for table_name in ("test_harness_runs", "workspace_review_runs"):
        for column in (
            sa.Column("owner_task_incarnation_id", sa.String(length=32), nullable=True),
            sa.Column("owner_task_retry_count", sa.Integer(), nullable=True),
            sa.Column("owner_task_turn_generation", sa.BigInteger(), nullable=True),
            sa.Column("owner_task_status", sa.String(length=24), nullable=True),
        ):
            op.add_column(table_name, column)

    # Freeze the current exact Task generations onto legacy ownership rows.
    # Launch-profile fields deliberately remain NULL: old Browser children
    # must be terminalized by startup recovery and can never become runnable
    # merely because a migration inferred provider/model values.
    connection = op.get_bind()
    tasks = sa.table(
        "tasks",
        sa.column("id", sa.Integer()),
        sa.column("incarnation_id", sa.String(length=32)),
        sa.column("retry_count", sa.Integer()),
        sa.column("turn_generation", sa.BigInteger()),
        sa.column("status", sa.String(length=24)),
    )
    task_identities = {
        int(row.id): row
        for row in connection.execute(
            sa.select(
                tasks.c.id,
                tasks.c.incarnation_id,
                tasks.c.retry_count,
                tasks.c.turn_generation,
                tasks.c.status,
            ).where(tasks.c.incarnation_id.is_not(None))
        )
    }

    def backfill_owner(table_name: str) -> None:
        table = sa.table(
            table_name,
            sa.column("id", sa.String(length=32)),
            sa.column("task_id", sa.Integer()),
            sa.column("owner_task_incarnation_id", sa.String(length=32)),
            sa.column("owner_task_retry_count", sa.Integer()),
            sa.column("owner_task_turn_generation", sa.BigInteger()),
            sa.column("owner_task_status", sa.String(length=24)),
        )
        for row_id, task_id in connection.execute(
            sa.select(table.c.id, table.c.task_id).where(
                table.c.task_id.is_not(None)
            )
        ):
            identity = task_identities.get(int(task_id))
            if identity is None:
                continue
            connection.execute(
                sa.update(table)
                .where(table.c.id == row_id)
                .values(
                    owner_task_incarnation_id=identity.incarnation_id,
                    owner_task_retry_count=identity.retry_count,
                    owner_task_turn_generation=identity.turn_generation,
                    owner_task_status=identity.status,
                )
            )

    backfill_owner("test_harness_runs")
    backfill_owner("workspace_review_runs")

    bindings = sa.table(
        "test_harness_child_bindings",
        sa.column("id", sa.String(length=32)),
        sa.column("owner_task_id", sa.Integer()),
        sa.column("child_task_id", sa.Integer()),
        sa.column("owner_task_incarnation_id", sa.String(length=32)),
        sa.column("owner_task_retry_count", sa.Integer()),
        sa.column("owner_task_turn_generation", sa.BigInteger()),
        sa.column("owner_task_status", sa.String(length=24)),
        sa.column("child_task_incarnation_id", sa.String(length=32)),
    )
    for binding_id, owner_task_id, child_task_id in connection.execute(
        sa.select(
            bindings.c.id,
            bindings.c.owner_task_id,
            bindings.c.child_task_id,
        )
    ):
        owner = task_identities.get(int(owner_task_id))
        child = task_identities.get(int(child_task_id))
        values = {}
        if owner is not None:
            values.update(
                owner_task_incarnation_id=owner.incarnation_id,
                owner_task_retry_count=owner.retry_count,
                owner_task_turn_generation=owner.turn_generation,
                owner_task_status=owner.status,
            )
        if child is not None:
            values["child_task_incarnation_id"] = child.incarnation_id
        if values:
            connection.execute(
                sa.update(bindings)
                .where(bindings.c.id == binding_id)
                .values(**values)
            )


def downgrade() -> None:
    for table_name in ("workspace_review_runs", "test_harness_runs"):
        for column in (
            "owner_task_status",
            "owner_task_turn_generation",
            "owner_task_retry_count",
            "owner_task_incarnation_id",
        ):
            op.drop_column(table_name, column)
    for column in (
        "child_task_incarnation_id",
        "owner_task_status",
        "owner_task_turn_generation",
        "owner_task_retry_count",
        "owner_task_incarnation_id",
        "launch_config_digest",
        "task_mode",
        "codex_service_tier",
        "reasoning_effort",
        "model",
        "provider",
        "launch_profile_version",
    ):
        op.drop_column("test_harness_child_bindings", column)
