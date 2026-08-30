"""add durable Browser interaction operation receipts

Revision ID: 6f3b9d2a7c10
Revises: 4e8a1c6d9b20
Create Date: 2026-08-09
"""

from alembic import context, op
import sqlalchemy as sa


revision: str = "6f3b9d2a7c10"
down_revision: str | None = "4e8a1c6d9b20"
branch_labels: str | None = None
depends_on: str | None = None


_TABLE = "browser_review_operation_receipts"
_MYSQL_TRANSACTION_TABLES = (
    "tasks",
    "instances",
    "projects",
    "task_shares",
    "project_shares",
    "team_task_shares",
    "team_project_shares",
    "workspace_review_runs",
    "test_harness_runs",
    "test_harness_attempts",
    "test_harness_events",
    "test_harness_evidence",
    "test_harness_findings",
    "test_harness_sandbox_leases",
    "test_harness_child_bindings",
    "ssh_profiles",
    "task_ssh_grants",
    "task_ssh_effect_receipts",
)


def _require_mysql_transaction_tables() -> None:
    """Fail closed if the Browser lifecycle cannot be one transaction."""

    dialect = op.get_context().dialect.name
    if dialect not in {"mysql", "mariadb"}:
        return
    if context.is_offline_mode():
        raise RuntimeError(
            "Offline Browser operation receipt upgrade is refused for MySQL "
            "because the existing Harness table engines cannot be verified"
        )

    bind = op.get_bind()
    placeholders = ", ".join(
        f":table_{index}" for index, _ in enumerate(_MYSQL_TRANSACTION_TABLES)
    )
    parameters = {
        f"table_{index}": table
        for index, table in enumerate(_MYSQL_TRANSACTION_TABLES)
    }
    rows = bind.execute(
        sa.text(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() "
            f"AND TABLE_NAME IN ({placeholders})"
        ),
        parameters,
    ).all()
    engines = {
        str(table_name).lower(): str(engine).lower()
        for table_name, engine in rows
        if engine is not None
    }
    unsafe = [
        table
        for table in _MYSQL_TRANSACTION_TABLES
        if engines.get(table) != "innodb"
    ]
    if unsafe:
        raise RuntimeError(
            "Browser operation receipt upgrade requires all transactional "
            "Task/Harness/SSH tables to use InnoDB; missing or non-InnoDB: "
            + ", ".join(unsafe)
        )


def _assert_downgrade_safe() -> None:
    """Never destroy durable at-most-once evidence during a downgrade."""

    if context.is_offline_mode():
        raise RuntimeError(
            "Offline Browser operation receipt downgrade is refused because "
            "permanent replay/ambiguity evidence cannot be inspected"
        )
    count = op.get_bind().execute(
        sa.text(f"SELECT COUNT(*) FROM {_TABLE}")
    ).scalar_one()
    if count:
        raise RuntimeError(
            "Browser operation receipt downgrade refused: permanent "
            "at-most-once evidence exists"
        )


def upgrade() -> None:
    _require_mysql_transaction_tables()
    op.create_table(
        _TABLE,
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("browser_review_job_id", sa.String(length=32), nullable=False),
        sa.Column("operation_id", sa.String(length=64), nullable=False),
        sa.Column("binding_id", sa.String(length=32), nullable=False),
        sa.Column("harness_run_id", sa.String(length=32), nullable=True),
        sa.Column("workspace_review_run_id", sa.String(length=32), nullable=True),
        sa.Column("owner_task_id", sa.Integer(), nullable=False),
        sa.Column("owner_task_incarnation_id", sa.String(length=32), nullable=False),
        sa.Column("owner_task_retry_count", sa.Integer(), nullable=False),
        sa.Column("owner_task_turn_generation", sa.BigInteger(), nullable=False),
        sa.Column("owner_task_status", sa.String(length=24), nullable=False),
        sa.Column("child_task_id", sa.Integer(), nullable=False),
        sa.Column("child_task_incarnation_id", sa.String(length=32), nullable=False),
        sa.Column("child_task_retry_count", sa.Integer(), nullable=False),
        sa.Column("child_task_turn_generation", sa.BigInteger(), nullable=False),
        sa.Column("child_task_status", sa.String(length=24), nullable=False),
        sa.Column("action_kind", sa.String(length=32), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("execution_nonce_digest", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("ack_digest", sa.String(length=64), nullable=True),
        sa.Column("result_data", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("acknowledged_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('permitted', 'completed', 'uncertain', 'aborted')",
            name="ck_browser_review_operation_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "browser_review_job_id",
            "operation_id",
            name="uq_browser_review_operation_job_id",
        ),
        mysql_engine="InnoDB",
    )
    for name, columns in (
        ("ix_browser_review_operation_receipts_browser_review_job_id", ["browser_review_job_id"]),
        ("ix_browser_review_operation_receipts_binding_id", ["binding_id"]),
        ("ix_browser_review_operation_receipts_harness_run_id", ["harness_run_id"]),
        ("ix_browser_review_operation_receipts_workspace_review_run_id", ["workspace_review_run_id"]),
        ("ix_browser_review_operation_receipts_owner_task_id", ["owner_task_id"]),
        ("ix_browser_review_operation_receipts_child_task_id", ["child_task_id"]),
        ("ix_browser_review_operation_receipts_status", ["status"]),
    ):
        op.create_index(name, _TABLE, columns)


def downgrade() -> None:
    _assert_downgrade_safe()
    op.drop_table(_TABLE)
