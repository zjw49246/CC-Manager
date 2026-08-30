"""add durable Delivery Loop orchestration state

Revision ID: 9e5b2a7c4d10
Revises: 8d4e1f7a9c20
Create Date: 2026-08-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "9e5b2a7c4d10"
down_revision: Union[str, None] = "8d4e1f7a9c20"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _legacy_sqlite_task_id_highwater() -> int | None:
    """Recover the largest security-relevant id used before AUTOINCREMENT."""

    context = op.get_context()
    bind = op.get_bind()
    if context.as_sql or bind.dialect.name != "sqlite":
        return None
    value = bind.execute(
        sa.text(
            "SELECT MAX(task_id) FROM ("
            "SELECT id AS task_id FROM tasks "
            "UNION ALL SELECT task_id FROM task_shares "
            "UNION ALL SELECT task_id FROM team_task_shares "
            "UNION ALL SELECT local_task_id AS task_id "
            "FROM shared_tasks_received WHERE local_task_id IS NOT NULL"
            ")"
        )
    ).scalar_one_or_none()
    return int(value) if value is not None else None


def _legacy_sqlite_shared_id_highwater() -> int | None:
    """Recover received-share ids still referenced by legacy shadow Tasks."""

    context = op.get_context()
    bind = op.get_bind()
    if context.as_sql or bind.dialect.name != "sqlite":
        return None
    value = bind.execute(
        sa.text(
            "SELECT MAX(shared_id) FROM ("
            "SELECT id AS shared_id FROM shared_tasks_received "
            "UNION ALL SELECT shared_from_id AS shared_id FROM tasks "
            "WHERE shared_from_id IS NOT NULL"
            ")"
        )
    ).scalar_one_or_none()
    return int(value) if value is not None else None


def _advance_sqlite_sequence(table_name: str, highwater: int | None) -> None:
    if highwater is None:
        return
    bind = op.get_bind()
    advanced = bind.execute(
        sa.text(
            "UPDATE sqlite_sequence "
            "SET seq = CASE WHEN seq < :highwater THEN :highwater ELSE seq END "
            "WHERE name = :table_name"
        ),
        {"highwater": highwater, "table_name": table_name},
    )
    if advanced.rowcount == 0:
        bind.execute(
            sa.text(
                "INSERT INTO sqlite_sequence(name, seq) "
                "VALUES (:table_name, :highwater)"
            ),
            {"highwater": highwater, "table_name": table_name},
        )


def _purge_stale_task_access_grants() -> None:
    """Remove ACL rows that cannot belong to the current Task incarnation."""

    bind = op.get_bind()
    for table_name in ("task_shares", "team_task_shares"):
        bind.execute(
            sa.text(
                f"DELETE FROM {table_name} "
                "WHERE NOT EXISTS ("
                f"SELECT 1 FROM tasks WHERE tasks.id = {table_name}.task_id"
                ") OR EXISTS ("
                "SELECT 1 FROM tasks "
                f"WHERE tasks.id = {table_name}.task_id "
                f"AND {table_name}.created_at < tasks.created_at"
                ")"
            )
        )


def _add_owner_columns(
    task_id_highwater: int | None = None,
    shared_id_highwater: int | None = None,
) -> None:
    """Add controller ownership fields to existing durable records.

    Batch mode keeps the check/unique constraint changes portable to SQLite;
    Alembic emits ordinary ALTER TABLE statements for PostgreSQL and MySQL.
    """

    with op.batch_alter_table("plan_agent_runs", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("capability_execution_id", sa.Integer(), nullable=True)
        )
        batch_op.create_unique_constraint(
            "uq_plan_agent_runs_capability_execution",
            ["capability_execution_id"],
        )

    with op.batch_alter_table("project_todos", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("task_request_hash", sa.String(length=64), nullable=True)
        )

    task_batch_kwargs = {}
    if op.get_bind().dialect.name == "sqlite":
        # A Task id names HTTP resources, WS channels, share tokens and Worker
        # mirrors.  SQLite's default ROWID may reuse the deleted highest id;
        # rebuilding with AUTOINCREMENT permanently closes that ABA class for
        # both pre-existing and newly-created databases.
        task_batch_kwargs = {
            "recreate": "always",
            "table_kwargs": {"sqlite_autoincrement": True},
        }
    with op.batch_alter_table(
        "tasks",
        schema=None,
        **task_batch_kwargs,
    ) as batch_op:
        batch_op.add_column(
            sa.Column("incarnation_id", sa.String(length=32), nullable=True)
        )
        batch_op.add_column(
            sa.Column("delivery_run_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("delivery_role", sa.String(length=24), nullable=True)
        )
        batch_op.create_index(
            "ix_tasks_delivery_run_id", ["delivery_run_id"], unique=False
        )
        batch_op.create_check_constraint(
            "ck_tasks_delivery_owner_shape",
            "(mode = 'delivery_loop' AND delivery_run_id IS NOT NULL "
            "AND delivery_role = 'developer') OR "
            "(mode <> 'delivery_loop' AND delivery_run_id IS NULL "
            "AND delivery_role IS NULL)",
        )
        batch_op.create_unique_constraint(
            "uq_tasks_incarnation_id",
            ["incarnation_id"],
        )

    _advance_sqlite_sequence("tasks", task_id_highwater)

    if op.get_bind().dialect.name == "sqlite":
        # ``SharedTaskReceived.id`` is copied into Task.shared_from_id and URL
        # identities.  Rebuild it with AUTOINCREMENT so a deleted share id can
        # never alias one of its surviving cancelled shadow Tasks.
        with op.batch_alter_table(
            "shared_tasks_received",
            schema=None,
            recreate="always",
            table_kwargs={"sqlite_autoincrement": True},
        ):
            pass
        _advance_sqlite_sequence(
            "shared_tasks_received",
            shared_id_highwater,
        )

    with op.batch_alter_table("worktrees", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("task_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("delivery_run_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "last_verified_head", sa.String(length=64), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "cleanup_status",
                sa.String(length=20),
                server_default="retained",
                nullable=False,
            )
        )
        batch_op.create_index("ix_worktrees_task_id", ["task_id"], unique=False)
        batch_op.create_index(
            "ix_worktrees_delivery_run_id", ["delivery_run_id"], unique=False
        )
        batch_op.create_unique_constraint(
            "uq_worktrees_delivery_run", ["delivery_run_id"]
        )
        batch_op.create_check_constraint(
            "ck_worktrees_cleanup_status",
            "cleanup_status IN ('retained', 'cleaning', 'removed', 'error')",
        )


def _create_delivery_runs() -> None:
    op.create_table(
        "delivery_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=True),
        sa.Column("admission_scope", sa.String(length=80), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("monitored_repo_id", sa.Integer(), nullable=True),
        sa.Column("source_todo_id", sa.Integer(), nullable=True),
        sa.Column("developer_task_id", sa.Integer(), nullable=True),
        sa.Column("pr_monitor_run_id", sa.Integer(), nullable=True),
        sa.Column("worktree_id", sa.Integer(), nullable=True),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("requirements", sa.Text(), nullable=False),
        sa.Column("requirements_hash", sa.String(length=64), nullable=False),
        sa.Column("policy_snapshot", sa.JSON(), nullable=False),
        sa.Column("policy_hash", sa.String(length=64), nullable=False),
        sa.Column("base_branch", sa.String(length=200), nullable=False),
        sa.Column("delivery_branch", sa.String(length=200), nullable=False),
        sa.Column("workspace_path", sa.String(length=1000), nullable=True),
        sa.Column("base_sha", sa.String(length=64), nullable=True),
        sa.Column("head_sha", sa.String(length=64), nullable=True),
        sa.Column("head_tree_sha", sa.String(length=64), nullable=True),
        sa.Column("patch_sha256", sa.String(length=64), nullable=True),
        sa.Column(
            "head_generation", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column("pr_number", sa.Integer(), nullable=True),
        sa.Column("pr_url", sa.String(length=1000), nullable=True),
        sa.Column(
            "phase",
            sa.String(length=24),
            server_default="planning",
            nullable=False,
        ),
        sa.Column(
            "activity",
            sa.String(length=24),
            server_default="ready",
            nullable=False,
        ),
        sa.Column("outcome", sa.String(length=24), nullable=True),
        sa.Column("wait_reason", sa.String(length=64), nullable=True),
        sa.Column("paused_from_activity", sa.String(length=24), nullable=True),
        sa.Column("pause_reason", sa.Text(), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "state_version", sa.Integer(), server_default="1", nullable=False
        ),
        sa.Column("current_cycle_id", sa.Integer(), nullable=True),
        sa.Column(
            "cycle_count", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column(
            "turn_count", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column(
            "max_cycles", sa.Integer(), server_default="10", nullable=False
        ),
        sa.Column(
            "no_progress_count", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column(
            "max_no_progress", sa.Integer(), server_default="3", nullable=False
        ),
        sa.Column(
            "last_progress_signature", sa.String(length=64), nullable=True
        ),
        sa.Column(
            "controller_generation",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("lease_owner", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("next_reconcile_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "phase IN ('planning', 'coding', 'pre_review', 'publishing', "
            "'monitoring', 'done')",
            name="ck_delivery_runs_phase",
        ),
        sa.CheckConstraint(
            "activity IN ('ready', 'running', 'waiting', 'paused', 'terminal')",
            name="ck_delivery_runs_activity",
        ),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN "
            "('success', 'failed', 'cancelled', 'superseded')",
            name="ck_delivery_runs_outcome",
        ),
        sa.CheckConstraint(
            "(phase = 'done' AND activity = 'terminal' AND "
            "outcome IS NOT NULL AND completed_at IS NOT NULL) OR "
            "(phase <> 'done' AND activity <> 'terminal' AND "
            "outcome IS NULL AND completed_at IS NULL)",
            name="ck_delivery_runs_terminal_shape",
        ),
        sa.CheckConstraint("state_version >= 1", name="ck_delivery_runs_version"),
        sa.CheckConstraint("cycle_count >= 0", name="ck_delivery_runs_cycle_count"),
        sa.CheckConstraint("turn_count >= 0", name="ck_delivery_runs_turn_count"),
        sa.CheckConstraint("max_cycles >= 1", name="ck_delivery_runs_max_cycles"),
        sa.CheckConstraint(
            "max_no_progress >= 1", name="ck_delivery_runs_max_no_progress"
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"]),
        sa.ForeignKeyConstraint(["monitored_repo_id"], ["monitored_repos.id"]),
        sa.ForeignKeyConstraint(["source_todo_id"], ["project_todos.id"]),
        sa.ForeignKeyConstraint(
            ["developer_task_id"], ["tasks.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["pr_monitor_run_id"], ["pr_monitor_runs.id"]),
        sa.ForeignKeyConstraint(
            ["worktree_id"], ["worktrees.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "project_id",
            "delivery_branch",
            name="uq_delivery_runs_project_branch",
        ),
        sa.UniqueConstraint(
            "admission_scope",
            "project_id",
            "idempotency_key",
            name="uq_delivery_runs_admission",
        ),
        sa.UniqueConstraint(
            "source_todo_id", name="uq_delivery_runs_source_todo"
        ),
        sa.UniqueConstraint(
            "developer_task_id", name="uq_delivery_runs_developer_task"
        ),
        sa.UniqueConstraint(
            "pr_monitor_run_id", name="uq_delivery_runs_monitor_run"
        ),
        sa.UniqueConstraint("worktree_id", name="uq_delivery_runs_worktree"),
    )
    for index_name, columns in (
        ("ix_delivery_runs_created_by", ["created_by"]),
        ("ix_delivery_runs_project_id", ["project_id"]),
        ("ix_delivery_runs_monitored_repo_id", ["monitored_repo_id"]),
        ("ix_delivery_runs_source_todo_id", ["source_todo_id"]),
        ("ix_delivery_runs_developer_task_id", ["developer_task_id"]),
        ("ix_delivery_runs_pr_monitor_run_id", ["pr_monitor_run_id"]),
        ("ix_delivery_runs_worktree_id", ["worktree_id"]),
        ("ix_delivery_runs_next_reconcile_at", ["next_reconcile_at"]),
        ("ix_delivery_runs_due", ["activity", "next_reconcile_at"]),
        ("ix_delivery_runs_project_created", ["project_id", "created_at"]),
        ("ix_delivery_runs_repo_pr", ["monitored_repo_id", "pr_number"]),
    ):
        op.create_index(index_name, "delivery_runs", columns, unique=False)


def _create_delivery_cycles() -> None:
    op.create_table(
        "delivery_cycles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("cycle_number", sa.Integer(), nullable=False),
        sa.Column("active_run_id", sa.Integer(), nullable=True),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="planning",
            nullable=False,
        ),
        sa.Column(
            "state_version", sa.Integer(), server_default="1", nullable=False
        ),
        sa.Column("trigger_kind", sa.String(length=64), nullable=False),
        sa.Column("trigger_payload", sa.JSON(), nullable=False),
        sa.Column("trigger_hash", sa.String(length=64), nullable=False),
        sa.Column("trigger_pr_review_id", sa.Integer(), nullable=True),
        sa.Column("trigger_pr_repair_wake_id", sa.Integer(), nullable=True),
        sa.Column("base_sha", sa.String(length=64), nullable=True),
        sa.Column("start_head_sha", sa.String(length=64), nullable=True),
        sa.Column("result_head_sha", sa.String(length=64), nullable=True),
        sa.Column("result_head_tree_sha", sa.String(length=64), nullable=True),
        sa.Column("result_patch_sha256", sa.String(length=64), nullable=True),
        sa.Column("plan_invocation_id", sa.Integer(), nullable=True),
        sa.Column("plan_version_id", sa.Integer(), nullable=True),
        sa.Column("review_invocation_id", sa.Integer(), nullable=True),
        sa.Column("review_result_id", sa.Integer(), nullable=True),
        sa.Column("review_verdict", sa.String(length=32), nullable=True),
        sa.Column("review_summary", sa.Text(), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('planning', 'coding', 'pre_review', 'publishing', "
            "'completed', 'failed', 'cancelled', 'superseded')",
            name="ck_delivery_cycles_status",
        ),
        sa.CheckConstraint(
            "(status IN ('planning', 'coding', 'pre_review', 'publishing') "
            "AND active_run_id IS NOT NULL AND active_run_id = run_id) OR "
            "(status NOT IN ('planning', 'coding', 'pre_review', 'publishing') "
            "AND active_run_id IS NULL)",
            name="ck_delivery_cycles_active_slot",
        ),
        sa.CheckConstraint("cycle_number >= 1", name="ck_delivery_cycles_number"),
        sa.CheckConstraint("state_version >= 1", name="ck_delivery_cycles_version"),
        sa.ForeignKeyConstraint(
            ["run_id"], ["delivery_runs.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["trigger_pr_review_id"], ["pr_reviews.id"]),
        sa.ForeignKeyConstraint(
            ["trigger_pr_repair_wake_id"], ["pr_repair_wakes.id"]
        ),
        sa.ForeignKeyConstraint(
            ["plan_invocation_id"], ["capability_invocations.id"]
        ),
        sa.ForeignKeyConstraint(["plan_version_id"], ["plan_versions.id"]),
        sa.ForeignKeyConstraint(
            ["review_invocation_id"], ["capability_invocations.id"]
        ),
        sa.ForeignKeyConstraint(
            ["review_result_id"], ["code_review_results.id"]
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "cycle_number", name="uq_delivery_cycles_run_number"
        ),
        sa.UniqueConstraint("active_run_id", name="uq_delivery_cycles_active_run"),
        sa.UniqueConstraint(
            "plan_invocation_id", name="uq_delivery_cycles_plan_invocation"
        ),
        sa.UniqueConstraint(
            "review_invocation_id", name="uq_delivery_cycles_review_invocation"
        ),
    )
    op.create_index(
        "ix_delivery_cycles_run_created",
        "delivery_cycles",
        ["run_id", "created_at"],
        unique=False,
    )


def _create_delivery_turns() -> None:
    op.create_table(
        "delivery_turns",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("cycle_id", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("correlation_id", sa.String(length=64), nullable=False),
        sa.Column("active_run_id", sa.Integer(), nullable=True),
        sa.Column("purpose", sa.String(length=32), nullable=False),
        sa.Column("trigger_kind", sa.String(length=64), nullable=False),
        sa.Column("trigger_payload", sa.JSON(), nullable=False),
        sa.Column("prompt_payload", sa.JSON(), nullable=False),
        sa.Column("prompt_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="queued",
            nullable=False,
        ),
        sa.Column("task_id", sa.Integer(), nullable=True),
        sa.Column("task_retry_count", sa.Integer(), nullable=True),
        sa.Column("task_instance_id", sa.Integer(), nullable=True),
        sa.Column("task_started_at", sa.DateTime(), nullable=True),
        sa.Column("task_session_id", sa.String(length=200), nullable=True),
        sa.Column("source_log_id", sa.Integer(), nullable=True),
        sa.Column("checkpoint", sa.JSON(), nullable=True),
        sa.Column("checkpoint_status", sa.String(length=32), nullable=True),
        sa.Column(
            "progress_signature_before", sa.String(length=64), nullable=True
        ),
        sa.Column(
            "progress_signature_after", sa.String(length=64), nullable=True
        ),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('queued', 'dispatching', 'running', 'reconciling', "
            "'completed', 'failed', 'cancelled', 'stale', 'superseded')",
            name="ck_delivery_turns_status",
        ),
        sa.CheckConstraint(
            "(status IN ('queued', 'dispatching', 'running', 'reconciling') "
            "AND active_run_id IS NOT NULL AND active_run_id = run_id) OR "
            "(status NOT IN ('queued', 'dispatching', 'running', 'reconciling') "
            "AND active_run_id IS NULL)",
            name="ck_delivery_turns_active_slot",
        ),
        sa.CheckConstraint("generation >= 1", name="ck_delivery_turns_generation"),
        sa.CheckConstraint("attempts >= 0", name="ck_delivery_turns_attempts"),
        sa.ForeignKeyConstraint(
            ["run_id"], ["delivery_runs.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["cycle_id"], ["delivery_cycles.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "generation", name="uq_delivery_turns_generation"
        ),
        sa.UniqueConstraint(
            "correlation_id", name="uq_delivery_turns_correlation"
        ),
        sa.UniqueConstraint("active_run_id", name="uq_delivery_turns_active_run"),
    )
    op.create_index(
        "ix_delivery_turns_cycle_id",
        "delivery_turns",
        ["cycle_id"],
        unique=False,
    )
    op.create_index(
        "ix_delivery_turns_task_id", "delivery_turns", ["task_id"], unique=False
    )
    op.create_index(
        "ix_delivery_turns_run_created",
        "delivery_turns",
        ["run_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_delivery_turns_status_created",
        "delivery_turns",
        ["status", "created_at"],
        unique=False,
    )


def _create_delivery_events() -> None:
    op.create_table(
        "delivery_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("cycle_id", sa.Integer(), nullable=True),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("source_event_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("subject_kind", sa.String(length=32), nullable=True),
        sa.Column("subject_ref", sa.JSON(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("lease_owner", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint("sequence >= 1", name="ck_delivery_events_sequence"),
        sa.CheckConstraint(
            "status IN ('pending', 'processing', 'processed', 'dead_letter')",
            name="ck_delivery_events_status",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["delivery_runs.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["cycle_id"], ["delivery_cycles.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source", "source_event_id", name="uq_delivery_events_source"
        ),
        sa.UniqueConstraint(
            "run_id", "sequence", name="uq_delivery_events_sequence"
        ),
    )
    op.create_index(
        "ix_delivery_events_pending",
        "delivery_events",
        ["status", "next_attempt_at"],
        unique=False,
    )


def _create_delivery_actions() -> None:
    op.create_table(
        "delivery_actions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("cycle_id", sa.Integer(), nullable=True),
        sa.Column("active_run_id", sa.Integer(), nullable=True),
        sa.Column("action_type", sa.String(length=48), nullable=False),
        sa.Column("idempotency_key", sa.String(length=191), nullable=False),
        sa.Column("desired_version", sa.Integer(), nullable=False),
        sa.Column("expected_head_sha", sa.String(length=64), nullable=True),
        sa.Column("expected_base_sha", sa.String(length=64), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("lease_owner", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(), nullable=True),
        sa.Column("remote_id", sa.String(length=200), nullable=True),
        sa.Column("remote_url", sa.String(length=1000), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'leased', 'unknown', 'succeeded', 'failed', "
            "'cancelled', 'stale')",
            name="ck_delivery_actions_status",
        ),
        sa.CheckConstraint(
            "(status IN ('pending', 'leased', 'unknown') "
            "AND active_run_id IS NOT NULL AND active_run_id = run_id) OR "
            "(status NOT IN ('pending', 'leased', 'unknown') "
            "AND active_run_id IS NULL)",
            name="ck_delivery_actions_active_slot",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["delivery_runs.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["cycle_id"], ["delivery_cycles.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_delivery_actions_idem"),
        sa.UniqueConstraint("active_run_id", name="uq_delivery_actions_active_run"),
    )
    op.create_index(
        "ix_delivery_actions_due",
        "delivery_actions",
        ["status", "next_attempt_at"],
        unique=False,
    )


def _create_delivery_transitions() -> None:
    op.create_table(
        "delivery_transitions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=True),
        sa.Column("cause", sa.String(length=64), nullable=False),
        sa.Column("actor_kind", sa.String(length=32), nullable=False),
        sa.Column("actor_id", sa.String(length=128), nullable=True),
        sa.Column("before_state", sa.JSON(), nullable=False),
        sa.Column("after_state", sa.JSON(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "state_version >= 1", name="ck_delivery_transitions_version"
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["delivery_runs.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["event_id"], ["delivery_events.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "state_version",
            name="uq_delivery_transitions_version",
        ),
    )
    op.create_index(
        "ix_delivery_transitions_run_created",
        "delivery_transitions",
        ["run_id", "created_at"],
        unique=False,
    )


def upgrade() -> None:
    task_id_highwater = _legacy_sqlite_task_id_highwater()
    shared_id_highwater = _legacy_sqlite_shared_id_highwater()
    _purge_stale_task_access_grants()
    _add_owner_columns(task_id_highwater, shared_id_highwater)
    _create_delivery_runs()
    _create_delivery_cycles()
    _create_delivery_turns()
    _create_delivery_events()
    _create_delivery_actions()
    _create_delivery_transitions()


def _assert_delivery_history_empty() -> None:
    """Refuse to discard Controller authority or historical provenance.

    A downgrade leaves the ordinary ``tasks`` and ``worktrees`` rows in place.
    If even one DeliveryRun exists, removing its ownership columns/tables could
    make an old application execute a formerly controlled Developer Task as an
    ordinary task.  Offline SQL cannot inspect the target database, so it is
    deliberately unsupported for this destructive revision.
    """

    context = op.get_context()
    if context.as_sql:
        raise RuntimeError(
            "Offline downgrade of Delivery Loop state is unsafe; run an online "
            "downgrade after proving delivery_runs is empty"
        )
    bind = op.get_bind()
    delivery_tables = (
        "delivery_runs",
        "delivery_cycles",
        "delivery_turns",
        "delivery_events",
        "delivery_actions",
        "delivery_transitions",
    )
    for table_name in delivery_tables:
        if bind.execute(
            sa.text(f"SELECT 1 FROM {table_name} LIMIT 1")
        ).first() is not None:
            raise RuntimeError(
                "Cannot downgrade Delivery Loop state while "
                f"{table_name} contains history; explicitly clean up all "
                "Delivery records first"
            )

    residual_owners = (
        (
            "tasks",
            "delivery_run_id IS NOT NULL OR delivery_role IS NOT NULL "
            "OR mode = 'delivery_loop'",
        ),
        (
            "worktrees",
            "delivery_run_id IS NOT NULL OR task_id IS NOT NULL "
            "OR last_verified_head IS NOT NULL OR cleanup_status <> 'retained'",
        ),
        ("plan_agent_runs", "capability_execution_id IS NOT NULL"),
        ("project_todos", "task_request_hash IS NOT NULL"),
    )
    for table_name, predicate in residual_owners:
        if bind.execute(
            sa.text(
                f"SELECT 1 FROM {table_name} WHERE {predicate} LIMIT 1"
            )
        ).first() is not None:
            raise RuntimeError(
                "Cannot downgrade Delivery Loop state while "
                f"{table_name} retains controller/capability ownership"
            )


def downgrade() -> None:
    _assert_delivery_history_empty()
    # Drop each newly-created table as a unit.  Explicitly dropping its
    # indexes first is unsafe on MySQL: an index may be the implementation of
    # a still-present foreign key (Error 1553), leaving a non-transactional
    # downgrade half applied.  DROP TABLE removes its own indexes atomically
    # with the table on every supported backend.
    op.drop_table("delivery_transitions")
    op.drop_table("delivery_actions")
    op.drop_table("delivery_events")
    op.drop_table("delivery_turns")
    op.drop_table("delivery_cycles")
    op.drop_table("delivery_runs")

    with op.batch_alter_table("worktrees", schema=None) as batch_op:
        batch_op.drop_constraint("ck_worktrees_cleanup_status", type_="check")
        batch_op.drop_constraint("uq_worktrees_delivery_run", type_="unique")
        batch_op.drop_index("ix_worktrees_delivery_run_id")
        batch_op.drop_index("ix_worktrees_task_id")
        batch_op.drop_column("cleanup_status")
        batch_op.drop_column("last_verified_head")
        batch_op.drop_column("delivery_run_id")
        batch_op.drop_column("task_id")

    task_batch_kwargs = {}
    if op.get_bind().dialect.name == "sqlite":
        # AUTOINCREMENT is an external identity safety property, not merely a
        # Delivery feature.  A rollback must not reopen Task-id reuse.
        task_batch_kwargs = {
            "recreate": "always",
            "table_kwargs": {"sqlite_autoincrement": True},
        }
    with op.batch_alter_table(
        "tasks",
        schema=None,
        **task_batch_kwargs,
    ) as batch_op:
        batch_op.drop_constraint("uq_tasks_incarnation_id", type_="unique")
        batch_op.drop_constraint("ck_tasks_delivery_owner_shape", type_="check")
        batch_op.drop_index("ix_tasks_delivery_run_id")
        batch_op.drop_column("delivery_role")
        batch_op.drop_column("delivery_run_id")
        batch_op.drop_column("incarnation_id")

    with op.batch_alter_table("plan_agent_runs", schema=None) as batch_op:
        batch_op.drop_constraint(
            "uq_plan_agent_runs_capability_execution", type_="unique"
        )
        batch_op.drop_column("capability_execution_id")

    with op.batch_alter_table("project_todos", schema=None) as batch_op:
        batch_op.drop_column("task_request_hash")
