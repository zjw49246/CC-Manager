"""add durable provider-neutral test harness records

Revision ID: 7d2f4b9a6c10
Revises: 5a7d2c9e1b40
Create Date: 2026-08-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "7d2f4b9a6c10"
down_revision: Union[str, None] = "5a7d2c9e1b40"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "test_harness_runs",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=True),
        sa.Column("project_id", sa.Integer(), nullable=True),
        sa.Column("owner_user_id", sa.Integer(), nullable=True),
        sa.Column("workspace_review_run_id", sa.String(length=32), nullable=True),
        sa.Column("browser_review_job_id", sa.String(length=32), nullable=True),
        sa.Column("agent_task_id", sa.Integer(), nullable=True),
        sa.Column("target_kind", sa.String(length=24), nullable=False),
        sa.Column("target_spec", sa.JSON(), nullable=False),
        sa.Column("test_plan", sa.JSON(), nullable=False),
        sa.Column("runtime_config", sa.JSON(), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("idempotency_scope", sa.String(length=200), nullable=True),
        sa.Column("idempotency_key", sa.String(length=200), nullable=True),
        sa.Column("parent_run_id", sa.String(length=32), nullable=True),
        sa.Column("root_run_id", sa.String(length=32), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("stage", sa.String(length=48), nullable=False),
        sa.Column("verdict", sa.String(length=24), nullable=True),
        sa.Column("source_git_head", sa.String(length=64), nullable=True),
        sa.Column("source_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("stale", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("report", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("cleanup_status", sa.String(length=24), nullable=False),
        sa.Column("cleanup_error", sa.Text(), nullable=True),
        sa.Column("event_sequence", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_scope",
            "idempotency_key",
            name="uq_test_harness_run_idempotency",
        ),
        sa.UniqueConstraint("workspace_review_run_id"),
    )
    for column in (
        "task_id",
        "project_id",
        "owner_user_id",
        "browser_review_job_id",
        "agent_task_id",
        "target_kind",
        "parent_run_id",
        "root_run_id",
        "status",
    ):
        op.create_index(
            f"ix_test_harness_runs_{column}", "test_harness_runs", [column]
        )

    op.create_table(
        "test_harness_attempts",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("run_id", sa.String(length=32), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("stage", sa.String(length=48), nullable=False),
        sa.Column("provider", sa.String(length=24), nullable=False),
        sa.Column("model", sa.String(length=100), nullable=False),
        sa.Column("reasoning_effort", sa.String(length=20), nullable=False),
        sa.Column("codex_service_tier", sa.String(length=20), nullable=False),
        sa.Column("agent_task_id", sa.Integer(), nullable=True),
        sa.Column("browser_review_job_id", sa.String(length=32), nullable=True),
        sa.Column("artifact_root", sa.String(length=1000), nullable=True),
        sa.Column("result_data", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("browser_review_job_id"),
        sa.UniqueConstraint(
            "run_id", "ordinal", name="uq_test_harness_attempt_ordinal"
        ),
    )
    for column in ("run_id", "status", "agent_task_id"):
        op.create_index(
            f"ix_test_harness_attempts_{column}", "test_harness_attempts", [column]
        )

    op.create_table(
        "test_harness_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=32), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("stage", sa.String(length=48), nullable=True),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("source_key", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "sequence", name="uq_test_harness_event_sequence"
        ),
        sa.UniqueConstraint(
            "run_id", "source_key", name="uq_test_harness_event_source"
        ),
    )
    op.create_index("ix_test_harness_events_run_id", "test_harness_events", ["run_id"])

    op.create_table(
        "test_harness_evidence",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("run_id", sa.String(length=32), nullable=False),
        sa.Column("attempt_id", sa.String(length=32), nullable=True),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("content_type", sa.String(length=120), nullable=False),
        sa.Column("storage_path", sa.String(length=1200), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("byte_size", sa.Integer(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "name", name="uq_test_harness_evidence_name"),
    )
    for column in ("run_id", "attempt_id", "kind"):
        op.create_index(
            f"ix_test_harness_evidence_{column}", "test_harness_evidence", [column]
        )

    op.create_table(
        "test_harness_findings",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("run_id", sa.String(length=32), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("scenario_id", sa.String(length=120), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("category", sa.String(length=80), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("route", sa.String(length=1000), nullable=True),
        sa.Column("locator", sa.String(length=1000), nullable=True),
        sa.Column("expected", sa.Text(), nullable=True),
        sa.Column("actual", sa.Text(), nullable=True),
        sa.Column("reproduction", sa.JSON(), nullable=False),
        sa.Column("evidence_names", sa.JSON(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "fingerprint", name="uq_test_harness_finding"),
    )
    for column in ("run_id", "fingerprint", "severity"):
        op.create_index(
            f"ix_test_harness_findings_{column}", "test_harness_findings", [column]
        )

    with op.batch_alter_table("workspace_review_runs", schema=None) as batch_op:
        batch_op.add_column(sa.Column("harness_run_id", sa.String(length=32), nullable=True))
        batch_op.create_unique_constraint(
            "uq_workspace_review_runs_harness_run_id", ["harness_run_id"]
        )


def downgrade() -> None:
    with op.batch_alter_table("workspace_review_runs", schema=None) as batch_op:
        batch_op.drop_constraint(
            "uq_workspace_review_runs_harness_run_id", type_="unique"
        )
        batch_op.drop_column("harness_run_id")

    for table, columns in (
        ("test_harness_findings", ("severity", "fingerprint", "run_id")),
        ("test_harness_evidence", ("kind", "attempt_id", "run_id")),
        ("test_harness_events", ("run_id",)),
        ("test_harness_attempts", ("agent_task_id", "status", "run_id")),
        (
            "test_harness_runs",
            (
                "status",
                "root_run_id",
                "parent_run_id",
                "target_kind",
                "agent_task_id",
                "browser_review_job_id",
                "owner_user_id",
                "project_id",
                "task_id",
            ),
        ),
    ):
        for column in columns:
            op.drop_index(f"ix_{table}_{column}", table_name=table)
        op.drop_table(table)
