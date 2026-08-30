"""add durable pre-PR code review runs and results

Revision ID: 8d4e1f7a9c20
Revises: 6a4c2e9f1b73
Create Date: 2026-08-05
"""

import json
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "8d4e1f7a9c20"
down_revision: Union[str, None] = "6a4c2e9f1b73"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "code_review_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("capability_invocation_id", sa.Integer(), nullable=False),
        sa.Column("capability_execution_id", sa.Integer(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="running",
            nullable=False,
        ),
        sa.Column(
            "state_version", sa.Integer(), server_default="1", nullable=False
        ),
        sa.Column("developer_task_id", sa.Integer(), nullable=False),
        sa.Column("reviewer_task_id", sa.Integer(), nullable=False),
        sa.Column(
            "reviewer_task_retry_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("repo_path", sa.String(length=1000), nullable=False),
        sa.Column("base_sha", sa.String(length=40), nullable=False),
        sa.Column("head_sha", sa.String(length=40), nullable=False),
        sa.Column("head_tree_sha", sa.String(length=40), nullable=False),
        sa.Column("patch_sha256", sa.String(length=64), nullable=False),
        sa.Column("subject_ref", sa.JSON(), nullable=False),
        sa.Column("subject_hash", sa.String(length=64), nullable=False),
        sa.Column("prompt_hash", sa.String(length=64), nullable=False),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('running', 'completed', 'failed', 'cancelled', 'stale')",
            name="ck_code_review_run_status",
        ),
        sa.CheckConstraint("attempt >= 1", name="ck_code_review_run_attempt"),
        sa.CheckConstraint(
            "state_version >= 1",
            name="ck_code_review_run_state_version",
        ),
        sa.ForeignKeyConstraint(
            ["capability_invocation_id"],
            ["capability_invocations.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["capability_execution_id"],
            ["capability_executions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["developer_task_id"],
            ["tasks.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["reviewer_task_id"],
            ["tasks.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "capability_execution_id",
            name="uq_code_review_run_execution",
        ),
        sa.UniqueConstraint(
            "reviewer_task_id",
            name="uq_code_review_run_reviewer_task",
        ),
    )
    op.create_index(
        "ix_code_review_run_invocation_attempt",
        "code_review_runs",
        ["capability_invocation_id", "attempt"],
        unique=False,
    )
    op.create_index(
        "ix_code_review_run_status_created",
        "code_review_runs",
        ["status", "created_at"],
        unique=False,
    )

    op.create_table(
        "code_review_results",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("capability_invocation_id", sa.Integer(), nullable=False),
        sa.Column("capability_execution_id", sa.Integer(), nullable=False),
        sa.Column("developer_task_id", sa.Integer(), nullable=False),
        sa.Column("reviewer_task_id", sa.Integer(), nullable=False),
        sa.Column("reviewer_task_retry_count", sa.Integer(), nullable=False),
        sa.Column("reviewer_task_instance_id", sa.Integer(), nullable=True),
        sa.Column("reviewer_task_started_at", sa.DateTime(), nullable=False),
        sa.Column("reviewer_task_completed_at", sa.DateTime(), nullable=False),
        sa.Column("output_log_id", sa.Integer(), nullable=False),
        sa.Column(
            "schema_version", sa.Integer(), server_default="1", nullable=False
        ),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("verdict", sa.String(length=24), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("findings", sa.JSON(), nullable=False),
        sa.Column("subject_ref", sa.JSON(), nullable=False),
        sa.Column("subject_hash", sa.String(length=64), nullable=False),
        sa.Column("result_hash", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "verdict IN ('approved', 'changes_requested')",
            name="ck_code_review_result_verdict",
        ),
        sa.CheckConstraint(
            "schema_version = 1",
            name="ck_code_review_result_schema_version",
        ),
        sa.CheckConstraint(
            "reviewer_task_retry_count >= 0",
            name="ck_code_review_result_retry_count",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["code_review_runs.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["capability_invocation_id"],
            ["capability_invocations.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["capability_execution_id"],
            ["capability_executions.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", name="uq_code_review_result_run"),
        sa.UniqueConstraint(
            "capability_invocation_id",
            name="uq_code_review_result_invocation",
        ),
        sa.UniqueConstraint(
            "capability_execution_id",
            name="uq_code_review_result_execution",
        ),
    )
    op.create_index(
        "ix_code_review_result_subject_created",
        "code_review_results",
        ["subject_hash", "created_at"],
        unique=False,
    )


def _decode_json(value):
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    return value


def _assert_code_review_history_empty() -> None:
    """Keep reviewer Tasks from escaping their durable execution fence."""

    context = op.get_context()
    if context.as_sql:
        raise RuntimeError(
            "Offline downgrade of pre-PR Code Review state is unsafe; run an "
            "online downgrade after proving review state is empty"
        )
    bind = op.get_bind()
    for table_name in ("code_review_results", "code_review_runs"):
        if bind.execute(
            sa.text(f"SELECT 1 FROM {table_name} LIMIT 1")
        ).first() is not None:
            raise RuntimeError(
                "Cannot downgrade pre-PR Code Review state while "
                f"{table_name} contains history"
            )

    rows = bind.execute(
        sa.text(
            "SELECT id, tags, metadata FROM tasks "
            "WHERE tags IS NOT NULL OR metadata IS NOT NULL"
        )
    ).mappings()
    identity_keys = {
        "code_review_run_id",
        "capability_invocation_id",
        "capability_execution_id",
    }
    for row in rows:
        tags = _decode_json(row["tags"])
        metadata = _decode_json(row["metadata"])
        tagged = isinstance(tags, list) and "pre-pr-code-review" in tags
        identified = isinstance(metadata, dict) and identity_keys.issubset(metadata)
        malformed_hint = any(
            marker in value
            for value in (tags, metadata)
            if isinstance(value, str)
            for marker in ("pre-pr-code-review", "code_review_run_id")
        )
        if tagged or identified or malformed_hint:
            raise RuntimeError(
                "Cannot downgrade pre-PR Code Review state while Task "
                f"{row['id']} retains reviewer ownership"
            )


def downgrade() -> None:
    _assert_code_review_history_empty()
    # Let DROP TABLE remove its indexes. On MySQL an explicit index drop may
    # be rejected while the table's foreign key still depends on that index,
    # and DDL failure is non-transactional.
    op.drop_table("code_review_results")
    op.drop_table("code_review_runs")
