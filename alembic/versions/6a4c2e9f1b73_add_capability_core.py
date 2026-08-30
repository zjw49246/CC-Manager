"""add provider-neutral Capability Core audit tables

Revision ID: 6a4c2e9f1b73
Revises: e5b8d1c4a7f2
Create Date: 2026-08-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "6a4c2e9f1b73"
down_revision: Union[str, None] = "e5b8d1c4a7f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ACTIVE_INVOCATIONS = (
    "'queued', 'running', 'waiting_user', 'ready', 'resuming', 'cancelling'"
)
_ALL_INVOCATIONS = (
    _ACTIVE_INVOCATIONS + ", 'completed', 'failed', 'cancelled', 'stale'"
)
_ACTIVE_EXECUTIONS = "'queued', 'running', 'waiting_user', 'cancelling'"
_ALL_EXECUTIONS = (
    _ACTIVE_EXECUTIONS + ", 'completed', 'failed', 'cancelled', 'stale'"
)


def upgrade() -> None:
    op.create_table(
        "capability_invocations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("capability_key", sa.String(length=64), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("purpose", sa.String(length=24), nullable=False),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="queued",
            nullable=False,
        ),
        sa.Column(
            "state_version", sa.Integer(), server_default="1", nullable=False
        ),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("input_payload", sa.JSON(), nullable=False),
        sa.Column("input_hash", sa.String(length=64), nullable=False),
        sa.Column("subject_kind", sa.String(length=32), nullable=False),
        sa.Column("subject_ref", sa.JSON(), nullable=False),
        sa.Column("subject_hash", sa.String(length=64), nullable=False),
        sa.Column("executor_kind", sa.String(length=64), nullable=False),
        sa.Column("executor_config", sa.JSON(), nullable=False),
        sa.Column("executor_config_hash", sa.String(length=64), nullable=False),
        sa.Column("policy_snapshot", sa.JSON(), nullable=False),
        sa.Column("policy_hash", sa.String(length=64), nullable=False),
        sa.Column("resume_policy", sa.String(length=24), nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="1", nullable=False),
        sa.Column("active_task_id", sa.Integer(), nullable=True),
        sa.Column("requested_by_user_id", sa.Integer(), nullable=True),
        sa.Column("request_task_retry_count", sa.Integer(), nullable=True),
        sa.Column("request_task_instance_id", sa.Integer(), nullable=True),
        sa.Column("request_task_started_at", sa.DateTime(), nullable=True),
        sa.Column("request_task_session_id", sa.String(length=200), nullable=True),
        sa.Column("request_task_turn_generation", sa.BigInteger(), nullable=True),
        sa.Column("request_source_log_id", sa.Integer(), nullable=True),
        sa.Column("result_kind", sa.String(length=32), nullable=True),
        sa.Column("result_id", sa.Integer(), nullable=True),
        sa.Column("result_hash", sa.String(length=64), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("ready_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            f"status IN ({_ALL_INVOCATIONS})", name="ck_cap_inv_status"
        ),
        sa.CheckConstraint(
            "source IN ('human_request', 'agent_request', 'delivery_controller')",
            name="ck_cap_inv_source",
        ),
        sa.CheckConstraint(
            "purpose IN ('advisory', 'required_gate')",
            name="ck_cap_inv_purpose",
        ),
        sa.CheckConstraint(
            "resume_policy IN ('attach_only', 'resume_task', 'controller')",
            name="ck_cap_inv_resume_policy",
        ),
        sa.CheckConstraint("state_version >= 1", name="ck_cap_inv_state_version"),
        sa.CheckConstraint("max_attempts >= 1", name="ck_cap_inv_max_attempts"),
        sa.CheckConstraint(
            f"(status IN ({_ACTIVE_INVOCATIONS}) AND active_task_id IS NOT NULL "
            f"AND active_task_id = task_id) OR (status NOT IN ({_ACTIVE_INVOCATIONS}) "
            "AND active_task_id IS NULL)",
            name="ck_cap_inv_active_slot",
        ),
        sa.CheckConstraint(
            "status NOT IN ('ready', 'resuming', 'completed') OR "
            "(result_kind IS NOT NULL AND result_id IS NOT NULL AND "
            "result_hash IS NOT NULL)",
            name="ck_cap_inv_result",
        ),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "task_id", "idempotency_key", name="uq_cap_inv_task_idem"
        ),
        sa.UniqueConstraint("active_task_id", name="uq_cap_inv_active_task"),
    )
    op.create_index(
        "ix_cap_inv_task_created",
        "capability_invocations",
        ["task_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_cap_inv_status_created",
        "capability_invocations",
        ["status", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_cap_inv_key_status",
        "capability_invocations",
        ["capability_key", "status"],
        unique=False,
    )

    op.create_table(
        "capability_executions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("invocation_id", sa.Integer(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="queued",
            nullable=False,
        ),
        sa.Column(
            "state_version", sa.Integer(), server_default="1", nullable=False
        ),
        sa.Column("active_invocation_id", sa.Integer(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("executor_kind", sa.String(length=64), nullable=False),
        sa.Column("input_hash", sa.String(length=64), nullable=False),
        sa.Column("handle_kind", sa.String(length=64), nullable=True),
        sa.Column("handle_id", sa.String(length=200), nullable=True),
        sa.Column("handle_generation", sa.Integer(), nullable=True),
        sa.Column("lease_token", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(), nullable=True),
        sa.Column("output_kind", sa.String(length=32), nullable=True),
        sa.Column("output_id", sa.Integer(), nullable=True),
        sa.Column("output_hash", sa.String(length=64), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            f"status IN ({_ALL_EXECUTIONS})", name="ck_cap_exec_status"
        ),
        sa.CheckConstraint("attempt >= 1", name="ck_cap_exec_attempt"),
        sa.CheckConstraint("state_version >= 1", name="ck_cap_exec_state_version"),
        sa.CheckConstraint(
            f"(status IN ({_ACTIVE_EXECUTIONS}) AND active_invocation_id IS NOT NULL "
            "AND active_invocation_id = invocation_id) OR "
            f"(status NOT IN ({_ACTIVE_EXECUTIONS}) AND active_invocation_id IS NULL)",
            name="ck_cap_exec_active_slot",
        ),
        sa.CheckConstraint(
            "(handle_kind IS NULL AND handle_id IS NULL) OR "
            "(handle_kind IS NOT NULL AND handle_id IS NOT NULL)",
            name="ck_cap_exec_handle",
        ),
        sa.CheckConstraint(
            "status <> 'completed' OR (output_kind IS NOT NULL AND "
            "output_id IS NOT NULL AND output_hash IS NOT NULL)",
            name="ck_cap_exec_output",
        ),
        sa.ForeignKeyConstraint(
            ["invocation_id"],
            ["capability_invocations.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "invocation_id", "attempt", name="uq_cap_exec_inv_attempt"
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_cap_exec_idem"),
        sa.UniqueConstraint(
            "active_invocation_id", name="uq_cap_exec_active_inv"
        ),
    )
    op.create_index(
        "ix_cap_exec_inv_created",
        "capability_executions",
        ["invocation_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_cap_exec_status_created",
        "capability_executions",
        ["status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_cap_exec_status_created", table_name="capability_executions")
    op.drop_index("ix_cap_exec_inv_created", table_name="capability_executions")
    op.drop_table("capability_executions")
    op.drop_index("ix_cap_inv_key_status", table_name="capability_invocations")
    op.drop_index("ix_cap_inv_status_created", table_name="capability_invocations")
    op.drop_index("ix_cap_inv_task_created", table_name="capability_invocations")
    op.drop_table("capability_invocations")
