"""Durable, provider-neutral capability invocation audit models.

Capability invocations are orchestration records.  They do not directly own a
Claude/Codex process; an executor adapter links each execution attempt to its
own durable handle (for example a PlanAgentRun or a future CodeReviewRun).
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


ACTIVE_INVOCATION_STATUSES = (
    "queued",
    "running",
    "waiting_user",
    "ready",
    "resuming",
    "cancelling",
)
TERMINAL_INVOCATION_STATUSES = (
    "completed",
    "failed",
    "cancelled",
    "stale",
)
INVOCATION_STATUSES = ACTIVE_INVOCATION_STATUSES + TERMINAL_INVOCATION_STATUSES

ACTIVE_EXECUTION_STATUSES = (
    "queued",
    "running",
    "waiting_user",
    "cancelling",
)
TERMINAL_EXECUTION_STATUSES = (
    "completed",
    "failed",
    "cancelled",
    "stale",
)
EXECUTION_STATUSES = ACTIVE_EXECUTION_STATUSES + TERMINAL_EXECUTION_STATUSES

ACTIVE_RESUME_OUTBOX_STATUSES = (
    "pending",
    "ready",
    "claiming",
    "claimed",
)
TERMINAL_RESUME_OUTBOX_STATUSES = (
    "completed",
    "cancelled",
    "failed",
)
RESUME_OUTBOX_STATUSES = (
    ACTIVE_RESUME_OUTBOX_STATUSES
    + ("launched",)
    + TERMINAL_RESUME_OUTBOX_STATUSES
)


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


class CapabilityInvocation(Base):
    """One logical request for a Plan, review, or future capability."""

    __tablename__ = "capability_invocations"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "idempotency_key",
            name="uq_cap_inv_task_idem",
        ),
        UniqueConstraint(
            "task_id",
            "request_output_log_id",
            name="uq_cap_inv_task_output_log",
        ),
        UniqueConstraint(
            "task_id",
            "request_terminal_log_id",
            name="uq_cap_inv_task_terminal_log",
        ),
        UniqueConstraint("active_task_id", name="uq_cap_inv_active_task"),
        CheckConstraint(
            f"status IN ({_sql_values(INVOCATION_STATUSES)})",
            name="ck_cap_inv_status",
        ),
        CheckConstraint(
            "source IN ('human_request', 'agent_request', "
            "'delivery_controller')",
            name="ck_cap_inv_source",
        ),
        CheckConstraint(
            "purpose IN ('advisory', 'required_gate')",
            name="ck_cap_inv_purpose",
        ),
        CheckConstraint(
            "resume_policy IN ('attach_only', 'resume_task', 'controller')",
            name="ck_cap_inv_resume_policy",
        ),
        CheckConstraint(
            "source <> 'agent_request' OR ("
            "purpose = 'advisory' AND resume_policy = 'resume_task' "
            "AND requested_by_user_id IS NULL "
            "AND request_task_incarnation_id IS NOT NULL "
            "AND LENGTH(request_task_incarnation_id) = 32 "
            "AND request_task_retry_count IS NOT NULL "
            "AND request_task_retry_count >= 0 "
            "AND request_task_turn_generation IS NOT NULL "
            "AND request_task_turn_generation >= 0 "
            "AND request_source_log_id IS NOT NULL "
            "AND request_source_log_id > 0 "
            "AND request_output_log_id IS NOT NULL "
            "AND request_output_log_id > 0 "
            "AND request_terminal_log_id IS NOT NULL "
            "AND request_terminal_log_id > 0 "
            "AND request_reason IS NOT NULL "
            "AND request_protocol_version IS NOT NULL "
            "AND request_protocol_version >= 1 "
            "AND request_output_hash IS NOT NULL "
            "AND LENGTH(request_output_hash) = 64)",
            name="ck_cap_inv_agent_request_identity",
        ),
        CheckConstraint("state_version >= 1", name="ck_cap_inv_state_version"),
        CheckConstraint("max_attempts >= 1", name="ck_cap_inv_max_attempts"),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(ACTIVE_INVOCATION_STATUSES)}"
            ") AND active_task_id IS NOT NULL AND active_task_id = task_id) "
            "OR (status NOT IN ("
            f"{_sql_values(ACTIVE_INVOCATION_STATUSES)}"
            ") AND active_task_id IS NULL)",
            name="ck_cap_inv_active_slot",
        ),
        CheckConstraint(
            "status NOT IN ('ready', 'resuming', 'completed') OR "
            "(result_kind IS NOT NULL AND result_id IS NOT NULL AND "
            "result_hash IS NOT NULL)",
            name="ck_cap_inv_result",
        ),
        Index("ix_cap_inv_task_created", "task_id", "created_at"),
        Index("ix_cap_inv_status_created", "status", "created_at"),
        Index("ix_cap_inv_key_status", "capability_key", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    capability_key: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    purpose: Mapped[str] = mapped_column(String(24), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="queued", server_default="queued"
    )
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    input_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    subject_ref: Mapped[dict] = mapped_column(JSON, nullable=False)
    subject_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    executor_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    executor_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    executor_config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    resume_policy: Mapped[str] = mapped_column(String(24), nullable=False)
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    # Cross-dialect active-slot fence. Multiple NULL values are allowed by all
    # supported databases, while one active invocation stores its task id.
    active_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_task_incarnation_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    request_task_retry_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_task_instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_task_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    request_task_session_id: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    # Reserved for the later exact-turn MCP adapter. The first capability-core
    # API never accepts agent_request and therefore leaves it NULL.
    request_task_turn_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    request_source_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_output_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Codex app-server records a terminal envelope separately from its exact
    # assistant output. Claude transports may point both identities at the
    # same LogEntry. These are durable audit ids rather than foreign keys:
    # Worker migration and history compaction can cross node-local log stores.
    request_terminal_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    request_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_protocol_version: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    request_output_hash: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    request_native_turn_id: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    result_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    result_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    ready_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class CapabilityExecution(Base):
    """One technical attempt to execute a CapabilityInvocation."""

    __tablename__ = "capability_executions"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id",
            "attempt",
            name="uq_cap_exec_inv_attempt",
        ),
        UniqueConstraint("idempotency_key", name="uq_cap_exec_idem"),
        UniqueConstraint(
            "active_invocation_id",
            name="uq_cap_exec_active_inv",
        ),
        CheckConstraint(
            f"status IN ({_sql_values(EXECUTION_STATUSES)})",
            name="ck_cap_exec_status",
        ),
        CheckConstraint("attempt >= 1", name="ck_cap_exec_attempt"),
        CheckConstraint("state_version >= 1", name="ck_cap_exec_state_version"),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(ACTIVE_EXECUTION_STATUSES)}"
            ") AND active_invocation_id IS NOT NULL "
            "AND active_invocation_id = invocation_id) OR "
            "(status NOT IN ("
            f"{_sql_values(ACTIVE_EXECUTION_STATUSES)}"
            ") AND active_invocation_id IS NULL)",
            name="ck_cap_exec_active_slot",
        ),
        CheckConstraint(
            "(handle_kind IS NULL AND handle_id IS NULL) OR "
            "(handle_kind IS NOT NULL AND handle_id IS NOT NULL)",
            name="ck_cap_exec_handle",
        ),
        CheckConstraint(
            "status <> 'completed' OR "
            "(output_kind IS NOT NULL AND output_id IS NOT NULL AND "
            "output_hash IS NOT NULL)",
            name="ck_cap_exec_output",
        ),
        Index("ix_cap_exec_inv_created", "invocation_id", "created_at"),
        Index("ix_cap_exec_status_created", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    invocation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("capability_invocations.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="queued", server_default="queued"
    )
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    active_invocation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    executor_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    handle_kind: Mapped[str | None] = mapped_column(String(64), nullable=True)
    handle_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    handle_generation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    output_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    output_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class CapabilityResumeOutbox(Base):
    """Crash-safe request to advance one exact Task turn after a capability.

    One row is created with the Invocation in the terminal-action admission
    transaction.  It therefore exists before the capability can finish, and
    later freezes both the Invocation outcome and the deterministic resume
    payload before a coordinator claims the Task's G -> G+1 transition.
    """

    __tablename__ = "capability_resume_outbox"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id",
            name="uq_cap_resume_outbox_invocation",
        ),
        UniqueConstraint(
            "task_id",
            "request_task_incarnation_id",
            "request_task_retry_count",
            "from_turn_generation",
            name="uq_cap_resume_outbox_task_generation",
        ),
        UniqueConstraint(
            "active_task_id",
            name="uq_cap_resume_outbox_active_task",
        ),
        UniqueConstraint(
            "active_invocation_id",
            name="uq_cap_resume_outbox_active_inv",
        ),
        CheckConstraint(
            f"status IN ({_sql_values(RESUME_OUTBOX_STATUSES)})",
            name="ck_cap_resume_outbox_status",
        ),
        CheckConstraint(
            "state_version >= 1 AND attempt_count >= 0",
            name="ck_cap_resume_outbox_counters",
        ),
        CheckConstraint(
            "LENGTH(request_task_incarnation_id) = 32 "
            "AND request_task_retry_count >= 0 "
            "AND from_turn_generation >= 0 "
            "AND request_source_log_id > 0 "
            "AND request_output_log_id > 0 "
            "AND request_terminal_log_id > 0 "
            "AND (request_native_turn_id IS NULL "
            "OR LENGTH(request_native_turn_id) > 0)",
            name="ck_cap_resume_outbox_request_identity",
        ),
        CheckConstraint(
            "request_execution_user_role IN ('member', 'admin', 'super_admin') "
            "AND request_execution_mode IN ('sandbox', 'unrestricted') "
            "AND request_execution_principal_kind IN "
            "('user', 'deployment_token', 'system', 'delegated_user', "
            "'delegated_deployment_token') "
            "AND ((request_execution_principal_kind IN "
            "('user', 'delegated_user') "
            "AND request_execution_user_id IS NOT NULL) OR "
            "(request_execution_principal_kind NOT IN "
            "('user', 'delegated_user') "
            "AND request_execution_user_id IS NULL)) "
            "AND ((request_execution_principal_kind = 'system' "
            "AND request_execution_user_role = 'member' "
            "AND request_execution_mode = 'sandbox') OR "
            "(request_execution_principal_kind IN "
            "('deployment_token', 'delegated_deployment_token') "
            "AND request_execution_user_role = 'super_admin' "
            "AND request_execution_mode = 'unrestricted') OR "
            "(request_execution_principal_kind IN "
            "('user', 'delegated_user') AND "
            "((request_execution_user_role IN ('admin', 'super_admin') "
            "AND request_execution_mode = 'unrestricted') OR "
            "(request_execution_user_role = 'member' "
            "AND request_execution_mode = 'sandbox'))))",
            name="ck_cap_resume_outbox_execution_principal",
        ),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(ACTIVE_RESUME_OUTBOX_STATUSES)}"
            ") AND active_task_id IS NOT NULL "
            "AND active_task_id = task_id "
            "AND active_invocation_id IS NOT NULL "
            "AND active_invocation_id = invocation_id) OR "
            "(status NOT IN ("
            f"{_sql_values(ACTIVE_RESUME_OUTBOX_STATUSES)}"
            ") AND active_task_id IS NULL "
            "AND active_invocation_id IS NULL)",
            name="ck_cap_resume_outbox_active_slot",
        ),
        CheckConstraint(
            "(invocation_terminal_status IS NULL "
            "AND invocation_result_kind IS NULL "
            "AND invocation_result_id IS NULL "
            "AND invocation_result_hash IS NULL "
            "AND invocation_error_code IS NULL "
            "AND invocation_error_message IS NULL) OR "
            "(invocation_terminal_status IN "
            "('completed', 'failed', 'cancelled', 'stale') AND "
            "((invocation_terminal_status = 'completed' "
            "AND invocation_result_kind IS NOT NULL "
            "AND invocation_result_id IS NOT NULL "
            "AND invocation_result_hash IS NOT NULL "
            "AND LENGTH(invocation_result_hash) = 64) OR "
            "(invocation_terminal_status <> 'completed' "
            "AND invocation_result_kind IS NULL "
            "AND invocation_result_id IS NULL "
            "AND invocation_result_hash IS NULL)))",
            name="ck_cap_resume_outbox_inv_result",
        ),
        CheckConstraint(
            "(resume_payload IS NULL AND resume_payload_hash IS NULL) OR "
            "(resume_payload IS NOT NULL "
            "AND resume_payload_hash IS NOT NULL "
            "AND LENGTH(resume_payload_hash) = 64)",
            name="ck_cap_resume_outbox_payload",
        ),
        CheckConstraint(
            "(status = 'pending' "
            "AND invocation_terminal_status IS NULL "
            "AND resume_payload IS NULL) OR "
            "(status IN ('ready', 'claiming', 'claimed', 'launched', "
            "'completed') "
            "AND invocation_terminal_status IS NOT NULL "
            "AND resume_payload IS NOT NULL) OR "
            "status IN ('cancelled', 'failed')",
            name="ck_cap_resume_outbox_status_payload",
        ),
        CheckConstraint(
            "(status IN ('claimed', 'launched', 'completed') "
            "AND resume_source_log_id IS NOT NULL "
            "AND resume_source_log_id > 0 "
            "AND claimed_turn_generation = from_turn_generation + 1 "
            "AND claimed_at IS NOT NULL) OR "
            "(status IN ('pending', 'ready', 'claiming') "
            "AND resume_source_log_id IS NULL "
            "AND claimed_turn_generation IS NULL "
            "AND claimed_at IS NULL) OR "
            "(status IN ('cancelled', 'failed') AND "
            "((resume_source_log_id IS NULL "
            "AND claimed_turn_generation IS NULL "
            "AND claimed_at IS NULL) OR "
            "(resume_source_log_id IS NOT NULL "
            "AND resume_source_log_id > 0 "
            "AND claimed_turn_generation = from_turn_generation + 1 "
            "AND claimed_at IS NOT NULL)))",
            name="ck_cap_resume_outbox_claim",
        ),
        CheckConstraint(
            "(status = 'claiming' "
            "AND lease_token IS NOT NULL "
            "AND LENGTH(lease_token) = 64 "
            "AND lease_expires_at IS NOT NULL "
            "AND attempt_count >= 1) OR "
            "(status = 'claimed' AND attempt_count >= 1 AND "
            "((lease_token IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_token IS NOT NULL "
            "AND LENGTH(lease_token) = 64 "
            "AND lease_expires_at IS NOT NULL))) OR "
            "(status NOT IN ('claiming', 'claimed') "
            "AND lease_token IS NULL "
            "AND lease_expires_at IS NULL)",
            name="ck_cap_resume_outbox_lease",
        ),
        CheckConstraint(
            "status IN ('ready', 'claimed') OR next_attempt_at IS NULL",
            name="ck_cap_resume_outbox_backoff",
        ),
        CheckConstraint(
            "((error_code IS NULL AND error_message IS NULL) OR "
            "(error_code IS NOT NULL AND LENGTH(error_code) > 0 "
            "AND error_message IS NOT NULL)) AND "
            "(status NOT IN ('cancelled', 'failed') OR "
            "error_code IS NOT NULL)",
            name="ck_cap_resume_outbox_error",
        ),
        CheckConstraint(
            "(status IN ('launched', 'completed') "
            "AND resume_actual_transport IN "
            "('claude_pty', 'claude_exec', 'codex_app_server', "
            "'codex_exec') "
            "AND launched_at IS NOT NULL) OR "
            "(status IN ('pending', 'ready', 'claiming', 'claimed') "
            "AND resume_actual_transport IS NULL "
            "AND launched_at IS NULL) OR "
            "(status IN ('cancelled', 'failed') AND "
            "((resume_actual_transport IS NULL AND launched_at IS NULL) OR "
            "(resume_actual_transport IN "
            "('claude_pty', 'claude_exec', 'codex_app_server', "
            "'codex_exec') AND launched_at IS NOT NULL "
            "AND resume_source_log_id IS NOT NULL)))",
            name="ck_cap_resume_outbox_transport",
        ),
        CheckConstraint(
            "((status = 'pending' AND ready_at IS NULL) OR "
            "(status IN ('ready', 'claiming', 'claimed', 'launched', "
            "'completed') "
            "AND ready_at IS NOT NULL) OR "
            "status IN ('cancelled', 'failed')) AND "
            "((status IN ('pending', 'ready', 'claiming', 'claimed', "
            "'launched') "
            "AND completed_at IS NULL) OR "
            "(status IN ('completed', 'cancelled', 'failed') "
            "AND completed_at IS NOT NULL)) AND "
            "(ready_at IS NULL OR ready_at >= created_at) AND "
            "(claimed_at IS NULL OR "
            "(ready_at IS NOT NULL AND claimed_at >= ready_at)) AND "
            "(launched_at IS NULL OR "
            "(claimed_at IS NOT NULL AND launched_at >= claimed_at)) AND "
            "(completed_at IS NULL OR completed_at >= created_at)",
            name="ck_cap_resume_outbox_timeline",
        ),
        Index(
            "ix_cap_resume_outbox_due",
            "status",
            "next_attempt_at",
        ),
        Index(
            "ix_cap_resume_outbox_task_created",
            "task_id",
            "created_at",
        ),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    invocation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("capability_invocations.id", ondelete="CASCADE"),
        nullable=False,
    )
    active_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    active_invocation_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="pending", server_default="pending"
    )
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )

    # Immutable identity of the exact terminal model turn that requested the
    # capability. Log ids intentionally are not foreign keys; see the matching
    # CapabilityInvocation audit fields above.
    request_task_incarnation_id: Mapped[str] = mapped_column(
        String(32), nullable=False
    )
    request_task_retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    from_turn_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    request_task_session_id: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    request_source_log_id: Mapped[int] = mapped_column(Integer, nullable=False)
    request_output_log_id: Mapped[int] = mapped_column(Integer, nullable=False)
    request_terminal_log_id: Mapped[int] = mapped_column(Integer, nullable=False)
    request_native_turn_id: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    # Immutable authority of the exact model turn that requested the
    # capability. Resume must inherit it rather than run as a system message.
    request_execution_user_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    request_execution_user_role: Mapped[str] = mapped_column(
        String(20), nullable=False, default="member", server_default="member"
    )
    request_execution_mode: Mapped[str] = mapped_column(
        String(20), nullable=False, default="sandbox", server_default="sandbox"
    )
    request_execution_principal_kind: Mapped[str] = mapped_column(
        String(32), nullable=False, default="system", server_default="system"
    )

    # Frozen Invocation terminal outcome. A non-success outcome is still a
    # ready resume: the original agent must be told that its request failed.
    invocation_terminal_status: Mapped[str | None] = mapped_column(
        String(24), nullable=True
    )
    invocation_result_kind: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    invocation_result_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    invocation_result_hash: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    invocation_error_code: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    invocation_error_message: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )
    resume_payload: Mapped[dict | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    resume_payload_hash: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )

    # Evidence that the same Task incarnation atomically accepted G+1.
    resume_source_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    claimed_turn_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    # Copied only after the resume source LogEntry's actual_transport proves
    # that G+1 crossed the concrete Claude/Codex provider boundary.
    resume_actual_transport: Mapped[str | None] = mapped_column(
        String(24), nullable=True
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    # Coordinator/reconciliation diagnostics are separate from the frozen
    # CapabilityInvocation outcome above. Active rows may retain a transient
    # failure; cancelled/failed outbox terminals must state their own cause.
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    ready_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    launched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
