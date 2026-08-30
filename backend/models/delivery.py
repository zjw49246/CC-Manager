"""Durable state for the autonomous delivery-loop controller.

The delivery tables deliberately store orchestration truth separately from a
``Task``.  A Task may finish many bounded turns while one DeliveryRun remains
active, waiting for a capability or an exact PR Monitor head.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
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


DELIVERY_PHASES = (
    "planning",
    "coding",
    "pre_review",
    "frontend_review",
    "publishing",
    "monitoring",
    "done",
)
DELIVERY_ACTIVITIES = ("ready", "running", "waiting", "paused", "terminal")
DELIVERY_OUTCOMES = ("success", "failed", "cancelled", "superseded")

DELIVERY_CYCLE_ACTIVE_STATUSES = (
    "planning",
    "coding",
    "pre_review",
    "frontend_review",
    "publishing",
)
DELIVERY_CYCLE_TERMINAL_STATUSES = (
    "completed",
    "failed",
    "cancelled",
    "superseded",
)
DELIVERY_CYCLE_STATUSES = (
    DELIVERY_CYCLE_ACTIVE_STATUSES + DELIVERY_CYCLE_TERMINAL_STATUSES
)

DELIVERY_TURN_ACTIVE_STATUSES = (
    "queued",
    "dispatching",
    "running",
    "reconciling",
)
DELIVERY_TURN_TERMINAL_STATUSES = (
    "completed",
    "failed",
    "cancelled",
    "stale",
    "superseded",
)
DELIVERY_TURN_STATUSES = DELIVERY_TURN_ACTIVE_STATUSES + DELIVERY_TURN_TERMINAL_STATUSES

DELIVERY_ACTION_ACTIVE_STATUSES = ("pending", "leased", "unknown")
DELIVERY_ACTION_TERMINAL_STATUSES = (
    "succeeded",
    "failed",
    "cancelled",
    "stale",
)
DELIVERY_ACTION_STATUSES = (
    DELIVERY_ACTION_ACTIVE_STATUSES + DELIVERY_ACTION_TERMINAL_STATUSES
)


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


class DeliveryRun(Base):
    """One request-to-ready-to-merge lifecycle."""

    __tablename__ = "delivery_runs"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "delivery_branch",
            name="uq_delivery_runs_project_branch",
        ),
        UniqueConstraint(
            "admission_scope",
            "project_id",
            "idempotency_key",
            name="uq_delivery_runs_admission",
        ),
        UniqueConstraint(
            "source_todo_id",
            name="uq_delivery_runs_source_todo",
        ),
        UniqueConstraint(
            "developer_task_id",
            name="uq_delivery_runs_developer_task",
        ),
        UniqueConstraint(
            "pr_monitor_run_id",
            name="uq_delivery_runs_monitor_run",
        ),
        UniqueConstraint("worktree_id", name="uq_delivery_runs_worktree"),
        CheckConstraint(
            f"phase IN ({_sql_values(DELIVERY_PHASES)})",
            name="ck_delivery_runs_phase",
        ),
        CheckConstraint(
            f"activity IN ({_sql_values(DELIVERY_ACTIVITIES)})",
            name="ck_delivery_runs_activity",
        ),
        CheckConstraint(
            f"outcome IS NULL OR outcome IN ({_sql_values(DELIVERY_OUTCOMES)})",
            name="ck_delivery_runs_outcome",
        ),
        CheckConstraint(
            "(phase = 'done' AND activity = 'terminal' AND "
            "outcome IS NOT NULL AND completed_at IS NOT NULL) OR "
            "(phase <> 'done' AND activity <> 'terminal' AND "
            "outcome IS NULL AND completed_at IS NULL)",
            name="ck_delivery_runs_terminal_shape",
        ),
        CheckConstraint("state_version >= 1", name="ck_delivery_runs_version"),
        CheckConstraint("cycle_count >= 0", name="ck_delivery_runs_cycle_count"),
        CheckConstraint("turn_count >= 0", name="ck_delivery_runs_turn_count"),
        CheckConstraint("max_cycles >= 1", name="ck_delivery_runs_max_cycles"),
        CheckConstraint(
            "max_no_progress >= 1", name="ck_delivery_runs_max_no_progress"
        ),
        Index(
            "ix_delivery_runs_due",
            "activity",
            "next_reconcile_at",
        ),
        Index("ix_delivery_runs_project_created", "project_id", "created_at"),
        Index(
            "ix_delivery_runs_repo_pr",
            "monitored_repo_id",
            "pr_number",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # ``created_by`` is nullable for auth-disabled/internal callers, so it
    # cannot itself form a portable idempotency scope: every supported
    # database permits multiple NULLs in a unique key.  Persist an explicit
    # principal namespace instead and combine it with project + caller key.
    admission_scope: Mapped[str] = mapped_column(String(80), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    project_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("projects.id"),
        nullable=False,
        index=True,
    )
    monitored_repo_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("monitored_repos.id"),
        nullable=True,
        index=True,
    )
    source_todo_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("project_todos.id"),
        nullable=True,
        index=True,
    )
    developer_task_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    pr_monitor_run_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("pr_monitor_runs.id"),
        nullable=True,
        index=True,
    )
    worktree_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("worktrees.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    title: Mapped[str] = mapped_column(String(200), nullable=False)
    requirements: Mapped[str] = mapped_column(Text, nullable=False)
    requirements_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    base_branch: Mapped[str] = mapped_column(String(200), nullable=False)
    delivery_branch: Mapped[str] = mapped_column(String(200), nullable=False)
    workspace_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    base_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    head_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    head_tree_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    patch_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    head_generation: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    pr_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pr_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)

    phase: Mapped[str] = mapped_column(
        String(24), nullable=False, default="planning", server_default="planning"
    )
    activity: Mapped[str] = mapped_column(
        String(24), nullable=False, default="ready", server_default="ready"
    )
    outcome: Mapped[str | None] = mapped_column(String(24), nullable=True)
    wait_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    paused_from_activity: Mapped[str | None] = mapped_column(String(24), nullable=True)
    pause_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )

    current_cycle_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cycle_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    turn_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    max_cycles: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10, server_default="10"
    )
    no_progress_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    max_no_progress: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default="3"
    )
    last_progress_signature: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )

    controller_generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    lease_owner: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_reconcile_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class DeliveryCycle(Base):
    """One Plan -> Code -> pre-PR Review attempt within a DeliveryRun."""

    __tablename__ = "delivery_cycles"
    __table_args__ = (
        UniqueConstraint(
            "run_id", "cycle_number", name="uq_delivery_cycles_run_number"
        ),
        UniqueConstraint("active_run_id", name="uq_delivery_cycles_active_run"),
        UniqueConstraint(
            "plan_invocation_id", name="uq_delivery_cycles_plan_invocation"
        ),
        UniqueConstraint(
            "review_invocation_id", name="uq_delivery_cycles_review_invocation"
        ),
        UniqueConstraint(
            "frontend_review_run_id",
            name="uq_delivery_cycles_frontend_review_run",
        ),
        CheckConstraint(
            f"status IN ({_sql_values(DELIVERY_CYCLE_STATUSES)})",
            name="ck_delivery_cycles_status",
        ),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(DELIVERY_CYCLE_ACTIVE_STATUSES)}"
            ") AND active_run_id IS NOT NULL AND active_run_id = run_id) OR "
            "(status NOT IN ("
            f"{_sql_values(DELIVERY_CYCLE_ACTIVE_STATUSES)}"
            ") AND active_run_id IS NULL)",
            name="ck_delivery_cycles_active_slot",
        ),
        CheckConstraint("cycle_number >= 1", name="ck_delivery_cycles_number"),
        CheckConstraint("state_version >= 1", name="ck_delivery_cycles_version"),
        CheckConstraint(
            "frontend_review_profile_index >= 0",
            name="ck_delivery_cycles_frontend_profile_index",
        ),
        Index("ix_delivery_cycles_run_created", "run_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("delivery_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    cycle_number: Mapped[int] = mapped_column(Integer, nullable=False)
    active_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="planning", server_default="planning"
    )
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    trigger_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    trigger_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_pr_review_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("pr_reviews.id"),
        nullable=True,
    )
    trigger_pr_repair_wake_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("pr_repair_wakes.id"),
        nullable=True,
    )
    base_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    start_head_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_head_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_head_tree_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_patch_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    plan_invocation_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("capability_invocations.id"),
        nullable=True,
    )
    plan_version_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("plan_versions.id"),
        nullable=True,
    )
    review_invocation_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("capability_invocations.id"),
        nullable=True,
    )
    review_result_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("code_review_results.id"),
        nullable=True,
    )
    review_verdict: Mapped[str | None] = mapped_column(String(32), nullable=True)
    review_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Browser/Test Harness is a read-only quality gate over the exact
    # Developer workspace. The string identity belongs to TestHarnessRun;
    # keep it as a durable external handle instead of creating a cross-domain
    # ORM relationship that could bypass Harness cleanup fences.
    frontend_review_run_id: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
    )
    frontend_review_config_snapshot: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
    )
    frontend_review_profile_ids: Mapped[list] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        server_default="[]",
    )
    frontend_review_profile_index: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    frontend_review_results: Mapped[list] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        server_default="[]",
    )
    frontend_review_verdict: Mapped[str | None] = mapped_column(
        String(24),
        nullable=True,
    )
    frontend_review_summary: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    frontend_review_skip_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class DeliveryTurn(Base):
    """One bounded Developer Task turn admitted by the controller."""

    __tablename__ = "delivery_turns"
    __table_args__ = (
        UniqueConstraint("run_id", "generation", name="uq_delivery_turns_generation"),
        UniqueConstraint("correlation_id", name="uq_delivery_turns_correlation"),
        UniqueConstraint("active_run_id", name="uq_delivery_turns_active_run"),
        CheckConstraint(
            f"status IN ({_sql_values(DELIVERY_TURN_STATUSES)})",
            name="ck_delivery_turns_status",
        ),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(DELIVERY_TURN_ACTIVE_STATUSES)}"
            ") AND active_run_id IS NOT NULL AND active_run_id = run_id) OR "
            "(status NOT IN ("
            f"{_sql_values(DELIVERY_TURN_ACTIVE_STATUSES)}"
            ") AND active_run_id IS NULL)",
            name="ck_delivery_turns_active_slot",
        ),
        CheckConstraint("generation >= 1", name="ck_delivery_turns_generation"),
        CheckConstraint("attempts >= 0", name="ck_delivery_turns_attempts"),
        Index("ix_delivery_turns_run_created", "run_id", "created_at"),
        Index("ix_delivery_turns_status_created", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("delivery_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    cycle_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("delivery_cycles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(64), nullable=False)
    active_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    purpose: Mapped[str] = mapped_column(String(32), nullable=False)
    trigger_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    prompt_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    prompt_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="queued", server_default="queued"
    )
    task_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    task_retry_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    task_instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    task_started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    task_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    source_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checkpoint: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    checkpoint_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    progress_signature_before: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    progress_signature_after: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class DeliveryEvent(Base):
    """Append-only normalized cause consumed by the delivery reducer."""

    __tablename__ = "delivery_events"
    __table_args__ = (
        UniqueConstraint("source", "source_event_id", name="uq_delivery_events_source"),
        UniqueConstraint("run_id", "sequence", name="uq_delivery_events_sequence"),
        CheckConstraint("sequence >= 1", name="ck_delivery_events_sequence"),
        CheckConstraint(
            "status IN ('pending', 'processing', 'processed', 'dead_letter')",
            name="ck_delivery_events_status",
        ),
        Index("ix_delivery_events_pending", "status", "next_attempt_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("delivery_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    cycle_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("delivery_cycles.id", ondelete="CASCADE"),
        nullable=True,
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    subject_ref: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="pending", server_default="pending"
    )
    lease_owner: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class DeliveryAction(Base):
    """Durable outbox row for a controller-owned Git or GitHub effect."""

    __tablename__ = "delivery_actions"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_delivery_actions_idem"),
        UniqueConstraint("active_run_id", name="uq_delivery_actions_active_run"),
        CheckConstraint(
            f"status IN ({_sql_values(DELIVERY_ACTION_STATUSES)})",
            name="ck_delivery_actions_status",
        ),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(DELIVERY_ACTION_ACTIVE_STATUSES)}"
            ") AND active_run_id IS NOT NULL AND active_run_id = run_id) OR "
            "(status NOT IN ("
            f"{_sql_values(DELIVERY_ACTION_ACTIVE_STATUSES)}"
            ") AND active_run_id IS NULL)",
            name="ck_delivery_actions_active_slot",
        ),
        Index("ix_delivery_actions_due", "status", "next_attempt_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("delivery_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    cycle_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("delivery_cycles.id", ondelete="CASCADE"),
        nullable=True,
    )
    active_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    action_type: Mapped[str] = mapped_column(String(48), nullable=False)
    # The canonical publish key embeds two Git object ids. SHA-256 repositories
    # therefore need 148+ characters even for a small Run id. Keep this unique
    # key within the common utf8mb4 index-safe width while covering the full
    # 64-character OID form.
    idempotency_key: Mapped[str] = mapped_column(String(191), nullable=False)
    desired_version: Mapped[int] = mapped_column(Integer, nullable=False)
    expected_head_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expected_base_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="pending", server_default="pending"
    )
    lease_owner: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    remote_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    remote_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class DeliveryTransition(Base):
    """Append-only audit of each accepted DeliveryRun state version."""

    __tablename__ = "delivery_transitions"
    __table_args__ = (
        UniqueConstraint(
            "run_id", "state_version", name="uq_delivery_transitions_version"
        ),
        CheckConstraint("state_version >= 1", name="ck_delivery_transitions_version"),
        Index("ix_delivery_transitions_run_created", "run_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("delivery_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("delivery_events.id", ondelete="SET NULL"),
        nullable=True,
    )
    cause: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    actor_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    before_state: Mapped[dict] = mapped_column(JSON, nullable=False)
    after_state: Mapped[dict] = mapped_column(JSON, nullable=False)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
