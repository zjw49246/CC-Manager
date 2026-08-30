from datetime import datetime
from sqlalchemy import (
    BigInteger,
    Integer,
    String,
    Text,
    DateTime,
    JSON,
    Boolean,
    ForeignKey,
    UniqueConstraint,
    CheckConstraint,
    and_,
    false,
    func,
    or_,
)
from sqlalchemy.orm import Mapped, mapped_column
from backend.database import Base


class MonitoredRepo(Base):
    __tablename__ = "monitored_repos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_full_name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)  # NULL = local, else Worker
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1")
    auto_merge: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0")
    webhook_secret: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = mapped_column(String(20), default="claude", server_default="claude")
    review_model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    review_effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    review_mode: Mapped[str] = mapped_column(
        String(20), default="single", server_default="single"
    )
    wait_for_ci: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=false()
    )
    required_checks: Mapped[list] = mapped_column(
        JSON, default=list, nullable=False
    )
    auto_repair: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=false()
    )
    max_repair_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    merge_queue_mode: Mapped[str] = mapped_column(
        String(20), default="manual", server_default="manual"
    )
    default_branch: Mapped[str] = mapped_column(String(100), default="main", server_default="main")
    allowed_authors: Mapped[dict | None] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String(20), default="active", server_default="active")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PRMonitorTaskTombstone(Base):
    """Durable identity for a PR Monitor Task after its owner graph is deleted."""

    __tablename__ = "pr_monitor_task_tombstones"

    # This intentionally has no foreign key.  The identity must not disappear
    # when a monitor's owner rows are removed, and must remain safe even while
    # legacy databases clean up Task rows outside an FK-enabled transaction.
    task_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )


class PRReview(Base):
    __tablename__ = "pr_reviews"
    __table_args__ = (
        UniqueConstraint(
            "repo_id",
            "pr_number",
            "base_ref",
            "base_sha",
            "head_sha",
            "attempt",
            name="uq_pr_reviews_repo_pr_base_ref_base_head_attempt",
        ),
        UniqueConstraint(
            "repo_id",
            "delivery_id",
            name="uq_pr_reviews_repo_delivery",
        ),
        UniqueConstraint(
            "rerun_of_review_id",
            "rerun_idempotency_key",
            name="uq_pr_reviews_rerun_idempotency",
        ),
        CheckConstraint(
            "attempt >= 1",
            name="ck_pr_reviews_attempt",
        ),
        CheckConstraint(
            "(attempt = 1 AND rerun_of_review_id IS NULL) OR ("
            "attempt > 1 AND rerun_idempotency_key IS NOT NULL AND ("
            "rerun_of_review_id IS NULL OR rerun_of_review_id <> id))",
            name="ck_pr_reviews_rerun_shape",
        ),
        CheckConstraint(
            "(error_category IS NULL AND error_measured IS NULL "
            "AND error_limit IS NULL AND error_unit IS NULL) OR ("
            "error_category IS NOT NULL "
            "AND error_category = 'unsupported_input_size' "
            "AND length(error_category) = 22 "
            "AND length(replace(error_category, 'unsupported_input_size', '')) = 0 "
            "AND error_measured IS NOT NULL "
            "AND error_limit IS NOT NULL "
            "AND error_unit IS NOT NULL "
            "AND error_limit > 0 "
            "AND error_measured > error_limit "
            "AND error_measured <= 9007199254740991 "
            "AND error_unit IN ('characters', 'UTF-8 bytes') "
            "AND ((length(error_unit) = 10 "
            "AND length(replace(error_unit, 'characters', '')) = 0) OR ("
            "length(error_unit) = 11 "
            "AND length(replace(error_unit, 'UTF-8 bytes', '')) = 0)))",
            name="ck_pr_reviews_input_error_evidence",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # A new immutable row is created for every explicit exact-head rerun.  The
    # original webhook admission is attempt 1; no reviewer Task or result is
    # ever reset in place.
    attempt: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    rerun_of_review_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("pr_reviews.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    rerun_idempotency_key: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    monitor_run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("pr_monitor_runs.id"), nullable=True, index=True
    )
    repo_id: Mapped[int] = mapped_column(Integer, ForeignKey("monitored_repos.id"), index=True, nullable=False)
    pr_number: Mapped[int] = mapped_column(Integer, nullable=False)
    # Immutable target branch captured when this PR subject is admitted.
    # Old binaries fail closed after the migration's backfill rather than
    # writing an ambiguous NULL subject during a rolling restart.
    base_ref: Mapped[str] = mapped_column(String(200), nullable=False)
    base_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    head_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    delivery_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    pr_title: Mapped[str] = mapped_column(String(500), nullable=False)
    pr_author: Mapped[str] = mapped_column(String(200), nullable=False)
    pr_url: Mapped[str] = mapped_column(String(500), nullable=False)
    task_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tasks.id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending", server_default="pending")
    # Immutable code result for the exact reviewed subject.  Publication and
    # PR lifecycle transitions are independent axes and must never rewrite or
    # erase this value once the completed Task generation freezes it.
    code_verdict: Mapped[str | None] = mapped_column(String(30), nullable=True)
    # Exact provenance for newly frozen verdicts.  These deliberately are not
    # foreign keys: result evidence must survive archived Reviewer Task cleanup
    # and legacy backfill may have no recoverable Task generation.
    code_verdict_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    code_verdict_retry_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    code_verdict_task_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    code_verdict_recorded_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    review_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_taken: Mapped[str | None] = mapped_column(String(50), nullable=True)
    ci_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    ci_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    ci_details: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # Durable GitHub publication outbox. The nonce is generated before the
    # review Task starts; pending fields are populated by the exact completed
    # Task generation before any external write.
    action_nonce: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pending_action: Mapped[str | None] = mapped_column(String(50), nullable=True)
    pending_review_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    publishing_actor: Mapped[str | None] = mapped_column(
        String(200),
        nullable=True,
    )
    publishing_retry_count: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    publishing_task_started_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    publishing_started_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    # Public result state is deliberately separate from reviewer verdict and
    # PR lifecycle.  The transient outbox fields below may be cleared after a
    # successful write; these fields are immutable publication evidence and
    # remain available to operators and the read-only Tasks result feed.
    publication_state: Mapped[str] = mapped_column(
        String(30), nullable=False, default="not_started", server_default="not_started"
    )
    publication_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_stage: Mapped[str | None] = mapped_column(String(30), nullable=True)
    # Deterministic admission failures are structured separately from both
    # the code-review body and GitHub publication diagnostics.  The current
    # supported category is intentionally narrow so the public result feed
    # can expose bounded evidence without selecting or leaking raw exceptions.
    error_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_measured: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    error_limit: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    error_unit: Mapped[str | None] = mapped_column(String(20), nullable=True)
    published_actor: Mapped[str | None] = mapped_column(String(200), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    github_review_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    github_review_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    github_review_state: Mapped[str | None] = mapped_column(String(30), nullable=True)
    # Immutable GitHub merge strategy selected before the first publication
    # mutation. New rows use an explicit frozen-ref fast-forward; merge/squash
    # remain valid only for recovery of outboxes armed by an older binary.
    merge_method: Mapped[str | None] = mapped_column(
        String(16),
        nullable=True,
    )
    # Cross-process publication lease.  ``publishing`` alone is a durable
    # outbox state, but multiple CCM processes can recover the same row at
    # once.  Only the holder of this random fencing token may call GitHub.
    publishing_lease_token: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    publishing_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    # Durable synchronize intent.  The immutable target snapshot is committed
    # before the old Task is stopped, so a crash can resume replacement
    # creation instead of stranding a completed Task in ``reviewing``.
    superseding_snapshot: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
    )
    superseding_token: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    superseding_started_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRReviewerRun(Base):
    """One independent reviewer role for an immutable PRReview snapshot."""

    __tablename__ = "pr_reviewer_runs"
    __table_args__ = (
        UniqueConstraint(
            "pr_review_id",
            "role",
            name="uq_pr_reviewer_runs_review_role",
        ),
        UniqueConstraint(
            "task_id",
            name="uq_pr_reviewer_runs_task_id",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pr_review_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviews.id"), nullable=False, index=True
    )
    role: Mapped[str] = mapped_column(String(40), nullable=False)
    task_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("tasks.id"), nullable=True, index=True
    )
    provider: Mapped[str] = mapped_column(String(20), nullable=False)
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending", server_default="pending"
    )
    verdict: Mapped[str | None] = mapped_column(String(30), nullable=True)
    result_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    prompt_policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    guide_pack_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRFinding(Base):
    """Structured, exact-subject evidence emitted by one reviewer role."""

    __tablename__ = "pr_findings"
    __table_args__ = (
        UniqueConstraint(
            "reviewer_run_id",
            "fingerprint",
            name="uq_pr_findings_run_fingerprint",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pr_review_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviews.id"), nullable=False, index=True
    )
    reviewer_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviewer_runs.id"), nullable=False, index=True
    )
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    role: Mapped[str] = mapped_column(String(40), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    path: Mapped[str] = mapped_column(String(1000), nullable=False)
    line: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hunk: Mapped[str | None] = mapped_column(String(500), nullable=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    evidence: Mapped[str] = mapped_column(Text, nullable=False)
    impact: Mapped[str] = mapped_column(Text, nullable=False)
    required_fix: Mapped[str] = mapped_column(Text, nullable=False)
    test: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="open", server_default="open"
    )
    thread_nonce: Mapped[str] = mapped_column(String(64), nullable=False)
    thread_status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending", server_default="pending"
    )
    # GitHub database IDs are not bounded by a signed 32-bit SQL INTEGER.
    github_comment_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    github_comment_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    github_thread_node_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    thread_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    thread_published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    thread_resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # One durable cross-process fence covers both accepted-rebuttal and
    # newer-green-head resolution effects for this exact Finding thread.
    resolution_lease_token: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    resolution_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    # Freeze the authenticated identity before a fixed-head fallback comment
    # is attempted so crash recovery can authenticate an existing marker.
    fixed_resolution_actor: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class PRFindingAction(Base):
    """Audited user decision or confirmed patch for one review Finding."""

    __tablename__ = "pr_finding_actions"
    __table_args__ = (
        UniqueConstraint(
            "idempotency_key",
            name="uq_pr_finding_actions_idempotency_key",
        ),
        UniqueConstraint(
            "active_fix_finding_id",
            name="uq_pr_finding_actions_active_fix",
        ),
        CheckConstraint(
            "action_type IN ('ignore', 'human_advice', 'ai_fix')",
            name="ck_pr_finding_actions_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'running', 'awaiting_confirmation', 'cancelling', "
            "'completed', 'failed', 'cancelled', 'stale')",
            name="ck_pr_finding_actions_status",
        ),
        CheckConstraint(
            "(action_type = 'ai_fix' AND status IN ('pending', 'running', "
            "'awaiting_confirmation', 'cancelling') AND "
            "active_fix_finding_id IS NOT NULL AND "
            "active_fix_finding_id = finding_id) OR "
            "((action_type <> 'ai_fix' OR status NOT IN ('pending', 'running', "
            "'awaiting_confirmation', 'cancelling')) AND "
            "active_fix_finding_id IS NULL)",
            name="ck_pr_finding_actions_active_slot",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    finding_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pr_findings.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    action_type: Mapped[str] = mapped_column(String(30), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30),
        default="pending",
        server_default="pending",
        nullable=False,
        index=True,
    )
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    human_advice: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("tasks.id"),
        nullable=True,
        index=True,
    )
    expected_head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    # Non-NULL only while one AI fix owns the Finding's active slot.  A
    # portable UNIQUE constraint supplies the cross-process fence that SQLite
    # cannot provide with SELECT ... FOR UPDATE.
    active_fix_finding_id: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    patch_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    download_receipt_hash: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    downloaded_by_user_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    confirmed_by_user_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Durable push outbox evidence.  ``candidate_commit_sha`` is committed
    # before the first external push and is regenerated deterministically from
    # ``confirmed_at`` after a crash.
    candidate_commit_sha: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    candidate_created_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    push_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    cancelled_by_user_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    cancelled_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    operation_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    operation_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRFindingRebuttal(Base):
    """Evidence-based challenge adjudicated by an isolated Reviewer Task."""

    __tablename__ = "pr_finding_rebuttals"
    __table_args__ = (
        UniqueConstraint(
            "finding_id", "attempt", name="uq_pr_finding_rebuttals_attempt"
        ),
        UniqueConstraint("task_id", name="uq_pr_finding_rebuttals_task_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    finding_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_findings.id"), nullable=False, index=True
    )
    pr_review_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviews.id"), nullable=False, index=True
    )
    monitor_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_monitor_runs.id"), nullable=False, index=True
    )
    developer_task_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("tasks.id"), nullable=False, index=True
    )
    task_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("tasks.id"), nullable=True, index=True
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending", server_default="pending"
    )
    verdict: Mapped[str | None] = mapped_column(String(20), nullable=True)
    result_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    resolution_nonce: Mapped[str] = mapped_column(String(64), nullable=False)
    resolution_actor: Mapped[str | None] = mapped_column(String(200), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRMonitorRun(Base):
    """One durable PR lifecycle spanning immutable review heads."""

    __tablename__ = "pr_monitor_runs"
    __table_args__ = (UniqueConstraint("repo_id", "pr_number", name="uq_pr_monitor_runs_repo_pr"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_id: Mapped[int] = mapped_column(Integer, ForeignKey("monitored_repos.id"), nullable=False, index=True)
    pr_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="observing", server_default="observing")
    current_base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    current_head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    current_review_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # Stable human-facing Task identity for this PR lifecycle. Reviewer Tasks
    # are immutable execution records and may be replaced on every new head;
    # this row remains one-to-one with the repo/PR Run.
    display_task_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("tasks.id"),
        nullable=True,
        index=True,
        unique=True,
    )
    developer_task_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tasks.id"), nullable=True, index=True)
    head_repo_full_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    head_branch: Mapped[str | None] = mapped_column(String(200), nullable=True)
    repair_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_repair_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    no_progress_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    state_version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    pause_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # A signed closed/merged webhook commits this exact intent before any
    # active-effect quiescence. Recovery scans only these rows, never every
    # open PR, so a long-running reviewer cannot outlive GitHub's retry window
    # or create a rate-limit-heavy polling loop.
    terminal_intent_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    terminal_intent_base_ref: Mapped[str | None] = mapped_column(String(200), nullable=True)
    terminal_intent_head_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    terminal_intent_delivery_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    terminal_intent_observed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    terminal_intent_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Set only by the migration for historical lifecycle/publication races.
    # New rows never enter the unsigned legacy recovery scanner by shape.
    legacy_terminal_recovery_pending: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=false(),
    )
    binding_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


def pr_monitor_run_has_terminal_intent(run: PRMonitorRun | None) -> bool:
    """Fail closed on a complete, partial, or legacy terminal intent shape.

    ``terminal_intent_checked_at`` is deliberately excluded: recovery uses it
    as a harmless throttle after disproving a legacy candidate. Every other
    intent field can be the surviving half of an interrupted durable write and
    must revoke new effects until recovery or a signed reopen resolves it.
    """

    if run is not None and hasattr(run, "terminal_intent_present"):
        return bool(run.terminal_intent_present)
    return bool(
        run is not None
        and (
            run.legacy_terminal_recovery_pending is True
            or run.terminal_intent_status is not None
            or run.terminal_intent_base_ref is not None
            or run.terminal_intent_head_sha is not None
            or run.terminal_intent_delivery_id is not None
            or run.terminal_intent_observed_at is not None
        )
    )


def pr_monitor_run_no_terminal_intent_predicate():
    """SQL equivalent of ``not pr_monitor_run_has_terminal_intent``.

    Keep every effect-admission CAS on the same fail-closed partial/legacy
    shape. ``terminal_intent_checked_at`` remains intentionally irrelevant.
    """

    return and_(
        PRMonitorRun.legacy_terminal_recovery_pending.is_(False),
        PRMonitorRun.terminal_intent_status.is_(None),
        PRMonitorRun.terminal_intent_base_ref.is_(None),
        PRMonitorRun.terminal_intent_head_sha.is_(None),
        PRMonitorRun.terminal_intent_delivery_id.is_(None),
        PRMonitorRun.terminal_intent_observed_at.is_(None),
    )


class PRRepairWake(Base):
    """Durable idempotent instruction to resume one Developer Task."""

    __tablename__ = "pr_repair_wakes"
    __table_args__ = (UniqueConstraint("monitor_run_id", "trigger_head_sha", "evidence_hash", name="uq_pr_repair_wakes_subject_evidence"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    monitor_run_id: Mapped[int] = mapped_column(Integer, ForeignKey("pr_monitor_runs.id"), nullable=False, index=True)
    review_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("pr_reviews.id"), nullable=True, index=True)
    developer_task_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tasks.id"), nullable=True, index=True)
    trigger_base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_kind: Mapped[str] = mapped_column(String(30), nullable=False)
    evidence_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="shadow", server_default="shadow")
    attempt: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    delivery_token: Mapped[str] = mapped_column(String(64), nullable=False)
    accepted_worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    accepted_task_retry_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    accepted_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    accepted_task_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    accepted_task_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRMergeQueueAction(Base):
    """Durable merge outbox for one exact PR head.

    ``queue`` rows are retained for recovery of deployments that enabled the
    legacy GitHub Merge Queue integration. New manual merge requests use the
    ``direct`` effect and the same lifecycle ownership fences.
    """

    __tablename__ = "pr_merge_queue_actions"
    __table_args__ = (
        UniqueConstraint(
            "monitor_run_id", "review_id",
            name="uq_pr_merge_queue_actions_run_review",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    monitor_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_monitor_runs.id"), nullable=False, index=True
    )
    review_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviews.id"), nullable=False, index=True
    )
    trigger_base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), default="shadow", server_default="shadow", nullable=False
    )
    effect_kind: Mapped[str] = mapped_column(
        String(20), default="queue", server_default="queue", nullable=False
    )
    trigger_kind: Mapped[str] = mapped_column(
        String(20), default="policy", server_default="policy", nullable=False
    )
    action_nonce: Mapped[str] = mapped_column(String(64), nullable=False)
    publishing_actor: Mapped[str | None] = mapped_column(String(200), nullable=True)
    publishing_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    merge_method: Mapped[str | None] = mapped_column(String(16), nullable=True)
    wait_for_ci: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=false(), nullable=False
    )
    required_checks: Mapped[list] = mapped_column(
        JSON, default=list, nullable=False
    )
    github_pr_node_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    github_queue_entry_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    merge_group_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    merge_group_ref: Mapped[str | None] = mapped_column(String(500), nullable=True)
    ci_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    ci_details: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


_LEGACY_QUEUE_REMOTE_RISK_PREFIXES = (
    "merge_queue_remote_cleanup_failed:",
    "merge_queue_existing_entry_",
    "merge_queue_entry_",
    "merge_group_",
)
_DIRECT_REMOTE_ABSENCE_PREFIX = "direct_merge_remote_absence_proven:"


def pr_merge_queue_action_has_ambiguous_remote_effect(
    action: PRMergeQueueAction | None,
) -> bool:
    """Recognize pre-fix terminal-looking rows that may still queue a PR."""

    if action is None or action.status not in {"paused", "failed"}:
        return False
    error = action.last_error or ""
    if action.effect_kind == "direct":
        if error.startswith(_DIRECT_REMOTE_ABSENCE_PREFIX):
            return False
        return bool(
            action.lease_token is not None
            or (action.attempt_count or 0) > 0
            or error.startswith("direct_merge_")
        )
    if error.startswith("merge_queue_remote_absence_proven:"):
        return False
    return bool(
        action.github_queue_entry_id is not None
        or action.merge_group_sha is not None
        or action.merge_group_ref is not None
        or action.lease_token is not None
        or (action.attempt_count or 0) > 0
        or error.startswith(_LEGACY_QUEUE_REMOTE_RISK_PREFIXES)
    )


def pr_merge_queue_action_ambiguous_remote_effect_predicate():
    """SQL equivalent for legacy Queue and direct-merge effect fences."""

    error = func.coalesce(PRMergeQueueAction.last_error, "")
    risk_error = or_(
        *(
            error.like(f"{prefix}%")
            for prefix in _LEGACY_QUEUE_REMOTE_RISK_PREFIXES
        )
    )
    queue_risk = and_(
        PRMergeQueueAction.effect_kind == "queue",
        PRMergeQueueAction.status.in_(("paused", "failed")),
        ~error.like("merge_queue_remote_absence_proven:%"),
        or_(
            PRMergeQueueAction.github_queue_entry_id.is_not(None),
            PRMergeQueueAction.merge_group_sha.is_not(None),
            PRMergeQueueAction.merge_group_ref.is_not(None),
            PRMergeQueueAction.lease_token.is_not(None),
            PRMergeQueueAction.attempt_count > 0,
            risk_error,
        ),
    )
    direct_risk = and_(
        PRMergeQueueAction.effect_kind == "direct",
        PRMergeQueueAction.status.in_(("paused", "failed")),
        ~error.like(f"{_DIRECT_REMOTE_ABSENCE_PREFIX}%"),
        or_(
            PRMergeQueueAction.lease_token.is_not(None),
            PRMergeQueueAction.attempt_count > 0,
            error.like("direct_merge_%"),
        ),
    )
    return or_(queue_risk, direct_risk)
