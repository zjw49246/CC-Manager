"""Durable, provider-neutral records for frontend test harness runs."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class TestHarnessRun(Base):
    """Immutable request plus mutable lifecycle for one harness invocation."""

    __tablename__ = "test_harness_runs"
    __table_args__ = (
        UniqueConstraint(
            "idempotency_scope",
            "idempotency_key",
            name="uq_test_harness_run_idempotency",
        ),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    owner_task_incarnation_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    owner_task_retry_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    owner_task_turn_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    owner_task_status: Mapped[str | None] = mapped_column(
        String(24), nullable=True
    )
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    owner_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    workspace_review_run_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, unique=True
    )
    browser_review_job_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, index=True
    )
    agent_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    target_kind: Mapped[str] = mapped_column(String(24), nullable=False, index=True)
    target_spec: Mapped[dict] = mapped_column(JSON, nullable=False)
    resolved_target: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    test_plan: Mapped[dict] = mapped_column(JSON, nullable=False)
    runtime_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_scope: Mapped[str | None] = mapped_column(String(200), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)

    parent_run_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    root_run_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    status: Mapped[str] = mapped_column(String(24), nullable=False, default="queued", index=True)
    stage: Mapped[str] = mapped_column(String(48), nullable=False, default="queued")
    verdict: Mapped[str | None] = mapped_column(String(24), nullable=True)
    source_git_head: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    report: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    cleanup_status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending")
    cleanup_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_sequence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TestHarnessAttempt(Base):
    """One concrete browser-agent execution belonging to a harness run."""

    __tablename__ = "test_harness_attempts"
    __table_args__ = (
        UniqueConstraint("run_id", "ordinal", name="uq_test_harness_attempt_ordinal"),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="queued", index=True)
    stage: Mapped[str] = mapped_column(String(48), nullable=False, default="queued")
    provider: Mapped[str] = mapped_column(String(24), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    reasoning_effort: Mapped[str] = mapped_column(String(20), nullable=False)
    codex_service_tier: Mapped[str] = mapped_column(String(20), nullable=False, default="default")
    agent_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    browser_review_job_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, unique=True
    )
    # Legacy compatibility projection. New code treats the explicit staging
    # and archive fields below as authoritative.
    artifact_root: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    artifact_staging_root: Mapped[str | None] = mapped_column(
        String(1000), nullable=True
    )
    artifact_archive_prefix: Mapped[str | None] = mapped_column(
        String(1000), nullable=True
    )
    archive_state: Mapped[str] = mapped_column(
        String(24),
        nullable=False,
        default="staging",
        server_default="staging",
        index=True,
    )
    archive_manifest: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        server_default=text("('{}')"),
    )
    archive_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    result_data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TestHarnessEvent(Base):
    """Ordered, user-visible observation/decision/action/lifecycle event."""

    __tablename__ = "test_harness_events"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="uq_test_harness_event_sequence"),
        UniqueConstraint("run_id", "source_key", name="uq_test_harness_event_source"),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    stage: Mapped[str | None] = mapped_column(String(48), nullable=True)
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    source_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class TestHarnessEvidence(Base):
    """Durable descriptor for a private screenshot/report/telemetry artifact."""

    __tablename__ = "test_harness_evidence"
    __table_args__ = (
        UniqueConstraint("run_id", "name", name="uq_test_harness_evidence_name"),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    attempt_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    content_type: Mapped[str] = mapped_column(String(120), nullable=False)
    storage_path: Mapped[str] = mapped_column(String(1200), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    byte_size: Mapped[int] = mapped_column(Integer, nullable=False)
    metadata_: Mapped[dict] = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class TestHarnessFinding(Base):
    """Normalized defect with a stable cross-run fingerprint."""

    __tablename__ = "test_harness_findings"
    __table_args__ = (
        UniqueConstraint("run_id", "fingerprint", name="uq_test_harness_finding"),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    scenario_id: Mapped[str] = mapped_column(String(120), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    route: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    locator: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    expected: Mapped[str | None] = mapped_column(Text, nullable=True)
    actual: Mapped[str | None] = mapped_column(Text, nullable=True)
    reproduction: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    evidence_names: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class TestHarnessSandboxLease(Base):
    """Durable identity for one ephemeral untrusted-code environment."""

    __tablename__ = "test_harness_sandbox_leases"
    __table_args__ = {"mysql_engine": "InnoDB"}

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(32), nullable=False, unique=True, index=True)
    backend: Mapped[str] = mapped_column(String(24), nullable=False)
    lease_nonce: Mapped[str] = mapped_column(String(48), nullable=False, unique=True)
    image_ref: Mapped[str] = mapped_column(String(500), nullable=False)
    image_digest: Mapped[str | None] = mapped_column(String(128), nullable=True)
    resource_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    resource_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="reserved", index=True)
    phase: Mapped[str] = mapped_column(String(48), nullable=False, default="reserved")
    runtime_metadata: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    cleanup_status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    cleanup_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TestHarnessChildBinding(Base):
    """Durable ownership and launch gate for one isolated Browser Agent Task."""

    __tablename__ = "test_harness_child_bindings"
    __table_args__ = (
        CheckConstraint(
            "harness_run_id IS NOT NULL OR workspace_review_run_id IS NOT NULL",
            name="ck_test_harness_child_binding_owner",
        ),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    harness_run_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, unique=True, index=True
    )
    workspace_review_run_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, unique=True, index=True
    )
    owner_task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    owner_task_incarnation_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    owner_task_retry_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    owner_task_turn_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    owner_task_status: Mapped[str | None] = mapped_column(
        String(24), nullable=True
    )
    child_task_id: Mapped[int] = mapped_column(
        Integer, nullable=False, unique=True, index=True
    )
    child_task_incarnation_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    browser_review_job_id: Mapped[str] = mapped_column(
        String(32), nullable=False, unique=True, index=True
    )
    launch_profile_version: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    provider: Mapped[str | None] = mapped_column(String(24), nullable=True)
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    reasoning_effort: Mapped[str | None] = mapped_column(
        String(20), nullable=True
    )
    codex_service_tier: Mapped[str | None] = mapped_column(
        String(20), nullable=True
    )
    task_mode: Mapped[str | None] = mapped_column(String(20), nullable=True)
    launch_config_digest: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    state: Mapped[str] = mapped_column(
        String(24), nullable=False, default="reserved", index=True
    )
    claimed_retry_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    claimed_instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    activated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    stop_requested_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class BrowserReviewOperationReceipt(Base):
    """At-most-once permit/ACK receipt for one interactive browser action."""

    __tablename__ = "browser_review_operation_receipts"
    __table_args__ = (
        UniqueConstraint(
            "browser_review_job_id",
            "operation_id",
            name="uq_browser_review_operation_job_id",
        ),
        CheckConstraint(
            "status IN ('permitted', 'completed', 'uncertain', 'aborted')",
            name="ck_browser_review_operation_status",
        ),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    browser_review_job_id: Mapped[str] = mapped_column(
        String(32), nullable=False, index=True
    )
    operation_id: Mapped[str] = mapped_column(String(64), nullable=False)
    binding_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    harness_run_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, index=True
    )
    workspace_review_run_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, index=True
    )
    owner_task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    owner_task_incarnation_id: Mapped[str] = mapped_column(
        String(32), nullable=False
    )
    owner_task_retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    owner_task_turn_generation: Mapped[int] = mapped_column(
        BigInteger, nullable=False
    )
    owner_task_status: Mapped[str] = mapped_column(String(24), nullable=False)
    child_task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    child_task_incarnation_id: Mapped[str] = mapped_column(
        String(32), nullable=False
    )
    child_task_retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    child_task_turn_generation: Mapped[int] = mapped_column(
        BigInteger, nullable=False
    )
    child_task_status: Mapped[str] = mapped_column(String(24), nullable=False)
    action_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    request_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    execution_nonce_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="permitted", index=True
    )
    ack_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
