"""Durable, provider-neutral records for frontend test harness runs."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
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
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
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
        server_default="{}",
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
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    harness_run_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, unique=True, index=True
    )
    workspace_review_run_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True, unique=True, index=True
    )
    owner_task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    child_task_id: Mapped[int] = mapped_column(
        Integer, nullable=False, unique=True, index=True
    )
    browser_review_job_id: Mapped[str] = mapped_column(
        String(32), nullable=False, unique=True, index=True
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
