"""Durable pre-PR code-review attempts and immutable results."""

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


CODE_REVIEW_RUN_STATUSES = (
    "running",
    "completed",
    "failed",
    "cancelled",
    "stale",
)
CODE_REVIEW_VERDICTS = ("approved", "changes_requested")


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


class CodeReviewRun(Base):
    """One CapabilityExecution bound to one immutable Git subject and Task."""

    __tablename__ = "code_review_runs"
    __table_args__ = (
        UniqueConstraint(
            "capability_execution_id",
            name="uq_code_review_run_execution",
        ),
        UniqueConstraint(
            "reviewer_task_id",
            name="uq_code_review_run_reviewer_task",
        ),
        CheckConstraint(
            f"status IN ({_sql_values(CODE_REVIEW_RUN_STATUSES)})",
            name="ck_code_review_run_status",
        ),
        CheckConstraint("attempt >= 1", name="ck_code_review_run_attempt"),
        CheckConstraint(
            "state_version >= 1",
            name="ck_code_review_run_state_version",
        ),
        Index(
            "ix_code_review_run_invocation_attempt",
            "capability_invocation_id",
            "attempt",
        ),
        Index(
            "ix_code_review_run_status_created",
            "status",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    capability_invocation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("capability_invocations.id", ondelete="CASCADE"),
        nullable=False,
    )
    capability_execution_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("capability_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="running", server_default="running"
    )
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    developer_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    reviewer_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    reviewer_task_retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    repo_path: Mapped[str] = mapped_column(String(1000), nullable=False)
    base_sha: Mapped[str] = mapped_column(String(40), nullable=False)
    head_sha: Mapped[str] = mapped_column(String(40), nullable=False)
    head_tree_sha: Mapped[str] = mapped_column(String(40), nullable=False)
    patch_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_ref: Mapped[dict] = mapped_column(JSON, nullable=False)
    subject_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    prompt_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class CodeReviewResult(Base):
    """Immutable normalized output from one exact reviewer Task generation."""

    __tablename__ = "code_review_results"
    __table_args__ = (
        UniqueConstraint("run_id", name="uq_code_review_result_run"),
        UniqueConstraint(
            "capability_invocation_id",
            name="uq_code_review_result_invocation",
        ),
        UniqueConstraint(
            "capability_execution_id",
            name="uq_code_review_result_execution",
        ),
        CheckConstraint(
            f"verdict IN ({_sql_values(CODE_REVIEW_VERDICTS)})",
            name="ck_code_review_result_verdict",
        ),
        CheckConstraint(
            "schema_version = 1",
            name="ck_code_review_result_schema_version",
        ),
        CheckConstraint(
            "reviewer_task_retry_count >= 0",
            name="ck_code_review_result_retry_count",
        ),
        Index(
            "ix_code_review_result_subject_created",
            "subject_hash",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("code_review_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    capability_invocation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("capability_invocations.id", ondelete="CASCADE"),
        nullable=False,
    )
    capability_execution_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("capability_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    developer_task_id: Mapped[int] = mapped_column(Integer, nullable=False)
    reviewer_task_id: Mapped[int] = mapped_column(Integer, nullable=False)
    reviewer_task_retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    reviewer_task_instance_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    reviewer_task_started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False
    )
    reviewer_task_completed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False
    )
    output_log_id: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    verdict: Mapped[str] = mapped_column(String(24), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    findings: Mapped[list] = mapped_column(JSON, nullable=False)
    subject_ref: Mapped[dict] = mapped_column(JSON, nullable=False)
    subject_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    result_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
