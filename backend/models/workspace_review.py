from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class WorkspaceReviewRun(Base):
    """Durable owner record for one current-workspace browser verification."""

    __tablename__ = "workspace_review_runs"
    __table_args__ = {"mysql_engine": "InnoDB"}

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
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
    harness_run_id: Mapped[str | None] = mapped_column(String(32), nullable=True, unique=True)
    agent_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    browser_review_job_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="review_only")
    profile: Mapped[str] = mapped_column(String(20), nullable=False, default="standard")
    goal: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="queued", index=True)
    stage: Mapped[str] = mapped_column(String(48), nullable=False, default="queued")
    workspace_path: Mapped[str] = mapped_column(String(1000), nullable=False)
    git_head: Mapped[str] = mapped_column(String(64), nullable=False)
    workspace_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    preview_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    preview_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    report: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    cleanup_status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending")
    cleanup_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
