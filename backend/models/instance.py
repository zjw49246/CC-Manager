from datetime import datetime

from sqlalchemy import CheckConstraint, Integer, String, Float, DateTime, JSON
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class Instance(Base):
    __tablename__ = "instances"
    __table_args__ = (
        CheckConstraint(
            "NOT (current_task_id IS NOT NULL AND current_plan_run_id IS NOT NULL)",
            name="ck_instances_task_xor_plan_run_owner",
        ),
        CheckConstraint(
            "process_identity IS NULL OR pid IS NOT NULL",
            name="ck_instances_process_identity_requires_pid",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Opaque kernel identity ("v1:<pid>:<start_ticks>:<boot_id>") for the
    # process in `pid`. A PID number alone cannot distinguish "this exact
    # process is still alive" from "an unrelated process reused this number",
    # so recovery probes compare the start time and boot session too.
    # The PID is embedded in the value, so a writer that sets `pid` without
    # refreshing this column produces a mismatch that reads as "unusable"
    # rather than as proof of death. NULL means the row predates identity
    # capture or the platform could not supply it; both stay fail-closed.
    # Always write and clear this together with `pid`.
    process_identity: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )
    status: Mapped[str] = mapped_column(String(20), default="idle")
    current_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_plan_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    worktree_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    worktree_branch: Mapped[str | None] = mapped_column(String(100), nullable=True)
    provider: Mapped[str] = mapped_column(String(20), default="claude", server_default="claude")
    model: Mapped[str] = mapped_column(String(50), default="default")
    effort_level: Mapped[str | None] = mapped_column(String(20), nullable=True)
    # Optional Extended Thinking budget (max tokens). Forwarded to Claude Code
    # subprocess via MAX_THINKING_TOKENS env var. NULL = use CLI default.
    thinking_budget: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tasks_completed: Mapped[int] = mapped_column(Integer, default=0)
    total_cost_usd: Mapped[float] = mapped_column(Float, default=0.0)
    config: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_heartbeat: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
