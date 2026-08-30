from datetime import datetime

from sqlalchemy import CheckConstraint, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class Worktree(Base):
    __tablename__ = "worktrees"
    __table_args__ = (
        UniqueConstraint("delivery_run_id", name="uq_worktrees_delivery_run"),
        CheckConstraint(
            "cleanup_status IN ('retained', 'cleaning', 'removed', 'error')",
            name="ck_worktrees_cleanup_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_path: Mapped[str] = mapped_column(String(500), nullable=False)
    worktree_path: Mapped[str] = mapped_column(String(500), nullable=False, unique=True)
    branch_name: Mapped[str] = mapped_column(String(100), nullable=False)
    base_branch: Mapped[str] = mapped_column(String(100), default="main")
    instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # DeliveryRun is deliberately an application-owned integer reference: a
    # Worker may hold the workspace record without mirroring Manager tables.
    delivery_run_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )
    last_verified_head: Mapped[str | None] = mapped_column(String(64), nullable=True)
    cleanup_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="retained", server_default="retained"
    )
    status: Mapped[str] = mapped_column(String(20), default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    removed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
