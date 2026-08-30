"""Task and project sharing models — tracks who shared what with whom."""

from datetime import datetime

from sqlalchemy import Integer, String, Text, DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class TaskShare(Base):
    __tablename__ = "task_shares"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("tasks.id", ondelete="CASCADE"), index=True
    )
    shared_to_open_id: Mapped[str] = mapped_column(String(100))
    shared_to_name: Mapped[str | None] = mapped_column(String(100))
    shared_to_ccm_url: Mapped[str] = mapped_column(String(500))
    share_token: Mapped[str] = mapped_column(String(200), unique=True)
    status: Mapped[str] = mapped_column(String(20), default="active")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("task_id", "shared_to_open_id", name="uq_task_share_recipient"),
    )


class ProjectShare(Base):
    __tablename__ = "project_shares"

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("projects.id", ondelete="CASCADE"), index=True
    )
    shared_to_open_id: Mapped[str] = mapped_column(String(100))
    shared_to_name: Mapped[str | None] = mapped_column(String(100))
    shared_to_ccm_url: Mapped[str] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(String(20), default="active")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("project_id", "shared_to_open_id", name="uq_project_share_recipient"),
    )


class SharedTaskReceived(Base):
    __tablename__ = "shared_tasks_received"

    id: Mapped[int] = mapped_column(primary_key=True)
    owner_ccm_url: Mapped[str] = mapped_column(String(500))
    owner_name: Mapped[str | None] = mapped_column(String(100))
    owner_feishu_open_id: Mapped[str | None] = mapped_column(String(100))
    remote_task_id: Mapped[int] = mapped_column(Integer)
    share_token: Mapped[str] = mapped_column(String(200))
    task_title: Mapped[str | None] = mapped_column(String(200))
    task_description: Mapped[str | None] = mapped_column(Text)
    project_name: Mapped[str | None] = mapped_column(String(100))
    local_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="active")
    received_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("owner_ccm_url", "remote_task_id", name="uq_shared_received_owner_task"),
        # Received-share ids escape into shadow Tasks and HTTP routes.  SQLite
        # must never recycle one after leave/revoke, or an old shadow could
        # resolve a different remote share through the reused integer id.
        {"sqlite_autoincrement": True},
    )
