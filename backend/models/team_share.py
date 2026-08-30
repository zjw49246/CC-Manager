"""Team CCM sharing: Project/Task permissions for local users."""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, func, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class TeamProjectShare(Base):
    """Grant a user or group access to a Project."""
    __tablename__ = "team_project_shares"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    target_type: Mapped[str] = mapped_column(String(20), nullable=False)  # 'user' | 'group'
    target_id: Mapped[int] = mapped_column(Integer, nullable=False)  # user.id or group.id
    shared_by: Mapped[int] = mapped_column(Integer, nullable=False)  # admin user.id
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("project_id", "target_type", "target_id", name="uq_team_project_share"),
    )


class TeamTaskShare(Base):
    """Grant chat access to one Task independently of Project membership."""
    __tablename__ = "team_task_shares"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    target_type: Mapped[str] = mapped_column(String(20), nullable=False)  # 'user' | 'group'
    target_id: Mapped[int] = mapped_column(Integer, nullable=False)  # user.id or group.id
    permission: Mapped[str] = mapped_column(String(20), nullable=False, default="chat")  # 'chat' = can only send messages
    shared_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("task_id", "target_type", "target_id", name="uq_team_task_share"),
    )
