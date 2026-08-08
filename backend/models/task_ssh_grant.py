from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class TaskSSHGrant(Base):
    """Explicit Task capability snapshot for one managed SSH profile."""

    __tablename__ = "task_ssh_grants"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "ssh_profile_id",
            name="uq_task_ssh_grants_task_profile",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ssh_profile_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ssh_profiles.id"),
        nullable=False,
        index=True,
    )
    profile_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    capabilities: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
