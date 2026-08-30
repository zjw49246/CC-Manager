from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class Discussion(Base):
    __tablename__ = "discussions"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'closing', 'closed')",
            name="ck_discussions_status",
        ),
        Index(
            "ix_discussions_project_status_id",
            "project_id",
            "status",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    max_agents: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    facilitator_model: Mapped[str] = mapped_column(String(100), nullable=False, default="claude-opus-4-6")
    agent_model: Mapped[str] = mapped_column(String(100), nullable=False, default="claude-opus-4-6")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    facilitator_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    creator_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class DiscussionMessage(Base):
    __tablename__ = "discussion_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    discussion_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(20), nullable=False)  # user / facilitator
    agent_role_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class DiscussionAgent(Base):
    __tablename__ = "discussion_agents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    discussion_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    role_name: Mapped[str] = mapped_column(String(100), nullable=False)
    system_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    last_cwd: Mapped[str | None] = mapped_column(String(500), nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="idle")  # idle / running / error
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class DiscussionEvent(Base):
    __tablename__ = "discussion_events"
    __table_args__ = (
        Index("ix_discussion_events_agent_id_id", "agent_id", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    discussion_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    agent_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    role: Mapped[str | None] = mapped_column(String(20), nullable=True)
    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    tool_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    tool_input: Mapped[str | None] = mapped_column(Text, nullable=True)
    tool_output: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_error: Mapped[bool] = mapped_column(Boolean, default=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
