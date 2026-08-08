from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class SSHProfile(Base):
    """Manager-owned SSH connection whose private key never leaves the host."""

    __tablename__ = "ssh_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    host: Mapped[str] = mapped_column(String(253), nullable=False)
    port: Mapped[int] = mapped_column(Integer, nullable=False, default=22, server_default="22")
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    key_path: Mapped[str] = mapped_column(String(1000), nullable=False)
    public_key_fingerprint: Mapped[str] = mapped_column(String(100), nullable=False)
    host_key_type: Mapped[str] = mapped_column(String(64), nullable=False)
    host_key_value: Mapped[str] = mapped_column(Text, nullable=False)
    host_key_fingerprint: Mapped[str] = mapped_column(String(100), nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
    task_access_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="0",
    )
    task_capabilities: Mapped[list[str]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        server_default="[]",
    )
    allowed_roots: Mapped[list[str]] = mapped_column(
        JSON,
        nullable=False,
        default=lambda: ["/"],
        server_default='["/"]',
    )
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    last_tested_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_test_ok: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error_detail: Mapped[str | None] = mapped_column(String(500), nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow,
    )

    @property
    def key_path_hint(self) -> str:
        from pathlib import Path

        return f"…/{Path(self.key_path).name}"
