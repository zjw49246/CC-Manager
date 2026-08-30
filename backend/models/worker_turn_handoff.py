"""Durable Manager/Worker receipts for one exact follow-up turn handoff."""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    false,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


HANDOFF_RECEIPT_SIDES = ("manager", "worker")
HANDOFF_RECEIPT_STATUSES = (
    "prepared",
    "acknowledged",
    "accepted",
    "claimed",
    "launching",
    "launched",
    "cancelled",
    "completed",
)


class WorkerTurnHandoffReceipt(Base):
    """One side's durable evidence for Manager turn G -> Worker turn G+1.

    Manager and Worker use separate databases, so the same globally-random
    ``handoff_id`` deliberately identifies one row in each database.  The
    ``side`` check keeps their different payload/state shapes fail-closed.
    """

    __tablename__ = "worker_turn_handoff_receipts"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "source_log_id",
            name="uq_worker_turn_handoff_task_source_log",
        ),
        CheckConstraint(
            "side IN ('manager', 'worker')",
            name="ck_worker_turn_handoff_side",
        ),
        CheckConstraint(
            "status IN ('prepared', 'acknowledged', 'accepted', "
            "'claimed', 'launching', 'launched', 'cancelled', 'completed')",
            name="ck_worker_turn_handoff_status",
        ),
        CheckConstraint(
            "retry_count >= 0 AND from_generation >= 0",
            name="ck_worker_turn_handoff_generation",
        ),
        CheckConstraint(
            "(side = 'manager' AND worker_id IS NOT NULL "
            "AND status IN ('prepared', 'acknowledged', 'cancelled', "
            "'completed') AND queue_payload IS NULL "
            "AND queue_payload_digest IS NULL) OR "
            "(side = 'worker' AND worker_id IS NULL "
            "AND status IN ('accepted', 'claimed', 'launching', 'launched', "
            "'cancelled') "
            "AND queue_payload IS NOT NULL "
            "AND queue_payload_digest IS NOT NULL "
            "AND response IS NOT NULL)",
            name="ck_worker_turn_handoff_side_shape",
        ),
        CheckConstraint(
            "(status IN ('claimed', 'launching', 'launched') "
            "AND claimed_turn_generation IS NOT NULL "
            "AND claimed_turn_generation "
            "= from_generation + 1) OR "
            "(status NOT IN ('claimed', 'launching', 'launched') "
            "AND claimed_turn_generation IS NULL)",
            name="ck_worker_turn_handoff_claim",
        ),
        Index(
            "ix_worker_turn_handoff_task_status",
            "task_id",
            "side",
            "status",
        ),
    )

    handoff_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_log_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("log_entries.id", ondelete="CASCADE"),
        nullable=False,
    )
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    # Present only on the Manager row.  A Worker-local Task intentionally has
    # ``worker_id = NULL`` and cannot satisfy a FK into its local workers table.
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    from_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    request_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    request_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    queue_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    queue_payload_digest: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    response: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    claimed_turn_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    terminal_pr_review_chat: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    cancel_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
