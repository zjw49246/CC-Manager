"""Durable Manager/Worker receipts for exact Task termination operations."""

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
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


WORKER_TASK_TERMINATION_RECEIPT_SIDES = ("manager", "worker")
WORKER_TASK_TERMINATION_OPERATIONS = (
    "cancel",
    "stop_session",
    "supersede",
    # Manager-only durable owner for remote-first Task+Plan deletion.  No
    # Worker termination receipt is created for this operation: the Worker
    # exposes a separate idempotent DELETE plus read-only cascade audit, while
    # this row quarantines the Manager mirror until local graph finalization.
    "delete",
)
WORKER_TASK_TERMINATION_SOURCE_STATUSES = (
    "pending",
    "in_progress",
    "executing",
    "plan_review",
    "merging",
    "migrating",
    "completed",
    "failed",
    "cancelled",
    "conflict",
    "superseded",
)

MANAGER_TASK_TERMINATION_STATUSES = (
    "pending_remote",
    "awaiting_ack",
    "settled",
    "rejected",
    "conflict",
)
WORKER_TASK_TERMINATION_STATUSES = (
    "accepted",
    "executing",
    "succeeded",
    "acknowledged",
    "rejected",
    "conflict",
)
WORKER_TASK_TERMINATION_STATUSES_ALL = tuple(
    dict.fromkeys(
        MANAGER_TASK_TERMINATION_STATUSES + WORKER_TASK_TERMINATION_STATUSES
    )
)

# ``conflict`` deliberately owns the slot.  An exact-generation mismatch is a
# quarantine, not a terminal release: an operator or future reconciler must
# establish that the remote side cannot still terminate the observed Task
# generation before a second operation may be admitted.
MANAGER_ACTIVE_TASK_TERMINATION_STATUSES = (
    "pending_remote",
    "awaiting_ack",
    "conflict",
)
WORKER_ACTIVE_TASK_TERMINATION_STATUSES = (
    "accepted",
    "executing",
    "succeeded",
    "rejected",
    "conflict",
)


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


_MANAGER_OUTCOME_STATUSES = ("awaiting_ack", "settled", "rejected")
_WORKER_OUTCOME_STATUSES = ("succeeded", "acknowledged", "rejected")
_OUTCOME_STATUSES = tuple(
    dict.fromkeys(_MANAGER_OUTCOME_STATUSES + _WORKER_OUTCOME_STATUSES)
)
_PRE_OUTCOME_STATUSES = ("pending_remote", "accepted", "executing")


class WorkerTaskTerminationReceipt(Base):
    """One side's durable evidence for one exact remote termination request.

    Manager and Worker keep separate databases.  A globally random
    ``operation_id`` therefore identifies one Manager row and one Worker row,
    rather than two rows in a shared database.  The request payload is frozen
    canonically by the runtime and its digest binds both copies to the same
    exact Task generation.
    """

    __tablename__ = "worker_task_termination_receipts"
    __table_args__ = (
        UniqueConstraint(
            "active_task_id",
            name="uq_worker_task_term_active_task",
        ),
        CheckConstraint(
            "LENGTH(operation_id) = 32",
            name="ck_worker_task_term_operation_id",
        ),
        CheckConstraint(
            f"side IN ({_sql_values(WORKER_TASK_TERMINATION_RECEIPT_SIDES)})",
            name="ck_worker_task_term_side",
        ),
        CheckConstraint(
            f"operation IN ({_sql_values(WORKER_TASK_TERMINATION_OPERATIONS)})",
            name="ck_worker_task_term_operation",
        ),
        CheckConstraint(
            "operation <> 'delete' OR side = 'manager'",
            name="ck_worker_task_term_delete_manager_only",
        ),
        CheckConstraint(
            f"status IN ({_sql_values(WORKER_TASK_TERMINATION_STATUSES_ALL)})",
            name="ck_worker_task_term_status",
        ),
        CheckConstraint(
            "(side = 'manager' AND worker_id IS NOT NULL AND worker_id > 0 "
            f"AND status IN ({_sql_values(MANAGER_TASK_TERMINATION_STATUSES)})) "
            "OR (side = 'worker' AND worker_id IS NULL "
            f"AND status IN ({_sql_values(WORKER_TASK_TERMINATION_STATUSES)}))",
            name="ck_worker_task_term_side_shape",
        ),
        CheckConstraint(
            "((side = 'manager' AND status IN ("
            f"{_sql_values(MANAGER_ACTIVE_TASK_TERMINATION_STATUSES)}"
            ")) OR (side = 'worker' AND status IN ("
            f"{_sql_values(WORKER_ACTIVE_TASK_TERMINATION_STATUSES)}"
            "))) AND active_task_id IS NOT NULL "
            "AND active_task_id = task_id OR "
            "((side = 'manager' AND status NOT IN ("
            f"{_sql_values(MANAGER_ACTIVE_TASK_TERMINATION_STATUSES)}"
            ")) OR (side = 'worker' AND status NOT IN ("
            f"{_sql_values(WORKER_ACTIVE_TASK_TERMINATION_STATUSES)}"
            "))) AND active_task_id IS NULL",
            name="ck_worker_task_term_active_slot",
        ),
        CheckConstraint(
            "source_task_retry_count >= 0 "
            "AND source_task_turn_generation >= 0 "
            "AND (source_task_source_log_id IS NULL "
            "OR source_task_source_log_id > 0) "
            "AND (source_task_instance_id IS NULL "
            "OR source_task_instance_id > 0)",
            name="ck_worker_task_term_source_generation",
        ),
        CheckConstraint(
            "source_task_status IN ("
            f"{_sql_values(WORKER_TASK_TERMINATION_SOURCE_STATUSES)}"
            ")",
            name="ck_worker_task_term_source_status",
        ),
        CheckConstraint(
            "(source_worker_turn_handoff_id IS NULL "
            "AND source_worker_turn_handoff_worker_id IS NULL "
            "AND source_worker_turn_handoff_retry_count IS NULL "
            "AND source_worker_turn_handoff_from_generation IS NULL "
            "AND source_worker_turn_handoff_source_log_id IS NULL "
            "AND source_worker_turn_handoff_acknowledged IS NULL) OR "
            "(source_worker_turn_handoff_id IS NOT NULL "
            "AND LENGTH(source_worker_turn_handoff_id) = 32 "
            "AND source_worker_turn_handoff_worker_id IS NOT NULL "
            "AND source_worker_turn_handoff_worker_id > 0 "
            "AND source_worker_turn_handoff_retry_count IS NOT NULL "
            "AND source_worker_turn_handoff_retry_count >= 0 "
            "AND source_worker_turn_handoff_from_generation IS NOT NULL "
            "AND source_worker_turn_handoff_from_generation >= 0 "
            "AND source_worker_turn_handoff_source_log_id IS NOT NULL "
            "AND source_worker_turn_handoff_source_log_id > 0 "
            "AND source_worker_turn_handoff_acknowledged IS NOT NULL "
            "AND source_worker_turn_handoff_acknowledged IN (TRUE, FALSE))",
            name="ck_worker_task_term_handoff_shape",
        ),
        CheckConstraint(
            "LENGTH(request_digest) = 64",
            name="ck_worker_task_term_request_digest",
        ),
        CheckConstraint(
            "(result_payload IS NULL AND result_digest IS NULL) OR "
            "(result_payload IS NOT NULL AND result_digest IS NOT NULL "
            "AND LENGTH(result_digest) = 64)",
            name="ck_worker_task_term_result_pair",
        ),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(_OUTCOME_STATUSES)}"
            ") AND result_payload IS NOT NULL) OR "
            "(status IN ("
            f"{_sql_values(_PRE_OUTCOME_STATUSES)}"
            ") AND result_payload IS NULL) OR status = 'conflict'",
            name="ck_worker_task_term_result_status",
        ),
        CheckConstraint(
            "state_version >= 1 AND attempt_count >= 0 "
            "AND reconcile_count >= 0",
            name="ck_worker_task_term_counters",
        ),
        CheckConstraint(
            "(side = 'manager' AND execution_token IS NULL) OR "
            "(side = 'worker' AND ((status = 'executing' "
            "AND execution_token IS NOT NULL "
            "AND LENGTH(execution_token) = 32 "
            "AND next_reconcile_at IS NOT NULL) OR "
            "(status <> 'executing' AND execution_token IS NULL)))",
            name="ck_worker_task_term_execution_owner",
        ),
        CheckConstraint(
            "(status IN ('awaiting_ack', 'settled', 'accepted', 'executing', "
            "'succeeded', 'acknowledged', 'rejected') "
            "AND accepted_at IS NOT NULL) OR "
            "(status = 'pending_remote' AND accepted_at IS NULL) "
            "OR status = 'conflict'",
            name="ck_worker_task_term_accepted_at",
        ),
        CheckConstraint(
            "(status IN ('awaiting_ack', 'settled', 'succeeded', "
            "'acknowledged', 'rejected') AND completed_at IS NOT NULL) OR "
            "(status IN ('pending_remote', 'accepted', 'executing') "
            "AND completed_at IS NULL) OR status = 'conflict'",
            name="ck_worker_task_term_completed_at",
        ),
        CheckConstraint(
            "(((status IN ('settled', 'acknowledged') "
            "OR (side = 'manager' AND status = 'rejected')) "
            "AND acknowledged_at IS NOT NULL) OR ("
            "(status NOT IN ('settled', 'acknowledged') "
            "AND NOT (side = 'manager' AND status = 'rejected') "
            ") AND acknowledged_at IS NULL))",
            name="ck_worker_task_term_acknowledged_at",
        ),
        CheckConstraint(
            "(side = 'worker' AND ack_intent_at IS NULL) OR "
            "(side = 'manager' AND ("
            "(status = 'pending_remote' AND ack_intent_at IS NULL) OR "
            "status IN ('awaiting_ack', 'conflict') OR "
            "(status IN ('settled', 'rejected') "
            "AND ack_intent_at IS NOT NULL)))",
            name="ck_worker_task_term_ack_intent",
        ),
        CheckConstraint(
            "(completed_at IS NULL OR accepted_at IS NULL "
            "OR completed_at >= accepted_at) "
            "AND (ack_intent_at IS NULL OR "
            "(completed_at IS NOT NULL "
            "AND ack_intent_at >= completed_at)) "
            "AND (acknowledged_at IS NULL OR "
            "(completed_at IS NOT NULL "
            "AND acknowledged_at >= completed_at "
            "AND (ack_intent_at IS NULL "
            "OR acknowledged_at >= ack_intent_at)))",
            name="ck_worker_task_term_timeline",
        ),
        Index(
            "ix_worker_task_term_task_created",
            "task_id",
            "created_at",
        ),
        Index(
            "ix_worker_task_term_due",
            "side",
            "status",
            "next_reconcile_at",
        ),
        Index(
            "ix_worker_task_term_worker_status",
            "worker_id",
            "status",
        ),
        # Alembic's crash-replay state machine requires atomic MySQL DDL and
        # enforced foreign keys.  Keep metadata.create_all() equivalent to
        # the migration even when a server's default engine is not InnoDB.
        {"mysql_engine": "InnoDB"},
    )

    operation_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    # Cross-dialect single-active-operation fence.  All supported databases
    # permit multiple NULLs in a UNIQUE column.
    active_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    # Manager routing identity.  A Worker-local row deliberately cannot point
    # into its unrelated local ``workers`` table.
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    operation: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    state_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default="1",
    )
    # Worker-local, renewable execution lease owner.  It never crosses the
    # Manager/Worker wire and is cleared by every terminal Worker state.
    execution_token: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )

    # Immutable exact Manager-side source generation.  Nullable source values
    # are meaningful evidence: a queued Task may have no Instance/session/log,
    # and legacy Tasks may not yet have an incarnation token.
    source_task_incarnation_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    source_task_status: Mapped[str] = mapped_column(String(20), nullable=False)
    source_task_retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    source_task_turn_generation: Mapped[int] = mapped_column(
        BigInteger, nullable=False
    )
    source_task_source_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    source_task_instance_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    source_task_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    source_task_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    source_task_session_id: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    source_task_pty_background_generation: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    source_worker_turn_handoff_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    source_worker_turn_handoff_worker_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    source_worker_turn_handoff_retry_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    source_worker_turn_handoff_from_generation: Mapped[int | None] = (
        mapped_column(BigInteger, nullable=True)
    )
    source_worker_turn_handoff_source_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    source_worker_turn_handoff_acknowledged: Mapped[bool | None] = mapped_column(
        Boolean, nullable=True
    )

    request_payload: Mapped[dict] = mapped_column(
        JSON(none_as_null=True), nullable=False
    )
    request_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    result_payload: Mapped[dict | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    result_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    reconcile_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    next_reconcile_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Manager-only durable proof that the exact digest-bound ACK is permitted
    # to cross the network.  It is committed after the result and before POST.
    ack_intent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
