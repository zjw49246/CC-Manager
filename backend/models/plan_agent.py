"""Durable audit records for independent Plan Task pipelines."""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


def _worker_plan_dispatch_digest_sql(column: str) -> str:
    """Portable lowercase-hex validation for SQLite/PostgreSQL/MySQL."""

    stripped = column
    for character in "0123456789abcdef":
        stripped = f"replace({stripped}, '{character}', '')"
    return f"(length({column}) = 64 AND {stripped} = '')"


_WORKER_PLAN_DISPATCH_DIGEST_SQL = _worker_plan_dispatch_digest_sql(
    "payload_digest"
)


_WORKER_PLAN_IMPORT_DIGEST_SQL = _worker_plan_dispatch_digest_sql(
    "payload_digest"
)


def _lower_hex_sql(column: str, length: int) -> str:
    """Portable exact lowercase-hex validation."""

    stripped = column
    for character in "0123456789abcdef":
        stripped = f"replace({stripped}, '{character}', '')"
    return f"(length({column}) = {length} AND {stripped} = '')"


def _boot_id_sql(column: str) -> str:
    """Validate the canonical lowercase UUID emitted by Linux boot_id."""

    stripped = column
    for character in "0123456789abcdef":
        stripped = f"replace({stripped}, '{character}', '')"
    return (
        f"(length({column}) = 36 AND substr({column}, 9, 1) = '-' AND "
        f"substr({column}, 14, 1) = '-' AND substr({column}, 19, 1) = '-' AND "
        f"substr({column}, 24, 1) = '-' AND {stripped} = '----')"
    )


_PLAN_RUNTIME_TOKEN_SQL = _lower_hex_sql("runtime_token", 32)
_PLAN_RUNTIME_PREPARED_BOOT_SQL = _boot_id_sql("prepared_boot_id")
_PLAN_RUNTIME_PROCESS_BOOT_SQL = _boot_id_sql("boot_id")
_PLAN_RUNTIME_PROCESS_EMPTY_SQL = (
    "process_id IS NULL AND process_group_id IS NULL AND "
    "process_start_ticks IS NULL AND process_uid IS NULL AND boot_id IS NULL"
)
_PLAN_RUNTIME_PROCESS_COMPLETE_SQL = (
    "process_id IS NOT NULL AND process_group_id IS NOT NULL AND "
    "process_start_ticks IS NOT NULL AND process_uid IS NOT NULL AND "
    "boot_id IS NOT NULL AND process_id > 1 AND process_group_id > 1 AND "
    "process_start_ticks >= 0 AND process_uid >= 0 AND "
    "process_uid = prepared_uid AND boot_id = prepared_boot_id "
    f"AND {_PLAN_RUNTIME_PROCESS_BOOT_SQL}"
)
_PLAN_RUNTIME_PROCESS_SHAPE_SQL = (
    f"(({_PLAN_RUNTIME_PROCESS_EMPTY_SQL}) OR "
    f"({_PLAN_RUNTIME_PROCESS_COMPLETE_SQL}))"
)
_PLAN_RUNTIME_CODEX_EMPTY_SQL = (
    "codex_home IS NULL AND codex_thread_id IS NULL"
)
_PLAN_RUNTIME_CODEX_COMPLETE_SQL = (
    "codex_home IS NOT NULL AND length(trim(codex_home)) > 0 AND "
    "codex_thread_id IS NOT NULL AND length(trim(codex_thread_id)) > 0"
)
_PLAN_RUNTIME_PROVIDER_IDENTITY_SQL = (
    f"((provider = 'claude' AND ({_PLAN_RUNTIME_CODEX_EMPTY_SQL})) OR "
    f"(provider = 'codex' AND (({_PLAN_RUNTIME_CODEX_EMPTY_SQL}) OR "
    f"({_PLAN_RUNTIME_CODEX_COMPLETE_SQL})) AND "
    f"(({_PLAN_RUNTIME_PROCESS_EMPTY_SQL}) OR "
    f"({_PLAN_RUNTIME_CODEX_COMPLETE_SQL}))))"
)
_PLAN_RUNTIME_STATE_SHAPE_SQL = (
    "((status IN ('prepared', 'admitting') AND "
    f"({_PLAN_RUNTIME_PROCESS_EMPTY_SQL}) AND "
    f"({_PLAN_RUNTIME_CODEX_EMPTY_SQL}) AND cleanup_error IS NULL AND "
    "cleaned_at IS NULL) OR "
    "(status = 'launching' AND cleanup_error IS NULL AND cleaned_at IS NULL AND "
    f"((provider = 'claude' AND ({_PLAN_RUNTIME_PROCESS_COMPLETE_SQL}) AND "
    f"({_PLAN_RUNTIME_CODEX_EMPTY_SQL})) OR "
    f"(provider = 'codex' AND ({_PLAN_RUNTIME_CODEX_COMPLETE_SQL}) AND "
    f"({_PLAN_RUNTIME_PROCESS_SHAPE_SQL})))) OR "
    "(status = 'cleaned' AND cleaned_at IS NOT NULL AND cleanup_error IS NULL AND "
    f"({_PLAN_RUNTIME_PROVIDER_IDENTITY_SQL})) OR "
    "(status = 'cleanup_failed' AND cleaned_at IS NULL AND "
    "cleanup_error IS NOT NULL AND length(trim(cleanup_error)) > 0 AND "
    f"({_PLAN_RUNTIME_PROVIDER_IDENTITY_SQL})))"
)


class PlanAgentRun(Base):
    __tablename__ = "plan_agent_runs"
    __table_args__ = (
        UniqueConstraint(
            "capability_execution_id",
            name="uq_plan_agent_runs_capability_execution",
        ),
        CheckConstraint(
            "(cancellation_target_generation IS NULL AND status != 'cancelling') "
            "OR (cancellation_target_generation IS NOT NULL AND "
            "cancellation_target_generation >= 0 AND "
            "generation = cancellation_target_generation + 1 AND "
            "status IN ('cancelling', 'cancelled'))",
            name="ck_plan_agent_run_cancellation_generation",
        ),
        CheckConstraint(
            "(relay_origin IS NOT NULL AND relay_origin = 'manager_v1' AND "
            "import_receipt_protocol IS NOT NULL AND "
            "import_receipt_protocol = 1) OR "
            "((relay_origin IS NULL OR relay_origin <> 'manager_v1') AND "
            "import_receipt_protocol IS NULL)",
            name="ck_plan_agent_run_import_receipt_protocol",
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    # Nullable legacy mapping during cutover. New runs are owned by ``plan_id``.
    plan_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    plan_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # Exact reverse owner for Capability-backed Runs.  Nullable keeps legacy,
    # ordinary and Worker-imported Plan Runs compatible; uniqueness prevents
    # one execution from acquiring two planner pipelines.
    capability_execution_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    run_type: Mapped[str] = mapped_column(String(30), nullable=False, default="legacy")
    source_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    base_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Planner proposals remain mutable Run-scoped candidates until the
    # Planner/Reviewer pipeline reaches a terminal review outcome. Only then
    # is the final candidate materialized as an immutable PlanVersion.
    draft_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    draft_step_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    draft_repo_revision: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    request_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    attachments: Mapped[list | None] = mapped_column(JSON, nullable=True)
    context_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    context_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    context_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    repo_revision: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    current_stage: Mapped[str] = mapped_column(String(30), nullable=False, default="planner")
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Local Capability and Manager-side Worker cancellation fence the exact
    # claimed generation before asking the runtime owner to stop. Keeping that
    # generation durable avoids inferring it from a mutable counter during
    # crash recovery or deletion preflight.
    cancellation_target_generation: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    relay_origin: Mapped[str | None] = mapped_column(String(30), nullable=True)
    # Stable identity of a Manager -> Worker protocol import. Manager claim
    # generations are deliberately excluded: the Worker Run owns its own
    # local generation after the first import.
    import_payload_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Permanent database-level old-writer gate for Manager -> Worker imports.
    # Pre-receipt importers omit this field and therefore fail the CHECK above;
    # the current importer first inserts the permanent receipt, then writes 1.
    import_receipt_protocol: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    import_attachment_receipt: Mapped[list | None] = mapped_column(JSON, nullable=True)
    open_input_request_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    interaction_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_interactions: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    execution_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    last_execution_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="planning", index=True
    )
    combo_used: Mapped[str | None] = mapped_column(String(20), nullable=True)
    planner_provider: Mapped[str | None] = mapped_column(String(20), nullable=True)
    planner_model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    planner_effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    reviewer_provider: Mapped[str | None] = mapped_column(String(20), nullable=True)
    reviewer_model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    reviewer_effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    pipeline_config: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    round: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    review_verdict: Mapped[str | None] = mapped_column(String(20), nullable=True)
    review_feedback: Mapped[str | None] = mapped_column(Text, nullable=True)
    review_exhausted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PlanAgentStep(Base):
    __tablename__ = "plan_agent_steps"
    __table_args__ = (
        UniqueConstraint(
            "worker_id", "worker_step_id", name="uq_plan_steps_worker_id"
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    plan_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    worker_step_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plan_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_request_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    step_type: Mapped[str] = mapped_column(String(20), nullable=False)
    round: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    provider: Mapped[str] = mapped_column(String(20), nullable=False)
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    route_slot: Mapped[str | None] = mapped_column(String(20), nullable=True)
    account_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="running"
    )
    output: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_delta_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    streamed_output_chars: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    last_event_type: Mapped[str | None] = mapped_column(
        String(100), nullable=True
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PlanAgentRuntimeReceipt(Base):
    """Durable identity and cleanup proof for one Plan provider attempt.

    A Step can retry an account or route, so runtime ownership is attempt-
    scoped rather than stored directly on ``PlanAgentStep``.  ``prepared`` is
    committed before provider launch; ``launching`` means an exact native
    identity has been committed before model input; only ``cleaned`` permits
    a cancelling Run to release its Instance owner.
    """

    __tablename__ = "plan_agent_runtime_receipts"
    __table_args__ = (
        UniqueConstraint(
            "step_id",
            "attempt_index",
            name="uq_plan_runtime_receipt_step_attempt",
        ),
        UniqueConstraint(
            "runtime_token",
            name="uq_plan_runtime_receipt_token",
        ),
        CheckConstraint(
            "status IN ('prepared', 'admitting', 'launching', 'cleaned', "
            "'cleanup_failed')",
            name="ck_plan_runtime_receipt_status",
        ),
        CheckConstraint(
            "run_id > 0 AND step_id > 0 AND run_generation >= 0 AND "
            "attempt_index >= 1 AND provider IN ('claude', 'codex') AND "
            f"{_PLAN_RUNTIME_TOKEN_SQL} AND "
            f"{_PLAN_RUNTIME_PREPARED_BOOT_SQL} AND "
            "prepared_start_ticks >= 0 AND prepared_uid >= 0",
            name="ck_plan_runtime_receipt_scalar_shape",
        ),
        CheckConstraint(
            _PLAN_RUNTIME_PROCESS_SHAPE_SQL,
            name="ck_plan_runtime_receipt_process_identity",
        ),
        CheckConstraint(
            _PLAN_RUNTIME_STATE_SHAPE_SQL,
            name="ck_plan_runtime_receipt_state_shape",
        ),
        Index(
            "ix_plan_runtime_receipt_run_status",
            "run_id",
            "status",
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    step_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    run_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    attempt_index: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(20), nullable=False)
    runtime_token: Mapped[str] = mapped_column(String(32), nullable=False)
    # The receipt is committed before provider admission.  This Linux boot/
    # tick boundary lets recovery ignore older same-UID processes whose
    # environments are intentionally unreadable (for example systemd --user)
    # without guessing whether a newly spawned child inherited our token.
    prepared_boot_id: Mapped[str] = mapped_column(String(64), nullable=False)
    prepared_start_ticks: Mapped[int] = mapped_column(BigInteger, nullable=False)
    prepared_uid: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="prepared"
    )

    # Claude subprocess identity.  The random token is also injected into the
    # child environment, so restart cleanup never signals a reused numeric
    # PID/PGID without finding a matching live process first.
    process_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    process_group_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    process_start_ticks: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    process_uid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    boot_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Codex auxiliary turns are owned by a disposable native thread under one
    # canonical CODEX_HOME.  The app-server callback commits this pair before
    # turn/start can send model input.
    codex_home: Mapped[str | None] = mapped_column(Text, nullable=True)
    codex_thread_id: Mapped[str | None] = mapped_column(String(200), nullable=True)

    cleanup_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    cleaned_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PlanAgentWorkerDispatchReceipt(Base):
    """Durable Manager -> Worker boundary for one Plan Run generation.

    ``prepared`` is committed atomically with the Manager claim and proves
    that no Worker Plan import has been attempted yet. ``remote_possible`` is
    committed immediately before the mutating import request and carries the
    immutable payload digest needed for read-only restart reconciliation.
    Only ``settled`` is terminal/deletable.
    """

    __tablename__ = "plan_agent_worker_dispatch_receipts"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "run_generation",
            name="uq_plan_worker_dispatch_run_generation",
        ),
        CheckConstraint(
            "status IN ('prepared', 'remote_possible', 'settled')",
            name="ck_plan_worker_dispatch_status",
        ),
        CheckConstraint(
            "protocol = 1",
            name="ck_plan_worker_dispatch_protocol",
        ),
        CheckConstraint(
            "(status = 'prepared' AND payload_digest IS NULL AND "
            "remote_status IS NULL AND settlement_reason IS NULL AND "
            "settled_at IS NULL) OR "
            f"(status = 'remote_possible' AND "
            f"{_WORKER_PLAN_DISPATCH_DIGEST_SQL} AND "
            "remote_status IS NULL AND settlement_reason IS NULL AND "
            "settled_at IS NULL) OR "
            "(status = 'settled' AND settled_at IS NOT NULL AND ("
            "(settlement_reason IN ('not_launched', 'preflight_failed') AND "
            "payload_digest IS NULL AND remote_status IS NULL) OR "
            "(settlement_reason = 'remote_cancelled' AND "
            f"(payload_digest IS NULL OR "
            f"{_WORKER_PLAN_DISPATCH_DIGEST_SQL}) AND "
            "remote_status = 'cancelled') OR "
            "(settlement_reason = 'remote_pause' AND "
            f"{_WORKER_PLAN_DISPATCH_DIGEST_SQL} AND remote_status IN "
            "('waiting_user', 'completed', 'failed', 'cancelled')) OR "
            "(settlement_reason = 'remote_absent' AND "
            f"{_WORKER_PLAN_DISPATCH_DIGEST_SQL} AND remote_status IS NULL) OR "
            "(settlement_reason = 'identity_conflict' AND "
            f"{_WORKER_PLAN_DISPATCH_DIGEST_SQL} AND "
            "remote_status = 'conflict') OR "
            "(settlement_reason = 'legacy_terminal' AND "
            "payload_digest IS NULL AND remote_status IN "
            "('completed', 'failed', 'cancelled')))) ",
            name="ck_plan_worker_dispatch_state_shape",
        ),
        Index(
            "ix_plan_worker_dispatch_worker_status",
            "worker_id",
            "status",
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    plan_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    # Standalone Plans intentionally have no target Task.  Keeping the NULL in
    # the receipt is still part of the exact frozen identity.
    target_task_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )
    worker_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    run_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    protocol: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="prepared"
    )
    payload_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    remote_status: Mapped[str | None] = mapped_column(String(30), nullable=True)
    settlement_reason: Mapped[str | None] = mapped_column(
        String(50), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    settled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PlanAgentWorkerImportReceipt(Base):
    """Permanent Worker-side identity fence for one Manager Plan import.

    The row is deliberately not owned by the mutable Plan graph.  Keeping it
    after Run/Plan deletion prevents a delayed or replayed import request from
    resurrecting an already-cancelled remote runtime.  ``imported`` records
    that the exact Run was admitted atomically; ``cancelled_before_import`` is
    the durable tombstone won by an exact cancellation that arrived first.
    """

    __tablename__ = "plan_agent_worker_import_receipts"
    __table_args__ = (
        CheckConstraint(
            "run_id > 0 AND plan_id > 0",
            name="ck_plan_worker_import_receipt_identity",
        ),
        CheckConstraint(
            "protocol = 1",
            name="ck_plan_worker_import_receipt_protocol",
        ),
        CheckConstraint(
            "relay_origin = 'manager_v1'",
            name="ck_plan_worker_import_receipt_origin",
        ),
        CheckConstraint(
            "outcome IN ('imported', 'cancelled_before_import')",
            name="ck_plan_worker_import_receipt_outcome",
        ),
        CheckConstraint(
            _WORKER_PLAN_IMPORT_DIGEST_SQL,
            name="ck_plan_worker_import_receipt_digest",
        ),
        Index(
            "ix_plan_worker_import_receipt_plan",
            "plan_id",
        ),
        # The migration's crash-replay state machine relies on transactional
        # DDL/data semantics.  Keep metadata.create_all() equivalent even
        # when a MySQL server's default storage engine is not InnoDB.
        {"mysql_engine": "InnoDB"},
    )

    # The Manager Run id is the collision domain on a Worker.  Making it the
    # primary key lets import and cancellation race on one portable database
    # uniqueness fence without relying on process-local locks.
    run_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=False
    )
    plan_id: Mapped[int] = mapped_column(Integer, nullable=False)
    protocol: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    relay_origin: Mapped[str] = mapped_column(
        String(30), nullable=False, default="manager_v1"
    )
    payload_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    outcome: Mapped[str] = mapped_column(String(30), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
