"""add durable terminal arbitration identity

Revision ID: 4b8d2f6a1c90
Revises: c3a7e9f1b2d4
Create Date: 2026-08-06
"""

from typing import Sequence, Union
import re

from alembic import op
import sqlalchemy as sa


revision: str = "4b8d2f6a1c90"
down_revision: Union[str, None] = "c3a7e9f1b2d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_OLD_AGENT_REQUEST_IDENTITY = (
    "source <> 'agent_request' OR ("
    "purpose = 'advisory' AND resume_policy = 'resume_task' "
    "AND requested_by_user_id IS NULL "
    "AND request_task_retry_count IS NOT NULL "
    "AND request_task_turn_generation IS NOT NULL "
    "AND request_source_log_id IS NOT NULL "
    "AND request_output_log_id IS NOT NULL)"
)

_NEW_AGENT_REQUEST_IDENTITY = (
    "source <> 'agent_request' OR ("
    "purpose = 'advisory' AND resume_policy = 'resume_task' "
    "AND requested_by_user_id IS NULL "
    "AND request_task_retry_count IS NOT NULL "
    "AND request_task_turn_generation IS NOT NULL "
    "AND request_source_log_id IS NOT NULL "
    "AND request_output_log_id IS NOT NULL "
    "AND request_reason IS NOT NULL "
    "AND request_protocol_version IS NOT NULL "
    "AND request_protocol_version >= 1 "
    "AND request_output_hash IS NOT NULL)"
)

_TURN_SCOPE_CHECK = (
    "turn_scope IS NULL OR turn_scope IN "
    "('source', 'foreground', 'autonomous', 'orphan')"
)

_ACTUAL_TRANSPORT_CHECK = (
    "actual_transport IS NULL OR (turn_scope IS NOT NULL "
    "AND turn_scope = 'source' "
    "AND actual_transport IN "
    "('claude_pty', 'claude_exec', 'codex_app_server', 'codex_exec'))"
)

_MYSQL_SHADOW_IDENTITY_CHECK = "ck_cap_inv_agent_request_identity_v2"
_MYSQL_DOWNGRADE_GATE = "ck_cap_inv_no_agent_request_downgrade"
_MYSQL_TASK_DOWNGRADE_GATE = "ck_tasks_no_terminal_arbitration_downgrade"
_MYSQL_LOG_DOWNGRADE_GATE = "ck_logs_no_terminal_arbitration_downgrade"
_MYSQL_NEW_COLUMNS = (
    "request_reason",
    "request_protocol_version",
    "request_output_hash",
)

_TASK_DOWNGRADE_GATE_CHECK = "turn_source_log_id IS NULL"
_LOG_DOWNGRADE_GATE_CHECK = (
    "turn_scope IS NULL AND actual_transport IS NULL"
)

_WORKER_TASK_TERMINATION_TABLE = "worker_task_termination_receipts"
_WORKER_TASK_TERMINATION_DOWNGRADE_GATE = (
    "ck_worker_task_term_no_downgrade_rows"
)
_POSTGRESQL_WORKER_TASK_TERMINATION_CHECK_PROBE = (
    "_ccm_worker_task_term_check_probe"
)
_MYSQL_WORKER_TASK_TERMINATION_CHECK_PROBE = (
    "_ccm_worker_task_term_mysql_check_probe"
)
_WORKER_TASK_TERMINATION_INDEXES = {
    "ix_worker_task_term_task_created": ("task_id", "created_at"),
    "ix_worker_task_term_due": ("side", "status", "next_reconcile_at"),
    "ix_worker_task_term_worker_status": ("worker_id", "status"),
}
_WORKER_TASK_TERMINATION_CHECKS = {
    "ck_worker_task_term_operation_id": "LENGTH(operation_id) = 32",
    "ck_worker_task_term_side": "side IN ('manager', 'worker')",
    "ck_worker_task_term_operation": (
        "operation IN ('cancel', 'stop_session', 'supersede')"
    ),
    "ck_worker_task_term_status": (
        "status IN ('pending_remote', 'awaiting_ack', 'settled', 'rejected', "
        "'conflict', 'accepted', 'executing', 'succeeded', 'acknowledged')"
    ),
    "ck_worker_task_term_side_shape": (
        "(side = 'manager' AND worker_id IS NOT NULL AND worker_id > 0 "
        "AND status IN ('pending_remote', 'awaiting_ack', 'settled', "
        "'rejected', 'conflict')) OR "
        "(side = 'worker' AND worker_id IS NULL "
        "AND status IN ('accepted', 'executing', 'succeeded', "
        "'acknowledged', 'rejected', 'conflict'))"
    ),
    "ck_worker_task_term_active_slot": (
        "(((side = 'manager' AND status IN ('pending_remote', "
        "'awaiting_ack', 'conflict')) OR (side = 'worker' AND status IN "
        "('accepted', 'executing', 'succeeded', 'rejected', 'conflict'))) "
        "AND active_task_id IS NOT NULL AND active_task_id = task_id) OR "
        "(((side = 'manager' AND status NOT IN ('pending_remote', "
        "'awaiting_ack', 'conflict')) OR (side = 'worker' AND status NOT IN "
        "('accepted', 'executing', 'succeeded', 'rejected', 'conflict'))) "
        "AND active_task_id IS NULL)"
    ),
    "ck_worker_task_term_source_generation": (
        "source_task_retry_count >= 0 "
        "AND source_task_turn_generation >= 0 "
        "AND (source_task_source_log_id IS NULL "
        "OR source_task_source_log_id > 0) "
        "AND (source_task_instance_id IS NULL "
        "OR source_task_instance_id > 0)"
    ),
    "ck_worker_task_term_source_status": (
        "source_task_status IN ('pending', 'in_progress', 'executing', "
        "'plan_review', 'merging', 'migrating', 'completed', 'failed', "
        "'cancelled', 'conflict')"
    ),
    "ck_worker_task_term_handoff_shape": (
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
        "AND source_worker_turn_handoff_acknowledged IN (TRUE, FALSE))"
    ),
    "ck_worker_task_term_request_digest": "LENGTH(request_digest) = 64",
    "ck_worker_task_term_result_pair": (
        "(result_payload IS NULL AND result_digest IS NULL) OR "
        "(result_payload IS NOT NULL AND result_digest IS NOT NULL "
        "AND LENGTH(result_digest) = 64)"
    ),
    "ck_worker_task_term_result_status": (
        "(status IN ('awaiting_ack', 'settled', 'rejected', 'succeeded', "
        "'acknowledged') AND result_payload IS NOT NULL) OR "
        "(status IN ('pending_remote', 'accepted', 'executing') "
        "AND result_payload IS NULL) OR status = 'conflict'"
    ),
    "ck_worker_task_term_counters": (
        "state_version >= 1 AND attempt_count >= 0 "
        "AND reconcile_count >= 0"
    ),
    "ck_worker_task_term_execution_owner": (
        "(side = 'manager' AND execution_token IS NULL) OR "
        "(side = 'worker' AND ((status = 'executing' "
        "AND execution_token IS NOT NULL "
        "AND LENGTH(execution_token) = 32 "
        "AND next_reconcile_at IS NOT NULL) OR "
        "(status <> 'executing' AND execution_token IS NULL)))"
    ),
    "ck_worker_task_term_accepted_at": (
        "(status IN ('awaiting_ack', 'settled', 'accepted', 'executing', "
        "'succeeded', 'acknowledged', 'rejected') "
        "AND accepted_at IS NOT NULL) OR "
        "(status = 'pending_remote' AND accepted_at IS NULL) "
        "OR status = 'conflict'"
    ),
    "ck_worker_task_term_completed_at": (
        "(status IN ('awaiting_ack', 'settled', 'succeeded', "
        "'acknowledged', 'rejected') AND completed_at IS NOT NULL) OR "
        "(status IN ('pending_remote', 'accepted', 'executing') "
        "AND completed_at IS NULL) OR status = 'conflict'"
    ),
    "ck_worker_task_term_acknowledged_at": (
        "(((status IN ('settled', 'acknowledged') "
        "OR (side = 'manager' AND status = 'rejected')) "
        "AND acknowledged_at IS NOT NULL) OR ("
        "(status NOT IN ('settled', 'acknowledged') "
        "AND NOT (side = 'manager' AND status = 'rejected') "
        ") AND acknowledged_at IS NULL))"
    ),
    "ck_worker_task_term_ack_intent": (
        "(side = 'worker' AND ack_intent_at IS NULL) OR "
        "(side = 'manager' AND ("
        "(status = 'pending_remote' AND ack_intent_at IS NULL) OR "
        "status IN ('awaiting_ack', 'conflict') OR "
        "(status IN ('settled', 'rejected') "
        "AND ack_intent_at IS NOT NULL)))"
    ),
    "ck_worker_task_term_timeline": (
        "(completed_at IS NULL OR accepted_at IS NULL "
        "OR completed_at >= accepted_at) "
        "AND (ack_intent_at IS NULL OR "
        "(completed_at IS NOT NULL "
        "AND ack_intent_at >= completed_at)) "
        "AND (acknowledged_at IS NULL OR "
        "(completed_at IS NOT NULL "
        "AND acknowledged_at >= completed_at "
        "AND (ack_intent_at IS NULL "
        "OR acknowledged_at >= ack_intent_at)))"
    ),
}


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _dialect_name() -> str:
    return op.get_bind().dialect.name


def _acquire_preflight_fence(
    *,
    expected_revision: str,
    include_worker_terminations: bool = False,
) -> None:
    """Order every supported writer before the destructive preflight.

    PostgreSQL keeps the ACCESS EXCLUSIVE lock until Alembic commits the
    revision.  SQLite's first statement is a real write to the one-row version
    table, upgrading its deferred transaction to the database-wide writer
    transaction before any preflight SELECT.  MySQL cannot retain a table lock
    across implicit-commit DDL; its online-safe guard is instead installed by
    the first atomic ALTER in the dialect-specific state machine below.
    """

    dialect = _dialect_name()
    if dialect == "postgresql":
        tables = "tasks, log_entries, capability_invocations"
        if include_worker_terminations:
            tables += f", {_WORKER_TASK_TERMINATION_TABLE}"
        op.execute(
            sa.text(
                f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"
            )
        )
        return
    if dialect != "sqlite" or _is_offline():
        return
    fenced = op.get_bind().execute(
        sa.text(
            "UPDATE alembic_version SET version_num = version_num "
            "WHERE version_num = :expected_revision"
        ),
        {"expected_revision": expected_revision},
    )
    if fenced.rowcount != 1:
        raise RuntimeError(
            "terminal arbitration migration could not acquire its SQLite "
            "revision writer fence"
        )


def _require_supported_mysql() -> None:
    """Fail closed where CHECK constraints or atomic ALTER are not reliable."""

    if _dialect_name() != "mysql":
        return
    if _is_offline():
        raise RuntimeError(
            "terminal arbitration migration refuses MySQL offline SQL: "
            "server version, enforced CHECK constraints, and InnoDB atomic "
            "ALTER TABLE cannot be proven without an online connection"
        )
    dialect = op.get_bind().dialect
    if getattr(dialect, "is_mariadb", False):
        raise RuntimeError(
            "terminal arbitration migration requires MySQL 8.0.16+; "
            "MariaDB must be migrated during a fully drained maintenance window"
        )
    version = getattr(dialect, "server_version_info", None)
    if not version or tuple(version[:3]) < (8, 0, 16):
        raise RuntimeError(
            "terminal arbitration migration requires MySQL 8.0.16+ for "
            "enforced CHECK constraints and atomic ALTER TABLE"
        )
    rows = op.get_bind().execute(
        sa.text(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN "
            "('capability_invocations', 'log_entries', 'tasks')"
        )
    )
    engines = {str(table).lower(): str(engine).lower() for table, engine in rows}
    required = {"capability_invocations", "log_entries", "tasks"}
    if set(engines) != required or any(
        engines[table] != "innodb" for table in required
    ):
        raise RuntimeError(
            "terminal arbitration migration requires capability_invocations, "
            "log_entries, and tasks to exist as InnoDB tables; MySQL atomic "
            "DDL is not guaranteed for any other engine"
        )


def _assert_upgrade_preconditions(*, require_zero_agent: bool = True) -> None:
    """Refuse unsafe legacy data before issuing any non-transactional DDL."""

    if _is_offline():
        return
    bind = op.get_bind()
    if require_zero_agent:
        agent_request_count = bind.execute(
            sa.text(
                "SELECT COUNT(*) FROM capability_invocations "
                "WHERE source = 'agent_request'"
            )
        ).scalar_one()
        if agent_request_count:
            raise RuntimeError(
                "terminal arbitration upgrade requires zero legacy agent_request "
                "invocations; audit fields cannot be reconstructed safely"
            )
    duplicate_output = bind.execute(
        sa.text(
            "SELECT task_id, request_output_log_id "
            "FROM capability_invocations "
            "WHERE request_output_log_id IS NOT NULL "
            "GROUP BY task_id, request_output_log_id "
            "HAVING COUNT(*) > 1"
        )
    ).first()
    if duplicate_output is not None:
        raise RuntimeError(
            "terminal arbitration upgrade found duplicate task/output-log "
            "capability invocations"
        )


def _emit_postgresql_offline_downgrade_guard() -> None:
    """Emit executable data assertions for PostgreSQL ``--sql`` output."""

    op.execute(
        sa.text(
            """
DO $ccm_terminal_arbitration$
BEGIN
    IF EXISTS (
        SELECT 1 FROM capability_invocations
        WHERE source = 'agent_request'
    ) THEN
        RAISE EXCEPTION
            'terminal arbitration downgrade refused: agent_request audit history would be destroyed';
    END IF;
    IF EXISTS (
        SELECT 1 FROM tasks
        WHERE turn_source_log_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'terminal arbitration downgrade refused: Task turn provenance would be destroyed';
    END IF;
    IF EXISTS (
        SELECT 1 FROM log_entries
        WHERE turn_scope IS NOT NULL OR actual_transport IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'terminal arbitration downgrade refused: LogEntry turn provenance would be destroyed';
    END IF;
    IF EXISTS (
        SELECT 1 FROM worker_task_termination_receipts
    ) THEN
        RAISE EXCEPTION
            'terminal arbitration downgrade refused: Worker termination receipt history would be destroyed';
    END IF;
END
$ccm_terminal_arbitration$
"""
        )
    )


def _assert_downgrade_preconditions(
    *,
    require_zero_agent: bool = True,
    require_zero_task_source: bool = True,
    require_zero_log_provenance: bool = True,
    require_zero_worker_terminations: bool = True,
) -> None:
    """Do not discard Agent audit or terminal-arbitration provenance."""

    if _is_offline():
        if _dialect_name() == "postgresql":
            _emit_postgresql_offline_downgrade_guard()
        return
    bind = op.get_bind()
    if require_zero_agent:
        agent_request_count = bind.execute(
            sa.text(
                "SELECT COUNT(*) FROM capability_invocations "
                "WHERE source = 'agent_request'"
            )
        ).scalar_one()
        if agent_request_count:
            raise RuntimeError(
                "terminal arbitration downgrade refused: agent_request audit "
                "history would be destroyed"
            )
    if require_zero_task_source:
        task_source_count = bind.execute(
            sa.text(
                "SELECT COUNT(*) FROM tasks "
                "WHERE turn_source_log_id IS NOT NULL"
            )
        ).scalar_one()
        if task_source_count:
            raise RuntimeError(
                "terminal arbitration downgrade refused: Task turn provenance "
                "would be destroyed"
            )
    if require_zero_log_provenance:
        log_provenance_count = bind.execute(
            sa.text(
                "SELECT COUNT(*) FROM log_entries "
                "WHERE turn_scope IS NOT NULL OR actual_transport IS NOT NULL"
            )
        ).scalar_one()
        if log_provenance_count:
            raise RuntimeError(
                "terminal arbitration downgrade refused: LogEntry turn "
                "provenance would be destroyed"
            )
    if require_zero_worker_terminations:
        receipt_count = bind.execute(
            sa.text(
                "SELECT COUNT(*) FROM worker_task_termination_receipts"
            )
        ).scalar_one()
        if receipt_count:
            raise RuntimeError(
                "terminal arbitration downgrade refused: Worker termination "
                "receipt history would be destroyed"
            )


def _sqlite_task_id_highwater() -> int | None:
    context = op.get_context()
    bind = op.get_bind()
    if context.as_sql or bind.dialect.name != "sqlite":
        return None
    value = bind.execute(
        sa.text("SELECT seq FROM sqlite_sequence WHERE name = 'tasks'")
    ).scalar_one_or_none()
    return int(value) if value is not None else None


def _restore_sqlite_task_id_highwater(highwater: int | None) -> None:
    if highwater is None:
        return
    bind = op.get_bind()
    advanced = bind.execute(
        sa.text(
            "UPDATE sqlite_sequence "
            "SET seq = CASE WHEN seq < :highwater THEN :highwater ELSE seq END "
            "WHERE name = 'tasks'"
        ),
        {"highwater": highwater},
    )
    if advanced.rowcount == 0:
        bind.execute(
            sa.text(
                "INSERT INTO sqlite_sequence(name, seq) "
                "VALUES ('tasks', :highwater)"
            ),
            {"highwater": highwater},
        )


def _task_batch_kwargs() -> dict:
    """Preserve the external Task-id non-reuse guarantee on SQLite."""

    if op.get_bind().dialect.name != "sqlite":
        return {}
    return {
        "recreate": "always",
        "table_kwargs": {"sqlite_autoincrement": True},
    }


def _normalized_check(sqltext: object) -> str:
    normalized = str("" if sqltext is None else sqltext).replace("`", "").lower()
    normalized = re.sub(r"_[a-z0-9]+\s*'", "'", normalized)
    return " ".join(normalized.split())


def _strip_balanced_outer_parentheses(expression: str) -> str:
    expression = expression.strip()
    while expression.startswith("(") and expression.endswith(")"):
        depth = 0
        quote = False
        closes_at_end = False
        for index, char in enumerate(expression):
            if char == "'":
                quote = not quote
            elif not quote:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0:
                        closes_at_end = index == len(expression) - 1
                        break
        if not closes_at_end:
            break
        expression = expression[1:-1].strip()
    return expression


def _split_top_level(expression: str, operator: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote = False
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            quote = not quote
            index += 1
            continue
        if not quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0 and expression.startswith(operator, index):
                parts.append(expression[start:index].strip())
                index += len(operator)
                start = index
                continue
        index += 1
    parts.append(expression[start:].strip())
    return parts


def _boolean_check_shape(sqltext: object):
    expression = _strip_balanced_outer_parentheses(
        _normalized_check(sqltext)
    )
    or_parts = _split_top_level(expression, " or ")
    if len(or_parts) > 1:
        return ("or", tuple(_boolean_check_shape(part) for part in or_parts))
    and_parts = _split_top_level(expression, " and ")
    if len(and_parts) > 1:
        return ("and", tuple(_boolean_check_shape(part) for part in and_parts))
    # All current atoms use identifiers and literals without meaningful
    # whitespace. Compacting here tolerates server formatting without erasing
    # the AND/OR grouping captured above.
    return ("atom", "".join(expression.split()))


def _identity_check_kind(sqltext: object) -> str | None:
    shape = _boolean_check_shape(sqltext)
    if shape == _boolean_check_shape(_NEW_AGENT_REQUEST_IDENTITY):
        return "new"
    if shape == _boolean_check_shape(_OLD_AGENT_REQUEST_IDENTITY):
        return "old"
    return None


def _is_mysql_downgrade_gate(sqltext: object) -> bool:
    return _boolean_check_shape(sqltext) == _boolean_check_shape(
        "source <> 'agent_request'"
    )


_WORKER_TASK_TERMINATION_COLUMN_SHAPES = {
    "operation_id": (sa.String, 32, False),
    "task_id": (sa.Integer, None, False),
    "active_task_id": (sa.Integer, None, True),
    "side": (sa.String, 16, False),
    "worker_id": (sa.Integer, None, True),
    "operation": (sa.String, 16, False),
    "status": (sa.String, 24, False),
    "state_version": (sa.Integer, None, False),
    "execution_token": (sa.String, 32, True),
    "source_task_incarnation_id": (sa.String, 32, True),
    "source_task_status": (sa.String, 20, False),
    "source_task_retry_count": (sa.Integer, None, False),
    "source_task_turn_generation": (sa.BigInteger, None, False),
    "source_task_source_log_id": (sa.Integer, None, True),
    "source_task_instance_id": (sa.Integer, None, True),
    "source_task_started_at": (sa.DateTime, None, True),
    "source_task_completed_at": (sa.DateTime, None, True),
    "source_task_session_id": (sa.String, 200, True),
    "source_task_pty_background_generation": (sa.String, 64, True),
    "source_worker_turn_handoff_id": (sa.String, 32, True),
    "source_worker_turn_handoff_worker_id": (sa.Integer, None, True),
    "source_worker_turn_handoff_retry_count": (sa.Integer, None, True),
    "source_worker_turn_handoff_from_generation": (sa.BigInteger, None, True),
    "source_worker_turn_handoff_source_log_id": (sa.Integer, None, True),
    "source_worker_turn_handoff_acknowledged": (sa.Boolean, None, True),
    "request_payload": (sa.JSON, None, False),
    "request_digest": (sa.String, 64, False),
    "result_payload": (sa.JSON, None, True),
    "result_digest": (sa.String, 64, True),
    "attempt_count": (sa.Integer, None, False),
    "reconcile_count": (sa.Integer, None, False),
    "next_reconcile_at": (sa.DateTime, None, True),
    "last_error": (sa.Text, None, True),
    "accepted_at": (sa.DateTime, None, True),
    "completed_at": (sa.DateTime, None, True),
    "ack_intent_at": (sa.DateTime, None, True),
    "acknowledged_at": (sa.DateTime, None, True),
    "created_at": (sa.DateTime, None, False),
    "updated_at": (sa.DateTime, None, False),
}


def _worker_task_termination_columns() -> list[sa.Column]:
    """Return fresh columns for ``op.create_table`` on every replay."""

    return [
        sa.Column("operation_id", sa.String(length=32), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("active_task_id", sa.Integer(), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("worker_id", sa.Integer(), nullable=True),
        sa.Column("operation", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column(
            "state_version",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
        sa.Column("execution_token", sa.String(length=32), nullable=True),
        sa.Column(
            "source_task_incarnation_id",
            sa.String(length=32),
            nullable=True,
        ),
        sa.Column("source_task_status", sa.String(length=20), nullable=False),
        sa.Column("source_task_retry_count", sa.Integer(), nullable=False),
        sa.Column(
            "source_task_turn_generation", sa.BigInteger(), nullable=False
        ),
        sa.Column("source_task_source_log_id", sa.Integer(), nullable=True),
        sa.Column("source_task_instance_id", sa.Integer(), nullable=True),
        sa.Column("source_task_started_at", sa.DateTime(), nullable=True),
        sa.Column("source_task_completed_at", sa.DateTime(), nullable=True),
        sa.Column(
            "source_task_session_id",
            sa.String(length=200),
            nullable=True,
        ),
        sa.Column(
            "source_task_pty_background_generation",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "source_worker_turn_handoff_id",
            sa.String(length=32),
            nullable=True,
        ),
        sa.Column(
            "source_worker_turn_handoff_worker_id",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "source_worker_turn_handoff_retry_count",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "source_worker_turn_handoff_from_generation",
            sa.BigInteger(),
            nullable=True,
        ),
        sa.Column(
            "source_worker_turn_handoff_source_log_id",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "source_worker_turn_handoff_acknowledged",
            sa.Boolean(),
            nullable=True,
        ),
        sa.Column("request_payload", sa.JSON(), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("result_payload", sa.JSON(), nullable=True),
        sa.Column("result_digest", sa.String(length=64), nullable=True),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "reconcile_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("next_reconcile_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("accepted_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("ack_intent_at", sa.DateTime(), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    ]


def _create_worker_task_termination_table() -> None:
    op.create_table(
        _WORKER_TASK_TERMINATION_TABLE,
        *_worker_task_termination_columns(),
        *(
            sa.CheckConstraint(expression, name=name)
            for name, expression in _WORKER_TASK_TERMINATION_CHECKS.items()
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("operation_id"),
        sa.UniqueConstraint(
            "active_task_id",
            name="uq_worker_task_term_active_task",
        ),
        mysql_engine="InnoDB",
    )
    for name, columns in _WORKER_TASK_TERMINATION_INDEXES.items():
        op.create_index(
            name,
            _WORKER_TASK_TERMINATION_TABLE,
            list(columns),
            unique=False,
        )


def _worker_task_termination_type_matches(
    reflected: sa.types.TypeEngine,
    expected_type: type[sa.types.TypeEngine],
    expected_length: int | None,
    *,
    dialect_name: str,
) -> bool:
    if expected_type is sa.Boolean and dialect_name == "mysql":
        # MySQL implements and reflects BOOLEAN as signed TINYINT(1). Accept
        # exactly that physical shape, not arbitrary integer columns that
        # merely happen to be truthy in application code.
        if isinstance(reflected, sa.Boolean):
            return True
        return bool(
            type(reflected).__name__.upper() == "TINYINT"
            and type(reflected).__module__.startswith(
                "sqlalchemy.dialects.mysql"
            )
            and getattr(reflected, "display_width", None) == 1
            and not bool(getattr(reflected, "unsigned", False))
            and not bool(getattr(reflected, "zerofill", False))
        )
    if expected_type in {sa.Integer, sa.BigInteger}:
        expected_name = (
            "INTEGER" if expected_type is sa.Integer else "BIGINT"
        )
        return bool(
            type(reflected).__name__.upper() == expected_name
            and not bool(getattr(reflected, "unsigned", False))
            and not bool(getattr(reflected, "zerofill", False))
        )
    if not isinstance(reflected, expected_type):
        return False
    if expected_length is not None:
        return getattr(reflected, "length", None) == expected_length
    return True


def _check_expression_shape(sqltext: object):
    """Return one formatting-insensitive CHECK expression shape.

    PostgreSQL's ``pg_get_constraintdef`` includes a leading ``CHECK`` wrapper,
    while SQLAlchemy's SQLite/MySQL inspectors normally return only the inner
    expression.  Remove that wrapper without weakening the AND/OR grouping
    retained by :func:`_boolean_check_shape`.
    """

    expression = _normalized_check(sqltext)
    if expression.startswith("check"):
        expression = expression[len("check") :].strip()
    return _boolean_check_shape(expression)


def _assert_worker_task_termination_check_semantics(
    actual: dict[str, object],
    expected: dict[str, object],
) -> None:
    """Reject same-named receipt CHECKs whose expressions were weakened."""

    expected_names = set(_WORKER_TASK_TERMINATION_CHECKS)
    if set(actual) != expected_names or set(expected) != expected_names:
        raise RuntimeError(
            "Worker termination receipt CHECK set is partial or foreign"
        )
    for name in sorted(expected_names):
        if _check_expression_shape(actual[name]) != _check_expression_shape(
            expected[name]
        ):
            raise RuntimeError(
                f"Worker termination receipt CHECK {name} is malformed"
            )


def _postgresql_worker_task_termination_check_definitions(
    relation_name: str,
) -> dict[str, str]:
    """Read canonical, validated PostgreSQL CHECK definitions for a relation."""

    rows = op.get_bind().execute(
        sa.text(
            "SELECT constraint_row.conname, "
            "pg_catalog.pg_get_constraintdef(constraint_row.oid, true), "
            "constraint_row.convalidated "
            "FROM pg_catalog.pg_constraint AS constraint_row "
            "WHERE constraint_row.conrelid = "
            "pg_catalog.to_regclass(:relation_name) "
            "AND constraint_row.contype = 'c'"
        ),
        {"relation_name": relation_name},
    )
    definitions: dict[str, str] = {}
    for name, definition, validated in rows:
        if not bool(validated):
            raise RuntimeError(
                "Worker termination receipt CHECK "
                f"{str(name).lower()} is not validated"
            )
        definitions[str(name).lower()] = str(definition)
    return definitions


def _postgresql_assert_worker_task_termination_check_semantics() -> None:
    """Ask PostgreSQL itself to canonicalize the expected CHECK expressions.

    PostgreSQL rewrites ``IN``/``NOT IN`` into typed ``ANY``/``ALL`` arrays and
    inserts implementation-dependent casts.  A temporary table built on the
    same connection gives us canonical expected definitions without guessing
    those server-version details.  It is dropped before migration continues
    and cannot leave durable schema behind.
    """

    bind = op.get_bind()
    probe = sa.Table(
        _POSTGRESQL_WORKER_TASK_TERMINATION_CHECK_PROBE,
        sa.MetaData(),
        *_worker_task_termination_columns(),
        *(
            sa.CheckConstraint(expression, name=name)
            for name, expression in _WORKER_TASK_TERMINATION_CHECKS.items()
        ),
        prefixes=["TEMPORARY"],
    )
    bind.execute(sa.schema.CreateTable(probe))
    try:
        actual = _postgresql_worker_task_termination_check_definitions(
            _WORKER_TASK_TERMINATION_TABLE
        )
        expected = _postgresql_worker_task_termination_check_definitions(
            _POSTGRESQL_WORKER_TASK_TERMINATION_CHECK_PROBE
        )
        _assert_worker_task_termination_check_semantics(actual, expected)
    finally:
        bind.execute(sa.schema.DropTable(probe))


def _mysql_assert_worker_task_termination_check_semantics(
    actual: dict[str, object],
) -> None:
    """Compare against the expressions canonicalized by this MySQL server.

    MySQL applies boolean rewrites while storing CHECKs, including De Morgan
    expansion of ``NOT (a AND b)``.  A connection-local temporary table lets
    the same server canonicalize the expected expressions without weakening
    the semantic comparison or leaving durable migration state behind.
    """

    bind = op.get_bind()
    probe = sa.Table(
        _MYSQL_WORKER_TASK_TERMINATION_CHECK_PROBE,
        sa.MetaData(),
        *_worker_task_termination_columns(),
        *(
            sa.CheckConstraint(expression, name=name)
            for name, expression in _WORKER_TASK_TERMINATION_CHECKS.items()
        ),
        prefixes=["TEMPORARY"],
        mysql_engine="InnoDB",
    )
    bind.execute(sa.schema.CreateTable(probe))
    try:
        expected = {
            str(constraint.get("name") or "").lower(): constraint.get(
                "sqltext"
            )
            for constraint in sa.inspect(bind).get_check_constraints(
                _MYSQL_WORKER_TASK_TERMINATION_CHECK_PROBE
            )
        }
        _assert_worker_task_termination_check_semantics(actual, expected)
    finally:
        bind.execute(sa.schema.DropTable(probe))


def _worker_task_termination_state(
    *,
    allow_downgrade_gate: bool = False,
) -> dict[str, object]:
    """Reflect and validate an atomic CREATE plus replayable index suffix."""

    if _is_offline():
        raise RuntimeError(
            "Worker termination receipt state requires an online connection"
        )
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table(_WORKER_TASK_TERMINATION_TABLE):
        return {
            "present": False,
            "missing_indexes": set(_WORKER_TASK_TERMINATION_INDEXES),
            "downgrade_gate": False,
            "downgrade_gate_enforced": False,
        }

    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_WORKER_TASK_TERMINATION_TABLE)
    }
    if set(columns) != set(_WORKER_TASK_TERMINATION_COLUMN_SHAPES):
        raise RuntimeError(
            "Worker termination receipt table has a partial or foreign "
            "column set"
        )
    for name, (expected_type, expected_length, nullable) in (
        _WORKER_TASK_TERMINATION_COLUMN_SHAPES.items()
    ):
        column = columns[name]
        if (
            not _worker_task_termination_type_matches(
                column["type"],
                expected_type,
                expected_length,
                dialect_name=bind.dialect.name,
            )
            or bool(column.get("nullable")) is not nullable
        ):
            raise RuntimeError(
                f"Worker termination receipt column {name} is malformed"
            )

    primary_key = tuple(
        str(column).lower()
        for column in (
            inspector.get_pk_constraint(_WORKER_TASK_TERMINATION_TABLE).get(
                "constrained_columns"
            )
            or ()
        )
    )
    if primary_key != ("operation_id",):
        raise RuntimeError(
            "Worker termination receipt operation primary key is malformed"
        )

    uniques = {
        str(constraint.get("name") or "").lower(): tuple(
            str(column).lower()
            for column in (constraint.get("column_names") or ())
        )
        for constraint in inspector.get_unique_constraints(
            _WORKER_TASK_TERMINATION_TABLE
        )
    }
    if uniques != {
        "uq_worker_task_term_active_task": ("active_task_id",)
    }:
        raise RuntimeError(
            "Worker termination receipt active-task UNIQUE is malformed"
        )

    foreign_keys = inspector.get_foreign_keys(_WORKER_TASK_TERMINATION_TABLE)
    expected_foreign_key = False
    for foreign_key in foreign_keys:
        if (
            tuple(foreign_key.get("constrained_columns") or ())
            == ("task_id",)
            and str(foreign_key.get("referred_table") or "").lower()
            == "tasks"
            and tuple(foreign_key.get("referred_columns") or ()) == ("id",)
            and str(
                (foreign_key.get("options") or {}).get("ondelete") or ""
            ).upper()
            == "CASCADE"
        ):
            expected_foreign_key = True
            break
    if not expected_foreign_key or len(foreign_keys) != 1:
        raise RuntimeError(
            "Worker termination receipt Task foreign key is malformed"
        )

    checks = {
        str(constraint.get("name") or "").lower(): constraint.get("sqltext")
        for constraint in inspector.get_check_constraints(
            _WORKER_TASK_TERMINATION_TABLE
        )
    }
    expected_check_names = set(_WORKER_TASK_TERMINATION_CHECKS)
    allowed_check_names = set(expected_check_names)
    if allow_downgrade_gate:
        allowed_check_names.add(_WORKER_TASK_TERMINATION_DOWNGRADE_GATE)
    if set(checks) != allowed_check_names and not (
        set(checks) == expected_check_names
        and allow_downgrade_gate
    ):
        raise RuntimeError(
            "Worker termination receipt CHECK set is partial or foreign"
        )
    # Names and ENFORCED flags are not a semantic proof: a same-named foreign
    # table could replace any expression with CHECK (TRUE). SQLite/MySQL retain
    # the authored expression closely enough for normalized comparison.
    # PostgreSQL rewrites IN lists and casts, so compare it with a temporary
    # canonical probe generated by the same server instead.
    if bind.dialect.name == "postgresql":
        _postgresql_assert_worker_task_termination_check_semantics()
    elif bind.dialect.name == "mysql":
        _mysql_assert_worker_task_termination_check_semantics(
            {name: checks[name] for name in expected_check_names}
        )
    else:
        _assert_worker_task_termination_check_semantics(
            {name: checks[name] for name in expected_check_names},
            _WORKER_TASK_TERMINATION_CHECKS,
        )

    downgrade_gate = (
        _WORKER_TASK_TERMINATION_DOWNGRADE_GATE in checks
    )
    if downgrade_gate and _boolean_check_shape(
        checks[_WORKER_TASK_TERMINATION_DOWNGRADE_GATE]
    ) != _boolean_check_shape("operation_id IS NULL"):
        raise RuntimeError(
            "Worker termination receipt downgrade fence is malformed"
        )
    downgrade_gate_enforced = False
    if bind.dialect.name == "mysql":
        engines = bind.execute(
            sa.text(
                "SELECT ENGINE FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
            ),
            {"table_name": _WORKER_TASK_TERMINATION_TABLE},
        ).scalars().all()
        if len(engines) != 1 or str(engines[0]).lower() != "innodb":
            raise RuntimeError(
                "Worker termination receipt table must use InnoDB"
            )
        enforced = _mysql_enforced_checks(_WORKER_TASK_TERMINATION_TABLE)
        if not expected_check_names <= enforced:
            raise RuntimeError(
                "Worker termination receipt CHECKs are not all enforced"
            )
        downgrade_gate_enforced = (
            _WORKER_TASK_TERMINATION_DOWNGRADE_GATE in enforced
        )
        if downgrade_gate and not downgrade_gate_enforced:
            raise RuntimeError(
                "Worker termination receipt downgrade fence is not enforced"
            )

    reflected_indexes = inspector.get_indexes(
        _WORKER_TASK_TERMINATION_TABLE
    )
    indexes = {
        str(index.get("name") or "").lower(): (
            tuple(
                str(column).lower()
                for column in (index.get("column_names") or ())
            ),
            bool(index.get("unique")),
        )
        for index in reflected_indexes
    }
    missing_indexes: set[str] = set()
    for name, expected_columns in _WORKER_TASK_TERMINATION_INDEXES.items():
        actual = indexes.get(name)
        if actual is None:
            missing_indexes.add(name)
        elif actual != (expected_columns, False):
            raise RuntimeError(
                f"Worker termination receipt index {name} is malformed"
            )
    # Some dialects also expose a PK/UNIQUE constraint through get_indexes().
    # Those two exact shapes are redundant and safe.  Any other unique index
    # changes receipt admission semantics and cannot be a migration remainder.
    allowed_constraint_indexes = {
        ("operation_id",),
        ("active_task_id",),
    }
    if any(
        unique and columns not in allowed_constraint_indexes
        for columns, unique in indexes.values()
    ):
        raise RuntimeError(
            "Worker termination receipt has a foreign UNIQUE index"
        )

    return {
        "present": True,
        "missing_indexes": missing_indexes,
        "downgrade_gate": downgrade_gate,
        "downgrade_gate_enforced": downgrade_gate_enforced,
    }


def _ensure_worker_task_termination_table() -> None:
    if _is_offline():
        _create_worker_task_termination_table()
        return
    state = _worker_task_termination_state()
    if not state["present"]:
        _create_worker_task_termination_table()
        state = _worker_task_termination_state()
    else:
        for name in sorted(state["missing_indexes"]):
            op.create_index(
                name,
                _WORKER_TASK_TERMINATION_TABLE,
                list(_WORKER_TASK_TERMINATION_INDEXES[name]),
                unique=False,
            )
        state = _worker_task_termination_state()
    if state["missing_indexes"]:
        raise RuntimeError(
            "Worker termination receipt index replay did not settle"
        )


def _drop_worker_task_termination_table_transactional() -> None:
    if not _is_offline():
        state = _worker_task_termination_state()
        if not state["present"]:
            return
    op.drop_table(_WORKER_TASK_TERMINATION_TABLE)


def _mysql_install_worker_task_termination_downgrade_gate() -> None:
    state = _worker_task_termination_state(allow_downgrade_gate=True)
    if not state["present"]:
        return
    if not state["downgrade_gate"]:
        _mysql_alter(
            _WORKER_TASK_TERMINATION_TABLE,
            [
                "ADD CONSTRAINT "
                f"{_WORKER_TASK_TERMINATION_DOWNGRADE_GATE} "
                "CHECK (operation_id IS NULL)"
            ],
        )
        state = _worker_task_termination_state(allow_downgrade_gate=True)
    if (
        state["downgrade_gate"] is not True
        or state["downgrade_gate_enforced"] is not True
    ):
        raise RuntimeError(
            "Worker termination receipt downgrade fence did not settle"
        )


def _mysql_drop_worker_task_termination_table() -> None:
    state = _worker_task_termination_state(allow_downgrade_gate=True)
    if not state["present"]:
        return
    if (
        state["downgrade_gate"] is not True
        or state["downgrade_gate_enforced"] is not True
    ):
        raise RuntimeError(
            "Worker termination receipt table cannot be dropped without its "
            "durable writer fence"
        )
    op.drop_table(_WORKER_TASK_TERMINATION_TABLE)


def _mysql_enforced_checks(table: str) -> set[str]:
    rows = op.get_bind().execute(
        sa.text(
            "SELECT CONSTRAINT_NAME, ENFORCED "
            "FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name "
            "AND CONSTRAINT_TYPE = 'CHECK'"
        ),
        {"table_name": table},
    )
    return {
        str(name).lower()
        for name, enforced in rows
        if str(enforced).upper() == "YES"
    }


def _mysql_capability_state() -> dict[str, object]:
    inspector = sa.inspect(op.get_bind())
    column_rows = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns("capability_invocations")
    }
    unique_rows = {
        str(constraint.get("name") or "").lower(): tuple(
            str(column).lower()
            for column in (constraint.get("column_names") or ())
        )
        for constraint in inspector.get_unique_constraints(
            "capability_invocations"
        )
    }
    checks = {
        str(constraint.get("name") or "").lower(): constraint.get("sqltext")
        for constraint in inspector.get_check_constraints(
            "capability_invocations"
        )
    }
    canonical_sql = checks.get("ck_cap_inv_agent_request_identity")
    shadow_sql = checks.get(_MYSQL_SHADOW_IDENTITY_CHECK.lower())
    gate_sql = checks.get(_MYSQL_DOWNGRADE_GATE.lower())
    enforced = _mysql_enforced_checks("capability_invocations")
    reason = column_rows.get("request_reason")
    protocol = column_rows.get("request_protocol_version")
    output_hash = column_rows.get("request_output_hash")
    column_shapes = {
        "request_reason": (
            reason is None
            or (
                type(reason["type"]).__name__.upper() == "TEXT"
                and reason.get("nullable") is True
            )
        ),
        "request_protocol_version": (
            protocol is None
            or (
                type(protocol["type"]).__name__.upper() == "INTEGER"
                and protocol.get("nullable") is True
                and not bool(getattr(protocol["type"], "unsigned", False))
                and not bool(getattr(protocol["type"], "zerofill", False))
            )
        ),
        "request_output_hash": (
            output_hash is None
            or (
                type(output_hash["type"]).__name__.upper() == "VARCHAR"
                and getattr(
                    output_hash["type"],
                    "length",
                    None,
                )
                == 64
                and output_hash.get("nullable") is True
            )
        ),
    }
    return {
        "columns": set(column_rows),
        "column_shapes": column_shapes,
        "unique": unique_rows.get("uq_cap_inv_task_output_log")
        == ("task_id", "request_output_log_id"),
        "unique_present": "uq_cap_inv_task_output_log" in unique_rows,
        "canonical": _identity_check_kind(canonical_sql),
        "canonical_present": canonical_sql is not None,
        "canonical_enforced": (
            "ck_cap_inv_agent_request_identity" in enforced
        ),
        "shadow": _identity_check_kind(shadow_sql),
        "shadow_present": shadow_sql is not None,
        "shadow_enforced": _MYSQL_SHADOW_IDENTITY_CHECK.lower() in enforced,
        "gate": (
            _is_mysql_downgrade_gate(gate_sql)
            if gate_sql is not None
            else False
        ),
        "gate_present": gate_sql is not None,
        "gate_enforced": _MYSQL_DOWNGRADE_GATE.lower() in enforced,
    }


def _mysql_auxiliary_state() -> dict[str, object]:
    """Reflect the crash-replayable Task/LogEntry migration state."""

    inspector = sa.inspect(op.get_bind())
    task_columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns("tasks")
    }
    task_checks = {
        str(constraint.get("name") or "").lower(): constraint.get("sqltext")
        for constraint in inspector.get_check_constraints("tasks")
    }
    log_columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns("log_entries")
    }
    log_checks = {
        str(constraint.get("name") or "").lower(): constraint.get("sqltext")
        for constraint in inspector.get_check_constraints("log_entries")
    }
    enforced_task_checks = _mysql_enforced_checks("tasks")
    enforced_log_checks = _mysql_enforced_checks("log_entries")

    task_source = task_columns.get("turn_source_log_id")
    turn_scope = log_columns.get("turn_scope")
    actual_transport = log_columns.get("actual_transport")
    task_gate_sql = task_checks.get(_MYSQL_TASK_DOWNGRADE_GATE.lower())
    scope_sql = log_checks.get("ck_log_entries_turn_scope")
    transport_sql = log_checks.get("ck_log_entries_actual_transport")
    log_gate_sql = log_checks.get(_MYSQL_LOG_DOWNGRADE_GATE.lower())
    return {
        "task_source_present": task_source is not None,
        "task_source_shape": (
            task_source is None
            or (
                type(task_source["type"]).__name__.upper() == "INTEGER"
                and task_source.get("nullable") is True
                and not bool(getattr(task_source["type"], "unsigned", False))
                and not bool(getattr(task_source["type"], "zerofill", False))
            )
        ),
        "task_gate": (
            _boolean_check_shape(task_gate_sql)
            == _boolean_check_shape(_TASK_DOWNGRADE_GATE_CHECK)
            if task_gate_sql is not None
            else False
        ),
        "task_gate_present": task_gate_sql is not None,
        "task_gate_enforced": (
            _MYSQL_TASK_DOWNGRADE_GATE.lower() in enforced_task_checks
        ),
        "turn_scope_present": turn_scope is not None,
        "turn_scope_shape": (
            turn_scope is None
            or (
                type(turn_scope["type"]).__name__.upper() == "VARCHAR"
                and getattr(turn_scope["type"], "length", None) == 16
                and turn_scope.get("nullable") is True
            )
        ),
        "actual_transport_present": actual_transport is not None,
        "actual_transport_shape": (
            actual_transport is None
            or (
                type(actual_transport["type"]).__name__.upper() == "VARCHAR"
                and getattr(actual_transport["type"], "length", None) == 24
                and actual_transport.get("nullable") is True
            )
        ),
        "scope_check": (
            _boolean_check_shape(scope_sql)
            == _boolean_check_shape(_TURN_SCOPE_CHECK)
            if scope_sql is not None
            else False
        ),
        "scope_check_present": scope_sql is not None,
        "scope_check_enforced": (
            "ck_log_entries_turn_scope" in enforced_log_checks
        ),
        "transport_check": (
            _boolean_check_shape(transport_sql)
            == _boolean_check_shape(_ACTUAL_TRANSPORT_CHECK)
            if transport_sql is not None
            else False
        ),
        "transport_check_present": transport_sql is not None,
        "transport_check_enforced": (
            "ck_log_entries_actual_transport" in enforced_log_checks
        ),
        "log_gate": (
            _boolean_check_shape(log_gate_sql)
            == _boolean_check_shape(_LOG_DOWNGRADE_GATE_CHECK)
            if log_gate_sql is not None
            else False
        ),
        "log_gate_present": log_gate_sql is not None,
        "log_gate_enforced": (
            _MYSQL_LOG_DOWNGRADE_GATE.lower() in enforced_log_checks
        ),
    }


def _assert_mysql_auxiliary_state(state: dict[str, object]) -> None:
    if state["task_source_shape"] is not True:
        raise RuntimeError(
            "terminal arbitration MySQL Task source column is malformed"
        )
    if state["task_gate_present"] and (
        state["task_gate"] is not True
        or state["task_gate_enforced"] is not True
        or state["task_source_present"] is not True
    ):
        raise RuntimeError(
            "terminal arbitration MySQL Task downgrade gate is malformed or "
            "not enforced"
        )
    if (
        state["turn_scope_shape"] is not True
        or state["actual_transport_shape"] is not True
    ):
        raise RuntimeError(
            "terminal arbitration MySQL LogEntry column shape is malformed"
        )
    if state["turn_scope_present"] != state["actual_transport_present"]:
        raise RuntimeError(
            "terminal arbitration MySQL LogEntry columns are only partially present"
        )
    log_columns_present = state["turn_scope_present"] is True
    for label, display_label in (
        ("scope", "turn-scope"),
        ("transport", "actual-transport"),
    ):
        present = state[f"{label}_check_present"]
        if present and (
            state[f"{label}_check"] is not True
            or state[f"{label}_check_enforced"] is not True
            or not log_columns_present
        ):
            raise RuntimeError(
                f"terminal arbitration MySQL {display_label} CHECK is malformed or "
                "not enforced"
            )
    if log_columns_present and (
        state["scope_check_present"] is not True
        or state["transport_check_present"] is not True
    ):
        raise RuntimeError(
            "terminal arbitration MySQL LogEntry runtime CHECKs are incomplete"
        )
    if state["log_gate_present"] and (
        state["log_gate"] is not True
        or state["log_gate_enforced"] is not True
        or not log_columns_present
    ):
        raise RuntimeError(
            "terminal arbitration MySQL LogEntry downgrade gate is malformed "
            "or not enforced"
        )


def _mysql_alter(table: str, actions: list[str]) -> None:
    if not actions:
        return
    op.execute(
        sa.text(
            f"ALTER TABLE {table}\n  " + ",\n  ".join(actions)
        )
    )


def _assert_mysql_guarded(state: dict[str, object]) -> None:
    canonical = state["canonical"]
    shadow = state["shadow"]
    gate = state["gate"]
    canonical_guard = bool(
        canonical in {"old", "new"} and state["canonical_enforced"] is True
    )
    shadow_guard = bool(
        shadow == "new" and state["shadow_enforced"] is True
    )
    gate_guard = bool(gate is True and state["gate_enforced"] is True)
    if not canonical_guard and not shadow_guard and not gate_guard:
        raise RuntimeError(
            "terminal arbitration MySQL recovery found no enforceable "
            "agent_request identity guard"
        )
    if state["canonical_present"] and (
        canonical not in {"old", "new"}
        or state["canonical_enforced"] is not True
    ):
        raise RuntimeError(
            "terminal arbitration MySQL canonical identity CHECK is malformed "
            "or not enforced"
        )
    if state["shadow_present"] and (
        shadow != "new" or state["shadow_enforced"] is not True
    ):
        raise RuntimeError(
            "terminal arbitration MySQL shadow identity CHECK is malformed "
            "or not enforced"
        )
    if state["gate_present"] and (
        gate is not True or state["gate_enforced"] is not True
    ):
        raise RuntimeError(
            "terminal arbitration MySQL downgrade gate is malformed or not enforced"
        )
    if state["unique_present"] and state["unique"] is not True:
        raise RuntimeError(
            "terminal arbitration MySQL output-log unique constraint is malformed"
        )
    column_shapes = state["column_shapes"]
    assert isinstance(column_shapes, dict)
    if not all(column_shapes.values()):
        raise RuntimeError(
            "terminal arbitration MySQL audit column shape is malformed"
        )


def _mysql_has_v2_identity_guard(state: dict[str, object]) -> bool:
    columns = state["columns"]
    assert isinstance(columns, set)
    if not set(_MYSQL_NEW_COLUMNS) <= columns:
        return False
    return bool(
        (
            state["canonical"] == "new"
            and state["canonical_enforced"] is True
        )
        or (
            state["shadow"] == "new"
            and state["shadow_enforced"] is True
        )
    )


def _mysql_has_v2_audit_schema(state: dict[str, object]) -> bool:
    columns = state["columns"]
    assert isinstance(columns, set)
    return bool(
        set(_MYSQL_NEW_COLUMNS) & columns
        or state["canonical"] == "new"
        or state["shadow"] == "new"
    )


def _mysql_upgrade_capability_online() -> None:
    """Advance a crash-replayable MySQL 8 capability-table state machine."""

    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    columns = state["columns"]
    assert isinstance(columns, set)
    if state["canonical"] == "new" and not set(_MYSQL_NEW_COLUMNS) <= columns:
        raise RuntimeError(
            "terminal arbitration MySQL canonical CHECK references missing columns"
        )

    phase_one: list[str] = []
    if "request_reason" not in columns:
        phase_one.append("ADD COLUMN request_reason TEXT NULL")
    if "request_protocol_version" not in columns:
        phase_one.append("ADD COLUMN request_protocol_version INTEGER NULL")
    if "request_output_hash" not in columns:
        phase_one.append(
            "ADD COLUMN request_output_hash VARCHAR(64) NULL"
        )
    if state["unique"] is not True:
        phase_one.append(
            "ADD CONSTRAINT uq_cap_inv_task_output_log "
            "UNIQUE (task_id, request_output_log_id)"
        )
    if state["canonical"] != "new" and state["shadow"] != "new":
        phase_one.append(
            f"ADD CONSTRAINT {_MYSQL_SHADOW_IDENTITY_CHECK} "
            f"CHECK ({_NEW_AGENT_REQUEST_IDENTITY})"
        )
    # MySQL 8 executes one ALTER atomically. A legacy Agent row or a duplicate
    # output racing the preflight makes this whole statement fail while the old
    # canonical CHECK remains installed.
    _mysql_alter("capability_invocations", phase_one)

    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    columns = state["columns"]
    assert isinstance(columns, set)
    if not set(_MYSQL_NEW_COLUMNS) <= columns or state["unique"] is not True:
        raise RuntimeError(
            "terminal arbitration MySQL phase one did not settle atomically"
        )

    if state["canonical"] != "new":
        if state["shadow"] != "new":
            raise RuntimeError(
                "terminal arbitration MySQL cannot replace its canonical "
                "identity CHECK without the v2 shadow"
            )
        actions = []
        if state["canonical_present"]:
            actions.append("DROP CHECK ck_cap_inv_agent_request_identity")
        actions.append(
            "ADD CONSTRAINT ck_cap_inv_agent_request_identity "
            f"CHECK ({_NEW_AGENT_REQUEST_IDENTITY})"
        )
        _mysql_alter("capability_invocations", actions)

    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    if state["canonical"] != "new":
        raise RuntimeError(
            "terminal arbitration MySQL canonical v2 CHECK was not installed"
        )
    if state["shadow_present"]:
        _mysql_alter(
            "capability_invocations",
            [f"DROP CHECK {_MYSQL_SHADOW_IDENTITY_CHECK}"],
        )

    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    if state["gate_present"]:
        if state["canonical"] != "new":
            raise RuntimeError(
                "terminal arbitration MySQL cannot release a stale downgrade "
                "gate before canonical v2 identity is enforced"
            )
        _mysql_alter(
            "capability_invocations",
            [f"DROP CHECK {_MYSQL_DOWNGRADE_GATE}"],
        )

    final = _mysql_capability_state()
    _assert_mysql_guarded(final)
    final_columns = final["columns"]
    assert isinstance(final_columns, set)
    if (
        final["canonical"] != "new"
        or final["shadow_present"]
        or final["gate_present"]
        or final["unique"] is not True
        or not set(_MYSQL_NEW_COLUMNS) <= final_columns
    ):
        raise RuntimeError(
            "terminal arbitration MySQL capability upgrade is incomplete"
        )


def _mysql_upgrade_auxiliary_online() -> None:
    state = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(state)
    if state["task_source_present"] is not True:
        _mysql_alter(
            "tasks",
            ["ADD COLUMN turn_source_log_id INTEGER NULL"],
        )

    state = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(state)
    log_actions: list[str] = []
    if state["turn_scope_present"] is not True:
        log_actions.append("ADD COLUMN turn_scope VARCHAR(16) NULL")
        log_actions.append(
            "ADD CONSTRAINT ck_log_entries_turn_scope "
            f"CHECK ({_TURN_SCOPE_CHECK})"
        )
        log_actions.append("ADD COLUMN actual_transport VARCHAR(24) NULL")
        log_actions.append(
            "ADD CONSTRAINT ck_log_entries_actual_transport "
            f"CHECK ({_ACTUAL_TRANSPORT_CHECK})"
        )
    _mysql_alter("log_entries", log_actions)

    # A crash-replayed upgrade may encounter temporary gates installed by a
    # downgrade that did not reach its Alembic stamp.  Release them only after
    # the complete v2 runtime schema and its normal CHECKs are present.
    state = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(state)
    if state["log_gate_present"]:
        _mysql_alter(
            "log_entries",
            [f"DROP CHECK {_MYSQL_LOG_DOWNGRADE_GATE}"],
        )
    if state["task_gate_present"]:
        _mysql_alter(
            "tasks",
            [f"DROP CHECK {_MYSQL_TASK_DOWNGRADE_GATE}"],
        )

    final = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(final)
    if (
        final["task_source_present"] is not True
        or final["turn_scope_present"] is not True
        or final["actual_transport_present"] is not True
        or final["scope_check"] is not True
        or final["transport_check"] is not True
        or final["task_gate_present"]
        or final["log_gate_present"]
    ):
        raise RuntimeError(
            "terminal arbitration MySQL auxiliary upgrade is incomplete"
        )


def _mysql_upgrade_offline() -> None:
    raise RuntimeError(
        "terminal arbitration migration refuses MySQL offline SQL because "
        "its safety prerequisites cannot be proven"
    )


def _mysql_ensure_capability_downgrade_gate_online() -> None:
    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    if not _mysql_has_v2_audit_schema(state):
        if state["canonical"] != "old" or state["shadow_present"]:
            raise RuntimeError(
                "terminal arbitration MySQL has no v2 audit schema but has "
                "not restored the legacy identity CHECK"
            )
        return
    if state["gate"] is not True:
        # The preflight proves the table is empty of Agent requests. This ALTER
        # then validates that fact under MySQL's metadata lock and installs a
        # durable gate; a racing writer makes the statement fail atomically.
        _mysql_alter(
            "capability_invocations",
            [
                f"ADD CONSTRAINT {_MYSQL_DOWNGRADE_GATE} "
                "CHECK (source <> 'agent_request')"
            ],
        )
    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    if state["gate"] is not True:
        raise RuntimeError(
            "terminal arbitration MySQL downgrade gate was not installed"
        )


def _mysql_install_auxiliary_downgrade_gates_online() -> None:
    """Install durable gates before any Task/LogEntry provenance is removed."""

    state = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(state)
    if state["task_source_present"] and not state["task_gate_present"]:
        _mysql_alter(
            "tasks",
            [
                f"ADD CONSTRAINT {_MYSQL_TASK_DOWNGRADE_GATE} "
                f"CHECK ({_TASK_DOWNGRADE_GATE_CHECK})"
            ],
        )
    state = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(state)
    if state["task_source_present"] and state["task_gate"] is not True:
        raise RuntimeError(
            "terminal arbitration MySQL Task downgrade gate was not installed"
        )

    if state["turn_scope_present"] and not state["log_gate_present"]:
        _mysql_alter(
            "log_entries",
            [
                f"ADD CONSTRAINT {_MYSQL_LOG_DOWNGRADE_GATE} "
                f"CHECK ({_LOG_DOWNGRADE_GATE_CHECK})"
            ],
        )
    final = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(final)
    if (
        final["task_source_present"]
        and final["task_gate"] is not True
    ) or (
        final["turn_scope_present"]
        and final["log_gate"] is not True
    ):
        raise RuntimeError(
            "terminal arbitration MySQL auxiliary downgrade gates are incomplete"
        )


def _mysql_downgrade_capability_online() -> None:
    _mysql_ensure_capability_downgrade_gate_online()
    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    if not _mysql_has_v2_audit_schema(state):
        if state["unique"] is True:
            _mysql_alter(
                "capability_invocations",
                ["DROP INDEX uq_cap_inv_task_output_log"],
            )
        return

    if state["canonical"] != "old":
        actions = []
        if state["canonical_present"]:
            actions.append("DROP CHECK ck_cap_inv_agent_request_identity")
        actions.append(
            "ADD CONSTRAINT ck_cap_inv_agent_request_identity "
            f"CHECK ({_OLD_AGENT_REQUEST_IDENTITY})"
        )
        _mysql_alter("capability_invocations", actions)

    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    if state["canonical"] != "old" or state["gate"] is not True:
        raise RuntimeError(
            "terminal arbitration MySQL legacy canonical CHECK is incomplete"
        )
    if state["shadow_present"]:
        _mysql_alter(
            "capability_invocations",
            [f"DROP CHECK {_MYSQL_SHADOW_IDENTITY_CHECK}"],
        )
        state = _mysql_capability_state()
        _assert_mysql_guarded(state)

    columns = state["columns"]
    assert isinstance(columns, set)
    removal: list[str] = []
    if state["unique"] is True:
        removal.append("DROP INDEX uq_cap_inv_task_output_log")
    for column in reversed(_MYSQL_NEW_COLUMNS):
        if column in columns:
            removal.append(f"DROP COLUMN {column}")
    _mysql_alter("capability_invocations", removal)

    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    columns = state["columns"]
    assert isinstance(columns, set)
    if (
        state["canonical"] != "old"
        or state["gate"] is not True
        or state["unique"] is True
        or set(_MYSQL_NEW_COLUMNS) & columns
    ):
        raise RuntimeError(
            "terminal arbitration MySQL capability downgrade is incomplete"
        )


def _mysql_downgrade_auxiliary_online() -> None:
    state = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(state)
    log_actions: list[str] = []
    if state["turn_scope_present"]:
        if state["log_gate"] is not True:
            raise RuntimeError(
                "terminal arbitration MySQL cannot remove LogEntry provenance "
                "without its enforced downgrade gate"
            )
        log_actions.append("DROP CHECK ck_log_entries_actual_transport")
        log_actions.append("DROP CHECK ck_log_entries_turn_scope")
        log_actions.append(f"DROP CHECK {_MYSQL_LOG_DOWNGRADE_GATE}")
        log_actions.append("DROP COLUMN actual_transport")
        log_actions.append("DROP COLUMN turn_scope")
    _mysql_alter("log_entries", log_actions)

    state = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(state)
    if state["task_source_present"]:
        if state["task_gate"] is not True:
            raise RuntimeError(
                "terminal arbitration MySQL cannot remove Task provenance "
                "without its enforced downgrade gate"
            )
        _mysql_alter(
            "tasks",
            [
                f"DROP CHECK {_MYSQL_TASK_DOWNGRADE_GATE}",
                "DROP COLUMN turn_source_log_id",
            ],
        )

    final = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(final)
    if (
        final["task_source_present"]
        or final["turn_scope_present"]
        or final["actual_transport_present"]
        or final["task_gate_present"]
        or final["log_gate_present"]
    ):
        raise RuntimeError(
            "terminal arbitration MySQL auxiliary downgrade is incomplete"
        )


def _mysql_finish_downgrade_online() -> None:
    auxiliary = _mysql_auxiliary_state()
    _assert_mysql_auxiliary_state(auxiliary)
    if (
        auxiliary["task_source_present"]
        or auxiliary["turn_scope_present"]
        or auxiliary["actual_transport_present"]
        or auxiliary["task_gate_present"]
        or auxiliary["log_gate_present"]
    ):
        raise RuntimeError(
            "terminal arbitration MySQL cannot release its capability gate "
            "before auxiliary downgrade settles"
        )
    state = _mysql_capability_state()
    _assert_mysql_guarded(state)
    columns = state["columns"]
    assert isinstance(columns, set)
    if (
        state["canonical"] != "old"
        or state["gate"] is not True
        or state["unique"] is True
        or set(_MYSQL_NEW_COLUMNS) & columns
    ):
        raise RuntimeError(
            "terminal arbitration MySQL cannot release its downgrade gate"
        )
    _mysql_alter(
        "capability_invocations",
        [f"DROP CHECK {_MYSQL_DOWNGRADE_GATE}"],
    )
    final = _mysql_capability_state()
    _assert_mysql_guarded(final)
    if final["canonical"] != "old" or final["gate_present"]:
        raise RuntimeError(
            "terminal arbitration MySQL downgrade gate did not settle"
        )


def _mysql_downgrade_offline() -> None:
    raise RuntimeError(
        "terminal arbitration migration refuses MySQL offline SQL because "
        "its safety prerequisites cannot be proven"
    )


def _upgrade_transactional() -> None:
    task_id_highwater = _sqlite_task_id_highwater()
    with op.batch_alter_table(
        "tasks",
        schema=None,
        **_task_batch_kwargs(),
    ) as batch_op:
        batch_op.add_column(
            sa.Column("turn_source_log_id", sa.Integer(), nullable=True)
        )
    _restore_sqlite_task_id_highwater(task_id_highwater)

    with op.batch_alter_table("log_entries", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("turn_scope", sa.String(length=16), nullable=True)
        )
        batch_op.add_column(
            sa.Column("actual_transport", sa.String(length=24), nullable=True)
        )
        batch_op.create_check_constraint(
            "ck_log_entries_turn_scope",
            _TURN_SCOPE_CHECK,
        )
        batch_op.create_check_constraint(
            "ck_log_entries_actual_transport",
            _ACTUAL_TRANSPORT_CHECK,
        )

    with op.batch_alter_table(
        "capability_invocations",
        schema=None,
    ) as batch_op:
        batch_op.add_column(
            sa.Column("request_reason", sa.Text(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("request_protocol_version", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "request_output_hash",
                sa.String(length=64),
                nullable=True,
            )
        )
        batch_op.create_unique_constraint(
            "uq_cap_inv_task_output_log",
            ["task_id", "request_output_log_id"],
        )
        batch_op.drop_constraint(
            "ck_cap_inv_agent_request_identity",
            type_="check",
        )
        batch_op.create_check_constraint(
            "ck_cap_inv_agent_request_identity",
            _NEW_AGENT_REQUEST_IDENTITY,
        )

    _ensure_worker_task_termination_table()


def upgrade() -> None:
    _acquire_preflight_fence(expected_revision=down_revision)
    _require_supported_mysql()
    if not _is_offline():
        # A MySQL 8 CREATE TABLE is atomic, but the following indexes are
        # separate implicit-commit DDL. Validate a crash remainder before
        # touching any of the older tables.
        _worker_task_termination_state()
    mysql_state = None
    if _dialect_name() == "mysql" and not _is_offline():
        mysql_state = _mysql_capability_state()
        _assert_mysql_guarded(mysql_state)
    _assert_upgrade_preconditions(
        require_zero_agent=(
            mysql_state is None
            or not _mysql_has_v2_identity_guard(mysql_state)
        )
    )
    if _dialect_name() == "mysql":
        if _is_offline():
            _mysql_upgrade_offline()
        else:
            _mysql_upgrade_capability_online()
            _mysql_upgrade_auxiliary_online()
            _ensure_worker_task_termination_table()
        return
    _upgrade_transactional()


def _downgrade_transactional() -> None:
    _drop_worker_task_termination_table_transactional()

    with op.batch_alter_table(
        "capability_invocations",
        schema=None,
    ) as batch_op:
        batch_op.drop_constraint(
            "ck_cap_inv_agent_request_identity",
            type_="check",
        )
        batch_op.create_check_constraint(
            "ck_cap_inv_agent_request_identity",
            _OLD_AGENT_REQUEST_IDENTITY,
        )
        batch_op.drop_constraint(
            "uq_cap_inv_task_output_log",
            type_="unique",
        )
        batch_op.drop_column("request_output_hash")
        batch_op.drop_column("request_protocol_version")
        batch_op.drop_column("request_reason")

    with op.batch_alter_table("log_entries", schema=None) as batch_op:
        batch_op.drop_constraint(
            "ck_log_entries_actual_transport",
            type_="check",
        )
        batch_op.drop_constraint(
            "ck_log_entries_turn_scope",
            type_="check",
        )
        batch_op.drop_column("actual_transport")
        batch_op.drop_column("turn_scope")

    task_id_highwater = _sqlite_task_id_highwater()
    with op.batch_alter_table(
        "tasks",
        schema=None,
        **_task_batch_kwargs(),
    ) as batch_op:
        batch_op.drop_column("turn_source_log_id")
    _restore_sqlite_task_id_highwater(task_id_highwater)


def downgrade() -> None:
    _acquire_preflight_fence(
        expected_revision=revision,
        include_worker_terminations=True,
    )
    _require_supported_mysql()
    mysql_state = None
    mysql_auxiliary_state = None
    worker_termination_state = None
    if not _is_offline():
        worker_termination_state = _worker_task_termination_state(
            allow_downgrade_gate=(_dialect_name() == "mysql")
        )
    if _dialect_name() == "mysql" and not _is_offline():
        mysql_state = _mysql_capability_state()
        _assert_mysql_guarded(mysql_state)
        mysql_auxiliary_state = _mysql_auxiliary_state()
        _assert_mysql_auxiliary_state(mysql_auxiliary_state)
    _assert_downgrade_preconditions(
        require_zero_agent=(
            mysql_state is None
            or _mysql_has_v2_audit_schema(mysql_state)
        ),
        require_zero_task_source=(
            mysql_auxiliary_state is None
            or mysql_auxiliary_state["task_source_present"] is True
        ),
        require_zero_log_provenance=(
            mysql_auxiliary_state is None
            or mysql_auxiliary_state["turn_scope_present"] is True
        ),
        require_zero_worker_terminations=(
            worker_termination_state is None
            or worker_termination_state["present"] is True
        ),
    )
    if _dialect_name() == "mysql":
        if _is_offline():
            _mysql_downgrade_offline()
        else:
            _mysql_install_worker_task_termination_downgrade_gate()
            _mysql_ensure_capability_downgrade_gate_online()
            _mysql_install_auxiliary_downgrade_gates_online()
            _mysql_drop_worker_task_termination_table()
            _mysql_downgrade_capability_online()
            _mysql_downgrade_auxiliary_online()
            settled = _mysql_capability_state()
            _assert_mysql_guarded(settled)
            if settled["gate_present"]:
                _mysql_finish_downgrade_online()
        return
    _downgrade_transactional()
