"""add durable capability resume outbox

Revision ID: 7c1e4a9d2f60
Revises: 4b8d2f6a1c90
Create Date: 2026-08-07
"""

import re
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "7c1e4a9d2f60"
down_revision: Union[str, None] = "4b8d2f6a1c90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_OUTBOX_TABLE = "capability_resume_outbox"
_MYSQL_CAPABILITY_GATE = "ck_cap_inv_no_resume_outbox_downgrade"
_MYSQL_OUTBOX_GATE = "ck_cap_resume_outbox_no_downgrade_rows"
_MYSQL_OUTBOX_CHECK_PROBE = "_ccm_cap_resume_outbox_check_probe"

_OLD_AGENT_REQUEST_IDENTITY = (
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

_NEW_AGENT_REQUEST_IDENTITY = (
    "source <> 'agent_request' OR ("
    "purpose = 'advisory' AND resume_policy = 'resume_task' "
    "AND requested_by_user_id IS NULL "
    "AND request_task_incarnation_id IS NOT NULL "
    "AND LENGTH(request_task_incarnation_id) = 32 "
    "AND request_task_retry_count IS NOT NULL "
    "AND request_task_retry_count >= 0 "
    "AND request_task_turn_generation IS NOT NULL "
    "AND request_task_turn_generation >= 0 "
    "AND request_source_log_id IS NOT NULL "
    "AND request_source_log_id > 0 "
    "AND request_output_log_id IS NOT NULL "
    "AND request_output_log_id > 0 "
    "AND request_terminal_log_id IS NOT NULL "
    "AND request_terminal_log_id > 0 "
    "AND request_reason IS NOT NULL "
    "AND request_protocol_version IS NOT NULL "
    "AND request_protocol_version >= 1 "
    "AND request_output_hash IS NOT NULL "
    "AND LENGTH(request_output_hash) = 64)"
)

_OUTBOX_CHECKS = {
    "ck_cap_resume_outbox_status": (
        "status IN ('pending', 'ready', 'claiming', 'claimed', 'launched', "
        "'completed', 'cancelled', 'failed')"
    ),
    "ck_cap_resume_outbox_counters": (
        "state_version >= 1 AND attempt_count >= 0"
    ),
    "ck_cap_resume_outbox_request_identity": (
        "LENGTH(request_task_incarnation_id) = 32 "
        "AND request_task_retry_count >= 0 "
        "AND from_turn_generation >= 0 "
        "AND request_source_log_id > 0 "
        "AND request_output_log_id > 0 "
        "AND request_terminal_log_id > 0 "
        "AND (request_native_turn_id IS NULL "
        "OR LENGTH(request_native_turn_id) > 0)"
    ),
    "ck_cap_resume_outbox_active_slot": (
        "(status IN ('pending', 'ready', 'claiming', 'claimed') "
        "AND active_task_id IS NOT NULL "
        "AND active_task_id = task_id "
        "AND active_invocation_id IS NOT NULL "
        "AND active_invocation_id = invocation_id) OR "
        "(status NOT IN ('pending', 'ready', 'claiming', 'claimed') "
        "AND active_task_id IS NULL "
        "AND active_invocation_id IS NULL)"
    ),
    "ck_cap_resume_outbox_inv_result": (
        "(invocation_terminal_status IS NULL "
        "AND invocation_result_kind IS NULL "
        "AND invocation_result_id IS NULL "
        "AND invocation_result_hash IS NULL "
        "AND invocation_error_code IS NULL "
        "AND invocation_error_message IS NULL) OR "
        "(invocation_terminal_status IN "
        "('completed', 'failed', 'cancelled', 'stale') AND "
        "((invocation_terminal_status = 'completed' "
        "AND invocation_result_kind IS NOT NULL "
        "AND invocation_result_id IS NOT NULL "
        "AND invocation_result_hash IS NOT NULL "
        "AND LENGTH(invocation_result_hash) = 64) OR "
        "(invocation_terminal_status <> 'completed' "
        "AND invocation_result_kind IS NULL "
        "AND invocation_result_id IS NULL "
        "AND invocation_result_hash IS NULL)))"
    ),
    "ck_cap_resume_outbox_payload": (
        "(resume_payload IS NULL AND resume_payload_hash IS NULL) OR "
        "(resume_payload IS NOT NULL "
        "AND resume_payload_hash IS NOT NULL "
        "AND LENGTH(resume_payload_hash) = 64)"
    ),
    "ck_cap_resume_outbox_status_payload": (
        "(status = 'pending' "
        "AND invocation_terminal_status IS NULL "
        "AND resume_payload IS NULL) OR "
        "(status IN ('ready', 'claiming', 'claimed', 'launched', "
        "'completed') "
        "AND invocation_terminal_status IS NOT NULL "
        "AND resume_payload IS NOT NULL) OR "
        "status IN ('cancelled', 'failed')"
    ),
    "ck_cap_resume_outbox_claim": (
        "(status IN ('claimed', 'launched', 'completed') "
        "AND resume_source_log_id IS NOT NULL "
        "AND resume_source_log_id > 0 "
        "AND claimed_turn_generation = from_turn_generation + 1 "
        "AND claimed_at IS NOT NULL) OR "
        "(status IN ('pending', 'ready', 'claiming') "
        "AND resume_source_log_id IS NULL "
        "AND claimed_turn_generation IS NULL "
        "AND claimed_at IS NULL) OR "
        "(status IN ('cancelled', 'failed') AND "
        "((resume_source_log_id IS NULL "
        "AND claimed_turn_generation IS NULL "
        "AND claimed_at IS NULL) OR "
        "(resume_source_log_id IS NOT NULL "
        "AND resume_source_log_id > 0 "
        "AND claimed_turn_generation = from_turn_generation + 1 "
        "AND claimed_at IS NOT NULL)))"
    ),
    "ck_cap_resume_outbox_lease": (
        "(status = 'claiming' "
        "AND lease_token IS NOT NULL "
        "AND LENGTH(lease_token) = 64 "
        "AND lease_expires_at IS NOT NULL "
        "AND attempt_count >= 1) OR "
        "(status = 'claimed' AND attempt_count >= 1 AND "
        "((lease_token IS NULL AND lease_expires_at IS NULL) OR "
        "(lease_token IS NOT NULL "
        "AND LENGTH(lease_token) = 64 "
        "AND lease_expires_at IS NOT NULL))) OR "
        "(status NOT IN ('claiming', 'claimed') "
        "AND lease_token IS NULL "
        "AND lease_expires_at IS NULL)"
    ),
    "ck_cap_resume_outbox_backoff": (
        "status IN ('ready', 'claimed') OR next_attempt_at IS NULL"
    ),
    "ck_cap_resume_outbox_error": (
        "((error_code IS NULL AND error_message IS NULL) OR "
        "(error_code IS NOT NULL AND LENGTH(error_code) > 0 "
        "AND error_message IS NOT NULL)) AND "
        "(status NOT IN ('cancelled', 'failed') OR "
        "error_code IS NOT NULL)"
    ),
    "ck_cap_resume_outbox_transport": (
        "(status IN ('launched', 'completed') "
        "AND resume_actual_transport IN "
        "('claude_pty', 'claude_exec', 'codex_app_server', "
        "'codex_exec') "
        "AND launched_at IS NOT NULL) OR "
        "(status IN ('pending', 'ready', 'claiming', 'claimed') "
        "AND resume_actual_transport IS NULL "
        "AND launched_at IS NULL) OR "
        "(status IN ('cancelled', 'failed') AND "
        "((resume_actual_transport IS NULL AND launched_at IS NULL) OR "
        "(resume_actual_transport IN "
        "('claude_pty', 'claude_exec', 'codex_app_server', "
        "'codex_exec') AND launched_at IS NOT NULL "
        "AND resume_source_log_id IS NOT NULL)))"
    ),
    "ck_cap_resume_outbox_timeline": (
        "((status = 'pending' AND ready_at IS NULL) OR "
        "(status IN ('ready', 'claiming', 'claimed', 'launched', "
        "'completed') "
        "AND ready_at IS NOT NULL) OR "
        "status IN ('cancelled', 'failed')) AND "
        "((status IN ('pending', 'ready', 'claiming', 'claimed', "
        "'launched') "
        "AND completed_at IS NULL) OR "
        "(status IN ('completed', 'cancelled', 'failed') "
        "AND completed_at IS NOT NULL)) AND "
        "(ready_at IS NULL OR ready_at >= created_at) AND "
        "(claimed_at IS NULL OR "
        "(ready_at IS NOT NULL AND claimed_at >= ready_at)) AND "
        "(launched_at IS NULL OR "
        "(claimed_at IS NOT NULL AND launched_at >= claimed_at)) AND "
        "(completed_at IS NULL OR completed_at >= created_at)"
    ),
}

_OUTBOX_INDEXES = {
    "ix_cap_resume_outbox_due": ("status", "next_attempt_at"),
    "ix_cap_resume_outbox_task_created": ("task_id", "created_at"),
}

_OUTBOX_UNIQUES = {
    "uq_cap_resume_outbox_invocation": ("invocation_id",),
    "uq_cap_resume_outbox_task_generation": (
        "task_id",
        "request_task_incarnation_id",
        "request_task_retry_count",
        "from_turn_generation",
    ),
    "uq_cap_resume_outbox_active_task": ("active_task_id",),
    "uq_cap_resume_outbox_active_inv": ("active_invocation_id",),
}


def _outbox_columns() -> list[sa.Column]:
    """Return fresh columns for replayable CREATE TABLE operations."""

    return [
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("invocation_id", sa.Integer(), nullable=False),
        sa.Column("active_task_id", sa.Integer(), nullable=True),
        sa.Column("active_invocation_id", sa.Integer(), nullable=True),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="pending",
            nullable=False,
        ),
        sa.Column(
            "state_version",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
        sa.Column(
            "request_task_incarnation_id",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column("request_task_retry_count", sa.Integer(), nullable=False),
        sa.Column("from_turn_generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "request_task_session_id",
            sa.String(length=200),
            nullable=True,
        ),
        sa.Column("request_source_log_id", sa.Integer(), nullable=False),
        sa.Column("request_output_log_id", sa.Integer(), nullable=False),
        sa.Column("request_terminal_log_id", sa.Integer(), nullable=False),
        sa.Column(
            "request_native_turn_id",
            sa.String(length=200),
            nullable=True,
        ),
        sa.Column(
            "invocation_terminal_status",
            sa.String(length=24),
            nullable=True,
        ),
        sa.Column(
            "invocation_result_kind",
            sa.String(length=32),
            nullable=True,
        ),
        sa.Column("invocation_result_id", sa.Integer(), nullable=True),
        sa.Column(
            "invocation_result_hash",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "invocation_error_code",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column("invocation_error_message", sa.Text(), nullable=True),
        sa.Column("resume_payload", sa.JSON(), nullable=True),
        sa.Column(
            "resume_payload_hash",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column("resume_source_log_id", sa.Integer(), nullable=True),
        sa.Column("claimed_turn_generation", sa.BigInteger(), nullable=True),
        sa.Column(
            "resume_actual_transport",
            sa.String(length=24),
            nullable=True,
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("next_attempt_at", sa.DateTime(), nullable=True),
        sa.Column("lease_token", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("ready_at", sa.DateTime(), nullable=True),
        sa.Column("claimed_at", sa.DateTime(), nullable=True),
        sa.Column("launched_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
    ]


def _create_outbox_table() -> None:
    op.create_table(
        _OUTBOX_TABLE,
        *_outbox_columns(),
        *(sa.CheckConstraint(sql, name=name) for name, sql in _OUTBOX_CHECKS.items()),
        sa.ForeignKeyConstraint(
            ["task_id"], ["tasks.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["invocation_id"],
            ["capability_invocations.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        *(
            sa.UniqueConstraint(*columns, name=name)
            for name, columns in _OUTBOX_UNIQUES.items()
        ),
        mysql_engine="InnoDB",
    )
    for name, columns in _OUTBOX_INDEXES.items():
        op.create_index(name, _OUTBOX_TABLE, list(columns), unique=False)


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _dialect_name() -> str:
    return op.get_bind().dialect.name


def _acquire_transactional_fence(*, downgrade: bool) -> None:
    dialect = _dialect_name()
    if dialect == "postgresql":
        tables = "capability_invocations"
        if downgrade:
            tables += f", {_OUTBOX_TABLE}"
        op.execute(sa.text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"))
        return
    if dialect != "sqlite" or _is_offline():
        return
    expected_revision = revision if downgrade else down_revision
    fenced = op.get_bind().execute(
        sa.text(
            "UPDATE alembic_version SET version_num = version_num "
            "WHERE version_num = :expected_revision"
        ),
        {"expected_revision": expected_revision},
    )
    if fenced.rowcount != 1:
        raise RuntimeError(
            "capability resume outbox migration could not acquire its "
            "SQLite revision writer fence"
        )


def _assert_zero_agent_requests() -> None:
    if _is_offline():
        if _dialect_name() == "postgresql":
            op.execute(
                sa.text(
                    """
DO $ccm_capability_resume_outbox_upgrade$
BEGIN
    IF EXISTS (
        SELECT 1 FROM capability_invocations
        WHERE source = 'agent_request'
    ) THEN
        RAISE EXCEPTION
            'capability resume outbox upgrade refused: exact identities cannot be reconstructed';
    END IF;
END
$ccm_capability_resume_outbox_upgrade$
"""
                )
            )
        return
    count = op.get_bind().execute(
        sa.text(
            "SELECT COUNT(*) FROM capability_invocations "
            "WHERE source = 'agent_request'"
        )
    ).scalar_one()
    if count:
        raise RuntimeError(
            "capability resume outbox migration requires zero agent_request "
            "invocations; exact incarnation/terminal identities cannot be "
            "reconstructed"
        )


def _assert_downgrade_empty(*, outbox_present: bool = True) -> None:
    if _is_offline():
        if _dialect_name() == "postgresql":
            op.execute(
                sa.text(
                    """
DO $ccm_capability_resume_outbox$
BEGIN
    IF EXISTS (SELECT 1 FROM capability_resume_outbox) THEN
        RAISE EXCEPTION
            'capability resume outbox downgrade refused: outbox history would be destroyed';
    END IF;
    IF EXISTS (
        SELECT 1 FROM capability_invocations
        WHERE source = 'agent_request'
    ) THEN
        RAISE EXCEPTION
            'capability resume outbox downgrade refused: agent request identity would be destroyed';
    END IF;
END
$ccm_capability_resume_outbox$
"""
                )
            )
        return
    bind = op.get_bind()
    if outbox_present:
        count = bind.execute(
            sa.text(f"SELECT COUNT(*) FROM {_OUTBOX_TABLE}")
        ).scalar_one()
        if count:
            raise RuntimeError(
                "capability resume outbox downgrade refused: durable resume "
                "history would be destroyed"
            )
    count = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM capability_invocations "
            "WHERE source = 'agent_request'"
        )
    ).scalar_one()
    if count:
        raise RuntimeError(
            "capability resume outbox downgrade refused: agent request exact "
            "identity would be destroyed"
        )


def _upgrade_transactional() -> None:
    with op.batch_alter_table("capability_invocations", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "request_task_incarnation_id",
                sa.String(length=32),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("request_terminal_log_id", sa.Integer(), nullable=True)
        )
        batch_op.create_unique_constraint(
            "uq_cap_inv_task_terminal_log",
            ["task_id", "request_terminal_log_id"],
        )
        batch_op.drop_constraint(
            "ck_cap_inv_agent_request_identity", type_="check"
        )
        batch_op.create_check_constraint(
            "ck_cap_inv_agent_request_identity",
            _NEW_AGENT_REQUEST_IDENTITY,
        )
    _create_outbox_table()


def _downgrade_transactional() -> None:
    for name in reversed(tuple(_OUTBOX_INDEXES)):
        op.drop_index(name, table_name=_OUTBOX_TABLE)
    op.drop_table(_OUTBOX_TABLE)
    with op.batch_alter_table("capability_invocations", schema=None) as batch_op:
        batch_op.drop_constraint(
            "ck_cap_inv_agent_request_identity", type_="check"
        )
        batch_op.create_check_constraint(
            "ck_cap_inv_agent_request_identity",
            _OLD_AGENT_REQUEST_IDENTITY,
        )
        batch_op.drop_constraint(
            "uq_cap_inv_task_terminal_log", type_="unique"
        )
        batch_op.drop_column("request_terminal_log_id")
        batch_op.drop_column("request_task_incarnation_id")


def _normalized_sql(sqltext: object) -> str:
    value = str(sqltext or "").strip().lower().replace("`", "")
    value = re.sub(r"_[a-z0-9]+\s*'", "'", value)
    return " ".join(value.split())


def _strip_outer_parentheses(expression: str) -> str:
    expression = expression.strip()
    while expression.startswith("(") and expression.endswith(")"):
        depth = 0
        quoted = False
        balanced = True
        for index, char in enumerate(expression):
            if char == "'":
                quoted = not quoted
            elif not quoted:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0 and index != len(expression) - 1:
                        balanced = False
                        break
        if not balanced or depth != 0:
            break
        expression = expression[1:-1].strip()
    return expression


def _split_top_level(expression: str, operator: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quoted = False
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            quoted = not quoted
            index += 1
            continue
        if not quoted:
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


def _check_shape(sqltext: object):
    expression = _normalized_sql(sqltext)
    if expression.startswith("check"):
        expression = expression[len("check") :].strip()
    expression = _strip_outer_parentheses(expression)
    or_parts = _split_top_level(expression, " or ")
    if len(or_parts) > 1:
        return ("or", tuple(_check_shape(part) for part in or_parts))
    and_parts = _split_top_level(expression, " and ")
    if len(and_parts) > 1:
        return ("and", tuple(_check_shape(part) for part in and_parts))
    return ("atom", "".join(expression.split()))


def _mysql_enforced_checks(table_name: str) -> set[str]:
    rows = op.get_bind().execute(
        sa.text(
            "SELECT CONSTRAINT_NAME, ENFORCED "
            "FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name "
            "AND CONSTRAINT_TYPE = 'CHECK'"
        ),
        {"table_name": table_name},
    )
    return {
        str(name).lower()
        for name, enforced in rows
        if str(enforced).upper() == "YES"
    }


def _require_supported_mysql() -> None:
    if _dialect_name() != "mysql":
        return
    if _is_offline():
        raise RuntimeError(
            "capability resume outbox migration refuses MySQL offline SQL: "
            "server version, CHECK enforcement, and InnoDB atomic DDL cannot "
            "be proven"
        )
    dialect = op.get_bind().dialect
    if getattr(dialect, "is_mariadb", False):
        raise RuntimeError(
            "capability resume outbox migration requires MySQL 8.0.16+"
        )
    version = getattr(dialect, "server_version_info", None)
    if not version or tuple(version[:3]) < (8, 0, 16):
        raise RuntimeError(
            "capability resume outbox migration requires MySQL 8.0.16+ "
            "with enforced CHECK constraints"
        )
    rows = op.get_bind().execute(
        sa.text(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN "
            "('tasks', 'capability_invocations')"
        )
    )
    engines = {str(name).lower(): str(engine).lower() for name, engine in rows}
    if engines != {"tasks": "innodb", "capability_invocations": "innodb"}:
        raise RuntimeError(
            "capability resume outbox migration requires tasks and "
            "capability_invocations to be InnoDB tables"
        )


def _mysql_capability_state() -> str:
    """Return old/new identity state; reject every partial or foreign shape."""

    inspector = sa.inspect(op.get_bind())
    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns("capability_invocations")
    }
    new_names = {"request_task_incarnation_id", "request_terminal_log_id"}
    present = new_names & set(columns)
    if present and present != new_names:
        raise RuntimeError(
            "capability resume outbox MySQL capability columns are partial"
        )
    if present:
        incarnation = columns["request_task_incarnation_id"]
        terminal = columns["request_terminal_log_id"]
        if (
            not isinstance(incarnation["type"], sa.String)
            or incarnation["type"].length != 32
            or not bool(incarnation["nullable"])
            or not isinstance(terminal["type"], sa.Integer)
            or not bool(terminal["nullable"])
        ):
            raise RuntimeError(
                "capability resume outbox MySQL capability columns are malformed"
            )

    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints("capability_invocations")
    }
    canonical = checks.get("ck_cap_inv_agent_request_identity")
    if canonical is None:
        raise RuntimeError(
            "capability resume outbox MySQL identity CHECK is missing"
        )
    shape = _check_shape(canonical)
    if shape == _check_shape(_OLD_AGENT_REQUEST_IDENTITY):
        state = "old"
    elif shape == _check_shape(_NEW_AGENT_REQUEST_IDENTITY):
        state = "new"
    else:
        raise RuntimeError(
            "capability resume outbox MySQL identity CHECK is malformed"
        )
    gate = checks.get(_MYSQL_CAPABILITY_GATE)
    if gate is not None and _check_shape(gate) != _check_shape(
        "source <> 'agent_request'"
    ):
        raise RuntimeError(
            "capability resume outbox MySQL downgrade gate is malformed"
        )
    enforced = _mysql_enforced_checks("capability_invocations")
    required = {"ck_cap_inv_agent_request_identity"}
    if gate is not None:
        required.add(_MYSQL_CAPABILITY_GATE)
    if not required <= enforced:
        raise RuntimeError(
            "capability resume outbox MySQL capability CHECK is not enforced"
        )

    uniques = {
        str(item.get("name") or "").lower(): tuple(
            str(column).lower() for column in (item.get("column_names") or ())
        )
        for item in inspector.get_unique_constraints("capability_invocations")
    }
    terminal_unique = uniques.get("uq_cap_inv_task_terminal_log")
    if terminal_unique is not None and terminal_unique != (
        "task_id",
        "request_terminal_log_id",
    ):
        raise RuntimeError(
            "capability resume outbox MySQL terminal-log UNIQUE is malformed"
        )
    if state == "old" and (present or terminal_unique is not None):
        raise RuntimeError(
            "capability resume outbox MySQL old identity has partial v3 schema"
        )
    if state == "new" and (
        present != new_names
        or terminal_unique != ("task_id", "request_terminal_log_id")
    ):
        raise RuntimeError(
            "capability resume outbox MySQL new identity schema is incomplete"
        )
    return state


def _mysql_capability_has_gate() -> bool:
    inspector = sa.inspect(op.get_bind())
    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints("capability_invocations")
    }
    gate = checks.get(_MYSQL_CAPABILITY_GATE)
    if gate is None:
        return False
    if _check_shape(gate) != _check_shape("source <> 'agent_request'"):
        raise RuntimeError(
            "capability resume outbox MySQL downgrade gate is malformed"
        )
    if _MYSQL_CAPABILITY_GATE not in _mysql_enforced_checks(
        "capability_invocations"
    ):
        raise RuntimeError(
            "capability resume outbox MySQL downgrade gate is not enforced"
        )
    return True


def _type_matches(
    reflected: sa.types.TypeEngine,
    expected: type[sa.types.TypeEngine],
    length: int | None,
) -> bool:
    if expected in {sa.Integer, sa.BigInteger}:
        expected_name = "INTEGER" if expected is sa.Integer else "BIGINT"
        return type(reflected).__name__.upper() == expected_name
    if not isinstance(reflected, expected):
        return False
    return length is None or getattr(reflected, "length", None) == length


_OUTBOX_COLUMN_SHAPES = {
    "id": (sa.Integer, None, False),
    "task_id": (sa.Integer, None, False),
    "invocation_id": (sa.Integer, None, False),
    "active_task_id": (sa.Integer, None, True),
    "active_invocation_id": (sa.Integer, None, True),
    "status": (sa.String, 24, False),
    "state_version": (sa.Integer, None, False),
    "request_task_incarnation_id": (sa.String, 32, False),
    "request_task_retry_count": (sa.Integer, None, False),
    "from_turn_generation": (sa.BigInteger, None, False),
    "request_task_session_id": (sa.String, 200, True),
    "request_source_log_id": (sa.Integer, None, False),
    "request_output_log_id": (sa.Integer, None, False),
    "request_terminal_log_id": (sa.Integer, None, False),
    "request_native_turn_id": (sa.String, 200, True),
    "invocation_terminal_status": (sa.String, 24, True),
    "invocation_result_kind": (sa.String, 32, True),
    "invocation_result_id": (sa.Integer, None, True),
    "invocation_result_hash": (sa.String, 64, True),
    "invocation_error_code": (sa.String, 64, True),
    "invocation_error_message": (sa.Text, None, True),
    "resume_payload": (sa.JSON, None, True),
    "resume_payload_hash": (sa.String, 64, True),
    "resume_source_log_id": (sa.Integer, None, True),
    "claimed_turn_generation": (sa.BigInteger, None, True),
    "resume_actual_transport": (sa.String, 24, True),
    "attempt_count": (sa.Integer, None, False),
    "next_attempt_at": (sa.DateTime, None, True),
    "lease_token": (sa.String, 64, True),
    "lease_expires_at": (sa.DateTime, None, True),
    "error_code": (sa.String, 64, True),
    "error_message": (sa.Text, None, True),
    "created_at": (sa.DateTime, None, False),
    "updated_at": (sa.DateTime, None, False),
    "ready_at": (sa.DateTime, None, True),
    "claimed_at": (sa.DateTime, None, True),
    "launched_at": (sa.DateTime, None, True),
    "completed_at": (sa.DateTime, None, True),
}


def _mysql_outbox_indexes() -> dict[str, tuple[str, ...]]:
    inspector = sa.inspect(op.get_bind())
    indexes: dict[str, tuple[str, ...]] = {}
    for item in inspector.get_indexes(_OUTBOX_TABLE):
        name = str(item.get("name") or "").lower()
        if name not in _OUTBOX_INDEXES:
            continue
        columns = tuple(
            str(column).lower() for column in (item.get("column_names") or ())
        )
        if columns != _OUTBOX_INDEXES[name] or bool(item.get("unique")):
            raise RuntimeError(
                f"capability resume outbox MySQL index {name} is malformed"
            )
        indexes[name] = columns
    return indexes


def _mysql_canonical_outbox_checks() -> dict[str, object]:
    """Have MySQL canonicalize expected CHECK expressions for comparison."""

    bind = op.get_bind()
    probe = sa.Table(
        _MYSQL_OUTBOX_CHECK_PROBE,
        sa.MetaData(),
        *_outbox_columns(),
        *(
            sa.CheckConstraint(sql, name=name)
            for name, sql in _OUTBOX_CHECKS.items()
        ),
        prefixes=["TEMPORARY"],
        mysql_engine="InnoDB",
    )
    bind.execute(sa.schema.CreateTable(probe))
    try:
        return {
            str(item.get("name") or "").lower(): item.get("sqltext")
            for item in sa.inspect(bind).get_check_constraints(
                _MYSQL_OUTBOX_CHECK_PROBE
            )
        }
    finally:
        bind.execute(sa.schema.DropTable(probe))


def _mysql_outbox_state(
    *,
    allow_gate: bool = False,
    require_gate: bool = False,
) -> bool:
    if require_gate and not allow_gate:
        raise RuntimeError(
            "capability resume outbox MySQL row gate cannot be required "
            "without allowing downgrade state"
        )
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_OUTBOX_TABLE):
        if require_gate:
            raise RuntimeError(
                "capability resume outbox MySQL row gate is missing"
            )
        return False
    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_OUTBOX_TABLE)
    }
    if set(columns) != set(_OUTBOX_COLUMN_SHAPES):
        raise RuntimeError(
            "capability resume outbox MySQL table has a partial column set"
        )
    for name, (expected, length, nullable) in _OUTBOX_COLUMN_SHAPES.items():
        column = columns[name]
        if not _type_matches(column["type"], expected, length) or (
            bool(column["nullable"]) is not nullable
        ):
            raise RuntimeError(
                f"capability resume outbox MySQL column {name} is malformed"
            )
    if tuple(
        str(column).lower()
        for column in (
            inspector.get_pk_constraint(_OUTBOX_TABLE).get(
                "constrained_columns"
            )
            or ()
        )
    ) != ("id",):
        raise RuntimeError(
            "capability resume outbox MySQL primary key is malformed"
        )
    uniques = {
        str(item.get("name") or "").lower(): tuple(
            str(column).lower() for column in (item.get("column_names") or ())
        )
        for item in inspector.get_unique_constraints(_OUTBOX_TABLE)
    }
    if uniques != _OUTBOX_UNIQUES:
        raise RuntimeError(
            "capability resume outbox MySQL UNIQUE set is malformed"
        )
    foreign_keys = {
        (
            tuple(str(c).lower() for c in item.get("constrained_columns") or ()),
            str(item.get("referred_table") or "").lower(),
            tuple(str(c).lower() for c in item.get("referred_columns") or ()),
            str((item.get("options") or {}).get("ondelete") or "").upper(),
        )
        for item in inspector.get_foreign_keys(_OUTBOX_TABLE)
    }
    if foreign_keys != {
        (("task_id",), "tasks", ("id",), "CASCADE"),
        (
            ("invocation_id",),
            "capability_invocations",
            ("id",),
            "CASCADE",
        ),
    }:
        raise RuntimeError(
            "capability resume outbox MySQL foreign keys are malformed"
        )
    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints(_OUTBOX_TABLE)
    }
    expected_names = set(_OUTBOX_CHECKS)
    allowed_names = set(expected_names)
    if allow_gate:
        allowed_names.add(_MYSQL_OUTBOX_GATE)
    if set(checks) not in (expected_names, allowed_names):
        raise RuntimeError(
            "capability resume outbox MySQL CHECK set is malformed"
        )
    canonical_checks = _mysql_canonical_outbox_checks()
    if set(canonical_checks) != expected_names:
        raise RuntimeError(
            "capability resume outbox MySQL canonical CHECK probe is malformed"
        )
    for name, expected in canonical_checks.items():
        if _check_shape(checks[name]) != _check_shape(expected):
            raise RuntimeError(
                f"capability resume outbox MySQL CHECK {name} is malformed"
            )
    if _MYSQL_OUTBOX_GATE in checks and _check_shape(
        checks[_MYSQL_OUTBOX_GATE]
    ) != _check_shape("id IS NULL"):
        raise RuntimeError(
            "capability resume outbox MySQL row gate is malformed"
        )
    if require_gate and _MYSQL_OUTBOX_GATE not in checks:
        raise RuntimeError(
            "capability resume outbox MySQL row gate is missing"
        )
    enforced = _mysql_enforced_checks(_OUTBOX_TABLE)
    if not set(checks) <= enforced:
        raise RuntimeError(
            "capability resume outbox MySQL CHECK is not enforced"
        )
    _mysql_outbox_indexes()
    engines = op.get_bind().execute(
        sa.text(
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
        ),
        {"table_name": _OUTBOX_TABLE},
    ).scalars().all()
    if len(engines) != 1 or str(engines[0]).lower() != "innodb":
        raise RuntimeError(
            "capability resume outbox MySQL table must use InnoDB"
        )
    return True


def _mysql_outbox_has_gate() -> bool:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_OUTBOX_TABLE):
        return False
    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints(_OUTBOX_TABLE)
    }
    gate = checks.get(_MYSQL_OUTBOX_GATE)
    if gate is None:
        return False
    if _check_shape(gate) != _check_shape("id IS NULL"):
        raise RuntimeError(
            "capability resume outbox MySQL row gate is malformed"
        )
    if _MYSQL_OUTBOX_GATE not in _mysql_enforced_checks(_OUTBOX_TABLE):
        raise RuntimeError(
            "capability resume outbox MySQL row gate is not enforced"
        )
    return True


def _ensure_mysql_outbox_indexes() -> None:
    indexes = _mysql_outbox_indexes()
    for name, columns in _OUTBOX_INDEXES.items():
        if name not in indexes:
            op.create_index(name, _OUTBOX_TABLE, list(columns), unique=False)
    remaining = set(_OUTBOX_INDEXES) - set(_mysql_outbox_indexes())
    if remaining:
        raise RuntimeError(
            "capability resume outbox MySQL index creation is incomplete: "
            + ", ".join(sorted(remaining))
        )


def _mysql_alter_capability(actions: list[str]) -> None:
    if actions:
        op.execute(
            sa.text(
                "ALTER TABLE capability_invocations\n  "
                + ",\n  ".join(actions)
            )
        )


def _upgrade_mysql() -> None:
    state = _mysql_capability_state()
    outbox_present = _mysql_outbox_state()
    if state == "old":
        if outbox_present:
            raise RuntimeError(
                "capability resume outbox MySQL old identity unexpectedly "
                "has an outbox table"
            )
        _assert_zero_agent_requests()
        _mysql_alter_capability(
            [
                "ADD COLUMN request_task_incarnation_id VARCHAR(32) NULL",
                "ADD COLUMN request_terminal_log_id INTEGER NULL",
                "ADD CONSTRAINT uq_cap_inv_task_terminal_log "
                "UNIQUE (task_id, request_terminal_log_id)",
                "DROP CHECK ck_cap_inv_agent_request_identity",
                "ADD CONSTRAINT ck_cap_inv_agent_request_identity "
                f"CHECK ({_NEW_AGENT_REQUEST_IDENTITY})",
            ]
        )
    if _mysql_capability_state() != "new":
        raise RuntimeError(
            "capability resume outbox MySQL capability upgrade is incomplete"
        )
    if not outbox_present:
        _create_outbox_table()
    if not _mysql_outbox_state():
        raise RuntimeError(
            "capability resume outbox MySQL table creation is incomplete"
        )
    _ensure_mysql_outbox_indexes()


def _downgrade_mysql() -> None:
    state = _mysql_capability_state()
    outbox_present = _mysql_outbox_state(allow_gate=True)
    capability_gate_present = _mysql_capability_has_gate()
    if state == "old":
        if outbox_present or capability_gate_present:
            raise RuntimeError(
                "capability resume outbox MySQL old identity unexpectedly "
                "retains partial downgrade state"
            )
        return

    _assert_downgrade_empty(outbox_present=outbox_present)
    if not capability_gate_present:
        _mysql_alter_capability(
            [
                f"ADD CONSTRAINT {_MYSQL_CAPABILITY_GATE} "
                "CHECK (source <> 'agent_request')"
            ]
        )
    if not _mysql_capability_has_gate():
        raise RuntimeError(
            "capability resume outbox MySQL capability downgrade gate "
            "was not installed"
        )
    if outbox_present:
        if not _mysql_outbox_has_gate():
            op.execute(
                sa.text(
                    f"ALTER TABLE {_OUTBOX_TABLE} "
                    f"ADD CONSTRAINT {_MYSQL_OUTBOX_GATE} CHECK (id IS NULL)"
                )
            )
        _mysql_outbox_state(allow_gate=True, require_gate=True)
    _assert_downgrade_empty(outbox_present=outbox_present)
    if outbox_present:
        op.drop_table(_OUTBOX_TABLE)
    _mysql_alter_capability(
        [
            "DROP CHECK ck_cap_inv_agent_request_identity",
            "ADD CONSTRAINT ck_cap_inv_agent_request_identity "
            f"CHECK ({_OLD_AGENT_REQUEST_IDENTITY})",
            "DROP INDEX uq_cap_inv_task_terminal_log",
            "DROP COLUMN request_terminal_log_id",
            "DROP COLUMN request_task_incarnation_id",
            f"DROP CHECK {_MYSQL_CAPABILITY_GATE}",
        ]
    )
    if (
        _mysql_capability_state() != "old"
        or _mysql_capability_has_gate()
    ):
        raise RuntimeError(
            "capability resume outbox MySQL downgrade is incomplete"
        )


def upgrade() -> None:
    _require_supported_mysql()
    if _dialect_name() == "mysql":
        _upgrade_mysql()
        return
    _acquire_transactional_fence(downgrade=False)
    _assert_zero_agent_requests()
    _upgrade_transactional()


def downgrade() -> None:
    _require_supported_mysql()
    if _dialect_name() == "mysql":
        _downgrade_mysql()
        return
    _acquire_transactional_fence(downgrade=True)
    _assert_downgrade_empty()
    _downgrade_transactional()
