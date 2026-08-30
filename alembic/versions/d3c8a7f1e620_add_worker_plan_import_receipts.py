"""add permanent Worker Plan import identity receipts

Revision ID: d3c8a7f1e620
Revises: b7f3d1a8c920
Create Date: 2026-08-08
"""

import re
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d3c8a7f1e620"
down_revision: Union[str, None] = "b7f3d1a8c920"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_RUN_TABLE = "plan_agent_runs"
_PLAN_TABLE = "plans"
_TABLE = "plan_agent_worker_import_receipts"
_INDEX = "ix_plan_worker_import_receipt_plan"
_RUN_PROTOCOL_COLUMN = "import_receipt_protocol"
_RUN_PROTOCOL_CHECK = "ck_plan_agent_run_import_receipt_protocol"
_MYSQL_RUN_PHASE_CHECK = "ck_plan_agent_run_import_receipt_phase"
_MYSQL_RUN_DOWNGRADE_GATE = "ck_plan_agent_run_no_import_downgrade"
_MYSQL_RECEIPT_DOWNGRADE_GATE = (
    "ck_plan_worker_import_receipt_no_downgrade_rows"
)


def _digest_sql(column: str) -> str:
    """Return lowercase-hex validation portable across supported databases."""

    stripped = column
    for character in "0123456789abcdef":
        stripped = f"replace({stripped}, '{character}', '')"
    return f"(length({column}) = 64 AND {stripped} = '')"


_DIGEST_SQL = _digest_sql("payload_digest")
_RUN_PROTOCOL_SQL = (
    "(relay_origin IS NOT NULL AND relay_origin = 'manager_v1' AND "
    "import_receipt_protocol IS NOT NULL AND import_receipt_protocol = 1) OR "
    "((relay_origin IS NULL OR relay_origin <> 'manager_v1') AND "
    "import_receipt_protocol IS NULL)"
)
_MYSQL_RUN_PHASE_SQL = (
    "import_receipt_protocol IS NULL OR import_receipt_protocol IN (0, 1)"
)

_RECEIPT_CHECKS = {
    "ck_plan_worker_import_receipt_identity": "run_id > 0 AND plan_id > 0",
    "ck_plan_worker_import_receipt_protocol": "protocol = 1",
    "ck_plan_worker_import_receipt_origin": "relay_origin = 'manager_v1'",
    "ck_plan_worker_import_receipt_outcome": (
        "outcome IN ('imported', 'cancelled_before_import')"
    ),
    "ck_plan_worker_import_receipt_digest": _DIGEST_SQL,
}


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _dialect_name() -> str:
    return op.get_bind().dialect.name


def _acquire_transactional_fence(*, downgrade: bool) -> None:
    """Acquire the writer transaction before inspecting mutable identities."""

    dialect = _dialect_name()
    if dialect == "postgresql":
        tables = f"{_RUN_TABLE}, {_PLAN_TABLE}"
        if downgrade:
            tables += f", {_TABLE}"
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
            "Worker Plan import receipt migration could not acquire its "
            "SQLite revision writer fence"
        )


def _invalid_import_identity_sql() -> str:
    return (
        f"SELECT COUNT(*) FROM {_RUN_TABLE} AS run "
        f"LEFT JOIN {_PLAN_TABLE} AS plan ON plan.id = run.plan_id "
        "WHERE run.relay_origin = 'manager_v1' AND ("
        "run.id <= 0 OR run.plan_id IS NULL OR run.plan_id <= 0 OR "
        "plan.id IS NULL OR plan.id <= 0 OR "
        "plan.relay_origin IS NULL OR plan.relay_origin != 'manager_v1' OR "
        "run.import_payload_digest IS NULL OR "
        f"NOT {_digest_sql('run.import_payload_digest')})"
    )


def _assert_valid_import_identities() -> None:
    """Reject legacy identities that cannot be safely backfilled."""

    if _is_offline():
        if _dialect_name() == "postgresql":
            op.execute(
                sa.text(
                    f"""
DO $ccm_worker_plan_import_upgrade$
BEGIN
    IF ({_invalid_import_identity_sql()}) <> 0 THEN
        RAISE EXCEPTION
            'Worker Plan import receipt upgrade refused: malformed immutable identity';
    END IF;
END
$ccm_worker_plan_import_upgrade$
"""
                )
            )
        return
    invalid = op.get_bind().execute(
        sa.text(_invalid_import_identity_sql())
    ).scalar_one()
    if invalid:
        raise RuntimeError(
            "Cannot add Worker Plan import receipts while an imported Run "
            "has malformed immutable identity"
        )


def _create_receipt_table() -> None:
    op.create_table(
        _TABLE,
        sa.Column("run_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("protocol", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "relay_origin",
            sa.String(length=30),
            nullable=False,
            server_default="manager_v1",
        ),
        sa.Column("payload_digest", sa.String(length=64), nullable=False),
        sa.Column("outcome", sa.String(length=30), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        *(
            sa.CheckConstraint(expression, name=name)
            for name, expression in _RECEIPT_CHECKS.items()
        ),
        sa.PrimaryKeyConstraint("run_id"),
        mysql_engine="InnoDB",
    )
    op.create_index(_INDEX, _TABLE, ["plan_id"], unique=False)


def _backfill_receipts() -> None:
    """Idempotently admit every live legacy import behind the Run gate."""

    op.execute(
        sa.text(
            f"INSERT INTO {_TABLE} "
            "(run_id, plan_id, protocol, relay_origin, payload_digest, "
            "outcome, created_at) "
            "SELECT run.id, run.plan_id, 1, 'manager_v1', "
            "run.import_payload_digest, 'imported', run.created_at "
            f"FROM {_RUN_TABLE} AS run "
            f"LEFT JOIN {_TABLE} AS receipt ON receipt.run_id = run.id "
            "WHERE run.relay_origin = 'manager_v1' "
            "AND receipt.run_id IS NULL"
        )
    )


def _missing_or_mismatched_live_receipts_sql() -> str:
    return (
        f"SELECT COUNT(*) FROM {_RUN_TABLE} AS run "
        f"LEFT JOIN {_TABLE} AS receipt ON receipt.run_id = run.id "
        "WHERE run.relay_origin = 'manager_v1' AND ("
        "receipt.run_id IS NULL OR receipt.outcome != 'imported' OR "
        "receipt.run_id != run.id OR receipt.plan_id != run.plan_id OR "
        "receipt.protocol != 1 OR receipt.relay_origin != 'manager_v1' OR "
        "receipt.payload_digest != run.import_payload_digest)"
    )


def _assert_live_runs_have_receipts() -> None:
    if _is_offline():
        return
    invalid = op.get_bind().execute(
        sa.text(_missing_or_mismatched_live_receipts_sql())
    ).scalar_one()
    if invalid:
        raise RuntimeError(
            "Worker Plan import receipt backfill did not cover every live "
            "Manager import"
        )


def _downgrade_unsafe_sql(*, receipt_protocols: tuple[int, ...]) -> str:
    protocols = ", ".join(str(value) for value in receipt_protocols)
    return (
        f"SELECT COUNT(*) FROM {_TABLE} AS receipt "
        f"LEFT JOIN {_RUN_TABLE} AS run ON run.id = receipt.run_id "
        f"LEFT JOIN {_PLAN_TABLE} AS plan ON plan.id = receipt.plan_id "
        "WHERE receipt.outcome != 'imported' OR receipt.run_id <= 0 OR "
        "receipt.plan_id <= 0 OR run.id IS NULL OR run.id <= 0 OR "
        "plan.id IS NULL OR plan.id <= 0 OR run.plan_id IS NULL OR "
        "run.plan_id <= 0 OR run.plan_id != receipt.plan_id OR "
        "run.relay_origin IS NULL OR run.relay_origin != 'manager_v1' OR "
        "run.import_receipt_protocol IS NULL OR "
        "run.import_receipt_protocol != 1 OR "
        "plan.relay_origin IS NULL OR plan.relay_origin != 'manager_v1' OR "
        "run.import_payload_digest IS NULL OR "
        "run.import_payload_digest != receipt.payload_digest OR "
        f"receipt.protocol NOT IN ({protocols}) OR "
        "receipt.relay_origin != 'manager_v1' OR "
        f"NOT {_digest_sql('receipt.payload_digest')}"
    )


def _assert_downgrade_safe(*, receipt_protocols: tuple[int, ...] = (1,)) -> None:
    """Refuse destruction of tombstones or detached permanent identities."""

    if _is_offline():
        if _dialect_name() == "postgresql":
            op.execute(
                sa.text(
                    f"""
DO $ccm_worker_plan_import_downgrade$
BEGIN
    IF ({_downgrade_unsafe_sql(receipt_protocols=receipt_protocols)}) <> 0
       OR ({_missing_or_mismatched_live_receipts_sql()}) <> 0 THEN
        RAISE EXCEPTION
            'Worker Plan import receipt downgrade refused: unsafe history';
    END IF;
END
$ccm_worker_plan_import_downgrade$
"""
                )
            )
        return
    bind = op.get_bind()
    unsafe = bind.execute(
        sa.text(_downgrade_unsafe_sql(receipt_protocols=receipt_protocols))
    ).scalar_one()
    missing = bind.execute(
        sa.text(_missing_or_mismatched_live_receipts_sql())
    ).scalar_one()
    if unsafe or missing:
        raise RuntimeError(
            "Cannot downgrade while a Worker Plan import receipt is a "
            "cancellation tombstone or outlives its exact imported graph"
        )


def _upgrade_transactional() -> None:
    op.add_column(
        _RUN_TABLE,
        sa.Column(_RUN_PROTOCOL_COLUMN, sa.Integer(), nullable=True),
    )
    op.execute(
        sa.text(
            f"UPDATE {_RUN_TABLE} SET {_RUN_PROTOCOL_COLUMN} = 1 "
            "WHERE relay_origin = 'manager_v1'"
        )
    )
    with op.batch_alter_table(_RUN_TABLE, schema=None) as batch_op:
        batch_op.create_check_constraint(
            _RUN_PROTOCOL_CHECK,
            _RUN_PROTOCOL_SQL,
        )
    _create_receipt_table()
    _backfill_receipts()
    _assert_live_runs_have_receipts()


def _downgrade_transactional() -> None:
    _assert_downgrade_safe()
    op.drop_index(_INDEX, table_name=_TABLE)
    op.drop_table(_TABLE)
    with op.batch_alter_table(_RUN_TABLE, schema=None) as batch_op:
        batch_op.drop_constraint(_RUN_PROTOCOL_CHECK, type_="check")
        batch_op.drop_column(_RUN_PROTOCOL_COLUMN)


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
        for index, character in enumerate(expression):
            if character == "'":
                quoted = not quoted
            elif not quoted:
                if character == "(":
                    depth += 1
                elif character == ")":
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
        character = expression[index]
        if character == "'":
            quoted = not quoted
            index += 1
            continue
        if not quoted:
            if character == "(":
                depth += 1
            elif character == ")":
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
            "Worker Plan import receipt migration refuses MySQL offline SQL: "
            "server version, CHECK enforcement, and InnoDB atomic DDL cannot "
            "be proven"
        )
    dialect = op.get_bind().dialect
    if getattr(dialect, "is_mariadb", False):
        raise RuntimeError(
            "Worker Plan import receipt migration requires MySQL 8.0.16+"
        )
    version = getattr(dialect, "server_version_info", None)
    if not version or tuple(version[:3]) < (8, 0, 16):
        raise RuntimeError(
            "Worker Plan import receipt migration requires MySQL 8.0.16+ "
            "with enforced CHECK constraints"
        )
    rows = op.get_bind().execute(
        sa.text(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN "
            "('plans', 'plan_agent_runs')"
        )
    )
    engines = {str(name).lower(): str(engine).lower() for name, engine in rows}
    if engines != {"plans": "innodb", "plan_agent_runs": "innodb"}:
        raise RuntimeError(
            "Worker Plan import receipt migration requires plans and "
            "plan_agent_runs to be InnoDB tables"
        )


def _type_matches(
    reflected: sa.types.TypeEngine,
    expected: type[sa.types.TypeEngine],
    length: int | None = None,
) -> bool:
    if expected is sa.Integer:
        return type(reflected).__name__.upper() == "INTEGER"
    if not isinstance(reflected, expected):
        return False
    return length is None or getattr(reflected, "length", None) == length


def _normalized_default(value: object) -> str | None:
    if value is None:
        return None
    default = _strip_outer_parentheses(str(value).strip().lower())
    if len(default) >= 2 and default[0] == default[-1] == "'":
        default = default[1:-1]
    return default


def _mysql_run_state() -> str:
    """Return a recognized Run-gate state; reject foreign partial shapes."""

    inspector = sa.inspect(op.get_bind())
    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_RUN_TABLE)
    }
    protocol = columns.get(_RUN_PROTOCOL_COLUMN)
    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints(_RUN_TABLE)
    }
    protocol_checks = {
        name: sqltext
        for name, sqltext in checks.items()
        if _RUN_PROTOCOL_COLUMN in _normalized_sql(sqltext)
    }
    known = {_RUN_PROTOCOL_CHECK, _MYSQL_RUN_PHASE_CHECK}
    if set(protocol_checks) - known:
        raise RuntimeError(
            "Worker Plan import receipt MySQL Run gate has a foreign CHECK"
        )
    downgrade_gate = checks.get(_MYSQL_RUN_DOWNGRADE_GATE)
    if downgrade_gate is not None and _check_shape(
        downgrade_gate
    ) != _check_shape(
        "relay_origin IS NULL OR relay_origin <> 'manager_v1'"
    ):
        raise RuntimeError(
            "Worker Plan import receipt MySQL Run downgrade gate is malformed"
        )
    if protocol is None:
        if protocol_checks or downgrade_gate is not None:
            raise RuntimeError(
                "Worker Plan import receipt MySQL Run gate is partial"
            )
        return "legacy"
    if (
        not _type_matches(protocol["type"], sa.Integer)
        or not bool(protocol["nullable"])
    ):
        raise RuntimeError(
            "Worker Plan import receipt MySQL Run protocol column is malformed"
        )
    if set(protocol_checks) == {_MYSQL_RUN_PHASE_CHECK}:
        if downgrade_gate is not None:
            raise RuntimeError(
                "Worker Plan import receipt MySQL Run phase has a foreign "
                "downgrade gate"
            )
        if _check_shape(protocol_checks[_MYSQL_RUN_PHASE_CHECK]) != _check_shape(
            _MYSQL_RUN_PHASE_SQL
        ) or _normalized_default(protocol.get("default")) != "0":
            raise RuntimeError(
                "Worker Plan import receipt MySQL Run phase gate is malformed"
            )
        state = "phase"
    elif set(protocol_checks) == {_RUN_PROTOCOL_CHECK}:
        if _check_shape(protocol_checks[_RUN_PROTOCOL_CHECK]) != _check_shape(
            _RUN_PROTOCOL_SQL
        ) or _normalized_default(protocol.get("default")) not in {None, "null"}:
            raise RuntimeError(
                "Worker Plan import receipt MySQL Run canonical gate is malformed"
            )
        state = (
            "canonical_gated"
            if downgrade_gate is not None
            else "canonical"
        )
    else:
        raise RuntimeError(
            "Worker Plan import receipt MySQL Run gate is partial"
        )
    required_enforced = set(protocol_checks)
    if downgrade_gate is not None:
        required_enforced.add(_MYSQL_RUN_DOWNGRADE_GATE)
    if not required_enforced <= _mysql_enforced_checks(_RUN_TABLE):
        raise RuntimeError(
            "Worker Plan import receipt MySQL Run gate is not enforced"
        )
    return state


_RECEIPT_COLUMN_SHAPES = {
    "run_id": (sa.Integer, None, False),
    "plan_id": (sa.Integer, None, False),
    "protocol": (sa.Integer, None, False),
    "relay_origin": (sa.String, 30, False),
    "payload_digest": (sa.String, 64, False),
    "outcome": (sa.String, 30, False),
    "created_at": (sa.DateTime, None, False),
}


def _mysql_receipt_index_present() -> bool:
    inspector = sa.inspect(op.get_bind())
    indexes = {
        str(item.get("name") or "").lower(): (
            tuple(str(column).lower() for column in item.get("column_names") or ()),
            bool(item.get("unique")),
        )
        for item in inspector.get_indexes(_TABLE)
    }
    if set(indexes) - {_INDEX}:
        raise RuntimeError(
            "Worker Plan import receipt MySQL table has unexpected indexes"
        )
    if _INDEX not in indexes:
        return False
    if indexes[_INDEX] != (("plan_id",), False):
        raise RuntimeError(
            "Worker Plan import receipt MySQL plan index is malformed"
        )
    return True


def _mysql_receipt_state() -> str:
    """Return absent/canonical/canonical_gated for replayable MySQL DDL."""

    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_TABLE):
        return "absent"
    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_TABLE)
    }
    if set(columns) != set(_RECEIPT_COLUMN_SHAPES):
        raise RuntimeError(
            "Worker Plan import receipt MySQL table has a partial column set"
        )
    for name, (expected, length, nullable) in _RECEIPT_COLUMN_SHAPES.items():
        column = columns[name]
        if not _type_matches(column["type"], expected, length) or (
            bool(column["nullable"]) is not nullable
        ):
            raise RuntimeError(
                f"Worker Plan import receipt MySQL column {name} is malformed"
            )
    defaults = {
        "protocol": "1",
        "relay_origin": "manager_v1",
    }
    for name, expected in defaults.items():
        if _normalized_default(columns[name].get("default")) != expected:
            raise RuntimeError(
                f"Worker Plan import receipt MySQL column {name} default is malformed"
            )
    pk = tuple(
        str(column).lower()
        for column in (
            inspector.get_pk_constraint(_TABLE).get("constrained_columns") or ()
        )
    )
    if pk != ("run_id",):
        raise RuntimeError(
            "Worker Plan import receipt MySQL primary key is malformed"
        )
    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints(_TABLE)
    }
    canonical = set(_RECEIPT_CHECKS)
    if set(checks) == canonical:
        state = "canonical"
    elif set(checks) == canonical | {_MYSQL_RECEIPT_DOWNGRADE_GATE}:
        state = "canonical_gated"
    else:
        raise RuntimeError(
            "Worker Plan import receipt MySQL CHECK set is malformed"
        )
    for name in _RECEIPT_CHECKS:
        if _check_shape(checks[name]) != _check_shape(_RECEIPT_CHECKS[name]):
            raise RuntimeError(
                f"Worker Plan import receipt MySQL CHECK {name} is malformed"
            )
    if _MYSQL_RECEIPT_DOWNGRADE_GATE in checks and _check_shape(
        checks[_MYSQL_RECEIPT_DOWNGRADE_GATE]
    ) != _check_shape("run_id IS NULL"):
        raise RuntimeError(
            "Worker Plan import receipt MySQL downgrade gate is malformed"
        )
    if not set(checks) <= _mysql_enforced_checks(_TABLE):
        raise RuntimeError(
            "Worker Plan import receipt MySQL CHECK is not enforced"
        )
    if inspector.get_unique_constraints(_TABLE):
        raise RuntimeError(
            "Worker Plan import receipt MySQL table has unexpected UNIQUEs"
        )
    if inspector.get_foreign_keys(_TABLE):
        raise RuntimeError(
            "Worker Plan import receipt MySQL table has unexpected foreign keys"
        )
    engines = op.get_bind().execute(
        sa.text(
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
        ),
        {"table_name": _TABLE},
    ).scalars().all()
    if len(engines) != 1 or str(engines[0]).lower() != "innodb":
        raise RuntimeError(
            "Worker Plan import receipt MySQL table must use InnoDB"
        )
    _mysql_receipt_index_present()
    return state


def _mysql_alter(table_name: str, actions: list[str]) -> None:
    if actions:
        op.execute(
            sa.text(
                f"ALTER TABLE {table_name}\n  " + ",\n  ".join(actions)
            )
        )


def _ensure_mysql_receipt_index() -> None:
    if not _mysql_receipt_index_present():
        op.create_index(_INDEX, _TABLE, ["plan_id"], unique=False)
    if not _mysql_receipt_index_present():
        raise RuntimeError(
            "Worker Plan import receipt MySQL index creation is incomplete"
        )


def _upgrade_mysql() -> None:
    run_state = _mysql_run_state()
    receipt_state = _mysql_receipt_state()
    if receipt_state == "canonical_gated" or run_state == "canonical_gated":
        raise RuntimeError(
            "Worker Plan import receipt MySQL upgrade found downgrade-only state"
        )
    if run_state == "legacy":
        if receipt_state != "absent":
            raise RuntimeError(
                "Worker Plan import receipt MySQL legacy Run schema has a "
                "receipt table"
            )
        _assert_valid_import_identities()
        _mysql_alter(
            _RUN_TABLE,
            [
                "ADD COLUMN import_receipt_protocol INTEGER NULL DEFAULT 0",
                f"ADD CONSTRAINT {_MYSQL_RUN_PHASE_CHECK} "
                f"CHECK ({_MYSQL_RUN_PHASE_SQL})",
            ],
        )
        run_state = _mysql_run_state()
    if run_state == "phase":
        if receipt_state != "absent":
            raise RuntimeError(
                "Worker Plan import receipt MySQL phase Run schema has a "
                "receipt table"
            )
        _assert_valid_import_identities()
        op.get_bind().execute(
            sa.text(
                f"UPDATE {_RUN_TABLE} SET {_RUN_PROTOCOL_COLUMN} = "
                "CASE WHEN relay_origin = 'manager_v1' THEN 1 ELSE NULL END"
            )
        )
        _mysql_alter(
            _RUN_TABLE,
            [
                f"DROP CHECK {_MYSQL_RUN_PHASE_CHECK}",
                "MODIFY COLUMN import_receipt_protocol INTEGER NULL DEFAULT NULL",
                f"ADD CONSTRAINT {_RUN_PROTOCOL_CHECK} "
                f"CHECK ({_RUN_PROTOCOL_SQL})",
            ],
        )
        run_state = _mysql_run_state()
    if run_state != "canonical":
        raise RuntimeError(
            "Worker Plan import receipt MySQL Run gate upgrade is incomplete"
        )

    # Re-run the semantic preflight only after the canonical gate blocks all
    # old importers.  A malformed row racing the first check therefore cannot
    # be stamped into the receipt era.
    _assert_valid_import_identities()
    receipt_state = _mysql_receipt_state()
    if receipt_state == "absent":
        _create_receipt_table()
        receipt_state = _mysql_receipt_state()
    if receipt_state != "canonical":
        raise RuntimeError(
            "Worker Plan import receipt MySQL table creation is incomplete"
        )
    _backfill_receipts()
    _assert_live_runs_have_receipts()
    _ensure_mysql_receipt_index()


def _downgrade_mysql() -> None:
    run_state = _mysql_run_state()
    receipt_state = _mysql_receipt_state()
    if run_state == "legacy":
        if receipt_state != "absent":
            raise RuntimeError(
                "Worker Plan import receipt MySQL legacy Run schema retains "
                "a receipt table"
            )
        return
    if run_state not in {"canonical", "canonical_gated"}:
        raise RuntimeError(
            "Worker Plan import receipt MySQL downgrade found a partial Run gate"
        )

    if receipt_state != "absent":
        receipt_count = op.get_bind().execute(
            sa.text(f"SELECT COUNT(*) FROM {_TABLE}")
        ).scalar_one()
        imported_run_count = op.get_bind().execute(
            sa.text(
                f"SELECT COUNT(*) FROM {_RUN_TABLE} "
                "WHERE relay_origin = 'manager_v1'"
            )
        ).scalar_one()
        if receipt_count or imported_run_count:
            raise RuntimeError(
                "Cannot safely downgrade Worker Plan import receipts on "
                "MySQL while receipt or imported Run history exists"
            )

    # MySQL DDL implicitly commits.  Install the receipt gate first because
    # every current import and cancellation is receipt-first.  If an in-flight
    # writer commits, CHECK validation fails without leaving a partial gate;
    # if the ALTER wins, that writer rolls back.  The canonical Run CHECK is
    # already the old-importer gate.  Once receipts are fenced, the second
    # ALTER can safely reject every Manager-origin Run as a downgrade guard.
    if receipt_state == "canonical":
        _mysql_alter(
            _TABLE,
            [
                f"ADD CONSTRAINT {_MYSQL_RECEIPT_DOWNGRADE_GATE} "
                "CHECK (run_id IS NULL)",
            ],
        )
        receipt_state = _mysql_receipt_state()
    if receipt_state not in {"absent", "canonical_gated"}:
        raise RuntimeError(
            "Worker Plan import receipt MySQL receipt downgrade gate was not installed"
        )
    if run_state == "canonical":
        _mysql_alter(
            _RUN_TABLE,
            [
                f"ADD CONSTRAINT {_MYSQL_RUN_DOWNGRADE_GATE} "
                "CHECK (relay_origin IS NULL OR "
                "relay_origin <> 'manager_v1')",
            ],
        )
        run_state = _mysql_run_state()
    if run_state != "canonical_gated":
        raise RuntimeError(
            "Worker Plan import receipt MySQL Run downgrade gate was not installed"
        )
    if receipt_state == "canonical_gated":
        receipt_count = op.get_bind().execute(
            sa.text(f"SELECT COUNT(*) FROM {_TABLE}")
        ).scalar_one()
        imported_run_count = op.get_bind().execute(
            sa.text(
                f"SELECT COUNT(*) FROM {_RUN_TABLE} "
                "WHERE relay_origin = 'manager_v1'"
            )
        ).scalar_one()
        if receipt_count or imported_run_count:
            raise RuntimeError(
                "Cannot safely downgrade Worker Plan import receipts on "
                "MySQL after installing writer gates"
            )
        op.drop_table(_TABLE)
        receipt_state = _mysql_receipt_state()
    if receipt_state != "absent":
        raise RuntimeError(
            "Worker Plan import receipt MySQL table downgrade is incomplete"
        )

    # The Run gate still rejects legacy importers while dropping the receipt
    # table rejects current importers.  Switch to the old schema in one atomic
    # ALTER so there is no generation where both writer protocols can commit.
    _mysql_alter(
        _RUN_TABLE,
        [
            f"DROP CHECK {_RUN_PROTOCOL_CHECK}",
            f"DROP CHECK {_MYSQL_RUN_DOWNGRADE_GATE}",
            f"DROP COLUMN {_RUN_PROTOCOL_COLUMN}",
        ],
    )
    if _mysql_run_state() != "legacy":
        raise RuntimeError(
            "Worker Plan import receipt MySQL Run gate downgrade is incomplete"
        )


def upgrade() -> None:
    _require_supported_mysql()
    if _dialect_name() == "mysql":
        _upgrade_mysql()
        return
    _acquire_transactional_fence(downgrade=False)
    _assert_valid_import_identities()
    _upgrade_transactional()


def downgrade() -> None:
    _require_supported_mysql()
    if _dialect_name() == "mysql":
        _downgrade_mysql()
        return
    _acquire_transactional_fence(downgrade=True)
    _downgrade_transactional()
