"""add durable Worker destroy lifecycle identity and termination receipt

Revision ID: b4e1c7d9f203
Revises: a3d9b5f7c1e4
Create Date: 2026-08-14

The nullable nonce is installed atomically by the first destroy CAS.  The JSON
receipt journals the final proof, nonce, and exact cloud-instance binding
before the irreversible provider call.  Existing interrupted destroy rows are
assigned a nonce by startup recovery, where no pre-upgrade in-process
coordinator can still be alive.  ADD/DROP are crash-replayable on
non-transactional DDL backends; downgrade refuses every active/interrupted
destroy or submitted termination and fences MySQL-family writers before
removing both columns.
"""

import re
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op


revision: str = "b4e1c7d9f203"
down_revision: Union[str, None] = "a3d9b5f7c1e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "workers"
_NONCE_COLUMN = "destroy_lifecycle_nonce"
_RECEIPT_COLUMN = "destroy_termination_receipt"
_MANAGED_COLUMNS = {_NONCE_COLUMN, _RECEIPT_COLUMN}
_MYSQL_DOWNGRADE_GATE = "ck_workers_destroy_nonce_no_downgrade_use"
_MYSQL_DOWNGRADE_GATE_SQL = (
    "status IS NOT NULL AND status <> 'destroying' "
    "AND destroy_lifecycle_nonce IS NULL "
    "AND destroy_termination_receipt IS NULL "
    "AND (bootstrap_step IS NULL OR bootstrap_step <> 'destroy')"
)
_MARIADB_RECEIPT_JSON_CHECK = (
    "json_valid(destroy_termination_receipt)"
)


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _is_mysql_family() -> bool:
    dialect = op.get_bind().dialect
    return dialect.name in {"mysql", "mariadb"} or bool(
        getattr(dialect, "is_mariadb", False)
    )


def _is_mariadb() -> bool:
    dialect = op.get_bind().dialect
    return dialect.name == "mariadb" or bool(
        getattr(dialect, "is_mariadb", False)
    )


def _require_supported_mysql_family() -> None:
    if not _is_mysql_family():
        return
    if _is_offline():
        raise RuntimeError(
            "Worker destroy lifecycle nonce migration refuses MySQL/MariaDB "
            "offline SQL because column shape, active destroy evidence, "
            "CHECK enforcement, and atomic DDL cannot be proven"
        )
    dialect = op.get_bind().dialect
    version = tuple((getattr(dialect, "server_version_info", None) or ())[:3])
    minimum = (10, 6, 1) if _is_mariadb() else (8, 0, 16)
    if not version or version < minimum:
        product = "MariaDB 10.6.1+" if _is_mariadb() else "MySQL 8.0.16+"
        raise RuntimeError(
            "Worker destroy lifecycle nonce migration requires "
            + product
            + " with enforced CHECK constraints and atomic DDL"
        )
    engines = op.get_bind().execute(
        sa.text(
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
        ),
        {"table": _TABLE},
    ).scalars().all()
    if len(engines) != 1 or str(engines[0]).lower() != "innodb":
        raise RuntimeError("Worker table must use InnoDB")


def _acquire_transactional_fence(*, downgrade: bool) -> None:
    if _is_offline():
        return
    bind = op.get_bind()
    dialect = bind.dialect.name
    if dialect == "sqlite":
        expected_revision = revision if downgrade else down_revision
        fenced = bind.execute(
            sa.text(
                "UPDATE alembic_version SET version_num = version_num "
                "WHERE version_num = :expected_revision"
            ),
            {"expected_revision": expected_revision},
        )
        if fenced.rowcount != 1:
            raise RuntimeError(
                "Worker destroy lifecycle nonce migration could not acquire "
                "its SQLite revision writer fence"
            )
    elif dialect == "postgresql" and downgrade:
        inspector = sa.inspect(bind)
        if inspector.has_table(_TABLE):
            op.execute(
                sa.text("LOCK TABLE workers IN ACCESS EXCLUSIVE MODE")
            )


def _normalized_sql(sqltext: object) -> str:
    value = str(sqltext or "").strip().lower().replace("`", "")
    value = re.sub(r"_[a-z0-9]+\s*'", "'", value)
    value = value.replace("!=", "<>")
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


def _is_mariadb_receipt_json_check(sqltext: object) -> bool:
    return _check_shape(sqltext) == _check_shape(
        _MARIADB_RECEIPT_JSON_CHECK
    )


def _mysql_enforced_checks() -> set[str]:
    # MariaDB 10.6 enforces CHECK constraints but does not expose MySQL's
    # TABLE_CONSTRAINTS.ENFORCED column.  The minimum-version gate above is
    # therefore its enforcement proof.
    if _is_mariadb():
        return {
            str(item.get("name") or "").lower()
            for item in sa.inspect(op.get_bind()).get_check_constraints(_TABLE)
        }
    rows = op.get_bind().execute(
        sa.text(
            "SELECT CONSTRAINT_NAME, ENFORCED "
            "FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
            "AND CONSTRAINT_TYPE = 'CHECK'"
        ),
        {"table": _TABLE},
    )
    return {
        str(name).lower()
        for name, enforced in rows
        if str(enforced).upper() == "YES"
    }


def _column_state(*, allow_gate: bool = False) -> str:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_TABLE):
        raise RuntimeError("Worker table is missing")
    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_TABLE)
    }
    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints(_TABLE)
    }
    gate = checks.get(_MYSQL_DOWNGRADE_GATE)
    present = _MANAGED_COLUMNS.intersection(columns)
    if not present:
        if gate is not None:
            raise RuntimeError(
                "Worker destroy lifecycle downgrade gate survived without "
                "its columns"
            )
        return "absent"

    nonce = columns.get(_NONCE_COLUMN)
    if nonce is not None and not (
        isinstance(nonce["type"], sa.String)
        and getattr(nonce["type"], "length", None) == 32
        and bool(nonce["nullable"])
        and nonce.get("default") is None
        and nonce.get("computed") is None
        and nonce.get("identity") is None
    ):
        raise RuntimeError(
            "Worker destroy lifecycle nonce column has a foreign shape"
        )
    receipt = columns.get(_RECEIPT_COLUMN)
    receipt_uses_mariadb_alias = False
    if receipt is not None:
        receipt_uses_mariadb_alias = (
            _is_mariadb()
            and type(receipt["type"]).__name__.upper() == "LONGTEXT"
        )
        if not (
            (
                isinstance(receipt["type"], sa.JSON)
                or receipt_uses_mariadb_alias
            )
            and bool(receipt["nullable"])
            and receipt.get("default") is None
            and receipt.get("computed") is None
            and receipt.get("identity") is None
        ):
            raise RuntimeError(
                "Worker destroy termination receipt column has a foreign "
                "shape"
            )

    for index in inspector.get_indexes(_TABLE):
        if _MANAGED_COLUMNS.intersection({
            str(name).lower() for name in (index.get("column_names") or ())
        }):
            raise RuntimeError(
                "Worker destroy lifecycle columns have an unexpected index"
            )
    for unique in inspector.get_unique_constraints(_TABLE):
        if _MANAGED_COLUMNS.intersection({
            str(name).lower() for name in (unique.get("column_names") or ())
        }):
            raise RuntimeError(
                "Worker destroy lifecycle columns have an unexpected UNIQUE "
                "constraint"
            )
    for foreign_key in inspector.get_foreign_keys(_TABLE):
        if _MANAGED_COLUMNS.intersection({
            str(name).lower()
            for name in (foreign_key.get("constrained_columns") or ())
        }):
            raise RuntimeError(
                "Worker destroy lifecycle columns have an unexpected foreign "
                "key"
            )
    related_checks = {
        name: sqltext
        for name, sqltext in checks.items()
        if any(
            column in _normalized_sql(sqltext)
            for column in _MANAGED_COLUMNS
        )
    }
    mariadb_json_checks = {
        name
        for name, sqltext in related_checks.items()
        if _is_mariadb_receipt_json_check(sqltext)
    }
    if receipt_uses_mariadb_alias and len(mariadb_json_checks) != 1:
        raise RuntimeError(
            "Worker destroy termination receipt MariaDB JSON alias is "
            "missing its exact JSON_VALID constraint"
        )
    if mariadb_json_checks and not _is_mariadb():
        raise RuntimeError(
            "Worker destroy termination receipt has a foreign JSON_VALID "
            "constraint"
        )
    allowed_related_checks = set(mariadb_json_checks)
    if gate is None:
        if set(related_checks) != allowed_related_checks:
            raise RuntimeError(
                "Worker destroy lifecycle columns have an unexpected CHECK"
            )
        if present == _MANAGED_COLUMNS:
            return "canonical"
        if present == {_NONCE_COLUMN}:
            return "nonce_only"
        return "receipt_only"
    if present != _MANAGED_COLUMNS:
        raise RuntimeError(
            "Worker destroy lifecycle downgrade gate has an incomplete "
            "column set"
        )
    allowed_related_checks.add(_MYSQL_DOWNGRADE_GATE)
    if set(related_checks) != allowed_related_checks or not allow_gate:
        raise RuntimeError(
            "Worker destroy lifecycle downgrade gate is unexpected"
        )
    if _check_shape(gate) != _check_shape(_MYSQL_DOWNGRADE_GATE_SQL):
        raise RuntimeError(
            "Worker destroy lifecycle downgrade gate is malformed"
        )
    if not _is_mysql_family():
        raise RuntimeError(
            "Worker destroy lifecycle found a MySQL-only downgrade gate"
        )
    if _MYSQL_DOWNGRADE_GATE not in _mysql_enforced_checks():
        raise RuntimeError(
            "Worker destroy lifecycle downgrade gate is not enforced"
        )
    return "canonical_gated"


def _assert_downgrade_safe() -> None:
    workers = sa.table(
        _TABLE,
        sa.column("id", sa.Integer()),
        sa.column("status", sa.String(length=20)),
        sa.column("bootstrap_step", sa.String(length=100)),
        sa.column(_NONCE_COLUMN, sa.String(length=32)),
        sa.column(_RECEIPT_COLUMN, sa.JSON()),
    )
    blocker = op.get_bind().execute(
        sa.select(workers.c.id)
        .where(
            sa.or_(
                workers.c.destroy_lifecycle_nonce.is_not(None),
                workers.c.destroy_termination_receipt.is_not(None),
                workers.c.status == "destroying",
                workers.c.bootstrap_step == "destroy",
            )
        )
        .limit(1)
    ).scalar_one_or_none()
    if blocker is not None:
        raise RuntimeError(
            "Cannot downgrade while Worker destroy lifecycle evidence exists"
        )


def _add_column(name: str) -> None:
    if name == _NONCE_COLUMN:
        column_type = sa.String(length=32)
    elif name == _RECEIPT_COLUMN:
        column_type = sa.JSON()
    else:  # pragma: no cover - internal invariant
        raise RuntimeError(f"Unknown Worker destroy lifecycle column: {name}")
    op.add_column(
        _TABLE,
        sa.Column(name, column_type, nullable=True),
    )


def _drop_columns(*, state: str) -> None:
    if _is_mysql_family():
        if state != "canonical_gated":
            raise RuntimeError(
                "Worker destroy lifecycle columns cannot be dropped without "
                "its enforced writer gate"
            )
        drop_gate = "DROP CONSTRAINT" if _is_mariadb() else "DROP CHECK"
        op.execute(
            sa.text(
                f"ALTER TABLE {_TABLE} {drop_gate} "
                f"{_MYSQL_DOWNGRADE_GATE}, "
                f"DROP COLUMN {_RECEIPT_COLUMN}, "
                f"DROP COLUMN {_NONCE_COLUMN}"
            )
        )
        return
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.drop_column(_RECEIPT_COLUMN)
        batch_op.drop_column(_NONCE_COLUMN)


def upgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        _add_column(_NONCE_COLUMN)
        _add_column(_RECEIPT_COLUMN)
        return
    _acquire_transactional_fence(downgrade=False)
    state = _column_state(allow_gate=True)
    if state == "canonical_gated":
        raise RuntimeError(
            "Worker destroy lifecycle nonce upgrade found a downgrade-only gate"
        )
    if state in {"absent", "receipt_only"}:
        _add_column(_NONCE_COLUMN)
    if state in {"absent", "nonce_only"}:
        _add_column(_RECEIPT_COLUMN)
    if _column_state() != "canonical":
        raise RuntimeError(
            "Worker destroy lifecycle column creation is incomplete"
        )


def downgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        raise RuntimeError(
            "Worker destroy lifecycle nonce offline downgrade is refused "
            "because active destroy evidence cannot be inspected"
        )
    _acquire_transactional_fence(downgrade=True)
    state = _column_state(allow_gate=True)
    if state == "absent":
        return
    if state in {"nonce_only", "receipt_only"}:
        raise RuntimeError(
            "Worker destroy lifecycle downgrade found an incomplete column set"
        )
    if state == "canonical_gated" and not _is_mysql_family():
        raise RuntimeError(
            "Worker destroy lifecycle nonce downgrade found a foreign gate"
        )
    _assert_downgrade_safe()
    if _is_mysql_family() and state == "canonical":
        op.create_check_constraint(
            _MYSQL_DOWNGRADE_GATE,
            _TABLE,
            _MYSQL_DOWNGRADE_GATE_SQL,
        )
        state = _column_state(allow_gate=True)
    if _is_mysql_family() and state != "canonical_gated":
        raise RuntimeError(
            "Worker destroy lifecycle nonce downgrade gate was not installed"
        )
    _assert_downgrade_safe()
    _drop_columns(state=state)
    if _column_state(allow_gate=True) != "absent":
        raise RuntimeError(
            "Worker destroy lifecycle column removal is incomplete"
        )
