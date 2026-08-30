"""add durable PR Monitor Task tombstones

Revision ID: e2a4c6f8b1d3
Revises: c4a8e2f6b190
Create Date: 2026-08-15

The owner graph for a deleted PR Monitor is intentionally removed while its
internal Task rows remain.  This table preserves their authorization identity.
CREATE and DROP are crash-replayable on non-transactional DDL backends;
downgrade refuses non-empty evidence and fences MySQL-family writers before
the final empty check and DROP.
"""

import re
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "e2a4c6f8b1d3"
down_revision: Union[str, None] = "c4a8e2f6b190"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE = "pr_monitor_task_tombstones"
_DOWNGRADE_GATE = "ck_pr_monitor_tombstones_no_downgrade_use"
_DOWNGRADE_GATE_SQL = "task_id < 0 AND task_id >= 0"


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
            "PR Monitor Task tombstone migration refuses MySQL/MariaDB "
            "offline SQL because table shape and enforced CHECK constraints "
            "cannot be proven"
        )
    dialect = op.get_bind().dialect
    version = tuple((getattr(dialect, "server_version_info", None) or ())[:3])
    minimum = (10, 6, 1) if _is_mariadb() else (8, 0, 16)
    if not version or version < minimum:
        product = "MariaDB 10.6.1+" if _is_mariadb() else "MySQL 8.0.16+"
        raise RuntimeError(
            "PR Monitor Task tombstone migration requires "
            + product
            + " with enforced CHECK constraints and atomic DDL"
        )


def _acquire_transactional_fence(*, downgrade: bool) -> None:
    if _is_offline():
        return
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
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
                "PR Monitor Task tombstone migration could not acquire its "
                "SQLite revision writer fence"
            )
    elif bind.dialect.name == "postgresql" and downgrade:
        if sa.inspect(bind).has_table(_TABLE):
            op.execute(sa.text(
                "LOCK TABLE pr_monitor_task_tombstones "
                "IN ACCESS EXCLUSIVE MODE"
            ))


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


def _check_shape(sqltext: object) -> str:
    expression = _normalized_sql(sqltext)
    if expression.startswith("check"):
        expression = expression[len("check") :].strip()
    return "".join(_strip_outer_parentheses(expression).split())


def _timestamp_default_is_current(default: object) -> bool:
    expression = _normalized_sql(default)
    expression = re.sub(
        r"::(?:timestamp(?:\s+without\s+time\s+zone)?)$",
        "",
        expression,
    ).strip()
    expression = _strip_outer_parentheses(expression)
    return expression in {"current_timestamp", "current_timestamp()", "now()"}


def _mysql_enforced_checks() -> set[str]:
    if _is_mariadb():
        return {
            str(item.get("name") or "").lower()
            for item in sa.inspect(op.get_bind()).get_check_constraints(_TABLE)
        }
    rows = op.get_bind().execute(sa.text(
        "SELECT CONSTRAINT_NAME, ENFORCED "
        "FROM information_schema.TABLE_CONSTRAINTS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
        "AND CONSTRAINT_TYPE = 'CHECK'"
    ), {"table": _TABLE})
    return {
        str(name).lower()
        for name, enforced in rows
        if str(enforced).upper() == "YES"
    }


def _table_state(*, allow_gate: bool = False) -> str:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_TABLE):
        return "absent"

    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_TABLE)
    }
    if set(columns) != {"task_id", "created_at"}:
        raise RuntimeError("PR Monitor Task tombstone table has foreign columns")
    task_id = columns["task_id"]
    if not (
        type(task_id["type"]).__name__.upper() == "INTEGER"
        and task_id.get("nullable") is False
        and task_id.get("default") is None
        and task_id.get("computed") is None
        and task_id.get("identity") is None
    ):
        raise RuntimeError("PR Monitor Task tombstone task_id has a foreign shape")
    created_at = columns["created_at"]
    if not (
        isinstance(created_at["type"], sa.DateTime)
        and created_at.get("nullable") is False
        and _timestamp_default_is_current(created_at.get("default"))
        and created_at.get("computed") is None
        and created_at.get("identity") is None
    ):
        raise RuntimeError("PR Monitor Task tombstone created_at has a foreign shape")

    primary_key = tuple(
        str(column).lower()
        for column in (
            inspector.get_pk_constraint(_TABLE).get("constrained_columns") or ()
        )
    )
    if primary_key != ("task_id",):
        raise RuntimeError("PR Monitor Task tombstone primary key is malformed")
    if inspector.get_unique_constraints(_TABLE):
        raise RuntimeError("PR Monitor Task tombstone has foreign UNIQUE constraints")
    if inspector.get_foreign_keys(_TABLE):
        raise RuntimeError("PR Monitor Task tombstone has foreign keys")
    if inspector.get_indexes(_TABLE):
        raise RuntimeError("PR Monitor Task tombstone has foreign indexes")

    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints(_TABLE)
    }
    if not checks:
        state = "canonical"
    elif allow_gate and set(checks) == {_DOWNGRADE_GATE}:
        if _check_shape(checks[_DOWNGRADE_GATE]) != _check_shape(
            _DOWNGRADE_GATE_SQL
        ):
            raise RuntimeError(
                "PR Monitor Task tombstone downgrade gate is malformed"
            )
        state = "canonical_gated"
    else:
        raise RuntimeError("PR Monitor Task tombstone CHECK set is malformed")

    if state == "canonical_gated":
        if not _is_mysql_family():
            raise RuntimeError(
                "PR Monitor Task tombstone found a MySQL-only downgrade gate"
            )
        if _DOWNGRADE_GATE not in _mysql_enforced_checks():
            raise RuntimeError(
                "PR Monitor Task tombstone downgrade gate is not enforced"
            )
    if _is_mysql_family():
        engines = op.get_bind().execute(sa.text(
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
        ), {"table": _TABLE}).scalars().all()
        if len(engines) != 1 or str(engines[0]).lower() != "innodb":
            raise RuntimeError("PR Monitor Task tombstone table must use InnoDB")
    return state


def _create_table() -> None:
    op.create_table(
        _TABLE,
        sa.Column(
            "task_id",
            sa.Integer(),
            nullable=False,
            autoincrement=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("task_id"),
        mysql_engine="InnoDB",
    )


def _assert_empty() -> None:
    tombstones = sa.table(_TABLE, sa.column("task_id", sa.Integer()))
    if op.get_bind().execute(
        sa.select(tombstones.c.task_id).limit(1)
    ).scalar_one_or_none() is not None:
        raise RuntimeError(
            "Cannot downgrade while PR Monitor Task tombstones exist"
        )


def upgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        _create_table()
        return
    _acquire_transactional_fence(downgrade=False)
    state = _table_state(allow_gate=True)
    if state == "canonical_gated":
        raise RuntimeError(
            "PR Monitor Task tombstone upgrade found a downgrade-only gate"
        )
    if state == "absent":
        _create_table()
    if _table_state() != "canonical":
        raise RuntimeError(
            "PR Monitor Task tombstone table creation is incomplete"
        )


def downgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        raise RuntimeError(
            "PR Monitor Task tombstone offline downgrade is refused because "
            "durable authorization evidence cannot be inspected"
        )
    _acquire_transactional_fence(downgrade=True)
    state = _table_state(allow_gate=True)
    if state == "absent":
        return
    _assert_empty()
    if _is_mysql_family() and state == "canonical":
        op.create_check_constraint(
            _DOWNGRADE_GATE,
            _TABLE,
            _DOWNGRADE_GATE_SQL,
        )
        state = _table_state(allow_gate=True)
    if _is_mysql_family() and state != "canonical_gated":
        raise RuntimeError(
            "PR Monitor Task tombstone downgrade gate was not installed"
        )
    _assert_empty()
    op.drop_table(_TABLE)
    if _table_state(allow_gate=True) != "absent":
        raise RuntimeError(
            "PR Monitor Task tombstone table removal is incomplete"
        )
