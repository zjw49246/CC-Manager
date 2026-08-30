"""add durable Task execution principal

Revision ID: b8e4a0d3f721
Revises: a7d3f9c2e610
Create Date: 2026-08-13

MySQL-family DDL is replayed as a reflected state machine.  Upgrade may
resume any exact prefix of the new columns/checks.  Downgrade first freezes
both principal-bearing tables behind enforced CHECK gates, then removes each
table's affected schema in one atomic ALTER.  SQLite takes a version-row write
before touching triggers so its batch rebuild really is transactional.
"""

from contextlib import contextmanager
import re
from typing import Iterator, Sequence, Union

import sqlalchemy as sa

from alembic import context, op


revision: str = "b8e4a0d3f721"
down_revision: Union[str, None] = "a7d3f9c2e610"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TASK_TABLE = "tasks"
_OUTBOX_TABLE = "capability_resume_outbox"
_TASK_GATE = "ck_tasks_no_execution_principal_downgrade"
_OUTBOX_GATE = "ck_cap_resume_no_execution_principal_downgrade"

_TASK_COLUMN_SPECS = {
    "execution_user_id": (sa.Integer, None, True, None),
    "execution_user_role": (sa.String, 20, False, "member"),
    "execution_mode": (sa.String, 20, False, "sandbox"),
    "execution_principal_kind": (sa.String, 32, False, "system"),
}
_OUTBOX_COLUMN_SPECS = {
    "request_execution_user_id": (sa.Integer, None, True, None),
    "request_execution_user_role": (sa.String, 20, False, "member"),
    "request_execution_mode": (sa.String, 20, False, "sandbox"),
    "request_execution_principal_kind": (
        sa.String,
        32,
        False,
        "system",
    ),
}

_TASK_CHECKS = {
    "ck_tasks_execution_user_role": (
        "execution_user_role IN ('member', 'admin', 'super_admin')"
    ),
    "ck_tasks_execution_mode": (
        "execution_mode IN ('sandbox', 'unrestricted')"
    ),
    "ck_tasks_execution_principal_kind": (
        "execution_principal_kind IN "
        "('user', 'deployment_token', 'system', 'delegated_user', "
        "'delegated_deployment_token')"
    ),
    "ck_tasks_execution_user_shape": (
        "(execution_principal_kind IN ('user', 'delegated_user') "
        "AND execution_user_id IS NOT NULL) OR "
        "(execution_principal_kind NOT IN ('user', 'delegated_user') "
        "AND execution_user_id IS NULL)"
    ),
    "ck_tasks_execution_principal_policy": (
        "(execution_principal_kind = 'system' "
        "AND execution_user_role = 'member' "
        "AND execution_mode = 'sandbox') OR "
        "(execution_principal_kind IN "
        "('deployment_token', 'delegated_deployment_token') "
        "AND execution_user_role = 'super_admin' "
        "AND execution_mode = 'unrestricted') OR "
        "(execution_principal_kind IN ('user', 'delegated_user') AND "
        "((execution_user_role IN ('admin', 'super_admin') "
        "AND execution_mode = 'unrestricted') OR "
        "(execution_user_role = 'member' "
        "AND execution_mode = 'sandbox')))"
    ),
}

_OUTBOX_CHECKS = {
    "ck_cap_resume_outbox_execution_principal": (
        "request_execution_user_role IN ('member', 'admin', 'super_admin') "
        "AND request_execution_mode IN ('sandbox', 'unrestricted') "
        "AND request_execution_principal_kind IN "
        "('user', 'deployment_token', 'system', 'delegated_user', "
        "'delegated_deployment_token') "
        "AND ((request_execution_principal_kind IN "
        "('user', 'delegated_user') "
        "AND request_execution_user_id IS NOT NULL) OR "
        "(request_execution_principal_kind NOT IN "
        "('user', 'delegated_user') "
        "AND request_execution_user_id IS NULL)) "
        "AND ((request_execution_principal_kind = 'system' "
        "AND request_execution_user_role = 'member' "
        "AND request_execution_mode = 'sandbox') OR "
        "(request_execution_principal_kind IN "
        "('deployment_token', 'delegated_deployment_token') "
        "AND request_execution_user_role = 'super_admin' "
        "AND request_execution_mode = 'unrestricted') OR "
        "(request_execution_principal_kind IN "
        "('user', 'delegated_user') AND "
        "((request_execution_user_role IN ('admin', 'super_admin') "
        "AND request_execution_mode = 'unrestricted') OR "
        "(request_execution_user_role = 'member' "
        "AND request_execution_mode = 'sandbox'))))"
    )
}

_TASK_GATE_SQL = (
    "execution_user_id IS NULL AND execution_user_role = 'member' AND "
    "execution_mode = 'sandbox' AND execution_principal_kind = 'system'"
)
_OUTBOX_GATE_SQL = (
    "request_execution_user_id IS NULL AND "
    "request_execution_user_role = 'member' AND "
    "request_execution_mode = 'sandbox' AND "
    "request_execution_principal_kind = 'system'"
)

_SQLITE_TASK_IDENTIFIER = re.compile(
    r"(?<![A-Za-z0-9_$])(?:tasks|\"tasks\"|`tasks`|\[tasks\])"
    r"(?![A-Za-z0-9_$])",
    re.IGNORECASE,
)
_SQL_STRING_OR_COMMENT = re.compile(
    r"'(?:''|[^'])*'|--[^\r\n]*|/\*.*?\*/",
    re.DOTALL,
)
_POSTGRESQL_CHECK_PROBES = {
    _TASK_TABLE: "ccm_b8_task_principal_check_probe",
    _OUTBOX_TABLE: "ccm_b8_resume_principal_check_probe",
}


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
            "Task execution principal migration refuses MySQL/MariaDB "
            "offline SQL because committed DDL state cannot be reflected"
        )
    dialect = op.get_bind().dialect
    version = tuple((getattr(dialect, "server_version_info", None) or ())[:3])
    minimum = (10, 6, 1) if _is_mariadb() else (8, 0, 16)
    if not version or version < minimum:
        product = "MariaDB 10.6.1+" if _is_mariadb() else "MySQL 8.0.16+"
        raise RuntimeError(
            "Task execution principal migration requires " + product
            + " with enforced CHECK constraints and atomic ALTER TABLE"
        )


def _acquire_transactional_fence(*, downgrade: bool) -> None:
    if _is_offline():
        return
    dialect = op.get_bind().dialect.name
    if dialect == "sqlite":
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
                "Task execution principal migration could not acquire its "
                "SQLite revision writer fence"
            )
    elif dialect == "postgresql":
        op.execute(
            sa.text(
                "LOCK TABLE tasks, capability_resume_outbox "
                "IN ACCESS EXCLUSIVE MODE"
            )
        )


def _sqlite_trigger_references_tasks(table_name: str, sql: str) -> bool:
    if table_name.casefold() == "tasks":
        return True
    searchable_sql = _SQL_STRING_OR_COMMENT.sub(" ", sql)
    return _SQLITE_TASK_IDENTIFIER.search(searchable_sql) is not None


def _downgrade_target_keeps_task_ssh_effects() -> bool:
    destination = context.get_revision_argument()
    if destination is None:
        return False
    destinations = (
        destination
        if isinstance(destination, (tuple, list, set, frozenset))
        else (destination,)
    )
    script = context.get_context().environment_context.script
    pending = [str(value) for value in destinations if value is not None]
    seen: set[str] = set()
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        if current == "c2f8a6d4e1b9":
            return True
        revision_node = script.get_revision(current)
        if revision_node is None:
            continue
        parents = revision_node.down_revision
        if isinstance(parents, str):
            pending.append(parents)
        elif parents:
            pending.extend(str(parent) for parent in parents)
    return False


@contextmanager
def _preserve_sqlite_task_triggers(
    *,
    restore: bool = True,
) -> Iterator[None]:
    bind = op.get_bind()
    if bind.dialect.name != "sqlite":
        yield
        return
    if _is_offline():
        raise RuntimeError(
            "Offline SQLite Task principal migration is refused because "
            "Task-referencing triggers cannot be inspected and preserved"
        )
    triggers = [
        (str(name), str(sql))
        for name, table_name, sql in bind.exec_driver_sql(
            "SELECT name, tbl_name, sql FROM sqlite_master "
            "WHERE type = 'trigger' AND sql IS NOT NULL ORDER BY name"
        )
        if _sqlite_trigger_references_tasks(str(table_name), str(sql))
    ]
    preparer = bind.dialect.identifier_preparer
    for name, _sql in triggers:
        bind.exec_driver_sql(
            f"DROP TRIGGER {preparer.quote_identifier(name)}"
        )
    try:
        yield
    finally:
        if restore:
            for _name, sql in triggers:
                bind.exec_driver_sql(sql)


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


def _postgresql_check_definitions(
    relation_name: str,
    *,
    constraint_names: set[str],
) -> dict[str, str]:
    """Return PostgreSQL-canonical definitions for selected CHECKs.

    PostgreSQL rewrites ``IN`` into typed ``ANY`` arrays and adds casts while
    storing a CHECK.  Comparing ``Inspector.sqltext`` with the source text
    therefore rejects constraints that PostgreSQL itself just created.  Read
    the server's canonical form and require every selected constraint to be
    validated instead.
    """

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
    for raw_name, definition, validated in rows:
        name = str(raw_name).lower()
        if name not in constraint_names:
            continue
        if not bool(validated):
            raise RuntimeError(
                f"Task principal CHECK {relation_name}.{name} is not validated"
            )
        definitions[name] = str(definition)
    return definitions


def _postgresql_canonical_check_state(
    table_name: str,
    specs: dict[
        str,
        tuple[type[sa.types.TypeEngine], int | None, bool, str | None],
    ],
    expected: dict[str, str],
    *,
    gate_name: str,
    gate_sql: str,
) -> set[str]:
    """Canonicalize expected CHECKs through a transaction-local probe table."""

    all_expected = {**expected, gate_name: gate_sql}
    names = set(all_expected)
    probe = sa.Table(
        _POSTGRESQL_CHECK_PROBES[table_name],
        sa.MetaData(),
        *(_new_column(name, spec) for name, spec in specs.items()),
        *(
            sa.CheckConstraint(expression, name=name)
            for name, expression in all_expected.items()
        ),
        prefixes=["TEMPORARY"],
    )
    bind = op.get_bind()
    bind.execute(sa.schema.CreateTable(probe))
    try:
        actual = _postgresql_check_definitions(
            table_name,
            constraint_names=names,
        )
        canonical = _postgresql_check_definitions(
            probe.name,
            constraint_names=names,
        )
    finally:
        bind.execute(sa.schema.DropTable(probe))
    if set(canonical) != names:
        raise RuntimeError(
            f"Task principal PostgreSQL CHECK probe for {table_name} is incomplete"
        )
    for name, definition in actual.items():
        if _normalized_sql(definition) != _normalized_sql(canonical[name]):
            raise RuntimeError(
                f"Task principal CHECK {table_name}.{name} has a foreign shape"
            )
    return set(actual)


def _type_matches(
    reflected: sa.types.TypeEngine,
    expected: type[sa.types.TypeEngine],
    length: int | None,
) -> bool:
    if expected is sa.Integer:
        return type(reflected).__name__.upper() == "INTEGER"
    return isinstance(reflected, expected) and (
        length is None or getattr(reflected, "length", None) == length
    )


def _normalized_default(value: object) -> str | None:
    if value is None:
        return None
    default = str(value).strip().lower()
    default = re.sub(r"::[a-z0-9_ ]+$", "", default)
    default = _strip_outer_parentheses(default)
    if len(default) >= 2 and default[0] == default[-1] == "'":
        default = default[1:-1]
    return default


def _column_state(
    table_name: str,
    specs: dict[str, tuple[type[sa.types.TypeEngine], int | None, bool, str | None]],
) -> set[str]:
    columns = {
        str(column["name"]).lower(): column
        for column in sa.inspect(op.get_bind()).get_columns(table_name)
    }
    present: set[str] = set()
    for name, (kind, length, nullable, default) in specs.items():
        column = columns.get(name)
        if column is None:
            continue
        if (
            not _type_matches(column["type"], kind, length)
            or bool(column["nullable"]) is not nullable
            or _normalized_default(column.get("default")) != default
        ):
            raise RuntimeError(
                f"Task principal column {table_name}.{name} has a foreign shape"
            )
        present.add(name)
    return present


def _check_state(
    table_name: str,
    expected: dict[str, str],
    *,
    gate_name: str,
    gate_sql: str,
) -> set[str]:
    if op.get_bind().dialect.name == "postgresql":
        return _postgresql_canonical_check_state(
            table_name,
            _TASK_COLUMN_SPECS if table_name == _TASK_TABLE else _OUTBOX_COLUMN_SPECS,
            expected,
            gate_name=gate_name,
            gate_sql=gate_sql,
        )
    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in sa.inspect(op.get_bind()).get_check_constraints(table_name)
    }
    present: set[str] = set()
    for name, expression in expected.items():
        if name not in checks:
            continue
        if _check_shape(checks[name]) != _check_shape(expression):
            raise RuntimeError(
                f"Task principal CHECK {table_name}.{name} has a foreign shape"
            )
        present.add(name)
    if gate_name in checks:
        if _check_shape(checks[gate_name]) != _check_shape(gate_sql):
            raise RuntimeError(
                f"Task principal downgrade gate {table_name}.{gate_name} "
                "has a foreign shape"
            )
        present.add(gate_name)
    return present


def _new_column(name: str, spec: tuple) -> sa.Column:
    kind, length, nullable, default = spec
    type_ = kind(length=length) if kind is sa.String else kind()
    return sa.Column(
        name,
        type_,
        nullable=nullable,
        server_default=default,
    )


def _add_missing_schema(
    table_name: str,
    specs: dict[str, tuple],
    checks: dict[str, str],
    *,
    gate_name: str,
    gate_sql: str,
) -> None:
    columns = _column_state(table_name, specs)
    for name, spec in specs.items():
        if name not in columns:
            with op.batch_alter_table(table_name, schema=None) as batch_op:
                batch_op.add_column(_new_column(name, spec))
    if _column_state(table_name, specs) != set(specs):
        raise RuntimeError(f"Task principal columns on {table_name} are incomplete")
    present_checks = _check_state(
        table_name,
        checks,
        gate_name=gate_name,
        gate_sql=gate_sql,
    )
    if gate_name in present_checks:
        raise RuntimeError(
            f"Task principal upgrade found downgrade gate on {table_name}"
        )
    for name, expression in checks.items():
        if name not in present_checks:
            with op.batch_alter_table(table_name, schema=None) as batch_op:
                batch_op.create_check_constraint(name, expression)
    if _check_state(
        table_name,
        checks,
        gate_name=gate_name,
        gate_sql=gate_sql,
    ) != set(checks):
        raise RuntimeError(f"Task principal CHECKs on {table_name} are incomplete")


def _table_phase(
    table_name: str,
    specs: dict[str, tuple],
    checks: dict[str, str],
    *,
    gate_name: str,
    gate_sql: str,
) -> str:
    columns = _column_state(table_name, specs)
    present_checks = _check_state(
        table_name,
        checks,
        gate_name=gate_name,
        gate_sql=gate_sql,
    )
    canonical_checks = set(checks)
    if not columns and not present_checks:
        return "old"
    if columns == set(specs) and present_checks == canonical_checks:
        return "new"
    if columns == set(specs) and present_checks == canonical_checks | {gate_name}:
        return "gated"
    raise RuntimeError(
        f"Task principal downgrade found a partial schema on {table_name}"
    )


def _unsafe_principal_count(table_name: str, *, outbox: bool) -> int:
    prefix = "request_" if outbox else ""
    return int(
        op.get_bind().execute(
            sa.text(
                f"SELECT COUNT(*) FROM {table_name} WHERE "
                f"{prefix}execution_user_id IS NOT NULL OR "
                f"{prefix}execution_user_role <> 'member' OR "
                f"{prefix}execution_mode <> 'sandbox' OR "
                f"{prefix}execution_principal_kind <> 'system'"
            )
        ).scalar_one()
    )


def _assert_downgrade_safe(task_phase: str, outbox_phase: str) -> None:
    if task_phase != "old" and _unsafe_principal_count(
        _TASK_TABLE,
        outbox=False,
    ):
        raise RuntimeError(
            "Cannot downgrade while Task execution principal evidence exists"
        )
    if outbox_phase != "old" and _unsafe_principal_count(
        _OUTBOX_TABLE,
        outbox=True,
    ):
        raise RuntimeError(
            "Cannot downgrade while Capability resume principal evidence exists"
        )


def _drop_schema_transactional(
    table_name: str,
    specs: dict[str, tuple],
    checks: dict[str, str],
    *,
    gate_name: str,
    gate_sql: str,
) -> None:
    present_checks = _check_state(
        table_name,
        checks,
        gate_name=gate_name,
        gate_sql=gate_sql,
    )
    with op.batch_alter_table(table_name, schema=None) as batch_op:
        for name in reversed(tuple(checks)):
            if name in present_checks:
                batch_op.drop_constraint(name, type_="check")
        if gate_name in present_checks:
            batch_op.drop_constraint(gate_name, type_="check")
        present_columns = _column_state(table_name, specs)
        for name in reversed(tuple(specs)):
            if name in present_columns:
                batch_op.drop_column(name)


def _mysql_install_gate(
    table_name: str,
    gate_name: str,
    gate_sql: str,
) -> None:
    op.create_check_constraint(gate_name, table_name, gate_sql)


def _mysql_drop_principal_schema(
    table_name: str,
    specs: dict[str, tuple],
    checks: dict[str, str],
    *,
    gate_name: str,
) -> None:
    drop_check = "DROP CONSTRAINT" if _is_mariadb() else "DROP CHECK"
    actions = [f"{drop_check} {name}" for name in checks]
    actions.append(f"{drop_check} {gate_name}")
    actions.extend(f"DROP COLUMN {name}" for name in reversed(tuple(specs)))
    op.execute(sa.text(f"ALTER TABLE {table_name} " + ", ".join(actions)))


def _upgrade_static() -> None:
    with op.batch_alter_table(_TASK_TABLE, schema=None) as batch_op:
        for name, spec in _TASK_COLUMN_SPECS.items():
            batch_op.add_column(_new_column(name, spec))
        for name, expression in _TASK_CHECKS.items():
            batch_op.create_check_constraint(name, expression)
    with op.batch_alter_table(_OUTBOX_TABLE, schema=None) as batch_op:
        for name, spec in _OUTBOX_COLUMN_SPECS.items():
            batch_op.add_column(_new_column(name, spec))
        for name, expression in _OUTBOX_CHECKS.items():
            batch_op.create_check_constraint(name, expression)


def upgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        if op.get_bind().dialect.name == "sqlite":
            raise RuntimeError(
                "Task execution principal migration requires online SQLite "
                "batch reflection"
            )
        _upgrade_static()
        return
    _acquire_transactional_fence(downgrade=False)
    with _preserve_sqlite_task_triggers():
        _add_missing_schema(
            _TASK_TABLE,
            _TASK_COLUMN_SPECS,
            _TASK_CHECKS,
            gate_name=_TASK_GATE,
            gate_sql=_TASK_GATE_SQL,
        )
    _add_missing_schema(
        _OUTBOX_TABLE,
        _OUTBOX_COLUMN_SPECS,
        _OUTBOX_CHECKS,
        gate_name=_OUTBOX_GATE,
        gate_sql=_OUTBOX_GATE_SQL,
    )


def downgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        raise RuntimeError(
            "Task execution principal offline downgrade is refused because "
            "durable principal evidence cannot be inspected"
        )
    _acquire_transactional_fence(downgrade=True)
    task_phase = _table_phase(
        _TASK_TABLE,
        _TASK_COLUMN_SPECS,
        _TASK_CHECKS,
        gate_name=_TASK_GATE,
        gate_sql=_TASK_GATE_SQL,
    )
    outbox_phase = _table_phase(
        _OUTBOX_TABLE,
        _OUTBOX_COLUMN_SPECS,
        _OUTBOX_CHECKS,
        gate_name=_OUTBOX_GATE,
        gate_sql=_OUTBOX_GATE_SQL,
    )
    _assert_downgrade_safe(task_phase, outbox_phase)

    if _is_mysql_family():
        if task_phase == "new":
            _mysql_install_gate(_TASK_TABLE, _TASK_GATE, _TASK_GATE_SQL)
            task_phase = _table_phase(
                _TASK_TABLE,
                _TASK_COLUMN_SPECS,
                _TASK_CHECKS,
                gate_name=_TASK_GATE,
                gate_sql=_TASK_GATE_SQL,
            )
        if outbox_phase == "new":
            _mysql_install_gate(_OUTBOX_TABLE, _OUTBOX_GATE, _OUTBOX_GATE_SQL)
            outbox_phase = _table_phase(
                _OUTBOX_TABLE,
                _OUTBOX_COLUMN_SPECS,
                _OUTBOX_CHECKS,
                gate_name=_OUTBOX_GATE,
                gate_sql=_OUTBOX_GATE_SQL,
            )
        if task_phase not in {"old", "gated"} or outbox_phase not in {
            "old",
            "gated",
        }:
            raise RuntimeError("Task principal downgrade gates are incomplete")
        _assert_downgrade_safe(task_phase, outbox_phase)
        if outbox_phase == "gated":
            _mysql_drop_principal_schema(
                _OUTBOX_TABLE,
                _OUTBOX_COLUMN_SPECS,
                _OUTBOX_CHECKS,
                gate_name=_OUTBOX_GATE,
            )
        if task_phase == "gated":
            _mysql_drop_principal_schema(
                _TASK_TABLE,
                _TASK_COLUMN_SPECS,
                _TASK_CHECKS,
                gate_name=_TASK_GATE,
            )
        if _table_phase(
            _TASK_TABLE,
            _TASK_COLUMN_SPECS,
            _TASK_CHECKS,
            gate_name=_TASK_GATE,
            gate_sql=_TASK_GATE_SQL,
        ) != "old" or _table_phase(
            _OUTBOX_TABLE,
            _OUTBOX_COLUMN_SPECS,
            _OUTBOX_CHECKS,
            gate_name=_OUTBOX_GATE,
            gate_sql=_OUTBOX_GATE_SQL,
        ) != "old":
            raise RuntimeError("Task principal schema removal is incomplete")
        return

    if outbox_phase != "old":
        _drop_schema_transactional(
            _OUTBOX_TABLE,
            _OUTBOX_COLUMN_SPECS,
            _OUTBOX_CHECKS,
            gate_name=_OUTBOX_GATE,
            gate_sql=_OUTBOX_GATE_SQL,
        )
    if task_phase != "old":
        with _preserve_sqlite_task_triggers(
            restore=_downgrade_target_keeps_task_ssh_effects(),
        ):
            _drop_schema_transactional(
                _TASK_TABLE,
                _TASK_COLUMN_SPECS,
                _TASK_CHECKS,
                gate_name=_TASK_GATE,
                gate_sql=_TASK_GATE_SQL,
            )
    if _table_phase(
        _TASK_TABLE,
        _TASK_COLUMN_SPECS,
        _TASK_CHECKS,
        gate_name=_TASK_GATE,
        gate_sql=_TASK_GATE_SQL,
    ) != "old" or _table_phase(
        _OUTBOX_TABLE,
        _OUTBOX_COLUMN_SPECS,
        _OUTBOX_CHECKS,
        gate_name=_OUTBOX_GATE,
        gate_sql=_OUTBOX_GATE_SQL,
    ) != "old":
        raise RuntimeError("Task principal schema removal is incomplete")
