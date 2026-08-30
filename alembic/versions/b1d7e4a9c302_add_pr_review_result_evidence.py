"""add PR review result axes, publication evidence, and rerun attempts

Revision ID: b1d7e4a9c302
Revises: a56a13b7f287
Create Date: 2026-08-16 00:00:00
"""

import re
from typing import Sequence, Union

from alembic import context, op
import sqlalchemy as sa


revision: str = "b1d7e4a9c302"
down_revision: Union[str, None] = "a56a13b7f287"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "pr_reviews"
_RUN_TABLE = "pr_monitor_runs"
_OLD_SUBJECT = "uq_pr_reviews_repo_pr_base_ref_base_head"
_NEW_SUBJECT = "uq_pr_reviews_repo_pr_base_ref_base_head_attempt"
_OLD_SUBJECT_COLUMNS = ("repo_id", "pr_number", "base_ref", "base_sha", "head_sha")
_NEW_SUBJECT_COLUMNS = (*_OLD_SUBJECT_COLUMNS, "attempt")
_RERUN_UNIQUE = "uq_pr_reviews_rerun_idempotency"
_RERUN_UNIQUE_COLUMNS = ("rerun_of_review_id", "rerun_idempotency_key")
_RERUN_FK = "fk_pr_reviews_rerun_of_review_id_pr_reviews"
_RERUN_INDEX = "ix_pr_reviews_rerun_of_review_id"
_ATTEMPT_CHECK = "ck_pr_reviews_attempt"
_ATTEMPT_CHECK_SQL = "attempt >= 1"
_RERUN_SHAPE_CHECK = "ck_pr_reviews_rerun_shape"
_RERUN_SHAPE_CHECK_SQL = (
    "(attempt = 1 AND rerun_of_review_id IS NULL) OR ("
    "attempt > 1 AND rerun_idempotency_key IS NOT NULL AND ("
    "rerun_of_review_id IS NULL OR rerun_of_review_id <> id))"
)
_INPUT_ERROR_CHECK = "ck_pr_reviews_input_error_evidence"
_INPUT_ERROR_CHECK_SQL = (
    "(error_category IS NULL AND error_measured IS NULL "
    "AND error_limit IS NULL AND error_unit IS NULL) OR ("
    "error_category IS NOT NULL "
    "AND error_category = 'unsupported_input_size' "
    # MySQL/MariaDB normally inherit a case-insensitive, PAD SPACE collation.
    # REPLACE is case-sensitive on every supported dialect; this pair makes the
    # categorical value byte-for-byte ASCII-exact on all three database families.
    "AND length(error_category) = 22 "
    "AND length(replace(error_category, 'unsupported_input_size', '')) = 0 "
    "AND error_measured IS NOT NULL "
    "AND error_limit IS NOT NULL "
    "AND error_unit IS NOT NULL "
    "AND error_limit > 0 "
    "AND error_measured > error_limit "
    "AND error_measured <= 9007199254740991 "
    "AND error_unit IN ('characters', 'UTF-8 bytes') "
    "AND ((length(error_unit) = 10 "
    "AND length(replace(error_unit, 'characters', '')) = 0) OR ("
    "length(error_unit) = 11 "
    "AND length(replace(error_unit, 'UTF-8 bytes', '')) = 0)))"
)
_CHECK_SPECS = {
    _ATTEMPT_CHECK: _ATTEMPT_CHECK_SQL,
    _RERUN_SHAPE_CHECK: _RERUN_SHAPE_CHECK_SQL,
    _INPUT_ERROR_CHECK: _INPUT_ERROR_CHECK_SQL,
}
_POSTGRESQL_CHECK_PROBE = "ccm_b1_pr_review_input_error_check_probe"
_LEGACY_RECONCILIATION_ERROR = (
    "Historical GitHub Review evidence was not retained; reconciliation required"
)
_SUPPORTED_DIALECTS = {"sqlite", "postgresql", "mysql", "mariadb"}
_NONTRANSACTIONAL_DDL_DIALECTS = {"sqlite", "mysql", "mariadb"}


def _column_specs() -> dict[str, sa.Column]:
    return {
        "attempt": sa.Column("attempt", sa.Integer(), server_default="1", nullable=False),
        "rerun_of_review_id": sa.Column("rerun_of_review_id", sa.Integer(), nullable=True),
        "rerun_idempotency_key": sa.Column("rerun_idempotency_key", sa.String(64), nullable=True),
        "code_verdict": sa.Column("code_verdict", sa.String(30), nullable=True),
        "code_verdict_task_id": sa.Column(
            "code_verdict_task_id", sa.Integer(), nullable=True
        ),
        "code_verdict_retry_count": sa.Column(
            "code_verdict_retry_count", sa.Integer(), nullable=True
        ),
        "code_verdict_task_started_at": sa.Column(
            "code_verdict_task_started_at", sa.DateTime(), nullable=True
        ),
        "code_verdict_recorded_at": sa.Column(
            "code_verdict_recorded_at", sa.DateTime(), nullable=True
        ),
        "publication_state": sa.Column(
            "publication_state", sa.String(30), server_default="not_started", nullable=False
        ),
        "publication_error": sa.Column("publication_error", sa.Text(), nullable=True),
        "failure_stage": sa.Column("failure_stage", sa.String(30), nullable=True),
        "error_category": sa.Column("error_category", sa.String(64), nullable=True),
        "error_measured": sa.Column("error_measured", sa.BigInteger(), nullable=True),
        "error_limit": sa.Column("error_limit", sa.BigInteger(), nullable=True),
        "error_unit": sa.Column("error_unit", sa.String(20), nullable=True),
        "published_actor": sa.Column("published_actor", sa.String(200), nullable=True),
        "published_at": sa.Column("published_at", sa.DateTime(), nullable=True),
        "github_review_id": sa.Column("github_review_id", sa.BigInteger(), nullable=True),
        "github_review_url": sa.Column("github_review_url", sa.String(1000), nullable=True),
        "github_review_state": sa.Column("github_review_state", sa.String(30), nullable=True),
    }


def _run_column_specs() -> dict[str, sa.Column]:
    return {
        "terminal_intent_status": sa.Column("terminal_intent_status", sa.String(20), nullable=True),
        "terminal_intent_base_ref": sa.Column("terminal_intent_base_ref", sa.String(200), nullable=True),
        "terminal_intent_head_sha": sa.Column("terminal_intent_head_sha", sa.String(64), nullable=True),
        "terminal_intent_delivery_id": sa.Column("terminal_intent_delivery_id", sa.String(100), nullable=True),
        "terminal_intent_observed_at": sa.Column("terminal_intent_observed_at", sa.DateTime(), nullable=True),
        "terminal_intent_checked_at": sa.Column("terminal_intent_checked_at", sa.DateTime(), nullable=True),
        "legacy_terminal_recovery_pending": sa.Column(
            "legacy_terminal_recovery_pending",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    }


def _dialect() -> str:
    name = op.get_context().dialect.name
    if name not in _SUPPORTED_DIALECTS:
        raise RuntimeError(f"PR result migration does not support dialect {name!r}")
    return name


def _is_mysql_family() -> bool:
    dialect = getattr(op.get_bind(), "dialect", None)
    name = getattr(dialect, "name", None)
    mariadb_marker = getattr(dialect, "is_mariadb", False)
    return name in {"mysql", "mariadb"} or mariadb_marker is True


def _is_mariadb() -> bool:
    dialect = getattr(op.get_bind(), "dialect", None)
    return getattr(dialect, "name", None) == "mariadb" or (
        getattr(dialect, "is_mariadb", False) is True
    )


def _require_supported_mysql_family() -> None:
    if not _is_mysql_family():
        return
    if context.is_offline_mode():
        raise RuntimeError(
            "PR result migration refuses MySQL/MariaDB offline SQL because "
            "partial-DDL replay and CHECK enforcement cannot be proven"
        )
    dialect = op.get_bind().dialect
    version = tuple((getattr(dialect, "server_version_info", None) or ())[:3])
    minimum = (10, 6, 1) if _is_mariadb() else (8, 0, 16)
    if not version or version < minimum:
        product = "MariaDB 10.6.1+" if _is_mariadb() else "MySQL 8.0.16+"
        raise RuntimeError(
            "PR result migration requires " + product
            + " with enforced CHECK constraints and atomic DDL"
        )
    rows = op.get_bind().execute(
        sa.text(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME IN (:review_table, :run_table)"
        ),
        {"review_table": _TABLE, "run_table": _RUN_TABLE},
    )
    engines = {
        str(table_name).lower(): str(engine or "").lower()
        for table_name, engine in rows
    }
    required = {_TABLE, _RUN_TABLE}
    if set(engines) != required or any(
        engines[table_name] != "innodb" for table_name in required
    ):
        raise RuntimeError(
            "PR result migration requires pr_reviews and pr_monitor_runs to "
            "exist as InnoDB tables; atomic DDL and the rerun foreign key are "
            "not guaranteed by any other MySQL/MariaDB storage engine"
        )


def _normalized_check_sql(sqltext: object) -> str:
    """Normalize SQL syntax without changing quoted literal case/content."""

    value = str("" if sqltext is None else sqltext).strip().replace("`", "")
    normalized: list[str] = []
    quoted = False
    pending_space = False
    index = 0
    while index < len(value):
        character = value[index]
        if not quoted and character == "_":
            # MySQL may reflect a string as ``_utf8mb4'value'``. Strip only
            # that unquoted introducer token. A global regex would also match
            # the ``_size`` suffix inside ``'unsupported_input_size'`` and
            # silently make distinct evidence categories compare equal.
            introducer = re.match(r"_[A-Za-z0-9]+(?=')", value[index:])
            if introducer is not None and (
                index == 0
                or not (value[index - 1].isalnum() or value[index - 1] == "_")
            ):
                index += len(introducer.group(0))
                continue
        if character == "'":
            if pending_space and normalized and normalized[-1] != " ":
                normalized.append(" ")
            pending_space = False
            normalized.append(character)
            if quoted and index + 1 < len(value) and value[index + 1] == "'":
                normalized.append("'")
                index += 2
                continue
            quoted = not quoted
        elif quoted:
            normalized.append(character)
        elif character.isspace():
            pending_space = True
        else:
            if pending_space and normalized and normalized[-1] != " ":
                normalized.append(" ")
            pending_space = False
            normalized.append(character.lower())
        index += 1
    return "".join(normalized).strip()


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
    expression = _normalized_check_sql(sqltext)
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


def _check_description(name: str) -> str:
    return {
        _ATTEMPT_CHECK: "attempt CHECK",
        _RERUN_SHAPE_CHECK: "rerun shape CHECK",
        _INPUT_ERROR_CHECK: "input evidence CHECK",
    }.get(name, f"CHECK {name!r}")


def _postgresql_check_definitions(
    relation_name: str,
    constraint_name: str = _INPUT_ERROR_CHECK,
) -> dict[str, str]:
    rows = op.get_bind().execute(
        sa.text(
            "SELECT constraint_row.conname, "
            "pg_catalog.pg_get_constraintdef(constraint_row.oid, true), "
            "constraint_row.convalidated "
            "FROM pg_catalog.pg_constraint AS constraint_row "
            "WHERE constraint_row.conrelid = "
            "pg_catalog.to_regclass(:relation_name) "
            "AND constraint_row.contype = 'c' "
            "AND lower(constraint_row.conname) = :constraint_name"
        ),
        {
            "relation_name": relation_name,
            "constraint_name": constraint_name.lower(),
        },
    )
    definitions: dict[str, str] = {}
    for raw_name, definition, validated in rows:
        name = str(raw_name).lower()
        if name in definitions:
            raise RuntimeError(
                "PR result migration found duplicate "
                + _check_description(constraint_name)
            )
        if not bool(validated):
            raise RuntimeError(
                "PR result " + _check_description(constraint_name)
                + " is not validated"
            )
        definitions[name] = str(definition)
    return definitions


def _postgresql_check_present(name: str, sql: str) -> bool:
    actual = _postgresql_check_definitions(_TABLE, name)
    definition = actual.get(name)
    if definition is None:
        return False
    probe = sa.Table(
        _POSTGRESQL_CHECK_PROBE,
        sa.MetaData(),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=True),
        sa.Column("rerun_of_review_id", sa.Integer(), nullable=True),
        sa.Column("rerun_idempotency_key", sa.String(64), nullable=True),
        sa.Column("error_category", sa.String(64), nullable=True),
        sa.Column("error_measured", sa.BigInteger(), nullable=True),
        sa.Column("error_limit", sa.BigInteger(), nullable=True),
        sa.Column("error_unit", sa.String(20), nullable=True),
        sa.CheckConstraint(sql, name=name),
        prefixes=["TEMPORARY"],
    )
    bind = op.get_bind()
    bind.execute(sa.schema.CreateTable(probe))
    try:
        canonical = _postgresql_check_definitions(probe.name, name).get(name)
    finally:
        bind.execute(sa.schema.DropTable(probe))
    if canonical is None or _normalized_check_sql(definition) != _normalized_check_sql(
        canonical
    ):
        raise RuntimeError(
            "PR result migration found incompatible " + _check_description(name)
        )
    return True


def _mysql_enforced_checks() -> set[str]:
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


def _check_present(name: str, sql: str) -> bool:
    dialect_name = getattr(
        getattr(op.get_bind(), "dialect", None),
        "name",
        None,
    )
    if dialect_name == "postgresql":
        return _postgresql_check_present(name, sql)
    matches = [
        item
        for item in sa.inspect(op.get_bind()).get_check_constraints(_TABLE)
        if str(item.get("name") or "").lower() == name
    ]
    if len(matches) > 1:
        raise RuntimeError(
            "PR result migration found duplicate " + _check_description(name)
        )
    if not matches:
        return False
    if _check_shape(matches[0].get("sqltext")) != _check_shape(sql):
        raise RuntimeError(
            "PR result migration found incompatible " + _check_description(name)
        )
    if _is_mysql_family() and name not in _mysql_enforced_checks():
        raise RuntimeError(
            "PR result " + _check_description(name) + " is not enforced"
        )
    return True


def _input_error_check_present() -> bool:
    """Compatibility wrapper retained for focused migration diagnostics."""

    return _check_present(_INPUT_ERROR_CHECK, _INPUT_ERROR_CHECK_SQL)


def _lowercase_hex_sql_remainder(column):
    """Return SQL text left after removing only lowercase hexadecimal."""

    remainder = column
    for character in "0123456789abcdef":
        remainder = sa.func.replace(remainder, character, "")
    return remainder


def _reflected_columns(table: str = _TABLE) -> dict[str, dict]:
    return {item["name"]: item for item in sa.inspect(op.get_bind()).get_columns(table)}


def _type_matches(
    actual: sa.types.TypeEngine,
    expected: sa.types.TypeEngine,
    *,
    dialect_name: str,
) -> bool:
    physical_name = type(actual).__name__.upper()
    if isinstance(expected, sa.Text):
        # Text is a String subclass, so this branch must precede VARCHAR.
        return isinstance(actual, sa.Text) and physical_name == "TEXT"
    if isinstance(expected, sa.String):
        return bool(
            isinstance(actual, sa.String)
            and not isinstance(actual, sa.Text)
            and physical_name in {"STRING", "VARCHAR"}
            and actual.length == expected.length
        )
    if isinstance(expected, sa.BigInteger):
        return bool(
            isinstance(actual, sa.BigInteger)
            and physical_name == "BIGINT"
            and not bool(getattr(actual, "unsigned", False))
            and not bool(getattr(actual, "zerofill", False))
        )
    if isinstance(expected, sa.Integer):
        return bool(
            isinstance(actual, sa.Integer)
            and not isinstance(actual, (sa.BigInteger, sa.SmallInteger, sa.Boolean))
            and physical_name in {"INTEGER", "INT"}
            and not bool(getattr(actual, "unsigned", False))
            and not bool(getattr(actual, "zerofill", False))
        )
    if isinstance(expected, sa.DateTime):
        expected_physical = (
            {"TIMESTAMP"}
            if dialect_name == "postgresql"
            else {"DATETIME"}
        )
        return bool(
            isinstance(actual, sa.DateTime)
            and physical_name in expected_physical
            and not bool(getattr(actual, "timezone", False))
            and getattr(actual, "precision", None) is None
            and getattr(actual, "fsp", None) in {None, 0}
        )
    if isinstance(expected, sa.Boolean):
        if isinstance(actual, sa.Boolean):
            return True
        if dialect_name not in {"mysql", "mariadb"}:
            return False
        # MySQL and MariaDB implement BOOLEAN as a signed TINYINT(1), and
        # SQLAlchemy reflects that physical type instead of sa.Boolean.  Be
        # deliberately strict here so a pre-existing arbitrary integer column
        # cannot be mistaken for the owned lifecycle marker.
        return bool(
            type(actual).__name__.upper() == "TINYINT"
            and type(actual).__module__.startswith(
                "sqlalchemy.dialects.mysql"
            )
            and getattr(actual, "display_width", None) == 1
            and not bool(getattr(actual, "unsigned", False))
            and not bool(getattr(actual, "zerofill", False))
        )
    return False


def _normalized_server_default(value) -> str | None:
    if isinstance(value, sa.schema.DefaultClause):
        value = value.arg
    if value is None:
        return None
    text = str(value).strip()
    while text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()
    if "::" in text:
        text = text.split("::", 1)[0].strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1]
    lowered = text.lower()
    if lowered in {"false", "0", "'0'"}:
        return "false"
    if lowered in {"true", "1", "'1'"}:
        return "true" if isinstance(value, bool) and value else "1"
    return lowered


def _server_default_matches(
    actual,
    expected,
    *,
    expected_type: sa.types.TypeEngine,
    dialect_name: str,
) -> bool:
    if not isinstance(expected_type, sa.Boolean):
        return _normalized_server_default(actual) == _normalized_server_default(
            expected
        )
    if expected is None:
        return actual is None
    if actual is None:
        return False

    if isinstance(actual, sa.schema.DefaultClause):
        actual = actual.arg
    raw = str(actual).strip()
    while raw.startswith("(") and raw.endswith(")"):
        raw = raw[1:-1].strip()
    cast = None
    literal = raw
    if "::" in raw:
        literal, cast = raw.split("::", 1)
        literal = literal.strip()
        cast = cast.strip().lower()
    quoted = bool(
        len(literal) >= 2
        and literal[0] == literal[-1]
        and literal[0] in {"'", '"'}
    )
    if quoted:
        literal = literal[1:-1]
    value = literal.strip().lower()
    if value not in {"0", "false"}:
        return False
    if dialect_name == "postgresql":
        # PostgreSQL may reflect false as either bare FALSE or a quoted literal
        # with an explicit boolean cast. A quoted untyped string is not the
        # physical default owned by this migration.
        return not quoted or cast in {"boolean", "bool"}
    if dialect_name in {"mysql", "mariadb"}:
        # MySQL commonly reflects BOOL/TINYINT defaults as either 0 or '0'.
        # Both are the same numeric default.  A quoted 'false' is text and is
        # deliberately rejected.
        return value == "0" and cast is None
    # SQLite quoted 'false' is text, not numeric/boolean false. Accept only
    # the unquoted physical default generated by sa.false().
    return not quoted and cast is None


def _assert_column_shape(reflected: dict, expected: sa.Column) -> None:
    dialect_name = _dialect()
    if (
        reflected.get("nullable") is not expected.nullable
        or not _type_matches(
            reflected.get("type"),
            expected.type,
            dialect_name=dialect_name,
        )
        or not _server_default_matches(
            reflected.get("default"),
            expected.server_default,
            expected_type=expected.type,
            dialect_name=dialect_name,
        )
    ):
        raise RuntimeError(
            f"PR result migration found incompatible column {expected.name!r}"
        )


def _ensure_columns() -> None:
    specs = _column_specs()
    dialect = _dialect()
    if context.is_offline_mode():
        if dialect in _NONTRANSACTIONAL_DDL_DIALECTS:
            raise RuntimeError(
                f"Offline PR result upgrade is refused for {dialect}: "
                "interrupted DDL cannot be reflected safely"
            )
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            for column in specs.values():
                batch_op.add_column(column)
        with op.batch_alter_table(_RUN_TABLE, schema=None) as batch_op:
            for column in _run_column_specs().values():
                batch_op.add_column(column)
        return

    # One reflected, replayable DDL step per column. SQLite/MySQL/MariaDB can
    # commit any ALTER before Alembic advances its revision stamp.
    for name, column in specs.items():
        reflected = _reflected_columns().get(name)
        if reflected is None:
            with op.batch_alter_table(_TABLE, schema=None) as batch_op:
                batch_op.add_column(column)
            reflected = _reflected_columns().get(name)
            if reflected is None:
                raise RuntimeError(f"PR result migration failed to add {name!r}")
        _assert_column_shape(reflected, column)

    for name, column in _run_column_specs().items():
        reflected = _reflected_columns(_RUN_TABLE).get(name)
        if reflected is None:
            with op.batch_alter_table(_RUN_TABLE, schema=None) as batch_op:
                batch_op.add_column(column)
            reflected = _reflected_columns(_RUN_TABLE).get(name)
            if reflected is None:
                raise RuntimeError(f"PR result migration failed to add Run column {name!r}")
        _assert_column_shape(reflected, column)


def _unique_constraints() -> dict[str, tuple[str, ...]]:
    return {
        item.get("name"): tuple(item.get("column_names") or ())
        for item in sa.inspect(op.get_bind()).get_unique_constraints(_TABLE)
    }


def _ensure_unique(name: str, columns: tuple[str, ...]) -> None:
    reflected = _unique_constraints().get(name)
    if reflected is None:
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.create_unique_constraint(name, list(columns))
        reflected = _unique_constraints().get(name)
    if reflected != columns:
        raise RuntimeError(f"PR result migration found incompatible constraint {name!r}")


def _drop_unique_if_present(name: str, columns: tuple[str, ...]) -> None:
    reflected = _unique_constraints().get(name)
    if reflected is None:
        return
    if reflected != columns:
        raise RuntimeError(f"PR result migration found incompatible constraint {name!r}")
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.drop_constraint(name, type_="unique")


def _option_is_present(value) -> bool:
    """Return whether reflected index metadata carries a real extra option."""

    if value is None:
        return False
    if isinstance(value, dict):
        return any(_option_is_present(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return bool(value)
    if isinstance(value, str):
        return bool(value)
    if isinstance(value, bool):
        return value
    # SQL expressions and numeric values (including WHERE 0) are meaningful.
    return True


def _index_signature(item: dict) -> tuple[tuple[str, ...], bool, bool]:
    extras = any(
        _option_is_present(item.get(key))
        for key in (
            "dialect_options",
            "include_columns",
            "column_sorting",
            "expressions",
            "where",
        )
    )
    return (
        tuple(item.get("column_names") or ()),
        bool(item.get("unique", False)),
        extras,
    )


def _indexes() -> dict[str, tuple[tuple[str, ...], bool, bool]]:
    return {
        item.get("name"): _index_signature(item)
        for item in sa.inspect(op.get_bind()).get_indexes(_TABLE)
    }


def _rerun_foreign_key_matches(fk: dict) -> bool:
    options = dict(fk.get("options") or {})
    return bool(
        fk.get("name") == _RERUN_FK
        and tuple(fk.get("constrained_columns") or ())
        == ("rerun_of_review_id",)
        and fk.get("referred_schema") is None
        and fk.get("referred_table") == _TABLE
        and tuple(fk.get("referred_columns") or ()) == ("id",)
        and set(options) == {"ondelete"}
        and str(options["ondelete"]).upper() == "SET NULL"
        and not _option_is_present(fk.get("dialect_options"))
    )


def _owned_rerun_foreign_keys() -> list[dict]:
    return [
        item
        for item in sa.inspect(op.get_bind()).get_foreign_keys(_TABLE)
        if (
            item.get("name") == _RERUN_FK
            or tuple(item.get("constrained_columns") or ())
            == ("rerun_of_review_id",)
        )
    ]


def _ensure_constraints() -> None:
    if context.is_offline_mode():
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            for name, sql in _CHECK_SPECS.items():
                batch_op.create_check_constraint(name, sql)
            # Preserve a uniqueness fence across non-transactional DDL by
            # installing the broader attempt key before removing the old key.
            batch_op.create_unique_constraint(_NEW_SUBJECT, list(_NEW_SUBJECT_COLUMNS))
            batch_op.create_unique_constraint(_RERUN_UNIQUE, list(_RERUN_UNIQUE_COLUMNS))
            batch_op.create_foreign_key(
                _RERUN_FK, _TABLE, ["rerun_of_review_id"], ["id"], ondelete="SET NULL"
            )
            batch_op.create_index(_RERUN_INDEX, ["rerun_of_review_id"], unique=False)
            batch_op.drop_constraint(_OLD_SUBJECT, type_="unique")
        return

    for name, sql in _CHECK_SPECS.items():
        if not _check_present(name, sql):
            with op.batch_alter_table(_TABLE, schema=None) as batch_op:
                batch_op.create_check_constraint(name, sql)
        if not _check_present(name, sql):
            raise RuntimeError(
                "PR result migration did not install " + _check_description(name)
            )

    current = _unique_constraints()
    old = current.get(_OLD_SUBJECT)
    new = current.get(_NEW_SUBJECT)
    if old is not None and old != _OLD_SUBJECT_COLUMNS:
        raise RuntimeError("PR result migration found incompatible legacy subject constraint")
    if new is not None and new != _NEW_SUBJECT_COLUMNS:
        raise RuntimeError("PR result migration found incompatible attempt subject constraint")
    if old is None and new is None:
        raise RuntimeError("PR result migration found no owned subject uniqueness fence")
    _ensure_unique(_NEW_SUBJECT, _NEW_SUBJECT_COLUMNS)
    _ensure_unique(_RERUN_UNIQUE, _RERUN_UNIQUE_COLUMNS)

    matching_fk = _owned_rerun_foreign_keys()
    if len(matching_fk) > 1:
        raise RuntimeError("PR result migration found duplicate rerun foreign keys")
    if not matching_fk:
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.create_foreign_key(
                _RERUN_FK, _TABLE, ["rerun_of_review_id"], ["id"], ondelete="SET NULL"
            )
        matching_fk = _owned_rerun_foreign_keys()
    if len(matching_fk) != 1 or not _rerun_foreign_key_matches(matching_fk[0]):
        raise RuntimeError(
            "PR result migration did not install the canonical rerun foreign key"
        )

    indexes = _indexes()
    index_columns = indexes.get(_RERUN_INDEX)
    if index_columns is None:
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.create_index(_RERUN_INDEX, ["rerun_of_review_id"], unique=False)
        index_columns = _indexes().get(_RERUN_INDEX)
    if index_columns != (("rerun_of_review_id",), False, False):
        raise RuntimeError("PR result migration found incompatible rerun index")

    # New first, old second: every crash point retains at least one subject
    # uniqueness fence and a replay converges by reflection.
    _drop_unique_if_present(_OLD_SUBJECT, _OLD_SUBJECT_COLUMNS)
    if _unique_constraints().get(_NEW_SUBJECT) != _NEW_SUBJECT_COLUMNS:
        raise RuntimeError("PR result migration did not converge subject uniqueness")


def _preflight_upgrade() -> None:
    """Validate every already-present owned object before the first DDL.

    SQLite/MySQL/MariaDB may commit one ALTER independently.  Discovering a
    malformed later column, FK, index, or uniqueness fence only after adding
    an earlier column would leave a new partial schema that this revision did
    not own.  Missing objects are valid previous/interrupted states; present
    objects must have the exact shape this migration owns.
    """

    if context.is_offline_mode():
        return
    review_columns = _reflected_columns()
    run_columns = _reflected_columns(_RUN_TABLE)
    for name, spec in _column_specs().items():
        reflected = review_columns.get(name)
        if reflected is not None:
            _assert_column_shape(reflected, spec)
    for name, spec in _run_column_specs().items():
        reflected = run_columns.get(name)
        if reflected is not None:
            _assert_column_shape(reflected, spec)

    # A missing CHECK is a valid pre-upgrade or interrupted-upgrade state.
    # A present owned name is adopted only after its complete SQL semantics
    # (and, for MySQL, enforcement status) are proven canonical.
    for name, sql in _CHECK_SPECS.items():
        _check_present(name, sql)

    uniques = _unique_constraints()
    for name, columns in (
        (_OLD_SUBJECT, _OLD_SUBJECT_COLUMNS),
        (_NEW_SUBJECT, _NEW_SUBJECT_COLUMNS),
        (_RERUN_UNIQUE, _RERUN_UNIQUE_COLUMNS),
    ):
        reflected = uniques.get(name)
        if reflected is not None and reflected != columns:
            raise RuntimeError(
                f"PR result migration found incompatible constraint {name!r}"
            )
    if _OLD_SUBJECT not in uniques and _NEW_SUBJECT not in uniques:
        raise RuntimeError(
            "PR result migration found no owned subject uniqueness fence"
        )

    rerun_fks = _owned_rerun_foreign_keys()
    if len(rerun_fks) > 1 or (
        rerun_fks and not _rerun_foreign_key_matches(rerun_fks[0])
    ):
        raise RuntimeError(
            "PR result migration found incompatible rerun foreign key"
        )
    rerun_index = _indexes().get(_RERUN_INDEX)
    if (
        rerun_index is not None
        and rerun_index != (("rerun_of_review_id",), False, False)
    ):
        raise RuntimeError("PR result migration found incompatible rerun index")


def _legacy_verdict_predicates(reviews):
    """Return the exact old-schema evidence used for verdict projection."""

    reviewer_runs = sa.table(
        "pr_reviewer_runs",
        sa.column("pr_review_id", sa.Integer()),
    )
    no_reviewer_runs = ~sa.exists(
        sa.select(sa.literal(1)).where(
            reviewer_runs.c.pr_review_id == reviews.c.id
        )
    )
    # MySQL's common collations make ``value = lower(value)`` case-insensitive.
    # REPLACE itself is case-sensitive, so strip only lowercase hex and require
    # an empty remainder to keep the legacy evidence predicate portable.
    nonce_remainder = _lowercase_hex_sql_remainder(reviews.c.action_nonce)
    complete_armed_outbox = sa.and_(
        reviews.c.status.in_(("publishing", "error")),
        reviews.c.pending_action.in_((
            "lgtm_comment",
            "approved_merged",
            "review_comments",
        )),
        reviews.c.action_nonce.is_not(None),
        sa.func.length(reviews.c.action_nonce) == 48,
        sa.func.length(nonce_remainder) == 0,
        reviews.c.task_id.is_not(None),
        reviews.c.pending_review_body.is_not(None),
        sa.func.length(reviews.c.pending_review_body) > 0,
        reviews.c.publishing_actor.is_not(None),
        sa.func.length(reviews.c.publishing_actor) > 0,
        reviews.c.publishing_retry_count >= 0,
        reviews.c.publishing_task_started_at.is_not(None),
        reviews.c.publishing_started_at.is_not(None),
    )
    coherent_pass = sa.or_(
        sa.and_(
            reviews.c.status == "approved",
            reviews.c.action_taken == "lgtm_comment",
        ),
        sa.and_(
            reviews.c.status == "merged",
            reviews.c.action_taken == "approved_merged",
        ),
        sa.and_(
            complete_armed_outbox,
            reviews.c.pending_action.in_((
                "lgtm_comment",
                "approved_merged",
            )),
        ),
    )
    coherent_changes = sa.or_(
        sa.and_(
            reviews.c.status == "commented",
            reviews.c.action_taken == "review_comments",
        ),
        sa.and_(
            complete_armed_outbox,
            reviews.c.pending_action == "review_comments",
        ),
    )
    return no_reviewer_runs, coherent_pass, coherent_changes


def _backfill() -> None:
    reviews = sa.table(
        _TABLE,
        sa.column("id", sa.Integer()),
        sa.column("status", sa.String()),
        sa.column("action_taken", sa.String()),
        sa.column("pending_action", sa.String()),
        sa.column("action_nonce", sa.String()),
        sa.column("task_id", sa.Integer()),
        sa.column("pending_review_body", sa.Text()),
        sa.column("publishing_actor", sa.String()),
        sa.column("publishing_retry_count", sa.Integer()),
        sa.column("publishing_task_started_at", sa.DateTime()),
        sa.column("publishing_started_at", sa.DateTime()),
        sa.column("code_verdict", sa.String()),
        sa.column("review_summary", sa.Text()),
        sa.column("publication_state", sa.String()),
        sa.column("failure_stage", sa.String()),
    )
    no_reviewer_runs, coherent_pass, coherent_changes = (
        _legacy_verdict_predicates(reviews)
    )
    # Legacy rows have no trustworthy verdict-recorded timestamp.  Backfill
    # only a coherent single-review result and deliberately leave all four
    # provenance columns NULL to make that evidence downgrade explicit.
    op.execute(
        sa.update(reviews)
        .where(
            reviews.c.code_verdict.is_(None),
            no_reviewer_runs,
            coherent_pass,
            ~coherent_changes,
        )
        .values(code_verdict="pass")
    )
    op.execute(
        sa.update(reviews)
        .where(
            reviews.c.code_verdict.is_(None),
            no_reviewer_runs,
            coherent_changes,
            ~coherent_pass,
        )
        .values(code_verdict="changes_required")
    )

    op.execute(sa.text("""
        UPDATE pr_reviews SET publication_state = 'publishing'
        WHERE status = 'publishing' AND publication_state = 'not_started'
    """))
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET publication_state = 'reconciling',
            failure_stage = 'recovery',
            publication_error = :legacy_reconciliation_error
        WHERE publication_state = 'not_started'
          AND (status IN ('approved', 'commented', 'merged')
           OR action_taken IN ('lgtm_comment', 'review_comments', 'approved_merged'))
    """).bindparams(legacy_reconciliation_error=_LEGACY_RECONCILIATION_ERROR))
    op.execute(sa.text("""
        UPDATE pr_reviews SET publication_state = 'not_applicable'
        WHERE status IN ('superseded', 'cancelled')
          AND publication_state = 'not_started'
    """))
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET publication_state = 'failed',
            failure_stage = 'publication',
            publication_error = review_summary
        WHERE status = 'error'
          AND pending_action IN ('lgtm_comment', 'review_comments', 'approved_merged')
          AND publication_state = 'not_started'
    """))
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET publication_state = 'not_applicable',
            failure_stage = 'lifecycle'
        WHERE status = 'error'
          AND pending_action IN ('lgtm_comment', 'review_comments', 'approved_merged')
          AND (
              lower(COALESCE(review_summary, '')) LIKE '%pr became draft%'
           OR lower(COALESCE(review_summary, '')) LIKE '%pr snapshot changed%'
           OR lower(COALESCE(review_summary, '')) LIKE '%pr changed without matching merge evidence%'
           OR lower(COALESCE(review_summary, '')) LIKE '%pr was closed%'
           OR lower(COALESCE(review_summary, '')) LIKE '%pr was merged%'
          )
    """))
    runs = sa.table(
        _RUN_TABLE,
        sa.column("current_review_id", sa.Integer()),
        sa.column("legacy_terminal_recovery_pending", sa.Boolean()),
    )
    summary = sa.func.lower(sa.func.coalesce(reviews.c.review_summary, ""))
    legacy_review_ids = sa.select(reviews.c.id).where(
        reviews.c.status == "error",
        reviews.c.action_taken == "error",
        reviews.c.pending_action.in_((
            "lgtm_comment",
            "review_comments",
            "approved_merged",
        )),
        reviews.c.publication_state == "not_applicable",
        reviews.c.failure_stage == "lifecycle",
        sa.or_(
            summary.contains("pr became draft"),
            summary.contains("pr snapshot changed"),
            summary.contains("pr changed without matching merge evidence"),
            summary.contains("pr was closed"),
            summary.contains("pr was merged"),
        ),
    )
    op.execute(
        sa.update(runs)
        .where(runs.c.current_review_id.in_(legacy_review_ids))
        .values(legacy_terminal_recovery_pending=True)
    )


def _acquire_downgrade_writer_fence() -> str:
    """Fence the evidence audit through the last destructive DDL statement.

    PostgreSQL and SQLite can hold a writer fence for the surrounding Alembic
    transaction. MySQL/MariaDB release every table or transaction lock around
    ``ALTER TABLE``; without a separately deployed write gate, an online
    downgrade could audit an empty evidence column, admit a concurrent write,
    and then drop that fact. Refuse that unprovable operation explicitly.
    """

    if context.is_offline_mode():
        raise RuntimeError("Offline PR result downgrade is refused")
    dialect = _dialect()
    if dialect == "postgresql":
        op.execute(
            sa.text(
                "LOCK TABLE pr_reviews, pr_monitor_runs "
                "IN ACCESS EXCLUSIVE MODE"
            )
        )
        return dialect
    if dialect == "sqlite":
        fenced = op.get_bind().execute(
            sa.text(
                "UPDATE alembic_version SET version_num = version_num "
                "WHERE version_num = :expected_revision"
            ),
            {"expected_revision": revision},
        )
        if fenced.rowcount != 1:
            raise RuntimeError(
                "PR result downgrade could not acquire its SQLite revision "
                "writer fence"
            )
        return dialect
    raise RuntimeError(
        "PR result downgrade is refused for MySQL/MariaDB because ALTER TABLE "
        "releases writer locks; fully drain the application and use a "
        "separately fenced/manual rollback procedure"
    )


def _normalize_reconstructible_legacy_projection(
    reflected_reviews: dict[str, dict],
    reflected_runs: dict[str, dict],
) -> None:
    """Discard only migration-derived values that the legacy columns retain.

    The migration projects old status/action/outbox facts into the new public
    axes. A rollback immediately after deployment must not become impossible
    merely because this migration wrote that redundant projection. Conversely,
    native verdict provenance, reruns, structured input errors, GitHub receipts,
    a checked recovery candidate, and signed terminal intent are never touched;
    the evidence audit below continues to reject every one of those new facts.

    A replay after committed non-transactional DDL may already be missing one
    or more owned columns. In that exact partial-downgrade shape, this cleanup
    necessarily ran before the first DROP and must not reference a removed
    column again.
    """

    if not set(_column_specs()).issubset(reflected_reviews) or not set(
        _run_column_specs()
    ).issubset(reflected_runs):
        return

    # This marker is migration-only. It remains reconstructible only before a
    # recovery probe records terminal_intent_checked_at and only while its exact
    # legacy lifecycle/publication candidate is still present.
    op.execute(sa.text("""
        UPDATE pr_monitor_runs
        SET legacy_terminal_recovery_pending = FALSE
        WHERE legacy_terminal_recovery_pending IS TRUE
          AND terminal_intent_status IS NULL
          AND terminal_intent_base_ref IS NULL
          AND terminal_intent_head_sha IS NULL
          AND terminal_intent_delivery_id IS NULL
          AND terminal_intent_observed_at IS NULL
          AND terminal_intent_checked_at IS NULL
          AND current_review_id IN (
              SELECT pr_reviews.id
              FROM pr_reviews
              WHERE pr_reviews.status = 'error'
                AND pr_reviews.action_taken = 'error'
                AND pr_reviews.pending_action IN (
                    'lgtm_comment', 'review_comments', 'approved_merged'
                )
                AND pr_reviews.publication_state = 'not_applicable'
                AND pr_reviews.failure_stage = 'lifecycle'
                AND pr_reviews.publication_error = pr_reviews.review_summary
                AND (
                    lower(COALESCE(pr_reviews.review_summary, ''))
                        LIKE '%pr became draft%'
                 OR lower(COALESCE(pr_reviews.review_summary, ''))
                        LIKE '%pr snapshot changed%'
                 OR lower(COALESCE(pr_reviews.review_summary, ''))
                        LIKE '%pr changed without matching merge evidence%'
                 OR lower(COALESCE(pr_reviews.review_summary, ''))
                        LIKE '%pr was closed%'
                 OR lower(COALESCE(pr_reviews.review_summary, ''))
                        LIKE '%pr was merged%'
                )
          )
    """))

    # A legacy verdict deliberately has no generation provenance. Every native
    # post-upgrade verdict writes all four provenance columns atomically and is
    # therefore left for the fail-closed audit. Reuse the upgrade predicates
    # verbatim: a merely similar legacy row is not proof that this migration
    # generated the value being discarded.
    reviews = sa.table(
        _TABLE,
        sa.column("id", sa.Integer()),
        sa.column("status", sa.String()),
        sa.column("action_taken", sa.String()),
        sa.column("pending_action", sa.String()),
        sa.column("action_nonce", sa.String()),
        sa.column("task_id", sa.Integer()),
        sa.column("pending_review_body", sa.Text()),
        sa.column("publishing_actor", sa.String()),
        sa.column("publishing_retry_count", sa.Integer()),
        sa.column("publishing_task_started_at", sa.DateTime()),
        sa.column("publishing_started_at", sa.DateTime()),
        sa.column("code_verdict", sa.String()),
        sa.column("code_verdict_task_id", sa.Integer()),
        sa.column("code_verdict_retry_count", sa.Integer()),
        sa.column("code_verdict_task_started_at", sa.DateTime()),
        sa.column("code_verdict_recorded_at", sa.DateTime()),
    )
    no_reviewer_runs, coherent_pass, coherent_changes = (
        _legacy_verdict_predicates(reviews)
    )
    op.execute(
        sa.update(reviews)
        .where(
            reviews.c.code_verdict_task_id.is_(None),
            reviews.c.code_verdict_retry_count.is_(None),
            reviews.c.code_verdict_task_started_at.is_(None),
            reviews.c.code_verdict_recorded_at.is_(None),
            no_reviewer_runs,
            sa.or_(
                sa.and_(
                    reviews.c.code_verdict == "pass",
                    coherent_pass,
                    ~coherent_changes,
                ),
                sa.and_(
                    reviews.c.code_verdict == "changes_required",
                    coherent_changes,
                    ~coherent_pass,
                ),
            ),
        )
        .values(code_verdict=None)
    )

    # Reset only exact projections whose complete information remains in the
    # legacy status/action/review_summary columns.
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET publication_state = 'not_started',
            publication_error = NULL,
            failure_stage = NULL
        WHERE publication_state = 'publishing'
          AND publication_error IS NULL
          AND failure_stage IS NULL
          AND status = 'publishing'
    """))
    op.execute(
        sa.text("""
            UPDATE pr_reviews
            SET publication_state = 'not_started',
                publication_error = NULL,
                failure_stage = NULL
            WHERE publication_state = 'reconciling'
              AND failure_stage = 'recovery'
              AND publication_error = :legacy_reconciliation_error
              AND (status IN ('approved', 'commented', 'merged')
               OR action_taken IN (
                   'lgtm_comment', 'review_comments', 'approved_merged'
               ))
        """).bindparams(
            legacy_reconciliation_error=_LEGACY_RECONCILIATION_ERROR
        )
    )
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET publication_state = 'not_started',
            publication_error = NULL,
            failure_stage = NULL
        WHERE publication_state = 'not_applicable'
          AND publication_error IS NULL
          AND failure_stage IS NULL
          AND status IN ('superseded', 'cancelled')
    """))
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET publication_state = 'not_started',
            publication_error = NULL,
            failure_stage = NULL
        WHERE publication_state = 'failed'
          AND failure_stage = 'publication'
          AND status = 'error'
          AND pending_action IN (
              'lgtm_comment', 'review_comments', 'approved_merged'
          )
          AND (
              publication_error = review_summary
              OR (publication_error IS NULL AND review_summary IS NULL)
          )
    """))
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET publication_state = 'not_started',
            publication_error = NULL,
            failure_stage = NULL
        WHERE publication_state = 'not_applicable'
          AND failure_stage = 'lifecycle'
          AND status = 'error'
          AND pending_action IN (
              'lgtm_comment', 'review_comments', 'approved_merged'
          )
          AND publication_error = review_summary
          AND (
              lower(COALESCE(review_summary, '')) LIKE '%pr became draft%'
           OR lower(COALESCE(review_summary, '')) LIKE '%pr snapshot changed%'
           OR lower(COALESCE(review_summary, ''))
                LIKE '%pr changed without matching merge evidence%'
           OR lower(COALESCE(review_summary, '')) LIKE '%pr was closed%'
           OR lower(COALESCE(review_summary, '')) LIKE '%pr was merged%'
          )
    """))


def upgrade() -> None:
    _require_supported_mysql_family()
    _preflight_upgrade()
    _ensure_columns()
    _backfill()
    _ensure_constraints()


def downgrade() -> None:
    _require_supported_mysql_family()
    _acquire_downgrade_writer_fence()
    review_specs = _column_specs()
    run_specs = _run_column_specs()
    reflected_reviews = _reflected_columns()
    reflected_runs = _reflected_columns(_RUN_TABLE)
    missing_reviews = set(review_specs) - set(reflected_reviews)
    missing_runs = set(run_specs) - set(reflected_runs)

    # A non-transactional SQLite/MySQL downgrade can be interrupted after the
    # one-time evidence audit and one or more column drops.  Missing owned
    # columns are accepted only after every constraint/index step that
    # precedes column removal has already converged to the downgrade shape.
    # This rejects an arbitrary pre-existing malformed schema while allowing
    # an interrupted owned downgrade to replay without selecting a dropped
    # column.
    uniques = _unique_constraints()
    rerun_fks = _owned_rerun_foreign_keys()
    indexes = _indexes()
    checks_present = {
        name: _check_present(name, sql) for name, sql in _CHECK_SPECS.items()
    }

    # Validate every still-present owned object before the first destructive
    # DDL. A later malformed index/FK/column must not be discovered only after
    # earlier constraints or columns have already been removed.
    for name, columns in (
        (_OLD_SUBJECT, _OLD_SUBJECT_COLUMNS),
        (_NEW_SUBJECT, _NEW_SUBJECT_COLUMNS),
        (_RERUN_UNIQUE, _RERUN_UNIQUE_COLUMNS),
    ):
        reflected = uniques.get(name)
        if reflected is not None and reflected != columns:
            raise RuntimeError(
                f"PR result downgrade found incompatible constraint {name!r}"
            )
    if _OLD_SUBJECT not in uniques and _NEW_SUBJECT not in uniques:
        raise RuntimeError(
            "PR result downgrade found no owned subject uniqueness fence"
        )
    if len(rerun_fks) > 1 or (
        rerun_fks and not _rerun_foreign_key_matches(rerun_fks[0])
    ):
        raise RuntimeError(
            "PR result downgrade found incompatible rerun foreign key"
        )
    reflected_rerun_index = indexes.get(_RERUN_INDEX)
    if (
        reflected_rerun_index is not None
        and reflected_rerun_index
        != (("rerun_of_review_id",), False, False)
    ):
        raise RuntimeError(
            "PR result downgrade found incompatible rerun index"
        )

    partial_shape = bool(
        uniques.get(_OLD_SUBJECT) == _OLD_SUBJECT_COLUMNS
        and _NEW_SUBJECT not in uniques
        and _RERUN_UNIQUE not in uniques
        and not rerun_fks
        and _RERUN_INDEX not in indexes
    )
    missing_checks = {
        name for name, present in checks_present.items() if not present
    }
    if missing_checks and not partial_shape:
        raise RuntimeError(
            "PR result downgrade found missing owned CHECK constraints outside "
            "an interrupted downgrade: " + ", ".join(sorted(missing_checks))
        )
    if missing_reviews or missing_runs:
        if not partial_shape:
            raise RuntimeError(
                "PR result downgrade found missing owned columns outside an "
                "interrupted downgrade"
            )

    for name, spec in review_specs.items():
        reflected = reflected_reviews.get(name)
        if reflected is not None:
            _assert_column_shape(reflected, spec)
    for name, spec in run_specs.items():
        reflected = reflected_runs.get(name)
        if reflected is not None:
            _assert_column_shape(reflected, spec)

    _normalize_reconstructible_legacy_projection(
        reflected_reviews,
        reflected_runs,
    )

    # Build audits only from columns that still exist. A missing column can
    # only occur in the verified partial-downgrade shape above and therefore
    # was dropped after a prior complete audit.
    review_predicates: list[str] = []
    if "attempt" in reflected_reviews:
        review_predicates.append("attempt <> 1")
    for name in (
        "code_verdict",
        "code_verdict_task_id",
        "code_verdict_retry_count",
        "code_verdict_task_started_at",
        "code_verdict_recorded_at",
        "rerun_of_review_id",
        "rerun_idempotency_key",
        "publication_error",
        "failure_stage",
        "error_category",
        "error_measured",
        "error_limit",
        "error_unit",
        "published_actor",
        "published_at",
        "github_review_id",
        "github_review_url",
        "github_review_state",
    ):
        if name in reflected_reviews:
            review_predicates.append(f"{name} IS NOT NULL")
    if "publication_state" in reflected_reviews:
        review_predicates.append("publication_state <> 'not_started'")
    if review_predicates:
        unsafe_review = op.get_bind().execute(sa.text(
            "SELECT 1 FROM pr_reviews WHERE "
            + " OR ".join(review_predicates)
            + " LIMIT 1"
        )).first()
        if unsafe_review is not None:
            raise RuntimeError(
                "Cannot downgrade PR review results without losing durable "
                "rerun or publication evidence"
            )

    run_predicates = [
        f"{name} IS NOT NULL"
        for name in (
            "terminal_intent_status",
            "terminal_intent_base_ref",
            "terminal_intent_head_sha",
            "terminal_intent_delivery_id",
            "terminal_intent_observed_at",
            "terminal_intent_checked_at",
        )
        if name in reflected_runs
    ]
    if "legacy_terminal_recovery_pending" in reflected_runs:
        run_predicates.append(
            "legacy_terminal_recovery_pending IS NOT FALSE"
        )
    if run_predicates:
        unsafe_run = op.get_bind().execute(sa.text(
            "SELECT 1 FROM pr_monitor_runs WHERE "
            + " OR ".join(run_predicates)
            + " LIMIT 1"
        )).first()
        if unsafe_run is not None:
            raise RuntimeError(
                "Cannot downgrade PR review results without losing durable "
                "terminal lifecycle evidence"
            )

    duplicate = op.get_bind().execute(sa.text("""
        SELECT 1 FROM pr_reviews
        WHERE repo_id IS NOT NULL
          AND pr_number IS NOT NULL
          AND base_ref IS NOT NULL
          AND base_sha IS NOT NULL
          AND head_sha IS NOT NULL
        GROUP BY repo_id, pr_number, base_ref, base_sha, head_sha
        HAVING COUNT(*) > 1
    """)).first()
    if duplicate is not None:
        raise RuntimeError("Cannot downgrade PR review attempts without losing rerun history")
    # Restore the narrow fence before removing the attempt fence.
    _ensure_unique(_OLD_SUBJECT, _OLD_SUBJECT_COLUMNS)
    _drop_unique_if_present(_NEW_SUBJECT, _NEW_SUBJECT_COLUMNS)

    matching_fk = _owned_rerun_foreign_keys()
    if len(matching_fk) > 1:
        raise RuntimeError("PR result downgrade found duplicate rerun foreign keys")
    if matching_fk:
        fk = matching_fk[0]
        if not _rerun_foreign_key_matches(fk):
            raise RuntimeError("PR result downgrade found incompatible rerun foreign key")
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.drop_constraint(fk["name"], type_="foreignkey")

    # InnoDB may use this exact index to support the self-FK.  Drop the FK
    # first, then remove the now-unowned index; reversing the order is rejected
    # by MySQL and leaves a non-transactional downgrade half-applied.
    indexes = _indexes()
    if _RERUN_INDEX in indexes:
        if indexes[_RERUN_INDEX] != (("rerun_of_review_id",), False, False):
            raise RuntimeError("PR result downgrade found incompatible rerun index")
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.drop_index(_RERUN_INDEX)

    _drop_unique_if_present(_RERUN_UNIQUE, _RERUN_UNIQUE_COLUMNS)
    for name, sql in reversed(tuple(_CHECK_SPECS.items())):
        if _check_present(name, sql):
            with op.batch_alter_table(_TABLE, schema=None) as batch_op:
                batch_op.drop_constraint(name, type_="check")
        if _check_present(name, sql):
            raise RuntimeError(
                "PR result downgrade did not remove " + _check_description(name)
            )
    for name in reversed(tuple(review_specs)):
        reflected = _reflected_columns().get(name)
        if reflected is None:
            continue
        _assert_column_shape(reflected, review_specs[name])
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.drop_column(name)
    for name in reversed(tuple(run_specs)):
        reflected = _reflected_columns(_RUN_TABLE).get(name)
        if reflected is None:
            continue
        _assert_column_shape(reflected, run_specs[name])
        with op.batch_alter_table(_RUN_TABLE, schema=None) as batch_op:
            batch_op.drop_column(name)
