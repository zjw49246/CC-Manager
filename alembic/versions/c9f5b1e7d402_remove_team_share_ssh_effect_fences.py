"""remove local Team Share SSH effect fences

Revision ID: c9f5b1e7d402
Revises: b8e4a0d3f721
Create Date: 2026-08-14

TeamProjectShare and TeamTaskShare are Manager-local ACL rows.  They do not
change the Task execution identity or cross a CCM trust boundary, so mutating
them must not be serialized behind a long-running remote SSH effect.  The
legacy ``project_shares``/``task_shares`` federation triggers intentionally
remain in place.
"""

import re
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c9f5b1e7d402"
down_revision: Union[str, None] = "b8e4a0d3f721"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TEAM_SHARE_TRIGGERS = (
    "trg_task_ssh_effect_team_task_share_insert",
    "trg_task_ssh_effect_team_task_share_update",
    "trg_task_ssh_effect_team_task_share_delete",
    "trg_task_ssh_effect_team_project_share_insert",
    "trg_task_ssh_effect_team_project_share_update",
    "trg_task_ssh_effect_team_project_share_delete",
)

_TEAM_SHARE_TRIGGER_DDL = (
    """
CREATE TRIGGER trg_task_ssh_effect_team_task_share_insert
BEFORE INSERT ON team_task_shares
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.task_id = NEW.task_id AND receipt.status = 'running'
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect sharing is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_team_task_share_update
BEFORE UPDATE ON team_task_shares
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.status = 'running'
      AND receipt.task_id IN (OLD.task_id, NEW.task_id)
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect sharing is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_team_task_share_delete
BEFORE DELETE ON team_task_shares
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.task_id = OLD.task_id AND receipt.status = 'running'
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect sharing is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_team_project_share_insert
BEFORE INSERT ON team_project_shares
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM task_ssh_effect_receipts AS receipt
    JOIN tasks AS task ON task.id = receipt.task_id
    WHERE task.project_id = NEW.project_id AND receipt.status = 'running'
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect sharing is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_team_project_share_update
BEFORE UPDATE ON team_project_shares
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM task_ssh_effect_receipts AS receipt
    JOIN tasks AS task ON task.id = receipt.task_id
    WHERE receipt.status = 'running'
      AND task.project_id IN (OLD.project_id, NEW.project_id)
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect sharing is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_team_project_share_delete
BEFORE DELETE ON team_project_shares
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM task_ssh_effect_receipts AS receipt
    JOIN tasks AS task ON task.id = receipt.task_id
    WHERE task.project_id = OLD.project_id AND receipt.status = 'running'
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect sharing is busy');
END
""",
)


def _is_sqlite() -> bool:
    return op.get_bind().dialect.name == "sqlite"


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _acquire_sqlite_writer_fence(*, downgrade: bool) -> None:
    if not _is_sqlite() or _is_offline():
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
            "Team Share SSH fence migration could not acquire its SQLite "
            "revision writer fence"
        )


def _normalized_trigger_sql(sql: object) -> str:
    value = str(sql or "").strip().lower().replace('"', "")
    value = re.sub(r"\s+", " ", value)
    return value.rstrip(";")


_EXPECTED_TRIGGER_SQL = {
    name: _normalized_trigger_sql(sql)
    for name, sql in zip(_TEAM_SHARE_TRIGGERS, _TEAM_SHARE_TRIGGER_DDL)
}


def _trigger_state() -> set[str]:
    rows = op.get_bind().execute(
        sa.text(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type = 'trigger' AND name IN ("
            + ", ".join(
                f":name_{index}" for index in range(len(_TEAM_SHARE_TRIGGERS))
            )
            + ")"
        ),
        {
            f"name_{index}": name
            for index, name in enumerate(_TEAM_SHARE_TRIGGERS)
        },
    ).all()
    present: set[str] = set()
    for raw_name, raw_sql in rows:
        name = str(raw_name)
        if _normalized_trigger_sql(raw_sql) != _EXPECTED_TRIGGER_SQL[name]:
            raise RuntimeError(
                f"Team Share SSH fence trigger {name} has a foreign shape"
            )
        present.add(name)
    return present


def upgrade() -> None:
    if not _is_sqlite():
        return
    if _is_offline():
        for trigger_name in _TEAM_SHARE_TRIGGERS:
            op.execute(f'DROP TRIGGER IF EXISTS "{trigger_name}"')
        return
    _acquire_sqlite_writer_fence(downgrade=False)
    present = _trigger_state()
    for trigger_name in _TEAM_SHARE_TRIGGERS:
        if trigger_name in present:
            op.execute(f'DROP TRIGGER "{trigger_name}"')
    if _trigger_state():
        raise RuntimeError("Team Share SSH fence trigger removal is incomplete")


def downgrade() -> None:
    if not _is_sqlite():
        return
    if _is_offline():
        for trigger_ddl in _TEAM_SHARE_TRIGGER_DDL:
            op.execute(trigger_ddl)
        return
    _acquire_sqlite_writer_fence(downgrade=True)
    present = _trigger_state()
    for trigger_name, trigger_ddl in zip(
        _TEAM_SHARE_TRIGGERS,
        _TEAM_SHARE_TRIGGER_DDL,
    ):
        if trigger_name not in present:
            op.execute(trigger_ddl)
    if _trigger_state() != set(_TEAM_SHARE_TRIGGERS):
        raise RuntimeError("Team Share SSH fence trigger restore is incomplete")
