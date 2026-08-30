"""add durable Task SSH effect receipts

Revision ID: c2f8a6d4e1b9
Revises: b4e7c1a9d2f0
"""

from alembic import context, op
import sqlalchemy as sa


revision = "c2f8a6d4e1b9"
down_revision = "b4e7c1a9d2f0"
branch_labels = None
depends_on = None

_TABLE = "task_ssh_effect_receipts"
_SUPPORTED_DIALECTS = {"sqlite", "postgresql", "mysql", "mariadb"}
_SQLITE_TRIGGERS = (
    "trg_task_ssh_effect_task_update",
    "trg_task_ssh_effect_task_delete",
    "trg_task_ssh_effect_profile_update",
    "trg_task_ssh_effect_profile_delete",
    "trg_task_ssh_effect_grant_insert",
    "trg_task_ssh_effect_grant_update",
    "trg_task_ssh_effect_grant_delete",
    "trg_task_ssh_effect_task_share_insert",
    "trg_task_ssh_effect_task_share_update",
    "trg_task_ssh_effect_task_share_delete",
    "trg_task_ssh_effect_project_share_insert",
    "trg_task_ssh_effect_project_share_update",
    "trg_task_ssh_effect_project_share_delete",
    "trg_task_ssh_effect_team_task_share_insert",
    "trg_task_ssh_effect_team_task_share_update",
    "trg_task_ssh_effect_team_task_share_delete",
    "trg_task_ssh_effect_team_project_share_insert",
    "trg_task_ssh_effect_team_project_share_update",
    "trg_task_ssh_effect_team_project_share_delete",
)
_SQLITE_TRIGGER_DDL = (
    """
CREATE TRIGGER trg_task_ssh_effect_task_update
BEFORE UPDATE ON tasks
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.task_id = OLD.id
      AND receipt.task_incarnation_id = OLD.incarnation_id
      AND receipt.task_retry_count = OLD.retry_count
      AND receipt.task_turn_generation = OLD.turn_generation
      AND receipt.task_status = OLD.status
      AND receipt.status = 'running'
)
AND (
    NEW.incarnation_id IS NOT OLD.incarnation_id
    OR NEW.retry_count IS NOT OLD.retry_count
    OR NEW.turn_generation IS NOT OLD.turn_generation
    OR NEW.status IS NOT OLD.status
    OR NEW.project_id IS NOT OLD.project_id
    OR NEW.instance_id IS NOT OLD.instance_id
    OR NEW.worker_id IS NOT OLD.worker_id
    OR NEW.shared_from_id IS NOT OLD.shared_from_id
    OR NEW.metadata IS NOT OLD.metadata
    OR NEW.provider IS NOT OLD.provider
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect generation is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_task_delete
BEFORE DELETE ON tasks
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.task_id = OLD.id
      AND receipt.task_incarnation_id = OLD.incarnation_id
      AND receipt.task_retry_count = OLD.retry_count
      AND receipt.task_turn_generation = OLD.turn_generation
      AND receipt.task_status = OLD.status
      AND receipt.status = 'running'
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect generation is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_profile_update
BEFORE UPDATE ON ssh_profiles
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.profile_id = OLD.id
      AND receipt.profile_revision = OLD.revision
      AND receipt.status = 'running'
)
AND (
    NEW.host IS NOT OLD.host
    OR NEW.port IS NOT OLD.port
    OR NEW.username IS NOT OLD.username
    OR NEW.key_path IS NOT OLD.key_path
    OR NEW.public_key_fingerprint IS NOT OLD.public_key_fingerprint
    OR NEW.host_key_type IS NOT OLD.host_key_type
    OR NEW.host_key_value IS NOT OLD.host_key_value
    OR NEW.host_key_fingerprint IS NOT OLD.host_key_fingerprint
    OR NEW.revision IS NOT OLD.revision
    OR NEW.enabled IS NOT OLD.enabled
    OR NEW.task_access_enabled IS NOT OLD.task_access_enabled
    OR NEW.task_capabilities IS NOT OLD.task_capabilities
    OR NEW.allowed_roots IS NOT OLD.allowed_roots
    OR NEW.deleted_at IS NOT OLD.deleted_at
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect profile is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_profile_delete
BEFORE DELETE ON ssh_profiles
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.profile_id = OLD.id
      AND receipt.profile_revision = OLD.revision
      AND receipt.status = 'running'
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect profile is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_grant_insert
BEFORE INSERT ON task_ssh_grants
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.task_id = NEW.task_id
      AND receipt.status = 'running'
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect grant is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_grant_update
BEFORE UPDATE ON task_ssh_grants
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.status = 'running'
      AND receipt.task_id IN (OLD.task_id, NEW.task_id)
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect grant is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_grant_delete
BEFORE DELETE ON task_ssh_grants
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM task_ssh_effect_receipts AS receipt
    WHERE receipt.task_id = OLD.task_id
      AND receipt.status = 'running'
)
BEGIN
    SELECT RAISE(ABORT, 'Task SSH effect grant is busy');
END
""",
    """
CREATE TRIGGER trg_task_ssh_effect_task_share_insert
BEFORE INSERT ON task_shares
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
CREATE TRIGGER trg_task_ssh_effect_task_share_update
BEFORE UPDATE ON task_shares
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
CREATE TRIGGER trg_task_ssh_effect_task_share_delete
BEFORE DELETE ON task_shares
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
CREATE TRIGGER trg_task_ssh_effect_project_share_insert
BEFORE INSERT ON project_shares
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
CREATE TRIGGER trg_task_ssh_effect_project_share_update
BEFORE UPDATE ON project_shares
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
CREATE TRIGGER trg_task_ssh_effect_project_share_delete
BEFORE DELETE ON project_shares
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


def _dialect_name() -> str:
    return op.get_bind().dialect.name


def _create_sqlite_permit_triggers() -> None:
    if _dialect_name() != "sqlite":
        return
    for statement in _SQLITE_TRIGGER_DDL:
        op.execute(sa.text(statement))


def _drop_sqlite_permit_triggers() -> None:
    if _dialect_name() != "sqlite":
        return
    for trigger in _SQLITE_TRIGGERS:
        op.execute(sa.text(f"DROP TRIGGER IF EXISTS {trigger}"))


def _assert_downgrade_safe() -> None:
    """Permanent effect evidence may be dropped only while still empty."""

    if context.is_offline_mode():
        raise RuntimeError(
            "Offline Task SSH effect receipt downgrade is refused because "
            "permanent evidence cannot be inspected"
        )
    bind = op.get_bind()
    dialect = bind.dialect.name
    if dialect not in _SUPPORTED_DIALECTS:
        raise RuntimeError(
            f"Task SSH effect receipt downgrade does not support {dialect!r}"
        )
    receipt_count = bind.execute(
        sa.text(f"SELECT COUNT(*) FROM {_TABLE}")
    ).scalar_one()
    if receipt_count:
        raise RuntimeError(
            "Cannot downgrade Task SSH effect receipts after a remote effect "
            "has been admitted; permanent replay/ambiguity evidence would "
            "be destroyed"
        )


def upgrade() -> None:
    op.create_table(
        "task_ssh_effect_receipts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("effect_id", sa.String(length=32), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column(
            "task_incarnation_id",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column("task_retry_count", sa.Integer(), nullable=False),
        sa.Column("task_turn_generation", sa.BigInteger(), nullable=False),
        sa.Column("task_status", sa.String(length=20), nullable=False),
        sa.Column("profile_id", sa.Integer(), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(length=16), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "result_payload",
            sa.JSON(none_as_null=True),
            nullable=True,
        ),
        sa.Column("result_digest", sa.String(length=64), nullable=True),
        sa.Column(
            "result_compacted",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("outcome_code", sa.String(length=48), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "LENGTH(effect_id) = 32",
            name="ck_task_ssh_effect_id_length",
        ),
        sa.CheckConstraint(
            "LENGTH(task_incarnation_id) = 32",
            name="ck_task_ssh_effect_incarnation_length",
        ),
        sa.CheckConstraint(
            "LENGTH(request_digest) = 64",
            name="ck_task_ssh_effect_request_digest_length",
        ),
        sa.CheckConstraint(
            "operation IN ('execute', 'write')",
            name="ck_task_ssh_effect_operation",
        ),
        sa.CheckConstraint(
            "status IN ('running', 'completed', 'ambiguous', 'aborted')",
            name="ck_task_ssh_effect_status",
        ),
        sa.CheckConstraint(
            "outcome_code IS NULL OR outcome_code IN ("
            "'success', 'cancelled_before_execution', 'generation_changed', "
            "'authorization_changed', 'preflight_failed', "
            "'remote_outcome_unknown', 'manager_restart_unknown')",
            name="ck_task_ssh_effect_outcome_code",
        ),
        sa.CheckConstraint(
            "task_id > 0 AND task_retry_count >= 0 "
            "AND task_turn_generation >= 0 AND profile_id > 0 "
            "AND profile_revision > 0",
            name="ck_task_ssh_effect_generation",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND result_payload IS NULL "
            "AND result_digest IS NULL AND outcome_code IS NULL "
            "AND result_compacted = false AND completed_at IS NULL) OR "
            "(status = 'completed' "
            "AND result_digest IS NOT NULL "
            "AND LENGTH(result_digest) = 64 "
            "AND result_compacted = false AND result_payload IS NOT NULL "
            "AND outcome_code = 'success' AND completed_at IS NOT NULL) OR "
            "(status IN ('ambiguous', 'aborted') "
            "AND result_payload IS NULL AND result_digest IS NULL "
            "AND result_compacted = false "
            "AND outcome_code IS NOT NULL AND completed_at IS NOT NULL)",
            name="ck_task_ssh_effect_outcome_shape",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "task_id",
            "effect_id",
            name="uq_task_ssh_effect_task_effect",
        ),
        mysql_engine="InnoDB",
    )
    op.create_index(
        "ix_task_ssh_effect_receipts_task_id",
        "task_ssh_effect_receipts",
        ["task_id"],
        unique=False,
    )
    _create_sqlite_permit_triggers()
    op.create_index(
        "ix_task_ssh_effect_unknown_digest",
        "task_ssh_effect_receipts",
        ["task_id", "task_incarnation_id", "request_digest", "status"],
        unique=False,
    )
    op.create_index(
        "ix_task_ssh_effect_generation_count",
        "task_ssh_effect_receipts",
        [
            "task_id",
            "task_incarnation_id",
            "task_retry_count",
            "task_turn_generation",
            "task_status",
        ],
        unique=False,
    )


def downgrade() -> None:
    _assert_downgrade_safe()
    _drop_sqlite_permit_triggers()
    op.drop_index(
        "ix_task_ssh_effect_generation_count",
        table_name="task_ssh_effect_receipts",
    )
    op.drop_index(
        "ix_task_ssh_effect_unknown_digest",
        table_name="task_ssh_effect_receipts",
    )
    op.drop_index(
        "ix_task_ssh_effect_receipts_task_id",
        table_name="task_ssh_effect_receipts",
    )
    op.drop_table("task_ssh_effect_receipts")
