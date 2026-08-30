"""Durable receipts for Task-scoped mutating SSH effects.

The rows deliberately do not reference ``tasks`` or ``ssh_profiles`` with a
foreign key.  A Task/Profile deletion must not erase evidence that a remote
command or write may already have happened.
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DDL,
    DateTime,
    Index,
    Integer,
    JSON,
    String,
    UniqueConstraint,
    event,
    false,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


TASK_SSH_EFFECT_OPERATIONS = ("execute", "write")
TASK_SSH_EFFECT_STATUSES = (
    "running",
    "completed",
    "ambiguous",
    "aborted",
)
TASK_SSH_EFFECT_OUTCOME_CODES = (
    "success",
    "cancelled_before_execution",
    "generation_changed",
    "authorization_changed",
    "preflight_failed",
    "remote_outcome_unknown",
    "manager_restart_unknown",
)
SQLITE_TASK_SSH_EFFECT_TRIGGER_NAMES = (
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
)


class TaskSSHEffectReceipt(Base):
    """One durable, generation-bound remote mutation attempt.

    ``request_digest`` binds the effect id without retaining a command, file
    body, credential, or remote path.  ``result_payload`` is present only for
    an acknowledged completion and contains the bounded public API response;
    failures retain only a small, enumerated ``outcome_code``.
    """

    __tablename__ = "task_ssh_effect_receipts"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "effect_id",
            name="uq_task_ssh_effect_task_effect",
        ),
        CheckConstraint(
            "LENGTH(effect_id) = 32",
            name="ck_task_ssh_effect_id_length",
        ),
        CheckConstraint(
            "LENGTH(task_incarnation_id) = 32",
            name="ck_task_ssh_effect_incarnation_length",
        ),
        CheckConstraint(
            "LENGTH(request_digest) = 64",
            name="ck_task_ssh_effect_request_digest_length",
        ),
        CheckConstraint(
            "operation IN ('execute', 'write')",
            name="ck_task_ssh_effect_operation",
        ),
        CheckConstraint(
            "status IN ('running', 'completed', 'ambiguous', 'aborted')",
            name="ck_task_ssh_effect_status",
        ),
        CheckConstraint(
            "outcome_code IS NULL OR outcome_code IN ("
            "'success', 'cancelled_before_execution', 'generation_changed', "
            "'authorization_changed', 'preflight_failed', "
            "'remote_outcome_unknown', 'manager_restart_unknown')",
            name="ck_task_ssh_effect_outcome_code",
        ),
        CheckConstraint(
            "task_id > 0 AND task_retry_count >= 0 "
            "AND task_turn_generation >= 0 AND profile_id > 0 "
            "AND profile_revision > 0",
            name="ck_task_ssh_effect_generation",
        ),
        CheckConstraint(
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
        Index(
            "ix_task_ssh_effect_unknown_digest",
            "task_id",
            "task_incarnation_id",
            "request_digest",
            "status",
        ),
        Index(
            "ix_task_ssh_effect_generation_count",
            "task_id",
            "task_incarnation_id",
            "task_retry_count",
            "task_turn_generation",
            "task_status",
        ),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    effect_id: Mapped[str] = mapped_column(String(32), nullable=False)
    task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    task_incarnation_id: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
    )
    task_retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    task_turn_generation: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )
    task_status: Mapped[str] = mapped_column(String(20), nullable=False)
    profile_id: Mapped[int] = mapped_column(Integer, nullable=False)
    profile_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    operation: Mapped[str] = mapped_column(String(16), nullable=False)
    request_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    result_payload: Mapped[dict | None] = mapped_column(
        JSON(none_as_null=True),
        nullable=True,
    )
    result_digest: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    result_compacted: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=false(),
    )
    outcome_code: Mapped[str | None] = mapped_column(
        String(48),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )


# SQLite has no row-level writer lock: holding a no-op UPDATE transaction over
# a 300-second SSH command would freeze every Manager write. These durable
# permit triggers make a committed ``running`` receipt the exact-generation
# fence instead. They reject only mutations of the owning Task/Profile/grant;
# unrelated Tasks remain writable. Production creates the same triggers in the
# Alembic revision below, while these DDL listeners keep ``metadata.create_all``
# test/dev databases faithful to that runtime contract.
_SQLITE_TASK_SSH_EFFECT_TRIGGER_DDL = (
    """
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_task_update
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_task_delete
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_profile_update
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_profile_delete
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_grant_insert
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_grant_update
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_grant_delete
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_task_share_insert
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_task_share_update
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_task_share_delete
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_project_share_insert
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_project_share_update
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
CREATE TRIGGER IF NOT EXISTS trg_task_ssh_effect_project_share_delete
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
)

for _trigger_ddl in _SQLITE_TASK_SSH_EFFECT_TRIGGER_DDL:
    event.listen(
        Base.metadata,
        "after_create",
        DDL(_trigger_ddl).execute_if(dialect="sqlite"),
    )

for _trigger_name in SQLITE_TASK_SSH_EFFECT_TRIGGER_NAMES:
    event.listen(
        Base.metadata,
        "before_drop",
        DDL(f"DROP TRIGGER IF EXISTS {_trigger_name}").execute_if(
            dialect="sqlite"
        ),
    )
