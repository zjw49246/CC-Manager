"""Durable, dialect-neutral Task-id ownership for Manager and Worker nodes.

Manager-created Tasks retain the historical database-native low namespace.
Worker-local derived Tasks (currently Browser Harness children) use a separate
transactional high namespace.  Manager mirrors copied to a Worker keep their
original low id, so both nodes can continue to address the same logical Task
without allowing either node's local allocator to collide with the other.
"""

from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.task import Task
from backend.models.task_id_allocator import (
    TASK_ID_ALLOCATOR_SINGLETON_ID,
    TASK_ID_SIGNED_INT_MAX,
    TASK_ID_WORKER_NAMESPACE_START,
    TaskIdAllocator,
)
from backend.services.skill_context import (
    WORKER_MANAGED_TASK_METADATA_KEY,
    is_worker_managed_task_metadata,
)


TASK_ID_NAMESPACE_PROTOCOL = 1
TASK_ID_NODE_ROLES = frozenset({"manager", "worker"})
_LEGACY_SCAN_BATCH_SIZE = 1_000


class TaskIdNamespaceError(RuntimeError):
    """The database cannot safely participate in the configured namespace."""


class TaskIdNamespaceProtocolError(TaskIdNamespaceError):
    """A remote Worker does not advertise the exact namespace contract."""


def _canonical_node_role(node_role: object) -> str:
    if node_role not in TASK_ID_NODE_ROLES:
        raise TaskIdNamespaceError(
            "CCM_NODE_ROLE must be exactly 'manager' or 'worker'"
        )
    return str(node_role)


async def _allocator_state(
    db: AsyncSession,
) -> tuple[str | None, int]:
    row = (
        await db.execute(
            select(
                TaskIdAllocator.node_role,
                TaskIdAllocator.next_worker_task_id,
            ).where(TaskIdAllocator.id == TASK_ID_ALLOCATOR_SINGLETON_ID)
        )
    ).one_or_none()
    if row is None:
        raise TaskIdNamespaceError(
            "Task-id allocator state is missing; apply the current database "
            "migration before creating Tasks"
        )
    role, next_worker_task_id = row
    if role is not None and role not in TASK_ID_NODE_ROLES:
        raise TaskIdNamespaceError("Task-id allocator has an invalid node role")
    if (
        type(next_worker_task_id) is not int
        or next_worker_task_id < TASK_ID_WORKER_NAMESPACE_START
        or next_worker_task_id > TASK_ID_SIGNED_INT_MAX
    ):
        raise TaskIdNamespaceError(
            "Task-id allocator high-range cursor is corrupt"
        )
    return role, next_worker_task_id


async def _validate_legacy_tasks_for_role(
    db: AsyncSession,
    *,
    node_role: str,
) -> None:
    """Prove that pre-protocol rows are safe before binding this database.

    A Worker may already contain low-range Manager mirrors from an older
    release.  Those rows are safe only when their durable metadata proves
    Manager ownership.  Any old Worker-local low row is ambiguous and cannot
    be remapped without rewriting every foreign key, log and external receipt,
    so upgrade fails closed and the Worker must be rebuilt.
    """

    first_high_id = await db.scalar(
        select(Task.id)
        .where(Task.id >= TASK_ID_WORKER_NAMESPACE_START)
        .order_by(Task.id)
        .limit(1)
    )
    if first_high_id is not None:
        raise TaskIdNamespaceError(
            f"Cannot bind database as {node_role}: legacy Task "
            f"{first_high_id} already occupies the reserved Worker-local "
            "namespace"
        )

    last_id: int | None = None
    while True:
        statement = select(Task.id, Task.metadata_).where(
            Task.id < TASK_ID_WORKER_NAMESPACE_START
        )
        if last_id is not None:
            statement = statement.where(Task.id > last_id)
        rows = (
            await db.execute(
                statement.order_by(Task.id).limit(_LEGACY_SCAN_BATCH_SIZE)
            )
        ).all()
        if not rows:
            return
        for task_id, metadata in rows:
            worker_managed = is_worker_managed_task_metadata(metadata)
            # ``ccm_user_skill_snapshots`` is intentionally accepted as a
            # compatibility marker when an operator has explicitly configured
            # this database as a Worker: old Manager->Worker copies always
            # carried that snapshot before the dedicated marker existed.
            # It is *not* proof that a database is a Worker when starting as a
            # Manager, because administrators can also attach frozen User
            # Skill snapshots to ordinary Manager-local Tasks.  Only the
            # dedicated mirror marker is unambiguous enough to reject a
            # Manager role claim.
            explicit_worker_mirror = bool(
                isinstance(metadata, Mapping)
                and metadata.get(WORKER_MANAGED_TASK_METADATA_KEY) is True
            )
            if node_role == "worker" and not worker_managed:
                raise TaskIdNamespaceError(
                    "Cannot bind database as worker: legacy low-range Task "
                    f"{task_id} is not a proven Manager mirror; rebuild this "
                    "Worker before accepting new Tasks"
                )
            if node_role == "manager" and explicit_worker_mirror:
                raise TaskIdNamespaceError(
                    "Cannot bind database as manager: legacy low-range Task "
                    f"{task_id} is marked as a Manager mirror, so this "
                    "database appears to belong to a Worker; configure "
                    "CCM_NODE_ROLE=worker before startup"
                )
        last_id = rows[-1][0]


async def bind_task_id_node_role(
    db: AsyncSession,
    *,
    node_role: str,
) -> None:
    """Atomically bind an unclaimed database to one immutable node role.

    The no-op UPDATE is intentional.  It takes the allocator row's writer
    lock on PostgreSQL/MySQL and SQLite without durably claiming the role
    before legacy validation succeeds.  Thus even a caller that catches the
    validation exception and commits cannot accidentally bless unsafe data.
    """

    role = _canonical_node_role(node_role)
    await db.execute(
        update(TaskIdAllocator)
        .where(
            TaskIdAllocator.id == TASK_ID_ALLOCATOR_SINGLETON_ID,
            TaskIdAllocator.node_role.is_(None),
        )
        .values(next_worker_task_id=TaskIdAllocator.next_worker_task_id)
        .execution_options(synchronize_session=False)
    )
    persisted_role, _ = await _allocator_state(db)
    if persisted_role is not None:
        if persisted_role != role:
            raise TaskIdNamespaceError(
                "Task database is already bound to node role "
                f"'{persisted_role}' and cannot run as '{role}'"
            )
        return

    await _validate_legacy_tasks_for_role(db, node_role=role)
    claimed = await db.execute(
        update(TaskIdAllocator)
        .where(
            TaskIdAllocator.id == TASK_ID_ALLOCATOR_SINGLETON_ID,
            TaskIdAllocator.node_role.is_(None),
        )
        .values(node_role=role)
        .execution_options(synchronize_session=False)
    )
    if claimed.rowcount != 1:
        # This should be unreachable while the preceding write lock is held,
        # but never infer ownership from an unusual driver rowcount.
        persisted_role, _ = await _allocator_state(db)
        if persisted_role != role:
            raise TaskIdNamespaceError(
                "Task database node-role claim changed concurrently"
            )


async def bind_task_id_namespace_at_startup(
    db_factory,
    *,
    node_role: str,
) -> None:
    """Bind and validate the database before any runtime producer starts."""

    async with db_factory() as db:
        try:
            await bind_task_id_node_role(db, node_role=node_role)
            await db.commit()
        except BaseException:
            await db.rollback()
            raise


async def allocate_worker_local_task_id(
    db: AsyncSession,
) -> int:
    """Reserve one Worker-local high id in the caller's transaction.

    UPDATE-before-SELECT is the portable concurrency primitive here: row locks
    serialize PostgreSQL/MySQL, while SQLite's single writer serializes the
    same statement.  Allocation and Task INSERT therefore commit or roll back
    together; a failed transaction does not leak or incorrectly skip an id.
    """

    incremented = await db.execute(
        update(TaskIdAllocator)
        .where(
            TaskIdAllocator.id == TASK_ID_ALLOCATOR_SINGLETON_ID,
            TaskIdAllocator.node_role == "worker",
            TaskIdAllocator.next_worker_task_id < TASK_ID_SIGNED_INT_MAX,
        )
        .values(
            next_worker_task_id=(
                TaskIdAllocator.next_worker_task_id + 1
            )
        )
        .execution_options(synchronize_session=False)
    )
    if incremented.rowcount != 1:
        await bind_task_id_node_role(db, node_role="worker")
        _, next_worker_task_id = await _allocator_state(db)
        if next_worker_task_id >= TASK_ID_SIGNED_INT_MAX:
            raise TaskIdNamespaceError(
                "Worker-local Task-id namespace is exhausted"
            )
        incremented = await db.execute(
            update(TaskIdAllocator)
            .where(
                TaskIdAllocator.id == TASK_ID_ALLOCATOR_SINGLETON_ID,
                TaskIdAllocator.node_role == "worker",
                TaskIdAllocator.next_worker_task_id
                < TASK_ID_SIGNED_INT_MAX,
            )
            .values(
                next_worker_task_id=(
                    TaskIdAllocator.next_worker_task_id + 1
                )
            )
            .execution_options(synchronize_session=False)
        )
        if incremented.rowcount != 1:
            raise TaskIdNamespaceError(
                "Worker-local Task-id allocation lost its durable owner"
            )

    _, next_worker_task_id = await _allocator_state(db)
    allocated = next_worker_task_id - 1
    if not (
        TASK_ID_WORKER_NAMESPACE_START
        <= allocated
        < TASK_ID_SIGNED_INT_MAX
    ):
        raise TaskIdNamespaceError(
            "Worker-local Task-id allocator returned an invalid value"
        )
    return allocated


async def fence_worker_task_insert(
    db: AsyncSession,
    *,
    bind_if_needed: bool = True,
) -> None:
    """Serialize every Worker Task INSERT with node-wide drain proof.

    High-range Worker-local allocation already takes this allocator row's
    writer lock while it advances the cursor.  Low-range Manager mirrors do
    not advance the cursor, but must still take the *same* durable lock: a
    drain snapshot cannot safely infer that the Worker is empty while an
    initial Manager ``POST /api/tasks`` is waiting to commit an explicit id.

    ``UPDATE`` rather than ``SELECT FOR UPDATE`` is intentional.  It is a row
    lock on PostgreSQL/MySQL and a database writer reservation on SQLite.  The
    no-op value also keeps the fence in the caller's Task INSERT transaction.
    Drain proof passes ``bind_if_needed=False`` so an unbound or wrongly bound
    database is rejected instead of being claimed as a Worker by an audit.
    """

    if bind_if_needed:
        await bind_task_id_node_role(db, node_role="worker")
    fenced = await db.execute(
        update(TaskIdAllocator)
        .where(
            TaskIdAllocator.id == TASK_ID_ALLOCATOR_SINGLETON_ID,
            TaskIdAllocator.node_role == "worker",
        )
        .values(
            next_worker_task_id=TaskIdAllocator.next_worker_task_id
        )
        .execution_options(synchronize_session=False)
    )
    if fenced.rowcount != 1:
        persisted_role, _ = await _allocator_state(db)
        raise TaskIdNamespaceError(
            "Worker Task-id allocator is not durably bound to this node"
            if persisted_role is None
            else "Task database is bound to node role "
            f"'{persisted_role}', not 'worker'"
        )


async def task_id_for_insert(
    db: AsyncSession,
    *,
    node_role: str,
    explicit_id: int | None,
) -> int | None:
    """Resolve the primary key shape for one canonical Task INSERT."""

    role = _canonical_node_role(node_role)
    if explicit_id is not None and (
        type(explicit_id) is not int or explicit_id <= 0
    ):
        raise TaskIdNamespaceError("Explicit Task id must be a positive integer")

    if role == "manager":
        if explicit_id is not None:
            raise TaskIdNamespaceError(
                "Manager databases cannot accept explicit Task ids"
            )
        await bind_task_id_node_role(db, node_role=role)
        return None

    if explicit_id is not None:
        if explicit_id >= TASK_ID_WORKER_NAMESPACE_START:
            raise TaskIdNamespaceError(
                "Manager-mirrored Task ids must stay below the Worker-local "
                "namespace boundary"
            )
        await fence_worker_task_insert(db)
        return explicit_id

    return await allocate_worker_local_task_id(db)


def validate_manager_allocated_task_id(task_id: object) -> int:
    """Reject native Manager allocation if its low namespace is exhausted."""

    if (
        type(task_id) is not int
        or task_id <= 0
        or task_id >= TASK_ID_WORKER_NAMESPACE_START
    ):
        raise TaskIdNamespaceError(
            "Manager Task-id namespace is exhausted or corrupt"
        )
    return task_id


def validate_worker_task_id_namespace_config(
    config: Mapping[str, object] | object,
) -> None:
    """Verify the exact protocol advertised by a remote Worker."""

    if not isinstance(config, Mapping):
        raise TaskIdNamespaceProtocolError(
            "Worker returned an invalid Task-id namespace configuration"
        )
    if (
        type(config.get("task_id_namespace_protocol")) is not int
        or config.get("task_id_namespace_protocol")
        != TASK_ID_NAMESPACE_PROTOCOL
    ):
        raise TaskIdNamespaceProtocolError(
            "Worker does not support the required Task-id namespace protocol"
        )
    if config.get("ccm_node_role") != "worker":
        raise TaskIdNamespaceProtocolError(
            "Remote CCM is not configured with CCM_NODE_ROLE=worker"
        )
    if (
        type(config.get("task_id_namespace_boundary")) is not int
        or config.get("task_id_namespace_boundary")
        != TASK_ID_WORKER_NAMESPACE_START
    ):
        raise TaskIdNamespaceProtocolError(
            "Worker Task-id namespace boundary does not match the Manager"
        )
