"""Worker-local durable admission and drain claim protocol.

Every mutation that can create executable ownership after a destroy snapshot
serializes on one database row.  The destroy coordinator installs an
irreversible, deterministic claim on that same row before it starts draining.
Whichever transaction wins the row lock defines the safe order:

* a mutation that wins first commits and is visible to the later drain proof;
* a drain that wins first makes every later mutation fail closed.

Background account-login paths additionally persist their attempt id until the
wrapper process and provider-specific credential state have been reconciled.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.worker import (
    WORKER_NODE_CONTROL_SINGLETON_ID,
    WorkerNodeControl,
)


WORKER_NODE_DRAIN_PROTOCOL = 3


class WorkerNodeDrainingConflict(HTTPException):
    def __init__(self, detail: str = "Worker node is draining") -> None:
        super().__init__(status_code=409, detail=detail)


def _require_hex_identity(
    value: object,
    *,
    name: str,
    length: int,
) -> str:
    if (
        not isinstance(value, str)
        or len(value) != length
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError(
            f"{name} must be exactly {length} lowercase hex characters"
        )
    return value


async def _control_snapshot(
    db: AsyncSession,
) -> tuple[str | None, str | None, str | None, str | None]:
    row = (
        await db.execute(
            select(
                WorkerNodeControl.drain_claim,
                WorkerNodeControl.runtime_seal_claim,
                WorkerNodeControl.active_login_attempt_id,
                WorkerNodeControl.active_login_kind,
            ).where(
                WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID
            )
        )
    ).one_or_none()
    if row is None:
        raise RuntimeError(
            "Worker node control state is missing; apply the current database "
            "migration before accepting Worker mutations"
        )
    return (
        row.drain_claim,
        row.runtime_seal_claim,
        row.active_login_attempt_id,
        row.active_login_kind,
    )


async def fence_worker_node_mutation(db: AsyncSession) -> None:
    """Serialize one Worker mutation against the irreversible drain claim.

    The caller must keep this transaction open through its durable mutation.
    Manager databases deliberately skip the Worker-local fence.
    """

    if settings.ccm_node_role != "worker":
        return
    locked = await db.execute(
        update(WorkerNodeControl)
        .where(
            WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID,
            WorkerNodeControl.drain_claim.is_(None),
        )
        .values(updated_at=datetime.utcnow())
        .execution_options(synchronize_session=False)
    )
    if locked.rowcount == 1:
        return
    drain_claim, _, _, _ = await _control_snapshot(db)
    if drain_claim is not None:
        raise WorkerNodeDrainingConflict(
            "Worker node destruction has begun; new mutations are refused"
        )
    raise RuntimeError("Worker node control state could not be locked")


async def fence_worker_node_account_mutation(db: AsyncSession) -> None:
    """Serialize account-pool writes against drain and background login.

    General Task/runtime mutations may proceed while an account login is
    running, so :func:`fence_worker_node_mutation` intentionally checks only
    the irreversible drain claim. Account reload/delete/preference/credential
    writes are different: they share the same pool files and in-memory router
    with login finalization and therefore require the active-login slot to be
    empty as well. The caller holds this writer through its complete mutation.
    """

    if settings.ccm_node_role != "worker":
        return
    locked = await db.execute(
        update(WorkerNodeControl)
        .where(
            WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID,
            WorkerNodeControl.drain_claim.is_(None),
            WorkerNodeControl.active_login_attempt_id.is_(None),
        )
        .values(updated_at=datetime.utcnow())
        .execution_options(synchronize_session=False)
    )
    if locked.rowcount == 1:
        return
    drain_claim, _, active_login, active_kind = await _control_snapshot(db)
    if drain_claim is not None:
        raise WorkerNodeDrainingConflict(
            "Worker node destruction has begun; account mutation is refused"
        )
    if active_login is not None:
        raise WorkerNodeDrainingConflict(
            "Worker account mutation is blocked by active login attempt "
            f"{active_login} ({active_kind or 'unknown'})"
        )
    raise RuntimeError("Worker node account-mutation state could not be locked")


async def fence_worker_node_runtime_persistence(db: AsyncSession) -> bool:
    """Fence one exact already-admitted runtime write against phase two.

    Returns whether phase-one drain has begun.  During phase one the caller
    must still lock and compare the full durable Task incarnation/retry/turn
    and runtime generation in this same transaction; this helper never admits
    a write merely by ``task_id``.  The no-op UPDATE is held through commit, so
    a runtime seal either waits for this write to become visible to backfill or
    wins first and rejects it.  Manager databases retain their no-op behavior.
    """

    if settings.ccm_node_role != "worker":
        return False
    locked = await db.execute(
        update(WorkerNodeControl)
        .where(
            WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID,
            WorkerNodeControl.runtime_seal_claim.is_(None),
        )
        .values(updated_at=datetime.utcnow())
        .execution_options(synchronize_session=False)
    )
    if locked.rowcount == 1:
        drain_claim, _, _, _ = await _control_snapshot(db)
        return drain_claim is not None
    drain_claim, runtime_seal_claim, _, _ = await _control_snapshot(db)
    if runtime_seal_claim is not None:
        raise WorkerNodeDrainingConflict(
            "Worker runtime is sealed; late callback persistence is refused"
        )
    if drain_claim is None:
        raise RuntimeError("Worker node runtime fence could not be locked")
    raise RuntimeError("Worker node runtime seal state is malformed")


async def fence_worker_node_receipt_resolution(db: AsyncSession) -> bool:
    """Serialize cleanup of ownership admitted before an irreversible drain.

    Returns whether a drain claim already exists. Callers may finish an exact
    prepared receipt in either state, but may create new ownership/tombstones
    only when the return value is false. When drain already won, the durable
    node claim itself rejects every later prepare and therefore substitutes
    for a missing-operation rollback tombstone.
    """

    if settings.ccm_node_role != "worker":
        return False
    locked = await db.execute(
        update(WorkerNodeControl)
        .where(WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID)
        .values(updated_at=datetime.utcnow())
        .execution_options(synchronize_session=False)
    )
    if locked.rowcount != 1:
        raise RuntimeError("Worker node control state could not be locked")
    drain_claim, _, _, _ = await _control_snapshot(db)
    return drain_claim is not None


async def require_worker_node_destroy_cleanup_claim(
    db: AsyncSession,
    *,
    claim: str,
) -> None:
    """Authorize only exact phase-one destroy cleanup in a held node writer.

    The caller first takes :func:`fence_worker_node_receipt_resolution`, then
    validates its authenticated request's opaque claim before locking Task and
    receipt rows.  A different claim, an unclaimed node, or phase two all fail
    closed; ordinary deployment-token traffic cannot use this exception.
    """

    canonical_claim = _require_hex_identity(
        claim,
        name="drain claim",
        length=64,
    )
    current_claim, runtime_seal_claim, _, _ = await _control_snapshot(db)
    if current_claim != canonical_claim:
        raise WorkerNodeDrainingConflict(
            "Worker destroy cleanup claim is absent or does not match"
        )
    if runtime_seal_claim is not None:
        raise WorkerNodeDrainingConflict(
            "Worker runtime is already sealed; destroy cleanup is closed"
        )


async def begin_worker_node_drain(
    db: AsyncSession,
    *,
    claim: str,
) -> None:
    """Install or idempotently resume one exact irreversible drain claim."""

    canonical_claim = _require_hex_identity(
        claim,
        name="drain claim",
        length=64,
    )
    now = datetime.utcnow()
    claimed = await db.execute(
        update(WorkerNodeControl)
        .where(
            WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID,
            WorkerNodeControl.active_login_attempt_id.is_(None),
            (
                WorkerNodeControl.drain_claim.is_(None)
                | (WorkerNodeControl.drain_claim == canonical_claim)
            ),
        )
        .values(
            drain_claim=canonical_claim,
            # Exact-claim replay is idempotent evidence reconciliation, not a
            # new drain. Preserve when the irreversible claim first won.
            drain_started_at=func.coalesce(
                WorkerNodeControl.drain_started_at,
                now,
            ),
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if claimed.rowcount == 1:
        return
    current_claim, _, active_login, active_kind = await _control_snapshot(db)
    if active_login is not None:
        raise WorkerNodeDrainingConflict(
            "Worker node drain is blocked by active account login attempt "
            f"{active_login} ({active_kind or 'unknown'})"
        )
    if current_claim is not None and current_claim != canonical_claim:
        raise WorkerNodeDrainingConflict(
            "Worker node already belongs to a different drain claim"
        )
    raise RuntimeError("Worker node drain claim could not be installed")


async def fence_worker_node_drain_claim(
    db: AsyncSession,
    *,
    claim: str,
    require_runtime_seal: bool = False,
) -> bool:
    """Lock and verify the claim, optionally requiring its exact phase two."""

    canonical_claim = _require_hex_identity(
        claim,
        name="drain claim",
        length=64,
    )
    locked = await db.execute(
        update(WorkerNodeControl)
        .where(
            WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID,
            WorkerNodeControl.drain_claim == canonical_claim,
            WorkerNodeControl.active_login_attempt_id.is_(None),
            *(
                (WorkerNodeControl.runtime_seal_claim == canonical_claim,)
                if require_runtime_seal
                else ()
            ),
        )
        .values(updated_at=datetime.utcnow())
        .execution_options(synchronize_session=False)
    )
    if locked.rowcount == 1:
        _, runtime_seal_claim, _, _ = await _control_snapshot(db)
        return runtime_seal_claim == canonical_claim
    current_claim, runtime_seal_claim, active_login, active_kind = (
        await _control_snapshot(db)
    )
    if current_claim != canonical_claim:
        raise RuntimeError("Worker node drain claim is absent or does not match")
    if require_runtime_seal and runtime_seal_claim != canonical_claim:
        raise RuntimeError(
            "Worker node runtime seal is absent or does not match the drain claim"
        )
    raise RuntimeError(
        "Worker node drain proof is blocked by active account login attempt "
        f"{active_login or 'unknown'} ({active_kind or 'unknown'})"
    )


async def begin_worker_node_runtime_seal(
    db: AsyncSession,
    *,
    claim: str,
) -> None:
    """Install phase two while holding the node writer through caller checks.

    Callers must keep this transaction open while locking all Task/runtime
    evidence.  If their quiescence scan finds a blocker they roll back, which
    also rolls back a newly installed seal.  Replaying an already committed
    seal with the same claim is idempotent; a different claim is rejected.
    """

    canonical_claim = _require_hex_identity(
        claim,
        name="drain claim",
        length=64,
    )
    now = datetime.utcnow()
    sealed = await db.execute(
        update(WorkerNodeControl)
        .where(
            WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID,
            WorkerNodeControl.drain_claim == canonical_claim,
            WorkerNodeControl.active_login_attempt_id.is_(None),
            (
                WorkerNodeControl.runtime_seal_claim.is_(None)
                | (WorkerNodeControl.runtime_seal_claim == canonical_claim)
            ),
        )
        .values(
            runtime_seal_claim=canonical_claim,
            # A restarted destroy coordinator may replay the same phase-two
            # claim; it must not rewrite the original durable seal time.
            runtime_sealed_at=func.coalesce(
                WorkerNodeControl.runtime_sealed_at,
                now,
            ),
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if sealed.rowcount == 1:
        return
    current_claim, runtime_seal_claim, active_login, active_kind = (
        await _control_snapshot(db)
    )
    if current_claim != canonical_claim:
        raise WorkerNodeDrainingConflict(
            "Worker node drain claim is absent or belongs to another destroy"
        )
    if runtime_seal_claim is not None and runtime_seal_claim != canonical_claim:
        raise WorkerNodeDrainingConflict(
            "Worker runtime seal belongs to another destroy claim"
        )
    raise WorkerNodeDrainingConflict(
        "Worker runtime seal is blocked by active account login attempt "
        f"{active_login or 'unknown'} ({active_kind or 'unknown'})"
    )


async def admit_worker_node_login_attempt(
    db: AsyncSession,
    *,
    attempt_id: str,
    kind: str = "codex",
) -> bool:
    """Persist one Worker-local background login before spawning its process."""

    if settings.ccm_node_role != "worker":
        return False
    canonical_attempt = _require_hex_identity(
        attempt_id,
        name="login attempt id",
        length=32,
    )
    if kind not in {"codex", "claude_ssh"}:
        raise ValueError("Worker login kind is invalid")
    admitted = await db.execute(
        update(WorkerNodeControl)
        .where(
            WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID,
            WorkerNodeControl.drain_claim.is_(None),
            WorkerNodeControl.active_login_attempt_id.is_(None),
        )
        .values(
            active_login_attempt_id=canonical_attempt,
            active_login_kind=kind,
            updated_at=datetime.utcnow(),
        )
        .execution_options(synchronize_session=False)
    )
    if admitted.rowcount == 1:
        return True
    drain_claim, _, active_login, active_kind = await _control_snapshot(db)
    if drain_claim is not None:
        raise WorkerNodeDrainingConflict(
            "Worker node destruction has begun; account login is refused"
        )
    raise WorkerNodeDrainingConflict(
        "Worker node already has active account login attempt "
        f"{active_login or 'unknown'} ({active_kind or 'unknown'})"
    )


async def finish_worker_node_login_attempt(
    db: AsyncSession,
    *,
    attempt_id: str,
    kind: str = "codex",
) -> bool:
    """Clear only the exact fully reconciled login attempt."""

    if settings.ccm_node_role != "worker":
        return False
    canonical_attempt = _require_hex_identity(
        attempt_id,
        name="login attempt id",
        length=32,
    )
    if kind not in {"codex", "claude_ssh"}:
        raise ValueError("Worker login kind is invalid")
    cleared = await db.execute(
        update(WorkerNodeControl)
        .where(
            WorkerNodeControl.id == WORKER_NODE_CONTROL_SINGLETON_ID,
            WorkerNodeControl.active_login_attempt_id == canonical_attempt,
            WorkerNodeControl.active_login_kind == kind,
        )
        .values(
            active_login_attempt_id=None,
            active_login_kind=None,
            updated_at=datetime.utcnow(),
        )
        .execution_options(synchronize_session=False)
    )
    return cleared.rowcount == 1


async def recover_worker_node_login_after_restart(
    db: AsyncSession,
    *,
    unresolved_attempt_ids: set[str],
) -> bool:
    """Clear a crash-left login fence only after journal recovery proved idle."""

    if settings.ccm_node_role != "worker":
        return False
    _, _, active_login, active_kind = await _control_snapshot(db)
    if active_login is None:
        return False
    if active_kind != "codex":
        raise RuntimeError(
            "Worker restart cannot prove completion of crash-left "
            f"{active_kind or 'unknown'} login attempt {active_login}"
        )
    if active_login in unresolved_attempt_ids:
        raise RuntimeError(
            "Worker Codex login recovery remains unresolved for attempt "
            f"{active_login}"
        )
    return await finish_worker_node_login_attempt(
        db,
        attempt_id=active_login,
        kind="codex",
    )
