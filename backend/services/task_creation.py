"""Canonical transaction-aware creation boundary for executable Tasks."""

from collections.abc import Mapping
import secrets

from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.task import Task
from backend.models.task_share import TaskShare
from backend.models.team_share import TeamTaskShare
from backend.services.auto_capability_policy import (
    validate_auto_capability_task_scope,
)
from backend.services.codex_models import validate_codex_service_tier
from backend.services.task_id_namespace import (
    TaskIdNamespaceError,
    task_id_for_insert,
    validate_manager_allocated_task_id,
)
from backend.services.worker_assignment import fence_ready_worker_assignment
from backend.services.worker_node_control import fence_worker_node_mutation


SOURCE_TASK_INCARNATION_METADATA_KEY = "ccm_source_task_incarnation_id"

TASK_EXECUTION_ROLES = frozenset({"member", "admin", "super_admin"})
TASK_EXECUTION_MODES = frozenset({"sandbox", "unrestricted"})
TASK_EXECUTION_PRINCIPAL_KINDS = frozenset({
    "user",
    "deployment_token",
    "system",
    "delegated_user",
    "delegated_deployment_token",
})
TASK_EXECUTION_USER_PRINCIPAL_KINDS = frozenset({
    "user",
    "delegated_user",
})
TASK_EXECUTION_TOKEN_PRINCIPAL_KINDS = frozenset({
    "deployment_token",
    "delegated_deployment_token",
})
TASK_EXECUTION_DELEGATED_PRINCIPAL_KINDS = frozenset({
    "delegated_user",
    "delegated_deployment_token",
})
TASK_EXECUTION_WORKER_PRINCIPAL_KINDS = (
    TASK_EXECUTION_DELEGATED_PRINCIPAL_KINDS | {"system"}
)


def task_execution_principal_values(
    *,
    user_id: int | None,
    role: str,
    principal_kind: str,
) -> dict[str, object]:
    """Build one complete, non-contradictory Task principal snapshot."""

    if role not in TASK_EXECUTION_ROLES:
        raise ValueError("invalid Task execution role")
    if principal_kind not in TASK_EXECUTION_PRINCIPAL_KINDS:
        raise ValueError("invalid Task execution principal kind")
    if principal_kind in TASK_EXECUTION_USER_PRINCIPAL_KINDS:
        if (
            isinstance(user_id, bool)
            or not isinstance(user_id, int)
            or user_id <= 0
        ):
            raise ValueError("user Task execution principal requires a user id")
    elif user_id is not None:
        raise ValueError("non-user Task execution principal cannot carry a user id")
    if principal_kind == "system":
        if role != "member":
            raise ValueError("system Task principal must be member")
        mode = "sandbox"
    elif principal_kind in TASK_EXECUTION_TOKEN_PRINCIPAL_KINDS:
        if role != "super_admin":
            raise ValueError(
                "deployment token Task principal must be super_admin"
            )
        mode = "unrestricted"
    else:
        mode = (
            "unrestricted"
            if role in {"admin", "super_admin"}
            else "sandbox"
        )
    return {
        "execution_user_id": user_id,
        "execution_user_role": role,
        "execution_mode": mode,
        "execution_principal_kind": principal_kind,
    }


def delegated_task_execution_principal_values(
    *,
    user_id: int | None,
    role: str,
    principal_kind: str,
) -> dict[str, object]:
    """Convert one trusted Manager principal into its Worker wire form.

    A Worker bearer token authenticates the Manager control plane; it must
    never become the runtime principal itself.  The delegated kind makes that
    trust boundary explicit while retaining the exact user id/role snapshot.
    Already-delegated values are accepted idempotently so retry/reconciliation
    code can canonicalize an envelope without silently changing authority.
    """

    if principal_kind in {"user", "delegated_user"}:
        delegated_kind = "delegated_user"
    elif principal_kind in {
        "deployment_token",
        "delegated_deployment_token",
    }:
        delegated_kind = "delegated_deployment_token"
    elif principal_kind == "system":
        delegated_kind = "system"
    else:
        raise ValueError("invalid Task execution principal kind")
    return task_execution_principal_values(
        user_id=user_id,
        role=role,
        principal_kind=delegated_kind,
    )


def system_task_execution_principal_values() -> dict[str, object]:
    """Return the fail-closed principal for derived/cross-boundary Tasks."""

    return task_execution_principal_values(
        user_id=None,
        role="member",
        principal_kind="system",
    )


async def purge_task_access_grants(db: AsyncSession, task_id: int) -> None:
    """Remove every ACL row whose authority is one exact Task id.

    SQLite deployments have historically run without foreign-key enforcement,
    and ``TeamTaskShare`` has no database foreign key at all.  Explicit cleanup
    is therefore required both when deleting a Task and when admitting a new
    Task whose integer id may have been reused.
    """

    from sqlalchemy import delete
    from backend.models.task_ssh_grant import TaskSSHGrant

    await db.execute(
        delete(TaskSSHGrant).where(TaskSSHGrant.task_id == task_id)
    )
    await db.execute(delete(TaskShare).where(TaskShare.task_id == task_id))
    await db.execute(
        delete(TeamTaskShare).where(TeamTaskShare.task_id == task_id)
    )


def resolve_task_runtime_defaults(
    *,
    provider: str | None,
    model: str | None,
    effort_level: str | None,
) -> tuple[str, str, str]:
    """Resolve the explicit provider/model/effort tuple for a new Task."""

    raw_provider = settings.default_provider if provider is None else provider
    if not isinstance(raw_provider, str) or not raw_provider.strip():
        raise ValueError("provider must be 'claude' or 'codex'")
    resolved_provider = raw_provider.strip().lower()
    if resolved_provider not in {"claude", "codex"}:
        raise ValueError("provider must be 'claude' or 'codex'")
    resolved_model = model or (
        settings.default_codex_model
        if resolved_provider == "codex"
        else settings.default_model
    )
    resolved_effort = effort_level or settings.default_effort
    return resolved_provider, resolved_model, resolved_effort


def prepare_task_create_values(values: Mapping[str, object]) -> dict:
    """Return canonical persisted values shared by every creation adapter."""

    prepared = dict(values)
    principal_values = {
        key: prepared.get(key)
        for key in (
            "execution_user_id",
            "execution_user_role",
            "execution_mode",
            "execution_principal_kind",
        )
    }
    if all(value is None for value in principal_values.values()):
        prepared.update(system_task_execution_principal_values())
    else:
        role = principal_values["execution_user_role"]
        mode = principal_values["execution_mode"]
        kind = principal_values["execution_principal_kind"]
        if (
            role not in TASK_EXECUTION_ROLES
            or mode not in TASK_EXECUTION_MODES
            or kind not in TASK_EXECUTION_PRINCIPAL_KINDS
        ):
            raise ValueError("invalid Task execution principal")
        expected = task_execution_principal_values(
            user_id=principal_values["execution_user_id"],
            role=role,
            principal_kind=kind,
        )
        if principal_values != expected:
            raise ValueError("Task execution principal role/mode mismatch")
    policy = validate_auto_capability_task_scope(
        prepared.get("capability_policy"),
        task_id=prepared.get("id"),
        mode=prepared.get("mode") or "auto",
        worker_id=prepared.get("worker_id"),
        shared_from_id=prepared.get("shared_from_id"),
        delivery_run_id=prepared.get("delivery_run_id"),
        delivery_role=prepared.get("delivery_role"),
        plan_target_task_id=prepared.get("plan_target_task_id"),
    )
    if policy is None:
        # Generic JSON otherwise serializes Python None as a JSON ``null`` on
        # some dialects. Omission preserves the SQL NULL disabled state.
        prepared.pop("capability_policy", None)
    else:
        prepared["capability_policy"] = policy
    provider, model, effort_level = resolve_task_runtime_defaults(
        provider=prepared.get("provider"),
        model=prepared.get("model"),
        effort_level=prepared.get("effort_level"),
    )
    prepared.update(
        provider=provider,
        model=model,
        effort_level=effort_level,
        codex_service_tier=prepared.get("codex_service_tier") or "default",
    )
    return prepared


async def stage_task_record(
    db: AsyncSession,
    *,
    source_incarnation_id: str | None = None,
    **values,
) -> Task:
    """Add and flush one canonical Task without owning the transaction.

    Callers such as Plan materialization can atomically persist related rows
    before committing.  Standalone creation adapters may commit immediately.
    """

    prepared = prepare_task_create_values(values)
    # Incarnations are system authority, never public caller-controlled. An
    # internal explicit-id Worker mirror carries the Manager's exact logical
    # incarnation so remote mutations can close the same-id ABA window.
    if source_incarnation_id is not None:
        if values.get("id") is None:
            raise ValueError(
                "source Task incarnation requires an internal explicit id"
            )
        prepared["incarnation_id"] = source_incarnation_id
        metadata = dict(prepared.get("metadata_") or {})
        metadata[SOURCE_TASK_INCARNATION_METADATA_KEY] = source_incarnation_id
        prepared["metadata_"] = metadata
    else:
        prepared["incarnation_id"] = secrets.token_hex(16)
    validate_task_service_tier_configuration(
        provider=prepared["provider"],
        model=prepared["model"],
        codex_service_tier=prepared["codex_service_tier"],
        mode=prepared.get("mode"),
        goal_evaluator_model=prepared.get("goal_evaluator_model"),
    )
    # A Worker destroy installs its irreversible node claim on this exact
    # database fence before taking any drain snapshot. Keep the lock through
    # allocation and INSERT so either this Task is fully visible to the proof
    # or the already-draining node rejects it.
    await fence_worker_node_mutation(db)
    # Keep this fence in the same transaction as the Task insert.  An earlier
    # Worker availability read cannot serialize against a concurrent destroy.
    await fence_ready_worker_assignment(db, prepared.get("worker_id"))
    explicit_id = prepared.get("id")
    resolved_id = await task_id_for_insert(
        db,
        node_role=settings.ccm_node_role,
        explicit_id=explicit_id,
    )
    if resolved_id is not None:
        prepared["id"] = resolved_id
    task = Task(**prepared)
    db.add(task)
    await db.flush()
    if settings.ccm_node_role == "manager":
        try:
            validate_manager_allocated_task_id(task.id)
        except TaskIdNamespaceError:
            # Keep the low-range boundary fail-closed even if an internal
            # caller catches the exception and later commits this transaction.
            # Native sequences may remain exhausted (a safe gap), but the
            # reserved Worker-local row must never become durable on Manager.
            await db.delete(task)
            await db.flush()
            raise
    # A newly flushed Task cannot yet have legitimate grants.  Clear orphaned
    # rows left by older deployments before this reused integer id becomes
    # visible, otherwise the new Task would inherit the previous Task's ACL.
    await purge_task_access_grants(db, task.id)
    return task


def validate_task_service_tier_configuration(
    *,
    provider: str | None,
    model: str | None,
    codex_service_tier: str | None,
    mode: str | None,
    goal_evaluator_model: str | None,
) -> None:
    """Validate every model request hidden behind one Task configuration."""

    validate_codex_service_tier(provider, model, codex_service_tier)
    if (
        (provider or "claude").lower() == "codex"
        and (codex_service_tier or "default") == "priority"
        and mode == "plan"
    ):
        raise ValueError(
            "Codex Fast is not supported for read-only Plan Agent tasks; "
            "use Standard"
        )
    if not (
        (provider or "claude").lower() == "codex"
        and (codex_service_tier or "default") == "priority"
        and mode == "goal"
    ):
        return

    task_model = model
    if not task_model or task_model == "default":
        task_model = settings.default_codex_model
    evaluator_model = goal_evaluator_model
    if not evaluator_model or evaluator_model == "default":
        evaluator_model = task_model
    if evaluator_model != task_model:
        raise ValueError(
            "Codex Fast Goal tasks must use the Task model for goal "
            "evaluation; clear goal_evaluator_model or select the same model"
        )
    validate_codex_service_tier("codex", evaluator_model, "priority")
