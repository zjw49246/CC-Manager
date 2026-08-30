"""Canonical validation boundary for Task-scoped Auto capability policy."""

from copy import deepcopy
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from backend.schemas.capability import AutoCapabilityPolicy

if TYPE_CHECKING:
    from backend.models.task import Task


def normalize_auto_capability_policy(value: Any) -> dict | None:
    """Return canonical JSON or ``None``; reject every malformed non-NULL value."""

    if value is None:
        return None
    try:
        # Pydantic models are mutable by default and ``model_validate`` does
        # not revalidate an instance of the requested class.  Treat even an
        # already-typed policy as untrusted: a caller may have mutated it after
        # construction or produced it through ``model_construct``.  A detached
        # snapshot plus strict validation keeps this canonical boundary from
        # inheriting either invalid fields or caller-owned nested mappings.
        candidate = deepcopy(
            value.model_dump(mode="python")
            if isinstance(value, AutoCapabilityPolicy)
            else value
        )
        policy = AutoCapabilityPolicy.model_validate(candidate, strict=True)
    except ValidationError as exc:
        raise ValueError(f"Invalid capability_policy: {exc}") from exc
    return deepcopy(policy.model_dump(mode="json"))


def validate_auto_capability_task_scope(
    policy: Any,
    *,
    task_id: int | None = None,
    mode: str | None,
    worker_id: int | None,
    shared_from_id: int | None = None,
    delivery_run_id: int | None = None,
    delivery_role: str | None = None,
    plan_target_task_id: int | None = None,
) -> dict | None:
    """Normalize policy and enforce the V1 local ordinary-Task boundary."""

    normalized = normalize_auto_capability_policy(policy)
    if normalized is None:
        return None
    if mode != "auto":
        raise ValueError("capability_policy requires mode=auto")
    if worker_id is not None:
        raise ValueError("capability_policy is local-task only")
    if task_id is not None:
        raise ValueError(
            "Manager-forwarded Worker Tasks cannot use capability_policy"
        )
    if shared_from_id is not None:
        raise ValueError("Shared shadow Tasks cannot use capability_policy")
    if delivery_run_id is not None or delivery_role is not None:
        raise ValueError("Delivery-owned Tasks cannot use capability_policy")
    if plan_target_task_id is not None:
        raise ValueError("Plan helper Tasks cannot use capability_policy")
    return normalized


def build_auto_capability_instructions(task: "Task") -> str | None:
    """Return the terminal protocol for one durably eligible Task.

    Both rollout switches gate model-visible admission.  Existing capability
    executions remain recoverable when either switch is later disabled, but a
    fresh provider turn must not be taught a protocol that its terminal
    consumer will refuse to honor.
    """

    from backend.config import settings

    if not (
        settings.capability_core_enabled
        and settings.auto_capability_enabled
    ):
        return None
    policy = validate_auto_capability_task_scope(
        task.capability_policy,
        task_id=None,
        mode=task.mode,
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        delivery_run_id=task.delivery_run_id,
        delivery_role=task.delivery_role,
        plan_target_task_id=task.plan_target_task_id,
    )
    if policy is None:
        return None

    from backend.services.capability_protocol import (
        build_capability_protocol_instructions,
    )

    enabled = policy["capabilities"]
    protocol = build_capability_protocol_instructions(enabled)
    request_contracts: list[str] = []
    if "plan" in enabled:
        request_contracts.append(
            '- For "plan", request must be an object with a non-empty string '
            '"prompt" and may include a string "title"; do not add other fields.'
        )
    if "code_review" in enabled:
        request_contracts.append(
            '- For "code_review", request must contain exactly string '
            '"base_sha" and "head_sha" using the full immutable Git commit '
            'IDs to review; each ID must be a 40-character lowercase hexadecimal '
            'SHA, never a branch name or abbreviated SHA.'
        )
    if not request_contracts:
        return protocol
    return "\n".join(
        (
            protocol,
            "Capability request contracts for this turn:",
            *request_contracts,
        )
    )
