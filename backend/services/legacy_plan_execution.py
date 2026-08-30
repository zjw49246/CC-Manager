"""Exact compatibility proof for pre-Plan-v2 execution carriers."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.task import Task


LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION = 1


@dataclass(frozen=True, slots=True)
class LegacyPlanExecutionCarrierProof:
    """Cross-node proof for one already-materialized legacy carrier.

    Plan/Version/Application primary keys are node-local because Manager and
    Worker databases contain different row sets.  The proof therefore binds
    only globally meaningful identity, immutable Version content, and the
    portable Task runtime configuration which both nodes must execute.
    Mutable lifecycle state is returned beside the digest for reconciliation,
    never folded into the authority itself.
    """

    task_id: int
    version_number: int
    version_content_sha256: str
    execution_fingerprint_sha256: str
    proof_digest: str
    task_status: str
    retry_count: int
    turn_generation: int

    def to_wire(self) -> dict[str, Any]:
        return {
            "protocol_version": (
                LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
            ),
            "task_id": self.task_id,
            "version_number": self.version_number,
            "version_content_sha256": self.version_content_sha256,
            "execution_fingerprint_sha256": (
                self.execution_fingerprint_sha256
            ),
            "proof_digest": self.proof_digest,
            "task_status": self.task_status,
            "retry_count": self.retry_count,
            "turn_generation": self.turn_generation,
        }


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _portable_execution_fingerprint(
    task: Task | Mapping[str, Any],
) -> dict[str, Any]:
    """Return only Task fields whose values share meaning across nodes."""

    def value(key: str) -> Any:
        if isinstance(task, Mapping):
            if key not in task:
                raise KeyError(key)
            return task[key]
        return getattr(task, key)

    raw_metadata = value("metadata_")
    metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
    portable_metadata = {
        key: metadata[key]
        for key in (
            "image_paths",
            "secret_ids",
            "ccm_user_skill_snapshots",
        )
        if key in metadata
    }
    return {
        "task_id": value("id"),
        "title": value("title"),
        "description": value("description"),
        "target_branch": value("target_branch"),
        "priority": value("priority"),
        "max_retries": value("max_retries"),
        "mode": value("mode"),
        "todo_file_path": value("todo_file_path"),
        "max_iterations": value("max_iterations"),
        "must_complete": value("must_complete"),
        "goal_condition": value("goal_condition"),
        "goal_max_turns": value("goal_max_turns"),
        "goal_evaluator_model": value("goal_evaluator_model"),
        "provider": value("provider"),
        "model": value("model"),
        "codex_service_tier": value("codex_service_tier"),
        "effort_level": value("effort_level"),
        "thinking_budget": value("thinking_budget"),
        "system_prompt_mode": value("system_prompt_mode"),
        "timeout_hours": value("timeout_hours"),
        "enable_workflows": value("enable_workflows"),
        "enabled_skills": value("enabled_skills"),
        "selected_user_skills": value("selected_user_skills"),
        "tags": value("tags"),
        "attention_tag": value("attention_tag"),
        "execution_metadata": portable_metadata,
        # ``session_id`` is runtime lifecycle state, not execution authority.
        # A legitimate carrier can start without one and acquire its native
        # session before the Manager's first readback.  Folding that mutable
        # value into the digest would permanently quarantine the very
        # crash/reconnect path this proof exists to recover.
    }


def legacy_plan_execution_snapshot_matches_proof(
    snapshot: object,
    proof: LegacyPlanExecutionCarrierProof,
) -> bool:
    """Bind a public Worker Task snapshot to its semantic proof response."""

    if (
        not isinstance(snapshot, dict)
        or not isinstance(proof, LegacyPlanExecutionCarrierProof)
        or type(snapshot.get("id")) is not int
        or snapshot["id"] != proof.task_id
        or snapshot.get("mode") != "plan"
        or snapshot.get("plan_approved") is not True
        or not isinstance(snapshot.get("plan_content"), str)
        or _sha256_text(snapshot["plan_content"])
        != proof.version_content_sha256
    ):
        return False
    try:
        execution_digest = _canonical_digest(
            _portable_execution_fingerprint(snapshot)
        )
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
        return False
    return execution_digest == proof.execution_fingerprint_sha256


def parse_legacy_plan_execution_carrier_proof(
    payload: object,
) -> LegacyPlanExecutionCarrierProof:
    """Strictly validate an untrusted Worker proof response."""

    expected_keys = {
        "protocol_version",
        "task_id",
        "version_number",
        "version_content_sha256",
        "execution_fingerprint_sha256",
        "proof_digest",
        "task_status",
        "retry_count",
        "turn_generation",
    }
    if not isinstance(payload, dict) or set(payload) != expected_keys:
        raise ValueError("legacy Plan carrier proof has an invalid shape")
    if (
        type(payload["protocol_version"]) is not int
        or payload["protocol_version"]
        != LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
    ):
        raise ValueError("legacy Plan carrier proof protocol is unsupported")
    if type(payload["task_id"]) is not int or payload["task_id"] <= 0:
        raise ValueError("legacy Plan carrier proof has an invalid task id")
    if (
        type(payload["version_number"]) is not int
        or payload["version_number"] <= 0
    ):
        raise ValueError("legacy Plan carrier proof has an invalid Version")
    for key in (
        "version_content_sha256",
        "execution_fingerprint_sha256",
        "proof_digest",
    ):
        value = payload[key]
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise ValueError(f"legacy Plan carrier proof has an invalid {key}")
    if (
        not isinstance(payload["task_status"], str)
        or not payload["task_status"]
        or payload["task_status"] != payload["task_status"].strip()
    ):
        raise ValueError("legacy Plan carrier proof has an invalid Task status")
    for key in ("retry_count", "turn_generation"):
        if type(payload[key]) is not int or payload[key] < 0:
            raise ValueError(f"legacy Plan carrier proof has an invalid {key}")

    authority = {
        "protocol_version": payload["protocol_version"],
        "task_id": payload["task_id"],
        "version_number": payload["version_number"],
        "version_content_sha256": payload["version_content_sha256"],
        "execution_fingerprint_sha256": payload[
            "execution_fingerprint_sha256"
        ],
        "human_decision": "approved",
        "application_type": "execution_task",
        "execution_task_id": payload["task_id"],
    }
    if _canonical_digest(authority) != payload["proof_digest"]:
        raise ValueError("legacy Plan carrier proof digest does not match")
    return LegacyPlanExecutionCarrierProof(
        task_id=payload["task_id"],
        version_number=payload["version_number"],
        version_content_sha256=payload["version_content_sha256"],
        execution_fingerprint_sha256=payload[
            "execution_fingerprint_sha256"
        ],
        proof_digest=payload["proof_digest"],
        task_status=payload["task_status"],
        retry_count=payload["retry_count"],
        turn_generation=payload["turn_generation"],
    )


async def legacy_approved_execution_carrier_proof(
    db: AsyncSession,
    task_id: int,
    *,
    for_update: bool = False,
) -> LegacyPlanExecutionCarrierProof | None:
    """Return the immutable semantic proof and current reconciliation state."""

    statement = (
        select(Task, PlanVersion)
            .join(
                PlanLegacyTaskLink,
                PlanLegacyTaskLink.legacy_task_id == Task.id,
            )
            .join(
                Plan,
                Plan.id == PlanLegacyTaskLink.plan_id,
            )
            .join(
                PlanVersion,
                PlanVersion.id == PlanLegacyTaskLink.plan_version_id,
            )
            .join(
                PlanApplication,
                PlanApplication.plan_version_id == PlanVersion.id,
            )
            .where(
                Task.id == task_id,
                Task.mode == "plan",
                Task.plan_approved.is_(True),
                PlanVersion.plan_id == Plan.id,
                PlanVersion.human_decision == "approved",
                PlanApplication.plan_id == Plan.id,
                PlanApplication.application_type == "execution_task",
                PlanApplication.execution_task_id == Task.id,
                PlanApplication.user_log_id.is_(None),
            )
            .limit(1)
    )
    if for_update:
        statement = statement.with_for_update()
    row = (await db.execute(statement)).first()
    if row is None:
        return None
    task, version = row
    if (
        type(task.id) is not int
        or task.id <= 0
        or type(version.version_number) is not int
        or version.version_number <= 0
        or not isinstance(version.content, str)
        or task.plan_content != version.content
        or not isinstance(task.status, str)
        or not task.status
        or type(task.retry_count) is not int
        or task.retry_count < 0
        or type(task.turn_generation) is not int
        or task.turn_generation < 0
    ):
        return None
    try:
        version_digest = _sha256_text(version.content)
        execution_digest = _canonical_digest(
            _portable_execution_fingerprint(task)
        )
        authority = {
            "protocol_version": (
                LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
            ),
            "task_id": task.id,
            "version_number": version.version_number,
            "version_content_sha256": version_digest,
            "execution_fingerprint_sha256": execution_digest,
            "human_decision": "approved",
            "application_type": "execution_task",
            "execution_task_id": task.id,
        }
        proof_digest = _canonical_digest(authority)
    except (TypeError, ValueError, OverflowError):
        return None
    return LegacyPlanExecutionCarrierProof(
        task_id=task.id,
        version_number=version.version_number,
        version_content_sha256=version_digest,
        execution_fingerprint_sha256=execution_digest,
        proof_digest=proof_digest,
        task_status=task.status,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
    )


async def is_legacy_approved_execution_carrier(
    db: AsyncSession,
    task_id: int,
) -> bool:
    """Prove that an approved Plan Task is the migrated execution Task.

    Before Plan v2, approval reused the planning Task for implementation.  The
    migration preserves that one compatibility case with three linked facts:
    the canonical legacy link, its approved Version, and an execution
    Application pointing back to the same Task.  ``plan_approved`` alone is not
    execution authority because stale or corrupt rows can carry the same bit.
    """

    return (
        await legacy_approved_execution_carrier_proof(db, task_id)
    ) is not None
