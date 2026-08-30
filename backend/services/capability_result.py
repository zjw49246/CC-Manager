"""Exact, provider-neutral materialization of completed Capability results.

This service deliberately owns neither HTTP ACLs nor transaction boundaries.
Callers must authorize access before resolving a result and must not rely on
this read-only helper to commit or lock orchestration state.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.services.capability_service import CapabilityConflictError


@dataclass(frozen=True, slots=True)
class ResolvedCapabilityResult:
    """One fully verified, detached Capability result.

    ``_data_json`` is the immutable canonical snapshot of the model-facing
    data.  The public accessors decode a new object on every call, so callers
    cannot mutate either the snapshot or SQLAlchemy's JSON identity-map state.
    """

    invocation_id: int
    invocation_status: str
    execution_id: int
    kind: str
    id: int
    hash: str
    resource_url: str
    _data_json: str

    @classmethod
    def from_data(
        cls,
        *,
        invocation: CapabilityInvocation,
        execution: CapabilityExecution,
        resource_url: str,
        data: dict[str, Any],
    ) -> ResolvedCapabilityResult:
        if not isinstance(data, dict):
            raise CapabilityConflictError(
                "Capability result must be a JSON object"
            )
        try:
            frozen = json.dumps(
                data,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise CapabilityConflictError(
                "Capability result is not finite JSON data"
            ) from exc
        # The tuple is proven by ``exact_completed_execution`` before this
        # constructor is called.
        assert invocation.result_kind is not None
        assert invocation.result_id is not None
        assert invocation.result_hash is not None
        return cls(
            invocation_id=invocation.id,
            invocation_status=invocation.status,
            execution_id=execution.id,
            kind=invocation.result_kind,
            id=invocation.result_id,
            hash=invocation.result_hash,
            resource_url=resource_url,
            _data_json=frozen,
        )

    @property
    def data(self) -> dict[str, Any]:
        """Return a detached JSON object suitable for a model or API."""

        decoded = json.loads(self._data_json)
        # All current result serializers return an object, and ``from_data``
        # only accepts a dict.  Keep the runtime assertion beside the cast so
        # a future serializer cannot silently change the public contract.
        assert isinstance(decoded, dict)
        return cast(dict[str, Any], decoded)

    def as_payload(self) -> dict[str, Any]:
        """Return the existing public/model-facing result envelope."""

        return {
            "invocation_id": self.invocation_id,
            "invocation_status": self.invocation_status,
            "kind": self.kind,
            "id": self.id,
            "hash": self.hash,
            "resource_url": self.resource_url,
            "data": self.data,
        }


async def exact_completed_execution(
    db: AsyncSession,
    invocation: CapabilityInvocation,
) -> CapabilityExecution:
    """Prove that an Invocation's result tuple names exactly one completion."""

    if (
        invocation.result_kind is None
        or invocation.result_id is None
        or invocation.result_hash is None
    ):
        raise CapabilityConflictError("Capability result is not ready")
    rows = list(
        (
            await db.execute(
                select(CapabilityExecution)
                .where(
                    CapabilityExecution.invocation_id == invocation.id,
                    CapabilityExecution.status == "completed",
                    CapabilityExecution.output_kind == invocation.result_kind,
                    CapabilityExecution.output_id == invocation.result_id,
                    CapabilityExecution.output_hash == invocation.result_hash,
                )
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    if len(rows) != 1:
        raise CapabilityConflictError(
            "Capability result lost its exact completed execution"
        )
    return rows[0]


async def resolve_capability_result(
    db: AsyncSession,
    invocation: CapabilityInvocation,
) -> ResolvedCapabilityResult:
    """Resolve and reverse-verify an Invocation's JSON-safe output graph."""

    execution = await exact_completed_execution(db, invocation)
    result_kind = invocation.result_kind
    result_id = invocation.result_id
    result_hash = invocation.result_hash
    assert result_kind is not None
    assert result_id is not None
    assert result_hash is not None

    if result_kind == "code_review_result":
        from backend.models.code_review import CodeReviewResult, CodeReviewRun
        from backend.schemas.capability import CodeReviewResultResource

        result = await db.get(CodeReviewResult, result_id, populate_existing=True)
        run = (
            await db.get(CodeReviewRun, result.run_id, populate_existing=True)
            if result is not None
            else None
        )
        if (
            invocation.capability_key != "code_review"
            or result is None
            or run is None
            or result.capability_invocation_id != invocation.id
            or result.capability_execution_id != execution.id
            or result.developer_task_id != invocation.task_id
            or result.result_hash != result_hash
            or run.id != result.run_id
            or run.capability_invocation_id != invocation.id
            or run.capability_execution_id != execution.id
            or run.developer_task_id != invocation.task_id
            or run.reviewer_task_id != result.reviewer_task_id
            or run.subject_hash != result.subject_hash
        ):
            raise CapabilityConflictError(
                "Code Review result identity does not match its Invocation"
            )
        data = CodeReviewResultResource.model_validate(result).model_dump(
            mode="json"
        )
        resource_url = f"/api/capability-invocations/{invocation.id}/result"
    elif result_kind == "plan_version":
        from backend.models.plan import Plan, PlanVersion
        from backend.models.plan_agent import PlanAgentRun
        from backend.services.plan_capability import (
            PLAN_RUN_HANDLE_KIND,
            plan_version_output_hash,
        )
        from backend.services.plan_service import version_resource

        run_id = None
        if (
            execution.handle_kind == PLAN_RUN_HANDLE_KIND
            and execution.handle_id is not None
        ):
            try:
                parsed_run_id = int(execution.handle_id)
            except (TypeError, ValueError):
                pass
            else:
                if parsed_run_id > 0 and str(parsed_run_id) == execution.handle_id:
                    run_id = parsed_run_id
        run = (
            await db.get(PlanAgentRun, run_id, populate_existing=True)
            if run_id is not None
            else None
        )
        version = await db.get(PlanVersion, result_id, populate_existing=True)
        plan = (
            await db.get(Plan, run.plan_id, populate_existing=True)
            if run is not None and run.plan_id is not None
            else None
        )
        if (
            invocation.capability_key != "plan"
            or run is None
            or version is None
            or plan is None
            or execution.handle_id != str(run.id)
            or run.capability_execution_id != execution.id
            or run.run_type != "capability"
            or run.plan_id != plan.id
            or run.result_version_id != version.id
            or version.plan_id != plan.id
            or version.produced_by_run_id != run.id
            or plan.target_task_id != invocation.task_id
        ):
            raise CapabilityConflictError(
                "Plan result identity does not match its Invocation"
            )
        if plan_version_output_hash(version) != result_hash:
            raise CapabilityConflictError(
                "Plan result hash does not match its authoritative PlanVersion"
            )
        data = (await version_resource(db, version)).model_dump(mode="json")
        resource_url = f"/api/plan-versions/{version.id}"
    else:
        raise CapabilityConflictError(
            f"Capability result kind {result_kind!r} has no public resource"
        )

    return ResolvedCapabilityResult.from_data(
        invocation=invocation,
        execution=execution,
        resource_url=resource_url,
        data=data,
    )
