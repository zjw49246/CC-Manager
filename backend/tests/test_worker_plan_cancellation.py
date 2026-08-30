"""Strict Manager/Worker cancellation receipt regressions for Plan Runs."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import backend.main as main_module
import backend.api.plan_resources as plan_resources_module
import backend.services.worker_proxy as worker_proxy_module
from backend.config import settings
from backend.models.plan import Plan, PlanInputRequest, PlanVersion
from backend.api.plan_resources import _worker_run_import_digest
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentStep,
    PlanAgentWorkerDispatchReceipt,
    PlanAgentWorkerImportReceipt,
)
from backend.models.worker import Worker
from backend.schemas.plan import default_plan_pipeline_config
from backend.schemas.plan_resource import WorkerPlanRunImportRequest
from backend.services.dispatcher import GlobalDispatcher
from backend.services.worker_proxy import WorkerProxy
from backend.services.plan_service import (
    apply_worker_plan_outcome,
    cancel_worker_mirror_run_after_ack,
    plan_operation_lock,
)
from backend.services.worker_plan_dispatch import (
    WorkerPlanDispatchConflict,
    fence_worker_mirror_cancellation,
    finalize_worker_mirror_cancellation,
    mark_worker_dispatch_remote_possible,
    worker_mirror_run_is_clean,
)
from backend.services.worker_node_control import begin_worker_node_drain
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


class _Response:
    def __init__(self, payload=None, *, invalid_json: bool = False):
        self._payload = payload
        self._invalid_json = invalid_json

    def raise_for_status(self) -> None:
        return None

    def json(self):
        if self._invalid_json:
            raise ValueError("not JSON")
        return self._payload


def _http_client_for_cancel_receipt(monkeypatch, *, payload=None, invalid_json=False):
    calls: list[tuple[str, str, dict | None]] = []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, *, headers):
            calls.append(("GET", url, None))
            return _Response(
                {
                    "versioned_plan_worker_protocol": 3,
                    "worker_plan_exact_cancel_protocol": 1,
                }
            )

        async def post(self, url, *, headers, json):
            calls.append(("POST", url, json))
            return _Response(payload, invalid_json=invalid_json)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    return calls


def _proxy_with_ready_worker() -> WorkerProxy:
    worker = Worker(
        id=41,
        name="strict-plan-worker",
        status="ready",
        private_ip="10.0.0.41",
        ccm_port=8000,
        auth_token="worker-token",
    )
    proxy = WorkerProxy(db_factory=None, relay=None)
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    return proxy


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {
            "protocol": 1,
            "state": "cancelled",
            "plan_id": 776,
            "run_id": 778,
            "payload_digest": "a" * 64,
            "run": {"id": 778, "plan_id": 776, "status": "cancelled"},
        },
        {
            "protocol": 1,
            "state": "cancelled",
            "plan_id": 776,
            "run_id": 777,
            "payload_digest": "a" * 64,
            "run": {"id": 777, "plan_id": 776, "status": "running"},
        },
        {
            "protocol": 1,
            "state": "cancelled",
            "plan_id": 776,
            "run_id": 777,
            "payload_digest": "a" * 64,
            "run": {"id": 777, "plan_id": 776, "status": "cancelled"},
        },
    ],
    ids=["wrong-run-id", "non-cancelled-status", "legacy-status-only-ack"],
)
async def test_worker_proxy_rejects_2xx_without_exact_cancelled_receipt(
    monkeypatch,
    payload,
):
    calls = _http_client_for_cancel_receipt(monkeypatch, payload=payload)

    with pytest.raises(
        RuntimeError,
        match="non-terminal exact Plan cancellation receipt",
    ):
        await _proxy_with_ready_worker().cancel_versioned_plan_run(
            41,
            777,
            plan_id=776,
            payload_digest="a" * 64,
        )

    assert calls == [
        ("GET", "http://10.0.0.41:8000/api/system/config", None),
        (
            "POST",
            "http://10.0.0.41:8000/api/plan-runs/777/worker-import-cancel",
            {
                "protocol": 1,
                "plan_id": 776,
                "payload_digest": "a" * 64,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_worker_proxy_rejects_2xx_invalid_json_receipt(monkeypatch):
    _http_client_for_cancel_receipt(monkeypatch, invalid_json=True)

    with pytest.raises(
        RuntimeError,
        match="invalid Plan cancellation receipt",
    ):
        await _proxy_with_ready_worker().cancel_versioned_plan_run(
            41,
            777,
            plan_id=776,
            payload_digest="a" * 64,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "hidden_field",
    ["base", "versions"],
)
async def test_worker_proxy_rejects_absent_cancel_with_hidden_graph(
    monkeypatch,
    hidden_field,
):
    payload = {
        "protocol": 1,
        "state": "absent",
        "plan_id": 776,
        "run_id": 777,
        "payload_digest": "a" * 64,
        "base_worker_version_id": None,
        "run": None,
        "versions": [],
    }
    if hidden_field == "base":
        payload["base_worker_version_id"] = 91
    else:
        payload["versions"] = [{"id": 92}]
    _http_client_for_cancel_receipt(monkeypatch, payload=payload)

    with pytest.raises(
        RuntimeError,
        match="non-terminal exact Plan cancellation receipt",
    ):
        await _proxy_with_ready_worker().cancel_versioned_plan_run(
            41,
            777,
            plan_id=776,
            payload_digest="a" * 64,
        )


@pytest.mark.asyncio
async def test_worker_proxy_accepts_only_complete_cancelled_terminal_graph(monkeypatch):
    receipt = _terminal_cancel_receipt(
        SimpleNamespace(
            plan_id=776,
            run_id=777,
            payload_digest="a" * 64,
            started_at=datetime.utcnow(),
        ),
        status="cancelled",
    )
    _http_client_for_cancel_receipt(monkeypatch, payload=receipt)

    result = await _proxy_with_ready_worker().cancel_versioned_plan_run(
        41,
        777,
        plan_id=776,
        payload_digest="a" * 64,
    )

    assert result is receipt


@pytest.mark.asyncio
async def test_worker_proxy_rejects_cancelled_graph_that_publishes_version(monkeypatch):
    graph = SimpleNamespace(
        plan_id=776,
        run_id=777,
        payload_digest="a" * 64,
        started_at=datetime.utcnow(),
    )
    receipt = _terminal_cancel_receipt(graph, status="completed")
    receipt["run"]["status"] = "cancelled"
    receipt["run"]["error"] = "Cancelled by user"
    _http_client_for_cancel_receipt(monkeypatch, payload=receipt)

    with pytest.raises(
        RuntimeError,
        match="invalid terminal Plan outcome graph",
    ):
        await _proxy_with_ready_worker().cancel_versioned_plan_run(
            41,
            graph.run_id,
            plan_id=graph.plan_id,
            payload_digest=graph.payload_digest,
        )


@pytest.mark.asyncio
async def test_worker_proxy_rejects_boolean_run_identity(monkeypatch):
    """A JSON boolean must not authenticate integer Run identity 1."""

    _http_client_for_cancel_receipt(
        monkeypatch,
        payload={
            "protocol": 1,
            "state": "cancelled",
            "plan_id": 776,
            "run_id": True,
            "payload_digest": "a" * 64,
            "run": {"id": True, "plan_id": 776, "status": "cancelled"},
        },
    )

    with pytest.raises(
        RuntimeError,
        match="non-terminal exact Plan cancellation receipt",
    ):
        await _proxy_with_ready_worker().cancel_versioned_plan_run(
            41,
            1,
            plan_id=776,
            payload_digest="a" * 64,
        )


async def _seed_worker_plan_run(
    session_factory,
    *,
    worker_id: int = 41,
    receipt_status: str | None = "remote_possible",
    run_status: str = "running",
    generation: int = 4,
) -> SimpleNamespace:
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    started_at = datetime.utcnow()
    async with session_factory() as db:
        plan = Plan(
            title="Manager Worker Plan mirror",
            initial_request="Plan remotely",
            worker_id=worker_id,
            pipeline_config=pipeline,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker_id,
            run_type="initial",
            request_text="Plan remotely",
            pipeline_config=pipeline,
            status=run_status,
            current_stage="planner",
            generation=generation,
            last_execution_started_at=(
                started_at if run_status == "running" else None
            ),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        if receipt_status is not None:
            db.add(
                PlanAgentWorkerDispatchReceipt(
                    plan_id=plan.id,
                    run_id=run.id,
                    worker_id=worker_id,
                    run_generation=run.generation,
                    protocol=1,
                    status=receipt_status,
                    payload_digest=(
                        "a" * 64
                        if receipt_status == "remote_possible"
                        else None
                    ),
                )
            )
        await db.commit()
        return SimpleNamespace(
            plan_id=plan.id,
            run_id=run.id,
            worker_id=worker_id,
            generation=run.generation,
            started_at=started_at,
            payload_digest="a" * 64,
        )


async def _assert_worker_mirror_cancellation_fenced(session_factory, graph) -> None:
    async with session_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        assert plan is not None and plan.active_run_id == graph.run_id
        assert run is not None and run.status == "cancelling"
        assert run.generation == graph.generation + 1
        assert run.cancellation_target_generation == graph.generation
        assert run.worker_id == graph.worker_id
        assert run.last_execution_started_at == graph.started_at
        assert run.finished_at is None
        assert run.error == "Cancellation requested"


def _terminal_cancel_receipt(
    graph,
    *,
    status: str,
) -> dict:
    timestamp = graph.started_at.isoformat()
    version_id = 900_000 + graph.run_id
    planner_step_id = 800_000 + graph.run_id
    reviewer_step_id = 700_000 + graph.run_id
    versions = []
    steps = []
    result_version_id = None
    if status == "cancelled":
        steps = [
            {
                "id": planner_step_id,
                "run_id": graph.run_id,
                "plan_id": graph.plan_id,
                "plan_version_id": None,
                "input_request_id": None,
                "step_type": "planner",
                "round": 1,
                "generation": 0,
                "provider": "codex",
                "model": "gpt-test",
                "effort": "high",
                "route_slot": "primary",
                "status": "cancelled",
                "output": "partial remote plan evidence",
                "error": "Cancelled by user",
                "last_delta_at": timestamp,
                "streamed_output_chars": 28,
                "last_event_type": "turn.cancelled",
                "started_at": timestamp,
                "finished_at": timestamp,
            }
        ]
    elif status == "completed":
        result_version_id = version_id
        steps = [
            {
                "id": planner_step_id,
                "run_id": graph.run_id,
                "plan_id": graph.plan_id,
                "plan_version_id": version_id,
                "input_request_id": None,
                "step_type": "planner",
                "round": 1,
                "generation": 1,
                "provider": "codex",
                "model": "gpt-test",
                "effort": "high",
                "route_slot": "primary",
                "status": "completed",
                "output": "remote terminal plan",
                "error": None,
                "last_delta_at": timestamp,
                "streamed_output_chars": 20,
                "last_event_type": "turn.completed",
                "started_at": timestamp,
                "finished_at": timestamp,
            },
            {
                "id": reviewer_step_id,
                "run_id": graph.run_id,
                "plan_id": graph.plan_id,
                "plan_version_id": None,
                "input_request_id": None,
                "step_type": "reviewer",
                "round": 1,
                "generation": 2,
                "provider": "claude",
                "model": "claude-test",
                "effort": "medium",
                "route_slot": "secondary",
                "status": "completed",
                "output": "approve",
                "error": None,
                "last_delta_at": timestamp,
                "streamed_output_chars": 7,
                "last_event_type": "result",
                "started_at": timestamp,
                "finished_at": timestamp,
            },
        ]
        versions = [
            {
                "id": version_id,
                "plan_id": graph.plan_id,
                "version_number": 1,
                "parent_version_id": None,
                "produced_by_run_id": graph.run_id,
                "produced_by_step_id": planner_step_id,
                "content": "remote terminal plan",
                "context_session_id": None,
                "context_log_id": None,
                "repo_revision": None,
                "reviewer_repo_revision": None,
                "review_verdict": "approve",
                "review_feedback": None,
                "reviewed_by_step_id": reviewer_step_id,
                "review_exhausted": False,
                "reviewed_at": timestamp,
                "human_decision": "pending",
                "decided_at": None,
                "decided_by": None,
                "superseded_by_version_id": None,
                "applied": False,
                "display_state": "approved",
                "created_at": timestamp,
            }
        ]
    run = {
        "id": graph.run_id,
        "plan_id": graph.plan_id,
        "run_type": "initial",
        "source_run_id": None,
        "status": status,
        # Interaction-limit failures retain the stage that requested input;
        # terminal validity must not depend on a cosmetic stage rewrite.
        "current_stage": "complete" if status == "completed" else "planner",
        "base_version_id": None,
        "result_version_id": result_version_id,
        "draft_content": "remote terminal plan" if status == "completed" else None,
        "draft_step_id": planner_step_id if status == "completed" else None,
        "draft_repo_revision": None,
        "request_text": "Plan remotely",
        "round": 1,
        "generation": 2 if status == "completed" else 0,
        "instance_id": None,
        "worker_id": None,
        "open_input_request_id": None,
        "interaction_count": 0,
        "max_interactions": 3,
        "execution_seconds": 1.0,
        "last_execution_started_at": None,
        "review_verdict": "approve" if status == "completed" else None,
        "review_feedback": None,
        "review_exhausted": False,
        "error": (
            None
            if status == "completed"
            else "Cancelled by user"
            if status == "cancelled"
            else "remote failure"
        ),
        "created_at": timestamp,
        "updated_at": timestamp,
        "finished_at": timestamp,
        "steps": steps,
        "input_requests": [],
    }
    return {
        "protocol": 1,
        "state": "terminal",
        "plan_id": graph.plan_id,
        "run_id": graph.run_id,
        "payload_digest": graph.payload_digest,
        "base_worker_version_id": None,
        "run": run,
        "versions": versions,
    }


def _cancelled_receipt_with_input(graph) -> dict:
    """Build the terminal form of a previously imported waiting_user graph."""

    receipt = _terminal_cancel_receipt(graph, status="cancelled")
    step = receipt["run"]["steps"][0]
    input_id = 600_000 + graph.run_id
    step.update(
        {
            "input_request_id": input_id,
            "status": "completed",
            "error": None,
            "last_event_type": "turn.completed",
        }
    )
    receipt["run"]["interaction_count"] = 1
    receipt["run"]["input_requests"] = [
        {
            "id": input_id,
            "plan_id": graph.plan_id,
            "run_id": graph.run_id,
            "source_step_id": step["id"],
            "requested_by": "planner",
            "reason": "Need a durable choice",
            "questions": [
                {
                    "id": "scope",
                    "header": "Scope",
                    "question": "Which scope should be used?",
                    "response_type": "text",
                    "options": [],
                    "required": True,
                }
            ],
            "status": "cancelled",
            "answers": None,
            "response_text": None,
            "attachments": None,
            "answered_by": None,
            "opened_at": graph.started_at.isoformat(),
            "answered_at": None,
            "created_at": graph.started_at.isoformat(),
        }
    ]
    return receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    [
        "boolean-generation",
        "foreign-version-run",
        "duplicate-version",
        "base-version-mismatch",
        "zero-step-id",
        "negative-counter",
        "interaction-input-count-mismatch",
        "nan-execution-seconds",
        "missing-finished-at",
        "step-version-reciprocal-mismatch",
        "input-step-reciprocal-mismatch",
        "non-terminal-input-status",
        "producer-not-planner",
        "producer-not-completed",
        "reviewer-not-completed",
        "non-pending-human-decision",
        "review-exhaustion-mismatch",
        "failed-published-version",
        "additional-consistent-version",
        "terminal-instance-owner",
        "terminal-worker-owner",
        "unfinished-terminal-step",
        "draft-producer-mismatch",
        "draft-content-mismatch",
        "missing-reviewed-at",
        "pending-decision-metadata",
        "approve-without-reviewer",
        "disabled-with-reviewer",
        "self-parent-result",
        "result-overlaps-base",
        "invalid-first-version-number",
    ],
)
async def test_worker_proxy_rejects_malformed_terminal_outcome_graph(
    monkeypatch,
    corruption,
):
    graph = SimpleNamespace(
        plan_id=776,
        run_id=777,
        started_at=datetime.utcnow(),
        payload_digest="a" * 64,
    )
    receipt = _terminal_cancel_receipt(graph, status="completed")
    if corruption == "boolean-generation":
        receipt["run"]["generation"] = True
    elif corruption == "foreign-version-run":
        receipt["versions"][0]["produced_by_run_id"] = True
    elif corruption == "duplicate-version":
        receipt["versions"].append(dict(receipt["versions"][0]))
    elif corruption == "base-version-mismatch":
        receipt["base_worker_version_id"] = 991
    elif corruption == "zero-step-id":
        receipt["run"]["steps"][0]["id"] = 0
        receipt["versions"][0]["produced_by_step_id"] = 0
    elif corruption == "negative-counter":
        receipt["run"]["interaction_count"] = -1
    elif corruption == "interaction-input-count-mismatch":
        receipt["run"]["interaction_count"] = 1
    elif corruption == "nan-execution-seconds":
        receipt["run"]["execution_seconds"] = float("nan")
    elif corruption == "missing-finished-at":
        receipt["run"]["finished_at"] = None
    elif corruption == "step-version-reciprocal-mismatch":
        receipt["run"]["steps"][0]["plan_version_id"] = None
    elif corruption in {
        "input-step-reciprocal-mismatch",
        "non-terminal-input-status",
    }:
        receipt["run"]["interaction_count"] = 1
        input_id = 700_000 + graph.run_id
        reviewer_step_id = 600_000 + graph.run_id
        receipt["run"]["steps"].append(
            {
                "id": reviewer_step_id,
                "run_id": graph.run_id,
                "plan_id": graph.plan_id,
                "plan_version_id": None,
                "input_request_id": input_id,
                "step_type": "reviewer",
                "round": 1,
                "generation": 0,
                "provider": "claude",
                "model": "claude-test",
                "effort": "medium",
                "route_slot": "secondary",
                "status": "completed",
                "output": "Need input",
                "error": None,
                "last_delta_at": None,
                "streamed_output_chars": 10,
                "last_event_type": "result",
                "started_at": graph.started_at.isoformat(),
                "finished_at": graph.started_at.isoformat(),
            }
        )
        receipt["run"]["input_requests"].append(
            {
                "id": input_id,
                "plan_id": graph.plan_id,
                "run_id": graph.run_id,
                "source_step_id": (
                    receipt["run"]["steps"][0]["id"]
                    if corruption == "input-step-reciprocal-mismatch"
                    else reviewer_step_id
                ),
                "requested_by": "reviewer",
                "reason": "Need input",
                "questions": [],
                "status": (
                    "open"
                    if corruption == "non-terminal-input-status"
                    else "answered"
                ),
                "answers": [],
                "response_text": None,
                "attachments": None,
                "answered_by": None,
                "opened_at": graph.started_at.isoformat(),
                "answered_at": graph.started_at.isoformat(),
                "created_at": graph.started_at.isoformat(),
            }
        )
    elif corruption == "producer-not-planner":
        receipt["run"]["steps"][0]["step_type"] = "reviewer"
    elif corruption == "producer-not-completed":
        receipt["run"]["steps"][0]["status"] = "failed"
    elif corruption == "reviewer-not-completed":
        reviewer = dict(receipt["run"]["steps"][0])
        reviewer["id"] += 100_000
        reviewer["step_type"] = "reviewer"
        reviewer["plan_version_id"] = None
        reviewer["status"] = "failed"
        receipt["run"]["steps"].append(reviewer)
        receipt["versions"][0]["reviewed_by_step_id"] = reviewer["id"]
    elif corruption == "non-pending-human-decision":
        receipt["versions"][0]["human_decision"] = "approved"
    elif corruption == "review-exhaustion-mismatch":
        receipt["versions"][0]["review_exhausted"] = True
    elif corruption == "failed-published-version":
        receipt["run"]["status"] = "failed"
        receipt["run"]["current_stage"] = "failed"
        receipt["run"]["error"] = "remote failure"
    elif corruption == "additional-consistent-version":
        extra_version = dict(receipt["versions"][0])
        extra_version["id"] += 1
        extra_version["version_number"] += 1
        extra_planner = dict(receipt["run"]["steps"][0])
        extra_planner["id"] += 200_000
        extra_planner["plan_version_id"] = extra_version["id"]
        extra_version["produced_by_step_id"] = extra_planner["id"]
        extra_version["reviewed_by_step_id"] = None
        receipt["run"]["steps"].append(extra_planner)
        receipt["versions"].append(extra_version)
    elif corruption == "terminal-instance-owner":
        receipt["run"]["instance_id"] = 42
    elif corruption == "terminal-worker-owner":
        receipt["run"]["worker_id"] = 42
    elif corruption == "unfinished-terminal-step":
        receipt["run"]["steps"][0]["finished_at"] = None
    elif corruption == "draft-producer-mismatch":
        receipt["run"]["draft_step_id"] = receipt["run"]["steps"][1]["id"]
    elif corruption == "draft-content-mismatch":
        receipt["run"]["draft_content"] = "different draft"
    elif corruption == "missing-reviewed-at":
        receipt["versions"][0]["reviewed_at"] = None
    elif corruption == "pending-decision-metadata":
        receipt["versions"][0]["decided_at"] = graph.started_at.isoformat()
        receipt["versions"][0]["decided_by"] = 1
    elif corruption == "approve-without-reviewer":
        receipt["versions"][0]["reviewed_by_step_id"] = None
    elif corruption == "disabled-with-reviewer":
        receipt["versions"][0]["review_verdict"] = "disabled"
        receipt["run"]["review_verdict"] = "disabled"
    elif corruption == "self-parent-result":
        receipt["versions"][0]["parent_version_id"] = receipt["versions"][0]["id"]
    elif corruption == "result-overlaps-base":
        version_id = receipt["versions"][0]["id"]
        receipt["base_worker_version_id"] = version_id
        receipt["run"]["base_version_id"] = version_id
        receipt["versions"][0]["parent_version_id"] = version_id
    else:
        receipt["versions"][0]["version_number"] = 99

    _http_client_for_cancel_receipt(monkeypatch, payload=receipt)

    with pytest.raises(
        RuntimeError,
        match="invalid terminal Plan outcome graph",
    ):
        await _proxy_with_ready_worker().cancel_versioned_plan_run(
            41,
            graph.run_id,
            plan_id=graph.plan_id,
            payload_digest=graph.payload_digest,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("run_type", "remote_version_number", "accepted"),
    [
        ("fork", 1, True),
        ("user_revision", 8, True),
        ("user_revision", 7, False),
        ("user_revision", 99, False),
    ],
)
async def test_worker_terminal_version_number_extends_effective_lineage(
    session_factory,
    run_type,
    remote_version_number,
    accepted,
):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    started_at = datetime.utcnow()
    async with session_factory() as db:
        source_plan = Plan(
            title="Source Plan",
            initial_request="Source",
            pipeline_config=pipeline,
            worker_id=None if run_type == "fork" else 41,
        )
        db.add(source_plan)
        await db.flush()
        base = PlanVersion(
            plan_id=source_plan.id,
            version_number=7,
            content="source version",
            review_verdict="approve",
            review_exhausted=False,
            human_decision="approved",
            reviewed_at=started_at,
        )
        db.add(base)
        await db.flush()
        source_plan.current_version_id = base.id
        if run_type == "fork":
            plan = Plan(
                title="Forked Plan",
                initial_request="Fork",
                pipeline_config=pipeline,
                worker_id=41,
                forked_from_version_id=base.id,
            )
            db.add(plan)
            await db.flush()
        else:
            plan = source_plan
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=41,
            run_type=run_type,
            base_version_id=base.id,
            request_text="Plan remotely",
            pipeline_config=pipeline,
            status="running",
            current_stage="planner",
            generation=0,
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()

        graph = SimpleNamespace(
            plan_id=plan.id,
            run_id=run.id,
            started_at=started_at,
            payload_digest="a" * 64,
        )
        receipt = _terminal_cancel_receipt(graph, status="completed")
        payload = {
            "protocol": 3,
            "base_worker_version_id": None,
            "run": receipt["run"],
            "versions": receipt["versions"],
        }
        payload["run"]["run_type"] = run_type
        payload["versions"][0]["version_number"] = remote_version_number
        if run_type != "fork":
            worker_base_id = 500_000 + base.id
            payload["base_worker_version_id"] = worker_base_id
            payload["run"]["base_version_id"] = worker_base_id
            payload["versions"][0]["parent_version_id"] = worker_base_id

        if not accepted:
            with pytest.raises(
                RuntimeError,
                match="does not extend its exact base",
            ):
                await apply_worker_plan_outcome(
                    db,
                    plan=plan,
                    run=run,
                    worker_id=41,
                    expected_generation=0,
                    payload=payload,
                )
            await db.rollback()
            return

        applied = await apply_worker_plan_outcome(
            db,
            plan=plan,
            run=run,
            worker_id=41,
            expected_generation=0,
            payload=payload,
        )
        version = await db.get(PlanVersion, applied.result_version_id)
        assert applied.status == "completed"
        assert version is not None
        assert version.version_number == remote_version_number
        assert version.parent_version_id == (
            None if run_type == "fork" else base.id
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "remote_outcome",
    [
        RuntimeError("Worker connection dropped"),
        {
            "protocol": 1,
            "state": "cancelled",
            "plan_id": -1,
            "run_id": -1,
            "payload_digest": "a" * 64,
        },
        {"protocol": 1, "state": "running"},
    ],
    ids=["remote-error", "wrong-run-id", "non-cancelled-status"],
)
async def test_manager_mirror_keeps_durable_cancelling_without_strict_remote_ack(
    client,
    session_factory,
    monkeypatch,
    remote_outcome,
):
    graph = await _seed_worker_plan_run(session_factory)
    cancel_remote = AsyncMock()
    if isinstance(remote_outcome, Exception):
        cancel_remote.side_effect = remote_outcome
    else:
        cancel_remote.return_value = remote_outcome
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 503, response.text
    assert "could not be cancelled safely" in response.text
    cancel_remote.assert_awaited_once_with(
        graph.worker_id,
        graph.run_id,
        plan_id=graph.plan_id,
        payload_digest=graph.payload_digest,
    )
    stop_lifecycle.assert_awaited_once_with(graph.run_id, None)
    await _assert_worker_mirror_cancellation_fenced(session_factory, graph)


@pytest.mark.asyncio
async def test_manager_rejects_legacy_status_only_cancelled_ack(
    client,
    session_factory,
    monkeypatch,
):
    graph = await _seed_worker_plan_run(session_factory)
    cancel_remote = AsyncMock(
        return_value={
            "protocol": 1,
            "state": "cancelled",
            "plan_id": graph.plan_id,
            "run_id": graph.run_id,
            "payload_digest": graph.payload_digest,
        }
    )
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 503, response.text
    assert "could not be cancelled safely" in response.text
    stop_lifecycle.assert_awaited_once_with(graph.run_id, None)
    await _assert_worker_mirror_cancellation_fenced(session_factory, graph)


@pytest.mark.asyncio
@pytest.mark.parametrize("hidden_field", ["run", "base", "versions"])
async def test_manager_rejects_absent_ack_with_hidden_graph(
    client,
    session_factory,
    monkeypatch,
    hidden_field,
):
    graph = await _seed_worker_plan_run(session_factory)
    remote = {
        "protocol": 1,
        "state": "absent",
        "plan_id": graph.plan_id,
        "run_id": graph.run_id,
        "payload_digest": graph.payload_digest,
        "base_worker_version_id": None,
        "run": None,
        "versions": [],
    }
    if hidden_field == "run":
        remote["run"] = {
            "id": graph.run_id,
            "plan_id": graph.plan_id,
            "status": "cancelled",
        }
    elif hidden_field == "base":
        remote["base_worker_version_id"] = 91
    else:
        remote["versions"] = [{"id": 92}]
    cancel_remote = AsyncMock(return_value=remote)
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 503, response.text
    assert "could not be cancelled safely" in response.text
    stop_lifecycle.assert_awaited_once_with(graph.run_id, None)
    await _assert_worker_mirror_cancellation_fenced(session_factory, graph)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_on_plan_run_get",
    (2, 3),
    ids=("refresh-disappears", "locked-refresh-disappears"),
)
async def test_manager_cancel_fails_closed_when_run_disappears_during_refresh(
    client,
    session_factory,
    monkeypatch,
    missing_on_plan_run_get,
):
    graph = await _seed_worker_plan_run(session_factory)
    original_get = AsyncSession.get
    plan_run_gets = 0

    async def disappear_on_refresh(self, entity, ident, *args, **kwargs):
        nonlocal plan_run_gets
        result = await original_get(self, entity, ident, *args, **kwargs)
        if entity is PlanAgentRun and ident == graph.run_id:
            plan_run_gets += 1
            if plan_run_gets == missing_on_plan_run_get:
                return None
        return result

    cancel_remote = AsyncMock()
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(AsyncSession, "get", disappear_on_refresh)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 409, response.text
    assert "mirror changed before cancellation" in response.text
    cancel_remote.assert_not_awaited()
    stop_lifecycle.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_does_not_cancel_active_exact_recovery_and_rearms_sweep(
    client,
    session_factory,
    monkeypatch,
):
    graph = await _seed_worker_plan_run(session_factory)
    async with session_factory() as db:
        await fence_worker_mirror_cancellation(
            db,
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            generation=graph.generation,
            payload_digest=graph.payload_digest,
        )

    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    dispatcher = GlobalDispatcher(
        db_factory=session_factory,
        instance_manager=manager,
        broadcaster=MagicMock(broadcast=AsyncMock()),
    )
    dispatcher._running = True
    dispatcher._shutting_down = False
    dispatcher.wake = MagicMock()

    recovery_entered = asyncio.Event()

    async def active_exact_recovery():
        recovery_entered.set()
        await asyncio.Event().wait()

    recovery = asyncio.create_task(active_exact_recovery())
    setattr(recovery, "_ccm_worker_plan_run_id", graph.run_id)
    setattr(recovery, "_ccm_worker_plan_cancellation_recovery", True)
    dispatcher._running_tasks[f"worker-plan-{graph.run_id}"] = recovery
    await recovery_entered.wait()

    cancel_remote = AsyncMock(side_effect=RuntimeError("retry ACK lost"))
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(main_module, "dispatcher", dispatcher)

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 503, response.text
    cancel_remote.assert_awaited_once_with(
        graph.worker_id,
        graph.run_id,
        plan_id=graph.plan_id,
        payload_digest=graph.payload_digest,
    )
    assert not recovery.done()
    assert dispatcher._plan_runtime_recovery_not_before is not None
    assert dispatcher.wake.called
    await _assert_worker_mirror_cancellation_fenced(session_factory, graph)

    recovery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await recovery


@pytest.mark.asyncio
async def test_post_commit_request_cancellation_still_reaps_old_lifecycle(
    client,
    session_factory,
    monkeypatch,
):
    graph = await _seed_worker_plan_run(session_factory)
    cancel_remote = AsyncMock()
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    refresh_entered = asyncio.Event()
    release_refresh = asyncio.Event()
    original_refresh = AsyncSession.refresh
    blocked = False

    async def block_post_commit_refresh(self, instance, *args, **kwargs):
        nonlocal blocked
        if (
            not blocked
            and isinstance(instance, PlanAgentRun)
            and instance.id == graph.run_id
        ):
            blocked = True
            refresh_entered.set()
            await release_refresh.wait()
        return await original_refresh(self, instance, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "refresh", block_post_commit_refresh)
    request_task = asyncio.create_task(
        client.post(f"/api/plan-runs/{graph.run_id}/cancel")
    )
    await asyncio.wait_for(refresh_entered.wait(), timeout=2)

    # The fence commit has completed, but the mutation helper has not yet
    # returned to the request task. Cancellation must be delayed until the
    # helper proves its result, arms cleanup and reaps the old lifecycle.
    request_task.cancel()
    await asyncio.sleep(0)
    release_refresh.set()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    cancel_remote.assert_not_awaited()
    stop_lifecycle.assert_awaited_once_with(graph.run_id, None)
    await _assert_worker_mirror_cancellation_fenced(session_factory, graph)


@pytest.mark.asyncio
async def test_lifecycle_reap_settles_repeated_cancellation_after_lock_release(
    client,
    session_factory,
    monkeypatch,
):
    graph = await _seed_worker_plan_run(session_factory)
    cancel_remote = AsyncMock(
        return_value=_terminal_cancel_receipt(graph, status="cancelled")
    )
    stop_entered = asyncio.Event()
    release_stop = asyncio.Event()
    stop_completed = asyncio.Event()

    async def blocking_stop(run_id, instance_id):
        assert run_id == graph.run_id
        assert instance_id is None
        # Acquiring the same aggregate lock proves the inner API context has
        # already exited before lifecycle cleanup begins.
        async with plan_operation_lock(graph.plan_id):
            stop_entered.set()
            await release_stop.wait()
        stop_completed.set()
        return True

    stop_lifecycle = AsyncMock(side_effect=blocking_stop)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    request_task = asyncio.create_task(
        client.post(f"/api/plan-runs/{graph.run_id}/cancel")
    )
    await asyncio.wait_for(stop_entered.wait(), timeout=2)
    request_task.cancel()
    await asyncio.sleep(0)
    request_task.cancel()
    await asyncio.sleep(0)
    assert not stop_completed.is_set()
    release_stop.set()

    with pytest.raises(asyncio.CancelledError):
        await request_task
    assert stop_completed.is_set()
    stop_lifecycle.assert_awaited_once_with(graph.run_id, None)
    async with session_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        assert plan is not None and plan.active_run_id is None
        assert run is not None and run.status == "cancelled"


@pytest.mark.asyncio
async def test_manager_mirror_terminalizes_after_exact_ack_and_replay_skips_rpc(
    client,
    session_factory,
    monkeypatch,
):
    graph = await _seed_worker_plan_run(session_factory, generation=0)
    cancel_remote = AsyncMock(
        return_value=_terminal_cancel_receipt(graph, status="cancelled")
    )
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    first = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")
    replay = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert first.json()["status"] == "cancelled"
    assert replay.json()["status"] == "cancelled"
    cancel_remote.assert_awaited_once_with(
        graph.worker_id,
        graph.run_id,
        plan_id=graph.plan_id,
        payload_digest=graph.payload_digest,
    )
    stop_lifecycle.assert_awaited_once_with(graph.run_id, None)

    async with session_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        assert plan is not None and plan.active_run_id is None
        assert run is not None and run.status == "cancelled"
        assert run.generation == graph.generation + 1
        assert run.cancellation_target_generation == graph.generation
        assert run.worker_id == graph.worker_id
        assert run.last_execution_started_at is None
        assert run.finished_at is not None
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep).where(PlanAgentStep.run_id == graph.run_id)
                )
            ).scalars()
        )
        assert len(steps) == 1
        assert steps[0].worker_id == graph.worker_id
        assert steps[0].output == "partial remote plan evidence"
        receipt = (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt)
                .where(PlanAgentWorkerDispatchReceipt.run_id == graph.run_id)
                .order_by(PlanAgentWorkerDispatchReceipt.run_generation.desc())
            )
        ).scalars().first()
        assert receipt is not None
        assert receipt.settlement_reason == "remote_pause"
        assert receipt.remote_status == "cancelled"
        assert await worker_mirror_run_is_clean(db, run_id=graph.run_id)
        assert run.error == "Cancelled by user"


@pytest.mark.asyncio
async def test_cancelled_graph_after_waiting_pause_uses_successor_receipt(
    client,
    session_factory,
    monkeypatch,
):
    graph = await _seed_worker_plan_run(
        session_factory,
        run_status="waiting_user",
        generation=0,
    )
    remote = _cancelled_receipt_with_input(graph)
    remote_step = remote["run"]["steps"][0]
    remote_input = remote["run"]["input_requests"][0]
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt).where(
                    PlanAgentWorkerDispatchReceipt.run_id == graph.run_id
                )
            )
        ).scalar_one()
        step = PlanAgentStep(
            run_id=graph.run_id,
            plan_id=graph.plan_id,
            worker_id=graph.worker_id,
            worker_step_id=remote_step["id"],
            generation=remote_step["generation"],
            step_type=remote_step["step_type"],
            round=remote_step["round"],
            provider=remote_step["provider"],
            model=remote_step["model"],
            effort=remote_step["effort"],
            route_slot=remote_step["route_slot"],
            status=remote_step["status"],
            output=remote_step["output"],
            error=remote_step["error"],
            last_delta_at=graph.started_at,
            streamed_output_chars=remote_step["streamed_output_chars"],
            last_event_type=remote_step["last_event_type"],
            started_at=graph.started_at,
            finished_at=graph.started_at,
        )
        db.add(step)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            worker_input_request_id=remote_input["id"],
            source_step_id=step.id,
            requested_by=remote_input["requested_by"],
            reason=remote_input["reason"],
            questions=remote_input["questions"],
            status="open",
            idempotency_key=f"worker:{graph.worker_id}:input:{remote_input['id']}",
            opened_at=graph.started_at,
            created_at=graph.started_at,
        )
        db.add(input_request)
        await db.flush()
        step.input_request_id = input_request.id
        assert run is not None
        run.open_input_request_id = input_request.id
        run.interaction_count = 1
        settled_at = datetime.utcnow()
        receipt.status = "settled"
        receipt.settlement_reason = "remote_pause"
        receipt.remote_status = "waiting_user"
        receipt.settled_at = settled_at
        receipt.updated_at = settled_at
        await db.commit()

    drifted_remote = deepcopy(remote)
    drifted_remote["run"]["input_requests"][0]["questions"][0][
        "question"
    ] = "A different immutable question"
    cancel_remote = AsyncMock(side_effect=[drifted_remote, remote])
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    rejected = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")
    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert rejected.status_code == 409, rejected.text
    assert "InputRequest mapping changed immutable content" in rejected.text
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "cancelled"
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipts = list(
            (
                await db.execute(
                    select(PlanAgentWorkerDispatchReceipt)
                    .where(PlanAgentWorkerDispatchReceipt.run_id == graph.run_id)
                    .order_by(PlanAgentWorkerDispatchReceipt.run_generation)
                )
            ).scalars()
        )
        assert run is not None and run.status == "cancelled"
        assert run.generation == 1
        assert run.cancellation_target_generation == 0
        assert [
            (item.run_generation, item.settlement_reason, item.remote_status)
            for item in receipts
        ] == [
            (0, "remote_pause", "waiting_user"),
            (1, "remote_pause", "cancelled"),
        ]
        input_request = (
            await db.execute(
                select(PlanInputRequest).where(
                    PlanInputRequest.run_id == graph.run_id
                )
            )
        ).scalar_one()
        step = (
            await db.execute(
                select(PlanAgentStep).where(PlanAgentStep.run_id == graph.run_id)
            )
        ).scalar_one()
        assert input_request.status == "cancelled"
        assert input_request.cancelled_at is not None
        assert step.input_request_id == input_request.id
        assert await worker_mirror_run_is_clean(db, run_id=graph.run_id)

        # Cardinality, not merely non-emptiness, closes multi-interaction
        # history. Keep a second complete Step/Input pair, then remove the first
        # pair's Input: one surviving pair must not hide that evidence loss.
        second_step = PlanAgentStep(
            run_id=graph.run_id,
            plan_id=graph.plan_id,
            worker_id=graph.worker_id,
            worker_step_id=remote_step["id"] + 1,
            generation=1,
            step_type="planner",
            round=1,
            provider="claude",
            status="cancelled",
            started_at=graph.started_at,
            finished_at=graph.started_at,
        )
        db.add(second_step)
        await db.flush()
        second_input = PlanInputRequest(
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            worker_input_request_id=remote_input["id"] + 1,
            source_step_id=second_step.id,
            requested_by="planner",
            questions=[],
            status="cancelled",
            idempotency_key=(
                f"worker:{graph.worker_id}:input:{remote_input['id'] + 1}"
            ),
            opened_at=graph.started_at,
            cancelled_at=graph.started_at,
            created_at=graph.started_at,
        )
        db.add(second_input)
        await db.flush()
        second_step.input_request_id = second_input.id
        run.interaction_count = 2
        await db.commit()
        assert await worker_mirror_run_is_clean(db, run_id=graph.run_id)

        step.input_request_id = None
        await db.delete(input_request)
        await db.commit()
        assert not await worker_mirror_run_is_clean(db, run_id=graph.run_id)


@pytest.mark.asyncio
async def test_legacy_remote_cancelled_without_imported_graph_is_not_clean(
    session_factory,
):
    graph = await _seed_worker_plan_run(session_factory, generation=0)
    async with session_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt).where(
                    PlanAgentWorkerDispatchReceipt.run_id == graph.run_id
                )
            )
        ).scalar_one()
        assert plan is not None and run is not None
        plan.active_run_id = None
        run.status = "cancelled"
        run.generation = 1
        run.cancellation_target_generation = 0
        run.last_execution_started_at = None
        run.finished_at = datetime.utcnow()
        receipt.status = "settled"
        receipt.settlement_reason = "remote_cancelled"
        receipt.remote_status = "cancelled"
        receipt.settled_at = datetime.utcnow()
        await db.commit()

        assert not await worker_mirror_run_is_clean(db, run_id=graph.run_id)


@pytest.mark.asyncio
async def test_absence_finalizer_rejects_legacy_status_only_cancelled_state(
    session_factory,
):
    async with session_factory() as db:
        with pytest.raises(
            WorkerPlanDispatchConflict,
            match="cancellation outcome identity is invalid",
        ):
            await finalize_worker_mirror_cancellation(
                db,
                plan_id=1,
                run_id=1,
                worker_id=1,
                target_generation=0,
                payload_digest="a" * 64,
                remote_state="cancelled",
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["completed", "failed"])
async def test_worker_terminal_outcome_wins_exact_manager_cancellation(
    client,
    session_factory,
    monkeypatch,
    terminal_status,
):
    # A clean Worker mirror needs a continuous Manager dispatch history.  Use
    # the first claim here so this focused fixture's single receipt is the
    # complete 0..G history rather than fabricating unrelated old receipts.
    graph = await _seed_worker_plan_run(session_factory, generation=0)
    remote = _terminal_cancel_receipt(graph, status=terminal_status)
    cancel_remote = AsyncMock(return_value=remote)
    stop_lifecycle = AsyncMock(return_value=True)
    request_recovery = MagicMock()
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            stop_plan_run_lifecycle=stop_lifecycle,
            _request_plan_runtime_recovery=request_recovery,
        ),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == terminal_status
    cancel_remote.assert_awaited_once_with(
        graph.worker_id,
        graph.run_id,
        plan_id=graph.plan_id,
        payload_digest=graph.payload_digest,
    )
    request_recovery.assert_called_once_with()
    stop_lifecycle.assert_awaited_once_with(graph.run_id, None)
    async with session_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt).where(
                    PlanAgentWorkerDispatchReceipt.run_id == graph.run_id,
                    PlanAgentWorkerDispatchReceipt.run_generation
                    == graph.generation,
                )
            )
        ).scalar_one()
        assert plan is not None and plan.active_run_id is None
        assert run is not None and run.status == terminal_status
        assert run.generation == graph.generation
        assert run.cancellation_target_generation is None
        assert receipt.status == "settled"
        assert receipt.settlement_reason == "remote_pause"
        assert receipt.remote_status == terminal_status
        assert await worker_mirror_run_is_clean(db, run_id=graph.run_id)
        if terminal_status == "completed":
            version = await db.get(PlanVersion, run.result_version_id)
            assert version is not None
            assert version.worker_version_id == remote["run"]["result_version_id"]
            assert plan.current_version_id == version.id
            await db.delete(version)
            await db.flush()
            assert not await worker_mirror_run_is_clean(
                db,
                run_id=graph.run_id,
            )
            await db.rollback()

            imported_steps = list(
                (
                    await db.execute(
                        select(PlanAgentStep).where(
                            PlanAgentStep.run_id == graph.run_id
                        )
                    )
                ).scalars()
            )
            producer = next(
                imported_step
                for imported_step in imported_steps
                if imported_step.step_type == "planner"
                and imported_step.plan_version_id is not None
            )
            await db.delete(producer)
            await db.commit()
            assert not await worker_mirror_run_is_clean(
                db,
                run_id=graph.run_id,
            )
        else:
            assert run.result_version_id is None
            assert run.error == "remote failure"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("run_status", "receipt_status", "settlement_reason"),
    [
        ("queued", None, None),
        ("running", "prepared", "not_launched"),
    ],
)
async def test_preimport_manager_cancel_never_calls_worker_and_is_clean(
    client,
    session_factory,
    monkeypatch,
    run_status,
    receipt_status,
    settlement_reason,
):
    graph = await _seed_worker_plan_run(
        session_factory,
        receipt_status=receipt_status,
        run_status=run_status,
    )
    cancel_remote = AsyncMock()
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "cancelled"
    cancel_remote.assert_not_awaited()
    async with session_factory() as db:
        receipt = (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt).where(
                    PlanAgentWorkerDispatchReceipt.run_id == graph.run_id
                )
            )
        ).scalar_one_or_none()
        if settlement_reason is None:
            assert receipt is None
        else:
            assert receipt is not None
            assert receipt.status == "settled"
            assert receipt.settlement_reason == settlement_reason
            assert receipt.remote_status is None
        assert await worker_mirror_run_is_clean(db, run_id=graph.run_id)


@pytest.mark.asyncio
async def test_preimport_cancel_remote_possible_winner_is_http_409(
    client,
    session_factory,
    monkeypatch,
):
    """A boundary winner after the stale pre-import read is not an API 500."""

    graph = await _seed_worker_plan_run(
        session_factory,
        receipt_status="prepared",
        run_status="running",
    )
    async with session_factory() as db:
        receipt = (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt).where(
                    PlanAgentWorkerDispatchReceipt.run_id == graph.run_id,
                    PlanAgentWorkerDispatchReceipt.run_generation
                    == graph.generation,
                )
            )
        ).scalar_one()
        receipt_id = receipt.id

    real_cancel = plan_resources_module.cancel_worker_mirror_run_after_ack

    async def race_remote_boundary(db, *, plan, run, **kwargs):
        # The endpoint already classified this receipt as prepared. Simulate
        # the dispatcher callback committing after that read but before the
        # cancellation service's fresh Run-first writer transaction.
        await mark_worker_dispatch_remote_possible(
            session_factory,
            receipt_id=receipt_id,
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            generation=graph.generation,
            payload_digest=graph.payload_digest,
        )
        return await real_cancel(db, plan=plan, run=run, **kwargs)

    monkeypatch.setattr(
        plan_resources_module,
        "cancel_worker_mirror_run_after_ack",
        race_remote_boundary,
    )
    cancel_remote = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=AsyncMock(return_value=True)),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 409, response.text
    assert "settlement contradicts" in response.text
    cancel_remote.assert_not_awaited()
    async with session_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(PlanAgentWorkerDispatchReceipt, receipt_id)
        assert plan is not None and plan.active_run_id == graph.run_id
        assert run is not None and run.status == "running"
        assert run.generation == graph.generation
        assert receipt is not None and receipt.status == "remote_possible"
        assert receipt.payload_digest == graph.payload_digest


@pytest.mark.asyncio
async def test_worker_mirror_cancel_locks_run_plan_then_dispatch_receipt(
    session_factory,
    monkeypatch,
):
    """Remote ACK settlement follows the graph-wide canonical lock order."""

    graph = await _seed_worker_plan_run(session_factory, receipt_status=None)
    async with session_factory() as db:
        receipt = PlanAgentWorkerDispatchReceipt(
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            run_generation=graph.generation,
            protocol=1,
            status="prepared",
        )
        db.add(receipt)
        input_request = PlanInputRequest(
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            worker_input_request_id=701,
            source_step_id=700,
            requested_by="planner",
            questions=[],
            status="open",
            idempotency_key=f"worker-cancel-lock-order:{graph.run_id}",
            opened_at=datetime.utcnow(),
        )
        db.add(input_request)
        await db.flush()
        run = await db.get(PlanAgentRun, graph.run_id)
        assert run is not None
        run.status = "waiting_user"
        run.open_input_request_id = input_request.id
        await db.commit()

    original_get = AsyncSession.get
    original_execute = AsyncSession.execute
    locked_entities: list[type] = []
    mutation_order: list[str] = []

    async def traced_get(self, entity, ident, **kwargs):
        if kwargs.get("with_for_update") and entity in {PlanAgentRun, Plan}:
            locked_entities.append(entity)
        return await original_get(self, entity, ident, **kwargs)

    async def traced_execute(self, statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if getattr(statement, "is_update", False) and table is not None:
            mutation_order.append(f"update:{table.name}")
        if getattr(statement, "_for_update_arg", None) is not None:
            descriptions = getattr(statement, "column_descriptions", ())
            entity = descriptions[0].get("entity") if descriptions else None
            if entity is PlanAgentWorkerDispatchReceipt:
                locked_entities.append(entity)
                mutation_order.append("lock:plan_agent_worker_dispatch_receipts")
        return await original_execute(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "get", traced_get)
    monkeypatch.setattr(AsyncSession, "execute", traced_execute)

    async with session_factory() as db:
        plan = await original_get(db, Plan, graph.plan_id)
        run = await original_get(db, PlanAgentRun, graph.run_id)
        assert plan is not None and run is not None
        result = await cancel_worker_mirror_run_after_ack(
            db,
            plan=plan,
            run=run,
        )

    assert result.status == "cancelled"
    assert result.cancellation_target_generation == graph.generation
    assert locked_entities[:3] == [
        PlanAgentRun,
        Plan,
        PlanAgentWorkerDispatchReceipt,
    ]
    assert mutation_order[:5] == [
        "update:plan_agent_runs",
        "update:plans",
        "lock:plan_agent_worker_dispatch_receipts",
        "update:plan_input_requests",
        "update:plan_agent_runs",
    ]


@pytest.mark.asyncio
async def test_manager_mirror_rejects_boolean_ack_identity(
    client,
    session_factory,
    monkeypatch,
):
    """The API's defense in depth must also compare remote ids by type."""

    graph = await _seed_worker_plan_run(session_factory)
    assert graph.run_id == 1
    cancel_remote = AsyncMock(
        return_value={
            "protocol": 1,
            "state": "cancelled",
            "plan_id": graph.plan_id,
            "run_id": True,
            "payload_digest": graph.payload_digest,
        }
    )
    stop_lifecycle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main_module,
        "worker_proxy",
        SimpleNamespace(cancel_versioned_plan_run=cancel_remote),
    )
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 503, response.text
    stop_lifecycle.assert_awaited_once_with(graph.run_id, None)
    await _assert_worker_mirror_cancellation_fenced(session_factory, graph)


def _worker_import_body(*, plan_id: int, run_id: int) -> dict:
    return {
        "protocol": 3,
        "plan_id": plan_id,
        "run_id": run_id,
        "manager_claim_generation": 0,
        "title": "Exact imported Plan",
        "initial_request": "Plan without identity ambiguity",
        "priority": 0,
        "pipeline_config": default_plan_pipeline_config().model_dump(mode="json"),
        "run_type": "initial",
        "request_text": "Plan without identity ambiguity",
        "max_interactions": 3,
    }


def _worker_import_digest(body: dict) -> str:
    parsed = WorkerPlanRunImportRequest.model_validate(body)
    return _worker_run_import_digest(parsed, [])


def _exact_cancel_body(*, plan_id: int, payload_digest: str) -> dict:
    return {
        "protocol": 1,
        "plan_id": plan_id,
        "payload_digest": payload_digest,
    }


@pytest.mark.asyncio
async def test_cancel_before_import_tombstone_blocks_late_import_and_audits(
    client,
    session_factory,
):
    body = _worker_import_body(plan_id=8101, run_id=8201)
    digest = _worker_import_digest(body)
    cancel_body = _exact_cancel_body(plan_id=8101, payload_digest=digest)

    first = await client.post(
        "/api/plan-runs/8201/worker-import-cancel",
        json=cancel_body,
    )
    replay = await client.post(
        "/api/plan-runs/8201/worker-import-cancel",
        json=cancel_body,
    )
    late_import = await client.post("/api/plans/worker-import", json=body)
    audit = await client.get(
        "/api/plan-runs/8201/worker-import-audit",
        params={"plan_id": 8101, "payload_digest": digest},
    )
    digest_collision = await client.post(
        "/api/plan-runs/8201/worker-import-cancel",
        json=_exact_cancel_body(plan_id=8101, payload_digest="f" * 64),
    )
    plan_collision = await client.post(
        "/api/plan-runs/8201/worker-import-cancel",
        json=_exact_cancel_body(plan_id=8102, payload_digest=digest),
    )

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert first.json()["state"] == replay.json()["state"] == "absent"
    assert first.json()["base_worker_version_id"] is None
    assert first.json()["versions"] == []
    assert late_import.status_code == 409
    assert "cancelled before admission" in late_import.text
    assert audit.status_code == 200, audit.text
    assert audit.json()["state"] == "cancelled"
    assert digest_collision.status_code == 409
    assert plan_collision.status_code == 409
    async with session_factory() as db:
        receipt = await db.get(PlanAgentWorkerImportReceipt, 8201)
        assert receipt is not None
        assert receipt.plan_id == 8101
        assert receipt.payload_digest == digest
        assert receipt.outcome == "cancelled_before_import"
        assert await db.get(PlanAgentRun, 8201) is None
        assert await db.get(Plan, 8101) is None


@pytest.mark.asyncio
async def test_import_winner_exact_cancel_is_idempotent_and_receipt_survives_delete(
    client,
    session_factory,
    monkeypatch,
):
    body = _worker_import_body(plan_id=8301, run_id=8401)
    imported = await client.post("/api/plans/worker-import", json=body)
    assert imported.status_code == 200, imported.text
    digest = imported.json()["import_payload_digest"]
    stop_lifecycle = AsyncMock(return_value=False)
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_lifecycle),
    )

    first = await client.post(
        "/api/plan-runs/8401/worker-import-cancel",
        json=_exact_cancel_body(plan_id=8301, payload_digest=digest),
    )
    replay = await client.post(
        "/api/plan-runs/8401/worker-import-cancel",
        json=_exact_cancel_body(plan_id=8301, payload_digest=digest),
    )

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert first.json()["state"] == replay.json()["state"] == "terminal"
    assert first.json()["run"]["status"] == "cancelled"
    assert first.json()["versions"] == replay.json()["versions"] == []
    async with session_factory() as db:
        receipt = await db.get(PlanAgentWorkerImportReceipt, 8401)
        run = await db.get(PlanAgentRun, 8401)
        plan = await db.get(Plan, 8301)
        assert receipt is not None and receipt.outcome == "imported"
        assert run is not None and run.status == "cancelled"
        assert run.import_receipt_protocol == 1
        assert receipt.run_id == run.id
        assert receipt.plan_id == run.plan_id
        assert receipt.payload_digest == run.import_payload_digest
        assert plan is not None and plan.active_run_id is None
        await db.delete(run)
        await db.delete(plan)
        await db.commit()

    late_replay = await client.post("/api/plans/worker-import", json=body)
    audit = await client.get(
        "/api/plan-runs/8401/worker-import-audit",
        params={"plan_id": 8301, "payload_digest": digest},
    )
    cancel_after_delete = await client.post(
        "/api/plan-runs/8401/worker-import-cancel",
        json=_exact_cancel_body(plan_id=8301, payload_digest=digest),
    )
    assert late_replay.status_code == 409
    assert "historical" in late_replay.text
    assert audit.status_code == 409
    assert "historical" in audit.text
    assert cancel_after_delete.status_code == 200
    assert cancel_after_delete.json()["state"] == "absent"
    async with session_factory() as db:
        assert await db.get(PlanAgentWorkerImportReceipt, 8401) is not None


@pytest.mark.asyncio
async def test_worker_drain_claim_rejects_late_exact_plan_cancel(
    client,
    session_factory,
    monkeypatch,
    worker_control_plane_auth,
):
    """A drain-first Worker must not publish a late cancelled Plan graph."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    body = _worker_import_body(plan_id=8411, run_id=8412)
    imported = await client.post("/api/plans/worker-import", json=body)
    assert imported.status_code == 200, imported.text
    digest = imported.json()["import_payload_digest"]

    async with session_factory() as db:
        run_before = await db.get(PlanAgentRun, 8412)
        receipt_before = await db.get(PlanAgentWorkerImportReceipt, 8412)
        assert run_before is not None
        assert receipt_before is not None and receipt_before.outcome == "imported"
        status_before = run_before.status
        await begin_worker_node_drain(db, claim="d" * 64)
        await db.commit()

    response = await client.post(
        "/api/plan-runs/8412/worker-import-cancel",
        json=_exact_cancel_body(plan_id=8411, payload_digest=digest),
    )

    assert response.status_code == 409, response.text
    assert "destruction has begun" in response.text
    async with session_factory() as db:
        run_after = await db.get(PlanAgentRun, 8412)
        receipt_after = await db.get(PlanAgentWorkerImportReceipt, 8412)
        assert run_after is not None and run_after.status == status_before
        assert receipt_after is not None and receipt_after.outcome == "imported"


@pytest.mark.asyncio
async def test_worker_fork_import_uses_fresh_lineage_and_receipt_gate(
    client,
    session_factory,
):
    body = _worker_import_body(plan_id=8451, run_id=8452)
    body.update(
        {
            "run_type": "fork",
            "source_run_id": 8401,
            "request_text": (
                "Fork the selected Plan\n\n"
                "[Base Version selected for this fork]\nsource content"
            ),
        }
    )

    imported = await client.post("/api/plans/worker-import", json=body)

    assert imported.status_code == 200, imported.text
    assert imported.json()["base_worker_version_id"] is None
    digest = imported.json()["import_payload_digest"]
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, 8452)
        assert run is not None
        assert run.run_type == "fork"
        assert run.source_run_id == 8401
        assert run.base_version_id is None
        assert run.import_receipt_protocol == 1
        receipt = await db.get(PlanAgentWorkerImportReceipt, 8452)
        assert receipt is not None
        assert receipt.plan_id == 8451
        assert receipt.payload_digest == digest == run.import_payload_digest
        assert receipt.outcome == "imported"
        assert await db.scalar(
            select(func.count())
            .select_from(PlanAgentWorkerImportReceipt)
            .where(PlanAgentWorkerImportReceipt.run_id == 8452)
        ) == 1


@pytest.mark.asyncio
async def test_exact_cancel_never_touches_same_run_id_local_collision(
    client,
    session_factory,
):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            id=8501,
            title="Local Plan",
            initial_request="local",
            pipeline_config=pipeline,
        )
        run = PlanAgentRun(
            id=8601,
            plan_id=plan.id,
            run_type="initial",
            request_text="local",
            pipeline_config=pipeline,
            status="queued",
            current_stage="planner",
        )
        db.add_all([plan, run])
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()

    response = await client.post(
        "/api/plan-runs/8601/worker-import-cancel",
        json=_exact_cancel_body(plan_id=8502, payload_digest="c" * 64),
    )
    assert response.status_code == 409
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, 8601)
        assert run is not None and run.status == "queued"
        assert await db.get(PlanAgentWorkerImportReceipt, 8601) is None


@pytest.mark.asyncio
async def test_import_cancel_concurrency_has_one_durable_identity_winner(
    client,
    session_factory,
    monkeypatch,
):
    body = _worker_import_body(plan_id=8701, run_id=8801)
    digest = _worker_import_digest(body)
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=AsyncMock(return_value=False)),
    )

    imported, cancelled = await asyncio.gather(
        client.post("/api/plans/worker-import", json=body),
        client.post(
            "/api/plan-runs/8801/worker-import-cancel",
            json=_exact_cancel_body(plan_id=8701, payload_digest=digest),
        ),
    )

    assert imported.status_code in {200, 409}, imported.text
    assert cancelled.status_code == 200, cancelled.text
    async with session_factory() as db:
        receipts = list(
            (
                await db.execute(
                    select(PlanAgentWorkerImportReceipt).where(
                        PlanAgentWorkerImportReceipt.run_id == 8801
                    )
                )
            ).scalars()
        )
        assert len(receipts) == 1
        assert receipts[0].plan_id == 8701
        assert receipts[0].payload_digest == digest
        run = await db.get(PlanAgentRun, 8801)
        if receipts[0].outcome == "cancelled_before_import":
            assert imported.status_code == 409
            assert run is None
        else:
            assert run is not None and run.status == "cancelled"
