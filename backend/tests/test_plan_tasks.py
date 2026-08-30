import asyncio
import json
import os
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import func, select, update

from backend.api.plans import _plan_upload_fields
from backend.models.log_entry import LogEntry
from backend.models.global_settings import GlobalSettings
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.sub_agent import SubAgentSession
from backend.models.task import Task
from backend.models.worker import Worker
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.plan_tasks import capture_repo_revision


def test_plan_upload_fields_recovers_image_subset_from_worker_metadata():
    task = Task(
        metadata_={
            "image_paths": ["/uploads/mockup.png", "/uploads/notes.txt"],
            "attachments": [
                {"name": "mockup.png", "is_image": True},
                {"name": "notes.txt", "is_image": False},
            ],
        },
    )

    files, images, attachments = _plan_upload_fields(task)

    assert files == ["/uploads/mockup.png", "/uploads/notes.txt"]
    assert images == ["/uploads/mockup.png"]
    assert attachments == task.metadata_["attachments"]


async def _target_with_session(client, session_factory) -> tuple[int, str]:
    response = await client.post(
        "/api/tasks",
        json={
            "title": "Target",
            "description": "initial request",
            "target_repo": "/tmp",
        },
    )
    task_id = response.json()["id"]
    session_id = "target-session-1"
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                session_id=session_id,
                status="completed",
                completed_at=None,
            )
        )
        db.add(
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="user_message",
                role="user",
                content="A real follow-up",
            )
        )
        db.add(
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="message",
                role="assistant",
                content="Existing session context",
            )
        )
        await db.commit()
    return task_id, session_id


async def _legacy_plan_task(session_factory, **values) -> int:
    """Create a historical carrier row without reopening its public write path."""

    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    fields = {
        "title": "Legacy Plan",
        "description": "Historical planning request",
        "target_repo": "/tmp",
        "mode": "plan",
        "provider": pipeline["planner"]["primary"]["provider"],
        "model": pipeline["planner"]["primary"]["model"],
        "effort_level": pipeline["planner"]["primary"]["effort"],
        "plan_pipeline_config": pipeline,
        "plan_repo_revision": await capture_repo_revision("/tmp"),
    }
    fields.update(values)
    async with session_factory() as db:
        task = Task(**fields)
        db.add(task)
        await db.commit()
        await db.refresh(task)
        return task.id


@pytest.mark.asyncio
async def test_related_plans_are_independent_and_limited(
    client,
    session_factory,
):
    target_id, session_id = await _target_with_session(
        client,
        session_factory,
    )
    plan_ids = []
    for index in range(3):
        response = await client.post(
            f"/api/tasks/{target_id}/plans",
            json={"input": f"Plan request {index}"},
        )
        assert response.status_code == 201
        data = response.json()
        plan_ids.append(data["id"])
        assert data["mode"] == "plan"
        assert data["plan_target_task_id"] == target_id
        assert data["provider"] == "claude"
        assert data["model"] == "claude-fable-5"
        assert data["plan_pipeline_config"]["planner"]["primary"] == {
            "provider": "claude",
            "model": "claude-fable-5",
            "effort": "high",
        }
        assert data["plan_pipeline_config"]["reviewer"]["primary"] == {
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "effort": "xhigh",
        }
        assert "plan_context_snapshot" not in data
        # The immutable transcript is deliberately not exposed in Task list
        # payloads; verify its durable capture directly.
        async with session_factory() as db:
            plan = await db.get(Task, data["id"])
            assert plan.plan_context_session_id == session_id
            assert "initial request" in plan.plan_context_snapshot
            assert "A real follow-up" in plan.plan_context_snapshot
            assert "Existing session context" in plan.plan_context_snapshot

    fourth = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "one too many"},
    )
    assert fourth.status_code == 429
    generic_bypass = await client.post(
        "/api/tasks",
        json={
            "title": "Bypass attempt",
            "description": "one too many through generic create",
            "mode": "plan",
            "plan_target_task_id": target_id,
        },
    )
    assert generic_bypass.status_code == 410
    assert "POST /api/plans" in generic_bypass.text

    history = await client.get(f"/api/tasks/{target_id}/plans")
    assert history.status_code == 200
    assert {item["id"] for item in history.json()} == set(plan_ids)

    async with session_factory() as db:
        target = await db.get(Task, target_id)
    assert target.status == "completed"
    assert target.session_id == session_id


@pytest.mark.asyncio
async def test_related_plan_title_does_not_expose_empty_target_placeholder(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == target_id).values(title="")
        )
        await db.commit()

    response = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Inspect the current behavior"},
    )

    assert response.status_code == 201
    assert response.json()["title"] == f"Plan for #{target_id}"


@pytest.mark.asyncio
async def test_related_plan_preserves_validated_uploads(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    uploaded = await client.post(
        "/api/uploads",
        files={"files": ("design notes.txt", b"reference", "text/plain")},
    )
    assert uploaded.status_code == 200, uploaded.text
    upload = uploaded.json()[0]

    response = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={
            "input": "Use the attached design notes",
            "file_paths": [upload["path"]],
            "image_paths": [],
            "attachments": [{
                "url": upload["url"],
                "name": upload["filename"],
                "is_image": False,
            }],
        },
    )

    assert response.status_code == 201, response.text
    metadata = response.json()["metadata_"]
    assert "file_paths" not in metadata
    assert "image_paths" not in metadata
    assert metadata["attachments"] == [{
        "url": upload["url"],
        "name": "design notes.txt",
        "is_image": False,
    }]

    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == response.json()["id"])
            .values(status="plan_review", plan_content="First draft")
        )
        await db.commit()
    revision = await client.post(
        f"/api/tasks/{response.json()['id']}/plan/revise",
        json={"feedback": "Add rollback details"},
    )
    assert revision.status_code == 201, revision.text
    revised_metadata = revision.json()["metadata_"]
    assert "file_paths" not in revised_metadata
    assert "image_paths" not in revised_metadata
    assert revised_metadata["attachments"] == metadata["attachments"]


@pytest.mark.asyncio
async def test_related_plan_rejects_unmanaged_attachment_paths(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)

    response = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={
            "input": "Read this file",
            "file_paths": ["/tmp/not-a-managed-upload.txt"],
        },
    )

    assert response.status_code == 400
    assert "CCM upload directory" in response.json()["detail"]


@pytest.mark.asyncio
async def test_related_plan_snapshots_custom_primary_and_fallback_routes(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    pipeline = {
        "version": 1,
        "planner": {
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-luna",
                "effort": "max",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "ultra",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 3,
    }

    response = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={
            "input": "Use explicit two-stage routes",
            "pipeline_config": pipeline,
        },
    )

    assert response.status_code == 422, response.text
    assert "configured globally" in response.text


@pytest.mark.asyncio
async def test_new_plans_snapshot_the_global_pipeline_settings(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    pipeline = {
        "version": 1,
        "planner": {
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "ultra",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
        },
        "max_revision_cycles": 2,
    }
    async with session_factory() as db:
        db.add(GlobalSettings(id=1, plan_pipeline_config=pipeline))
        await db.commit()

    related = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Use global settings"},
    )
    standalone = await client.post(
        "/api/plans",
        json={
            "input": "Use global settings",
            "title": "Standalone global Plan",
            "target_repo": "/tmp",
        },
    )

    assert related.status_code == 201, related.text
    assert standalone.status_code == 201, standalone.text
    expected = {**pipeline, "max_interactions": 3}
    related_data = related.json()
    assert related_data["provider"] == "codex"
    assert related_data["model"] == "gpt-5.6-terra"
    assert related_data["plan_pipeline_config"] == expected
    assert standalone.json()["pipeline_config"] == expected


@pytest.mark.asyncio
async def test_related_plan_revision_creates_successor_and_retires_source(
    client,
    session_factory,
):
    import backend.main

    target_id, _ = await _target_with_session(client, session_factory)
    created = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Design the first version"},
    )
    source_id = created.json()["id"]
    source_pipeline = created.json()["plan_pipeline_config"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == source_id)
            .values(status="plan_review", plan_content="Original proposal")
        )
        await db.commit()
    queue_fence = await backend.main.dispatcher.snapshot_queue_admission(source_id)

    revised = await client.post(
        f"/api/tasks/{source_id}/plan/revise",
        json={"feedback": "Keep the API backwards compatible"},
    )

    assert revised.status_code == 201, revised.text
    successor = revised.json()
    assert successor["id"] != source_id
    assert successor["status"] == "pending"
    assert successor["plan_target_task_id"] == target_id
    assert successor["supersedes_plan_task_id"] == source_id
    assert successor["plan_pipeline_config"] == source_pipeline
    assert successor["metadata_"]["revised_from_plan_task_id"] == source_id
    assert "Original proposal" in successor["description"]
    assert "Keep the API backwards compatible" in successor["description"]

    async with session_factory() as db:
        source = await db.get(Task, source_id)
    assert source.status == "superseded"
    assert source.completed_at is not None
    assert source.metadata_["plan_superseded_by_task_id"] == successor["id"]
    assert (
        backend.main.dispatcher._task_queue_generations[source_id]
        >= queue_fence.generation + 2
    )
    assert not await backend.main.dispatcher.enqueue_message(
        source_id,
        "late related Plan report",
        source="monitor:complete",
        queue_admission_fence=queue_fence,
    )

    stale_approval = await client.post(f"/api/tasks/{source_id}/plan/approve")
    stale_rejection = await client.post(f"/api/tasks/{source_id}/plan/reject")
    assert stale_approval.status_code == 409
    assert stale_rejection.status_code == 409
    assert f"Plan #{successor['id']}" in stale_approval.json()["detail"]
    assert f"Plan #{successor['id']}" in stale_rejection.json()["detail"]
    stale_revision = await client.post(
        f"/api/tasks/{source_id}/plan/revise",
        json={"feedback": "Try a third direction"},
    )
    assert stale_revision.status_code == 409
    assert f"Plan #{successor['id']}" in stale_revision.json()["detail"]


@pytest.mark.asyncio
async def test_standalone_plan_revision_preserves_independent_version_history(
    client,
    session_factory,
):
    import backend.main

    source_id = await _legacy_plan_task(
        session_factory,
        title="Standalone v1",
        description="Plan the migration",
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == source_id)
            .values(status="plan_review", plan_content="Migration v1")
        )
        await db.commit()
    queue_fence = await backend.main.dispatcher.snapshot_queue_admission(source_id)

    revised = await client.post(
        f"/api/tasks/{source_id}/plan/revise",
        json={"feedback": "Add a rollback phase"},
    )

    assert revised.status_code == 201, revised.text
    successor = revised.json()
    assert successor["plan_target_task_id"] is None
    assert successor["supersedes_plan_task_id"] == source_id
    assert successor["metadata_"]["revised_from_plan_task_id"] == source_id
    async with session_factory() as db:
        source = await db.get(Task, source_id)
    assert source.status == "superseded"
    assert source.metadata_["plan_superseded_by_task_id"] == successor["id"]
    assert (
        backend.main.dispatcher._task_queue_generations[source_id]
        >= queue_fence.generation + 2
    )
    assert not await backend.main.dispatcher.enqueue_message(
        source_id,
        "late standalone Plan report",
        source="sub-agent:result",
        queue_admission_fence=queue_fence,
    )


@pytest.mark.asyncio
async def test_generic_plan_create_supersedes_worker_side_source_atomically(
    client,
    session_factory,
):
    source_id = await _legacy_plan_task(
        session_factory,
        title="Worker source",
        description="Plan on a Worker",
        status="plan_review",
        plan_content="Worker proposal",
    )

    successor = await client.post(
        "/api/tasks",
        json={
            "title": "Worker successor",
            "description": "Revise on the execution node",
            "target_repo": "/tmp",
            "mode": "plan",
            "supersedes_plan_task_id": source_id,
        },
    )

    assert successor.status_code == 410, successor.text
    async with session_factory() as db:
        source = await db.get(Task, source_id)
    assert source.status == "plan_review"
    assert not source.metadata_ or source.metadata_.get("plan_superseded_by_task_id") is None


@pytest.mark.asyncio
async def test_destroying_worker_blocks_legacy_related_plan_creation(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    async with session_factory() as db:
        worker = Worker(name="legacy-related-plan-worker", status="destroying")
        db.add(worker)
        await db.flush()
        target = await db.get(Task, target_id)
        target.worker_id = worker.id
        await db.commit()

    response = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Must not cross Worker destruction"},
    )

    assert response.status_code == 409, response.text
    assert "not ready for assignment" in response.json()["detail"]
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(Task.id)).where(
                Task.plan_target_task_id == target_id,
            )
        ) == 0


@pytest.mark.asyncio
async def test_destroying_worker_blocks_legacy_standalone_plan_revision(
    client,
    session_factory,
):
    async with session_factory() as db:
        worker = Worker(name="legacy-plan-revision-worker", status="destroying")
        db.add(worker)
        await db.commit()
        worker_id = worker.id
    source_id = await _legacy_plan_task(
        session_factory,
        status="plan_review",
        plan_content="Original Worker Plan",
        worker_id=worker_id,
    )

    response = await client.post(
        f"/api/tasks/{source_id}/plan/revise",
        json={"feedback": "Must not cross Worker destruction"},
    )

    assert response.status_code == 409, response.text
    assert "not ready for assignment" in response.json()["detail"]
    async with session_factory() as db:
        source = await db.get(Task, source_id)
        assert source.status == "plan_review"
        assert await db.scalar(
            select(func.count(Task.id)).where(
                Task.supersedes_plan_task_id == source_id,
            )
        ) == 0


@pytest.mark.asyncio
async def test_destroying_worker_blocks_legacy_plan_execution_materialization(
    client,
    session_factory,
):
    async with session_factory() as db:
        worker = Worker(name="legacy-plan-execution-worker", status="destroying")
        db.add(worker)
        await db.commit()
        worker_id = worker.id
    plan_id = await _legacy_plan_task(
        session_factory,
        status="completed",
        plan_content="Approved Worker Plan",
        plan_approved=True,
        worker_id=worker_id,
    )

    response = await client.post(
        f"/api/tasks/{plan_id}/plan/create-execution-task",
    )

    assert response.status_code == 409, response.text
    assert "not ready for assignment" in response.json()["detail"]
    async with session_factory() as db:
        plan = await db.get(Task, plan_id)
        assert plan.plan_execution_task_id is None


@pytest.mark.asyncio
async def test_plan_approval_and_revision_are_serialized(
    client,
    session_factory,
):
    source_id = await _legacy_plan_task(
        session_factory,
        title="Racing Plan",
        description="Choose one terminal decision",
        status="plan_review",
        plan_content="Race-safe proposal",
    )

    approved, revised = await asyncio.gather(
        client.post(f"/api/tasks/{source_id}/plan/approve"),
        client.post(
            f"/api/tasks/{source_id}/plan/revise",
            json={"feedback": "Create a successor"},
        ),
    )

    assert (approved.status_code == 200) != (revised.status_code == 201)
    async with session_factory() as db:
        source = await db.get(Task, source_id)
        successor_count = await db.scalar(
            select(func.count(Task.id)).where(
                Task.supersedes_plan_task_id == source_id
            )
        )
    if revised.status_code == 201:
        assert source.status == "superseded"
        assert successor_count == 1
    else:
        assert source.status == "completed"
        assert source.plan_approved is True
        assert successor_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "terminal_status", "plan_approved"),
    [
        ("approve", "completed", True),
        ("reject", "cancelled", False),
    ],
)
async def test_plan_terminal_decision_quiesces_late_auxiliary_producer(
    client,
    session_factory,
    action,
    terminal_status,
    plan_approved,
):
    """A producer that committed before G advances cannot revive the Plan."""

    import backend.main

    plan_id = await _legacy_plan_task(
        session_factory,
        title=f"Terminal {action}",
        description="Review this Plan",
        status="plan_review",
        plan_content="Safe terminal proposal",
    )
    async with session_factory() as db:
        monitor = SubAgentSession(
            task_id=plan_id,
            agent_type="monitor",
            source="ccm",
            description="late monitor",
            status="running",
            next_check_at=None,
        )
        already_cancelled = SubAgentSession(
            task_id=plan_id,
            agent_type="sub_agent",
            source="ccm",
            description="retryable cancelled child",
            status="cancelled",
            next_check_at=None,
        )
        db.add_all([monitor, already_cancelled])
        await db.commit()
        await db.refresh(monitor)
        await db.refresh(already_cancelled)
        monitor_id = monitor.id
        sub_agent_id = already_cancelled.id

    dispatcher = backend.main.dispatcher
    fence = await dispatcher.snapshot_queue_admission(plan_id)
    events = []
    late_admissions = []

    async def observe_terminal_commit(label):
        assert plan_id in dispatcher._cancel_durable_queue_tasks
        assert dispatcher._task_queue_generations[plan_id] > fence.generation
        async with session_factory() as db:
            stored = await db.get(Task, plan_id)
            sessions = list(
                (
                    await db.execute(
                        select(SubAgentSession).where(
                            SubAgentSession.id.in_((monitor_id, sub_agent_id))
                        )
                    )
                ).scalars()
            )
        assert stored.status == terminal_status
        assert {session.status for session in sessions} == {"cancelled"}
        events.append(label)

    async def stop_monitor(session_id, *, terminal=False):
        assert session_id == monitor_id
        assert terminal is True
        await observe_terminal_commit("monitor-stopped")
        late_admissions.append(
            await dispatcher.enqueue_message(
                plan_id,
                "late terminal report",
                source="monitor:complete",
                queue_admission_fence=fence,
            )
        )

    async def stop_sub_agent(session_id):
        assert session_id == sub_agent_id
        await observe_terminal_commit("sub-agent-stopped")

    async def observe_broadcast(channel, payload):
        assert channel == "tasks"
        assert payload["task_id"] == plan_id
        assert payload["new_status"] == terminal_status
        assert plan_id in dispatcher._cancel_durable_queue_tasks
        assert (
            dispatcher._task_queue_generations[plan_id]
            >= fence.generation + 2
        )
        events.append("broadcast")

    with (
        patch.object(
            dispatcher,
            "stop_monitor_session_process",
            new=AsyncMock(side_effect=stop_monitor),
        ) as monitor_stop,
        patch.object(
            dispatcher,
            "stop_sub_agent_session_process",
            new=AsyncMock(side_effect=stop_sub_agent),
        ) as sub_agent_stop,
        patch.object(
            backend.main.broadcaster,
            "broadcast",
            new=AsyncMock(side_effect=observe_broadcast),
        ),
    ):
        response = await client.post(f"/api/tasks/{plan_id}/plan/{action}")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == terminal_status
    assert response.json()["plan_approved"] is plan_approved
    assert late_admissions == [False]
    assert events[-1] == "broadcast"
    monitor_stop.assert_awaited_once_with(monitor_id, terminal=True)
    sub_agent_stop.assert_awaited_once_with(sub_agent_id)
    assert plan_id not in dispatcher._cancel_durable_queue_tasks
    assert dispatcher._task_queue_generations[plan_id] >= fence.generation + 2
    queue = dispatcher._task_queues.get(plan_id)
    assert queue is None or queue.empty()


@pytest.mark.asyncio
async def test_plan_terminal_cleanup_failure_still_drains_and_publishes(
    client,
    session_factory,
):
    import backend.main

    plan_id = await _legacy_plan_task(
        session_factory,
        title="Cleanup failure",
        description="Reject safely",
        status="plan_review",
        plan_content="Proposal",
    )
    async with session_factory() as db:
        monitor = SubAgentSession(
            task_id=plan_id,
            agent_type="monitor",
            source="ccm",
            description="unreapable monitor",
            status="running",
            next_check_at=None,
        )
        db.add(monitor)
        await db.commit()
        await db.refresh(monitor)
        monitor_id = monitor.id

    dispatcher = backend.main.dispatcher
    fence = await dispatcher.snapshot_queue_admission(plan_id)
    abort_spy = AsyncMock(wraps=dispatcher.abort_task_queue)
    broadcast = AsyncMock()
    with (
        patch.object(dispatcher, "abort_task_queue", new=abort_spy),
        patch.object(
            dispatcher,
            "stop_monitor_session_process",
            new=AsyncMock(side_effect=RuntimeError("reap failed")),
        ) as stop_monitor,
        patch.object(backend.main.broadcaster, "broadcast", new=broadcast),
    ):
        response = await client.post(f"/api/tasks/{plan_id}/plan/reject")

    assert response.status_code == 409
    assert "terminal state was committed" in response.json()["detail"]
    assert abort_spy.await_count == 2
    stop_monitor.assert_awaited_once_with(monitor_id, terminal=True)
    broadcast.assert_awaited_once_with(
        "tasks",
        {
            "event": "status_change",
            "task_id": plan_id,
            "new_status": "cancelled",
        },
    )
    async with session_factory() as db:
        stored = await db.get(Task, plan_id)
        stored_monitor = await db.get(SubAgentSession, monitor_id)
    assert stored.status == "cancelled"
    assert stored.plan_approved is False
    assert stored_monitor.status == "cancelled"
    assert plan_id not in dispatcher._cancel_durable_queue_tasks
    assert dispatcher._task_queue_generations[plan_id] >= fence.generation + 2
    assert not await dispatcher.enqueue_message(
        plan_id,
        "late report after failed reap",
        source="monitor:complete",
        queue_admission_fence=fence,
    )


@pytest.mark.asyncio
async def test_plan_terminal_caller_cancellation_waits_for_settlement(
    client,
    session_factory,
):
    import backend.main

    plan_id = await _legacy_plan_task(
        session_factory,
        title="Cancelled HTTP request",
        description="Finish terminal settlement",
        status="plan_review",
        plan_content="Proposal",
    )
    async with session_factory() as db:
        monitor = SubAgentSession(
            task_id=plan_id,
            agent_type="monitor",
            source="ccm",
            description="slow stop",
            status="running",
            next_check_at=None,
        )
        db.add(monitor)
        await db.commit()
        await db.refresh(monitor)
        monitor_id = monitor.id

    dispatcher = backend.main.dispatcher
    fence = await dispatcher.snapshot_queue_admission(plan_id)
    stop_started = asyncio.Event()
    release_stop = asyncio.Event()
    broadcast = AsyncMock()
    abort_spy = AsyncMock(wraps=dispatcher.abort_task_queue)

    async def slow_stop(session_id, *, terminal=False):
        assert session_id == monitor_id
        assert terminal is True
        assert plan_id in dispatcher._cancel_durable_queue_tasks
        async with session_factory() as db:
            stored = await db.get(Task, plan_id)
        assert stored.status == "cancelled"
        stop_started.set()
        await release_stop.wait()
        assert plan_id in dispatcher._cancel_durable_queue_tasks

    with (
        patch.object(dispatcher, "abort_task_queue", new=abort_spy),
        patch.object(
            dispatcher,
            "stop_monitor_session_process",
            new=AsyncMock(side_effect=slow_stop),
        ),
        patch.object(backend.main.broadcaster, "broadcast", new=broadcast),
    ):
        request_task = asyncio.create_task(
            client.post(f"/api/tasks/{plan_id}/plan/reject")
        )
        try:
            await asyncio.wait_for(stop_started.wait(), timeout=3)
            request_task.cancel()
            await asyncio.sleep(0)
        finally:
            release_stop.set()
        with pytest.raises(asyncio.CancelledError):
            await request_task

    assert abort_spy.await_count == 2
    broadcast.assert_awaited_once_with(
        "tasks",
        {
            "event": "status_change",
            "task_id": plan_id,
            "new_status": "cancelled",
        },
    )
    async with session_factory() as db:
        stored = await db.get(Task, plan_id)
        stored_monitor = await db.get(SubAgentSession, monitor_id)
    assert stored.status == "cancelled"
    assert stored_monitor.status == "cancelled"
    assert plan_id not in dispatcher._cancel_durable_queue_tasks
    assert dispatcher._task_queue_generations[plan_id] >= fence.generation + 2


@pytest.mark.asyncio
async def test_plan_terminal_decision_rejects_detached_pty_generation(
    client,
    session_factory,
):
    import backend.main

    plan_id = await _legacy_plan_task(
        session_factory,
        title="Detached legacy Plan",
        description="Do not publish over live output",
        status="plan_review",
        plan_content="Proposal",
        pty_background_generation="legacy-detached-generation",
    )
    dispatcher = backend.main.dispatcher
    fence = await dispatcher.snapshot_queue_admission(plan_id)
    broadcast = AsyncMock()
    with patch.object(backend.main.broadcaster, "broadcast", new=broadcast):
        response = await client.post(f"/api/tasks/{plan_id}/plan/approve")

    assert response.status_code == 409
    assert "detached PTY output" in response.json()["detail"]
    broadcast.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(Task, plan_id)
    assert stored.status == "plan_review"
    assert stored.plan_approved is None
    assert stored.pty_background_generation == "legacy-detached-generation"
    assert plan_id not in dispatcher._cancel_durable_queue_tasks
    assert dispatcher._task_queue_generations[plan_id] > fence.generation


@pytest.mark.asyncio
async def test_plan_supersede_rolls_back_if_detached_pty_appears_precommit(
    client,
    session_factory,
):
    from backend.services.plan_tasks import mark_plan_superseded as real_mark

    plan_id = await _legacy_plan_task(
        session_factory,
        title="PTY race",
        description="Revise atomically",
        status="plan_review",
        plan_content="Original proposal",
    )

    async def mark_then_attach_pty(db, source, *, successor_id, **kwargs):
        changed = await real_mark(
            db,
            source,
            successor_id=successor_id,
            **kwargs,
        )
        await db.execute(
            update(Task)
            .where(Task.id == source.id)
            .values(pty_background_generation="raced-detached-generation")
        )
        return changed

    with patch(
        "backend.api.plans.mark_plan_superseded",
        new=mark_then_attach_pty,
    ):
        response = await client.post(
            f"/api/tasks/{plan_id}/plan/revise",
            json={"feedback": "Add a rollback section"},
        )

    assert response.status_code == 409
    assert "before its terminal decision could commit" in response.json()["detail"]
    async with session_factory() as db:
        source = await db.get(Task, plan_id)
        successor_count = await db.scalar(
            select(func.count(Task.id)).where(
                Task.supersedes_plan_task_id == plan_id
            )
        )
    assert source.status == "plan_review"
    assert source.pty_background_generation is None
    assert not source.metadata_ or "plan_superseded_by_task_id" not in source.metadata_
    assert successor_count == 0


@pytest.mark.asyncio
async def test_plan_tasks_never_silently_downgrade_codex_fast(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == target_id)
            .values(
                provider="codex",
                model="gpt-5.6-sol",
                codex_service_tier="priority",
            )
        )
        await db.commit()

    related = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Plan this Fast target safely"},
    )
    assert related.status_code == 201, related.text
    assert related.json()["codex_service_tier"] == "default"

    standalone = await client.post(
        "/api/plans",
        json={
            "input": "Plan it",
            "title": "No hidden Fast downgrade",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )
    assert standalone.status_code == 422
    assert "extra_forbidden" in standalone.text


@pytest.mark.asyncio
async def test_repo_fingerprint_cancel_reaps_git_process_under_anyio(
    monkeypatch,
    tmp_path,
):
    from anyio import CancelScope

    scope_holder: dict[str, CancelScope] = {}
    communicate_started = asyncio.Event()
    killed = asyncio.Event()
    reaped = asyncio.Event()

    class FakeProcess:
        returncode = None
        communicate_owner = None

        async def communicate(self):
            self.communicate_owner = asyncio.current_task()
            communicate_started.set()
            await asyncio.Future()

        async def wait(self):
            await asyncio.sleep(0)
            self.returncode = -9
            reaped.set()
            return self.returncode

        def kill(self):
            killed.set()
            assert self.communicate_owner is not None
            self.communicate_owner.cancel()

    process = FakeProcess()

    async def create_process(*_args, **_kwargs):
        return process

    async def cancel_capture():
        await communicate_started.wait()
        scope_holder["scope"].cancel()

    monkeypatch.setattr(
        "backend.services.plan_tasks.asyncio.create_subprocess_exec",
        create_process,
    )
    canceller = asyncio.create_task(cancel_capture())
    try:
        with CancelScope() as scope:
            scope_holder["scope"] = scope
            with pytest.raises(asyncio.CancelledError):
                await capture_repo_revision(str(tmp_path))
        await canceller
    finally:
        if not canceller.done():
            canceller.cancel()
        await asyncio.gather(canceller, return_exceptions=True)

    assert killed.is_set()
    assert reaped.is_set()
    assert process.returncode == -9


@pytest.mark.asyncio
async def test_repo_fingerprint_detects_repeated_edits_to_same_dirty_path(
    tmp_path,
):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "plan-test@example.invalid"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Plan Test"],
        cwd=tmp_path,
        check=True,
    )
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-qm", "base"],
        cwd=tmp_path,
        check=True,
    )

    clean = await capture_repo_revision(str(tmp_path))
    assert clean["dirty"] is False

    tracked.write_text("edit-one\n", encoding="utf-8")
    first_mtime = tracked.stat().st_mtime_ns
    first = await capture_repo_revision(str(tmp_path))

    # Keep the same porcelain status and byte length; only the actual dirty
    # worktree generation changes.
    tracked.write_text("edit-two\n", encoding="utf-8")
    os.utime(
        tracked,
        ns=(first_mtime + 1_000_000, first_mtime + 1_000_000),
    )
    second = await capture_repo_revision(str(tmp_path))

    assert first["dirty"] is True
    assert second["dirty"] is True
    assert first["head"] == second["head"]
    assert first["dirty_sha256"] != second["dirty_sha256"]


@pytest.mark.asyncio
async def test_related_plan_approval_requires_stale_confirmation_and_no_turn(
    client,
    session_factory,
):
    import backend.main

    target_id, session_id = await _target_with_session(
        client,
        session_factory,
    )
    created = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Design this carefully"},
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(status="plan_review", plan_content="Approved candidate")
        )
        db.add(
            LogEntry(
                instance_id=1,
                task_id=target_id,
                event_type="user_message",
                role="user",
                content="Newer context",
            )
        )
        await db.commit()
        before_logs = await db.scalar(
            select(func.count(LogEntry.id)).where(
                LogEntry.task_id == target_id
            )
        )

    stale = await client.post(f"/api/tasks/{plan_id}/plan/approve")
    assert stale.status_code == 409
    assert "conversation_changed" in stale.json()["detail"]["staleness"]["reasons"]

    with (
        patch.object(
            backend.main.dispatcher,
            "enqueue_message",
            new_callable=AsyncMock,
        ) as enqueue_message,
        patch.object(backend.main.dispatcher, "wake") as wake,
    ):
        approved = await client.post(
            f"/api/tasks/{plan_id}/plan/approve",
            json={"confirm_stale": True},
        )
    assert approved.status_code == 200
    assert approved.json()["status"] == "completed"
    assert approved.json()["plan_approved"] is True
    enqueue_message.assert_not_awaited()
    wake.assert_not_called()

    async with session_factory() as db:
        target = await db.get(Task, target_id)
        after_logs = await db.scalar(
            select(func.count(LogEntry.id)).where(
                LogEntry.task_id == target_id
            )
        )
    assert target.status == "completed"
    assert target.session_id == session_id
    assert after_logs == before_logs


@pytest.mark.asyncio
async def test_approved_plan_is_applied_only_with_selected_user_message(
    client,
    session_factory,
):
    target_id, session_id = await _target_with_session(
        client,
        session_factory,
    )
    created = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Make a plan"},
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(
                status="completed",
                plan_content="1. Change API\n2. Add tests",
                plan_approved=True,
            )
        )
        await db.commit()

    dispatcher = MagicMock()
    dispatcher.snapshot_plan_queue_admission = AsyncMock(return_value=None)
    dispatcher.enqueue_message = AsyncMock()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    with (
        patch("backend.main.dispatcher", dispatcher),
        patch("backend.main.broadcaster", broadcaster),
    ):
        response = await client.post(
            f"/api/tasks/{target_id}/chat",
            json={
                "message": "Please implement it",
                "plan_task_ids": [plan_id],
            },
        )
    assert response.status_code == 200
    assert response.json()["applied_plan_task_ids"] == [plan_id]
    prompt = dispatcher.enqueue_message.call_args.kwargs["prompt"]
    assert f'<approved_plan task_id="{plan_id}">' in prompt
    assert "1. Change API" in prompt
    assert prompt.index("1. Change API") < prompt.index("Please implement it")

    async with session_factory() as db:
        plan = await db.get(Task, plan_id)
        applied_log = await db.get(LogEntry, plan.plan_applied_log_id)
    assert plan.plan_applied_at is not None
    assert plan.plan_applied_to_session_id == session_id
    assert applied_log.content.endswith("Please implement it")
    metadata = json.loads(applied_log.raw_json)
    assert metadata["applied_plans"] == [{
        "id": plan_id,
        "title": f"Plan for #{target_id}: Target",
        "content": "1. Change API\n2. Add tests",
    }]

    # Historical applied messages did not persist the Plan snapshot. They are
    # reconstructed from plan_applied_log_id while the Plan row still exists.
    async with session_factory() as db:
        legacy_log = await db.get(LogEntry, applied_log.id)
        legacy_log.raw_json = json.dumps({"raw_content": "Please implement it"})
        await db.commit()

    history = await client.get(f"/api/tasks/{target_id}/chat/history")
    applied_message = next(
        message
        for message in history.json()
        if message["id"] == applied_log.id
    )
    assert applied_message["applied_plans"] == metadata["applied_plans"]

    second = await client.post(
        f"/api/tasks/{target_id}/chat",
        json={"message": "again", "plan_task_ids": [plan_id]},
    )
    assert second.status_code == 400
    assert "already been applied" in second.json()["detail"]


@pytest.mark.asyncio
async def test_plan_application_is_restored_when_dispatcher_is_shutting_down(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    created = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Make a shutdown-safe plan"},
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(
                status="completed",
                plan_content="A plan that must remain attachable",
                plan_approved=True,
            )
        )
        await db.commit()

    dispatcher = MagicMock()
    dispatcher.snapshot_plan_queue_admission = AsyncMock(return_value=None)
    dispatcher.enqueue_message = AsyncMock(
        side_effect=RuntimeError(
            "Dispatcher is shutting down; message admission is closed"
        )
    )
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    with (
        patch("backend.main.dispatcher", dispatcher),
        patch("backend.main.broadcaster", broadcaster),
    ):
        response = await client.post(
            f"/api/tasks/{target_id}/chat",
            json={
                "message": "Please implement it",
                "plan_task_ids": [plan_id],
            },
        )

    assert response.status_code == 409
    async with session_factory() as db:
        plan = await db.get(Task, plan_id)
    assert plan.plan_applied_at is None
    assert plan.plan_applied_to_session_id is None
    assert plan.plan_applied_log_id is None


@pytest.mark.asyncio
async def test_cancel_active_plan_reaps_legacy_ralph_lifecycle_first(
    client,
    session_factory,
):
    import backend.main

    plan_id = await _legacy_plan_task(
        session_factory,
        title="Cancellable Plan",
        description="Plan safely",
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(status="executing", instance_id=77)
        )
        await db.commit()

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.dispatcher,
            "stop_plan_agent_lifecycle",
            new_callable=AsyncMock,
            return_value=False,
        ) as dispatcher_stop,
        patch.object(
            backend.main.ralph_loop,
            "stop_plan_agent_lifecycle",
            new_callable=AsyncMock,
            return_value=True,
        ) as ralph_stop,
        patch(
            "backend.api.tasks._settle_task_launch_barrier",
            new_callable=AsyncMock,
        ),
    ):
        response = await client.post(f"/api/tasks/{plan_id}/cancel")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "cancelled"
    dispatcher_stop.assert_awaited_once_with(plan_id, 77)
    ralph_stop.assert_awaited_once_with(plan_id)


@pytest.mark.asyncio
async def test_standalone_plan_creates_one_idempotent_execution_task(
    client,
    session_factory,
):
    plan_id = await _legacy_plan_task(
        session_factory,
        title="Standalone",
        description="Plan a migration",
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(status="plan_review", plan_content="Migration plan")
        )
        await db.commit()
    approved = await client.post(f"/api/tasks/{plan_id}/plan/approve")
    assert approved.status_code == 200

    first = await client.post(
        f"/api/tasks/{plan_id}/plan/create-execution-task"
    )
    second = await client.post(
        f"/api/tasks/{plan_id}/plan/create-execution-task"
    )
    assert first.status_code == 201
    assert second.status_code == 201
    first_task = first.json()["execution_task"]
    second_task = second.json()["execution_task"]
    assert first_task["id"] == second_task["id"]
    assert first_task["mode"] == "auto"
    assert "Migration plan" in first_task["description"]


@pytest.mark.asyncio
async def test_migrated_plan_carrier_cannot_create_duplicate_execution_task(
    client,
    session_factory,
):
    carrier_id = await _legacy_plan_task(
        session_factory,
        title="Migrated approved carrier",
        description="Execute exactly once",
        status="pending",
        plan_content="# Approved migrated Plan",
        plan_approved=True,
    )
    async with session_factory() as db:
        plan = Plan(
            title="Canonical migrated Plan",
            initial_request="Execute exactly once",
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Approved migrated Plan",
            human_decision="approved",
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        db.add_all(
            [
                PlanLegacyTaskLink(
                    legacy_task_id=carrier_id,
                    plan_id=plan.id,
                    plan_version_id=version.id,
                ),
                PlanApplication(
                    plan_id=plan.id,
                    plan_version_id=version.id,
                    application_type="execution_task",
                    execution_task_id=carrier_id,
                ),
            ]
        )
        before = await db.scalar(select(func.count(Task.id)))
        await db.commit()

    response = await client.post(
        f"/api/tasks/{carrier_id}/plan/create-execution-task"
    )

    assert response.status_code == 409, response.text
    assert "exact execution application" in response.json()["detail"]
    async with session_factory() as db:
        after = await db.scalar(select(func.count(Task.id)))
        carrier = await db.get(Task, carrier_id)
        assert after == before
        assert carrier.plan_execution_task_id is None
        assert carrier.status == "pending"


@pytest.mark.asyncio
async def test_plan_run_history_returns_steps(
    client,
    session_factory,
):
    plan_id = await _legacy_plan_task(
        session_factory,
        title="Audited Plan",
        description="Plan it",
    )
    async with session_factory() as db:
        run = PlanAgentRun(
            plan_task_id=plan_id,
            status="completed",
            combo_used="codex+codex",
        )
        db.add(run)
        await db.flush()
        db.add(
            PlanAgentStep(
                run_id=run.id,
                step_type="planner",
                round=1,
                provider="codex",
                status="completed",
                output='{"plan":"ok"}',
            )
        )
        await db.commit()

    response = await client.get(f"/api/tasks/{plan_id}/plan/runs")
    assert response.status_code == 200
    assert response.json()[0]["steps"][0]["step_type"] == "planner"
