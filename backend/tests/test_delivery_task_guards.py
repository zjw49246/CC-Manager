"""Public Task APIs cannot bypass DeliveryRun orchestration authority."""

from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select, update

from backend.api.shared_access import SharedChatMessage, shared_chat
from backend.models.delivery import DeliveryCycle, DeliveryRun, DeliveryTurn
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.project import Project
from backend.models.task import Task
from backend.models.task_share import TaskShare
from backend.models.worktree import Worktree
from backend.services.instance_manager import (
    InstanceManager,
    LaunchSupersededError,
    _require_delivery_workspace_launch_boundary,
)
from backend.services.delivery_service import value_hash
from backend.services.pr_review_runtime import (
    PRE_PR_CODE_REVIEW_TAG,
    PR_REVIEW_TAG,
)
from backend.services.task_sharing import share_task


async def _delivery_task(session_factory) -> int:
    async with session_factory() as db:
        task = Task(
            title="Delivery developer",
            description="controlled prompt",
            status="completed",
            mode="delivery_loop",
            delivery_run_id=77,
            delivery_role="developer",
            provider="codex",
            model="gpt-5.6-sol",
            target_repo="/repo",
            session_id="delivery-session",
            last_cwd="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        return task.id


async def _pre_pr_review_task(
    session_factory,
    *,
    tags: list[str] | None = None,
    metadata_marker: bool = True,
) -> int:
    async with session_factory() as db:
        task = Task(
            title="Pre-PR reviewer",
            description="immutable structured review prompt",
            status="completed",
            mode="auto",
            provider="codex",
            model="gpt-5.6-sol",
            target_repo="",
            session_id="review-session",
            last_cwd="/review-sandbox",
            tags=tags,
            metadata_=(
                {
                    "code_review_run_id": 11,
                    "capability_invocation_id": 12,
                    "capability_execution_id": 13,
                }
                if metadata_marker
                else {}
            ),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        return task.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "json_body"),
    [
        ("PUT", "/api/tasks/{id}", {"title": "bypass"}),
        ("DELETE", "/api/tasks/{id}", None),
        ("POST", "/api/tasks/{id}/cancel", None),
        ("POST", "/api/tasks/{id}/retry", None),
        ("POST", "/api/tasks/{id}/stop-session", None),
        ("POST", "/api/tasks/{id}/archive", None),
        ("POST", "/api/tasks/{id}/chat", {"message": "bypass"}),
        ("GET", "/api/tasks/{id}/fork-anchors", None),
        ("GET", "/api/tasks/{id}/inject-capabilities", None),
        ("POST", "/api/tasks/{id}/inject", {"message": "bypass"}),
    ],
)
async def test_delivery_task_lifecycle_mutations_are_rejected(
    client,
    session_factory,
    method,
    path,
    json_body,
):
    task_id = await _delivery_task(session_factory)

    response = await client.request(
        method,
        path.format(id=task_id),
        json=json_body,
    )

    assert response.status_code == 409, response.text
    assert "Delivery" in response.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["star", "read", "unread"])
async def test_delivery_task_presentation_controls_remain_available(
    client,
    session_factory,
    path,
):
    task_id = await _delivery_task(session_factory)

    response = await client.post(f"/api/tasks/{task_id}/{path}")

    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_delivery_task_rejects_shared_access_chat_before_persisting_log(
    session_factory,
):
    task_id = await _delivery_task(session_factory)
    async with session_factory() as db:
        db.add(
            TaskShare(
                task_id=task_id,
                shared_to_open_id="remote-user",
                shared_to_name="Remote User",
                shared_to_ccm_url="https://remote.example",
                share_token="delivery-share-token",
                status="active",
            )
        )
        await db.commit()

    async with session_factory() as db:
        with pytest.raises(HTTPException) as blocked:
            await shared_chat(
                task_id,
                SharedChatMessage(message="bypass", sender_name="Remote User"),
                token="delivery-share-token",
                db=db,
            )
        assert blocked.value.status_code == 409
        count = await db.scalar(
            select(func.count(LogEntry.id)).where(LogEntry.task_id == task_id)
        )
        assert count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tags", "path", "json_body"),
    [
        ([PRE_PR_CODE_REVIEW_TAG], "/api/tasks/{id}/chat", {"message": "change verdict"}),
        ([PRE_PR_CODE_REVIEW_TAG], "/api/tasks/{id}/inject", {"message": "change verdict"}),
        (None, "/api/tasks/{id}/chat", {"message": "metadata-only bypass"}),
        (None, "/api/tasks/{id}/inject", {"message": "metadata-only bypass"}),
    ],
)
async def test_pre_pr_review_task_rejects_chat_and_injection_before_persisting_log(
    client,
    session_factory,
    tags,
    path,
    json_body,
):
    task_id = await _pre_pr_review_task(session_factory, tags=tags)

    response = await client.post(path.format(id=task_id), json=json_body)

    assert response.status_code == 409, response.text
    assert "Pre-PR Code Review Capability" in response.json()["detail"]
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count(LogEntry.id)).where(LogEntry.task_id == task_id)
        )
        assert count == 0


@pytest.mark.asyncio
async def test_pre_pr_review_task_rejects_shared_chat_before_persisting_log(
    session_factory,
):
    task_id = await _pre_pr_review_task(
        session_factory,
        tags=[PRE_PR_CODE_REVIEW_TAG],
    )
    async with session_factory() as db:
        db.add(
            TaskShare(
                task_id=task_id,
                shared_to_open_id="remote-reviewer",
                shared_to_name="Remote Reviewer",
                shared_to_ccm_url="https://remote.example",
                share_token="pre-pr-review-share-token",
                status="active",
            )
        )
        await db.commit()

    async with session_factory() as db:
        with pytest.raises(HTTPException) as blocked:
            await shared_chat(
                task_id,
                SharedChatMessage(
                    message="change verdict",
                    sender_name="Remote Reviewer",
                ),
                token="pre-pr-review-share-token",
                db=db,
            )
        assert blocked.value.status_code == 409
        assert "Pre-PR Code Review Capability" in blocked.value.detail
        count = await db.scalar(
            select(func.count(LogEntry.id)).where(LogEntry.task_id == task_id)
        )
        assert count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tags", "metadata_marker"),
    [
        ([PRE_PR_CODE_REVIEW_TAG], False),
        (None, True),
    ],
)
async def test_pre_pr_review_task_rejects_writable_remote_share(
    session_factory,
    tags,
    metadata_marker,
):
    task_id = await _pre_pr_review_task(
        session_factory,
        tags=tags,
        metadata_marker=metadata_marker,
    )

    async with session_factory() as db:
        with pytest.raises(ValueError, match="Automated PR workflow"):
            await share_task(
                db,
                task_id,
                [
                    {
                        "open_id": "remote-reviewer",
                        "name": "Remote Reviewer",
                        "ccm_url": "https://remote.example",
                    }
                ],
            )
        count = await db.scalar(
            select(func.count(TaskShare.id)).where(TaskShare.task_id == task_id)
        )
        assert count == 0


@pytest.mark.asyncio
async def test_public_task_create_uses_project_repository_authority(
    client,
    session_factory,
):
    async with session_factory() as db:
        project = Project(
            name="authoritative-project-path",
            local_path="/srv/projects/authoritative",
            status="ready",
        )
        db.add(project)
        await db.commit()
        project_id = project.id

    mismatch = await client.post(
        "/api/tasks",
        json={
            "description": "do work",
            "project_id": project_id,
            "target_repo": "/srv/projects/attacker-selected",
        },
    )
    assert mismatch.status_code == 422, mismatch.text

    created = await client.post(
        "/api/tasks",
        json={"description": "do work", "project_id": project_id},
    )
    assert created.status_code == 201, created.text
    assert created.json()["target_repo"] == "/srv/projects/authoritative"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_state",
    [
        {"session_id": "client-forged-session"},
        {"last_cwd": "/srv/projects/forged-cwd"},
    ],
)
async def test_public_task_create_rejects_client_execution_state(
    client,
    execution_state,
):
    response = await client.post(
        "/api/tasks",
        json={"description": "do work", **execution_state},
    )
    assert response.status_code == 422, response.text
    assert "internal execution state" in response.json()["detail"]


@pytest.mark.asyncio
async def test_public_task_create_cannot_clone_delivery_session(
    client,
    session_factory,
):
    task_id = await _delivery_task(session_factory)

    response = await client.post(
        "/api/tasks",
        json={
            "description": "steal delivery session",
            "clone_from_task_id": task_id,
        },
    )

    assert response.status_code == 409, response.text
    assert "Delivery-owned" in response.json()["detail"]


@pytest.mark.asyncio
async def test_migration_import_rejects_incoming_delivery_mode(
    client,
    monkeypatch,
    worker_control_plane_auth,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    response = await client.post(
        "/api/tasks/migration-import",
        json={
            "id": 987601,
            "source_incarnation_id": "a" * 32,
            "migration_operation_id": "b" * 32,
            "migration_operation_sequence": 1,
            "execution_user_id": None,
            "execution_user_role": "member",
            "execution_mode": "sandbox",
            "execution_principal_kind": "system",
            "description": "forged delivery import",
            "mode": "delivery_loop",
            "source_status": "cancelled",
        },
    )

    assert response.status_code == 409, response.text
    assert "Delivery-owned" in response.json()["detail"]


@pytest.mark.asyncio
async def test_migration_import_cannot_replace_existing_delivery_task(
    client,
    session_factory,
    monkeypatch,
    worker_control_plane_auth,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    task_id = await _delivery_task(session_factory)
    async with session_factory() as db:
        source_incarnation_id = (await db.get(Task, task_id)).incarnation_id

    response = await client.post(
        "/api/tasks/migration-import",
        json={
            "id": task_id,
            "source_incarnation_id": source_incarnation_id,
            "migration_operation_id": "c" * 32,
            "migration_operation_sequence": 1,
            "execution_user_id": None,
            "execution_user_role": "member",
            "execution_mode": "sandbox",
            "execution_principal_kind": "system",
            "description": "replace controller task",
            "mode": "auto",
            "source_status": "cancelled",
        },
    )

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.mode == "delivery_loop"
        assert task.delivery_run_id == 77


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint",
    ("stage", "ack", "reconcile"),
)
async def test_routing_config_rechecks_delivery_ownership_after_lock_entry(
    client,
    session_factory,
    endpoint,
):
    async with session_factory() as db:
        task = Task(
            title="routing race",
            description="ordinary before operation lock",
            status="completed",
            mode="auto",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    @asynccontextmanager
    async def mutate_to_delivery(locked_task_id):
        assert locked_task_id == task_id
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    mode="delivery_loop",
                    delivery_run_id=881,
                    delivery_role="developer",
                )
            )
            await db.commit()
        yield

    with patch(
        "backend.api.tasks.get_task_operation_lock",
        side_effect=mutate_to_delivery,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/routing-config/{endpoint}",
            json={
                "op_id": "delivery-race",
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "codex_service_tier": "default",
            },
        )

    assert response.status_code == 409, response.text
    assert "Delivery-owned" in response.json()["detail"]


async def _workflow_instance(
    session_factory,
    *,
    kind: str,
) -> tuple[int, int, datetime]:
    started_at = datetime(2026, 8, 5, 1, 2, 3)
    async with session_factory() as db:
        task_kwargs = {
            "title": kind,
            "description": "workflow-owned",
            "status": "executing",
            "provider": "codex",
        }
        if kind == "delivery":
            task_kwargs.update(
                mode="delivery_loop",
                delivery_run_id=921,
                delivery_role="developer",
            )
        elif kind == "pre-review":
            task_kwargs.update(
                tags=[PRE_PR_CODE_REVIEW_TAG],
                metadata_={
                    "code_review_run_id": 1,
                    "capability_invocation_id": 2,
                    "capability_execution_id": 3,
                },
            )
        else:
            task_kwargs.update(tags=[PR_REVIEW_TAG])
        task = Task(**task_kwargs)
        db.add(task)
        await db.flush()
        instance = Instance(
            name=f"{kind}-instance",
            status="running",
            pid=81234,
            current_task_id=task.id,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        return instance.id, task.id, started_at


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("delivery", "pre-review", "pr-review"))
async def test_instance_stop_api_rejects_workflow_owned_effect_task(
    client,
    session_factory,
    kind,
):
    instance_id, task_id, started_at = await _workflow_instance(
        session_factory,
        kind=kind,
    )
    instance_manager = MagicMock(stop=AsyncMock(return_value=True))
    ralph_loop = MagicMock(
        is_running=MagicMock(return_value=False),
        stop=AsyncMock(return_value=True),
    )
    with (
        patch("backend.main.instance_manager", instance_manager),
        patch("backend.main.ralph_loop", ralph_loop),
    ):
        response = await client.post(
            f"/api/instances/{instance_id}/stop",
            json={
                "expected_task_id": task_id,
                "expected_task_turn_generation": 0,
                "expected_pid": 81234,
                "expected_started_at": started_at.isoformat(),
            },
        )

    assert response.status_code == 409, response.text
    ralph_loop.stop.assert_not_awaited()
    instance_manager.stop.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("delivery", "pre-review", "pr-review"))
async def test_instance_manager_stop_fails_closed_before_signal(
    session_factory,
    kind,
):
    instance_id, task_id, started_at = await _workflow_instance(
        session_factory,
        kind=kind,
    )
    process = MagicMock(returncode=None, pid=81234)
    process.send_signal = MagicMock()
    manager = InstanceManager(
        session_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    manager.processes[instance_id] = process

    stopped = await manager.stop(
        instance_id,
        expected_task_id=task_id,
        expected_pid=81234,
        expected_started_at=started_at,
    )

    assert stopped is False
    process.send_signal.assert_not_called()
    assert manager.processes[instance_id] is process


@pytest.mark.asyncio
async def test_internal_recovery_can_explicitly_stop_workflow_effect_task(
    session_factory,
):
    instance_id, task_id, started_at = await _workflow_instance(
        session_factory,
        kind="pre-review",
    )
    process = MagicMock(returncode=None, pid=81234)
    process.send_signal = MagicMock()

    async def wait_process():
        process.returncode = 130
        return 130

    process.wait = wait_process
    manager = InstanceManager(
        session_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    manager.processes[instance_id] = process

    stopped = await manager.stop(
        instance_id,
        expected_task_id=task_id,
        expected_pid=81234,
        expected_started_at=started_at,
        allow_delivery_effect_stop=True,
    )

    assert stopped is True
    process.send_signal.assert_called_once()


@pytest.mark.asyncio
async def test_launch_boundary_rejects_ordinary_task_in_reserved_delivery_path(
    session_factory,
    tmp_path,
):
    delivery_path = (
        tmp_path / "repo" / ".claude-manager" / "worktrees" / "delivery-44"
    )
    async with session_factory() as db:
        task = Task(
            title="ordinary intruder",
            description="must not launch",
            target_repo=str(delivery_path),
            mode="auto",
        )
        db.add(task)
        await db.flush()
        with pytest.raises(LaunchSupersededError, match="exact active owner"):
            await _require_delivery_workspace_launch_boundary(
                db,
                task,
                cwd=str(delivery_path),
            )


@pytest.mark.asyncio
async def test_launch_boundary_rejects_registered_nonstandard_delivery_path(
    session_factory,
    tmp_path,
):
    delivery_path = tmp_path / "controller-owned-workspace"
    async with session_factory() as db:
        owner = Task(title="owner", description="controller owner")
        intruder = Task(
            title="ordinary intruder",
            description="must not launch",
            target_repo=str(delivery_path),
            mode="auto",
        )
        db.add_all([owner, intruder])
        await db.flush()
        db.add(
            Worktree(
                repo_path=str(tmp_path / "repo"),
                worktree_path=str(delivery_path),
                branch_name="ccm/delivery/nonstandard",
                task_id=owner.id,
                delivery_run_id=991,
                cleanup_status="retained",
                status="active",
            )
        )
        await db.flush()

        with pytest.raises(LaunchSupersededError, match="exact active owner"):
            await _require_delivery_workspace_launch_boundary(
                db,
                intruder,
                cwd=str(delivery_path / "src"),
            )


@pytest.mark.asyncio
async def test_launch_boundary_ignores_ordinary_legacy_worktree_row(
    session_factory,
    tmp_path,
):
    legacy_path = (
        tmp_path / "repo" / ".claude-manager" / "worktrees" / "task-44"
    )
    async with session_factory() as db:
        task = Task(
            title="ordinary worktree task",
            description="run normally",
            target_repo=str(legacy_path),
            last_cwd=str(legacy_path),
            mode="auto",
        )
        db.add(task)
        await db.flush()
        db.add(
            Worktree(
                repo_path=str(tmp_path / "repo"),
                worktree_path=str(legacy_path),
                branch_name="ccm/task/44",
                base_branch="main",
                task_id=task.id,
                delivery_run_id=None,
                cleanup_status="retained",
                status="active",
            )
        )
        await db.flush()

        assert await _require_delivery_workspace_launch_boundary(
            db,
            task,
            cwd=str(legacy_path / "src"),
        ) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "model", "other_model"),
    (
        ("codex", "gpt-5.6-sol", "gpt-5.6-terra"),
        ("claude", "claude-opus-4-6", "claude-sonnet-4-6"),
    ),
)
async def test_launch_boundary_allows_only_exact_delivery_binding(
    session_factory,
    tmp_path,
    provider,
    model,
    other_model,
):
    repo_path = tmp_path / "repo"
    workspace_path = repo_path / ".claude-manager" / "worktrees" / "delivery-1"
    async with session_factory() as db:
        project = Project(
            name="launch-boundary-project",
            local_path=str(repo_path),
            status="ready",
        )
        db.add(project)
        await db.flush()
        policy = {
            "schema_version": 1,
            "provider": provider,
            "model": model,
            "codex_service_tier": "default",
            "effort_level": "high",
        }
        run = DeliveryRun(
            created_by=None,
            admission_scope="test:launch-boundary",
            idempotency_key="launch-boundary",
            request_hash="q" * 64,
            project_id=project.id,
            title="delivery",
            requirements="implement",
            requirements_hash="r" * 64,
            policy_snapshot=policy,
            policy_hash=value_hash(policy),
            base_branch="main",
            delivery_branch="ccm/delivery/1-delivery",
            workspace_path=str(workspace_path),
            phase="coding",
            activity="running",
            state_version=1,
            turn_count=1,
            max_cycles=10,
            max_no_progress=3,
        )
        db.add(run)
        await db.flush()
        task = Task(
            title="exact developer",
            description="code",
            status="executing",
            project_id=project.id,
            target_repo=str(workspace_path),
            last_cwd=str(workspace_path),
            mode="delivery_loop",
            delivery_run_id=run.id,
            delivery_role="developer",
            provider=provider,
            model=model,
            codex_service_tier="default",
            effort_level="high",
        )
        db.add(task)
        await db.flush()
        cycle = DeliveryCycle(
            run_id=run.id,
            cycle_number=1,
            active_run_id=run.id,
            status="coding",
            trigger_kind="initial_request",
            trigger_payload={},
            trigger_hash="t" * 64,
        )
        db.add(cycle)
        await db.flush()
        turn = DeliveryTurn(
            run_id=run.id,
            cycle_id=cycle.id,
            generation=1,
            correlation_id=f"delivery:{run.id}:turn:1",
            active_run_id=run.id,
            purpose="code",
            trigger_kind="plan_ready",
            trigger_payload={},
            prompt_payload={},
            prompt_hash="u" * 64,
            status="queued",
            task_id=task.id,
            task_retry_count=task.retry_count,
        )
        db.add(turn)
        worktree = Worktree(
            repo_path=str(repo_path),
            worktree_path=str(workspace_path),
            branch_name=run.delivery_branch,
            base_branch="main",
            task_id=task.id,
            delivery_run_id=run.id,
            cleanup_status="retained",
            status="active",
        )
        db.add(worktree)
        await db.flush()
        run.developer_task_id = task.id
        run.worktree_id = worktree.id
        run.current_cycle_id = cycle.id
        await db.flush()

        await _require_delivery_workspace_launch_boundary(
            db,
            task,
            cwd=str(workspace_path),
        )

        worktree.task_id = task.id + 1
        await db.flush()
        with pytest.raises(LaunchSupersededError, match="exact active owner"):
            await _require_delivery_workspace_launch_boundary(
                db,
                task,
                cwd=str(workspace_path),
            )

        worktree.task_id = task.id
        turn.purpose = "review"
        await db.flush()
        with pytest.raises(LaunchSupersededError, match="exact active owner"):
            await _require_delivery_workspace_launch_boundary(
                db,
                task,
                cwd=str(workspace_path),
            )

        turn.purpose = "code"
        run.turn_count = 2
        await db.flush()
        with pytest.raises(LaunchSupersededError, match="exact active owner"):
            await _require_delivery_workspace_launch_boundary(
                db,
                task,
                cwd=str(workspace_path),
            )

        run.turn_count = 1
        cycle.status = "pre_review"
        await db.flush()
        with pytest.raises(LaunchSupersededError, match="exact active owner"):
            await _require_delivery_workspace_launch_boundary(
                db,
                task,
                cwd=str(workspace_path),
            )

        cycle.status = "coding"
        task.model = other_model
        await db.flush()
        with pytest.raises(LaunchSupersededError, match="exact active owner"):
            await _require_delivery_workspace_launch_boundary(
                db,
                task,
                cwd=str(workspace_path),
            )
