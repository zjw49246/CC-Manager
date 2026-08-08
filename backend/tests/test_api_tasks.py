"""Tests for Task API endpoints."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession


# === Existing tests ===


@pytest.mark.asyncio
async def test_create_task(client):
    resp = await client.post("/api/tasks", json={
        "title": "Test",
        "description": "Do something",
        "target_repo": "/tmp/repo",
        "priority": 1,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["title"] == "Test"
    assert data["status"] == "pending"
    assert data["priority"] == 1


@pytest.mark.asyncio
async def test_create_task_wakes_dispatcher_after_commit(client):
    """New work should not wait for the dispatcher's 2-second safety poll."""
    from backend.main import dispatcher

    with patch.object(dispatcher, "wake") as wake:
        resp = await client.post("/api/tasks", json={
            "title": "Wake now",
            "description": "Dispatch immediately",
        })

    assert resp.status_code == 201
    wake.assert_called_once_with()


@pytest.mark.asyncio
async def test_update_task_can_clear_nullable_runtime_configuration(
    client,
    session_factory,
):
    from backend.models.task import Task

    response = await client.post("/api/tasks", json={
        "title": "Clear nullable config",
        "description": "d",
        "thinking_budget": 123,
        "timeout_hours": 4,
        "system_prompt_mode": "append",
    })
    task_id = response.json()["id"]

    response = await client.put(f"/api/tasks/{task_id}", json={
        "thinking_budget": None,
        "timeout_hours": None,
        "system_prompt_mode": "off",
    })

    assert response.status_code == 200
    assert response.json()["thinking_budget"] is None
    assert response.json()["timeout_hours"] is None
    assert response.json()["system_prompt_mode"] is None
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.thinking_budget is None
        assert task.timeout_hours is None
        assert task.system_prompt_mode is None


@pytest.mark.asyncio
async def test_explicit_skill_save_clears_temporary_generation_marker(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.services.task_skill_overrides import (
        TEMP_SKILLS_GENERATION_KEY,
    )

    response = await client.post("/api/tasks", json={
        "title": "Save temporary-looking skills",
        "description": "d",
        "provider": "claude",
        "enabled_skills": {"monitor": True},
    })
    task_id = response.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.metadata_ = {
            TEMP_SKILLS_GENERATION_KEY: "temporary-generation",
            "keep": "metadata",
        }
        await db.commit()

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"enabled_skills": {"monitor": True}},
    )

    assert response.status_code == 200
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.enabled_skills == {"monitor": True}
        assert task.metadata_ == {"keep": "metadata"}


@pytest.mark.asyncio
async def test_internal_skill_update_endpoint_accepts_only_enabled_skills(client):
    created = await client.post("/api/tasks", json={
        "title": "Scoped skill update",
        "description": "d",
        "provider": "claude",
    })
    task_id = created.json()["id"]

    updated = await client.put(
        f"/api/tasks/{task_id}/internal/enabled-skills",
        json={"enabled_skills": {"monitor": True}},
    )
    rejected = await client.put(
        f"/api/tasks/{task_id}/internal/enabled-skills",
        json={
            "enabled_skills": {},
            "title": "must not be writable through the MCP credential",
        },
    )

    assert updated.status_code == 200
    assert updated.json()["enabled_skills"] == {"monitor": True}
    assert rejected.status_code == 422


@pytest.mark.asyncio
async def test_local_codex_accepts_monitor_when_main_mcp_is_enabled(
    client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    response = await client.post("/api/tasks", json={
        "title": "Local Codex Monitor",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"monitor": True},
    })

    assert response.status_code == 201, response.text
    assert response.json()["enabled_skills"]["monitor"] is True


@pytest.mark.asyncio
async def test_local_codex_accepts_monitor_command_without_persisting_skill(
    client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    response = await client.post("/api/tasks", json={
        "title": "Local Codex Monitor command",
        "description": "$monitor watch the build",
        "provider": "codex",
        "enabled_skills": {},
    })

    assert response.status_code == 201, response.text
    assert response.json()["enabled_skills"].get("monitor") is not True


@pytest.mark.asyncio
async def test_local_codex_accepts_monitor_command_added_by_task_update(
    client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    created = await client.post("/api/tasks", json={
        "title": "Updated local Codex Monitor command",
        "description": "ordinary task",
        "provider": "codex",
    })

    response = await client.put(
        f"/api/tasks/{created.json()['id']}",
        json={"description": "$monitor watch the build"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["description"] == "$monitor watch the build"


@pytest.mark.asyncio
async def test_codex_worker_create_rejects_monitor_before_task_write(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.task import Task
    from backend.models.worker import Worker

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with session_factory() as db:
        db.add(Worker(
            id=40,
            name="monitor-closed-worker",
            status="ready",
            private_ip="10.0.0.40",
            auth_token="token",
        ))
        await db.commit()

    response = await client.post("/api/tasks", json={
        "title": "No Worker Codex Monitor",
        "description": "d",
        "provider": "codex",
        "worker_id": 40,
        "enabled_skills": {"monitor": True},
    })

    assert response.status_code == 400
    assert "does not support Skills: monitor" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_codex_main_mcp_kill_switch_keeps_only_sub_agent(client):
    rejected = await client.post("/api/tasks", json={
        "title": "No ordinary skills",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"code-review": True},
    })
    allowed = await client.post("/api/tasks", json={
        "title": "Sub-Agent remains independent",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"sub-agent": True},
    })

    assert rejected.status_code == 400
    assert "main-task MCP is disabled" in rejected.text
    assert allowed.status_code == 201


@pytest.mark.asyncio
async def test_selected_user_skills_are_validated_and_deduplicated(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.user_skill import UserSkill

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with session_factory() as db:
        skill = UserSkill(
            name="API selected skill",
            description="selected",
            content="body",
        )
        db.add(skill)
        await db.commit()
        await db.refresh(skill)

    created = await client.post("/api/tasks", json={
        "title": "Selected User Skill",
        "description": "d",
        "provider": "codex",
        "selected_user_skills": [skill.id, skill.id],
    })
    missing = await client.post("/api/tasks", json={
        "title": "Missing User Skill",
        "description": "d",
        "provider": "codex",
        "selected_user_skills": [skill.id + 1000],
    })

    assert created.status_code == 201, created.text
    assert created.json()["selected_user_skills"] == [skill.id]
    assert missing.status_code == 400
    assert "do not exist" in missing.text


@pytest.mark.asyncio
async def test_worker_snapshot_ids_never_fall_back_to_local_user_skills(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.user_skill import UserSkill

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with session_factory() as db:
        local_skill = UserSkill(
            name="Worker-local collision",
            description="must not authorize a Manager selection",
            content="local body",
        )
        db.add(local_skill)
        await db.commit()
        await db.refresh(local_skill)

    response = await client.post("/api/tasks", json={
        "title": "Authoritative empty snapshot",
        "description": "d",
        "provider": "codex",
        "selected_user_skills": [local_skill.id],
        "user_skill_snapshots": [],
    })

    assert response.status_code == 400
    assert "do not exist" in response.text


@pytest.mark.asyncio
async def test_unrelated_update_allows_legacy_codex_monitor_configuration(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="Legacy Codex Monitor",
            description="d",
            provider="codex",
            model="gpt-5.6-sol",
            enabled_skills={"monitor": True},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"model": "gpt-5.6-terra"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["model"] == "gpt-5.6-terra"


@pytest.mark.asyncio
async def test_local_provider_switch_accepts_inherited_monitor(
    client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    created = await client.post("/api/tasks", json={
        "title": "Claude Monitor",
        "description": "d",
        "provider": "claude",
        "enabled_skills": {"monitor": True},
    })

    response = await client.put(
        f"/api/tasks/{created.json()['id']}",
        json={"provider": "codex"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["provider"] == "codex"
    assert response.json()["enabled_skills"]["monitor"] is True


@pytest.mark.asyncio
async def test_invalid_skill_update_is_rejected_before_worker_migration(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.task import Task
    from backend.models.worker import Worker

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)

    async with session_factory() as db:
        worker = Worker(
            id=41,
            name="monitor-destination",
            status="ready",
            private_ip="10.0.0.41",
            auth_token="token",
        )
        task = Task(
            title="Do not migrate invalid configuration",
            description="d",
            provider="codex",
            worker_id=None,
            enabled_skills={"monitor": True},
        )
        db.add_all([worker, task])
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    migrator = MagicMock()
    migrator.migrate = AsyncMock()
    with patch("backend.main.task_migrator", migrator):
        response = await client.put(f"/api/tasks/{task_id}", json={
            "worker_id": 41,
        })

    assert response.status_code == 400
    assert "does not support Skills: monitor" in response.text
    migrator.migrate.assert_not_awaited()
    async with session_factory() as db:
        persisted = await db.get(Task, task_id)
    assert persisted.worker_id is None
    assert persisted.provider == "codex"
    assert persisted.enabled_skills == {"monitor": True}


@pytest.mark.asyncio
async def test_valid_skill_update_is_coordinated_with_worker_migration(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.models.worker import Worker

    async with session_factory() as db:
        worker = Worker(
            id=42,
            name="skill-destination",
            status="ready",
            private_ip="10.0.0.42",
            auth_token="token",
        )
        task = Task(
            title="Migrate final configuration",
            description="d",
            provider="claude",
            enabled_skills={},
        )
        db.add_all([worker, task])
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    captured = {}

    async def migrate(task_id, target, *, task_updates):
        captured.update(
            task_id=task_id,
            target=target,
            task_updates=task_updates,
        )
        async with session_factory() as db:
            persisted = await db.get(Task, task_id)
            for field, value in task_updates.items():
                setattr(persisted, field, value)
            persisted.worker_id = target
            await db.commit()

    migrator = MagicMock()
    migrator.migrate = AsyncMock(side_effect=migrate)
    with patch("backend.main.task_migrator", migrator):
        response = await client.put(f"/api/tasks/{task_id}", json={
            "worker_id": 42,
            "provider": "codex",
            "enabled_skills": {"sub-agent": True},
        })

    assert response.status_code == 200, response.text
    assert response.json()["worker_id"] == 42
    assert response.json()["provider"] == "codex"
    assert response.json()["enabled_skills"]["sub-agent"] is True
    assert captured == {
        "task_id": task_id,
        "target": 42,
        "task_updates": {
            "provider": "codex",
            "enabled_skills": {"sub-agent": True},
            "metadata_": {},
        },
    }


@pytest.mark.asyncio
async def test_migration_import_preserves_inert_status_without_waking_dispatcher(
    client, session_factory,
):
    """Worker imports preserve plan review without a pending dispatch window."""
    from backend.main import dispatcher
    from backend.models.task import Task

    with patch.object(dispatcher, "wake") as wake:
        resp = await client.post("/api/tasks/migration-import", json={
            "id": 7001,
            "title": "Migrated",
            "description": "Resume an existing session",
            "provider": "claude",
            "session_id": "session-1",
            "last_cwd": "/workspace/repo",
            "retry_count": 2,
            "source_status": "plan_review",
            "mode": "plan",
            "selected_user_skills": [81],
            "user_skill_snapshots": [{
                "id": 81,
                "name": "Migrated skill",
                "description": "Manager copy",
                "content": "full body",
            }],
        })

    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "plan_review"
    wake.assert_not_called()
    async with session_factory() as db:
        task = await db.get(Task, 7001)
    assert task.status == "plan_review"
    assert task.session_id == "session-1"
    assert task.retry_count == 2
    assert task.selected_user_skills == [81]
    assert task.metadata_["ccm_user_skill_snapshots"] == [{
        "id": 81,
        "name": "Migrated skill",
        "description": "Manager copy",
        "content": "full body",
    }]
    assert task.metadata_["ccm_worker_managed_task"] is True


@pytest.mark.asyncio
async def test_migration_import_rejects_codex_monitor_worker_copy(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.task import Task

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    response = await client.post("/api/tasks/migration-import", json={
        "id": 7008,
        "title": "Worker Monitor remains closed",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"monitor": True},
        "source_status": "completed",
        "user_skill_snapshots": [],
    })

    assert response.status_code == 400
    assert "does not support Skills: monitor" in response.text
    async with session_factory() as db:
        assert await db.get(Task, 7008) is None


@pytest.mark.asyncio
async def test_migration_import_refuses_active_existing_task(client, session_factory):
    """An import must never cancel a same-ID task which is already running."""
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            id=7002,
            title="Already running",
            description="d",
            status="in_progress",
        )
        db.add(task)
        await db.commit()

    resp = await client.post("/api/tasks/migration-import", json={
        "id": 7002,
        "title": "Migrated",
        "description": "d",
    })

    assert resp.status_code == 409
    async with session_factory() as db:
        task = await db.get(Task, 7002)
    assert task.status == "in_progress"
    assert task.title == "Already running"


@pytest.mark.asyncio
async def test_migration_import_existing_row_uses_full_generation_cas(
    client,
    session_factory,
    monkeypatch,
):
    """A same-status retry ABA cannot be overwritten by an old import."""

    import backend.api.tasks as task_api
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            id=7003,
            title="Current generation",
            description="d",
            status="cancelled",
            retry_count=4,
        )
        db.add(task)
        await db.commit()

    real_fence = task_api._task_generation_fence

    def replace_generation_after_snapshot(task_id, observed):
        predicates = real_fence(task_id, observed)
        # Autoflush applies this same-status generation change immediately
        # before the import's guarded UPDATE. The old retry_count already
        # captured in ``predicates`` must make that UPDATE miss.
        observed.retry_count += 1
        return predicates

    monkeypatch.setattr(
        task_api,
        "_task_generation_fence",
        replace_generation_after_snapshot,
    )

    response = await client.post("/api/tasks/migration-import", json={
        "id": 7003,
        "title": "Stale imported copy",
        "description": "d",
        "retry_count": 4,
    })

    assert response.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, 7003)
    assert current.title == "Current generation"
    assert current.status == "cancelled"
    assert current.retry_count == 4


@pytest.mark.asyncio
async def test_create_task_with_project_id(client, session_factory):
    from backend.models.project import Project

    async with session_factory() as db:
        project = Project(
            name="task-project",
            local_path="/tmp/task-project",
            status="ready",
        )
        db.add(project)
        await db.commit()
        await db.refresh(project)
        project_id = project.id

    resp = await client.post("/api/tasks", json={
        "title": "Test",
        "description": "Do something",
        "project_id": project_id,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["project_id"] == project_id


@pytest.mark.asyncio
async def test_list_tasks(client):
    await client.post("/api/tasks", json={
        "title": "A", "description": "d", "target_repo": "/tmp",
    })
    await client.post("/api/tasks", json={
        "title": "B", "description": "d", "target_repo": "/tmp",
    })
    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


@pytest.mark.asyncio
async def test_get_task(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "T"


@pytest.mark.asyncio
async def test_get_task_not_found(client):
    resp = await client.get("/api/tasks/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_task(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/tasks/{task_id}")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_cancel_task(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_retry_task(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    # Manual retry only accepts terminal generations.
    cancelled = await client.post(f"/api/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200
    resp = await client.post(f"/api/tasks/{task_id}/retry")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_retry_rejects_stale_fast_view_before_mutating_generation(
    client,
    session_factory,
):
    """A stale Fast retry must not enqueue the Task as Standard."""
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Stale retry",
        "description": "d",
        "target_repo": "/tmp",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "default",
    })
    task_id = create_resp.json()["id"]
    cancelled = await client.post(f"/api/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200

    response = await client.post(
        f"/api/tasks/{task_id}/retry",
        json={
            "expected_routing": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "codex_service_tier": "priority",
            },
        },
    )

    assert response.status_code == 409
    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "cancelled"
    assert task.retry_count == 0


@pytest.mark.parametrize("status", ["pending", "in_progress", "executing", "migrating"])
@pytest.mark.asyncio
async def test_manual_retry_rejects_non_terminal_status(
    client,
    session_factory,
    status,
):
    """Manual retry cannot steal active, queued, or migrating work."""

    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Not retryable",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status=status)
        )
        await db.commit()

    response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert status in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == status
    assert task.retry_count == 0


@pytest.mark.asyncio
async def test_retry_rejects_orphan_pid_that_may_be_alive(
    client, session_factory,
):
    """Manual retry must not erase an unknown live process owner."""
    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="orphan-live",
            description="d",
            status="failed",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="orphan-live-slot",
            status="error",
            pid=43210,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    with patch("backend.api.tasks.os.kill", return_value=None):
        response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "still alive" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id == instance_id
        assert instance.pid == 43210
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_retry_reconciles_dead_orphan_before_releasing_task(
    client, session_factory,
):
    """A definitively dead PID is detached before the task becomes pending."""
    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="orphan-dead",
            description="d",
            status="failed",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="orphan-dead-slot",
            status="error",
            pid=54321,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    with patch(
        "backend.api.tasks.os.kill",
        side_effect=ProcessLookupError,
    ):
        response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    assert response.json()["instance_id"] is None
    async with session_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.status == "error"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_retry_does_not_clear_owner_that_changes_while_waiting(
    client, session_factory,
):
    """A retry waiting on an old slot lock cannot erase a newer orphan link."""
    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(title="retry-owner-race", description="d", status="failed")
        old_instance = Instance(name="old-owner", status="error")
        new_instance = Instance(name="new-owner", status="error", pid=65432)
        db.add_all([task, old_instance, new_instance])
        await db.flush()
        task.instance_id = old_instance.id
        old_instance.current_task_id = task.id
        await db.commit()
        task_id = task.id
        old_instance_id = old_instance.id
        new_instance_id = new_instance.id

    old_lock = asyncio.Lock()
    reached_lock = asyncio.Event()
    await old_lock.acquire()
    manager = MagicMock()
    manager.processes = {}

    def lifecycle_lock(instance_id):
        assert instance_id == old_instance_id
        reached_lock.set()
        return old_lock

    manager._instance_lifecycle_lock.side_effect = lifecycle_lock
    try:
        with patch("backend.main.instance_manager", manager):
            request = asyncio.create_task(
                client.post(f"/api/tasks/{task_id}/retry")
            )
            await asyncio.wait_for(reached_lock.wait(), timeout=1)
            async with session_factory() as db:
                task = await db.get(Task, task_id)
                old_instance = await db.get(Instance, old_instance_id)
                new_instance = await db.get(Instance, new_instance_id)
                old_instance.current_task_id = None
                task.instance_id = new_instance_id
                new_instance.current_task_id = task_id
                await db.commit()
            old_lock.release()
            response = await asyncio.wait_for(request, timeout=1)
    finally:
        if old_lock.locked():
            old_lock.release()

    assert response.status_code == 409
    assert "ownership changed" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        new_instance = await db.get(Instance, new_instance_id)
        assert task.status == "failed"
        assert task.instance_id == new_instance_id
        assert new_instance.pid == 65432
        assert new_instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_retry_checks_reverse_owner_when_task_side_owner_is_null(
    client,
    session_factory,
):
    """A one-sided reverse owner is still process evidence and blocks retry."""

    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="reverse-only-live-owner",
            description="d",
            status="failed",
            instance_id=None,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="reverse-only-slot",
            status="error",
            pid=76543,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    with patch("backend.api.tasks.os.kill", return_value=None):
        response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "still alive" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert instance.pid == 76543
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_retry_uses_full_manager_generation_evidence(
    client,
    session_factory,
):
    """A reaped parent is insufficient while descendants/consumer remain."""

    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="managed-descendant-owner",
            description="d",
            status="failed",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="managed-descendant-slot",
            status="error",
            pid=None,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    manager = MagicMock()
    manager._instance_lifecycle_lock.return_value = asyncio.Lock()
    manager.is_running.return_value = True
    # The old parent object looks terminal; is_running additionally covers its
    # process group, container supervisor and output consumer generation.
    manager.processes = {
        instance_id: MagicMock(returncode=0),
    }
    with patch("backend.main.instance_manager", manager):
        response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "live managed generation" in response.json()["detail"]
    manager.is_running.assert_called_with(instance_id)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id == instance_id
        assert instance.current_task_id == task_id


# === New tests (Phase 2 gaps) ===


@pytest.mark.asyncio
async def test_update_task(client):
    """PUT /api/tasks/{id} updates task fields."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Original", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.put(f"/api/tasks/{task_id}", json={"title": "Updated"})
    assert resp.status_code == 200
    assert resp.json()["title"] == "Updated"


@pytest.mark.asyncio
async def test_attention_tag_create_update_and_clear_preserves_system_tags(client):
    created = await client.post("/api/tasks", json={
        "title": "Tagged session",
        "description": "d",
        "target_repo": "/tmp",
        "attention_tag": "  等它结束后再看  ",
        "tags": ["existing-system-marker"],
    })

    assert created.status_code == 201
    task_id = created.json()["id"]
    assert created.json()["attention_tag"] == "等它结束后再看"
    assert created.json()["tags"] == ["existing-system-marker"]

    updated = await client.put(
        f"/api/tasks/{task_id}",
        json={"attention_tag": "  今晚继续  "},
    )

    assert updated.status_code == 200
    assert updated.json()["attention_tag"] == "今晚继续"
    assert updated.json()["tags"] == ["existing-system-marker"]

    cleared = await client.put(
        f"/api/tasks/{task_id}",
        json={"attention_tag": "   "},
    )

    assert cleared.status_code == 200
    assert cleared.json()["attention_tag"] is None
    assert cleared.json()["tags"] == ["existing-system-marker"]


@pytest.mark.asyncio
async def test_attention_tag_rejects_values_longer_than_80_characters(client):
    created = await client.post("/api/tasks", json={
        "title": "Tagged session",
        "description": "d",
        "target_repo": "/tmp",
        "attention_tag": "x" * 81,
    })

    assert created.status_code == 422


@pytest.mark.asyncio
async def test_cloned_task_inherits_attention_tag_unless_overridden(client):
    source = await client.post("/api/tasks", json={
        "title": "Source",
        "description": "d",
        "target_repo": "/tmp",
        "attention_tag": "等源任务结束",
    })
    source_id = source.json()["id"]

    inherited = await client.post("/api/tasks", json={
        "title": "Inherited",
        "description": "d",
        "target_repo": "/tmp",
        "clone_from_task_id": source_id,
    })
    overridden = await client.post("/api/tasks", json={
        "title": "Overridden",
        "description": "d",
        "target_repo": "/tmp",
        "clone_from_task_id": source_id,
        "attention_tag": "单独关注",
    })
    cleared = await client.post("/api/tasks", json={
        "title": "Cleared",
        "description": "d",
        "target_repo": "/tmp",
        "clone_from_task_id": source_id,
        "attention_tag": None,
    })

    assert inherited.status_code == 201
    assert inherited.json()["attention_tag"] == "等源任务结束"
    assert overridden.status_code == 201
    assert overridden.json()["attention_tag"] == "单独关注"
    assert cleared.status_code == 201
    assert cleared.json()["attention_tag"] is None


@pytest.mark.asyncio
async def test_update_task_not_found(client):
    resp = await client.put("/api/tasks/9999", json={"title": "X"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_tasks_filter_status(client):
    """GET /api/tasks?status=pending returns only matching tasks."""
    await client.post("/api/tasks", json={
        "title": "A", "description": "d", "target_repo": "/tmp",
    })
    create2 = await client.post("/api/tasks", json={
        "title": "B", "description": "d", "target_repo": "/tmp",
    })
    # Cancel B so it's not pending
    await client.post(f"/api/tasks/{create2.json()['id']}/cancel")

    resp = await client.get("/api/tasks?status=pending")
    assert resp.status_code == 200
    tasks = resp.json()
    assert all(t["status"] == "pending" for t in tasks)


@pytest.mark.asyncio
async def test_list_and_count_tasks_filter_by_task_kind(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        main = Task(title="Main", description="d", mode="auto")
        standalone = Task(
            title="Standalone Plan",
            description="d",
            mode="plan",
        )
        db.add_all([main, standalone])
        await db.flush()
        related = Task(
            title="Related Plan",
            description="d",
            mode="plan",
            plan_target_task_id=main.id,
        )
        db.add(related)
        await db.commit()

    expected = {
        "main": "Main",
        "standalone_plan": "Standalone Plan",
        "related_plan": "Related Plan",
    }
    for task_kind, title in expected.items():
        response = await client.get(
            f"/api/tasks?task_kind={task_kind}"
        )
        assert response.status_code == 200
        assert [task["title"] for task in response.json()] == [title]

        count = await client.get(
            f"/api/tasks/count?task_kind={task_kind}"
        )
        assert count.status_code == 200
        assert count.json() == {"total": 1}

    invalid = await client.get("/api/tasks?task_kind=unknown")
    assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_list_tasks_pagination(client):
    """GET /api/tasks?limit=1&offset=1 returns second task."""
    await client.post("/api/tasks", json={
        "title": "First", "description": "d", "target_repo": "/tmp",
    })
    await client.post("/api/tasks", json={
        "title": "Second", "description": "d", "target_repo": "/tmp",
    })
    resp = await client.get("/api/tasks?limit=1&offset=1")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1


@pytest.mark.asyncio
async def test_queue_next(client):
    """GET /api/tasks/queue/next returns pending tasks."""
    await client.post("/api/tasks", json={
        "title": "Pending", "description": "d", "target_repo": "/tmp",
    })
    resp = await client.get("/api/tasks/queue/next")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) >= 1
    assert all(t["status"] == "pending" for t in tasks)


@pytest.mark.asyncio
async def test_delete_in_progress_rejected(client, session_factory):
    """Cannot delete a task in in_progress state."""
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Set to in_progress directly in DB
    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == task_id).values(status="in_progress")
        )
        await db.commit()

    resp = await client.delete(f"/api/tasks/{task_id}")
    assert resp.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["plan_review", "superseded"])
async def test_delete_stopped_plan_cleans_pipeline_history(
    client,
    session_factory,
    status,
):
    from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
    from backend.models.task import Task

    async with session_factory() as db:
        plan = Task(
            title="Disposable Plan",
            description="Plan this",
            status=status,
            mode="plan",
            plan_content="A completed proposal",
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(plan_task_id=plan.id, status="completed")
        db.add(run)
        await db.flush()
        db.add(
            PlanAgentStep(
                run_id=run.id,
                step_type="planner",
                provider="claude",
                status="completed",
            )
        )
        await db.commit()
        plan_id = plan.id
        run_id = run.id

    response = await client.delete(f"/api/tasks/{plan_id}")

    assert response.status_code == 200
    async with session_factory() as db:
        assert await db.get(Task, plan_id) is None
        assert await db.get(PlanAgentRun, run_id) is None
        steps = (
            await db.execute(
                select(PlanAgentStep).where(PlanAgentStep.run_id == run_id)
            )
        ).scalars().all()
        assert steps == []


@pytest.mark.asyncio
async def test_delete_non_plan_in_plan_review_state_is_rejected(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="Not a Plan",
            description="work",
            status="plan_review",
            mode="auto",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    response = await client.delete(f"/api/tasks/{task_id}")

    assert response.status_code == 400


# === image_paths tests ===


@pytest.mark.asyncio
async def test_create_task_with_image_paths(client, session_factory):
    """image_paths are stored in task.metadata_['image_paths']."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "Img Task",
        "description": "look at this image",
        "target_repo": "/tmp",
        "image_paths": ["/uploads/a.png", "/uploads/b.jpg"],
    })
    assert resp.status_code == 201
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.metadata_ is not None
    assert task.metadata_["image_paths"] == ["/uploads/a.png", "/uploads/b.jpg"]


@pytest.mark.asyncio
async def test_create_task_without_image_paths(client, session_factory):
    """Task created without image_paths has no image_paths in metadata_."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "No Img", "description": "plain task", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert (task.metadata_ or {}).get("image_paths") is None


@pytest.mark.asyncio
async def test_create_task_image_paths_not_in_response(client):
    """image_paths field is not leaked in the TaskResponse (stored in metadata_)."""
    resp = await client.post("/api/tasks", json={
        "title": "Img Task",
        "description": "check response",
        "target_repo": "/tmp",
        "image_paths": ["/uploads/x.png"],
    })
    assert resp.status_code == 201
    data = resp.json()
    # image_paths should not appear as a top-level key in the response schema
    assert "image_paths" not in data


# === max_iterations tests ===


@pytest.mark.asyncio
async def test_create_loop_task_default_max_iterations(client):
    """Loop task created without max_iterations gets default value of 50."""
    resp = await client.post("/api/tasks", json={
        "title": "Loop Default",
        "mode": "loop",
        "todo_file_path": "TODO.md",
        "target_repo": "/tmp/repo",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["max_iterations"] == 50


@pytest.mark.asyncio
async def test_create_loop_task_custom_max_iterations(client):
    """Loop task created with custom max_iterations stores it correctly."""
    resp = await client.post("/api/tasks", json={
        "title": "Loop Custom",
        "mode": "loop",
        "todo_file_path": "TODO.md",
        "target_repo": "/tmp/repo",
        "max_iterations": 10,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["max_iterations"] == 10


@pytest.mark.asyncio
async def test_create_auto_task_max_iterations_in_response(client):
    """Non-loop task also exposes max_iterations in response (always 50 by default)."""
    resp = await client.post("/api/tasks", json={
        "title": "Auto Task",
        "description": "do something",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert "max_iterations" in data
    assert data["max_iterations"] == 50


@pytest.mark.asyncio
async def test_update_task_max_iterations(client):
    """PUT /api/tasks/{id} can update max_iterations."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Loop Task",
        "mode": "loop",
        "todo_file_path": "TODO.md",
        "target_repo": "/tmp/repo",
        "max_iterations": 20,
    })
    task_id = create_resp.json()["id"]

    resp = await client.put(f"/api/tasks/{task_id}", json={"max_iterations": 5})
    assert resp.status_code == 200
    assert resp.json()["max_iterations"] == 5


@pytest.mark.asyncio
async def test_create_loop_task_requires_todo_file_path(client):
    """Loop task without todo_file_path returns 422."""
    resp = await client.post("/api/tasks", json={
        "title": "Missing Todo",
        "mode": "loop",
        "target_repo": "/tmp/repo",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_loop_task_max_iterations_persisted(client, session_factory):
    """max_iterations value is actually persisted to the database."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "Persisted",
        "mode": "loop",
        "todo_file_path": "TODO.md",
        "target_repo": "/tmp/repo",
        "max_iterations": 7,
    })
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.max_iterations == 7


# === has_unread tests ===


@pytest.mark.asyncio
async def test_create_task_has_unread_defaults_false(client):
    """New task has has_unread=False by default."""
    resp = await client.post("/api/tasks", json={
        "title": "Unread test", "description": "d", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["has_unread"] is False


@pytest.mark.asyncio
async def test_mark_task_read_clears_unread(client, session_factory):
    """POST /api/tasks/{id}/read sets has_unread=False."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Unread", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Set has_unread=True directly in DB
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(has_unread=True))
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/read")
    assert resp.status_code == 200
    assert resp.json()["has_unread"] is False


@pytest.mark.asyncio
async def test_mark_task_read_not_found(client):
    """POST /api/tasks/9999/read returns 404 for missing task."""
    resp = await client.post("/api/tasks/9999/read")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_has_unread_persisted_in_db(client, session_factory):
    """has_unread=True set in DB is returned in task response."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Persist unread", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(has_unread=True))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["has_unread"] is True


# === Model field tests ===


@pytest.mark.asyncio
async def test_create_task_with_model(client):
    """Task created with model field stores and returns the model."""
    resp = await client.post("/api/tasks", json={
        "title": "Opus task", "description": "d", "target_repo": "/tmp", "model": "opus",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["model"] == "opus"


@pytest.mark.asyncio
async def test_create_task_without_model_fills_default(client):
    """设置归 Task：不指定 model 时创建即填入全局默认值。"""
    from backend.config import settings
    with (
        patch.object(settings, "default_provider", "codex"),
        patch.object(settings, "default_codex_model", "gpt-5.6-sol"),
    ):
        resp = await client.post("/api/tasks", json={
            "title": "No model", "description": "d", "target_repo": "/tmp",
        })
    assert resp.status_code == 201
    data = resp.json()
    assert data["provider"] == "codex"
    assert data["model"] == "gpt-5.6-sol"


@pytest.mark.asyncio
async def test_create_task_model_persisted_in_get(client):
    """Model value survives a round-trip through GET."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp", "model": "sonnet",
    })
    task_id = create_resp.json()["id"]

    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["model"] == "sonnet"


@pytest.mark.asyncio
async def test_create_task_model_in_list(client):
    """model field is included when listing tasks."""
    await client.post("/api/tasks", json={
        "title": "A", "description": "d", "target_repo": "/tmp", "model": "haiku",
    })
    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    tasks = resp.json()
    assert tasks[0]["model"] == "haiku"


# === Codex service tier tests ===


@pytest.mark.asyncio
async def test_create_task_defaults_to_standard_service_tier(client):
    resp = await client.post("/api/tasks", json={
        "title": "Standard task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })

    assert resp.status_code == 201, resp.text
    task_id = resp.json()["id"]
    assert resp.json()["codex_service_tier"] == "default"

    get_resp = await client.get(f"/api/tasks/{task_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["codex_service_tier"] == "default"


@pytest.mark.asyncio
async def test_create_fast_codex_task_persists_priority(client):
    resp = await client.post("/api/tasks", json={
        "title": "Fast task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 201, resp.text
    assert resp.json()["codex_service_tier"] == "priority"


@pytest.mark.asyncio
async def test_create_fast_goal_inherits_task_model_for_evaluator(client):
    resp = await client.post("/api/tasks", json={
        "title": "Fast Goal",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 201, resp.text
    assert resp.json()["goal_evaluator_model"] is None
    assert resp.json()["codex_service_tier"] == "priority"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "goal_evaluator_model"),
    [
        (None, "configured-default"),
        ("default", "configured-default"),
        ("configured-default", "default"),
    ],
)
async def test_create_fast_goal_normalizes_default_model_aliases(
    client,
    model,
    goal_evaluator_model,
):
    from backend.config import settings

    default_model = settings.default_codex_model
    payload = {
        "title": "Fast Goal default aliases",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "provider": "codex",
        "model": default_model if model == "configured-default" else model,
        "goal_evaluator_model": (
            default_model
            if goal_evaluator_model == "configured-default"
            else goal_evaluator_model
        ),
        "codex_service_tier": "priority",
    }

    resp = await client.post("/api/tasks", json=payload)

    assert resp.status_code == 201, resp.text
    assert resp.json()["codex_service_tier"] == "priority"


@pytest.mark.asyncio
async def test_create_fast_goal_rejects_distinct_evaluator_model(client):
    resp = await client.post("/api/tasks", json={
        "title": "Split Fast Goal",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "goal_evaluator_model": "gpt-5.6-terra",
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 422
    assert "must use the Task model" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_fast_goal_rejects_distinct_evaluator_model(
    client,
    session_factory,
):
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Fast Goal update",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })
    task_id = created.json()["id"]
    cancelled = await client.post(f"/api/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"goal_evaluator_model": "gpt-5.6-terra"},
    )

    assert response.status_code == 422
    assert "must use the Task model" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.goal_evaluator_model is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "model", "detail"),
    [
        ("claude", "claude-opus-4-6", "only available for Codex"),
        ("codex", "gpt-5.4-mini", "not supported by model"),
        ("codex", "gpt-5.3-codex-spark", "not supported by model"),
    ],
)
async def test_create_fast_task_rejects_incompatible_configuration(
    client,
    provider,
    model,
    detail,
):
    resp = await client.post("/api/tasks", json={
        "title": "Invalid Fast task",
        "description": "d",
        "provider": provider,
        "model": model,
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 422
    assert detail in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_validates_merged_provider_model_and_service_tier(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "Fast task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })
    assert create_resp.status_code == 201
    task_id = create_resp.json()["id"]

    claude_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={"provider": "claude"},
    )
    assert claude_resp.status_code == 422
    assert "only available for Codex" in claude_resp.json()["detail"]

    mini_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={"model": "gpt-5.4-mini"},
    )
    assert mini_resp.status_code == 422
    assert "not supported by model" in mini_resp.json()["detail"]

    disable_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={
            "provider": "claude",
            "model": "claude-opus-4-6",
            "codex_service_tier": "default",
        },
    )
    assert disable_resp.status_code == 200, disable_resp.text
    assert disable_resp.json()["provider"] == "claude"
    assert disable_resp.json()["codex_service_tier"] == "default"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "mode"),
    (("in_progress", "loop"), ("executing", "goal")),
)
async def test_local_fast_update_rejects_active_mode_generation(
    client,
    session_factory,
    status,
    mode,
):
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Active Standard task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status=status, mode=mode)
        )
        await db.commit()

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409
    assert "execution claim became active" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.codex_service_tier == "default"


@pytest.mark.asyncio
async def test_local_fast_update_rejects_running_ccm_sub_agent(
    client,
    session_factory,
):
    from backend.models.monitor_session import MonitorSession
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Parent with old Standard child",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        db.add(
            MonitorSession(
                task_id=task_id,
                agent_type="sub_agent",
                source="ccm",
                description="still running",
                status="running",
            )
        )
        await db.commit()

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409
    assert "sub-agent is running" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.codex_service_tier == "default"


@pytest.mark.asyncio
async def test_local_fast_update_holds_codex_thread_guard_through_commit(
    client,
    session_factory,
    monkeypatch,
):
    import backend.main as main_module
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Idle native thread",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.session_id = "idle-native-thread"
        await db.commit()

    guard_exited = False

    @asynccontextmanager
    async def routing_guard(_home, thread_id):
        nonlocal guard_exited
        assert thread_id == "idle-native-thread"
        async with session_factory() as db:
            before = await db.get(Task, task_id)
            assert before.codex_service_tier == "default"
        yield {"thread": {"status": {"type": "idle"}}, "goal": None}
        async with session_factory() as db:
            committed = await db.get(Task, task_id)
            assert committed.codex_service_tier == "priority"
        guard_exited = True

    monkeypatch.setattr(
        main_module.instance_manager,
        "codex_thread_routing_guard",
        routing_guard,
    )
    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "priority"
    assert guard_exited


@pytest.mark.asyncio
async def test_local_fast_update_fails_before_commit_when_thread_not_idle(
    client,
    session_factory,
    monkeypatch,
):
    import backend.main as main_module
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Active native Goal",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.session_id = "active-goal-thread"
        await db.commit()

    @asynccontextmanager
    async def routing_guard(_home, _thread_id):
        raise RuntimeError("goal:active")
        yield  # pragma: no cover

    monkeypatch.setattr(
        main_module.instance_manager,
        "codex_thread_routing_guard",
        routing_guard,
    )
    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409
    assert "could not be proven idle" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.codex_service_tier == "default"


@pytest.mark.asyncio
async def test_update_rejects_null_or_unknown_service_tier(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "Tier validation",
        "description": "d",
    })
    task_id = create_resp.json()["id"]

    null_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": None},
    )
    assert null_resp.status_code == 422

    unknown_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "turbo"},
    )
    assert unknown_resp.status_code == 422


@pytest.mark.asyncio
async def test_migration_import_preserves_fast_service_tier(client):
    resp = await client.post("/api/tasks/migration-import", json={
        "id": 7091,
        "title": "Migrated Fast task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.5",
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "cancelled"
    assert resp.json()["codex_service_tier"] == "priority"


@pytest.mark.asyncio
async def test_migration_import_rejects_incompatible_fast_service_tier(client):
    resp = await client.post("/api/tasks/migration-import", json={
        "id": 7092,
        "title": "Invalid migrated Fast task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.4-mini",
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 422
    assert "not supported by model" in resp.json()["detail"]


# === Title update tests ===


@pytest.mark.asyncio
async def test_update_task_title_only(client):
    """PUT /api/tasks/{id} with only title preserves other fields."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Original Title", "description": "Keep this", "target_repo": "/tmp", "priority": 2,
    })
    task_id = create_resp.json()["id"]

    resp = await client.put(f"/api/tasks/{task_id}", json={"title": "New Title"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["title"] == "New Title"
    assert data["description"] == "Keep this"
    assert data["priority"] == 2


@pytest.mark.asyncio
async def test_update_task_title_empty_string(client):
    """PUT /api/tasks/{id} can set title to empty string."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Has Title", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    resp = await client.put(f"/api/tasks/{task_id}", json={"title": ""})
    assert resp.status_code == 200
    assert resp.json()["title"] == ""


@pytest.mark.asyncio
async def test_update_task_title_persisted_in_get(client):
    """Updated title is returned on subsequent GET."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Old", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    await client.put(f"/api/tasks/{task_id}", json={"title": "Renamed"})
    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "Renamed"


# === Effort level tests ===


@pytest.mark.asyncio
async def test_create_task_with_effort_level(client):
    """Task created with effort_level field stores and returns it."""
    resp = await client.post("/api/tasks", json={
        "title": "Effort task", "description": "d", "target_repo": "/tmp", "effort_level": "high",
    })
    assert resp.status_code == 201
    assert resp.json()["effort_level"] == "high"


@pytest.mark.asyncio
async def test_create_task_without_effort_level_fills_default(client):
    """设置归 Task：不指定 effort 时创建即填入全局默认值。"""
    from backend.config import settings
    resp = await client.post("/api/tasks", json={
        "title": "No effort", "description": "d", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["effort_level"] == settings.default_effort


@pytest.mark.asyncio
async def test_create_task_effort_level_persisted_in_get(client):
    """effort_level survives a round-trip through GET."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp", "effort_level": "max",
    })
    task_id = create_resp.json()["id"]

    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["effort_level"] == "max"


@pytest.mark.asyncio
async def test_create_task_effort_level_in_list(client):
    """effort_level is included when listing tasks."""
    await client.post("/api/tasks", json={
        "title": "A", "description": "d", "target_repo": "/tmp", "effort_level": "low",
    })
    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    tasks = resp.json()
    assert tasks[0]["effort_level"] == "low"


# === Goal mode tests ===


@pytest.mark.asyncio
async def test_create_goal_task(client):
    """Goal task with condition is created successfully."""
    resp = await client.post("/api/tasks", json={
        "title": "Goal Task",
        "description": "implement feature",
        "mode": "goal",
        "goal_condition": "all tests pass",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["mode"] == "goal"
    assert data["goal_condition"] == "all tests pass"
    assert data["goal_max_turns"] == 30
    assert data["goal_turns_used"] == 0
    assert data["goal_last_reason"] is None


@pytest.mark.asyncio
async def test_create_goal_task_custom_max_turns(client):
    """Goal task with custom max_turns stores it correctly."""
    resp = await client.post("/api/tasks", json={
        "title": "Goal Custom",
        "description": "do it",
        "mode": "goal",
        "goal_condition": "lint clean",
        "goal_max_turns": 15,
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["goal_max_turns"] == 15


@pytest.mark.asyncio
async def test_create_goal_task_requires_condition(client):
    """Goal task without goal_condition returns 422."""
    resp = await client.post("/api/tasks", json={
        "title": "No Condition",
        "description": "do it",
        "mode": "goal",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_goal_task_with_evaluator_model(client):
    """Goal task with custom evaluator model stores it."""
    resp = await client.post("/api/tasks", json={
        "title": "Goal Eval",
        "description": "do it",
        "mode": "goal",
        "goal_condition": "condition",
        "goal_evaluator_model": "claude-sonnet-4-6",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["goal_evaluator_model"] == "claude-sonnet-4-6"


@pytest.mark.asyncio
async def test_goal_fields_persisted_in_db(client, session_factory):
    """Goal fields are actually persisted to the database."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "Persist Goal",
        "description": "do it",
        "mode": "goal",
        "goal_condition": "all green",
        "goal_max_turns": 20,
        "target_repo": "/tmp",
    })
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.goal_condition == "all green"
    assert task.goal_max_turns == 20
    assert task.goal_turns_used == 0


@pytest.mark.asyncio
async def test_goal_fields_in_get_response(client):
    """Goal fields are returned in GET /api/tasks/{id}."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["goal_condition"] == "tests pass"
    assert data["goal_max_turns"] == 30
    assert data["goal_turns_used"] == 0


@pytest.mark.asyncio
async def test_update_goal_task_fields(client):
    """PUT /api/tasks/{id} can update goal-specific fields."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Goal Update",
        "description": "d",
        "mode": "goal",
        "goal_condition": "old condition",
        "goal_max_turns": 10,
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    resp = await client.put(f"/api/tasks/{task_id}", json={
        "goal_condition": "new condition",
        "goal_max_turns": 50,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["goal_condition"] == "new condition"
    assert data["goal_max_turns"] == 50


@pytest.mark.asyncio
async def test_non_goal_task_has_null_goal_fields(client):
    """Auto-mode task has null goal fields."""
    resp = await client.post("/api/tasks", json={
        "title": "Auto", "description": "d", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["goal_condition"] is None
    assert data["goal_max_turns"] == 30
    assert data["goal_turns_used"] == 0
    assert data["goal_last_reason"] is None


# === Cancel task kills process tests ===


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_tracked_pty_background_terminal_request_clears_marker(
    client,
    session_factory,
    endpoint,
    terminal_status,
):
    """An exact live owner lets terminal CAS retire its PTY marker."""

    import backend.main
    from backend.models.instance import Instance
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": f"Tracked background {endpoint}",
        "description": "d",
    })
    task_id = created.json()["id"]
    started_at = datetime(2026, 7, 28, 1, 2, 3)
    async with session_factory() as db:
        instance = Instance(
            name=f"tracked-background-{endpoint}",
            status="running",
            pid=41001,
            started_at=started_at,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="executing",
                instance_id=instance.id,
                started_at=started_at,
                session_id=f"tracked-session-{endpoint}",
                pty_background_generation=f"tracked-generation-{endpoint}",
            )
        )
        await db.commit()
        instance_id = instance.id

    async def stop_exact(
        stopped_task_id,
        _db,
        *,
        expected_generations,
        task_status,
    ):
        assert stopped_task_id == task_id
        assert task_status == terminal_status
        assert expected_generations == [
            (instance_id, 41001, started_at)
        ]
        async with session_factory() as db:
            instance = await db.get(Instance, instance_id)
            instance.status = "idle"
            instance.pid = None
            instance.current_task_id = None
            task = await db.get(Task, task_id)
            task.status = terminal_status
            task.completed_at = datetime.utcnow()
            task.pty_background_generation = None
            await db.commit()
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            terminal_status,
            background_active=False,
        )
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            side_effect=stop_exact,
        ),
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 200, response.text
    publish.assert_awaited_once_with(
        task_id,
        terminal_status,
        background_active=False,
    )
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == terminal_status
        assert task.pty_background_generation is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_owner_stop_preserves_new_background_generation(
    client,
    session_factory,
    endpoint,
    terminal_status,
):
    """A post-stop same-Task epoch must never be mistaken for the old tail."""

    import backend.main
    from backend.models.instance import Instance
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": f"Owner ABA {endpoint}",
        "description": "d",
    })
    task_id = created.json()["id"]
    started_at = datetime(2026, 7, 28, 4, 5, 6)
    new_generation = f"new-owner-background-{endpoint}"
    async with session_factory() as db:
        instance = Instance(
            name=f"owner-aba-{endpoint}",
            status="running",
            pid=42001,
            started_at=started_at,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="executing",
                instance_id=instance.id,
                started_at=started_at,
                session_id=f"owner-aba-session-{endpoint}",
            )
        )
        await db.commit()
        instance_id = instance.id

    async def stop_then_new_epoch(
        _task_id,
        _db,
        *,
        expected_generations,
        task_status,
    ):
        assert _task_id == task_id
        assert task_status == terminal_status
        assert expected_generations == [
            (instance_id, 42001, started_at)
        ]
        async with session_factory() as db:
            instance = await db.get(Instance, instance_id)
            instance.status = "idle"
            instance.pid = None
            instance.current_task_id = None
            task = await db.get(Task, task_id)
            task.status = terminal_status
            task.completed_at = datetime.utcnow()
            # Models a newer same-task turn completing and arming after the
            # old Instance stop fence is released.
            task.pty_background_generation = new_generation
            await db.commit()
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            side_effect=stop_then_new_epoch,
        ),
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 409
    assert "newer PTY background generation" in response.json()["detail"]
    publish.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == terminal_status
        assert task.pty_background_generation == new_generation


@pytest.mark.asyncio
async def test_stop_session_stops_ownerless_pty_background_generation(
    client,
    session_factory,
):
    """Late output is stopped by Task/session token, not historical Instance."""

    import backend.main
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Detached background stop",
        "description": "d",
    })
    task_id = created.json()["id"]
    session_id = "detached-api-session"
    generation = "detached-api-generation"
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="completed",
                session_id=session_id,
                pty_background_generation=generation,
            )
        )
        await db.commit()

    async def settle_exact(
        settled_task_id,
        settled_session_id,
        settled_generation,
        **expected,
    ):
        assert (
            settled_task_id,
            settled_session_id,
            settled_generation,
        ) == (task_id, session_id, generation)
        assert expected["expected_status"] == "completed"
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.pty_background_generation == generation,
                )
                .values(pty_background_generation=None)
            )
            await db.commit()
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch.object(
            backend.main.instance_manager,
            "stop_detached_pty_background_generation",
            new_callable=AsyncMock,
            side_effect=settle_exact,
        ) as stop_detached,
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/stop-session"
        )

    assert response.status_code == 200, response.text
    assert response.json()["stopped"] is True
    stop_detached.assert_awaited_once()
    publish.assert_awaited_once_with(
        task_id,
        "completed",
        background_active=False,
    )
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.pty_background_generation is None


@pytest.mark.asyncio
async def test_cancel_ownerless_pty_background_requires_stop_session(
    client,
    session_factory,
):
    """Cancel cannot silently retire a detached Session it does not stop."""

    import backend.main
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Detached background cancel failure",
        "description": "d",
    })
    task_id = created.json()["id"]
    generation = "detached-cancel-failure"
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="completed",
                session_id="detached-cancel-session",
                pty_background_generation=generation,
            )
        )
        await db.commit()

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch.object(
            backend.main.instance_manager,
            "stop_detached_pty_background_generation",
            new_callable=AsyncMock,
            return_value=False,
        ) as stop_detached,
    ):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 400
    stop_detached.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.pty_background_generation == generation


@pytest.mark.asyncio
async def test_ownerless_pending_cancel_does_not_infer_process(
    client, session_factory
):
    """A pending Task without an exact owner needs no process stop."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Cancel Me", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/cancel")

    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_task_still_works_if_no_process(client):
    """Cancel works even when no process is running (stop returns False)."""
    create_resp = await client.post("/api/tasks", json={
        "title": "No Process", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")

    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_returns_409_when_queue_worker_does_not_settle(
    client, session_factory
):
    from backend.models.task import Task
    from backend.services.dispatcher import TaskQueueAbortTimeoutError
    import backend.main

    create_resp = await client.post("/api/tasks", json={
        "title": "Stubborn queue", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        side_effect=TaskQueueAbortTimeoutError("still active"),
    ):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")

    assert resp.status_code == 409
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "initial_status"),
    (("cancel", "pending"), ("stop-session", "executing")),
)
async def test_terminal_generation_uses_database_normalized_completed_at(
    client,
    session_factory,
    endpoint,
    initial_status,
):
    """MySQL DATETIME may truncate Python microseconds before postcheck."""

    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Timestamp fence",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    if initial_status != "pending":
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(status=initial_status)
            )
            await db.commit()

    normalized = datetime(2026, 7, 23, 12, 34, 56)
    observed_postcheck: dict = {}

    async def capture_postcheck(locked_task_id, db, **kwargs):
        assert locked_task_id == task_id
        observed_postcheck.update(kwargs)
        db.expire_all()
        return await db.get(Task, task_id)

    with (
        patch(
            "backend.api.tasks._read_persisted_task_completed_at",
            new_callable=AsyncMock,
            return_value=normalized,
        ) as read_persisted,
        patch(
            "backend.api.tasks._lock_task_generation",
            side_effect=capture_postcheck,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 200, response.text
    read_persisted.assert_awaited_once()
    assert observed_postcheck["expected_completed_at"] == normalized


@pytest.mark.asyncio
async def test_ownerless_stop_drops_stale_completed_publication_after_retry(
    client,
    session_factory,
):
    """A retry committed after stop cannot receive the old completed event."""

    import backend.main
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Stop publication retry race",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="executing")
        )
        await db.commit()

    async def retry_before_publication(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status="pending",
                    retry_count=Task.retry_count + 1,
                    completed_at=None,
                )
            )
            await db.commit()
        return None

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "backend.api.tasks._lock_task_generation",
            new_callable=AsyncMock,
            side_effect=retry_before_publication,
        ),
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/stop-session"
        )

    assert response.status_code == 409
    publish.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.retry_count == 1


@pytest.mark.asyncio
async def test_detached_stop_drops_false_event_if_new_background_rearms(
    client,
    session_factory,
):
    """A new exact PTY epoch suppresses the old background=false event."""

    import backend.main
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Detached publication rearm race",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    old_generation = "old-detached-generation"
    new_generation = "new-detached-generation"
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="completed",
                completed_at=datetime(2026, 7, 28, 2, 3, 4),
                session_id="detached-publication-session",
                pty_background_generation=old_generation,
            )
        )
        await db.commit()

    async def stop_old_generation(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.pty_background_generation == old_generation,
                )
                .values(pty_background_generation=None)
            )
            await db.commit()
        return True

    async def rearm_before_publication(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(pty_background_generation=new_generation)
            )
            await db.commit()
            return await db.get(Task, task_id)

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.instance_manager,
            "stop_detached_pty_background_generation",
            new_callable=AsyncMock,
            side_effect=stop_old_generation,
        ),
        patch(
            "backend.api.tasks._lock_task_generation",
            new_callable=AsyncMock,
            side_effect=rearm_before_publication,
        ),
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/stop-session"
        )

    assert response.status_code == 409
    publish.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.pty_background_generation == new_generation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_terminal_request_cancellation_before_first_commit_still_reaps(
    client,
    session_factory,
    endpoint,
    terminal_status,
):
    """A disconnected caller cannot strand a terminal Task with a live owner."""

    import backend.main
    from backend.models.instance import Instance
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": f"Cancel-safe {endpoint}",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    started_at = datetime(2026, 7, 23, 13, 14, 15)
    async with session_factory() as db:
        instance = Instance(
            name=f"cancel-safe-{endpoint}",
            status="running",
            pid=54101,
            current_task_id=task_id,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="executing",
                instance_id=instance.id,
                started_at=started_at,
            )
        )
        await db.commit()
        instance_id = instance.id

    stop_entered = asyncio.Event()
    allow_stop = asyncio.Event()

    async def stop_exact(
        stopped_task_id,
        _db,
        *,
        expected_generations,
        task_status,
    ):
        assert stopped_task_id == task_id
        assert task_status == terminal_status
        assert [
            (owner_id, pid, owner_started_at)
            for owner_id, pid, owner_started_at in expected_generations
        ] == [(instance_id, 54101, started_at)]
        stop_entered.set()
        await allow_stop.wait()
        async with session_factory() as db:
            owner = await db.get(Instance, instance_id)
            owner.status = "idle"
            owner.pid = None
            owner.current_task_id = None
            task = await db.get(Task, task_id)
            task.status = terminal_status
            task.completed_at = datetime.utcnow()
            task.pty_background_generation = None
            await db.commit()
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            terminal_status,
            background_active=False,
        )
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            side_effect=stop_exact,
        ) as stop,
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        request = asyncio.create_task(
            client.post(f"/api/tasks/{task_id}/{endpoint}")
        )
        await stop_entered.wait()
        request.cancel()
        await asyncio.sleep(0)
        assert not request.done()
        allow_stop.set()
        with pytest.raises(asyncio.CancelledError):
            await request

    stop.assert_awaited_once()
    publish.assert_awaited_once_with(
        task_id,
        terminal_status,
        background_active=False,
    )
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == terminal_status
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_pending_stop_session_does_not_infer_process(
    client, session_factory
):
    """A pending Task with no exact owner has no running session to stop."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Stop Me", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=True,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_session_409_when_hidden_launch_cannot_be_proven_reaped(
    client, session_factory
):
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    create_resp = await client.post("/api/tasks", json={
        "title": "Hidden launch", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        instance = Instance(name="hidden-launch", status="idle")
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="executing", instance_id=instance.id)
        )
        await db.commit()
        instance_id = instance.id

    with (
        patch.object(
            backend.main.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
            return_value=False,
        ) as barrier,
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 409
    barrier.assert_awaited_once_with(instance_id, task_id)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"


@pytest.mark.asyncio
async def test_stop_session_no_process_returns_400(client):
    """POST /api/tasks/{id}/stop-session returns 400 when no process found."""
    create_resp = await client.post("/api/tasks", json={
        "title": "No Session", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_stop_session_reports_unresolved_exact_owner(
    client,
    session_factory,
):
    """Terminal status must not masquerade as successful process cleanup."""

    from backend.models.instance import Instance
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Unresolved stop",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = Instance(
            name="unresolved-stop-slot",
            status="error",
            pid=45678,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        task.status = "executing"
        task.instance_id = instance.id
        await db.commit()
        instance_id = instance.id

    with patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=False,
    ):
        response = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert response.status_code == 409
    assert "cleanup could not be confirmed" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert instance.pid == 45678
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_cancel_reports_unresolved_exact_owner(
    client,
    session_factory,
):
    """Cancellation remains fail-closed while its exact process owner exists."""

    from backend.models.instance import Instance
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Unresolved cancel",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = Instance(
            name="unresolved-cancel-slot",
            status="error",
            pid=45679,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        task.status = "executing"
        task.instance_id = instance.id
        await db.commit()
        instance_id = instance.id

    with patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=False,
    ):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 409
    assert "cleanup could not be confirmed" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert instance.pid == 45679
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_cancel_retries_cancelled_auxiliary_cleanup(
    client,
    session_factory,
):
    """A failed auxiliary reap remains reachable through a repeated cancel."""

    import backend.main
    from backend.models.monitor_session import MonitorSession
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Retry auxiliary cancel",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="retained process",
            status="running",
        )
        db.add(monitor)
        await db.commit()
        monitor_id = monitor.id

    attempts = 0
    terminal_values = []

    async def fail_once(session_id, *, terminal=False):
        nonlocal attempts
        assert session_id == monitor_id
        terminal_values.append(terminal)
        attempts += 1
        if attempts == 1:
            raise RuntimeError("process group still alive")

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.dispatcher,
            "stop_monitor_session_process",
            new_callable=AsyncMock,
            side_effect=fail_once,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        first = await client.post(f"/api/tasks/{task_id}/cancel")
        assert first.status_code == 409

        async with session_factory() as db:
            task = await db.get(Task, task_id)
            monitor = await db.get(MonitorSession, monitor_id)
            assert task.status == "cancelled"
            assert monitor.status == "cancelled"

        second = await client.post(f"/api/tasks/{task_id}/cancel")

    assert second.status_code == 200, second.text
    assert second.json()["status"] == "cancelled"
    assert attempts == 2
    assert terminal_values == [True, True]


@pytest.mark.asyncio
async def test_stop_helper_never_uses_historical_recycled_instance(
    session_factory,
):
    """Stopping old Task A must not stop slot now owned by Task B."""
    from backend.api.tasks import _stop_task_process
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        old_task = Task(title="old", description="d", status="completed")
        new_task = Task(title="new", description="d", status="executing")
        db.add_all([old_task, new_task])
        await db.flush()
        inst = Instance(
            name="reused",
            status="running",
            current_task_id=new_task.id,
        )
        db.add(inst)
        await db.flush()
        old_task.instance_id = inst.id
        new_task.instance_id = inst.id
        await db.commit()

        with patch.object(
            backend.main.instance_manager,
            "stop",
            new_callable=AsyncMock,
            return_value=True,
        ) as stop:
            assert await _stop_task_process(
                old_task.id,
                db,
                expected_generations=[],
            ) is False
            stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_helper_rechecks_live_owner_inside_manager_lock(
    session_factory,
):
    from backend.api.tasks import _stop_task_process
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task = Task(title="live", description="d", status="executing")
        db.add(task)
        await db.flush()
        inst = Instance(
            name="owned",
            status="running",
            current_task_id=task.id,
        )
        db.add(inst)
        await db.commit()

        with patch.object(
            backend.main.instance_manager,
            "stop",
            new_callable=AsyncMock,
            return_value=True,
        ) as stop:
            assert await _stop_task_process(
                task.id,
                db,
                expected_generations=[(inst.id, None, None)],
            ) is True
            stop.assert_awaited_once_with(
                inst.id,
                expected_task_id=task.id,
                expected_pid=None,
                expected_started_at=None,
                task_status="completed",
                terminal_consumer_timeout=30.0,
                consumer_cancel_timeout=10.0,
            )


@pytest.mark.asyncio
async def test_stop_helper_reconciles_an_exact_dead_reverse_owner(
    session_factory,
):
    from backend.api.tasks import _stop_task_process
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    started_at = datetime(2026, 8, 2, 7, 36, 23)
    manager = backend.main.instance_manager
    async with session_factory() as db:
        task = Task(title="orphan owner", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dead reverse owner",
            status="running",
            pid=145_0775,
            current_task_id=task.id,
            started_at=started_at,
        )
        db.add(instance)
        await db.commit()
        task_id, instance_id = task.id, instance.id

        with (
            patch.object(
                manager,
                "stop",
                new_callable=AsyncMock,
                return_value=False,
            ) as stop,
            patch.object(
                manager,
                "reconcile_dead_reverse_task_owner",
                new_callable=AsyncMock,
                return_value=True,
            ) as reconcile,
        ):
            assert await _stop_task_process(
                task_id,
                db,
                expected_generations=[(
                    instance_id,
                    145_0775,
                    started_at,
                )],
            ) is True

    stop.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_pid=145_0775,
        expected_started_at=started_at,
        task_status="completed",
        terminal_consumer_timeout=30.0,
        consumer_cancel_timeout=10.0,
    )
    reconcile.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_pid=145_0775,
        expected_started_at=started_at,
    )


@pytest.mark.asyncio
async def test_stop_helper_passes_exact_generation_for_same_task_aba(
    session_factory,
):
    """Task id equality alone cannot authorize stopping a rapid retry."""

    from datetime import datetime
    from backend.api.tasks import _stop_task_process
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    old_started_at = datetime(2026, 3, 4, 5, 6, 7)
    new_started_at = datetime(2026, 3, 4, 5, 6, 8)
    async with session_factory() as db:
        task = Task(
            title="same task ABA",
            description="d",
            status="executing",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="same-task-reused-slot",
            status="running",
            pid=1111,
            started_at=old_started_at,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    async def reject_old_generation(
        stopped_instance_id,
        *,
        expected_task_id,
        expected_pid,
        expected_started_at,
        task_status,
        terminal_consumer_timeout,
        consumer_cancel_timeout,
    ):
        assert stopped_instance_id == instance_id
        assert expected_task_id == task_id
        assert expected_pid == 1111
        assert expected_started_at == old_started_at
        assert task_status == "completed"
        assert terminal_consumer_timeout == 30.0
        assert consumer_cancel_timeout == 10.0
        async with session_factory() as db:
            instance = await db.get(Instance, instance_id)
            instance.pid = 2222
            instance.started_at = new_started_at
            await db.commit()
        # Models the manager's lock-internal exact-generation rejection.
        return False

    async with session_factory() as db:
        with (
            patch.object(
                backend.main.instance_manager,
                "stop",
                side_effect=reject_old_generation,
            ),
            patch.object(
                backend.main.instance_manager,
                "reconcile_dead_reverse_task_owner",
                new_callable=AsyncMock,
                return_value=False,
            ) as reconcile,
        ):
            assert await _stop_task_process(
                task_id,
                db,
                expected_generations=[
                    (instance_id, 1111, old_started_at)
                ],
            ) is False
            reconcile.assert_not_awaited()

    async with session_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.current_task_id == task_id
        assert instance.pid == 2222
        assert instance.started_at == new_started_at


@pytest.mark.asyncio
async def test_cancel_stops_exact_owner_before_publishing_status(
    client, session_factory
):
    """The live owner is reaped while its Task generation is still active."""
    from backend.models.instance import Instance
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Race Test", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        instance = Instance(
            name="cancel-order-owner",
            status="running",
            pid=9911,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="executing", instance_id=instance.id)
        )
        await db.commit()
        instance_id = instance.id

    async def tracking_stop(
        tid,
        db,
        *,
        expected_generations,
        task_status,
    ):
        assert tid == task_id
        assert task_status == "cancelled"
        assert expected_generations == [(instance_id, 9911, None)]
        async with session_factory() as verify_db:
            task = await verify_db.get(Task, task_id)
            assert task.status == "executing"
            owner = await verify_db.get(Instance, instance_id)
            owner.status = "idle"
            owner.pid = None
            owner.current_task_id = None
            task.status = "cancelled"
            task.completed_at = datetime.utcnow()
            await verify_db.commit()
        return True

    with patch("backend.api.tasks._stop_task_process", side_effect=tracking_stop):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")

    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


# === Mark unread tests ===


@pytest.mark.asyncio
async def test_mark_task_unread(client, session_factory):
    """POST /api/tasks/{id}/unread sets has_unread=True."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Read task", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    assert create_resp.json()["has_unread"] is False

    resp = await client.post(f"/api/tasks/{task_id}/unread")
    assert resp.status_code == 200
    assert resp.json()["has_unread"] is True


@pytest.mark.asyncio
async def test_mark_task_unread_not_found(client):
    """POST /api/tasks/9999/unread returns 404 for missing task."""
    resp = await client.post("/api/tasks/9999/unread")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_toggle_read_unread_roundtrip(client, session_factory):
    """Can toggle between read and unread states."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Toggle", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Mark unread
    resp = await client.post(f"/api/tasks/{task_id}/unread")
    assert resp.json()["has_unread"] is True

    # Mark read
    resp = await client.post(f"/api/tasks/{task_id}/read")
    assert resp.json()["has_unread"] is False

    # Mark unread again
    resp = await client.post(f"/api/tasks/{task_id}/unread")
    assert resp.json()["has_unread"] is True


@pytest.mark.asyncio
async def test_mark_already_unread_task_unread(client, session_factory):
    """Marking an already-unread task as unread is idempotent."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Already unread", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Set unread via DB
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(has_unread=True))
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/unread")
    assert resp.status_code == 200
    assert resp.json()["has_unread"] is True


# === Starred on create tests ===


@pytest.mark.asyncio
async def test_create_task_starred_default_false(client):
    """Task created without starred flag has starred=False."""
    resp = await client.post("/api/tasks", json={
        "title": "No star", "description": "d", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["starred"] is False


@pytest.mark.asyncio
async def test_create_task_with_starred_true(client):
    """Task created with starred=True is starred immediately."""
    resp = await client.post("/api/tasks", json={
        "title": "Starred", "description": "d", "target_repo": "/tmp",
        "starred": True,
    })
    assert resp.status_code == 201
    assert resp.json()["starred"] is True


@pytest.mark.asyncio
async def test_create_task_starred_persisted_in_db(client, session_factory):
    """starred=True at creation is persisted to the database."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "Starred persist", "description": "d", "target_repo": "/tmp",
        "starred": True,
    })
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.starred is True


@pytest.mark.asyncio
async def test_create_task_starred_in_list(client):
    """Starred task appears with starred=True in list endpoint."""
    await client.post("/api/tasks", json={
        "title": "Starred list", "description": "d", "target_repo": "/tmp",
        "starred": True,
    })
    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    tasks = resp.json()
    assert any(t["starred"] is True for t in tasks)


@pytest.mark.asyncio
async def test_create_task_starred_filter(client):
    """Starred filter returns only starred tasks including those starred at creation."""
    await client.post("/api/tasks", json={
        "title": "Not starred", "description": "d", "target_repo": "/tmp",
    })
    await client.post("/api/tasks", json={
        "title": "Starred", "description": "d", "target_repo": "/tmp",
        "starred": True,
    })
    resp = await client.get("/api/tasks?starred=true")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["starred"] is True


# === has_unread filter tests ===


@pytest.mark.asyncio
async def test_filter_unread_tasks(client, session_factory):
    """has_unread=true filter returns only unread tasks."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Read task", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Unread task", "description": "d", "target_repo": "/tmp",
    })
    unread_id = r2.json()["id"]

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == unread_id).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks?has_unread=true")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == unread_id
    assert tasks[0]["has_unread"] is True


@pytest.mark.asyncio
async def test_filter_read_tasks(client, session_factory):
    """has_unread=false filter returns only read tasks."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Read task", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Unread task", "description": "d", "target_repo": "/tmp",
    })
    unread_id = r2.json()["id"]
    read_id = r1.json()["id"]

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == unread_id).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks?has_unread=false")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == read_id
    assert tasks[0]["has_unread"] is False


@pytest.mark.asyncio
async def test_count_unread_tasks(client, session_factory):
    """has_unread filter works with count endpoint."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Task A", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Task B", "description": "d", "target_repo": "/tmp",
    })
    r3 = await client.post("/api/tasks", json={
        "title": "Task C", "description": "d", "target_repo": "/tmp",
    })

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == r1.json()["id"]).values(has_unread=True))
        await db.execute(update(Task).where(Task.id == r2.json()["id"]).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks/count?has_unread=true")
    assert resp.status_code == 200
    assert resp.json()["total"] == 2

    resp = await client.get("/api/tasks/count?has_unread=false")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


@pytest.mark.asyncio
async def test_filter_unread_combined_with_status(client, session_factory):
    """has_unread filter works combined with status filter."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Pending unread", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Pending read", "description": "d", "target_repo": "/tmp",
    })

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == r1.json()["id"]).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks?has_unread=true&status=pending")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == r1.json()["id"]


@pytest.mark.asyncio
async def test_filter_unread_with_no_results(client):
    """has_unread=true returns empty list when no unread tasks exist."""
    await client.post("/api/tasks", json={
        "title": "All read", "description": "d", "target_repo": "/tmp",
    })

    resp = await client.get("/api/tasks?has_unread=true")
    assert resp.status_code == 200
    assert resp.json() == []

    resp = await client.get("/api/tasks/count?has_unread=true")
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


@pytest.mark.asyncio
async def test_no_unread_filter_returns_all(client, session_factory):
    """Without has_unread filter, both read and unread tasks are returned."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Read", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Unread", "description": "d", "target_repo": "/tmp",
    })

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == r2.json()["id"]).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


# === enable_workflows tests ===


@pytest.mark.asyncio
async def test_create_task_enable_workflows_default(client):
    """Task created without enable_workflows defaults to False."""
    resp = await client.post("/api/tasks", json={
        "title": "Default WF",
        "description": "test default",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["enable_workflows"] is False


@pytest.mark.asyncio
async def test_create_task_enable_workflows_true(client):
    """Task created with enable_workflows=True stores it correctly."""
    resp = await client.post("/api/tasks", json={
        "title": "WF Enabled",
        "description": "workflows enabled",
        "target_repo": "/tmp",
        "enable_workflows": True,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["enable_workflows"] is True


@pytest.mark.asyncio
async def test_create_task_enable_workflows_false(client):
    """Task created with enable_workflows=False stores it correctly."""
    resp = await client.post("/api/tasks", json={
        "title": "WF Disabled",
        "description": "workflows disabled",
        "target_repo": "/tmp",
        "enable_workflows": False,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["enable_workflows"] is False


@pytest.mark.asyncio
async def test_update_task_enable_workflows(client):
    """PUT /api/tasks/{id} can update enable_workflows."""
    create_resp = await client.post("/api/tasks", json={
        "title": "WF Toggle",
        "description": "toggle test",
        "target_repo": "/tmp",
        "enable_workflows": False,
    })
    task_id = create_resp.json()["id"]
    assert create_resp.json()["enable_workflows"] is False

    update_resp = await client.put(f"/api/tasks/{task_id}", json={
        "enable_workflows": True,
    })
    assert update_resp.status_code == 200
    assert update_resp.json()["enable_workflows"] is True


@pytest.mark.asyncio
async def test_create_task_enable_workflows_persisted_in_db(client, session_factory):
    """enable_workflows value is persisted in the database."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "DB Check",
        "description": "check db",
        "target_repo": "/tmp",
        "enable_workflows": True,
    })
    assert resp.status_code == 201
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.enable_workflows is True


@pytest.mark.asyncio
async def test_stop_session_clears_pending_queue(client):
    """A pending Task may clear its queue without inventing a live process."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Stop Queue", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    import backend.main
    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=2,
    ) as mock_clear, patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=True,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["stopped"] is False
    assert body["cleared_messages"] == 2
    mock_clear.assert_awaited_once()
    assert mock_clear.await_args.args == (task_id,)
    assert mock_clear.await_args.kwargs["durable_db"] is not None
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_session_closes_auxiliary_producers_before_final_queue_drain(
    client,
    session_factory,
):
    """A monitor cannot refill the queue after Interrupt drains it."""

    import backend.main
    from backend.models.monitor_session import MonitorSession

    create_resp = await client.post("/api/tasks", json={
        "title": "Stop monitor producer",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="keeps reporting",
            status="running",
        )
        db.add(monitor)
        await db.commit()
        monitor_id = monitor.id

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            side_effect=[46, 1],
        ) as abort_queue,
        patch.object(
            backend.main.dispatcher,
            "stop_monitor_session_process",
            new_callable=AsyncMock,
        ) as stop_monitor,
    ):
        response = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert response.status_code == 200
    assert response.json()["cleared_messages"] == 47
    assert abort_queue.await_count == 2
    stop_monitor.assert_awaited_once_with(monitor_id, terminal=True)
    async with session_factory() as db:
        monitor = await db.get(MonitorSession, monitor_id)
        assert monitor.status == "cancelled"
        assert monitor.next_check_at is None
        assert monitor.active_turn_generation is None


@pytest.mark.asyncio
async def test_stop_session_no_process_reports_not_stopped(client, session_factory):
    """When no process is found but task is executing, response says stopped=False."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "No Proc", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(status="executing"))
        await db.commit()

    import backend.main
    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=0,
    ), patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=False,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    body = resp.json()
    assert body["stopped"] is False
    assert "note" in body
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_session_ownerless_generation_does_not_infer_process(
    client,
    session_factory,
):
    """An ownerless active Task is terminalized without addressing a slot."""

    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Stop launch race",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="executing")
        )
        await db.commit()

    import backend.main
    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=0,
    ), patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=True,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    assert resp.json()["stopped"] is False
    mock_stop.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.instance_id is None


@pytest.mark.asyncio
async def test_stop_session_cleared_only_returns_ok(client):
    """No process and task not executing, but messages were cleared -> 200 not 400."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Cleared Only", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    import backend.main
    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=1,
    ), patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=False,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    assert resp.json()["stopped"] is False
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_order_starred_then_access_then_manual(client, session_factory):
    """排序：标星置顶 → 手动 sort_order / 最近访问时间（越新越靠前）。"""
    from datetime import datetime, timedelta
    from backend.models.task import Task

    ids = []
    for i in range(4):
        resp = await client.post("/api/tasks", json={
            "title": f"T{i}", "description": "d", "target_repo": "/tmp",
        })
        ids.append(resp.json()["id"])

    now = datetime.utcnow()
    async with session_factory() as db:
        a, b, c, d = [await db.get(Task, i) for i in ids]
        a.last_accessed_at = now - timedelta(hours=3)
        b.last_accessed_at = now - timedelta(hours=1)   # 最近访问
        c.last_accessed_at = now - timedelta(hours=2)
        c.starred = True                                 # 标星 → 置顶
        d.last_accessed_at = now - timedelta(hours=4)
        d.sort_order = now.timestamp() + 999             # 手动拖到最前（非星组）
        await db.commit()

    resp = await client.get("/api/tasks?limit=50")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    # c 标星置顶；非星组按位置键：d 手动键最大 → b（访问较近）→ a
    assert order == [ids[2], ids[3], ids[1], ids[0]]


@pytest.mark.asyncio
async def test_chat_history_touches_last_accessed(client, session_factory):
    """打开 chat（拉历史，touch=true）应更新 last_accessed_at；
    不带 touch 的拉取（分页/后台轮询/旧版客户端）不得更新——
    生产实录：旧版前端残留标签页轮询导致任务在列表里来回跳。"""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = resp.json()["id"]
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.last_accessed_at is None

    # 不带 touch：不更新（回归：每次 history 拉取都 touch 会被轮询滥用）
    await client.get(f"/api/tasks/{task_id}/chat/history")
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.last_accessed_at is None

    await client.get(f"/api/tasks/{task_id}/chat/history?touch=true")
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.last_accessed_at is not None


@pytest.mark.asyncio
async def test_open_chat_moves_task_to_front_of_group(client, session_factory):
    """Touch updates last_accessed_at; for tasks without sort_order,
    the query sorts by last_accessed_at (auto_sort_on_access=True default),
    so the most recently accessed task appears first."""
    from datetime import datetime, timedelta
    from backend.models.task import Task

    now = datetime.utcnow()
    ids = []
    for i in range(3):
        resp = await client.post("/api/tasks", json={
            "title": f"T{i}", "description": "d", "target_repo": "/tmp",
        })
        ids.append(resp.json()["id"])

    # Set distinct created_at timestamps with 10s gaps to avoid strftime("%s") collisions
    async with session_factory() as db:
        for i, tid in enumerate(ids):
            t = await db.get(Task, tid)
            t.created_at = now - timedelta(seconds=30 - i * 10)  # t0 oldest, t2 newest
            t.sort_order = None
            t.last_accessed_at = None
        await db.commit()

    # Before touch: t2 (newest created_at) should be first
    resp = await client.get("/api/tasks?limit=50")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    assert order[0] == ids[2]

    # Touch t0 → t0's last_accessed_at becomes now → should sort first
    await client.get(f"/api/tasks/{ids[0]}/chat/history?touch=true")
    resp = await client.get("/api/tasks?limit=50")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    assert order[0] == ids[0]


@pytest.mark.asyncio
async def test_update_sort_order_via_api_moves_task(client):
    """回归：sort_order 曾只加在 TaskCreate 上，PUT 被 pydantic 丢弃 →
    前端拖拽永远不生效。必须走 API 全链路验证。"""
    ids = []
    for i in range(3):
        resp = await client.post("/api/tasks", json={
            "title": f"T{i}", "description": "d", "target_repo": "/tmp",
        })
        ids.append(resp.json()["id"])

    # 默认按创建时间倒序：[t2, t1, t0]；把 t0 拖到第一
    resp = await client.get("/api/tasks?limit=10")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    assert order == [ids[2], ids[1], ids[0]]

    import time
    resp = await client.put(f"/api/tasks/{ids[0]}", json={"sort_order": time.time() + 9999})
    assert resp.status_code == 200
    assert resp.json()["sort_order"] is not None

    resp = await client.get("/api/tasks?limit=10")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    assert order == [ids[0], ids[2], ids[1]]


# === status_change 广播收口（2026-07 状态显示大排查）===
# API 侧改 Task.status 的路径必须广播 status_change，否则 ChatView（WS 驱动）
# 与任务列表（轮询驱动）状态分叉。


@pytest.mark.asyncio
async def test_cancel_task_broadcasts_status_change(client):
    create = await client.post("/api/tasks", json={"description": "to cancel"})
    task_id = create.json()["id"]

    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()
    with patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 200

    payloads = [
        c.args[1] for c in mock_broadcaster.broadcast.await_args_list
        if c.args[1].get("event") == "status_change"
    ]
    assert any(p["task_id"] == task_id and p["new_status"] == "cancelled" for p in payloads)
