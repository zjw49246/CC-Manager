"""Tests for project todo API endpoints."""

import pytest
import pytest_asyncio
from sqlalchemy import func, select

from backend.models.delivery import DeliveryRun
from backend.models.project import Project
from backend.models.project_todo import ProjectTodo
from backend.models.task import Task
from backend.tests.group_acl_test_helpers import (
    grant_group_project_access,
    revoke_group_membership_at_effect_fence,
)
from backend.tests.test_auth_ws_security import (
    _create_user,
    secured_client as secured_client,
)


@pytest_asyncio.fixture
async def project_id(session_factory):
    async with session_factory() as session:
        project = Project(name="todo-proj", has_remote=False, status="ready")
        session.add(project)
        await session.commit()
        await session.refresh(project)
        return project.id


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "update", "delete", "task"])
async def test_todo_effect_reauthorizes_after_project_fence(
    secured_client,
    monkeypatch,
    operation,
):
    """A group revocation that wins the final fence vetoes every Todo write."""

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email=f"todo-effect-{operation}@example.com",
        role="member",
    )
    async with session_factory() as session:
        project = Project(
            name=f"todo-effect-{operation}",
            has_remote=False,
            status="ready",
        )
        session.add(project)
        await session.commit()
        project_id = project.id
    await grant_group_project_access(
        session_factory,
        project_id=project_id,
        user_id=member_id,
    )

    todo_id = None
    if operation != "create":
        async with session_factory() as session:
            todo = ProjectTodo(
                project_id=project_id,
                title="authority race",
                prompt="must remain unchanged",
                status="open",
                sort_order=100,
            )
            session.add(todo)
            await session.commit()
            todo_id = todo.id

    fence = revoke_group_membership_at_effect_fence(monkeypatch)
    headers = {"Authorization": f"Bearer {member_token}"}

    if operation == "create":
        response = await client.post(
            f"/api/projects/{project_id}/todos",
            headers=headers,
            json={"title": "denied", "prompt": "must not be created"},
        )
    elif operation == "update":
        response = await client.patch(
            f"/api/projects/{project_id}/todos/{todo_id}",
            headers=headers,
            json={"title": "mutated"},
        )
    elif operation == "delete":
        response = await client.delete(
            f"/api/projects/{project_id}/todos/{todo_id}",
            headers=headers,
        )
    else:
        response = await client.post(
            f"/api/projects/{project_id}/todos/{todo_id}/task",
            headers=headers,
            json={"title": "denied", "prompt": "must not materialize"},
        )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 1, "revoked": True}
    async with session_factory() as session:
        todos = list((await session.execute(
            select(ProjectTodo).where(ProjectTodo.project_id == project_id)
        )).scalars())
        tasks = list((await session.execute(select(Task))).scalars())
    if operation == "create":
        assert todos == []
    else:
        assert len(todos) == 1
        assert todos[0].title == "authority race"
        assert todos[0].status == "open"
    assert tasks == []


@pytest.mark.asyncio
async def test_project_todo_lifecycle(client, project_id):
    resp = await client.get(f"/api/projects/{project_id}/todos")
    assert resp.status_code == 200
    assert resp.json() == []

    resp = await client.post(
        f"/api/projects/{project_id}/todos",
        json={"title": "  Refactor auth  ", "prompt": "  Inspect auth module first.  "},
    )
    assert resp.status_code == 201
    todo = resp.json()
    assert todo["title"] == "Refactor auth"
    assert todo["prompt"] == "Inspect auth module first."
    assert todo["status"] == "open"
    assert todo["sort_order"] == 100

    resp = await client.patch(
        f"/api/projects/{project_id}/todos/{todo['id']}",
        json={"title": "Refactor auth plan", "prompt": "Write a plan.", "status": "done"},
    )
    assert resp.status_code == 200
    updated = resp.json()
    assert updated["title"] == "Refactor auth plan"
    assert updated["prompt"] == "Write a plan."
    assert updated["status"] == "done"
    assert updated["created_task_id"] is None

    # Archiving is a soft hide via PATCH (not DELETE).
    resp = await client.patch(
        f"/api/projects/{project_id}/todos/{todo['id']}",
        json={"status": "archived"},
    )
    assert resp.status_code == 200

    resp = await client.get(f"/api/projects/{project_id}/todos")
    assert resp.status_code == 200
    assert resp.json() == []  # archived hidden by default

    resp = await client.get(f"/api/projects/{project_id}/todos?include_archived=true")
    assert resp.status_code == 200
    archived = resp.json()
    assert len(archived) == 1
    assert archived[0]["status"] == "archived"

    # DELETE permanently removes the todo, even from the archived view.
    resp = await client.delete(f"/api/projects/{project_id}/todos/{todo['id']}")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    resp = await client.get(f"/api/projects/{project_id}/todos?include_archived=true")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_project_todo_rejects_blank_fields(client, project_id):
    resp = await client.post(
        f"/api/projects/{project_id}/todos",
        json={"title": "   ", "prompt": "Do work"},
    )
    assert resp.status_code == 400

    resp = await client.post(
        f"/api/projects/{project_id}/todos",
        json={"title": "Do work", "prompt": "   "},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_project_todos_require_existing_project(client):
    resp = await client.get("/api/projects/9999/todos")
    assert resp.status_code == 404

    resp = await client.post(
        "/api/projects/9999/todos",
        json={"title": "Missing", "prompt": "Missing project"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_project_removes_project_todos(client, project_id, session_factory):
    resp = await client.post(
        f"/api/projects/{project_id}/todos",
        json={"title": "Clean up", "prompt": "Remove with project"},
    )
    assert resp.status_code == 201

    resp = await client.delete(f"/api/projects/{project_id}")
    assert resp.status_code == 200

    async with session_factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(ProjectTodo).where(ProjectTodo.project_id == project_id)
        )
    assert count == 0


@pytest.mark.asyncio
async def test_todo_task_admission_claims_task_and_provenance_atomically(
    client,
    project_id,
    session_factory,
):
    created = await client.post(
        f"/api/projects/{project_id}/todos",
        json={"title": "Atomic task", "prompt": "Create exactly one task."},
    )
    todo_id = created.json()["id"]

    response = await client.post(
        f"/api/projects/{project_id}/todos/{todo_id}/task",
        json={
            "title": "Atomic task",
            "prompt": "Create exactly one task.",
            "provider": "codex",
        },
    )

    assert response.status_code == 201, response.text
    assert response.json()["mode"] == "auto"
    task_id = response.json()["id"]
    async with session_factory() as session:
        todo = await session.get(ProjectTodo, todo_id)
        task = await session.get(Task, task_id)
        assert todo is not None
        assert task is not None
        assert todo.status == "done"
        assert todo.created_task_id == task_id
        assert todo.task_request_hash == (
            task.metadata_["project_todo_task_admission"]["request_hash"]
        )
        assert (
            task.metadata_["project_todo_task_admission"]["todo_id"]
            == todo_id
        )
        assert await session.scalar(select(func.count(Task.id))) == 1

    replay = await client.post(
        f"/api/projects/{project_id}/todos/{todo_id}/task",
        json={
            "title": "Atomic task",
            "prompt": "Create exactly one task.",
            "provider": "codex",
        },
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["id"] == task_id

    duplicate = await client.post(
        f"/api/projects/{project_id}/todos/{todo_id}/task",
        json={"title": "Duplicate", "prompt": "Must roll back."},
    )
    assert duplicate.status_code == 409, duplicate.text
    async with session_factory() as session:
        assert await session.scalar(select(func.count(Task.id))) == 1


@pytest.mark.asyncio
async def test_todo_task_provenance_cannot_be_cleared_to_duplicate_admission(
    client,
    project_id,
    session_factory,
):
    created = await client.post(
        f"/api/projects/{project_id}/todos",
        json={"title": "Single use", "prompt": "Create one Task only."},
    )
    todo_id = created.json()["id"]
    request = {
        "title": "Single use",
        "prompt": "Create one Task only.",
        "provider": "codex",
    }
    first = await client.post(
        f"/api/projects/{project_id}/todos/{todo_id}/task",
        json=request,
    )
    assert first.status_code == 201, first.text

    forged_reset = await client.patch(
        f"/api/projects/{project_id}/todos/{todo_id}",
        json={"status": "open", "created_task_id": None},
    )
    assert forged_reset.status_code == 422

    # Even a legitimate status-only restore cannot release the immutable
    # provenance slot.  Same intent replays; changed intent conflicts.
    restored = await client.patch(
        f"/api/projects/{project_id}/todos/{todo_id}",
        json={"status": "open"},
    )
    assert restored.status_code == 200
    replay = await client.post(
        f"/api/projects/{project_id}/todos/{todo_id}/task",
        json=request,
    )
    changed = await client.post(
        f"/api/projects/{project_id}/todos/{todo_id}/task",
        json={"title": "Second", "prompt": "Must not execute."},
    )
    assert replay.status_code == 200
    assert replay.json()["id"] == first.json()["id"]
    assert changed.status_code == 409

    async with session_factory() as session:
        todo = await session.get(ProjectTodo, todo_id)
        assert todo.created_task_id == first.json()["id"]
        assert todo.task_request_hash is not None
        assert await session.scalar(select(func.count(Task.id))) == 1


@pytest.mark.asyncio
async def test_delivery_owned_todo_rejects_patch_delete_and_ordinary_task(
    client,
    project_id,
    session_factory,
):
    created = await client.post(
        f"/api/projects/{project_id}/todos",
        json={"title": "Owned", "prompt": "Delivery owns this source."},
    )
    todo_id = created.json()["id"]
    async with session_factory() as session:
        owner = DeliveryRun(
            admission_scope="system",
            idempotency_key="todo-owner-test",
            request_hash="a" * 64,
            project_id=project_id,
            source_todo_id=todo_id,
            title="Owned",
            requirements="Delivery owns this source.",
            requirements_hash="b" * 64,
            policy_snapshot={"terminal": "ready_to_merge"},
            policy_hash="c" * 64,
            base_branch="main",
            delivery_branch=f"ccm/delivery/todo-{todo_id}",
            phase="planning",
            activity="ready",
        )
        session.add(owner)
        await session.commit()
        owner_id = owner.id

    patched = await client.patch(
        f"/api/projects/{project_id}/todos/{todo_id}",
        json={"title": "Overwritten"},
    )
    deleted = await client.delete(
        f"/api/projects/{project_id}/todos/{todo_id}"
    )
    ordinary = await client.post(
        f"/api/projects/{project_id}/todos/{todo_id}/task",
        json={"title": "Overwritten", "prompt": "Must not create."},
    )

    assert [patched.status_code, deleted.status_code, ordinary.status_code] == [
        409,
        409,
        409,
    ]
    assert all(
        f"Delivery Run {owner_id}" in response.text
        for response in (patched, deleted, ordinary)
    )
    async with session_factory() as session:
        todo = await session.get(ProjectTodo, todo_id)
        assert todo is not None
        assert todo.title == "Owned"
        assert await session.scalar(select(func.count(Task.id))) == 0
