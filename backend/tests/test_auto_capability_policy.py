"""Strict admission contract for model-requested Task capabilities."""

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from backend.config import settings
from backend.models.task import Task
from backend.schemas.capability import AutoCapabilityPolicy
from backend.schemas.task import TaskCreate, TaskMigrationImport, TaskUpdate
from backend.services.auto_capability_policy import (
    build_auto_capability_instructions,
    normalize_auto_capability_policy,
)


VALID_POLICY = {
    "version": 1,
    "max_invocations": 2,
    "capabilities": {
        "plan": 1,
        "code_review": 1,
    },
}


def test_model_protocol_requires_both_rollout_switches(monkeypatch):
    task = Task(
        title="protocol",
        description="d",
        mode="auto",
        capability_policy=VALID_POLICY,
    )

    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(settings, "auto_capability_enabled", False)
    assert build_auto_capability_instructions(task) is None

    monkeypatch.setattr(settings, "capability_core_enabled", False)
    monkeypatch.setattr(settings, "auto_capability_enabled", True)
    assert build_auto_capability_instructions(task) is None

    monkeypatch.setattr(settings, "capability_core_enabled", True)
    instructions = build_auto_capability_instructions(task)
    assert instructions is not None
    assert '"code_review"' in instructions
    assert '"plan"' in instructions
    assert instructions.count("<ccm_terminal_action>") == 1
    assert 'For "plan"' in instructions
    assert 'non-empty string "prompt"' in instructions
    assert 'For "code_review"' in instructions
    assert 'exactly string "base_sha" and "head_sha"' in instructions
    assert "full immutable Git commit IDs" in instructions
    assert "40-character lowercase hexadecimal SHA" in instructions


def test_model_protocol_only_describes_enabled_request_contracts(monkeypatch):
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(settings, "auto_capability_enabled", True)
    task = Task(
        title="plan only",
        description="d",
        mode="auto",
        capability_policy={
            "version": 1,
            "max_invocations": 1,
            "capabilities": {"plan": 1},
        },
    )

    instructions = build_auto_capability_instructions(task)

    assert instructions is not None
    assert 'For "plan"' in instructions
    assert 'For "code_review"' not in instructions


def test_model_protocol_revalidates_persisted_task_scope(monkeypatch):
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(settings, "auto_capability_enabled", True)
    task = Task(
        title="invalid persisted scope",
        description="d",
        mode="goal",
        capability_policy=VALID_POLICY,
    )

    with pytest.raises(ValueError, match="mode=auto"):
        build_auto_capability_instructions(task)


def test_policy_normalizes_capability_order_without_changing_budgets():
    policy = AutoCapabilityPolicy.model_validate({
        "version": 1,
        "max_invocations": 2,
        "capabilities": {
            "code_review": 2,
            "plan": 2,
        },
    })

    assert policy.model_dump(mode="json") == {
        "version": 1,
        "max_invocations": 2,
        "capabilities": {
            "plan": 2,
            "code_review": 2,
        },
    }


def test_normalizer_revalidates_mutated_policy_instance():
    policy = AutoCapabilityPolicy.model_validate(VALID_POLICY)
    policy.max_invocations = 99
    policy.capabilities["shell"] = 99

    with pytest.raises(ValueError, match="Invalid capability_policy"):
        normalize_auto_capability_policy(policy)


def test_normalizer_revalidates_model_construct_and_detaches_result():
    constructed = AutoCapabilityPolicy.model_construct(
        version=1,
        max_invocations=1,
        capabilities={"shell": 1},
    )
    with pytest.raises(ValueError, match="Invalid capability_policy"):
        normalize_auto_capability_policy(constructed)

    source = {
        "version": 1,
        "max_invocations": 2,
        "capabilities": {"code_review": 1, "plan": 1},
    }
    normalized = normalize_auto_capability_policy(source)
    source["capabilities"]["plan"] = 8

    assert normalized == VALID_POLICY


@pytest.mark.parametrize(
    "policy",
    [
        {},
        {"version": 2, "max_invocations": 1, "capabilities": {"plan": 1}},
        {"version": True, "max_invocations": 1, "capabilities": {"plan": 1}},
        {"version": 1, "max_invocations": True, "capabilities": {"plan": 1}},
        {"version": 1, "max_invocations": 0, "capabilities": {"plan": 1}},
        {"version": 1, "max_invocations": 9, "capabilities": {"plan": 9}},
        {"version": 1, "max_invocations": 1, "capabilities": {}},
        {"version": 1, "max_invocations": 1, "capabilities": {"shell": 1}},
        {"version": 1, "max_invocations": 1, "capabilities": {"plan": True}},
        {"version": 1, "max_invocations": 1, "capabilities": {"plan": 0}},
        {"version": 1, "max_invocations": 2, "capabilities": {"plan": 1}},
        {"version": 1, "max_invocations": 1, "capabilities": {"plan": 2}},
        {
            "version": 1,
            "max_invocations": 1,
            "capabilities": {"plan": 1},
            "used": 0,
        },
    ],
)
def test_policy_rejects_ambiguous_or_unbounded_shapes(policy):
    with pytest.raises(ValidationError):
        AutoCapabilityPolicy.model_validate(policy)


def test_task_policy_is_create_only_and_local_auto_only():
    created = TaskCreate(description="d", capability_policy=VALID_POLICY)
    assert created.capability_policy is not None

    with pytest.raises(ValidationError, match="mode=auto"):
        TaskCreate(
            description="d",
            mode="goal",
            goal_condition="done",
            capability_policy=VALID_POLICY,
        )
    with pytest.raises(ValidationError, match="local"):
        TaskCreate(
            description="d",
            worker_id=7,
            capability_policy=VALID_POLICY,
        )
    with pytest.raises(ValidationError, match="Manager-forwarded"):
        TaskCreate(
            id=99,
            description="d",
            capability_policy=VALID_POLICY,
        )
    with pytest.raises(ValidationError, match="immutable"):
        TaskUpdate(capability_policy=VALID_POLICY)
    with pytest.raises(ValidationError, match="immutable"):
        TaskUpdate(capability_policy=None)


def test_migration_import_cannot_carry_auto_capability_policy():
    with pytest.raises(ValidationError):
        TaskMigrationImport(
            id=7,
            description="d",
            capability_policy=VALID_POLICY,
        )


@pytest.mark.asyncio
async def test_create_task_persists_frozen_policy_and_returns_it(
    client,
    session_factory,
):
    response = await client.post("/api/tasks", json={
        "title": "Policy task",
        "description": "d",
        "capability_policy": VALID_POLICY,
    })

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["capability_policy"] == VALID_POLICY
    async with session_factory() as db:
        stored = await db.scalar(
            text("SELECT capability_policy FROM tasks WHERE id = :task_id"),
            {"task_id": body["id"]},
        )
        assert stored is not None


@pytest.mark.asyncio
async def test_default_policy_is_real_sql_null(client, session_factory):
    response = await client.post("/api/tasks", json={
        "title": "Default-off task",
        "description": "d",
    })

    assert response.status_code == 201, response.text
    assert response.json()["capability_policy"] is None
    async with session_factory() as db:
        is_sql_null = await db.scalar(
            text(
                "SELECT capability_policy IS NULL FROM tasks "
                "WHERE id = :task_id"
            ),
            {"task_id": response.json()["id"]},
        )
        assert is_sql_null == 1


@pytest.mark.asyncio
async def test_project_worker_resolution_rejects_policy_before_task_write(
    client,
    session_factory,
):
    from backend.models.project import Project
    from backend.models.task import Task
    from backend.models.worker import Worker
    from sqlalchemy import func, select

    async with session_factory() as db:
        worker = Worker(
            name="remote-policy-worker",
            status="ready",
            private_ip="10.0.0.91",
            auth_token="token",
        )
        db.add(worker)
        await db.flush()
        project = Project(
            name="remote-policy-project",
            local_path="/srv/remote-policy-project",
            worker_id=worker.id,
        )
        db.add(project)
        await db.commit()
        project_id = project.id

    response = await client.post("/api/tasks", json={
        "title": "Must remain local",
        "description": "d",
        "project_id": project_id,
        "capability_policy": VALID_POLICY,
    })

    assert response.status_code == 422, response.text
    assert "local" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_task_put_rejects_policy_instead_of_silently_ignoring_it(client):
    created = await client.post("/api/tasks", json={
        "title": "Immutable policy",
        "description": "d",
    })
    task_id = created.json()["id"]

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"capability_policy": VALID_POLICY},
    )

    assert response.status_code == 422, response.text
    assert "immutable" in response.text


@pytest.mark.asyncio
async def test_task_put_cannot_move_frozen_policy_outside_auto_scope(
    client,
    session_factory,
):
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Frozen Auto scope",
        "description": "d",
        "capability_policy": VALID_POLICY,
    })
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]

    response = await client.put(f"/api/tasks/{task_id}", json={
        "mode": "goal",
        "goal_condition": "done",
    })

    assert response.status_code == 422, response.text
    assert "mode is immutable" in response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.mode == "auto"
        assert task.goal_condition is None
        assert task.capability_policy == VALID_POLICY


@pytest.mark.asyncio
async def test_clone_requires_explicit_policy_opt_in(client):
    source = await client.post("/api/tasks", json={
        "title": "Policy source",
        "description": "d",
        "capability_policy": VALID_POLICY,
    })
    assert source.status_code == 201, source.text

    cloned = await client.post("/api/tasks", json={
        "title": "Policy clone",
        "description": "d",
        "clone_from_task_id": source.json()["id"],
    })

    assert cloned.status_code == 201, cloned.text
    assert cloned.json()["capability_policy"] is None


@pytest.mark.asyncio
async def test_migration_import_cannot_repurpose_existing_policy_task(
    client,
    session_factory,
    monkeypatch,
    worker_control_plane_auth,
):
    from backend.config import settings
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Local policy authority",
        "description": "local task",
        "capability_policy": VALID_POLICY,
    })
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        source_incarnation_id = task.incarnation_id
    monkeypatch.setattr(settings, "ccm_node_role", "worker")

    response = await client.post("/api/tasks/migration-import", json={
        "id": task_id,
        "source_incarnation_id": source_incarnation_id,
        "migration_operation_id": "a" * 32,
        "migration_operation_sequence": 1,
        "execution_user_id": None,
        "execution_user_role": "member",
        "execution_mode": "sandbox",
        "execution_principal_kind": "system",
        "title": "Remote mirror collision",
        "description": "manager task",
        "source_status": "cancelled",
    })

    assert response.status_code == 409, response.text
    assert "immutable local Auto capability policy" in response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.title == "Local policy authority"
        assert task.description == "local task"
        assert task.status == "pending"
        assert task.capability_policy == VALID_POLICY
        assert task.metadata_ is None


def test_canonical_task_creation_rejects_shared_or_malformed_policy():
    from backend.services.task_creation import prepare_task_create_values

    with pytest.raises(ValueError, match="Shared shadow"):
        prepare_task_create_values({
            "description": "d",
            "mode": "auto",
            "shared_from_id": 3,
            "capability_policy": VALID_POLICY,
        })
    with pytest.raises(ValueError, match="Invalid capability_policy"):
        prepare_task_create_values({
            "description": "d",
            "mode": "auto",
            "capability_policy": {},
        })
    with pytest.raises(ValueError, match="Manager-forwarded"):
        prepare_task_create_values({
            "id": 99,
            "description": "d",
            "mode": "auto",
            "capability_policy": VALID_POLICY,
        })
