"""Mounted Task API response-projection regressions."""

import pytest

from backend.config import settings
from backend.models.project import Project
from backend.models.task import Task
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.user_group import UserGroup, UserGroupMember
from backend.services.internal_service_auth import issue_internal_service_token
from backend.tests.test_auth_ws_security import _create_user, secured_client


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


_INTERNAL_TOP_LEVEL = {
    "incarnation_id",
    "execution_user_id",
    "execution_user_role",
    "execution_mode",
    "execution_principal_kind",
    "instance_id",
    "plan_approved_by",
    "plan_applied_to_session_id",
    "session_id",
}
_INTERNAL_METADATA = {
    "file_paths",
    "image_paths",
    "secret_ids",
    "codex_account_id",
    "claude_account_id",
    "ccm_user_skill_snapshots",
    "ccm_worker_managed_task",
    "frontend_review_activation",
    "task_control_effect_receipt",
    "worker_migration_receipt",
}
_UPLOAD_BASENAME = "11111111-1111-4111-8111-111111111111.txt"
_UPLOAD_URL = f"/api/uploads/{_UPLOAD_BASENAME}"


def _assert_public_task(payload: dict) -> None:
    assert payload["access_scope"] == "control"
    assert payload["has_session"] is True
    assert payload["is_worker_managed"] is True
    assert not (_INTERNAL_TOP_LEVEL & payload.keys())
    assert not (_INTERNAL_METADATA & payload["metadata_"].keys())
    assert payload["metadata_"]["attachments"] == [
        {
            "url": _UPLOAD_URL,
            "name": "public.txt",
            "is_image": False,
        }
    ]
    assert payload["metadata_"]["frontend_review"] == {
        "mode": "goal",
        "profile": "standard",
        "max_iterations": 2,
    }
    assert "host_path" not in payload["metadata_"]["frontend_review"]
    assert "forked_from_task_id" not in payload["metadata_"]
    assert "plan_review_feedback" not in payload["metadata_"]


def _assert_chat_task(payload: dict) -> None:
    assert payload["access_scope"] == "chat"
    assert payload["has_session"] is True
    assert payload["is_worker_managed"] is True
    assert not (_INTERNAL_TOP_LEVEL & payload.keys())
    assert not (_INTERNAL_METADATA & payload["metadata_"].keys())
    for key in (
        "session_id",
        "worker_id",
        "created_by",
        "instance_id",
        "target_repo",
        "todo_file_path",
        "plan_approved_by",
        "plan_applied_to_session_id",
        "enabled_skills",
        "selected_user_skills",
        "error_message",
    ):
        assert key not in payload
    for key in ("fork_seed_message", "fork_seed_log_id", "fork_seed_uploads"):
        assert key not in (payload["metadata_"] or {})


@pytest.mark.asyncio
async def test_human_task_projection_follows_exact_acl_scope(secured_client):
    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="projection-owner@example.com",
        role="member",
    )
    project_user_id, project_user_token = await _create_user(
        session_factory,
        email="projection-project-user@example.com",
        role="member",
    )
    project_group_user_id, project_group_user_token = await _create_user(
        session_factory,
        email="projection-project-group@example.com",
        role="member",
    )
    task_user_id, task_user_token = await _create_user(
        session_factory,
        email="projection-task-user@example.com",
        role="member",
    )
    task_group_user_id, task_group_user_token = await _create_user(
        session_factory,
        email="projection-task-group@example.com",
        role="member",
    )
    no_access_id, no_access_token = await _create_user(
        session_factory,
        email="projection-no-access@example.com",
        role="member",
    )
    _admin_id, admin_token = await _create_user(
        session_factory,
        email="projection-admin@example.com",
        role="admin",
    )

    async with session_factory() as db:
        project = Project(
            name="response-projection-project",
            local_path="/srv/private/repository",
            default_branch="main",
            status="ready",
        )
        project_group = UserGroup(
            name="response-projection-project-group",
            created_by=owner_id,
        )
        task_group = UserGroup(
            name="response-projection-task-group",
            created_by=owner_id,
        )
        db.add_all([project, project_group, task_group])
        await db.flush()
        task = Task(
            title="projection target",
            description="shared conversation",
            status="completed",
            project_id=project.id,
            target_repo=project.local_path,
            instance_id=876,
            session_id="native-session-must-not-reach-chat-share",
            created_by=owner_id,
            execution_user_id=owner_id,
            execution_user_role="member",
            execution_mode="sandbox",
            execution_principal_kind="user",
            enabled_skills={"code-review": True},
            selected_user_skills=[17],
            metadata_={
                "attachments": [
                    {
                        "url": _UPLOAD_URL,
                        "name": "public.txt",
                        "is_image": False,
                        "absolute_path": "/private/upload/public.txt",
                    },
                    {
                        "url": "/srv/private/repository/.env",
                        "name": "/srv/private/repository/.env",
                        "is_image": False,
                    },
                ],
                "frontend_review": {
                    "mode": "goal",
                    "profile": "standard",
                    "max_iterations": 2,
                    "host_path": "/srv/private/repository",
                },
                "forked_from_task_id": {
                    "host_path": "/srv/private/repository"
                },
                "plan_review_feedback": {"secret": "not a string"},
                "fork_seed_message": "owner-only unsent draft",
                "fork_seed_log_id": 999,
                "fork_seed_uploads": [
                    {
                        "id": "fork-seed-0",
                        "filename": "public.txt",
                        "path": "/srv/private/upload/public.txt",
                        "url": _UPLOAD_URL,
                        "is_image": False,
                    }
                ],
                "file_paths": ["/private/upload/public.txt"],
                "image_paths": ["/private/upload/screenshot.png"],
                "secret_ids": [44],
                "codex_account_id": "codex-secret-account",
                "claude_account_id": "claude-secret-account",
                "ccm_user_skill_snapshots": [
                    {"id": 17, "name": "private", "content": "secret"}
                ],
                "ccm_worker_managed_task": True,
                "frontend_review_activation": {
                    "message": "private restore prompt",
                    "restore": {"mode": "auto"},
                },
                "task_control_effect_receipt": {"nonce": "private"},
                "worker_migration_receipt": {"digest": "private"},
            },
        )
        db.add(task)
        await db.flush()
        task_id = task.id
        related_plan = Task(
            title="projection related plan",
            description="plan history inherits the target ACL",
            status="plan_review",
            mode="plan",
            project_id=project.id,
            target_repo=project.local_path,
            plan_target_task_id=task_id,
            session_id="related-plan-native-session",
            created_by=owner_id,
            execution_user_id=owner_id,
            execution_user_role="member",
            execution_mode="sandbox",
            execution_principal_kind="user",
            metadata_={"codex_account_id": "related-plan-secret"},
        )
        db.add(related_plan)
        await db.flush()
        related_plan_id = related_plan.id
        db.add_all(
            [
                TeamProjectShare(
                    project_id=project.id,
                    target_type="user",
                    target_id=project_user_id,
                    shared_by=owner_id,
                ),
                UserGroupMember(
                    group_id=project_group.id,
                    user_id=project_group_user_id,
                ),
                TeamProjectShare(
                    project_id=project.id,
                    target_type="group",
                    target_id=project_group.id,
                    shared_by=owner_id,
                ),
                TeamTaskShare(
                    task_id=task_id,
                    target_type="user",
                    target_id=task_user_id,
                    permission="chat",
                    shared_by=owner_id,
                ),
                UserGroupMember(
                    group_id=task_group.id,
                    user_id=task_group_user_id,
                ),
                TeamTaskShare(
                    task_id=task_id,
                    target_type="group",
                    target_id=task_group.id,
                    permission="chat",
                    shared_by=owner_id,
                ),
            ]
        )
        await db.commit()

    for token in (
        owner_token,
        project_user_token,
        project_group_user_token,
        admin_token,
    ):
        response = await client.get(
            f"/api/tasks/{task_id}",
            headers=_headers(token),
        )
        assert response.status_code == 200, response.text
        _assert_public_task(response.json())

    for token in (task_user_token, task_group_user_token):
        response = await client.get(
            f"/api/tasks/{task_id}",
            headers=_headers(token),
        )
        assert response.status_code == 200, response.text
        _assert_chat_task(response.json())

        listed = await client.get("/api/tasks", headers=_headers(token))
        assert listed.status_code == 200, listed.text
        listed_task = next(item for item in listed.json() if item["id"] == task_id)
        _assert_chat_task(listed_task)

        related = await client.get(
            f"/api/tasks/{task_id}/plans",
            headers=_headers(token),
        )
        assert related.status_code == 200, related.text
        related_payload = next(
            item for item in related.json() if item["id"] == related_plan_id
        )
        assert related_payload["access_scope"] == "chat"
        assert related_payload["has_session"] is True
        assert "session_id" not in related_payload
        assert "codex_account_id" not in (related_payload["metadata_"] or {})

    denied = await client.get(
        f"/api/tasks/{task_id}",
        headers=_headers(no_access_token),
    )
    assert denied.status_code == 403
    denied_list = await client.get(
        "/api/tasks",
        headers=_headers(no_access_token),
    )
    assert denied_list.status_code == 200
    assert all(item["id"] != task_id for item in denied_list.json())
    assert no_access_id > 0  # Keep the identity explicit in this matrix.


@pytest.mark.asyncio
async def test_manager_deployment_token_uses_public_task_projection(
    secured_client,
):
    client, session_factory = secured_client
    owner_id, _owner_token = await _create_user(
        session_factory,
        email="projection-deployment-token-owner@example.com",
        role="admin",
    )
    incarnation_id = "b" * 32
    async with session_factory() as db:
        task = Task(
            title="deployment token public projection",
            description="human control-plane response",
            status="completed",
            incarnation_id=incarnation_id,
            session_id="deployment-token-native-session",
            created_by=owner_id,
            execution_user_id=owner_id,
            execution_user_role="admin",
            execution_mode="unrestricted",
            execution_principal_kind="user",
            metadata_={
                "attachments": [
                    {
                        "url": _UPLOAD_URL,
                        "name": "public.txt",
                        "is_image": False,
                        "absolute_path": "/private/upload/public.txt",
                    }
                ],
                "frontend_review": {
                    "mode": "goal",
                    "profile": "standard",
                    "max_iterations": 2,
                    "host_path": "/srv/private/repository",
                },
                "ccm_worker_managed_task": True,
                "codex_account_id": "private-codex-account",
                "ccm_user_skill_snapshots": [
                    {
                        "id": 17,
                        "name": "private-skill",
                        "content": "private instructions",
                    }
                ],
                "worker_migration_receipt": {"nonce": "private"},
            },
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    headers = _headers("security-service-token")
    identity = await client.get("/api/auth/me", headers=headers)
    assert identity.status_code == 200, identity.text
    assert identity.json()["auth_type"] == "token"

    response = await client.get(f"/api/tasks/{task_id}", headers=headers)
    assert response.status_code == 200, response.text
    payload = response.json()
    _assert_public_task(payload)
    assert payload["access_scope"] == "control"
    assert payload["has_session"] is True
    assert incarnation_id not in response.text
    assert "deployment-token-native-session" not in response.text
    assert "private-codex-account" not in response.text
    assert "private instructions" not in response.text


@pytest.mark.asyncio
async def test_scoped_internal_service_keeps_complete_task_wire(
    secured_client,
):
    client, session_factory = secured_client
    owner_id, _owner_token = await _create_user(
        session_factory,
        email="projection-internal-owner@example.com",
        role="admin",
    )
    incarnation_id = "c" * 32
    async with session_factory() as db:
        task = Task(
            title="internal service complete projection",
            description="machine wire response",
            status="executing",
            incarnation_id=incarnation_id,
            retry_count=3,
            turn_generation=5,
            session_id="internal-native-session",
            created_by=owner_id,
            execution_user_id=owner_id,
            execution_user_role="admin",
            execution_mode="unrestricted",
            execution_principal_kind="user",
            metadata_={
                "codex_account_id": "internal-codex-account",
                "ccm_user_skill_snapshots": [
                    {
                        "id": 29,
                        "name": "internal-skill",
                        "content": "internal skill instructions",
                    }
                ],
                "worker_migration_receipt": {
                    "nonce": "internal-wire-only",
                    "digest": "d" * 64,
                },
                "ccm_worker_managed_task": True,
            },
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    token = issue_internal_service_token(
        audience="ccm_skills",
        task_id=task_id,
        task_incarnation_id=incarnation_id,
        task_retry_count=3,
        task_turn_generation=5,
        task_status="executing",
        owner_kind="task-projection-test",
        owner_id=task_id,
    )
    assert token
    response = await client.get(
        f"/api/tasks/{task_id}",
        headers=_headers(token),
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["incarnation_id"] == incarnation_id
    assert payload["session_id"] == "internal-native-session"
    assert payload["execution_user_id"] == owner_id
    assert payload["execution_user_role"] == "admin"
    assert payload["execution_mode"] == "unrestricted"
    assert payload["execution_principal_kind"] == "user"
    assert payload["metadata_"]["codex_account_id"] == "internal-codex-account"
    assert payload["metadata_"]["ccm_user_skill_snapshots"][0][
        "content"
    ] == "internal skill instructions"
    assert payload["metadata_"]["worker_migration_receipt"] == {
        "nonce": "internal-wire-only",
        "digest": "d" * 64,
    }


@pytest.mark.asyncio
async def test_worker_control_plane_keeps_complete_task_wire(
    secured_client,
    monkeypatch,
):
    client, session_factory = secured_client
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    task_id = 910_321
    incarnation_id = "a" * 32
    payload = {
        "id": task_id,
        "source_incarnation_id": incarnation_id,
        "source_retry_count": 2,
        "source_turn_generation": 4,
        "execution_user_id": 77,
        "execution_user_role": "admin",
        "execution_mode": "unrestricted",
        "execution_principal_kind": "delegated_user",
        "title": "worker wire",
        "description": "complete Manager mirror",
        "provider": "claude",
        "selected_user_skills": [17],
        "user_skill_snapshots": [
            {
                "id": 17,
                "name": "private-worker-skill",
                "description": "internal",
                "content": "never expose this on a human Task response",
            }
        ],
    }
    headers = _headers("security-service-token")

    created = await client.post("/api/tasks", headers=headers, json=payload)
    assert created.status_code == 201, created.text
    created_payload = created.json()
    assert created_payload["incarnation_id"] == incarnation_id
    assert created_payload["execution_user_id"] == 77
    assert created_payload["execution_user_role"] == "admin"
    assert created_payload["execution_mode"] == "unrestricted"
    assert created_payload["execution_principal_kind"] == "delegated_user"
    assert created_payload["metadata_"]["ccm_user_skill_snapshots"][0][
        "content"
    ].startswith("never expose")
    control_headers = {
        **headers,
        "X-CCM-Task-Incarnation": incarnation_id,
    }

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        metadata = dict(task.metadata_ or {})
        metadata["worker_migration_receipt"] = {
            "nonce": "wire-only",
            "digest": "f" * 64,
        }
        task.metadata_ = metadata
        await db.commit()

    fetched = await client.get(
        f"/api/tasks/{task_id}",
        headers=control_headers,
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["metadata_"]["worker_migration_receipt"][
        "nonce"
    ] == "wire-only"

    updated = await client.put(
        f"/api/tasks/{task_id}",
        headers=control_headers,
        json={"title": "worker wire updated"},
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["incarnation_id"] == incarnation_id
    assert updated.json()["execution_principal_kind"] == "delegated_user"
    assert updated.json()["metadata_"]["worker_migration_receipt"][
        "digest"
    ] == "f" * 64
