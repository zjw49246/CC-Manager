"""Tests for Chat and Plan API endpoints."""
import asyncio
import json
from pathlib import Path
import subprocess

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.task import Task
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.project import Project
from backend.models.task_share import TaskShare
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.plan_tasks import capture_repo_revision


async def _legacy_plan_task(session_factory, **values) -> int:
    """Create a historical Plan Task without using the closed write entry."""

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


# === Chat tests ===


@pytest.mark.asyncio
async def test_chat_history_not_found(client):
    resp = await client.get("/api/tasks/9999/chat/history")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_chat_history_empty(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_codex_fork_starts_before_selected_user_message(
    client, session_factory,
):
    from backend.models.log_entry import LogEntry

    created = await client.post("/api/tasks", json={
        "title": "Source",
        "description": "initial prompt",
        "target_repo": "/tmp/project",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.session_id = "thread-source"
        task.last_cwd = "/tmp/project"
        task.enabled_skills = {"code-review": True}
        task.selected_user_skills = [41]
        task.attention_tag = "等 Fork 完成后继续"
        task.metadata_ = {
            "codex_account_id": "codex-a",
            "attachments": [{
                "url": "/api/uploads/initial.png",
                "name": "initial.png",
                "is_image": True,
            }],
            "image_paths": ["/tmp/not-an-upload/initial.png"],
        }
        first = LogEntry(
            instance_id=1, task_id=task_id, event_type="message",
            role="assistant", content="first answer", is_error=False,
            raw_json='{"item_id":"item-1","turn_id":"turn-1"}',
        )
        anchor = LogEntry(
            instance_id=1, task_id=task_id, event_type="user_message",
            role="user", content="fork here", is_error=False,
            raw_json=(
                '{"raw_content":"fork here","attachments":[{'
                '"url":"/api/uploads/followup.txt","name":"followup.txt",'
                '"is_image":false}],"file_paths":["/tmp/not-an-upload/followup.txt"]}'
            ),
        )
        second = LogEntry(
            instance_id=1, task_id=task_id, event_type="message",
            role="assistant", content="second answer", is_error=False,
            raw_json='{"item_id":"item-2","turn_id":"turn-2"}',
        )
        injected = LogEntry(
            instance_id=1, task_id=task_id, event_type="user_message",
            role="user", content="mid-turn steer", is_error=False,
            raw_json='{"source":"inject","raw_content":"mid-turn steer"}',
        )
        later_user = LogEntry(
            instance_id=1, task_id=task_id, event_type="user_message",
            role="user", content="do not copy", is_error=False,
        )
        later_answer = LogEntry(
            instance_id=1, task_id=task_id, event_type="message",
            role="assistant", content="third answer", is_error=False,
            raw_json='{"item_id":"item-3","turn_id":"turn-3"}',
        )
        db.add_all([first, anchor, second, injected, later_user, later_answer])
        await db.commit()
        await db.refresh(anchor)
        await db.refresh(later_user)
        anchor_id = anchor.id
        later_user_id = later_user.id

    history = await client.get(f"/api/tasks/{task_id}/chat/history")
    first_message = history.json()[0]
    assert first_message["item_id"] == "item-1"
    assert first_message["turn_id"] == "turn-1"

    anchors = await client.get(f"/api/tasks/{task_id}/fork-anchors")
    assert anchors.status_code == 200, anchors.text
    assert [
        (item["type"], item["id"], item["content"])
        for item in anchors.json()
    ] == [
        ("latest", None, "完整复制当前上下文"),
        ("initial", None, "initial prompt"),
        ("user_message", anchor_id, "fork here"),
        ("user_message", later_user_id, "do not copy"),
    ]

    turns = [
        {"id": "turn-1", "status": "completed", "items": [{"id": "item-1"}]},
        {"id": "turn-2", "status": "completed", "items": [{"id": "item-2"}]},
        {"id": "turn-3", "status": "completed", "items": [{"id": "item-3"}]},
    ]
    with (
        patch(
            "backend.api.chat._codex_fork_home",
            return_value=("/tmp/codex-home", "codex-a"),
        ),
        patch(
            "backend.main.instance_manager.read_codex_thread",
            new=AsyncMock(return_value={"id": "thread-source", "turns": turns}),
        ) as read_thread,
        patch(
            "backend.main.instance_manager.fork_codex_thread",
            new=AsyncMock(return_value={
                "id": "thread-fork",
                "forkedFromId": "thread-source",
                "turns": turns[:1],
            }),
        ) as fork_thread,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/fork",
            json={"anchor": {"type": "user_message", "id": anchor_id}},
        )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["mode"] == "auto"
    assert payload["session_id"] == "thread-fork"
    assert payload["enabled_skills"] == {"code-review": True}
    assert payload["selected_user_skills"] == [41]
    assert payload["codex_service_tier"] == "priority"
    assert payload["attention_tag"] == "等 Fork 完成后继续"
    assert payload["metadata_"]["codex_account_id"] == "codex-a"
    assert payload["metadata_"]["forked_from_task_id"] == task_id
    assert payload["metadata_"]["forked_from_log_id"] == anchor_id
    assert payload["metadata_"]["forked_from_turn_id"] == "turn-1"
    assert payload["metadata_"]["fork_seed_message"] == "fork here"
    assert payload["metadata_"]["fork_seed_log_id"] == anchor_id
    assert payload["metadata_"]["fork_seed_uploads"] == [{
        "id": "fork-seed-0",
        "filename": "followup.txt",
        "path": str(
            (
                Path(__file__).resolve().parents[2] / "uploads/followup.txt"
            ).resolve()
        ),
        "url": "/api/uploads/followup.txt",
        "is_image": False,
    }]
    read_thread.assert_awaited_once_with("/tmp/codex-home", "thread-source")
    fork_thread.assert_awaited_once_with(
        "/tmp/codex-home",
        "thread-source",
        last_turn_id="turn-1",
    )

    async with session_factory() as db:
        copied = list((await db.execute(
            select(LogEntry)
            .where(LogEntry.task_id == payload["id"])
            .order_by(LogEntry.id.asc())
        )).scalars().all())
    assert [entry.content for entry in copied] == [
        "first answer",
        f"Forked from Task #{task_id}",
    ]


@pytest.mark.asyncio
async def test_codex_fork_latest_copies_full_completed_context(
    client, session_factory,
):
    from backend.models.log_entry import LogEntry

    created = await client.post("/api/tasks", json={
        "title": "Source",
        "description": "initial prompt",
        "target_repo": "/tmp/project",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.session_id = "thread-source"
        task.last_cwd = "/tmp/project"
        task.metadata_ = {
            "codex_account_id": "codex-a",
            "fork_seed_message": "must not leak",
            "fork_seed_uploads": [{"id": "old"}],
        }
        db.add_all([
            LogEntry(
                instance_id=1, task_id=task_id, event_type="message",
                role="assistant", content="first answer", is_error=False,
                raw_json='{"turn_id":"turn-1"}',
            ),
            LogEntry(
                instance_id=1, task_id=task_id, event_type="user_message",
                role="user", content="follow up", is_error=False,
            ),
            LogEntry(
                instance_id=1, task_id=task_id, event_type="message",
                role="assistant", content="latest answer", is_error=False,
                raw_json='{"turn_id":"turn-2"}',
            ),
        ])
        await db.commit()

    turns = [
        {"id": "turn-1", "status": "completed"},
        {"id": "turn-2", "status": "completed"},
    ]
    with (
        patch(
            "backend.api.chat._codex_fork_home",
            return_value=("/tmp/codex-home", "codex-a"),
        ),
        patch(
            "backend.main.instance_manager.read_codex_thread",
            new=AsyncMock(return_value={"id": "thread-source", "turns": turns}),
        ),
        patch(
            "backend.main.instance_manager.fork_codex_thread",
            new=AsyncMock(return_value={"id": "thread-copy"}),
        ) as fork_thread,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/fork",
            json={"anchor": {"type": "latest"}, "title": "Full copy"},
        )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["title"] == "Full copy"
    assert payload["description"] == "initial prompt"
    assert payload["metadata_"]["fork_mode"] == "full_copy"
    assert payload["metadata_"]["forked_from_turn_id"] == "turn-2"
    assert "fork_seed_message" not in payload["metadata_"]
    assert "fork_seed_uploads" not in payload["metadata_"]
    fork_thread.assert_awaited_once_with(
        "/tmp/codex-home", "thread-source", last_turn_id="turn-2",
    )

    async with session_factory() as db:
        copied = list((await db.execute(
            select(LogEntry)
            .where(LogEntry.task_id == payload["id"])
            .order_by(LogEntry.id.asc())
        )).scalars().all())
    assert [entry.content for entry in copied] == [
        "first answer", "follow up", "latest answer",
        f"Forked from Task #{task_id}",
    ]


@pytest.mark.asyncio
async def test_codex_fork_latest_rejects_non_completed_last_turn(
    client, session_factory,
):
    created = await client.post("/api/tasks", json={
        "title": "Source",
        "description": "initial prompt",
        "target_repo": "/tmp/project",
        "provider": "codex",
    })
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.session_id = "thread-source"
        task.last_cwd = "/tmp/project"
        await db.commit()

    with (
        patch(
            "backend.api.chat._codex_fork_home",
            return_value=("/tmp/codex-home", None),
        ),
        patch(
            "backend.main.instance_manager.read_codex_thread",
            new=AsyncMock(return_value={
                "id": "thread-source",
                "turns": [{"id": "turn-1", "status": "interrupted"}],
            }),
        ),
        patch(
            "backend.main.instance_manager.fork_codex_thread",
            new=AsyncMock(),
        ) as fork_thread,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/fork",
            json={"anchor": {"type": "latest"}},
        )

    assert response.status_code == 409
    assert "not completed" in response.json()["detail"]
    fork_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_fork_from_initial_prompt_creates_empty_thread(
    client, session_factory,
):
    created = await client.post("/api/tasks", json={
        "title": "Source",
        "description": "start again",
        "target_repo": "/tmp/project",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.session_id = "thread-source"
        task.last_cwd = "/tmp/project"
        task.metadata_ = {
            "codex_account_id": "codex-a",
            "attachments": [{
                "url": "/api/uploads/initial.png",
                "name": "initial.png",
                "is_image": True,
            }],
            "image_paths": ["/tmp/not-an-upload/initial.png"],
        }
        await db.commit()

    with (
        patch(
            "backend.api.chat._codex_fork_home",
            return_value=("/tmp/codex-home", "codex-a"),
        ),
        patch(
            "backend.main.instance_manager.create_codex_thread",
            new=AsyncMock(return_value={"id": "thread-empty", "turns": []}),
        ) as create_thread,
        patch(
            "backend.main.instance_manager.read_codex_thread",
            new=AsyncMock(),
        ) as read_thread,
        patch(
            "backend.main.instance_manager.fork_codex_thread",
            new=AsyncMock(),
        ) as fork_thread,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/fork",
            json={"anchor": {"type": "initial"}},
        )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["session_id"] == "thread-empty"
    assert payload["description"] is None
    assert payload["metadata_"]["forked_from_log_id"] is None
    assert payload["metadata_"]["forked_from_turn_id"] is None
    assert payload["metadata_"]["fork_seed_message"] == "start again"
    assert payload["metadata_"]["fork_seed_uploads"][0]["url"] == (
        "/api/uploads/initial.png"
    )
    assert "attachments" not in payload["metadata_"]
    assert "image_paths" not in payload["metadata_"]
    create_thread.assert_awaited_once_with(
        "/tmp/codex-home",
        cwd="/tmp/project",
        model="gpt-5.6-sol",
    )
    read_thread.assert_not_awaited()
    fork_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_fork_rejects_active_source_without_native_rpc(
    client, session_factory,
):
    created = await client.post("/api/tasks", json={
        "title": "Active",
        "description": "working",
        "target_repo": "/tmp/project",
        "provider": "codex",
    })
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "executing"
        task.session_id = "thread-active"
        await db.commit()

    with patch(
        "backend.main.instance_manager.read_codex_thread",
        new=AsyncMock(),
    ) as read_thread:
        response = await client.post(
            f"/api/tasks/{task_id}/fork",
            json={"anchor": {"type": "user_message", "id": 1}},
        )

    assert response.status_code == 409
    read_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_task_distill_routes_to_codex_provider(
    client, session_factory,
):
    from backend.models.log_entry import LogEntry

    create_resp = await client.post("/api/tasks", json={
        "title": "Codex distill",
        "description": "d",
        "target_repo": "/tmp",
        "provider": "codex",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.metadata_ = {"codex_account_id": "codex-2"}
        db.add(LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="user_message",
            role="user",
            content="fix the bug",
            is_error=False,
        ))
        await db.commit()

    sentinel_pool = object()
    sentinel_claude_pool = object()
    sentinel_cloudrouter_store = object()
    with (
        patch("backend.main.codex_pool", sentinel_pool),
        patch("backend.main.dispatcher.pool", sentinel_claude_pool),
        patch("backend.main.cloudrouter_store", sentinel_cloudrouter_store),
        patch(
            "backend.services.skill_distill.distill_task_conversation",
            new=AsyncMock(return_value={
                "provider": "codex",
                "model": "gpt-test",
                "content": "# Skill",
            }),
        ) as distill,
    ):
        resp = await client.post(
            f"/api/tasks/{task_id}/distill",
            json={"custom_instruction": "focus on tests"},
        )

    assert resp.status_code == 200
    assert resp.json()["provider"] == "codex"
    assert resp.json()["model"] == "gpt-test"
    assert resp.json()["content"] == "# Skill"
    kwargs = distill.await_args.kwargs
    assert kwargs["provider"] == "codex"
    assert kwargs["claude_pool"] is sentinel_claude_pool
    assert kwargs["codex_pool"] is sentinel_pool
    assert kwargs["codex_account_id"] == "codex-2"
    assert kwargs["cloudrouter_store"] is sentinel_cloudrouter_store
    assert kwargs["custom_instruction"] == "focus on tests"
    assert "fix the bug" in kwargs["conversation"]


@pytest.mark.asyncio
async def test_codex_fast_distill_fails_before_spawning_provider(
    client,
    session_factory,
):
    create_resp = await client.post("/api/tasks", json={
        "title": "Fast distill",
        "description": "d",
        "target_repo": "/tmp",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        db.add(LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="user_message",
            role="user",
            content="evidence",
            is_error=False,
        ))
        await db.commit()

    with patch(
        "backend.services.skill_distill.distill_task_conversation",
        new=AsyncMock(),
    ) as distill:
        response = await client.post(
            f"/api/tasks/{task_id}/distill",
            json={
                "expected_routing": {
                    "provider": "codex",
                    "model": "gpt-5.6-sol",
                    "codex_service_tier": "priority",
                },
            },
        )

    assert response.status_code == 409
    assert "priority admission" in response.json()["detail"]
    distill.assert_not_awaited()


@pytest.mark.asyncio
async def test_distill_route_change_cannot_commit_before_standard_spawn_finishes(
    client,
    session_factory,
):
    created = await client.post("/api/tasks", json={
        "title": "Distill routing barrier",
        "description": "d",
        "target_repo": "/tmp",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "default",
    })
    task_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="completed")
        )
        db.add(LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="user_message",
            role="user",
            content="evidence",
            is_error=False,
        ))
        await db.commit()

    provider_started = asyncio.Event()
    release_provider = asyncio.Event()

    async def controlled_distill(**kwargs):
        assert kwargs["provider"] == "codex"
        provider_started.set()
        await release_provider.wait()
        return {
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "content": "# Skill",
        }

    with patch(
        "backend.services.skill_distill.distill_task_conversation",
        new=AsyncMock(side_effect=controlled_distill),
    ):
        distill_request = asyncio.create_task(
            client.post(
                f"/api/tasks/{task_id}/distill",
                json={
                    "expected_routing": {
                        "provider": "codex",
                        "model": "gpt-5.6-sol",
                        "codex_service_tier": "default",
                    },
                },
            )
        )
        await asyncio.wait_for(provider_started.wait(), timeout=1)

        route_update = asyncio.create_task(
            client.put(
                f"/api/tasks/{task_id}",
                json={"codex_service_tier": "priority"},
            )
        )
        await asyncio.sleep(0.05)
        assert not route_update.done()

        release_provider.set()
        distilled = await asyncio.wait_for(distill_request, timeout=2)
        updated = await asyncio.wait_for(route_update, timeout=2)

    assert distilled.status_code == 200, distilled.text
    assert updated.status_code == 200, updated.text
    assert updated.json()["codex_service_tier"] == "priority"


async def _create_task_with_tools(client, session_factory):
    """Helper: create task + insert tool_use/tool_result log entries."""
    from backend.models.log_entry import LogEntry
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        db.add(LogEntry(
            instance_id=1, task_id=task_id, event_type="tool_use",
            role="assistant", tool_name="Edit",
            tool_input='{"file_path": "/tmp/test.py", "old_string": "foo", "new_string": "bar"}',
            is_error=False,
        ))
        db.add(LogEntry(
            instance_id=1, task_id=task_id, event_type="tool_result",
            role="assistant", tool_name="Edit",
            tool_output="File updated successfully",
            is_error=False,
        ))
        await db.commit()
    return task_id


@pytest.mark.asyncio
async def test_chat_history_compact_returns_summary(client, session_factory):
    """Default compact mode: tool_input is a summary, tool_output is null."""
    task_id = await _create_task_with_tools(client, session_factory)
    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    assert resp.status_code == 200
    msgs = resp.json()
    assert len(msgs) == 2

    # tool_use: summary extracted from file_path
    assert msgs[0]["event_type"] == "tool_use"
    assert msgs[0]["tool_name"] == "Edit"
    assert msgs[0]["tool_input"] == "/tmp/test.py"

    # tool_result: output stripped in compact mode
    assert msgs[1]["event_type"] == "tool_result"
    assert msgs[1]["tool_output"] is None


@pytest.mark.asyncio
async def test_chat_history_full_returns_tool_fields(client, session_factory):
    """compact=false: full tool_input/tool_output returned."""
    task_id = await _create_task_with_tools(client, session_factory)
    resp = await client.get(f"/api/tasks/{task_id}/chat/history?compact=false")
    assert resp.status_code == 200
    msgs = resp.json()
    assert len(msgs) == 2

    assert msgs[0]["event_type"] == "tool_use"
    assert msgs[0]["tool_input"] is not None
    assert "file_path" in msgs[0]["tool_input"]

    assert msgs[1]["event_type"] == "tool_result"
    assert msgs[1]["tool_output"] == "File updated successfully"


@pytest.mark.asyncio
async def test_message_detail_endpoint(client, session_factory):
    """Detail endpoint returns full tool_input/tool_output for a single message."""
    task_id = await _create_task_with_tools(client, session_factory)

    # Get compact history first to find message ids
    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    msgs = resp.json()
    tool_use_id = msgs[0]["id"]
    tool_result_id = msgs[1]["id"]

    # Fetch detail for tool_use
    resp = await client.get(f"/api/tasks/{task_id}/chat/{tool_use_id}/detail")
    assert resp.status_code == 200
    detail = resp.json()
    assert "file_path" in detail["tool_input"]

    # Fetch detail for tool_result
    resp = await client.get(f"/api/tasks/{task_id}/chat/{tool_result_id}/detail")
    assert resp.status_code == 200
    detail = resp.json()
    assert detail["tool_output"] == "File updated successfully"


@pytest.mark.asyncio
async def test_message_detail_not_found(client):
    """Detail for nonexistent message returns 404."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.get(f"/api/tasks/{task_id}/chat/99999/detail")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_chat_send_no_session(client):
    """Sending chat to a task with no session should return 400."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.post(f"/api/tasks/{task_id}/chat", json={"message": "hello"})
    assert resp.status_code == 400
    assert "session" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_chat_send_task_not_found(client):
    resp = await client.post("/api/tasks/9999/chat", json={"message": "hello"})
    assert resp.status_code == 404


# === Plan tests ===


@pytest.mark.asyncio
async def test_plan_approve_not_plan_review(client):
    """Approving a task not in plan_review state should return 400."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.post(f"/api/tasks/{task_id}/plan/approve")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_plan_reject_not_plan_review(client):
    """Rejecting a task not in plan_review state should return 400."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.post(f"/api/tasks/{task_id}/plan/reject")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_migrated_plan_task_is_visible_but_legacy_decisions_are_blocked(
    client,
    session_factory,
):
    """The Task remains history; its canonical Plan owns future decisions."""
    from backend.models.plan import PlanLegacyTaskLink

    task_id = await _legacy_plan_task(
        session_factory,
        title="Migrated Plan Task",
        description="d",
    )
    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == task_id).values(
                status="plan_review",
                plan_content="Historical plan",
            )
        )
        db.add(PlanLegacyTaskLink(legacy_task_id=task_id, plan_id=456))
        await db.commit()

    task_response = await client.get(f"/api/tasks/{task_id}")
    assert task_response.status_code == 200
    assert task_response.json()["canonical_plan_id"] == 456

    for action in ("approve", "reject"):
        response = await client.post(f"/api/tasks/{task_id}/plan/{action}")
        assert response.status_code == 409
        assert response.json()["detail"] == (
            "Legacy Plan Task has migrated to canonical Plan #456"
        )


@pytest.mark.asyncio
async def test_plan_approve_success(client, session_factory):
    """Approval completes the Plan without scheduling an execution turn."""
    task_id = await _legacy_plan_task(
        session_factory,
        title="Plan Task",
        description="d",
    )

    # Set task to plan_review state directly in DB
    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == task_id).values(
                status="plan_review", plan_content="Here is my plan..."
            )
        )
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/plan/approve")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "completed"
    assert data["plan_approved"] is True
    assert data["plan_approved_at"] is not None


@pytest.mark.asyncio
async def test_plan_approve_rejects_stale_fast_view_before_queueing(
    client,
    session_factory,
):
    task_id = await _legacy_plan_task(
        session_factory,
        title="Plan Task",
        description="d",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="plan_review", plan_content="plan")
        )
        await db.commit()

    response = await client.post(
        f"/api/tasks/{task_id}/plan/approve",
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
    assert task.status == "plan_review"
    assert task.plan_approved is None


@pytest.mark.asyncio
async def test_plan_reject_success(client, session_factory):
    """Rejecting a plan-mode task in plan_review state should cancel it."""
    task_id = await _legacy_plan_task(
        session_factory,
        title="Plan Task",
        description="d",
    )

    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == task_id).values(
                status="plan_review", plan_content="Here is my plan..."
            )
        )
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/plan/reject")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "cancelled"
    assert data["plan_approved"] is False


@pytest.mark.parametrize("action", ["approve", "reject"])
@pytest.mark.asyncio
async def test_plan_transition_revalidates_after_operation_lock(
    client,
    session_factory,
    monkeypatch,
    action,
):
    """A stale plan_review read cannot overwrite a migration generation."""

    task_id = await _legacy_plan_task(
        session_factory,
        title="Plan race",
        description="d",
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="plan_review")
        )
        await db.commit()

    lock_waiting = asyncio.Event()
    release_lock = asyncio.Event()

    class ControlledOperationLock:
        async def __aenter__(self):
            lock_waiting.set()
            await release_lock.wait()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    controlled_lock = ControlledOperationLock()
    monkeypatch.setattr(
        "backend.api.tasks.get_task_operation_lock",
        lambda observed_task_id: controlled_lock,
    )
    request = asyncio.create_task(
        client.post(f"/api/tasks/{task_id}/plan/{action}")
    )
    await asyncio.wait_for(lock_waiting.wait(), timeout=1)

    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="migrating", retry_count=Task.retry_count + 1)
        )
        await db.commit()
    release_lock.set()
    response = await request

    assert response.status_code == 400
    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "migrating"
    assert task.retry_count == 1
    assert task.plan_approved is None


@pytest.mark.asyncio
async def test_plan_approve_not_found(client):
    resp = await client.post("/api/tasks/9999/plan/approve")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_plan_reject_not_found(client):
    resp = await client.post("/api/tasks/9999/plan/reject")
    assert resp.status_code == 404


# === Chat send: enqueue contract ===
# POST /chat no longer launches an instance directly. It stores + broadcasts
# the user message and enqueues the prompt via dispatcher.enqueue_message;
# launch-time concerns (model/effort/cwd) moved to the dispatcher's
# _process_queued_message (tested below at the dispatcher level).


async def _create_task_with_session(client, session_factory, **extra_fields):
    """Helper: create a task and set session_id + target_repo in DB."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Chat Task", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    values = {"session_id": "test-session-123", **extra_fields}
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(**values))
        await db.commit()
    return task_id


def _mock_dispatcher():
    d = MagicMock()
    d.enqueue_message = AsyncMock()
    return d


@pytest.mark.asyncio
async def test_chat_can_start_frontend_review_goal_on_same_task(
    client,
    session_factory,
    tmp_path: Path,
):
    repo = tmp_path / "frontend-review-repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", str(repo)],
        check=True,
        capture_output=True,
        text=True,
    )
    task_id = await _create_task_with_session(
        client,
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        retry_count=2,
        target_repo=str(repo),
        last_cwd=str(repo),
    )
    async with session_factory() as db:
        project = Project(
            name=f"frontend-review-{task_id}",
            local_path=str(repo),
            status="ready",
            preview_config={
                "version": 1,
                "name": "Test preview",
                "setup": [],
                "processes": [{
                    "name": "web",
                    "command": [
                        "python",
                        "-m",
                        "http.server",
                        "{preview_port}",
                    ],
                    "cwd": ".",
                }],
                "url": "http://127.0.0.1:{preview_port}/",
                "health_url": "http://127.0.0.1:{preview_port}/",
                "startup_timeout_seconds": 30,
            },
        )
        db.add(project)
        await db.flush()
        task = await db.get(Task, task_id)
        assert task is not None
        task.project_id = project.id
        await db.commit()

    capability_response = await client.get(
        f"/api/tasks/{task_id}/frontend-review-goal/capabilities",
    )
    assert capability_response.status_code == 200
    assert capability_response.json() == {
        "available": True,
        "reason": None,
        "repo_path": str(repo),
    }

    response = await client.post(
        f"/api/tasks/{task_id}/frontend-review-goal",
        json={
            "message": "审查登录页桌面和移动端，修复后重新验证",
            "file_paths": ["/tmp/login-reference.png"],
            "profile": "standard",
            "max_iterations": 5,
            "expected_routing": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "codex_service_tier": "default",
            },
        },
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["id"] == task_id
    assert data["status"] == "pending"
    assert data["mode"] == "goal"
    assert data["session_id"] == "test-session-123"
    assert data["retry_count"] == 0
    assert data["goal_turns_used"] == 0
    assert data["goal_max_turns"] == 5
    assert "审查登录页桌面和移动端" in data["goal_condition"]
    assert data["metadata_"]["frontend_review"] == {
        "mode": "goal",
        "profile": "standard",
        "max_iterations": 5,
    }
    assert data["metadata_"]["frontend_review_activation"] == {
        "message": "审查登录页桌面和移动端，修复后重新验证",
        "file_paths": ["/tmp/login-reference.png"],
        "secret_ids": [],
        "restore": {
            "mode": "auto",
            "goal_condition": None,
            "goal_max_turns": 30,
            "goal_turns_used": 0,
            "goal_last_reason": None,
        },
    }

    async with session_factory() as db:
        rows = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )).scalars().all())
    assert len(rows) == 1
    assert rows[0].content == "审查登录页桌面和移动端，修复后重新验证"
    assert json.loads(rows[0].raw_json)["source"] == "frontend-review-goal"


@pytest.mark.asyncio
async def test_frontend_review_goal_rejects_non_git_resume_directory(
    client,
    session_factory,
    tmp_path: Path,
):
    ordinary_directory = tmp_path / "not-a-repository"
    ordinary_directory.mkdir()
    repo = tmp_path / "configured-repository"
    repo.mkdir()
    subprocess.run(
        ["git", "init", str(repo)],
        check=True,
        capture_output=True,
        text=True,
    )
    task_id = await _create_task_with_session(
        client,
        session_factory,
        status="completed",
        target_repo=str(repo),
        last_cwd=str(ordinary_directory),
    )

    capability_response = await client.get(
        f"/api/tasks/{task_id}/frontend-review-goal/capabilities",
    )
    assert capability_response.status_code == 200
    capability = capability_response.json()
    assert capability["available"] is False
    assert "Git" in capability["reason"]
    assert capability["repo_path"] is None

    response = await client.post(
        f"/api/tasks/{task_id}/frontend-review-goal",
        json={"message": "现在开始循环审查"},
    )

    assert response.status_code == 409
    assert "Git" in response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        rows = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )).scalars().all())
    assert task is not None
    assert task.status == "completed"
    assert task.mode == "auto"
    assert rows == []


@pytest.mark.asyncio
async def test_frontend_review_goal_requires_idle_local_session(
    client,
    session_factory,
):
    task_id = await _create_task_with_session(
        client,
        session_factory,
        status="executing",
    )

    response = await client.post(
        f"/api/tasks/{task_id}/frontend-review-goal",
        json={"message": "现在开始循环审查"},
    )

    assert response.status_code == 409
    assert "not idle" in response.text


@pytest.mark.asyncio
async def test_chat_send_enqueues_message(client, session_factory):
    """Chat send returns 200 queued=True and enqueues via the dispatcher."""
    from backend.services.dispatcher import PRIORITY_USER

    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="claude",
        model="claude-sonnet-4-6",
    )

    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(f"/api/tasks/{task_id}/chat", json={"message": "hi"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["queued"] is True
    assert data["session_id"] == "test-session-123"
    assert data["workspace_review_expected"] is False
    assert data["workspace_review_baseline_run_id"] is None

    mock_d.enqueue_message.assert_awaited_once()
    kwargs = mock_d.enqueue_message.call_args.kwargs
    assert kwargs["task_id"] == task_id
    assert kwargs["prompt"] == "hi"
    assert kwargs["priority"] == PRIORITY_USER
    assert kwargs["source"] == "user"
    assert isinstance(kwargs["source_log_id"], int)

    # User message broadcast to task channel before enqueue
    task_broadcasts = [
        c for c in mock_broadcaster.broadcast.call_args_list
        if c[0][0] == f"task:{task_id}" and c[0][1].get("event_type") == "user_message"
    ]
    assert len(task_broadcasts) == 1
    assert task_broadcasts[0][0][1]["content"] == "hi"


@pytest.mark.asyncio
async def test_chat_closes_legacy_terminal_frontend_review_goal_before_enqueue(
    client,
    session_factory,
):
    task_id = await _create_task_with_session(
        client,
        session_factory,
        status="completed",
        mode="goal",
        goal_condition="temporary browser review",
        goal_max_turns=5,
        goal_turns_used=2,
        goal_last_reason="review passed",
        metadata_={
            "keep": "account-binding",
            "frontend_review": {
                "mode": "goal",
                "profile": "standard",
                "max_iterations": 5,
            },
            # Legacy activations did not contain a restore snapshot.
            "frontend_review_activation": {
                "message": "审查并修复前端",
                "file_paths": [],
                "secret_ids": [],
            },
        },
    )
    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), patch(
        "backend.main.broadcaster",
        mock_broadcaster,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": "这是 Goal 结束后的普通后续问题"},
        )

    assert response.status_code == 200, response.text
    mock_d.enqueue_message.assert_awaited_once()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        assert task.mode == "auto"
        assert task.goal_condition is None
        assert task.goal_max_turns == 30
        assert task.goal_turns_used == 0
        assert task.goal_last_reason is None
        assert task.metadata_ == {"keep": "account-binding"}


@pytest.mark.asyncio
async def test_chat_frontend_pr_acceptance_injects_browser_review_protocol(
    client,
    session_factory,
):
    """Natural-language PR UI acceptance cannot silently become code-only QA."""

    task_id = await _create_task_with_session(client, session_factory)
    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()
    message = "审查一下pr99分支的前端内容是否实现"

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        response = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": message},
        )

    assert response.status_code == 200
    assert response.json()["workspace_review_expected"] is True
    assert response.json()["workspace_review_baseline_run_id"] is None
    prompt = mock_d.enqueue_message.await_args.kwargs["prompt"]
    assert prompt.startswith(message)
    assert "ccm_workspace_browser_review_request" in prompt
    assert "test_git_target" in prompt
    assert "PR #99" in prompt
    assert "精确 SHA" in prompt
    assert "Sandbox cleanup" in prompt
    assert "不得改测当前工作区" in prompt

    history = await client.get(f"/api/tasks/{task_id}/chat/history")
    user_rows = [row for row in history.json() if row["role"] == "user"]
    assert user_rows[-1]["content"] == message


@pytest.mark.asyncio
async def test_chat_sender_prefix_is_display_only(session_factory):
    """Authenticated sender names stay in the UI copy, never the model prompt."""
    import json
    from types import SimpleNamespace
    from sqlalchemy import select

    from backend.api.chat import ChatMessage, get_chat_history, send_chat_message
    from backend.models.log_entry import LogEntry
    from backend.models.user import User

    async with session_factory() as db:
        sender = User(
            email="alice-prefix@test.local",
            name="Alice",
            password_hash="unused",
            role="super_admin",
        )
        task = Task(
            title="Prefix test",
            description="d",
            target_repo="/tmp",
            session_id="prefix-session",
        )
        db.add_all([sender, task])
        await db.commit()
        await db.refresh(sender)
        await db.refresh(task)

        mock_d = _mock_dispatcher()
        mock_broadcaster = MagicMock()
        mock_broadcaster.broadcast = AsyncMock()
        request = SimpleNamespace(
            state=SimpleNamespace(user_id=sender.id, user_role="super_admin")
        )

        with patch("backend.main.dispatcher", mock_d), \
             patch("backend.main.broadcaster", mock_broadcaster):
            result = await send_chat_message(
                task.id,
                ChatMessage(message="[BUG] preserve this tag"),
                request,
                db,
            )

        stored = (await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type == "user_message",
            )
        )).scalar_one()
        history = await get_chat_history(
            task.id,
            request=request,
            limit=0,
            compact=True,
            db=db,
        )

    assert result["queued"] is True
    assert mock_d.enqueue_message.call_args.kwargs["prompt"] == "[BUG] preserve this tag"
    assert stored.content == "[Alice] [BUG] preserve this tag"
    assert json.loads(stored.raw_json)["raw_content"] == "[BUG] preserve this tag"
    assert history[-1]["raw_content"] == "[BUG] preserve this tag"
    display_event = mock_broadcaster.broadcast.call_args.args[1]
    assert display_event["content"] == "[Alice] [BUG] preserve this tag"
    assert display_event["sender_name"] == "Alice"


@pytest.mark.asyncio
async def test_service_token_sender_prefix_is_display_only(session_factory):
    """A service-token request has an Admin label even without a bound User."""
    import json
    from types import SimpleNamespace
    from sqlalchemy import select

    from backend.api.chat import ChatMessage, send_chat_message
    from backend.models.log_entry import LogEntry

    async with session_factory() as db:
        task = Task(
            title="Token prefix test",
            description="d",
            target_repo="/tmp",
            session_id="token-prefix-session",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

        mock_d = _mock_dispatcher()
        mock_broadcaster = MagicMock()
        mock_broadcaster.broadcast = AsyncMock()
        request = SimpleNamespace(state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        ))

        with patch("backend.main.dispatcher", mock_d), \
             patch("backend.main.broadcaster", mock_broadcaster):
            await send_chat_message(
                task.id,
                ChatMessage(message="[BUG] keep this raw"),
                request,
                db,
            )

        stored = (await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type == "user_message",
            )
        )).scalar_one()

    assert mock_d.enqueue_message.call_args.kwargs["prompt"] == "[BUG] keep this raw"
    assert stored.content == "[Admin] [BUG] keep this raw"
    assert json.loads(stored.raw_json) == {
        "raw_content": "[BUG] keep this raw",
        "sender_name": "Admin",
    }
    event = mock_broadcaster.broadcast.call_args.args[1]
    assert event["content"] == "[Admin] [BUG] keep this raw"
    assert event["raw_content"] == "[BUG] keep this raw"


@pytest.mark.asyncio
async def test_shared_chat_sender_prefix_is_display_only(client, session_factory):
    """Shared-task sender names are shown in chat but excluded from enqueue."""
    import json
    from sqlalchemy import select
    from backend.api.shared_access import SharedChatMessage, shared_chat
    from backend.models.log_entry import LogEntry

    task_id = await _create_task_with_session(client, session_factory)
    async with session_factory() as db:
        db.add(TaskShare(
            task_id=task_id,
            shared_to_open_id="ou-prefix-test",
            shared_to_name="Remote Alice",
            shared_to_ccm_url="https://receiver.test",
            share_token="prefix-share-token",
            status="active",
        ))
        await db.commit()

    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()
    async with session_factory() as db:
        with patch("backend.main.dispatcher", mock_d), \
             patch("backend.main.broadcaster", mock_broadcaster):
            response = await shared_chat(
                task_id,
                SharedChatMessage(
                    message="[TODO] keep the tag",
                    sender_name="Remote Alice",
                ),
                token="prefix-share-token",
                db=db,
            )

    assert response["queued"] is True
    assert mock_d.enqueue_message.call_args.kwargs["prompt"] == "[TODO] keep the tag"
    assert isinstance(
        mock_d.enqueue_message.call_args.kwargs["source_log_id"],
        int,
    )
    async with session_factory() as db:
        stored = (await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )).scalar_one()
    assert stored.content == "[Remote Alice] [TODO] keep the tag"
    assert json.loads(stored.raw_json)["raw_content"] == "[TODO] keep the tag"
    event = mock_broadcaster.broadcast.call_args.args[1]
    assert event["content"] == stored.content
    assert event["id"] == stored.id
    assert event["task_id"] == task_id
    assert event["timestamp"].endswith("Z")


@pytest.mark.asyncio
async def test_shared_pr_review_chat_waits_for_terminal_owner_state(
    client,
    session_factory,
):
    from fastapi import HTTPException

    from backend.api.shared_access import SharedChatMessage, shared_chat
    from backend.models.pr_monitor import MonitoredRepo, PRReview

    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="claude",
    )
    async with session_factory() as db:
        repo = MonitoredRepo(
            repo_full_name="owner/shared-review",
            webhook_secret="shared-review-secret",
        )
        db.add(repo)
        await db.flush()
        review = PRReview(
            repo_id=repo.id,
            pr_number=1,
            pr_title="Shared review",
            pr_author="alice",
            pr_url="https://example.test/pr/1",
            task_id=task_id,
            status="reviewing",
        )
        db.add(review)
        db.add(TaskShare(
            task_id=task_id,
            shared_to_open_id="ou-shared-review",
            shared_to_name="Remote Reviewer",
            shared_to_ccm_url="https://receiver.test",
            share_token="shared-review-token",
            status="active",
        ))
        await db.commit()
        review_id = review.id

    dispatcher = _mock_dispatcher()
    broadcaster = MagicMock(broadcast=AsyncMock())
    async with session_factory() as db:
        with patch("backend.main.dispatcher", dispatcher), patch(
            "backend.main.broadcaster",
            broadcaster,
        ), pytest.raises(HTTPException) as blocked:
            await shared_chat(
                task_id,
                SharedChatMessage(message="too early"),
                token="shared-review-token",
                db=db,
            )
    assert blocked.value.status_code == 409
    broadcaster.broadcast.assert_not_awaited()

    async with session_factory() as db:
        review = await db.get(PRReview, review_id)
        review.status = "approved"
        await db.commit()
    async with session_factory() as db:
        with patch("backend.main.dispatcher", dispatcher), patch(
            "backend.main.broadcaster",
            broadcaster,
        ):
            accepted = await shared_chat(
                task_id,
                SharedChatMessage(message="explain the result"),
                token="shared-review-token",
                db=db,
            )

    assert accepted["queued"] is True
    dispatcher.enqueue_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_shared_relay_replaces_remote_chat_identity_with_local_log_entry(
    client,
    session_factory,
):
    """A shadow Task must never expose the sharer's database id as local."""
    from types import SimpleNamespace

    from backend.services.shared_relay import SharedRelay

    created = await client.post("/api/tasks", json={
        "title": "Shadow",
        "description": "shared relay",
        "target_repo": "/tmp",
    })
    task_id = created.json()["id"]
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    relay = SharedRelay(session_factory, broadcaster)

    await relay._handle(
        {
            "data": {
                "id": 987654,
                "task_id": 44,
                "event_type": "message",
                "role": "assistant",
                "content": "remote answer",
                "timestamp": "2026-07-30T01:02:03Z",
            },
        },
        SimpleNamespace(local_task_id=task_id),
    )

    async with session_factory() as db:
        stored = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "message",
                )
            )
        ).scalar_one()

    event = broadcaster.broadcast.call_args.args[1]
    assert event["id"] == stored.id
    assert event["id"] != 987654
    assert event["task_id"] == task_id
    assert event["timestamp"].endswith("Z")
    assert event["content"] == "remote answer"


@pytest.mark.asyncio
async def test_chat_send_queues_even_when_task_busy(client, session_factory):
    """Busy/no-idle-instance states no longer 4xx at the endpoint — the
    message is queued and the dispatcher serializes processing."""
    task_id = await _create_task_with_session(client, session_factory)

    # An instance currently "running" this task — irrelevant to the endpoint now
    async with session_factory() as db:
        inst = Instance(name="busy-inst", status="idle", current_task_id=task_id)
        db.add(inst)
        await db.commit()

    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(f"/api/tasks/{task_id}/chat", json={"message": "hi"})

    assert resp.status_code == 200
    assert resp.json()["queued"] is True
    mock_d.enqueue_message.assert_awaited_once()


# === Chat with image_paths ===


@pytest.mark.asyncio
async def test_chat_send_with_image_paths_appends_to_prompt(client, session_factory):
    """When image_paths are provided, the enqueued prompt includes the file list."""
    task_id = await _create_task_with_session(client, session_factory)

    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": "check this", "image_paths": ["/uploads/img1.png", "/uploads/img2.jpg"]},
        )
    assert resp.status_code == 200

    prompt_used = mock_d.enqueue_message.call_args.kwargs["prompt"]
    assert "/uploads/img1.png" in prompt_used
    assert "/uploads/img2.jpg" in prompt_used
    assert "Read" in prompt_used  # the instruction to use the Read tool


@pytest.mark.asyncio
async def test_chat_send_without_image_paths_plain_prompt(client, session_factory):
    """When no image_paths are provided, the enqueued prompt is just the message."""
    task_id = await _create_task_with_session(client, session_factory)

    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": "plain message"},
        )
    assert resp.status_code == 200
    assert mock_d.enqueue_message.call_args.kwargs["prompt"] == "plain message"


@pytest.mark.asyncio
async def test_enabled_skills_do_not_impersonate_command_invocations(
    client, session_factory,
):
    """Enabled means discoverable, not invoked: ordinary chat stays untouched."""
    task_id = await _create_task_with_session(
        client,
        session_factory,
        enabled_skills={"monitor": True, "sub-agent": True},
    )
    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": "你好"},
        )

    assert resp.status_code == 200
    kwargs = mock_d.enqueue_message.call_args.kwargs
    assert kwargs["prompt"] == "你好"
    assert kwargs["command_skills"] is None
    assert "用户通过 $monitor 命令触发" not in kwargs["prompt"]
    assert "用户通过 $sub-agent 命令触发" not in kwargs["prompt"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message", "skill_name", "provider"),
    [
        ("$monitor watch the build", "monitor", "claude"),
        ("$sub-agent review the change", "sub-agent", "codex"),
    ],
)
async def test_explicit_skill_command_injects_only_selected_invocation(
    client, session_factory, message, skill_name, provider,
):
    """A real leading $command activates exactly that command for the turn."""
    task_id = await _create_task_with_session(
        client,
        session_factory,
        enabled_skills={"monitor": True, "sub-agent": True},
        provider=provider,
    )
    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": message},
        )

    assert resp.status_code == 200
    kwargs = mock_d.enqueue_message.call_args.kwargs
    assert kwargs["prompt"].startswith(message.split(None, 1)[1])
    assert f"用户通过 ${skill_name} 命令触发了技能 '{skill_name}'" in kwargs["prompt"]
    assert kwargs["command_skills"] == {skill_name: True}
    other = "sub-agent" if skill_name == "monitor" else "monitor"
    assert f"用户通过 ${other} 命令触发" not in kwargs["prompt"]


@pytest.mark.asyncio
async def test_local_codex_chat_accepts_monitor_command(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.log_entry import LogEntry

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    task_id = await _create_task_with_session(
        client,
        session_factory,
        enabled_skills={},
        provider="codex",
    )
    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        response = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": "$monitor watch the build"},
        )

    assert response.status_code == 200, response.text
    assert mock_d.enqueue_message.await_args.kwargs["command_skills"] == {
        "monitor": True,
    }
    mock_broadcaster.broadcast.assert_awaited()
    async with session_factory() as db:
        stored = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )).scalars().all())
    assert len(stored) == 1
    assert stored[0].content == "$monitor watch the build"


@pytest.mark.asyncio
async def test_codex_worker_chat_rejects_monitor_before_proxy_or_log(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.log_entry import LogEntry

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with session_factory() as db:
        task = Task(
            title="Worker Codex monitor command",
            description="d",
            status="completed",
            provider="codex",
            worker_id=77,
            session_id="worker-session",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    worker_proxy = MagicMock()
    worker_proxy.require_ready_worker = AsyncMock()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    monkeypatch.setattr("backend.main.worker_proxy", worker_proxy)
    monkeypatch.setattr("backend.main.broadcaster", broadcaster)

    response = await client.post(
        f"/api/tasks/{task_id}/chat",
        json={"message": "$monitor watch the build"},
    )

    assert response.status_code == 400
    assert "does not support Skills: monitor" in response.text
    worker_proxy.require_ready_worker.assert_not_awaited()
    broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        stored = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )).scalars().all())
    assert stored == []


@pytest.mark.asyncio
async def test_codex_shared_chat_rejects_monitor_before_local_side_effects(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.feishu_binding import FeishuUserBinding
    from backend.models.task_share import SharedTaskReceived
    import backend.services.shared_proxy as shared_proxy_module

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    engine = session_factory.kw["bind"]
    async with engine.begin() as conn:
        await conn.run_sync(FeishuUserBinding.__table__.create, checkfirst=True)

    async with session_factory() as db:
        shared = SharedTaskReceived(
            owner_ccm_url="https://owner.test",
            remote_task_id=91,
            share_token="shared-command-token",
            task_title="Shared Codex task",
            task_description="d",
            status="active",
        )
        db.add(shared)
        await db.flush()
        shadow = Task(
            title="Shared Codex task",
            description="d",
            status="completed",
            provider="codex",
            shared_from_id=shared.id,
        )
        db.add(shadow)
        await db.flush()
        shared.local_task_id = shadow.id
        await db.commit()
        task_id = shadow.id

    proxy_chat = AsyncMock()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    monkeypatch.setattr(shared_proxy_module, "proxy_chat", proxy_chat)
    monkeypatch.setattr("backend.main.broadcaster", broadcaster)

    response = await client.post(
        f"/api/tasks/{task_id}/chat",
        json={"message": "$monitor watch the build"},
    )

    assert response.status_code == 400
    assert "does not support Skills: monitor" in response.text
    proxy_chat.assert_not_awaited()
    broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        stored = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )).scalars().all())
    assert stored == []


@pytest.mark.asyncio
async def test_shared_owner_rejection_leaves_no_local_ghost_message(
    client,
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.models.feishu_binding import FeishuUserBinding
    from backend.models.task_share import SharedTaskReceived
    import backend.services.shared_proxy as shared_proxy_module

    engine = session_factory.kw["bind"]
    async with engine.begin() as conn:
        await conn.run_sync(FeishuUserBinding.__table__.create, checkfirst=True)

    async with session_factory() as db:
        shared = SharedTaskReceived(
            owner_ccm_url="https://owner.test",
            remote_task_id=92,
            share_token="active-review-token",
            task_title="Shared PR review",
            task_description="d",
            status="active",
        )
        db.add(shared)
        await db.flush()
        shadow = Task(
            title="Shared PR review",
            description="d",
            status="completed",
            provider="claude",
            shared_from_id=shared.id,
            session_id="shadow-review-session",
        )
        db.add(shadow)
        await db.flush()
        shared.local_task_id = shadow.id
        await db.commit()
        task_id = shadow.id

    rejection = RuntimeError("owner rejected active review")
    rejection.response = SimpleNamespace(
        status_code=409,
        json=lambda: {"detail": "review is still active"},
    )
    proxy_chat = AsyncMock(side_effect=rejection)
    broadcaster = MagicMock(broadcast=AsyncMock())
    monkeypatch.setattr(shared_proxy_module, "proxy_chat", proxy_chat)
    monkeypatch.setattr("backend.main.broadcaster", broadcaster)

    response = await client.post(
        f"/api/tasks/{task_id}/chat",
        json={"message": "explain this review"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "review is still active"
    broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        stored = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )).scalars().all())
    assert stored == []


@pytest.mark.asyncio
async def test_mentioning_skill_command_mid_message_does_not_invoke_it(
    client, session_factory,
):
    """Quoting or discussing $monitor is ordinary text unless it leads."""
    task_id = await _create_task_with_session(
        client,
        session_factory,
        enabled_skills={"monitor": True},
    )
    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()
    message = "你怎么知道 $monitor？"

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": message},
        )

    assert resp.status_code == 200
    kwargs = mock_d.enqueue_message.call_args.kwargs
    assert kwargs["prompt"] == message
    assert kwargs["command_skills"] is None


@pytest.mark.asyncio
async def test_chat_send_with_image_paths_stores_original_message(client, session_factory):
    """LogEntry content stores the original user message (without image instruction)."""
    from backend.models.log_entry import LogEntry
    from sqlalchemy import select

    task_id = await _create_task_with_session(client, session_factory)

    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": "my message", "image_paths": ["/uploads/z.png"]},
        )

    async with session_factory() as db:
        result = await db.execute(
            select(LogEntry)
            .where(LogEntry.task_id == task_id, LogEntry.event_type == "user_message")
        )
        log = result.scalar_one()

    # Stored content should be the clean user message, not the augmented prompt
    assert log.content == "my message"


# === Dispatcher: queued message → launch resolution (model/effort/cwd) ===


from backend.services.dispatcher import (
    GlobalDispatcher,
    PRIORITY_USER,
    QueuedMessage,
    QueuedMessageRoutingMismatchError,
)


def _make_dispatcher(db_factory):
    mock_im = MagicMock()
    mock_im.processes = {}
    mock_im.launch = AsyncMock()
    mock_im.pty_mode_enabled = False
    mock_im.is_pty_managed_turn = MagicMock(return_value=False)
    mock_im.transient_error_seen = MagicMock(return_value=False)
    mock_im.pty_rate_limit_seen = MagicMock(return_value=False)
    mock_im._try_proactive_pool_switch = AsyncMock()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()
    return GlobalDispatcher(db_factory, mock_im, mock_broadcaster)


async def _seed_task_for_queue(db_factory, **task_fields):
    async with db_factory() as db:
        task = Task(
            title="t", description="d", status="completed", target_repo="/tmp",
            session_id="sess-1", **task_fields,
        )
        db.add(task)
        inst = Instance(name="idle-inst", status="idle")
        db.add(inst)
        await db.commit()
        await db.refresh(task)
        return task.id


def _queued(prompt="hi"):
    import time
    return QueuedMessage(
        priority=PRIORITY_USER, timestamp=time.monotonic(),
        prompt=prompt, source="user",
    )


@pytest.mark.asyncio
async def test_queued_message_rejects_route_changed_after_api_admission(
    db_factory,
):
    dispatcher = _make_dispatcher(db_factory)
    task_id = await _seed_task_for_queue(
        db_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    message = _queued()
    message.expected_task_routing = (
        "codex",
        "gpt-5.6-sol",
        "priority",
    )

    with pytest.raises(
        QueuedMessageRoutingMismatchError,
        match="changed after message admission",
    ):
        await dispatcher._process_queued_message(task_id, message)

    dispatcher.instance_manager.launch.assert_not_awaited()


@pytest.mark.asyncio
async def test_model_history_rebuild_uses_raw_user_content(db_factory):
    """Compaction and Distill exclude UI sender names without stripping real tags."""
    import json

    from backend.api.chat import _collect_conversation_for_distill
    from backend.models.log_entry import LogEntry

    async with db_factory() as db:
        task = Task(
            title="History sender test",
            description="d",
            status="completed",
            target_repo="/tmp",
            session_id="history-session",
        )
        db.add(task)
        await db.flush()
        db.add_all([
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="user_message",
                role="user",
                content="[Alice] [BUG] preserve this tag",
                raw_json=json.dumps({
                    "sender_name": "Alice",
                    "raw_content": "[BUG] preserve this tag",
                }),
                is_error=False,
            ),
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="message",
                role="assistant",
                content="First reply",
                is_error=False,
            ),
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="user_message",
                role="user",
                content="[Monitor #7] keep this operational label",
                raw_json=json.dumps({"source": "monitor"}),
                is_error=False,
            ),
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="message",
                role="assistant",
                content="Second reply",
                is_error=False,
            ),
        ])
        await db.commit()

        dispatcher = _make_dispatcher(db_factory)
        compacted = await dispatcher._compact_session(task.id, task.session_id, db)
        distilled = await _collect_conversation_for_distill(task.id, db)

    for model_input in (compacted, distilled):
        assert model_input is not None
        assert "[Alice]" not in model_input
        assert "[BUG] preserve this tag" in model_input
        assert "[Monitor #7] keep this operational label" in model_input


@pytest.mark.asyncio
async def test_compaction_prefers_final_recent_facts_and_excludes_current_row(
    db_factory,
):
    """Old task text and process chatter must not outrank the latest facts."""
    import json

    from backend.models.log_entry import LogEntry
    from backend.services.context_compaction import (
        build_compacted_resume_prompt,
    )

    async with db_factory() as db:
        task = Task(
            title="Compaction recency",
            description="OLD_BACKGROUND: inspect Task 90 with 24 nodes",
            status="completed",
            target_repo="/tmp",
            session_id="history-session",
        )
        db.add(task)
        await db.flush()
        prior = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="[Alice] check the active training",
            raw_json=json.dumps({
                "sender_name": "Alice",
                "raw_content": "check the active training",
            }),
            is_error=False,
        )
        db.add(prior)
        await db.flush()
        db.add_all([
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="message",
                role="assistant",
                content="PROCESS_CHATTER: I will check it now",
                is_error=False,
            ),
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="message",
                role="assistant",
                content="RECENT_FINAL: job 0738z is running on 20 nodes",
                is_error=False,
            ),
        ])
        current = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="CURRENT_REQUEST: what is the latest loss?",
            raw_json=json.dumps({
                "raw_content": "CURRENT_REQUEST: what is the latest loss?",
            }),
            is_error=False,
        )
        db.add(current)
        await db.flush()
        # A different ordinary message can already be queued behind the
        # current turn. It has not run yet and must neither enter this retry
        # nor hide a later live injection into the current turn.
        db.add(
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="user_message",
                role="user",
                content="FUTURE_REQUEST: stop it",
                raw_json=json.dumps({
                    "raw_content": "FUTURE_REQUEST: stop it",
                }),
                is_error=False,
            )
        )
        await db.flush()
        db.add_all([
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="user_message",
                role="user",
                content="INJECTED_CORRECTION: use the live job, not Task 90",
                raw_json=json.dumps({
                    "source": "inject",
                    "raw_content": (
                        "INJECTED_CORRECTION: use the live job, not Task 90"
                    ),
                }),
                is_error=False,
            ),
            LogEntry(
                instance_id=None,
                task_id=task.id,
                event_type="message",
                role="assistant",
                content="INJECTED_FINAL: live job is healthy",
                is_error=False,
            ),
        ])
        await db.commit()

        dispatcher = _make_dispatcher(db_factory)
        compacted = await dispatcher._compact_session(
            task.id,
            task.session_id,
            db,
            exclude_log_entry_id=current.id,
            post_source_injects_are_current=True,
        )

    assert compacted is not None
    assert "check the active training" in compacted
    assert "[Alice]" not in compacted
    assert "RECENT_FINAL: job 0738z is running on 20 nodes" in compacted
    assert "PROCESS_CHATTER" not in compacted
    assert "CURRENT_REQUEST" not in compacted
    assert "INJECTED_CORRECTION: use the live job, not Task 90" in compacted
    assert "INJECTED_FINAL: live job is healthy" in compacted
    assert "FUTURE_REQUEST" not in compacted
    assert "当前消息执行期间的后续补充/纠正" in compacted
    assert "冲突时以此为准" in compacted
    assert compacted.index("## 近期对话") < compacted.index(
        "## 原始任务背景"
    )

    resumed = build_compacted_resume_prompt(
        compacted,
        "CURRENT_REQUEST: what is the latest loss?",
    )
    assert resumed.count("CURRENT_REQUEST: what is the latest loss?") == 1
    assert "冲突时以该补充/纠正为准" in resumed
    assert resumed.endswith(
        "[基础当前消息 — 默认最高优先级]\n"
        "CURRENT_REQUEST: what is the latest loss?"
    )


@pytest.mark.asyncio
async def test_repeated_lifecycle_compaction_does_not_nest_old_wrapper(
    db_factory,
):
    from backend.models.log_entry import LogEntry

    async with db_factory() as db:
        task = Task(
            title="Repeated compaction",
            description=(
                "[Context compacted]\n"
                "NESTED_OLD_SUMMARY that must not be wrapped again\n\n"
                "## 原始任务背景（最低优先级，可能已被近期信息取代）\n"
                "ORIGINAL_ACCEPTANCE: preserve this invariant"
            ),
            status="executing",
            target_repo="/tmp",
            session_id="second-session",
        )
        db.add(task)
        await db.flush()
        db.add_all([
            LogEntry(
                task_id=task.id,
                event_type="user_message",
                role="user",
                content="RECENT_STAGE",
                is_error=False,
            ),
            LogEntry(
                task_id=task.id,
                event_type="message",
                role="assistant",
                content="RECENT_RESULT",
                is_error=False,
            ),
        ])
        await db.commit()

        dispatcher = _make_dispatcher(db_factory)
        compacted = await dispatcher._compact_session(
            task.id,
            task.session_id,
            db,
        )

    assert compacted is not None
    assert "RECENT_STAGE" in compacted
    assert "RECENT_RESULT" in compacted
    assert "NESTED_OLD_SUMMARY" not in compacted
    assert "[Context compacted]" not in compacted
    assert "ORIGINAL_ACCEPTANCE: preserve this invariant" in compacted


@pytest.mark.asyncio
async def test_legacy_lifecycle_wrapper_preserves_background_head_and_tail(
    db_factory,
):
    original = (
        "ORIGINAL_HEAD: keep the task identity\n"
        + ("middle context " * 220)
        + "\nORIGINAL_TAIL_ACCEPTANCE: preserve this final invariant"
    )
    async with db_factory() as db:
        task = Task(
            title="Legacy repeated compaction",
            description=(
                "[Context compacted]\n"
                "LEGACY_OLD_SUMMARY that must not be retained"
                "\n\n---\n\n"
                f"{original}"
            ),
            status="executing",
            target_repo="/tmp",
            session_id="legacy-second-session",
        )
        db.add(task)
        await db.commit()

        dispatcher = _make_dispatcher(db_factory)
        compacted = await dispatcher._compact_session(
            task.id,
            task.session_id,
            db,
        )

    assert compacted is not None
    assert "LEGACY_OLD_SUMMARY" not in compacted
    assert "ORIGINAL_HEAD: keep the task identity" in compacted
    assert "ORIGINAL_TAIL_ACCEPTANCE: preserve this final invariant" in compacted
    assert "...[中间省略]..." in compacted


@pytest.fixture
def fake_session_on_disk(tmp_path, monkeypatch):
    """Make the seeded sess-1 resumable on disk.

    _process_queued_message now checks the session JSONL exists before resuming
    (recovers if gone — prod task #725). Drop a sess-1 JSONL under a temp
    CLAUDE_CONFIG_DIR (globbed across project subdirs) so the resume path — not
    the recovery path — is what these tests exercise.
    """
    import backend.main as main_mod
    monkeypatch.setattr(main_mod, "dispatcher", None, raising=False)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))
    proj = tmp_path / "projects" / "-tmp"
    proj.mkdir(parents=True)
    (proj / "sess-1.jsonl").write_text("{}\n")
    return tmp_path


@pytest.mark.asyncio
async def test_process_queued_message_uses_task_model(db_factory, fake_session_on_disk):
    """Queued message launch resumes with task.model (the model that created
    the session), not any instance-level model."""
    dispatcher = _make_dispatcher(db_factory)
    task_id = await _seed_task_for_queue(db_factory, model="claude-opus-4-6", last_cwd="/tmp")

    await dispatcher._process_queued_message(task_id, _queued())

    dispatcher.instance_manager.launch.assert_awaited_once()
    kwargs = dispatcher.instance_manager.launch.call_args.kwargs
    assert kwargs["model"] == "claude-opus-4-6"
    assert kwargs["resume_session_id"] == "sess-1"
    assert kwargs["prompt"].endswith(
        "</ccm_task_artifact_policy>\n\nhi"
    )
    assert kwargs["chat_initiated"] is True


@pytest.mark.asyncio
async def test_process_queued_message_effort_uses_task_effort(db_factory, fake_session_on_disk):
    """Queued message launch uses task.effort_level."""
    dispatcher = _make_dispatcher(db_factory)
    task_id = await _seed_task_for_queue(db_factory, effort_level="high", last_cwd="/tmp")

    await dispatcher._process_queued_message(task_id, _queued())

    kwargs = dispatcher.instance_manager.launch.call_args.kwargs
    assert kwargs["effort_level"] == "high"


@pytest.mark.asyncio
async def test_process_queued_message_cwd_uses_last_cwd(db_factory, fake_session_on_disk):
    """When last_cwd is set, the launch cwd uses it."""
    dispatcher = _make_dispatcher(db_factory)
    task_id = await _seed_task_for_queue(db_factory, last_cwd="/tmp/somewhere")

    await dispatcher._process_queued_message(task_id, _queued())

    kwargs = dispatcher.instance_manager.launch.call_args.kwargs
    assert kwargs["cwd"] == "/tmp/somewhere"


@pytest.mark.asyncio
async def test_process_queued_message_cwd_falls_back_to_target_repo(db_factory, fake_session_on_disk):
    """Without last_cwd, the launch cwd falls back to task.target_repo
    (the old endpoint-level 400-on-missing-cwd check no longer exists)."""
    dispatcher = _make_dispatcher(db_factory)
    task_id = await _seed_task_for_queue(db_factory, last_cwd=None)

    await dispatcher._process_queued_message(task_id, _queued())

    kwargs = dispatcher.instance_manager.launch.call_args.kwargs
    assert kwargs["cwd"] == "/tmp"


@pytest.mark.asyncio
async def test_chat_send_with_model_override(client, session_factory):
    """临时模型：body.model 透传为 enqueue 的 model_override，不落库。"""
    from backend.models.task import Task

    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="claude",
        model="claude-sonnet-4-6",
    )

    mock_d = _mock_dispatcher()
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_d), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={
                "message": "hard problem",
                "model": "claude-opus-4-8",
                "expected_routing": {
                    "provider": "claude",
                    "model": "claude-opus-4-8",
                    "codex_service_tier": "default",
                },
            },
        )

    assert resp.status_code == 200
    kwargs = mock_d.enqueue_message.call_args.kwargs
    assert kwargs["model_override"] == "claude-opus-4-8"
    assert kwargs["expected_task_routing"] == (
        "claude",
        "claude-opus-4-8",
        "default",
    )

    # task.model 不被修改
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.model != "claude-opus-4-8"


@pytest.mark.asyncio
async def test_chat_stale_fast_view_rejected_before_logging(
    client,
    session_factory,
):
    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    dispatcher = _mock_dispatcher()

    with patch("backend.main.dispatcher", dispatcher):
        response = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={
                "message": "must not run Standard",
                "expected_routing": {
                    "provider": "codex",
                    "model": "gpt-5.6-sol",
                    "codex_service_tier": "priority",
                },
            },
        )

    assert response.status_code == 409
    dispatcher.enqueue_message.assert_not_awaited()
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count())
            .select_from(LogEntry)
            .where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )
    assert count == 0


@pytest.mark.asyncio
async def test_codex_fast_rejects_unsupported_chat_model_before_logging(
    client,
    session_factory,
):
    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    mock_d = _mock_dispatcher()

    with patch("backend.main.dispatcher", mock_d):
        resp = await client.post(
            f"/api/tasks/{task_id}/chat",
            json={"message": "do not persist", "model": "gpt-5.4-mini"},
        )

    assert resp.status_code == 422
    assert "not supported" in resp.json()["detail"]
    mock_d.enqueue_message.assert_not_awaited()
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count())
            .select_from(LogEntry)
            .where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )
    assert count == 0


@pytest.mark.asyncio
async def test_update_task_model_persists(client, session_factory):
    """持久模型切换：PATCH/PUT task.model 生效。"""
    from backend.models.task import Task

    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="claude",
    )
    resp = await client.put(
        f"/api/tasks/{task_id}", json={"model": "claude-sonnet-4-6"}
    )
    assert resp.status_code == 200
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.model == "claude-sonnet-4-6"


@pytest.mark.asyncio
async def test_inject_capabilities_advertise_attachment_protocol(
    client,
    session_factory,
):
    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="codex",
    )

    response = await client.get(
        f"/api/tasks/{task_id}/inject-capabilities",
    )

    assert response.status_code == 200
    assert response.json() == {
        "attachment_protocol": 1,
        "codex_native_inputs": True,
    }


@pytest.mark.asyncio
async def test_inject_requires_pty_mode(client, session_factory):
    """PTY 模式关闭时注入返回 400。"""
    task_id = await _create_task_with_session(
        client, session_factory, provider="claude"
    )

    mock_im = MagicMock()
    mock_im.pty_mode_enabled = False
    mock_im.has_pty_session = MagicMock(return_value=False)
    with patch("backend.main.instance_manager", mock_im), \
         patch("backend.main.broadcaster", MagicMock(broadcast=AsyncMock())):
        resp = await client.post(
            f"/api/tasks/{task_id}/inject", json={"message": "hint"}
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_inject_rejects_direct_turn_when_global_pty_is_enabled(
    client, session_factory
):
    task_id = await _create_task_with_session(
        client, session_factory, provider="claude"
    )
    mock_im = MagicMock()
    mock_im.pty_mode_enabled = True
    mock_im.has_pty_session = MagicMock(return_value=False)
    mock_im.inject_pty_message = AsyncMock()

    with patch("backend.main.instance_manager", mock_im), \
         patch(
             "backend.main.broadcaster",
             MagicMock(broadcast=AsyncMock()),
         ):
        resp = await client.post(
            f"/api/tasks/{task_id}/inject", json={"message": "hint"}
        )

    assert resp.status_code == 400
    assert "直连进程" in resp.json()["detail"]
    mock_im.inject_pty_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_inject_delivers_to_pty_session(client, session_factory):
    """An in-flight PTY session remains injectable after the global toggle."""
    from backend.models.task import Task

    task_id = await _create_task_with_session(
        client, session_factory, provider="claude"
    )

    mock_im = MagicMock()
    mock_im.pty_mode_enabled = False
    mock_im.has_pty_session = MagicMock(return_value=True)
    mock_im.inject_pty_message = AsyncMock(return_value=True)
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.instance_manager", mock_im), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(
            f"/api/tasks/{task_id}/inject", json={"message": "focus on tests"}
        )

    assert resp.status_code == 200
    # 回归：chat 路径不更新 task.instance_id，必须按 session_id 定位 PTY 会话
    mock_im.inject_pty_message.assert_awaited_once_with(
        "test-session-123", "focus on tests"
    )
    casts = [c for c in mock_broadcaster.broadcast.call_args_list
             if c[0][1].get("source") == "inject"]
    assert len(casts) == 1
    event = casts[0][0][1]
    assert event["raw_content"] == "focus on tests"
    assert event["task_id"] == task_id
    assert isinstance(event["id"], int)
    assert event["id"] > 0
    assert event["timestamp"].endswith("Z")

    async with session_factory() as db:
        stored = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "user_message",
                )
            )
        ).scalar_one()
    assert event["id"] == stored.id


@pytest.mark.asyncio
async def test_inject_delivers_uploaded_image_to_pty_and_persists_metadata(
    client,
    session_factory,
    monkeypatch,
    tmp_path,
):
    """A file-only PTY injection must carry a readable path and attachment UI."""
    saved_name = "11111111-1111-4111-8111-111111111111.png"
    upload_path = tmp_path / saved_name
    upload_path.write_bytes(b"not-a-real-png")
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", tmp_path)
    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="claude",
    )

    mock_im = MagicMock()
    mock_im.pty_mode_enabled = True
    mock_im.has_pty_session = MagicMock(return_value=True)
    mock_im.inject_pty_message = AsyncMock(return_value=True)
    mock_broadcaster = MagicMock(broadcast=AsyncMock())

    with patch("backend.main.instance_manager", mock_im), patch(
        "backend.main.broadcaster",
        mock_broadcaster,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/inject",
            json={
                "message": "",
                "file_paths": [str(upload_path)],
                "image_paths": [str(upload_path)],
                "attachments": [{
                    "url": f"/api/uploads/{saved_name}",
                    "name": "screenshot.png",
                    "is_image": True,
                }],
            },
        )

    assert response.status_code == 200
    assert response.json()["attachment_count"] == 1
    injected = mock_im.inject_pty_message.await_args
    assert injected.args[0] == "test-session-123"
    assert str(upload_path) in injected.args[1]
    assert injected.kwargs == {"require_host_file_access": True}

    events = [
        call.args[1]
        for call in mock_broadcaster.broadcast.call_args_list
        if call.args[1].get("source") == "inject"
    ]
    assert len(events) == 1
    assert events[0]["attachments"] == [{
        "url": f"/api/uploads/{saved_name}",
        "name": "screenshot.png",
        "is_image": True,
    }]
    assert events[0]["image_urls"] == [f"/api/uploads/{saved_name}"]

    async with session_factory() as db:
        stored = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "user_message",
                )
            )
        ).scalar_one()
    metadata = json.loads(stored.raw_json)
    assert events[0]["id"] == stored.id
    assert events[0]["task_id"] == task_id
    assert events[0]["timestamp"].endswith("Z")
    assert metadata["source"] == "inject"
    assert metadata["raw_content"] == ""
    assert metadata["file_paths"] == [str(upload_path)]
    assert metadata["image_paths"] == [str(upload_path)]
    assert metadata["attachments"] == events[0]["attachments"]


@pytest.mark.asyncio
async def test_inject_no_live_session_409(client, session_factory):
    """会话不存活时注入返回 409。"""
    from backend.models.task import Task

    task_id = await _create_task_with_session(
        client, session_factory, provider="claude"
    )

    mock_im = MagicMock()
    mock_im.pty_mode_enabled = True
    mock_im.has_pty_session = MagicMock(return_value=True)
    mock_im.inject_pty_message = AsyncMock(return_value=False)
    with patch("backend.main.instance_manager", mock_im), \
         patch("backend.main.broadcaster", MagicMock(broadcast=AsyncMock())):
        resp = await client.post(
            f"/api/tasks/{task_id}/inject", json={"message": "x"}
        )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_codex_inject_steers_without_pty_mode(
    client, session_factory, monkeypatch
):
    """Codex injection uses app-server steering and is independent of PTY."""
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    task_id = await _create_task_with_session(
        client, session_factory, provider="codex"
    )
    mock_im = MagicMock()
    mock_im.pty_mode_enabled = False
    mock_im.inject_codex_message = AsyncMock(return_value=True)
    mock_broadcaster = MagicMock(broadcast=AsyncMock())

    with patch("backend.main.instance_manager", mock_im), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(
            f"/api/tasks/{task_id}/inject", json={"message": "steer now"}
        )

    assert resp.status_code == 200
    mock_im.inject_codex_message.assert_awaited_once_with(
        "test-session-123", "steer now"
    )
    injected = [
        call for call in mock_broadcaster.broadcast.call_args_list
        if call.args[1].get("source") == "inject"
    ]
    assert len(injected) == 1
    async with session_factory() as db:
        stored = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "user_message",
                )
            )
        ).scalar_one()
    event = injected[0].args[1]
    assert event["id"] == stored.id
    assert event["task_id"] == task_id
    assert event["timestamp"].endswith("Z")


@pytest.mark.asyncio
async def test_codex_inject_uses_native_image_and_file_inputs(
    client,
    session_factory,
    monkeypatch,
    tmp_path,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", tmp_path)
    image_saved_name = "22222222-2222-4222-8222-222222222222.png"
    file_saved_name = "33333333-3333-4333-8333-333333333333.txt"
    image_path = tmp_path / image_saved_name
    file_path = tmp_path / file_saved_name
    image_path.write_bytes(b"image")
    file_path.write_text("details", encoding="utf-8")
    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="codex",
    )
    mock_im = MagicMock()
    mock_im.inject_codex_message = AsyncMock(return_value=True)
    mock_broadcaster = MagicMock(broadcast=AsyncMock())

    with patch("backend.main.instance_manager", mock_im), patch(
        "backend.main.broadcaster",
        mock_broadcaster,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/inject",
            json={
                "message": "Use both attachments",
                "file_paths": [str(image_path), str(file_path)],
                "image_paths": [str(image_path)],
                "attachments": [
                    {
                        "url": f"/api/uploads/{image_saved_name}",
                        "name": "evidence.png",
                        "is_image": True,
                    },
                    {
                        "url": f"/api/uploads/{file_saved_name}",
                        "name": "notes.txt",
                        "is_image": False,
                    },
                ],
            },
        )

    assert response.status_code == 200
    injected = mock_im.inject_codex_message.await_args
    assert injected.args[0] == "test-session-123"
    assert str(image_path) in injected.args[1]
    assert str(file_path) in injected.args[1]
    assert injected.kwargs["input_items"] == [
        {"type": "text", "text": injected.args[1]},
        {"type": "localImage", "path": str(image_path)},
        {"type": "mention", "name": "notes.txt", "path": str(file_path)},
    ]


@pytest.mark.asyncio
async def test_inject_rejects_non_upload_path_without_side_effects(
    client,
    session_factory,
    monkeypatch,
    tmp_path,
):
    from backend.config import settings

    upload_dir = tmp_path / "uploads"
    outside_path = tmp_path / "outside.txt"
    outside_path.write_text("must not be injected", encoding="utf-8")
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", upload_dir)
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="codex",
    )
    mock_im = MagicMock()
    mock_im.inject_codex_message = AsyncMock(return_value=True)
    mock_broadcaster = MagicMock(broadcast=AsyncMock())

    with patch("backend.main.instance_manager", mock_im), patch(
        "backend.main.broadcaster",
        mock_broadcaster,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/inject",
            json={
                "message": "read this",
                "file_paths": [str(outside_path)],
            },
        )

    assert response.status_code == 422
    assert "upload directory" in response.json()["detail"]
    mock_im.inject_codex_message.assert_not_awaited()
    mock_broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count())
            .select_from(LogEntry)
            .where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )
    assert count == 0


@pytest.mark.asyncio
async def test_inject_container_attachment_fails_before_persisting(
    client,
    session_factory,
    monkeypatch,
    tmp_path,
):
    from backend.services.instance_manager import (
        LiveAttachmentInjectionUnsupportedError,
    )

    upload_path = (
        tmp_path / "44444444-4444-4444-8444-444444444444.txt"
    )
    upload_path.write_text("evidence", encoding="utf-8")
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", tmp_path)
    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="claude",
    )
    mock_im = MagicMock()
    mock_im.pty_mode_enabled = True
    mock_im.has_pty_session = MagicMock(return_value=True)
    mock_im.inject_pty_message = AsyncMock(
        side_effect=LiveAttachmentInjectionUnsupportedError(
            "container cannot read manager upload",
        )
    )
    mock_broadcaster = MagicMock(broadcast=AsyncMock())

    with patch("backend.main.instance_manager", mock_im), patch(
        "backend.main.broadcaster",
        mock_broadcaster,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/inject",
            json={
                "message": "inspect",
                "file_paths": [str(upload_path)],
            },
        )

    assert response.status_code == 409
    assert "隔离容器" in response.json()["detail"]
    mock_broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count())
            .select_from(LogEntry)
            .where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            )
        )
    assert count == 0


@pytest.mark.asyncio
async def test_codex_inject_rejects_stale_fast_view_before_steer(
    client,
    session_factory,
    monkeypatch,
):
    """A stale Fast tab must not steer a turn whose Task is now Standard."""
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    task_id = await _create_task_with_session(
        client,
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    mock_im = MagicMock()
    mock_im.inject_codex_message = AsyncMock(return_value=True)

    with patch("backend.main.instance_manager", mock_im), \
         patch(
             "backend.main.broadcaster",
             MagicMock(broadcast=AsyncMock()),
         ):
        response = await client.post(
            f"/api/tasks/{task_id}/inject",
            json={
                "message": "must remain Fast",
                "expected_routing": {
                    "provider": "codex",
                    "model": "gpt-5.6-sol",
                    "codex_service_tier": "priority",
                },
            },
        )

    assert response.status_code == 409
    mock_im.inject_codex_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_inject_without_live_app_server_turn_returns_409(
    client, session_factory, monkeypatch
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    task_id = await _create_task_with_session(
        client, session_factory, provider="codex"
    )
    mock_im = MagicMock()
    mock_im.inject_codex_message = AsyncMock(return_value=False)

    with patch("backend.main.instance_manager", mock_im), \
         patch("backend.main.broadcaster", MagicMock(broadcast=AsyncMock())):
        resp = await client.post(
            f"/api/tasks/{task_id}/inject", json={"message": "too late"}
        )

    assert resp.status_code == 409
    assert "exec fallback" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_codex_inject_requires_app_server_enabled(
    client, session_factory, monkeypatch
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    task_id = await _create_task_with_session(
        client, session_factory, provider="codex"
    )
    mock_im = MagicMock()
    mock_im.inject_codex_message = AsyncMock()

    with patch("backend.main.instance_manager", mock_im), \
         patch("backend.main.broadcaster", MagicMock(broadcast=AsyncMock())):
        resp = await client.post(
            f"/api/tasks/{task_id}/inject", json={"message": "x"}
        )

    assert resp.status_code == 400
    mock_im.inject_codex_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_inject_rejects_remote_worker_task(client, session_factory):
    task_id = await _create_task_with_session(
        client, session_factory, provider="codex", worker_id=7
    )
    mock_im = MagicMock()
    mock_im.inject_codex_message = AsyncMock()

    with patch("backend.main.instance_manager", mock_im), patch(
        "backend.main.broadcaster",
        MagicMock(broadcast=AsyncMock()),
    ):
        resp = await client.post(
            f"/api/tasks/{task_id}/inject", json={"message": "x"}
        )

    assert resp.status_code == 400
    assert "Worker" in resp.json()["detail"]
    mock_im.inject_codex_message.assert_not_awaited()
