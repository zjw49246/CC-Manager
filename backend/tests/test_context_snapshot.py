import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from backend.models.log_entry import LogEntry
from backend.models.sub_agent import SubAgentSession
from backend.models.task import Task
from backend.services.context_compaction import build_compacted_resume_prompt
from backend.services.context_snapshot import (
    CONTEXT_SNAPSHOT_EVENT_TYPE,
    CONTEXT_SNAPSHOT_TYPE,
)
from backend.services.dispatcher import GlobalDispatcher


def _dispatcher(db_factory) -> GlobalDispatcher:
    instance_manager = MagicMock()
    broadcaster = MagicMock()
    return GlobalDispatcher(db_factory, instance_manager, broadcaster)


@pytest.mark.asyncio
async def test_compaction_persists_structured_work_state_before_session_reset(
    db_factory,
):
    async with db_factory() as db:
        task = Task(
            title="durable context recovery",
            description="Keep the implementation behavior stable",
            status="failed",
            provider="claude",
            model="claude-opus-5",
            mode="auto",
            target_repo="/workspace/project",
            target_branch="main",
            result_branch="fix/context-recovery",
            last_cwd="/workspace/project",
            session_id="old-native-session",
            retry_count=2,
            turn_generation=9,
        )
        db.add(task)
        await db.flush()
        db.add(
            LogEntry(
                task_id=task.id,
                event_type="user_message",
                role="user",
                content="inspect the attached report",
                raw_json=json.dumps(
                    {
                        "raw_content": "inspect the attached report",
                        "file_paths": ["/workspace/uploads/report.pdf"],
                        "attachments": [
                            {
                                "name": "report.pdf",
                                "path": "/workspace/uploads/report.pdf",
                                "url": "/api/uploads/report.pdf",
                                "is_image": False,
                            }
                        ],
                    }
                ),
                is_error=False,
            )
        )
        db.add_all(
            [
                LogEntry(
                    task_id=task.id,
                    event_type="tool_use",
                    role="assistant",
                    tool_name="Edit",
                    tool_input=json.dumps(
                        {
                            "file_path": "/workspace/project/backend/app.py",
                            "old_string": "old behavior",
                            "new_string": (
                                "OPENAI_API_KEY=sk-secret-value-that-must-hide"
                            ),
                        }
                    ),
                    is_error=False,
                ),
                LogEntry(
                    task_id=task.id,
                    event_type="tool_result",
                    role="tool",
                    tool_output="Updated backend/app.py",
                    is_error=False,
                ),
                LogEntry(
                    task_id=task.id,
                    event_type="tool_use",
                    role="assistant",
                    tool_name="FileChange",
                    tool_input=json.dumps(
                        {
                            "changes": [
                                {
                                    "path": "/workspace/project/frontend/app.tsx",
                                    "kind": {"type": "update"},
                                    "diff": "a very large patch body is not recovery state",
                                }
                            ]
                        }
                    ),
                    is_error=False,
                ),
                LogEntry(
                    task_id=task.id,
                    event_type="tool_result",
                    role="tool",
                    tool_name="FileChange",
                    tool_output="Patch completed for frontend/app.tsx",
                    is_error=False,
                ),
                LogEntry(
                    task_id=task.id,
                    event_type="tool_use",
                    role="assistant",
                    tool_name="Bash",
                    tool_input=json.dumps(
                        {
                            "command": "git status --short && git rev-parse HEAD",
                            "description": "Capture repository state",
                        }
                    ),
                    is_error=False,
                ),
                LogEntry(
                    task_id=task.id,
                    event_type="tool_result",
                    role="tool",
                    tool_output=(
                        " M backend/app.py\n"
                        "0123456789abcdef\n"
                        "https://example.invalid/check?token=secret-query-value"
                    ),
                    is_error=False,
                ),
                LogEntry(
                    task_id=task.id,
                    event_type="result",
                    role="assistant",
                    content="Implementation is complete and focused tests pass.",
                    is_error=False,
                ),
            ]
        )
        db.add(
            SubAgentSession(
                task_id=task.id,
                agent_type="monitor",
                description="Watch the focused test run",
                status="completed",
                last_summary="All focused tests passed",
            )
        )
        await db.flush()
        current = LogEntry(
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="CURRENT_MESSAGE: continue from the saved state",
            raw_json=json.dumps(
                {"raw_content": "CURRENT_MESSAGE: continue from the saved state"}
            ),
            is_error=False,
        )
        db.add(current)
        await db.flush()
        task.turn_source_log_id = current.id
        await db.commit()

        summary = await _dispatcher(db_factory)._compact_session(
            task.id,
            "old-native-session",
            db,
            reason="prompt_too_long",
            exclude_log_entry_id=current.id,
        )
        assert summary is not None
        task.session_id = None
        await db.commit()

        snapshot = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task.id,
                    LogEntry.event_type == CONTEXT_SNAPSHOT_EVENT_TYPE,
                )
            )
        ).scalar_one()
        payload = json.loads(snapshot.raw_json)

    assert payload["type"] == CONTEXT_SNAPSHOT_TYPE
    assert payload["reason"] == "prompt_too_long"
    assert payload["source"] == {
        "before_log_entry_id": current.id,
        "previous_snapshot_log_id": None,
        "retry_count": 2,
        "session_id": "old-native-session",
        "turn_generation": 9,
        "turn_source_log_id": current.id,
    }
    assert "## 持久化工作状态快照" in summary
    assert "Implementation is complete and focused tests pass." in summary
    assert "Updated backend/app.py" in summary
    assert "git status --short && git rev-parse HEAD" in summary
    assert "/workspace/project/backend/app.py" in summary
    assert "/workspace/project/frontend/app.tsx" in summary
    assert "a very large patch body is not recovery state" not in summary
    assert "/workspace/uploads/report.pdf" in summary
    assert "Watch the focused test run" in summary
    assert "All focused tests passed" in summary
    assert "sk-secret-value-that-must-hide" not in snapshot.raw_json
    assert "secret-query-value" not in snapshot.raw_json
    assert "[redacted]" in snapshot.raw_json

    resumed = build_compacted_resume_prompt(
        summary,
        "CURRENT_MESSAGE: continue from the saved state",
    )
    assert resumed.count("CURRENT_MESSAGE: continue from the saved state") == 1
    assert "持久化工作状态快照" in resumed
    assert summary.index("## 近期对话") < summary.index(
        "## 持久化工作状态快照"
    ) < summary.index("## 原始任务背景")


@pytest.mark.asyncio
async def test_repeated_compaction_inherits_snapshot_newer_than_reused_source(
    db_factory,
):
    async with db_factory() as db:
        task = Task(
            title="repeat compact retry",
            description="preserve prior work",
            status="failed",
            target_repo="/workspace/project",
            session_id="session-one",
            turn_generation=3,
        )
        db.add(task)
        await db.flush()
        db.add_all(
            [
                LogEntry(
                    task_id=task.id,
                    event_type="tool_use",
                    role="assistant",
                    tool_name="Write",
                    tool_input=json.dumps(
                        {"file_path": "/workspace/project/kept.py"}
                    ),
                ),
                LogEntry(
                    task_id=task.id,
                    event_type="tool_result",
                    role="tool",
                    tool_output="created kept.py",
                ),
            ]
        )
        await db.flush()
        source = LogEntry(
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="continue",
            raw_json=json.dumps({"raw_content": "continue"}),
        )
        db.add(source)
        await db.commit()

        dispatcher = _dispatcher(db_factory)
        first = await dispatcher._compact_session(
            task.id,
            "session-one",
            db,
            reason="prompt_too_long",
            exclude_log_entry_id=source.id,
        )
        assert first is not None
        await db.commit()
        first_snapshot = (
            await db.execute(
                select(LogEntry)
                .where(
                    LogEntry.task_id == task.id,
                    LogEntry.event_type == CONTEXT_SNAPSHOT_EVENT_TYPE,
                )
                .order_by(LogEntry.id.asc())
            )
        ).scalars().one()

        task.session_id = "session-two"
        task.turn_generation = 4
        await db.commit()
        second = await dispatcher._compact_session(
            task.id,
            "session-two",
            db,
            reason="prompt_too_long",
            # Automatic compact retry reuses this older source id.
            exclude_log_entry_id=source.id,
        )
        assert second is not None
        await db.commit()
        snapshots = list(
            (
                await db.execute(
                    select(LogEntry)
                    .where(
                        LogEntry.task_id == task.id,
                        LogEntry.event_type == CONTEXT_SNAPSHOT_EVENT_TYPE,
                    )
                    .order_by(LogEntry.id.asc())
                )
            ).scalars()
        )

    assert len(snapshots) == 2
    second_payload = json.loads(snapshots[1].raw_json)
    assert second_payload["source"]["previous_snapshot_log_id"] == first_snapshot.id
    assert "/workspace/project/kept.py" in second
    assert "created kept.py" in second
    serialized_state = json.dumps(second_payload["state"], ensure_ascii=False)
    assert "previous_snapshot_log_id" not in serialized_state
    assert CONTEXT_SNAPSHOT_TYPE not in serialized_state


@pytest.mark.asyncio
async def test_snapshot_failure_leaves_transaction_usable_and_session_intact(
    db_factory,
):
    async with db_factory() as db:
        task = Task(
            title="snapshot failure",
            description="do not clear the session",
            status="failed",
            target_repo="/workspace/project",
            session_id="must-remain",
        )
        db.add(task)
        await db.commit()
        task_id = task.id
        session_id = task.session_id

        with patch(
            "backend.services.dispatcher.capture_context_recovery_snapshot",
            new=AsyncMock(side_effect=RuntimeError("snapshot store failed")),
        ):
            summary = await _dispatcher(db_factory)._compact_session(
                task_id,
                session_id,
                db,
                reason="prompt_too_long",
            )

        assert summary is None
        # The rollback must leave the caller's session usable for its exact-
        # generation fail-closed checks.
        current = await db.get(Task, task_id, populate_existing=True)
        assert current.session_id == "must-remain"
        db.add(
            LogEntry(
                task_id=task_id,
                event_type="system_event",
                role="system",
                content="fail-closed verification",
                is_error=False,
            )
        )
        await db.commit()
        snapshots = list(
            (
                await db.execute(
                    select(LogEntry).where(
                        LogEntry.task_id == task_id,
                        LogEntry.event_type == CONTEXT_SNAPSHOT_EVENT_TYPE,
                    )
                )
            ).scalars()
        )

    assert snapshots == []


@pytest.mark.asyncio
async def test_snapshot_rolls_back_with_lost_session_reset_cas(db_factory):
    async with db_factory() as db:
        task = Task(
            title="lost compaction cas",
            description="snapshot and reset are atomic",
            status="failed",
            target_repo="/workspace/project",
            session_id="still-authoritative",
            turn_generation=6,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

        summary = await _dispatcher(db_factory)._compact_session(
            task_id,
            task.session_id,
            db,
            reason="prompt_too_long",
        )
        assert summary is not None
        # This is the caller behavior when its exact Task/session update loses
        # a concurrent CAS. The hidden snapshot must not survive on its own.
        await db.rollback()

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        snapshots = list(
            (
                await db.execute(
                    select(LogEntry).where(
                        LogEntry.task_id == task_id,
                        LogEntry.event_type == CONTEXT_SNAPSHOT_EVENT_TYPE,
                    )
                )
            ).scalars()
        )

    assert current.session_id == "still-authoritative"
    assert snapshots == []
