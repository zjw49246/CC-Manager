"""Database-enforced invariants for the durable capability resume outbox."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import IntegrityError

from backend.models.capability import (
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.log_entry import LogEntry
from backend.models.task import Task


_DIGEST = "a" * 64
_INCARNATION = "b" * 32
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _run_migration(db_path: str, command_fn, revision: str) -> None:
    cfg = Config(str(_PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_PROJECT_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    with patch(
        "backend.config.settings.database_url",
        f"sqlite+aiosqlite:///{db_path}",
    ):
        command_fn(cfg, revision)


async def _reload(session, outbox_id: int) -> CapabilityResumeOutbox:
    return await session.scalar(
        select(CapabilityResumeOutbox).where(
            CapabilityResumeOutbox.id == outbox_id
        )
    )


@pytest.mark.asyncio
async def test_claim_and_provider_launch_are_distinct_durable_boundaries(
    db_factory,
):
    async with db_factory() as session:
        task = Task(
            title="Capability resume target",
            description="schema exercise",
            status="waiting_capability",
            incarnation_id=_INCARNATION,
            retry_count=0,
            turn_generation=7,
        )
        session.add(task)
        await session.flush()
        source = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=7,
            event_type="user_message",
            role="user",
            content="source",
            turn_scope="source",
            actual_transport="codex_app_server",
        )
        output = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=7,
            event_type="result",
            role="assistant",
            content="request capability",
            turn_scope="foreground",
        )
        terminal = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=7,
            event_type="system_event",
            content="turn completed",
            turn_scope="foreground",
        )
        resume_source = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=8,
            event_type="user_message",
            role="user",
            content="capability result",
            turn_scope="source",
            actual_transport="codex_app_server",
        )
        session.add_all((source, output, terminal, resume_source))
        await session.flush()
        invocation = CapabilityInvocation(
            task_id=task.id,
            capability_key="plan",
            source="agent_request",
            purpose="advisory",
            status="running",
            state_version=1,
            idempotency_key="agent-request-turn-7",
            input_payload={"prompt": "plan"},
            input_hash=_DIGEST,
            subject_kind="task_generation",
            subject_ref={"task_id": task.id, "turn_generation": 7},
            subject_hash=_DIGEST,
            executor_kind="plan_agent",
            executor_config={},
            executor_config_hash=_DIGEST,
            policy_snapshot={},
            policy_hash=_DIGEST,
            resume_policy="resume_task",
            max_attempts=1,
            active_task_id=task.id,
            request_task_incarnation_id=_INCARNATION,
            request_task_retry_count=0,
            request_task_session_id="thread-1",
            request_task_turn_generation=7,
            request_source_log_id=source.id,
            request_output_log_id=output.id,
            request_terminal_log_id=terminal.id,
            request_reason="Need a plan",
            request_protocol_version=1,
            request_output_hash=_DIGEST,
            request_native_turn_id="turn-7",
        )
        session.add(invocation)
        await session.flush()
        outbox = CapabilityResumeOutbox(
            task_id=task.id,
            invocation_id=invocation.id,
            active_task_id=task.id,
            active_invocation_id=invocation.id,
            status="pending",
            request_task_incarnation_id=_INCARNATION,
            request_task_retry_count=0,
            from_turn_generation=7,
            request_task_session_id="thread-1",
            request_source_log_id=source.id,
            request_output_log_id=output.id,
            request_terminal_log_id=terminal.id,
            request_native_turn_id="turn-7",
        )
        session.add(outbox)
        await session.commit()
        outbox_id = outbox.id
        invocation_id = invocation.id

    # A coordinator cannot skip the exact Task claim evidence.
    async with db_factory() as session:
        outbox = await _reload(session, outbox_id)
        outbox.status = "claimed"
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

    ready_at = datetime.utcnow()
    async with db_factory() as session:
        invocation = await session.get(CapabilityInvocation, invocation_id)
        invocation.status = "failed"
        invocation.active_task_id = None
        invocation.error_code = "executor_failed"
        invocation.error_message = "planner unavailable"
        invocation.completed_at = ready_at
        outbox = await _reload(session, outbox_id)
        outbox.status = "ready"
        outbox.invocation_terminal_status = "failed"
        outbox.invocation_error_code = invocation.error_code
        outbox.invocation_error_message = invocation.error_message
        outbox.resume_payload = {
            "status": "failed",
            "error_code": invocation.error_code,
        }
        outbox.resume_payload_hash = _DIGEST
        outbox.ready_at = ready_at
        outbox.updated_at = ready_at
        await session.commit()

    async with db_factory() as session:
        outbox = await _reload(session, outbox_id)
        outbox.status = "claiming"
        outbox.attempt_count = 1
        outbox.lease_token = "c" * 64
        outbox.lease_expires_at = ready_at + timedelta(minutes=1)
        await session.commit()

    claimed_at = ready_at + timedelta(seconds=1)
    async with db_factory() as session:
        outbox = await _reload(session, outbox_id)
        outbox.status = "claimed"
        outbox.resume_source_log_id = resume_source.id
        outbox.claimed_turn_generation = 8
        outbox.claimed_at = claimed_at
        outbox.lease_token = None
        outbox.lease_expires_at = None
        outbox.next_attempt_at = claimed_at + timedelta(seconds=5)
        await session.commit()

    # Task G+1 being claimed is not proof that a provider accepted the turn.
    async with db_factory() as session:
        outbox = await _reload(session, outbox_id)
        outbox.status = "launched"
        outbox.next_attempt_at = None
        outbox.active_task_id = None
        outbox.active_invocation_id = None
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

    launched_at = claimed_at + timedelta(seconds=1)
    async with db_factory() as session:
        outbox = await _reload(session, outbox_id)
        outbox.status = "launched"
        outbox.next_attempt_at = None
        outbox.active_task_id = None
        outbox.active_invocation_id = None
        # The coordinator may copy this value only after checking the exact
        # resume source LogEntry.actual_transport in the same transaction.
        outbox.resume_actual_transport = resume_source.actual_transport
        outbox.launched_at = launched_at
        await session.commit()

    async with db_factory() as session:
        outbox = await _reload(session, outbox_id)
        assert outbox.status == "launched"
        assert outbox.resume_source_log_id == resume_source.id
        assert outbox.claimed_turn_generation == 8
        outbox.status = "completed"
        outbox.completed_at = launched_at + timedelta(seconds=1)
        await session.commit()


@pytest.mark.asyncio
async def test_agent_invocation_requires_incarnation_and_terminal_envelope(
    db_factory,
):
    async with db_factory() as session:
        task = Task(
            title="Incomplete request identity",
            description="schema exercise",
            status="waiting_capability",
            incarnation_id="d" * 32,
            retry_count=0,
            turn_generation=1,
        )
        session.add(task)
        await session.flush()
        invocation = CapabilityInvocation(
            task_id=task.id,
            capability_key="plan",
            source="agent_request",
            purpose="advisory",
            status="failed",
            state_version=1,
            idempotency_key="missing-terminal-envelope",
            input_payload={},
            input_hash=_DIGEST,
            subject_kind="task_generation",
            subject_ref={},
            subject_hash=_DIGEST,
            executor_kind="plan_agent",
            executor_config={},
            executor_config_hash=_DIGEST,
            policy_snapshot={},
            policy_hash=_DIGEST,
            resume_policy="resume_task",
            max_attempts=1,
            request_task_retry_count=0,
            request_task_turn_generation=1,
            request_source_log_id=1,
            request_output_log_id=2,
            request_reason="Need a plan",
            request_protocol_version=1,
            request_output_hash=_DIGEST,
        )
        session.add(invocation)
        with pytest.raises(IntegrityError):
            await session.commit()


def test_migration_downgrade_refuses_outbox_or_agent_audit_history(tmp_path):
    db_path = str(tmp_path / "resume-outbox-downgrade.db")
    _run_migration(db_path, command.upgrade, "head")
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, "
                "turn_generation, incarnation_id, created_at) VALUES "
                "('resume downgrade', 'd', 'waiting_capability', 0, "
                "'main', 'pending', 0, 2, 'auto', 3, :incarnation, "
                "'2026-08-07 00:00:00')"
            ),
            {"incarnation": _INCARNATION},
        )
        task_id = connection.execute(
            text("SELECT id FROM tasks WHERE title = 'resume downgrade'")
        ).scalar_one()
        connection.execute(
            text(
                "INSERT INTO capability_invocations "
                "(task_id, capability_key, source, purpose, status, "
                "state_version, idempotency_key, input_payload, input_hash, "
                "subject_kind, subject_ref, subject_hash, executor_kind, "
                "executor_config, executor_config_hash, policy_snapshot, "
                "policy_hash, resume_policy, max_attempts, active_task_id, "
                "created_at, updated_at) VALUES "
                "(:task_id, 'plan', 'human_request', 'advisory', 'failed', "
                "1, 'resume-downgrade', '{}', :digest, 'task_generation', "
                "'{}', :digest, 'plan_agent', '{}', :digest, '{}', :digest, "
                "'attach_only', 1, NULL, '2026-08-07 00:00:01', "
                "'2026-08-07 00:00:01')"
            ),
            {"task_id": task_id, "digest": _DIGEST},
        )
        invocation_id = connection.execute(
            text(
                "SELECT id FROM capability_invocations "
                "WHERE idempotency_key = 'resume-downgrade'"
            )
        ).scalar_one()
        connection.execute(
            text(
                "INSERT INTO capability_resume_outbox "
                "(task_id, invocation_id, active_task_id, "
                "active_invocation_id, status, state_version, "
                "request_task_incarnation_id, request_task_retry_count, "
                "from_turn_generation, request_source_log_id, "
                "request_output_log_id, request_terminal_log_id, "
                "attempt_count, created_at, updated_at) VALUES "
                "(:task_id, :invocation_id, :task_id, :invocation_id, "
                "'pending', 1, :incarnation, 0, 3, 11, 12, 13, 0, "
                "'2026-08-07 00:00:02', '2026-08-07 00:00:02')"
            ),
            {
                "task_id": task_id,
                "invocation_id": invocation_id,
                "incarnation": _INCARNATION,
            },
        )
    with pytest.raises(RuntimeError, match="resume history would be destroyed"):
        _run_migration(db_path, command.downgrade, "4b8d2f6a1c90")

    with engine.begin() as connection:
        connection.execute(text("DELETE FROM capability_resume_outbox"))
        connection.execute(
            text(
                "UPDATE capability_invocations SET "
                "source = 'agent_request', resume_policy = 'resume_task', "
                "requested_by_user_id = NULL, "
                "request_task_incarnation_id = :incarnation, "
                "request_task_retry_count = 0, "
                "request_task_turn_generation = 3, "
                "request_source_log_id = 11, request_output_log_id = 12, "
                "request_terminal_log_id = 13, request_reason = 'plan', "
                "request_protocol_version = 1, request_output_hash = :digest "
                "WHERE id = :invocation_id"
            ),
            {
                "incarnation": _INCARNATION,
                "digest": _DIGEST,
                "invocation_id": invocation_id,
            },
        )
    with pytest.raises(RuntimeError, match="exact identity would be destroyed"):
        _run_migration(db_path, command.downgrade, "4b8d2f6a1c90")

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE capability_invocations SET source = 'human_request', "
                "resume_policy = 'attach_only' WHERE id = :invocation_id"
            ),
            {"invocation_id": invocation_id},
        )
    engine.dispose()
    _run_migration(db_path, command.downgrade, "4b8d2f6a1c90")
