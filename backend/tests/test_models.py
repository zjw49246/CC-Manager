"""Tests for ORM models — schema correctness and defaults."""
import pytest
import pytest_asyncio

from backend.models.task import Task
from backend.models.instance import Instance
from backend.models.project import Project


@pytest.mark.asyncio
async def test_task_defaults(db_session):
    task = Task(title="t", description="d")
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)

    assert task.status == "pending"
    assert task.priority == 0
    assert task.retry_count == 0
    assert task.max_retries == 2
    assert task.mode == "auto"
    assert task.turn_generation == 0
    assert task.turn_source_log_id is None
    assert task.capability_policy is None
    assert task.merge_status == "pending"
    assert task.project_id is None
    assert task.target_repo is not None  # defaults to ""
    assert task.enable_workflows is False


@pytest.mark.asyncio
async def test_task_with_project_id(db_session):
    task = Task(title="t", description="d", project_id=42)
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)
    assert task.project_id == 42


@pytest.mark.asyncio
async def test_instance_defaults(db_session):
    inst = Instance(name="worker-1")
    db_session.add(inst)
    await db_session.commit()
    await db_session.refresh(inst)

    assert inst.status == "idle"
    assert inst.model == "default"
    assert inst.total_tasks_completed == 0
    assert inst.total_cost_usd == 0.0
    assert inst.pid is None


@pytest.mark.asyncio
async def test_project_defaults(db_session):
    proj = Project(name="my-project", git_url="https://github.com/user/repo.git")
    db_session.add(proj)
    await db_session.commit()
    await db_session.refresh(proj)

    assert proj.default_branch == "main"
    assert proj.status == "pending"
    assert proj.local_path is None
    assert proj.error_message is None


@pytest.mark.asyncio
async def test_project_no_git_url(db_session):
    proj = Project(name="local-project")
    db_session.add(proj)
    await db_session.commit()
    await db_session.refresh(proj)

    assert proj.git_url is None
    assert proj.has_remote is False
    assert proj.default_branch == "main"
    assert proj.status == "pending"


@pytest.mark.asyncio
async def test_project_unique_name(db_session):
    from sqlalchemy.exc import IntegrityError

    proj1 = Project(name="same-name", git_url="https://a.git")
    db_session.add(proj1)
    await db_session.commit()

    proj2 = Project(name="same-name", git_url="https://b.git")
    db_session.add(proj2)
    with pytest.raises(IntegrityError):
        await db_session.commit()


# === LogEntry tests ===


@pytest.mark.asyncio
async def test_log_entry_defaults(db_session):
    from backend.models.log_entry import LogEntry

    entry = LogEntry(instance_id=1, event_type="message")
    db_session.add(entry)
    await db_session.commit()
    await db_session.refresh(entry)

    assert entry.is_error is False
    assert entry.timestamp is not None
    assert entry.role is None
    assert entry.content is None
    assert entry.tool_name is None
    assert entry.tool_input is None
    assert entry.tool_output is None
    assert entry.task_id is None
    assert entry.task_turn_generation is None
    assert entry.native_turn_id is None
    assert entry.turn_scope is None
    assert entry.actual_transport is None


@pytest.mark.asyncio
async def test_task_and_log_exact_turn_identity_round_trip(db_session):
    from backend.models.log_entry import LogEntry

    generation = 2**40 + 7
    task = Task(
        title="exact turn",
        description="d",
        turn_generation=generation,
        capability_policy={
            "version": 1,
            "max_invocations": 2,
            "capabilities": {"plan": 1, "code_review": 1},
        },
    )
    db_session.add(task)
    await db_session.flush()
    entry = LogEntry(
        task_id=task.id,
        event_type="result",
        task_turn_generation=generation,
        native_turn_id="turn_native_123",
        turn_scope="foreground",
    )
    db_session.add(entry)
    await db_session.flush()
    task.turn_source_log_id = entry.id
    await db_session.commit()
    await db_session.refresh(task)
    await db_session.refresh(entry)

    assert task.turn_generation == generation
    assert task.turn_source_log_id == entry.id
    assert task.capability_policy == {
        "version": 1,
        "max_invocations": 2,
        "capabilities": {"plan": 1, "code_review": 1},
    }
    assert entry.task_turn_generation == generation
    assert entry.native_turn_id == "turn_native_123"
    assert entry.turn_scope == "foreground"


@pytest.mark.asyncio
async def test_log_entry_rejects_unknown_turn_scope(db_session):
    from sqlalchemy.exc import IntegrityError

    from backend.models.log_entry import LogEntry

    db_session.add(
        LogEntry(
            event_type="result",
            turn_scope="background",
        )
    )

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_log_entry_actual_transport_is_source_only_and_enumerated(db_session):
    from sqlalchemy.exc import IntegrityError

    from backend.models.log_entry import LogEntry

    db_session.add(
        LogEntry(
            event_type="turn_source",
            turn_scope="source",
            actual_transport="codex_app_server",
        )
    )
    await db_session.commit()

    db_session.add(
        LogEntry(
            event_type="result",
            turn_scope="foreground",
            actual_transport="claude_exec",
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()

    # SQL CHECK constraints accept UNKNOWN, so the constraint must explicitly
    # reject a NULL scope instead of relying on ``turn_scope = 'source'``.
    db_session.add(
        LogEntry(
            event_type="turn_source",
            turn_scope=None,
            actual_transport="codex_exec",
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()

    db_session.add(
        LogEntry(
            event_type="turn_source",
            turn_scope="source",
            actual_transport="codex",
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_log_entry_with_tool_fields(db_session):
    from backend.models.log_entry import LogEntry

    entry = LogEntry(
        instance_id=1,
        event_type="tool_use",
        role="assistant",
        tool_name="Edit",
        tool_input='{"file_path": "/tmp/x.py"}',
        tool_output="ok",
        raw_json='{"type": "tool_use"}',
    )
    db_session.add(entry)
    await db_session.commit()
    await db_session.refresh(entry)

    assert entry.tool_name == "Edit"
    assert entry.tool_input == '{"file_path": "/tmp/x.py"}'
    assert entry.tool_output == "ok"
    assert entry.raw_json == '{"type": "tool_use"}'


# === Worktree tests ===


@pytest.mark.asyncio
async def test_worktree_defaults(db_session):
    from backend.models.worktree import Worktree

    wt = Worktree(repo_path="/repo", worktree_path="/wt/1", branch_name="task-1")
    db_session.add(wt)
    await db_session.commit()
    await db_session.refresh(wt)

    assert wt.status == "active"
    assert wt.base_branch == "main"
    assert wt.created_at is not None
    assert wt.removed_at is None
    assert wt.instance_id is None


@pytest.mark.asyncio
async def test_worktree_unique_path(db_session):
    from sqlalchemy.exc import IntegrityError
    from backend.models.worktree import Worktree

    wt1 = Worktree(repo_path="/repo", worktree_path="/wt/same", branch_name="b1")
    db_session.add(wt1)
    await db_session.commit()

    wt2 = Worktree(repo_path="/repo", worktree_path="/wt/same", branch_name="b2")
    db_session.add(wt2)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_worktree_removed_at_nullable(db_session):
    from backend.models.worktree import Worktree
    from datetime import datetime

    wt = Worktree(repo_path="/repo", worktree_path="/wt/2", branch_name="task-2")
    db_session.add(wt)
    await db_session.commit()
    await db_session.refresh(wt)
    assert wt.removed_at is None

    # Can set removed_at
    wt.removed_at = datetime.utcnow()
    await db_session.commit()
    await db_session.refresh(wt)
    assert wt.removed_at is not None
