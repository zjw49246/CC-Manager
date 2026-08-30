"""Regression tests for Discussion subprocess ownership and shutdown."""

from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select

from backend.models.discussion import Discussion, DiscussionAgent, DiscussionEvent
from backend.models.project import Project
from backend.models.task_share import ProjectShare
from backend.models.user import User
from backend.services import discussion_service
from backend.services.discussion_service import (
    DiscussionProcessCleanupError,
    DiscussionSecurityError,
    DiscussionService,
)
from backend.services.project_share_admission import (
    ProjectShareAdmissionError,
    lock_project_share_authority,
    require_project_agents_quiescent,
)


class _FakeDb:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def execute(self, *_args, **_kwargs):
        return None

    async def commit(self):
        return None


class _Broadcaster:
    def __init__(self):
        self.broadcast = AsyncMock()


async def _wait_for_process(
    service: DiscussionService,
    agent_id: int,
) -> asyncio.subprocess.Process:
    for _ in range(100):
        process = service._processes.get(agent_id)
        if process is not None:
            return process
        await asyncio.sleep(0.01)
    raise AssertionError("discussion subprocess was not registered")


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["active", "closing"])
async def test_active_and_closing_discussions_veto_first_project_share(
    db_factory,
    status,
):
    async with db_factory() as db:
        project = Project(name=f"discussion-share-lease-{status}", status="ready")
        db.add(project)
        await db.flush()
        db.add(Discussion(
            title=f"{status} provider lease",
            project_id=project.id,
            status=status,
        ))
        await db.commit()
        project_id = project.id

    async with db_factory() as db:
        project = await lock_project_share_authority(db, project_id)
        with pytest.raises(
            ProjectShareAdmissionError,
            match="active or closing Project Discussion",
        ):
            await require_project_agents_quiescent(db, project)
        await db.rollback()


@pytest.mark.asyncio
async def test_cancelling_consumer_reaps_live_process():
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    consumer = asyncio.create_task(
        service._run_and_consume(
            41,
            7,
            [sys.executable, "-c", "import time; time.sleep(60)"],
            {},
        )
    )
    service._consumers[41] = consumer
    process = await _wait_for_process(service, 41)

    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(consumer, timeout=5)

    assert process.returncode is not None
    assert 41 not in service._processes
    assert 41 not in service._consumers


@pytest.mark.asyncio
async def test_agent_anyio_cancel_finishes_stderr_and_durable_status(
    db_factory,
    monkeypatch,
):
    from anyio import CancelScope

    async with db_factory() as db:
        discussion = Discussion(title="cancelled agent finalization")
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
            status="running",
        )
        db.add(agent)
        await db.commit()
        discussion_id = discussion.id
        agent_id = agent.id

    scope_holder: dict[str, CancelScope] = {}
    terminated = asyncio.Event()
    release_stderr = asyncio.Event()
    stderr_drained = asyncio.Event()

    class FakeStream:
        async def read(self):
            await release_stderr.wait()
            stderr_drained.set()
            return b"cancelled safely"

    class FakeStdout:
        async def readline(self):
            return b""

    class FakeProcess:
        pid = 515_201
        returncode = None
        stdout = FakeStdout()
        stderr = FakeStream()

    process = FakeProcess()

    async def create_process(*_args, **_kwargs):
        scope_holder["scope"].cancel()
        return process

    async def terminate(target):
        assert target is process
        await asyncio.sleep(0)
        target.returncode = -2
        terminated.set()

    async def release_reader():
        await terminated.wait()
        await asyncio.sleep(0)
        release_stderr.set()

    service = DiscussionService(db_factory, _Broadcaster())
    monkeypatch.setattr(
        discussion_service.asyncio,
        "create_subprocess_exec",
        create_process,
    )
    monkeypatch.setattr(service, "_terminate_process", terminate)
    monkeypatch.setattr(
        service,
        "_process_tree_alive",
        lambda target: target.returncode is None,
    )

    releaser = asyncio.create_task(release_reader())
    try:
        with CancelScope() as scope:
            scope_holder["scope"] = scope
            with pytest.raises(asyncio.CancelledError):
                await service._run_and_consume(
                    agent_id,
                    discussion_id,
                    ["fake-agent"],
                    {},
                )
        await releaser
    finally:
        release_stderr.set()
        if not releaser.done():
            releaser.cancel()
        await asyncio.gather(releaser, return_exceptions=True)

    assert stderr_drained.is_set()
    assert process.returncode == -2
    async with db_factory() as db:
        durable = await db.get(DiscussionAgent, agent_id)
    assert durable.status == "idle"
    assert durable.pid is None
    assert agent_id not in service._processes


@pytest.mark.asyncio
async def test_facilitator_anyio_cancel_finishes_stderr_finalizer(
    db_factory,
    monkeypatch,
):
    from anyio import CancelScope

    scope_holder: dict[str, CancelScope] = {}
    terminated = asyncio.Event()
    release_stderr = asyncio.Event()
    stderr_drained = asyncio.Event()

    class FakeStream:
        async def read(self):
            await release_stderr.wait()
            stderr_drained.set()
            return b"cancelled safely"

    class FakeStdout:
        async def readline(self):
            return b""

    class FakeProcess:
        pid = 515_202
        returncode = None
        stdout = FakeStdout()
        stderr = FakeStream()

    process = FakeProcess()

    async def create_process(*_args, **_kwargs):
        scope_holder["scope"].cancel()
        return process

    async def terminate(target):
        assert target is process
        await asyncio.sleep(0)
        target.returncode = -2
        terminated.set()

    async def release_reader():
        await terminated.wait()
        await asyncio.sleep(0)
        release_stderr.set()

    service = DiscussionService(db_factory, _Broadcaster())
    service._prepare_claude_security_context = AsyncMock(
        return_value=(["fake-facilitator"], {}, os.path.abspath(os.sep))
    )
    monkeypatch.setattr(
        discussion_service.asyncio,
        "create_subprocess_exec",
        create_process,
    )
    monkeypatch.setattr(service, "_terminate_process", terminate)
    monkeypatch.setattr(
        service,
        "_process_tree_alive",
        lambda target: target.returncode is None,
    )

    releaser = asyncio.create_task(release_reader())
    try:
        with CancelScope() as scope:
            scope_holder["scope"] = scope
            with pytest.raises(asyncio.CancelledError):
                await service._run_facilitator_process(
                    SimpleNamespace(
                        id=515_202,
                        facilitator_model="model",
                        facilitator_session_id=None,
                        project_id=None,
                    ),
                    "prompt",
                )
        await releaser
    finally:
        release_stderr.set()
        if not releaser.done():
            releaser.cancel()
        await asyncio.gather(releaser, return_exceptions=True)

    assert stderr_drained.is_set()
    assert process.returncode == -2
    assert service._facilitator_processes == {}
    assert service._facilitator_tasks == set()


@pytest.mark.asyncio
async def test_stderr_is_drained_while_process_is_running():
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    consumer = asyncio.create_task(
        service._run_and_consume(
            42,
            8,
            [
                sys.executable,
                "-c",
                (
                    "import sys; "
                    "sys.stderr.write('x' * 2_000_000); "
                    "sys.stderr.flush(); "
                    "sys.exit(3)"
                ),
            ],
            {},
        )
    )
    service._consumers[42] = consumer

    await asyncio.wait_for(consumer, timeout=5)
    assert 42 not in service._processes
    assert 42 not in service._consumers


@pytest.mark.asyncio
async def test_shutdown_reaps_registered_process_without_consumer():
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(60)",
        start_new_session=True,
    )
    service._processes[43] = process

    await asyncio.wait_for(service.shutdown(), timeout=5)

    assert process.returncode is not None
    assert service._processes == {}
    assert service._consumers == {}


@pytest.mark.asyncio
async def test_cancellation_during_spawn_still_reaps_created_process(
    monkeypatch,
):
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    real_spawn = asyncio.create_subprocess_exec
    spawn_started = asyncio.Event()
    release_spawn = asyncio.Event()
    created: list[asyncio.subprocess.Process] = []

    async def delayed_spawn(*args, **kwargs):
        spawn_started.set()
        await release_spawn.wait()
        process = await real_spawn(*args, **kwargs)
        created.append(process)
        return process

    monkeypatch.setattr(
        discussion_service.asyncio,
        "create_subprocess_exec",
        delayed_spawn,
    )
    consumer = asyncio.create_task(
        service._run_and_consume(
            44,
            9,
            [sys.executable, "-c", "import time; time.sleep(60)"],
            {},
        )
    )
    service._consumers[44] = consumer
    await spawn_started.wait()

    consumer.cancel()
    release_spawn.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(consumer, timeout=5)

    assert len(created) == 1
    assert created[0].returncode is not None
    assert service._processes == {}
    assert service._consumers == {}


@pytest.mark.asyncio
async def test_shutdown_is_bounded_and_retains_unreaped_process(
    monkeypatch,
):
    class _StubbornProcess:
        pid = 12345
        returncode = None

        async def wait(self):
            await asyncio.Future()

    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    process = _StubbornProcess()
    service._processes[45] = process
    monkeypatch.setattr(
        discussion_service,
        "_PROCESS_SIGNAL_TIMEOUTS",
        (0.01, 0.01, 0.01),
    )
    monkeypatch.setattr(service, "_process_tree_alive", lambda _process: True)
    monkeypatch.setattr(
        service,
        "_send_process_signal",
        lambda _process, _signal: None,
    )

    with pytest.raises(DiscussionProcessCleanupError):
        await asyncio.wait_for(service.shutdown(), timeout=1)

    assert service._processes[45] is process


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="process-group regression")
async def test_leader_exit_does_not_leave_descendant_process_group(tmp_path):
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    child_pid_file = tmp_path / "child.pid"
    script = (
        "import os,time; "
        "pid=os.fork(); "
        "\nif pid == 0:\n"
        " d=os.open(os.devnull, os.O_RDWR); os.dup2(d,1); os.dup2(d,2); "
        "time.sleep(60); os._exit(0)\n"
        f"open({str(child_pid_file)!r},'w').write(str(pid))"
    )
    consumer = asyncio.create_task(
        service._run_and_consume(
            46,
            10,
            [sys.executable, "-c", script],
            {},
        )
    )
    service._consumers[46] = consumer

    await asyncio.wait_for(consumer, timeout=5)

    assert child_pid_file.exists()
    assert service._processes == {}
    assert service._consumers == {}


@pytest.mark.asyncio
async def test_shutdown_cancels_and_reaps_facilitator(monkeypatch):
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    real_spawn = asyncio.create_subprocess_exec
    # Keep the test deadline longer than the complete signal escalation.
    # A just-spawned interpreter is not guaranteed to exit on the first
    # SIGINT before its startup has settled, especially under suite load.
    monkeypatch.setattr(
        discussion_service,
        "_PROCESS_SIGNAL_TIMEOUTS",
        (0.1, 0.1, 0.1),
    )
    monkeypatch.setattr(
        discussion_service,
        "_CONSUMER_SHUTDOWN_TIMEOUT",
        1.0,
    )

    async def spawn_sleeper(*_args, **kwargs):
        return await real_spawn(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            stdout=kwargs["stdout"],
            stderr=kwargs["stderr"],
            env=kwargs["env"],
            cwd=kwargs["cwd"],
            limit=kwargs["limit"],
            start_new_session=kwargs["start_new_session"],
        )

    monkeypatch.setattr(
        discussion_service.asyncio,
        "create_subprocess_exec",
        spawn_sleeper,
    )
    service._prepare_claude_security_context = AsyncMock(
        return_value=(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            {},
            os.path.abspath(os.sep),
        )
    )
    facilitator = asyncio.create_task(
        service._run_facilitator_process(
            SimpleNamespace(
                id=11,
                facilitator_model="model",
                facilitator_session_id=None,
                project_id=None,
            ),
            "prompt",
        )
    )
    for _ in range(100):
        if service._facilitator_processes:
            break
        await asyncio.sleep(0.01)
    assert service._facilitator_processes
    process = next(iter(service._facilitator_processes.values()))

    await asyncio.wait_for(service.shutdown(), timeout=5)

    assert process.returncode is not None
    assert facilitator.done()
    assert service._facilitator_processes == {}
    assert service._facilitator_tasks == set()


@pytest.mark.asyncio
async def test_concurrent_agent_start_has_single_winner(db_factory):
    async with db_factory() as db:
        discussion = Discussion(title="atomic start")
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
            session_id="session",
            status="idle",
        )
        db.add(agent)
        await db.commit()
        discussion_id = discussion.id
        agent_id = agent.id

    service = DiscussionService(db_factory, _Broadcaster())
    launches: list[str] = []
    service._launch_agent_resume = (
        lambda _agent, _disc, message, cwd=None: launches.append(message)
    )

    async def send(message):
        async with db_factory() as db:
            await service.send_to_agent(db, agent_id, message)

    results = await asyncio.gather(
        send("first"),
        send("second"),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, ValueError) for result in results) == 1
    assert len(launches) == 1
    async with db_factory() as db:
        current = await db.get(DiscussionAgent, agent_id)
        event_count = await db.scalar(
            select(func.count(DiscussionEvent.id)).where(
                DiscussionEvent.discussion_id == discussion_id,
                DiscussionEvent.agent_id == agent_id,
                DiscussionEvent.event_type == "user_message",
            )
        )
    assert current.status == "running"
    assert event_count == 1


@pytest.mark.asyncio
async def test_spawn_failure_rolls_back_running_claim(db_factory):
    async with db_factory() as db:
        discussion = Discussion(title="spawn failure")
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
            status="running",
        )
        db.add(agent)
        await db.commit()
        agent_id = agent.id
        discussion_id = discussion.id

    service = DiscussionService(db_factory, _Broadcaster())
    consumer = asyncio.create_task(
        service._run_and_consume(
            agent_id,
            discussion_id,
            ["/definitely/missing/ccm-discussion-binary"],
            {},
        )
    )
    service._consumers[agent_id] = consumer

    with pytest.raises(FileNotFoundError):
        await consumer

    async with db_factory() as db:
        current = await db.get(DiscussionAgent, agent_id)
    assert current.status == "error"
    assert current.pid is None
    assert service._consumers == {}


@pytest.mark.asyncio
async def test_discussion_provider_route_is_admin_only_and_scrubbed(
    db_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(discussion_service.settings, "auth_token", "configured")
    monkeypatch.setenv("AUTH_TOKEN", "manager-token")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///manager-secret.db")
    monkeypatch.setenv("SMTP_PASSWORD", "smtp-secret")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "provider-key")

    async with db_factory() as db:
        admin = User(
            email="discussion-admin@example.test",
            name="Discussion Admin",
            password_hash="hash",
            role="admin",
            is_active=True,
        )
        db.add(admin)
        await db.flush()
        discussion = Discussion(
            title="isolated",
            creator_user_id=admin.id,
        )
        db.add(discussion)
        await db.commit()
        await db.refresh(discussion)
        discussion_id = discussion.id

    projection_dir = tmp_path / "projection"
    projection_dir.mkdir()
    projection = SimpleNamespace(
        config_dir=projection_dir,
        oauth_access_token=None,
    )
    prepare = MagicMock(return_value=projection)
    monkeypatch.setattr(
        discussion_service,
        "prepare_claude_auth_projection",
        prepare,
    )

    def apply_projection(env, selected):
        assert selected is projection
        env["CLAUDE_CONFIG_DIR"] = str(projection_dir)

    monkeypatch.setattr(
        discussion_service,
        "apply_claude_auth_projection",
        apply_projection,
    )
    protected = AsyncMock(return_value=("/manager/secret",))
    monkeypatch.setattr(
        discussion_service,
        "task_ssh_protected_paths",
        protected,
    )
    isolation = tmp_path / "isolation.json"
    isolation.write_text("{}", encoding="utf-8")
    generate = MagicMock(return_value=isolation)
    validate = MagicMock()
    monkeypatch.setattr(
        discussion_service,
        "generate_claude_read_only_isolation_settings",
        generate,
    )
    monkeypatch.setattr(
        discussion_service,
        "validate_claude_task_isolation_settings",
        validate,
    )

    service = DiscussionService(db_factory, _Broadcaster())
    command, env, cwd = await service._prepare_claude_security_context(
        discussion_service._DiscussionClaudeSecurityContext(
            discussion_id=discussion_id,
            namespace="discussion-facilitator",
            identifier=discussion_id,
            model="claude-opus-4-6",
            resume_session_id=None,
            repository_cwd="/project/repo",
            binding="discussion-generation-a",
        )
    )

    assert "--dangerously-skip-permissions" not in command
    assert command[command.index("--permission-mode") + 1] == "plan"
    assert command[command.index("--setting-sources") + 1] == ""
    assert command[command.index("--tools") + 1] == "Glob,Grep,Read"
    assert env["ANTHROPIC_API_KEY"] == "provider-key"
    assert "AUTH_TOKEN" not in env
    assert "DATABASE_URL" not in env
    assert "SMTP_PASSWORD" not in env
    assert cwd == os.path.abspath(os.sep)
    protected.assert_awaited_once()
    generate.assert_called_once_with(
        "discussion-facilitator",
        discussion_id,
        ("/manager/secret",),
    )
    validate.assert_called_once()


@pytest.mark.asyncio
async def test_member_discussion_rejected_before_auth_projection(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(discussion_service.settings, "auth_token", "configured")
    async with db_factory() as db:
        member = User(
            email="discussion-member@example.test",
            name="Discussion Member",
            password_hash="hash",
            role="member",
            is_active=True,
        )
        db.add(member)
        await db.flush()
        discussion = Discussion(
            title="member-owned",
            creator_user_id=member.id,
        )
        db.add(discussion)
        await db.commit()
        await db.refresh(discussion)
        discussion_id = discussion.id

    prepare = MagicMock()
    monkeypatch.setattr(
        discussion_service,
        "prepare_claude_auth_projection",
        prepare,
    )
    service = DiscussionService(db_factory, _Broadcaster())

    with pytest.raises(DiscussionSecurityError, match="active admins"):
        await service._prepare_claude_security_context(
            discussion_service._DiscussionClaudeSecurityContext(
                discussion_id=discussion_id,
                namespace="discussion-facilitator",
                identifier=discussion_id,
                model="model",
                resume_session_id=None,
                repository_cwd=None,
                binding="member-row",
            )
        )
    prepare.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_discussion_without_creator_rejected_before_auth_projection(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(discussion_service.settings, "auth_token", "configured")
    async with db_factory() as db:
        discussion = Discussion(
            title="legacy-unowned",
            creator_user_id=None,
        )
        db.add(discussion)
        await db.commit()
        await db.refresh(discussion)
        discussion_id = discussion.id

    prepare = MagicMock()
    monkeypatch.setattr(
        discussion_service,
        "prepare_claude_auth_projection",
        prepare,
    )
    service = DiscussionService(db_factory, _Broadcaster())

    with pytest.raises(DiscussionSecurityError, match="active admins"):
        await service._prepare_claude_security_context(
            discussion_service._DiscussionClaudeSecurityContext(
                discussion_id=discussion_id,
                namespace="discussion-facilitator",
                identifier=discussion_id,
                model="model",
                resume_session_id=None,
                repository_cwd=None,
                binding="legacy-unowned",
            )
        )
    prepare.assert_not_called()


@pytest.mark.asyncio
async def test_auth_disabled_discussion_rejected_before_provider_effect(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(discussion_service.settings, "auth_token", "")
    prepare = MagicMock()
    monkeypatch.setattr(
        discussion_service,
        "prepare_claude_auth_projection",
        prepare,
    )
    service = DiscussionService(db_factory, _Broadcaster())

    with pytest.raises(DiscussionSecurityError, match="security admission"):
        await service._prepare_claude_security_context(
            discussion_service._DiscussionClaudeSecurityContext(
                discussion_id=1,
                namespace="discussion-facilitator",
                identifier=1,
                model="model",
                resume_session_id=None,
                repository_cwd=None,
                binding="no-auth",
            )
        )
    prepare.assert_not_called()


@pytest.mark.asyncio
async def test_preclaimed_agent_becomes_visible_error_when_outbound_share_gate_vetoes(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(discussion_service.settings, "auth_token", "configured")
    async with db_factory() as db:
        project = Project(name="discussion-final-share-gate", status="ready")
        db.add(project)
        await db.flush()
        discussion = Discussion(
            title="final share gate",
            project_id=project.id,
            status="active",
        )
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
            status="running",
            pid=None,
        )
        db.add(agent)
        db.add(ProjectShare(
            project_id=project.id,
            shared_to_open_id="remote-discussion-reviewer",
            shared_to_name="Remote reviewer",
            shared_to_ccm_url="https://remote.example.test",
            status="active",
        ))
        await db.commit()
        project_id = project.id
        discussion_id = discussion.id
        agent_id = agent.id

    spawn = AsyncMock()
    monkeypatch.setattr(
        discussion_service.asyncio,
        "create_subprocess_exec",
        spawn,
    )
    service = DiscussionService(db_factory, _Broadcaster())

    with pytest.raises(DiscussionSecurityError, match="shared"):
        await service._run_and_consume(
            agent_id,
            discussion_id,
            ["-p", "review"],
            {},
            security_context=discussion_service._DiscussionClaudeSecurityContext(
                discussion_id=discussion_id,
                namespace="discussion-agent",
                identifier=agent_id,
                model="model",
                resume_session_id=None,
                repository_cwd=None,
                binding="preclaimed-final-share-gate",
                project_id=project_id,
            ),
        )

    spawn.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(DiscussionAgent, agent_id)
        assert current.status == "error"
        assert current.pid is None


@pytest.mark.asyncio
async def test_stop_agent_waits_for_consumer_finalization(db_factory):
    async with db_factory() as db:
        discussion = Discussion(title="delete fence")
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
            status="running",
        )
        db.add(agent)
        await db.commit()
        agent_id = agent.id
        discussion_id = discussion.id

    service = DiscussionService(db_factory, _Broadcaster())
    consumer = asyncio.create_task(
        service._run_and_consume(
            agent_id,
            discussion_id,
            [sys.executable, "-c", "import time; time.sleep(60)"],
            {},
        )
    )
    service._consumers[agent_id] = consumer
    await _wait_for_process(service, agent_id)

    await asyncio.wait_for(service.stop_agent(agent_id), timeout=5)

    assert consumer.done()
    assert agent_id not in service._consumers
    assert agent_id not in service._processes
    async with db_factory() as db:
        current = await db.get(DiscussionAgent, agent_id)
    assert current.status == "idle"
    assert current.pid is None


@pytest.mark.asyncio
async def test_delete_barrier_blocks_stale_agent_launch(db_factory):
    async with db_factory() as db:
        discussion = Discussion(title="delete admission fence")
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
            status="idle",
        )
        db.add(agent)
        await db.commit()
        discussion_id = discussion.id
        agent_id = agent.id

    service = DiscussionService(db_factory, _Broadcaster())
    service.cleanup_runtime = AsyncMock()
    launches: list[int] = []
    service._launch_agent_with_prompt = (
        lambda launched, *_args, **_kwargs: launches.append(launched.id)
    )
    barrier_entered = asyncio.Event()
    permit_delete = asyncio.Event()

    async def delete_graph():
        async with db_factory() as db:
            async with service.deletion_barrier(discussion_id, db):
                barrier_entered.set()
                await permit_delete.wait()
                current_agent = await db.get(DiscussionAgent, agent_id)
                current_discussion = await db.get(Discussion, discussion_id)
                await db.delete(current_agent)
                await db.delete(current_discussion)
                await db.commit()

    async def trigger_after_barrier():
        await barrier_entered.wait()
        async with db_factory() as db:
            await service.trigger_agent(db, agent_id)

    deleting = asyncio.create_task(delete_graph())
    await barrier_entered.wait()
    triggering = asyncio.create_task(trigger_after_barrier())
    await asyncio.sleep(0)
    assert not triggering.done()

    permit_delete.set()
    await deleting
    result = await asyncio.gather(triggering, return_exceptions=True)

    assert len(result) == 1
    assert isinstance(result[0], DiscussionSecurityError)
    assert launches == []


@pytest.mark.asyncio
async def test_delete_barrier_finishes_quiesce_before_delivering_cancellation(
    db_factory,
):
    async with db_factory() as db:
        discussion = Discussion(title="cancelled delete fence")
        db.add(discussion)
        await db.commit()
        await db.refresh(discussion)
        discussion_id = discussion.id

    service = DiscussionService(db_factory, _Broadcaster())
    quiesce_started = asyncio.Event()
    permit_quiesce = asyncio.Event()
    cleanup = AsyncMock()
    service.cleanup_runtime = cleanup

    async def slow_stop_facilitator(_discussion_id):
        quiesce_started.set()
        await permit_quiesce.wait()

    service.stop_facilitator = slow_stop_facilitator
    entered_body = False

    async def delete_with_barrier():
        nonlocal entered_body
        async with db_factory() as db:
            async with service.deletion_barrier(discussion_id, db):
                entered_body = True

    deleting = asyncio.create_task(delete_with_barrier())
    await quiesce_started.wait()
    deleting.cancel()
    await asyncio.sleep(0)

    assert not deleting.done()
    permit_quiesce.set()
    with pytest.raises(asyncio.CancelledError):
        await deleting

    cleanup.assert_awaited_once_with(discussion_id, [])
    assert entered_body is False
    assert not service._get_lock(discussion_id).locked()
