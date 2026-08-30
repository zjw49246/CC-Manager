"""Tests for UpdateService — the self-update pipeline.

Regression tests for the 2026-07-16 ccm-xiaoyu incident: the migration-path
script lived in the service's own cgroup, so its `systemctl stop` killed the
script itself and the service was never started again (502 until manual fix).
"""
import asyncio
import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.services.update_service import (
    STEP_NAMES,
    StepInfo,
    UpdateService,
    UpdateState,
)
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.sub_agent import SubAgentSession

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "update_migrate.sh"


def _make_service(
    tmp_path: Path,
    db_factory=None,
    dispatcher=None,
    running_commit: str = "",
) -> UpdateService:
    scripts = tmp_path / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    helper = scripts / "update_migrate.sh"
    helper.write_text(
        "#!/bin/bash\nCCM_UPDATE_PROTOCOL_VERSION=2\nexit 0\n"
    )
    helper.chmod(0o700)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    svc = UpdateService(
        broadcaster,
        port=8999,
        project_dir=str(tmp_path),
        db_factory=db_factory,
        dispatcher=dispatcher,
        running_commit=running_commit,
        update_runtime_root=tmp_path / ".update-runtime",
        legacy_update_runtime_root=tmp_path / ".legacy-update-runtime",
    )
    svc._status_file = tmp_path / "status.json"
    svc._journal_file = tmp_path / "backups" / "deployment-status.json"
    svc._lease_file = tmp_path / "backups" / "deployment-lease.json"
    svc._lease_lock_file = tmp_path / "backups" / "deployment-lease.lock"
    svc._fetch_and_validate_target_protocol = AsyncMock(
        return_value=(True, "", "")
    )
    svc._database_revision_status = AsyncMock(
        return_value={
            "database_current_revisions": ["head"],
            "database_head_revisions": ["head"],
            "database_up_to_date": True,
            "db_current_revision": "head",
            "db_head_revision": "head",
            "db_up_to_date": True,
            "database_revision_error": "",
        }
    )
    svc._dirty_worktree_files = AsyncMock(return_value=[])
    return svc


def _make_state() -> UpdateState:
    return UpdateState(
        update_id="upd_test",
        status="running",
        steps=[StepInfo(name=n) for n in STEP_NAMES],
        old_commit="old" * 10,
        new_commit="new" * 10,
        backup_file="/tmp/backup.db",
    )


def _make_gate_dispatcher(db_factory):
    from backend.services.dispatcher import GlobalDispatcher

    manager = MagicMock()
    manager.launch = AsyncMock(return_value=123)
    manager.processes = {}
    manager._tasks = {}
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    return GlobalDispatcher(db_factory, manager, broadcaster)


# ---- update safety and version detection ----


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("v1.2.3", (1, 2, 3)),
        ("v10.0.0+build.4", (10, 0, 0)),
        ("v1.2.3-rc.1", None),
        ("v1.2", None),
    ],
)
def test_stable_tag_parser_accepts_only_semver_releases(tag, expected):
    assert UpdateService._stable_tag_key(tag) == expected


@pytest.mark.asyncio
async def test_stable_check_treats_main_ahead_as_explicit_channel_switch(
    tmp_path,
):
    service = _make_service(tmp_path, running_commit="b" * 40)
    current = "b" * 40
    release = "a" * 40

    async def run_cmd(args, **_kwargs):
        command = tuple(args)
        if command == ("git", "fetch", "--tags"):
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if command == ("git", "rev-parse", "HEAD"):
            return {"returncode": 0, "stdout": current, "stderr": ""}
        if command == ("git", "tag", "--list", "v*"):
            return {"returncode": 0, "stdout": "v1.0.0\n", "stderr": ""}
        if command == ("git", "rev-list", "-n", "1", "v1.0.0"):
            return {"returncode": 0, "stdout": release, "stderr": ""}
        if command == ("git", "merge-base", "--is-ancestor", release, current):
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if command == ("git", "log", "--oneline", f"{current}..{release}"):
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if command == ("git", "diff", "--name-only", f"{current}..{release}"):
            return {
                "returncode": 0,
                "stdout": "frontend/src/App.tsx\n",
                "stderr": "",
            }
        raise AssertionError(command)

    service._run_cmd = AsyncMock(side_effect=run_cmd)

    result = await service._check_stable_updates()

    assert result["has_updates"] is True
    assert result["update_kind"] == "stable_switch"
    assert result["is_stable_downgrade"] is True
    assert result["stable_switch_blocked"] is False
    assert result["latest_version"] == "v1.0.0"
    assert result["latest_commit"] == release


@pytest.mark.asyncio
async def test_stable_check_blocks_test_to_release_database_downgrade(
    tmp_path,
):
    service = _make_service(tmp_path, running_commit="b" * 40)
    current = "b" * 40
    release = "a" * 40

    async def run_cmd(args, **_kwargs):
        command = tuple(args)
        responses = {
            ("git", "fetch", "--tags"): (0, ""),
            ("git", "rev-parse", "HEAD"): (0, current),
            ("git", "tag", "--list", "v*"): (0, "v1.0.0\n"),
            ("git", "rev-list", "-n", "1", "v1.0.0"): (0, release),
            ("git", "merge-base", "--is-ancestor", release, current): (0, ""),
            ("git", "log", "--oneline", f"{current}..{release}"): (0, ""),
            ("git", "diff", "--name-only", f"{current}..{release}"): (
                0,
                "alembic/versions/new_test_schema.py\n",
            ),
        }
        if command not in responses:
            raise AssertionError(command)
        returncode, stdout = responses[command]
        return {"returncode": returncode, "stdout": stdout, "stderr": ""}

    service._run_cmd = AsyncMock(side_effect=run_cmd)

    result = await service._check_stable_updates()

    assert result["has_updates"] is False
    assert result["stable_switch_blocked"] is True
    assert result["migration_count"] == 1
    assert "不能自动切回 Stable" in result["error"]


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups required")
@pytest.mark.asyncio
async def test_run_cmd_timeout_kills_grandchild_process_group(tmp_path):
    service = _make_service(tmp_path)
    pid_file = tmp_path / "child.pid"
    code = (
        "import os,time\n"
        "pid=os.fork()\n"
        "if pid == 0:\n"
        " open(%r,'w').write(str(os.getpid()))\n"
        " time.sleep(60)\n"
        "else:\n"
        " time.sleep(60)\n"
    ) % str(pid_file)

    result = await service._run_cmd(
        [sys.executable, "-c", code], timeout=1
    )

    assert result["returncode"] == -1
    child_pid = int(pid_file.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)


@pytest.mark.asyncio
async def test_run_cmd_anyio_cancel_reaps_process_and_streams(
    tmp_path,
    monkeypatch,
):
    from anyio import CancelScope

    service = _make_service(tmp_path)
    scope_holder: dict[str, CancelScope] = {}
    reaped = asyncio.Event()
    killed = asyncio.Event()

    class FakeStream:
        async def readline(self):
            await reaped.wait()
            return b""

    class FakeProcess:
        pid = 515_151
        returncode = None
        stdout = FakeStream()
        stderr = FakeStream()
        wait_calls = 0

        async def wait(self):
            self.wait_calls += 1
            if self.wait_calls == 1:
                scope_holder["scope"].cancel()
                await asyncio.Future()
            await asyncio.sleep(0)
            self.returncode = -9
            reaped.set()
            return self.returncode

        def kill(self):
            killed.set()

    process = FakeProcess()

    async def create_process(*_args, **_kwargs):
        return process

    def kill_group(pid, _signal):
        assert pid == process.pid
        killed.set()

    monkeypatch.setattr(
        "backend.services.update_service.asyncio.create_subprocess_exec",
        create_process,
    )
    monkeypatch.setattr(
        "backend.services.update_service.os.killpg",
        kill_group,
    )

    with CancelScope() as scope:
        scope_holder["scope"] = scope
        with pytest.raises(asyncio.CancelledError):
            await service._run_cmd(["fake-update-command"])

    assert killed.is_set()
    assert reaped.is_set()
    assert process.returncode == -9


def test_helper_missing_at_process_start_cannot_be_captured_late(tmp_path):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    svc = UpdateService(
        broadcaster,
        port=8999,
        project_dir=str(tmp_path),
        running_commit="a" * 40,
        update_runtime_root=tmp_path / ".update-runtime",
        legacy_update_runtime_root=tmp_path / ".legacy-update-runtime",
    )
    assert svc._trusted_update_script is None

    helper = tmp_path / "scripts" / "update_migrate.sh"
    helper.parent.mkdir(parents=True)
    helper.write_text(
        "#!/bin/bash\nCCM_UPDATE_PROTOCOL_VERSION=2\nexit 0\n",
        encoding="utf-8",
    )
    helper.chmod(0o700)

    assert svc.ensure_runtime_snapshot() is None
    assert "已拒绝停服操作" in svc._update_script_block_reason()


def test_captured_helper_can_retry_materialization_after_startup_gate(tmp_path):
    helper = tmp_path / "scripts" / "update_migrate.sh"
    helper.parent.mkdir(parents=True)
    helper.write_text(
        "#!/bin/bash\nCCM_UPDATE_PROTOCOL_VERSION=2\nexit 0\n",
        encoding="utf-8",
    )
    helper.chmod(0o700)
    outside = tmp_path / "outside"
    outside.mkdir()
    runtime_root = tmp_path / ".update-runtime"
    runtime_root.symlink_to(outside, target_is_directory=True)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    svc = UpdateService(
        broadcaster,
        port=8999,
        project_dir=str(tmp_path),
        running_commit="a" * 40,
        update_runtime_root=runtime_root,
        legacy_update_runtime_root=tmp_path / ".legacy-update-runtime",
    )
    assert svc._trusted_update_script is None
    assert svc._trusted_update_runtime is not None
    assert svc._trusted_update_runtime.has_captured_script is True

    runtime_root.unlink()
    snapshot = svc.ensure_runtime_snapshot()

    assert snapshot is not None
    assert svc._update_script_block_reason() == ""
    svc.close_runtime_snapshot()


def test_deleted_runtime_directory_is_rematerialized_from_captured_bytes(
    tmp_path,
):
    svc = _make_service(tmp_path)
    first = svc._trusted_update_script
    assert first is not None
    first_dir = first.parent
    first.unlink()
    (first_dir / "owner.json").unlink()
    first_dir.rmdir()

    assert svc._update_script_block_reason() == ""
    assert svc._trusted_update_script is not None
    assert svc._trusted_update_script != first
    assert svc._trusted_update_script.is_file()
    svc.close_runtime_snapshot()


@pytest.mark.asyncio
async def test_get_active_tasks_only_returns_running_states(tmp_path, db_factory):
    async with db_factory() as db:
        db.add_all([
            Task(title="queued", description="d", status="pending"),
            Task(title="claimed", description="d", status="in_progress"),
            Task(title="running", description="d", status="executing"),
            Task(title="done", description="d", status="completed"),
            Task(
                title="remote worker",
                description="d",
                status="executing",
                worker_id=77,
            ),
            Task(
                title="remote shadow",
                description="d",
                status="executing",
                shared_from_id=987654,
            ),
        ])
        await db.commit()

    svc = _make_service(tmp_path, db_factory=db_factory)
    active = await svc._get_active_tasks()

    assert [task["title"] for task in active] == [
        "claimed",
        "running",
        "remote worker",
    ]
    assert [task["status"] for task in active] == [
        "in_progress",
        "executing",
        "executing",
    ]


@pytest.mark.asyncio
async def test_live_auxiliary_generations_block_restart_without_active_task(
    tmp_path, db_factory,
):
    dispatcher = _make_gate_dispatcher(db_factory)
    # A sleeping scheduled monitor owns no process, so its asyncio lifecycle
    # alone must not block a restart; only a claimed/running turn is evidence.
    monitor_lifecycle = asyncio.create_task(asyncio.sleep(60))
    dispatcher._monitor_tasks[6] = monitor_lifecycle
    dispatcher._monitor_active_turns = {7}
    dispatcher._sub_agent_processes[9] = MagicMock(returncode=None)
    service = _make_service(
        tmp_path,
        db_factory=db_factory,
        dispatcher=dispatcher,
    )

    blockers = await service._get_blocking_tasks(
        pending_task_ids=set()
    )

    assert blockers == [
        {
            "id": 7,
            "title": "监控子 Agent #7",
            "status": "running_auxiliary",
            "kind": "monitor",
        },
        {
            "id": 9,
            "title": "子 Agent #9",
            "status": "running_auxiliary",
            "kind": "sub_agent",
        },
    ]


@pytest.mark.asyncio
async def test_database_only_stale_auxiliary_row_does_not_block_restart(
    tmp_path, db_factory,
):
    async with db_factory() as db:
        db.add(SubAgentSession(
            task_id=123,
            description="stale auxiliary",
            agent_type="monitor",
            source="ccm",
            status="running",
        ))
        await db.commit()

    dispatcher = _make_gate_dispatcher(db_factory)
    service = _make_service(
        tmp_path,
        db_factory=db_factory,
        dispatcher=dispatcher,
    )

    assert await service._get_blocking_tasks(
        pending_task_ids=set()
    ) == []


@pytest.mark.asyncio
async def test_get_blocking_tasks_includes_queued_resumes(tmp_path, db_factory):
    async with db_factory() as db:
        queued = Task(title="queued resume", description="d", status="completed")
        running = Task(title="running", description="d", status="executing")
        db.add_all([queued, running])
        await db.commit()
        await db.refresh(queued)

    dispatcher = MagicMock()
    dispatcher.pending_task_start_ids = AsyncMock(return_value={queued.id})
    svc = _make_service(
        tmp_path,
        db_factory=db_factory,
        dispatcher=dispatcher,
    )

    blockers = await svc._get_blocking_tasks()

    assert [(task["title"], task["status"]) for task in blockers] == [
        ("running", "executing"),
        ("queued resume", "queued_resume"),
    ]


@pytest.mark.asyncio
async def test_start_update_blocks_running_prompt_only_instance(
    tmp_path, db_factory,
):
    """A launched prompt-only instance remains restart-blocking without a Task."""
    async with db_factory() as db:
        instance = Instance(
            name="ad-hoc prompt",
            status="idle",
        )
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    dispatcher = _make_gate_dispatcher(db_factory)
    # Model the prompt-only API/InstanceManager admission point: launch has
    # completed and persisted a running Instance, but there is intentionally
    # no Task row that UpdateService could discover.
    async with dispatcher.task_start_guard():
        async with db_factory() as db:
            launched = await db.get(Instance, instance_id)
            launched.status = "running"
            launched.current_task_id = None
            launched.pid = 43210
            await db.commit()

    svc = _make_service(
        tmp_path,
        db_factory=db_factory,
        dispatcher=dispatcher,
    )
    svc._run_pipeline = AsyncMock()

    result = await svc.start_update(force=True)

    assert result["update_blocked"] is True
    assert result["active_task_count"] == 1
    assert result["active_tasks"] == [{
        "id": instance_id,
        "instance_id": instance_id,
        "title": "实例 ad-hoc prompt（未关联任务）",
        "status": "running_instance",
        "kind": "instance",
    }]
    assert dispatcher.status()["paused"] is False
    svc._run_pipeline.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_task_with_running_instance_still_blocks_update(
    tmp_path, db_factory,
):
    """Late output may revive this Task; the live process is authoritative."""
    async with db_factory() as db:
        task = Task(
            title="prematurely completed",
            description="d",
            status="completed",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="late-output",
            status="running",
            current_task_id=task.id,
            pid=43211,
        )
        db.add(instance)
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    service = _make_service(tmp_path, db_factory=db_factory)
    blockers = await service._get_blocking_tasks()

    assert blockers == [
        {
            "id": instance_id,
            "instance_id": instance_id,
            "title": f"实例 late-output（任务 #{task_id} 仍有运行进程）",
            "status": "running_instance",
            "kind": "instance",
        }
    ]


@pytest.mark.asyncio
async def test_enqueue_then_clear_before_dequeue_removes_resume_blocker(
    tmp_path, db_factory,
):
    """Stop-session cannot leave a phantom queued_resume maintenance blocker."""
    async with db_factory() as db:
        task = Task(
            title="cleared resume",
            description="d",
            status="completed",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    dispatcher = _make_gate_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()
    svc = _make_service(
        tmp_path,
        db_factory=db_factory,
        dispatcher=dispatcher,
    )

    await dispatcher.enqueue_message(task_id, "cancel before dequeue")
    assert [item["status"] for item in await svc._get_blocking_tasks()] == [
        "queued_resume"
    ]

    assert await dispatcher.clear_task_queue(task_id) == 1
    assert await dispatcher.pending_task_start_ids() == set()
    assert await svc._get_blocking_tasks() == []


@pytest.mark.asyncio
async def test_start_update_pauses_and_refuses_active_tasks(tmp_path):
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.pending_task_start_ids = AsyncMock(return_value=set())
    svc = _make_service(tmp_path, dispatcher=dispatcher)
    svc._get_active_tasks = AsyncMock(return_value=[
        {"id": 7, "title": "still running", "status": "executing"}
    ])

    with patch("backend.services.update_service.asyncio.create_task") as create_task:
        result = await svc.start_update(force=True)

    assert result["update_blocked"] is True
    assert result["active_task_count"] == 1
    assert "1 个任务" in result["error"]
    dispatcher.pause_dispatching.assert_awaited_once()
    dispatcher.resume_dispatching.assert_called_once()
    create_task.assert_not_called()
    assert svc._current is None


@pytest.mark.asyncio
async def test_start_update_fails_closed_when_task_check_errors(tmp_path):
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.pending_task_start_ids = AsyncMock(return_value=set())
    svc = _make_service(tmp_path, dispatcher=dispatcher)
    svc._get_active_tasks = AsyncMock(side_effect=RuntimeError("database offline"))

    with patch("backend.services.update_service.asyncio.create_task") as create_task:
        result = await svc.start_update()

    assert "无法确认当前任务状态" in result["error"]
    dispatcher.pause_dispatching.assert_awaited_once()
    dispatcher.resume_dispatching.assert_called_once()
    create_task.assert_not_called()
    assert svc._current is None


@pytest.mark.asyncio
async def test_cancelled_update_admission_reopens_task_start_gate(tmp_path):
    """A disconnected update request must not leave dispatch paused forever."""
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.pending_task_start_ids = AsyncMock(return_value=set())
    svc = _make_service(tmp_path, dispatcher=dispatcher)
    checking = asyncio.Event()
    never_finishes = asyncio.Event()

    async def blocked_task_check():
        checking.set()
        await never_finishes.wait()
        return []

    svc._get_blocking_tasks = AsyncMock(side_effect=blocked_task_check)
    request_task = asyncio.create_task(svc.start_update())
    await asyncio.wait_for(checking.wait(), timeout=1)

    request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    dispatcher.pause_dispatching.assert_awaited_once()
    dispatcher.resume_dispatching.assert_called_once()
    assert svc._current is None


@pytest.mark.asyncio
async def test_rollback_pauses_and_refuses_active_tasks(tmp_path):
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.pending_task_start_ids = AsyncMock(return_value=set())
    svc = _make_service(tmp_path, dispatcher=dispatcher)
    svc._current = _make_state()
    svc._current.status = "completed"
    svc._get_active_tasks = AsyncMock(return_value=[
        {"id": 8, "title": "still running", "status": "in_progress"}
    ])
    svc._spawn_update_script = MagicMock()

    result = await svc.rollback()

    assert result["update_blocked"] is True
    assert result["active_task_count"] == 1
    assert "1 个任务" in result["error"]
    dispatcher.pause_dispatching.assert_awaited_once()
    dispatcher.resume_dispatching.assert_called_once()
    svc._spawn_update_script.assert_not_called()


@pytest.mark.asyncio
async def test_cancelled_rollback_admission_reopens_task_start_gate(tmp_path):
    """Rollback cancellation before shutdown must release maintenance pause."""
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.pending_task_start_ids = AsyncMock(return_value=set())
    svc = _make_service(tmp_path, dispatcher=dispatcher)
    svc._current = _make_state()
    svc._current.status = "completed"
    checking = asyncio.Event()
    never_finishes = asyncio.Event()

    async def blocked_task_check():
        checking.set()
        await never_finishes.wait()
        return []

    svc._get_blocking_tasks = AsyncMock(side_effect=blocked_task_check)
    request_task = asyncio.create_task(svc.rollback())
    await asyncio.wait_for(checking.wait(), timeout=1)

    request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    dispatcher.pause_dispatching.assert_awaited_once()
    dispatcher.resume_dispatching.assert_called_once()
    assert svc._current.status == "completed"


@pytest.mark.asyncio
async def test_rollback_and_update_share_operation_admission_lock(tmp_path):
    """A paused rollback cannot have its captured state replaced by update."""
    svc = _make_service(tmp_path)
    rollback_state = _make_state()
    rollback_state.status = "completed"
    rollback_state.old_commit = "rollback-source-commit"
    rollback_state.backup_file = "rollback-source-backup.db"
    svc._current = rollback_state

    rollback_paused = asyncio.Event()
    release_rollback = asyncio.Event()
    pause_calls = 0

    async def pause_after_initial_check():
        nonlocal pause_calls
        pause_calls += 1
        if pause_calls == 1:
            rollback_paused.set()
            await release_rollback.wait()

    svc._pause_dispatching = AsyncMock(side_effect=pause_after_initial_check)
    svc._get_blocking_tasks = AsyncMock(return_value=[])
    svc._broadcast = AsyncMock()
    svc._write_status_file = MagicMock()
    svc._spawn_update_script = MagicMock()
    svc._run_pipeline = AsyncMock()
    real_sleep = asyncio.sleep

    with patch(
        "backend.services.update_service.asyncio.sleep", new=AsyncMock()
    ):
        rollback_task = asyncio.create_task(svc.rollback())
        await asyncio.wait_for(rollback_paused.wait(), timeout=1)

        update_task = asyncio.create_task(svc.start_update(force=True))
        await real_sleep(0)
        update_waited_for_admission = not update_task.done()

        release_rollback.set()
        rollback_result = await asyncio.wait_for(rollback_task, timeout=1)
        update_result = await asyncio.wait_for(update_task, timeout=1)

    assert rollback_result == {
        "status": "rolling_back",
        "old_commit": "rollback-source-commit",
    }
    assert update_waited_for_admission is True
    assert "正在进行中" in update_result["error"]
    assert svc._current is rollback_state
    svc._pause_dispatching.assert_awaited_once()
    call = svc._spawn_update_script.call_args
    assert call.args[:3] == (
        "rollback_code",
        "rollback-source-commit",
        "",
    )
    svc._run_pipeline.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_rollbacks_admit_only_one_operation(tmp_path):
    """A second rollback waits for admission, then sees the reservation."""
    svc = _make_service(tmp_path)
    rollback_state = _make_state()
    rollback_state.status = "completed"
    svc._current = rollback_state

    first_paused = asyncio.Event()
    release_first = asyncio.Event()
    pause_calls = 0

    async def pause_first_rollback():
        nonlocal pause_calls
        pause_calls += 1
        if pause_calls == 1:
            first_paused.set()
            await release_first.wait()

    svc._pause_dispatching = AsyncMock(side_effect=pause_first_rollback)
    svc._get_blocking_tasks = AsyncMock(return_value=[])
    svc._broadcast = AsyncMock()
    svc._write_status_file = MagicMock()
    svc._spawn_update_script = MagicMock()
    real_sleep = asyncio.sleep

    with patch(
        "backend.services.update_service.asyncio.sleep", new=AsyncMock()
    ):
        first_task = asyncio.create_task(svc.rollback())
        await asyncio.wait_for(first_paused.wait(), timeout=1)

        second_task = asyncio.create_task(svc.rollback())
        await real_sleep(0)
        second_waited_for_admission = not second_task.done()

        release_first.set()
        first_result = await asyncio.wait_for(first_task, timeout=1)
        second_result = await asyncio.wait_for(second_task, timeout=1)

    assert first_result["status"] == "rolling_back"
    assert second_waited_for_admission is True
    assert second_result == {"error": "有操作正在进行中"}
    svc._pause_dispatching.assert_awaited_once()
    svc._spawn_update_script.assert_called_once()


@pytest.mark.asyncio
async def test_needs_restart_compares_running_and_disk_sha_without_systemd(tmp_path):
    svc = _make_service(tmp_path, running_commit="a" * 40)
    svc._disk_commit = AsyncMock(return_value="b" * 40)
    assert await svc._needs_restart() is True

    svc._disk_commit = AsyncMock(return_value="a" * 40)
    assert await svc._needs_restart() is False


@pytest.mark.asyncio
async def test_concurrent_dry_runs_share_one_remote_check(tmp_path):
    svc = _make_service(tmp_path)
    calls = 0

    async def remote_check(branch):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return {
            "has_updates": True,
            "branch": branch,
            "latest_commit": "b" * 7,
        }

    svc._check_remote_updates = AsyncMock(side_effect=remote_check)

    results = await asyncio.gather(
        svc.dry_run(),
        svc.dry_run(),
        svc.dry_run(),
    )

    assert calls == 1
    assert all(result["has_updates"] is True for result in results)
    assert all(result["active_task_count"] == 0 for result in results)


@pytest.mark.asyncio
async def test_main_dry_run_identifies_its_update_channel(tmp_path):
    svc = _make_service(tmp_path)
    svc._check_remote_updates = AsyncMock(return_value={
        "has_updates": False,
        "branch": "main",
    })

    result = await svc.dry_run(channel="main", force=True)

    assert result["channel"] == "main"


@pytest.mark.asyncio
async def test_dry_run_cache_keeps_blockers_fresh_and_force_bypasses_cache(tmp_path):
    svc = _make_service(tmp_path)
    svc._check_remote_updates = AsyncMock(return_value={
        "has_updates": True,
        "latest_commit": "b" * 7,
    })
    svc._get_active_tasks = AsyncMock(side_effect=[
        [],
        [{"id": 11, "title": "now running", "status": "executing"}],
        [],
    ])

    first = await svc.dry_run()
    second = await svc.dry_run()
    forced = await svc.dry_run(force=True)

    assert first["update_blocked"] is False
    assert second["update_blocked"] is True
    assert second["active_task_count"] == 1
    assert forced["update_blocked"] is False
    assert svc._check_remote_updates.await_count == 2


@pytest.mark.asyncio
async def test_resolve_remote_uses_tracking_remote_then_origin_fallback(tmp_path):
    svc = _make_service(tmp_path)
    svc._run_cmd = AsyncMock(return_value={
        "returncode": 0, "stdout": "upstream\n", "stderr": "",
    })
    assert await svc._resolve_remote("main") == "upstream"

    svc._run_cmd = AsyncMock(return_value={
        "returncode": 1, "stdout": "", "stderr": "not configured",
    })
    assert await svc._resolve_remote("feature") == "origin"


@pytest.mark.asyncio
async def test_manual_pull_uses_running_commit_as_deployment_base(tmp_path):
    running = "a" * 40
    disk = "b" * 40
    svc = _make_service(tmp_path, running_commit=running)
    svc._run_cmd = AsyncMock(return_value={
        "returncode": 0, "stdout": "", "stderr": "",
    })

    assert await svc._deployment_base_commit(disk) == running
    svc._run_cmd.assert_awaited_once_with(
        ["git", "merge-base", "--is-ancestor", running, disk]
    )


@pytest.mark.asyncio
async def test_dry_run_detects_manual_update_and_returns_blockers(tmp_path):
    running = "a" * 40
    disk = "b" * 40
    svc = _make_service(tmp_path, running_commit=running)
    svc._get_active_tasks = AsyncMock(return_value=[
        {"id": 9, "title": "busy", "status": "in_progress"}
    ])
    svc._resolve_remote = AsyncMock(return_value="upstream")

    async def run_cmd(cmd, **_kwargs):
        if cmd[:2] == ["git", "fetch"]:
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if cmd == ["git", "rev-parse", "HEAD"]:
            return {"returncode": 0, "stdout": disk, "stderr": ""}
        if cmd == ["git", "rev-parse", "upstream/main"]:
            return {"returncode": 0, "stdout": disk, "stderr": ""}
        raise AssertionError(f"unexpected command: {cmd}")

    svc._run_cmd = AsyncMock(side_effect=run_cmd)
    result = await svc.dry_run()

    assert result["has_updates"] is False
    assert result["needs_restart"] is True
    assert result["manual_update_detected"] is True
    assert result["remote"] == "upstream"
    assert result["running_commit"] == running
    assert result["active_task_count"] == 1
    assert result["update_blocked"] is True


@pytest.mark.asyncio
async def test_dry_run_keeps_manual_restart_signal_when_fetch_fails(tmp_path):
    running = "a" * 40
    disk = "b" * 40
    svc = _make_service(tmp_path, running_commit=running)
    svc._resolve_remote = AsyncMock(return_value="upstream")
    svc._disk_commit = AsyncMock(return_value=disk)
    svc._run_cmd = AsyncMock(return_value={
        "returncode": 1,
        "stdout": "",
        "stderr": "network unavailable",
    })

    result = await svc.dry_run()

    assert result["has_updates"] is False
    assert result["needs_restart"] is True
    assert result["manual_update_detected"] is True
    assert result["current_commit"] == disk
    assert result["running_commit"] == running
    assert result["error"] == "network unavailable"


@pytest.mark.asyncio
async def test_dry_run_does_not_report_local_ahead_as_update(tmp_path):
    head = "c" * 40
    remote_head = "b" * 40
    svc = _make_service(tmp_path, running_commit=head)
    svc._resolve_remote = AsyncMock(return_value="upstream")

    async def run_cmd(cmd, **_kwargs):
        if cmd[:2] == ["git", "fetch"]:
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if cmd == ["git", "rev-parse", "HEAD"]:
            return {"returncode": 0, "stdout": head, "stderr": ""}
        if cmd == ["git", "rev-parse", "upstream/main"]:
            return {"returncode": 0, "stdout": remote_head, "stderr": ""}
        if cmd[:3] == ["git", "log", "--oneline"]:
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if cmd[:3] == ["git", "diff", "--name-only"]:
            return {"returncode": 0, "stdout": "", "stderr": ""}
        raise AssertionError(f"unexpected command: {cmd}")

    svc._run_cmd = AsyncMock(side_effect=run_cmd)
    result = await svc.dry_run()

    assert result["has_updates"] is False
    assert result["commits_behind"] == 0


@pytest.mark.asyncio
async def test_pipeline_rechecks_tasks_before_restart_and_resumes_dispatcher(tmp_path):
    dispatcher = MagicMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.pending_task_start_ids = AsyncMock(return_value=set())
    svc = _make_service(tmp_path, dispatcher=dispatcher)
    state = _make_state()
    commit = "a" * 40

    async def run_cmd(cmd, **_kwargs):
        if cmd == ["git", "rev-parse", "HEAD"]:
            return {"returncode": 0, "stdout": commit, "stderr": ""}
        if cmd == ["git", "rev-parse", "--abbrev-ref", "HEAD"]:
            return {"returncode": 0, "stdout": "main", "stderr": ""}
        return {"returncode": 0, "stdout": "", "stderr": ""}

    svc._run_cmd = AsyncMock(side_effect=run_cmd)
    svc._reserve_database_backup = MagicMock(
        return_value=str(tmp_path / "backup.db")
    )
    svc._get_active_tasks = AsyncMock(return_value=[
        {"id": 12, "title": "started during update", "status": "executing"}
    ])
    svc._migration_path = AsyncMock()
    svc._fast_restart_path = AsyncMock()
    assert svc._claim_deployment_lease("update")

    await svc._run_pipeline(state, force=True)

    assert state.status == "failed"
    assert "已取消重启" in state.error
    assert state.steps[7].status == "failed"
    svc._migration_path.assert_not_awaited()
    svc._fast_restart_path.assert_not_awaited()
    dispatcher.resume_dispatching.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path_name", "source"),
    [("fast", "user"), ("migration", "monitor:complete")],
)
async def test_restart_paths_block_queued_resume_from_pre_restart_window(
    tmp_path, db_factory, path_name, source,
):
    """A resume accepted during the warning await must cancel shutdown."""
    dispatcher = _make_gate_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()
    await dispatcher.pause_dispatching()
    async with db_factory() as db:
        task = Task(
            title="resume during update",
            description="d",
            status="completed",
            session_id="session-1",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    svc = _make_service(tmp_path, db_factory=db_factory, dispatcher=dispatcher)
    state = _make_state()
    state.new_commit = "new" * 10
    svc._restart_service = MagicMock()
    svc._spawn_update_script = MagicMock()

    async def broadcast(event, **_kwargs):
        if event == "restarting" and not dispatcher._pending_task_starts:
            await dispatcher.enqueue_message(
                task_id=task_id,
                prompt="continue",
                source=source,
            )

    svc._broadcast = AsyncMock(side_effect=broadcast)
    with patch(
        "backend.services.update_service.asyncio.sleep", new=AsyncMock()
    ):
        if path_name == "fast":
            result = await svc._fast_restart_path(state)
        else:
            result = await svc._migration_path(state)

    assert result is False
    assert state.status == "failed"
    svc._restart_service.assert_not_called()
    svc._spawn_update_script.assert_not_called()
    assert await dispatcher.pending_task_start_ids() == {task_id}
    dispatcher.resume_dispatching()


@pytest.mark.asyncio
async def test_manual_pull_fast_restart_branch_uses_final_gate(
    tmp_path, db_factory,
):
    dispatcher = _make_gate_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()
    await dispatcher.pause_dispatching()
    async with db_factory() as db:
        task = Task(
            title="queued manual-pull resume",
            description="d",
            status="completed",
            session_id="session-1",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
    await dispatcher.enqueue_message(task_id, "continue")

    commit = "a" * 40
    svc = _make_service(
        tmp_path,
        db_factory=db_factory,
        dispatcher=dispatcher,
        running_commit=commit,
    )
    state = _make_state()
    svc._needs_restart = AsyncMock(return_value=True)
    svc._restart_service = MagicMock()

    async def run_cmd(cmd, **_kwargs):
        if cmd == ["git", "rev-parse", "HEAD"]:
            return {"returncode": 0, "stdout": commit, "stderr": ""}
        if cmd == ["git", "rev-parse", "--abbrev-ref", "HEAD"]:
            return {"returncode": 0, "stdout": "main", "stderr": ""}
        return {"returncode": 0, "stdout": "", "stderr": ""}

    svc._run_cmd = AsyncMock(side_effect=run_cmd)
    with patch(
        "backend.services.update_service.asyncio.sleep", new=AsyncMock()
    ):
        await svc._pipeline_inner(state, False, False, branch="main")

    assert state.status == "completed"
    svc._restart_service.assert_not_called()
    dispatcher.resume_dispatching()


@pytest.mark.asyncio
async def test_rollback_rechecks_queued_resume_after_warning(
    tmp_path, db_factory,
):
    dispatcher = _make_gate_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()
    async with db_factory() as db:
        task = Task(
            title="rollback race",
            description="d",
            status="completed",
            session_id="session-1",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    svc = _make_service(tmp_path, db_factory=db_factory, dispatcher=dispatcher)
    svc._current = _make_state()
    svc._current.status = "completed"
    svc._spawn_update_script = MagicMock()

    async def broadcast(event, **_kwargs):
        if event == "restarting":
            await dispatcher.enqueue_message(task_id, "continue", source="user")

    svc._broadcast = AsyncMock(side_effect=broadcast)
    with patch(
        "backend.services.update_service.asyncio.sleep", new=AsyncMock()
    ):
        result = await svc.rollback()

    assert result["update_blocked"] is True
    assert result["active_tasks"][0]["status"] == "queued_resume"
    svc._spawn_update_script.assert_not_called()
    assert dispatcher.status()["paused"] is False


@pytest.mark.asyncio
async def test_shutdown_commit_is_atomic_and_seals_new_enqueues(
    tmp_path, db_factory,
):
    from backend.services.dispatcher import TaskStartPausedError

    dispatcher = _make_gate_dispatcher(db_factory)
    await dispatcher.pause_dispatching()
    svc = _make_service(tmp_path, db_factory=db_factory, dispatcher=dispatcher)
    observed = []

    def action():
        observed.append(
            (
                dispatcher._dispatch_claim_lock.locked(),
                dispatcher._maintenance_shutdown_committed,
            )
        )

    blockers = await svc._commit_shutdown_if_idle(action)

    assert blockers == []
    assert observed == [(True, False)]
    assert dispatcher._maintenance_shutdown_committed is True
    with pytest.raises(TaskStartPausedError):
        await dispatcher.enqueue_message(999, "too late")


@pytest.mark.asyncio
async def test_final_shutdown_check_fails_closed_on_query_error(
    tmp_path, db_factory,
):
    dispatcher = _make_gate_dispatcher(db_factory)
    await dispatcher.pause_dispatching()
    svc = _make_service(tmp_path, db_factory=db_factory, dispatcher=dispatcher)
    svc._get_blocking_tasks = AsyncMock(side_effect=RuntimeError("database offline"))
    action = MagicMock()

    with pytest.raises(RuntimeError, match="database offline"):
        await svc._commit_shutdown_if_idle(action)

    action.assert_not_called()
    assert dispatcher._maintenance_shutdown_committed is False


# ---- _migration_path escapes the service cgroup ----


@pytest.mark.asyncio
async def test_migration_path_uses_systemd_run_when_managed(tmp_path):
    svc = _make_service(tmp_path)
    state = _make_state()
    svc._current = state
    assert svc._claim_deployment_lease("update")

    with patch.object(svc, "_systemd_scope", return_value="user"), \
         patch("backend.services.update_service.subprocess.Popen") as popen, \
         patch("backend.services.update_service.asyncio.sleep", new=AsyncMock()):
        await svc._migration_path(state)

    argv = popen.call_args[0][0]
    assert "systemd-run" in Path(argv[0]).name
    assert "--user" in argv
    assert "--collect" in argv
    assert any(arg.startswith(f"--unit=ccm-update-{svc.port}-") for arg in argv)
    assert str(SCRIPT.name) in " ".join(argv)
    # the script itself must NOT rely on start_new_session here
    assert "start_new_session" not in popen.call_args.kwargs
    assert state.status == "restarting"


@pytest.mark.asyncio
async def test_migration_path_uses_system_systemd_run_for_system_service(tmp_path):
    svc = _make_service(tmp_path)
    state = _make_state()
    svc._current = state
    assert svc._claim_deployment_lease("update")

    with patch.object(svc, "_systemd_scope", return_value="system"), \
         patch("backend.services.update_service.subprocess.Popen") as popen, \
         patch("backend.services.update_service.asyncio.sleep", new=AsyncMock()):
        await svc._migration_path(state)

    argv = popen.call_args[0][0]
    assert argv[:4] == [svc._tools["sudo"], "-n", svc._tools["systemd-run"], "--collect"]
    assert "--user" not in argv
    assert "system" in argv
    assert f"--uid={os.getuid()}" in argv


@pytest.mark.asyncio
async def test_migration_path_plain_popen_when_not_managed(tmp_path):
    svc = _make_service(tmp_path)
    state = _make_state()
    svc._current = state
    assert svc._claim_deployment_lease("update")

    with patch.object(svc, "_systemd_scope", return_value=None), \
         patch("backend.services.update_service.subprocess.Popen") as popen, \
         patch("backend.services.update_service.asyncio.sleep", new=AsyncMock()):
        await svc._migration_path(state)

    argv = popen.call_args[0][0]
    assert "systemd-run" not in argv[0]
    assert popen.call_args.kwargs.get("start_new_session") is True


# ---- recover_from_status_file handles interrupted updates ----


def _write_status(svc: UpdateService, status: str, step: str):
    svc._status_file.write_text(json.dumps({
        "status": status,
        "message": "x",
        "step": step,
        "old_commit": "abc",
        "backup_file": "/tmp/b.db",
        "port": 8999,
        "timestamp": "2026-07-16T05:33:40+00:00",
    }))


@pytest.mark.parametrize("status,step", [("stopping", "stop_service"), ("migrating", "alembic_upgrade")])
def test_recover_marks_interrupted_update_failed(tmp_path, status, step):
    svc = _make_service(tmp_path)
    _write_status(svc, status, step)

    svc.recover_from_status_file()

    assert svc._current is not None
    assert svc._current.status == "failed"
    assert "中断" in svc._current.error
    failed = [s for s in svc._current.steps if s.status == "failed"]
    assert [s.name for s in failed] == [step]


@pytest.mark.parametrize("status", ["restarting", "starting"])
def test_legacy_unverified_restart_is_not_assumed_completed(tmp_path, status):
    svc = _make_service(tmp_path)
    _write_status(svc, status, "start_service")

    svc.recover_from_status_file()

    assert svc._current is not None
    assert svc._current.status == "failed"
    assert svc._current.deployment_incomplete is True


# ---- rollback must never touch the DB while the service is running ----


@pytest.mark.asyncio
async def test_rollback_delegates_to_script_when_managed(tmp_path):
    svc = _make_service(tmp_path)
    svc._current = _make_state()
    svc._current.status = "completed"
    backup = tmp_path / "backup.db"
    backup.write_text("db")
    svc._current.backup_file = str(backup)

    with patch.object(svc, "_systemd_scope", return_value="user"), \
         patch("backend.services.update_service.subprocess.Popen") as popen, \
         patch("backend.services.update_service.asyncio.sleep", new=AsyncMock()):
        result = await svc.rollback()

    assert result["status"] == "rolling_back"
    argv = popen.call_args[0][0]
    assert "systemd-run" in Path(argv[0]).name
    assert "rollback" in argv


@pytest.mark.asyncio
async def test_rollback_non_systemd_delegates_to_script_kill_mode(tmp_path):
    svc = _make_service(tmp_path)
    svc._current = _make_state()
    svc._current.status = "completed"
    backup = tmp_path / "backup.db"
    backup.write_text("db")
    svc._current.backup_file = str(backup)

    with patch.object(svc, "_systemd_scope", return_value=None), \
         patch("backend.services.update_service.subprocess.Popen") as popen, \
         patch("backend.services.update_service.asyncio.sleep", new=AsyncMock()):
        result = await svc.rollback()

    assert result["status"] == "rolling_back"
    argv = popen.call_args[0][0]
    assert "systemd-run" not in argv[0]
    assert "-" in argv and "rollback" in argv          # kill/respawn mode
    assert str(os.getpid()) in argv                    # pid to kill


# ---- _is_managed_by_systemd answers for THIS process, not the unit ----


def test_is_managed_true_only_when_self_in_service_cgroup(tmp_path):
    svc = _make_service(tmp_path)
    svc._service_name = "ccm.service"  # pin: settings.service_name varies per .env
    with patch.object(svc, "_cgroup_text",
                      return_value="0::/user.slice/user-1000.slice/user@1000.service/app.slice/ccm.service\n"):
        assert svc._is_managed_by_systemd() is True
        assert svc._systemd_scope() == "user"
    with patch.object(svc, "_cgroup_text",
                      return_value="0::/system.slice/ccm.service\n"):
        assert svc._is_managed_by_systemd() is True
        assert svc._systemd_scope() == "system"
    # orphan uvicorn in a login session — unit may be active, but WE are not it
    with patch.object(svc, "_cgroup_text",
                      return_value="0::/user.slice/user-1000.slice/session-19215.scope\n"):
        assert svc._is_managed_by_systemd() is False
    with patch.object(svc, "_cgroup_text", side_effect=FileNotFoundError):
        assert svc._is_managed_by_systemd() is False


def test_migrate_script_rollback_mode(tmp_path):
    """Script rollback mode: stop → restore DB → git reset → start."""
    env, call_log = _script_env(tmp_path)

    project = tmp_path / "proj"
    project.mkdir()
    genv = {**env, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run(["git", "init", "-q"], cwd=project, check=True, env=genv)
    (project / "f.txt").write_text("v1")
    subprocess.run(["git", "add", "."], cwd=project, check=True, env=genv)
    subprocess.run(["git", "commit", "-qm", "v1"], cwd=project, check=True, env=genv)
    old = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, capture_output=True, text=True, env=genv).stdout.strip()
    (project / "f.txt").write_text("v2")
    subprocess.run(["git", "commit", "-aqm", "v2"], cwd=project, check=True, env=genv)

    db = tmp_path / "claude_manager.db"
    backup = tmp_path / "backup.db"
    for path, value in ((db, "new"), (backup, "old")):
        connection = sqlite3.connect(path)
        try:
            connection.execute("CREATE TABLE marker(value TEXT)")
            connection.execute("INSERT INTO marker VALUES (?)", (value,))
            connection.commit()
        finally:
            connection.close()

    subprocess.run(
        ["bash", str(SCRIPT), str(project), old, str(backup), "8999",
         str(db), "ccm.service", "rollback"],
        env=env, check=True, timeout=30,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    connection = sqlite3.connect(db)
    try:
        assert connection.execute("SELECT value FROM marker").fetchone() == ("old",)
    finally:
        connection.close()
    assert (project / "f.txt").read_text() == "v1"
    calls = call_log.read_text()
    assert "stop ccm.service" in calls and "start ccm.service" in calls
    status = json.loads(Path("/tmp/ccm-update-status-8999.json").read_text())
    assert status["status"] == "rolled_back"


# ---- update_migrate.sh always brings the service back up ----


def _script_env(
    tmp_path: Path, *, escaped: bool = True
) -> tuple[dict, Path]:
    """Stub service-management tools into PATH; systemctl logs its calls.

    The test runner itself may live inside ``ccm.service``.  Normal-flow tests
    exercise the transient worker, not the short-lived trampoline, so mark it
    escaped explicitly; otherwise the trampoline legitimately returns before
    its asynchronous systemd-run worker has restored the DB or restarted the
    service.  Escape-specific tests opt out below.

    The runner may also live under /system.slice, in which case the script can
    choose system scope and call `sudo -n systemctl ...`.  Stub sudo too so a
    test can never escape the fake PATH and touch real systemd units.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    call_log = tmp_path / "systemctl.log"
    active_state = tmp_path / "systemctl.active"
    active_state.write_text("active")

    systemctl = bin_dir / "systemctl"
    systemctl.write_text(
        "#!/bin/bash\n"
        f'echo "$@" >> {call_log}\n'
        'if [ "${1:-}" = "--user" ]; then shift; fi\n'
        'case "${1:-}" in\n'
        f'  stop) echo inactive > {active_state}; exit 0;;\n'
        f'  start) echo active > {active_state}; exit 0;;\n'
        f'  is-active) grep -qx active {active_state}; exit $?;;\n'
        "esac\n"
        "exit 0\n"
    )
    sudo = bin_dir / "sudo"
    sudo.write_text('#!/bin/bash\nif [ "${1:-}" = "-n" ]; then shift; fi\nexec "$@"\n')
    # stub uv: alembic hangs so the test can kill the script mid-migration
    uv = bin_dir / "uv"
    uv.write_text('#!/bin/bash\nif [[ "$*" == *alembic* ]]; then sleep 30; fi\nexit 0\n')
    bwrap = bin_dir / "bwrap"
    bwrap.write_text("#!/bin/bash\nexit 0\n")
    socat = bin_dir / "socat"
    socat.write_text("#!/bin/bash\nexit 0\n")
    npm_root = tmp_path / "npm-root"
    npm = bin_dir / "npm"
    npm.write_text(
        "#!/bin/bash\n"
        'if [ "$*" = "root -g" ]; then\n'
        f"  printf '%s\\n' '{npm_root}'\n"
        "fi\n"
        "exit 0\n"
    )
    for arch in ("x64", "arm64"):
        apply_seccomp = (
            npm_root
            / "@anthropic-ai"
            / "sandbox-runtime"
            / "vendor"
            / "seccomp"
            / arch
            / "apply-seccomp"
        )
        apply_seccomp.parent.mkdir(parents=True, exist_ok=True)
        apply_seccomp.write_text("#!/bin/bash\nexit 0\n")
        apply_seccomp.chmod(
            apply_seccomp.stat().st_mode | stat.S_IEXEC
        )
    for f in (systemctl, sudo, uv, bwrap, socat, npm):
        f.chmod(f.stat().st_mode | stat.S_IEXEC)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["CCM_UPDATE_HEALTHCHECK_MODE"] = "skip"
    env["CCM_UPDATE_PIDCHECK_MODE"] = "skip"
    env["CCM_UPDATE_START_ATTEMPTS"] = "1"
    env["CCM_UPDATE_HEALTH_ATTEMPTS"] = "1"
    env["CCM_UPDATE_STABILITY_CHECKS"] = "1"
    if escaped:
        env["CCM_ESCAPED"] = "1"
    else:
        env.pop("CCM_ESCAPED", None)
    return env, call_log


def test_migrate_script_bare_uvicorn_mode(tmp_path):
    """SERVICE_NAME='-': stop = kill the given pid, start = respawn uvicorn."""
    env, call_log = _script_env(tmp_path)
    bin_dir = tmp_path / "bin"
    spawned_pid = tmp_path / "spawned.pid"
    python_stub = bin_dir / "python-stub"
    python_stub.write_text(
        "#!/bin/bash\n"
        'if [ "${1:-}" = "-m" ] && [ "${2:-}" = "uvicorn" ]; then\n'
        f'  echo "python $@" >> {call_log}\n'
        f'  echo "$$" > {spawned_pid}\n'
        "  exec sleep 30\n"
        "fi\n"
        f'exec {sys.executable} "$@"\n'
    )
    python_stub.chmod(python_stub.stat().st_mode | stat.S_IEXEC)

    project = tmp_path / "proj"
    project.mkdir()
    genv = {**env, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run(["git", "init", "-q"], cwd=project, check=True, env=genv)
    (project / "f.txt").write_text("v1")
    subprocess.run(["git", "add", "."], cwd=project, check=True, env=genv)
    subprocess.run(["git", "commit", "-qm", "v1"], cwd=project, check=True, env=genv)
    old = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, capture_output=True, text=True, env=genv).stdout.strip()

    db = tmp_path / "claude_manager.db"
    backup = tmp_path / "backup.db"
    for path, value in ((db, "new"), (backup, "old")):
        connection = sqlite3.connect(path)
        try:
            connection.execute("CREATE TABLE marker(value TEXT)")
            connection.execute("INSERT INTO marker VALUES (?)", (value,))
            connection.commit()
        finally:
            connection.close()

    dummy_server = subprocess.Popen(["sleep", "60"])
    try:
        subprocess.run(
            ["bash", str(SCRIPT), str(project), old, str(backup), "8999",
             str(db), "-", "rollback", str(dummy_server.pid), str(python_stub)],
            env=env, check=True, timeout=60,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        assert dummy_server.wait(timeout=5) != 0  # killed by svc_stop
        connection = sqlite3.connect(db)
        try:
            assert connection.execute("SELECT value FROM marker").fetchone() == ("old",)
        finally:
            connection.close()
        calls = call_log.read_text()
        assert "systemctl" not in calls or "ccm.service" not in calls
        assert "python -m uvicorn backend.main:app" in calls  # respawned
    finally:
        if dummy_server.poll() is None:
            dummy_server.kill()
        if spawned_pid.exists():
            os.kill(int(spawned_pid.read_text()), signal.SIGKILL)


def test_migrate_script_trap_starts_service_even_if_killed(tmp_path):
    """Reproduces the incident: script dies after stopping the service —
    the EXIT trap must still start the service."""
    env, call_log = _script_env(tmp_path)
    project = tmp_path / "proj"
    project.mkdir()
    genv = {
        **env,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    subprocess.run(["git", "init", "-q"], cwd=project, check=True, env=genv)
    (project / "f.txt").write_text("v1")
    subprocess.run(["git", "add", "."], cwd=project, check=True, env=genv)
    subprocess.run(
        ["git", "commit", "-qm", "v1"], cwd=project, check=True, env=genv
    )
    old = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project,
        check=True,
        capture_output=True,
        text=True,
        env=genv,
    ).stdout.strip()
    for path in (tmp_path / "backup.db", tmp_path / "claude_manager.db"):
        connection = sqlite3.connect(path)
        try:
            connection.execute("CREATE TABLE marker(value TEXT)")
            connection.execute("INSERT INTO marker VALUES ('db')")
            connection.commit()
        finally:
            connection.close()

    proc = subprocess.Popen(
        ["bash", str(SCRIPT), str(project), old,
         str(tmp_path / "backup.db"), "8999",
         str(tmp_path / "claude_manager.db"), "ccm.service"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    # wait until it has stopped the service and is stuck in "migration"
    deadline = time.time() + 10
    while time.time() < deadline:
        if call_log.exists() and "stop ccm.service" in call_log.read_text():
            break
        time.sleep(0.1)
    time.sleep(1.5)  # let it pass `sleep 1` and enter the hanging alembic

    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=10)

    calls = call_log.read_text()
    assert "stop ccm.service" in calls
    assert "start ccm.service" in calls, "EXIT trap must restart the service"


# ---- self-escape trampoline: the script itself must survive old backends ----
# The 2026-07-16 production outage: a pre-systemd-run backend spawned the
# (freshly pulled, already "fixed") script as a plain uvicorn child — inside
# the service cgroup — and its own `systemctl stop` killed it mid-stop.
# Fixing the Python spawn can't reach old deployments (they run their old
# Python), but the script is pulled fresh each update, so it must save itself.


def _self_cgroup_leaf() -> str | None:
    """Leaf name of the current process's cgroup (cgroup v2), e.g.
    'session-42.scope' — lets tests trigger the trampoline's match for real."""
    try:
        text = Path("/proc/self/cgroup").read_text()
    except OSError:
        return None
    path = text.strip().splitlines()[0].split("::", 1)[-1]
    leaf = path.rstrip("/").rsplit("/", 1)[-1]
    return leaf or None


def _stub_systemd_run(tmp_path: Path) -> Path:
    """Replace systemd-run with a logger so the escape is observable."""
    run_log = tmp_path / "systemd-run.log"
    stub = tmp_path / "bin" / "systemd-run"
    stub.write_text(f'#!/bin/bash\necho "$@" >> {run_log}\nexit 0\n')
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    return run_log


def test_migrate_script_escapes_own_service_cgroup(tmp_path):
    """Launched inside the service's own cgroup, the script must re-exec via
    systemd-run and NOT run `systemctl stop` from its doomed position."""
    env, call_log = _script_env(tmp_path, escaped=False)
    leaf = _self_cgroup_leaf()
    if not leaf:
        pytest.skip("cgroup v2 unavailable")
    run_log = _stub_systemd_run(tmp_path)

    project = tmp_path / "proj"
    project.mkdir()
    # SERVICE_NAME = our own cgroup leaf → the in-service detection matches
    result = subprocess.run(
        ["bash", str(SCRIPT), str(project), "deadbeef",
         str(tmp_path / "backup.db"), "8999",
         str(tmp_path / "claude_manager.db"), leaf, "migrate"],
        env=env, timeout=30, capture_output=True, text=True,
    )

    assert result.returncode == 0
    calls = run_log.read_text()
    assert "--setenv=CCM_ESCAPED=1" in calls
    assert str(SCRIPT) in calls, "must re-exec itself by absolute path"
    assert f"--working-directory={project}" in calls, \
        "transient units don't inherit cwd — must be pinned"
    stop_calls = call_log.read_text() if call_log.exists() else ""
    assert "stop" not in stop_calls, "must never stop the service from inside its cgroup"


def test_migrate_script_trampoline_runs_once(tmp_path):
    """CCM_ESCAPED=1 (the re-exec'd copy) must skip the trampoline and proceed
    into the normal flow — no infinite escape loop."""
    env, call_log = _script_env(tmp_path)
    leaf = _self_cgroup_leaf()
    if not leaf:
        pytest.skip("cgroup v2 unavailable")
    run_log = _stub_systemd_run(tmp_path)
    env["CCM_ESCAPED"] = "1"

    # project dir intentionally missing: proceeding past the trampoline means
    # dying at the `cd` guard with exit 1 — proof the escape was skipped
    result = subprocess.run(
        ["bash", str(SCRIPT), str(tmp_path / "missing"), "deadbeef",
         str(tmp_path / "backup.db"), "8999",
         str(tmp_path / "claude_manager.db"), leaf, "migrate"],
        env=env, timeout=30, capture_output=True, text=True,
    )

    assert result.returncode == 1
    assert not run_log.exists(), "escaped copy must not systemd-run again"
