"""Process-lifecycle tests for shared-project container execution."""

import asyncio
import fcntl
import os
import signal
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from backend.services.container_manager import (
    ContainerExecSpec,
    ContainerExecSpawnCleanupError,
    ContainerManager,
    ContainerTmpPressureError,
    _EXEC_CONTROL,
    _EXEC_SUPERVISOR,
    _TMP_LEASE_INIT,
    _TMP_PRESSURE_LIB,
)
from backend.services.instance_manager import (
    InstanceManager,
    SharedProjectAgentLaunchDisabledError,
)
from backend.services.process_safety import UnsafeProcessGroupError
from backend.models.instance import Instance
from backend.models.project import Project
from backend.models.task import Task


@pytest.mark.asyncio
async def test_api_account_mount_retirement_fails_when_docker_unavailable(
    tmp_path,
):
    account_root = tmp_path / "cloudrouter-1"
    account_root.mkdir()
    manager = ContainerManager()
    manager.is_docker_available = MagicMock(return_value=False)

    with pytest.raises(RuntimeError, match="Docker is unavailable"):
        await manager.retire_api_account_mounts(account_root)


@pytest.mark.asyncio
async def test_api_account_mount_retirement_fails_when_daemon_unverifiable(
    tmp_path,
):
    account_root = tmp_path / "cloudrouter-1"
    account_root.mkdir()
    manager = ContainerManager()
    manager.is_docker_available = MagicMock(return_value=True)
    manager._run = AsyncMock(return_value=(1, "daemon unavailable"))

    with pytest.raises(RuntimeError, match="Could not verify"):
        await manager.retire_api_account_mounts(account_root)


@pytest.mark.asyncio
async def test_api_account_mount_inspect_failure_requires_absence_proof(
    tmp_path,
):
    account_root = tmp_path / "cloudrouter-1"
    account_root.mkdir()
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"
    manager.is_docker_available = MagicMock(return_value=True)
    manager._run = AsyncMock(side_effect=[
        (0, "ccm-project-7\n"),
        (1, "inspect denied"),
        (0, "ccm-project-7\n"),
    ])

    with pytest.raises(RuntimeError, match="Could not inspect"):
        await manager.retire_api_account_mounts(account_root)

    assert manager._containers == {7: "ccm-project-7"}


@pytest.mark.asyncio
async def test_api_account_mount_disappearance_is_proven_before_forget(
    tmp_path,
):
    account_root = tmp_path / "cloudrouter-1"
    account_root.mkdir()
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"
    manager.is_docker_available = MagicMock(return_value=True)
    manager._run = AsyncMock(side_effect=[
        (0, ""),
        (1, "no such container"),
        (0, ""),
    ])

    assert await manager.retire_api_account_mounts(account_root) == 0
    assert manager._containers == {}


@pytest.mark.asyncio
async def test_api_account_mount_retirement_removes_only_exact_source(
    tmp_path,
):
    account_root = tmp_path / "cloudrouter-1"
    account_root.mkdir()
    other_root = tmp_path / "cloudrouter-2"
    other_root.mkdir()
    manager = ContainerManager()
    manager.is_docker_available = MagicMock(return_value=True)
    manager._run = AsyncMock(side_effect=[
        (0, "ccm-project-7\nccm-project-8\n"),
        (0, f"{account_root}\n"),
        (0, ""),
        (0, ""),
        (0, f"{other_root}\n"),
    ])

    assert await manager.retire_api_account_mounts(account_root) == 1
    commands = [item.args[0] for item in manager._run.await_args_list]
    assert ["docker", "stop", "-t", "10", "ccm-project-7"] in commands
    assert ["docker", "rm", "-f", "ccm-project-7"] in commands
    assert not any(
        command[-1] == "ccm-project-8"
        for command in commands
        if command[:2] in (["docker", "stop"], ["docker", "rm"])
    )


def _tmp_pressure_namespace() -> dict:
    namespace: dict = {}
    exec(_TMP_PRESSURE_LIB, namespace)
    return namespace


def _create_test_lease(tmp_path: Path) -> Path:
    lease = tmp_path / "tmp-pressure.lock"
    lease.touch(mode=0o444)
    return lease


def test_container_tmp_lease_requires_protected_parent_directory():
    assert "parent_stat.st_uid == 0" in _TMP_LEASE_INIT
    assert "parent_stat.st_mode & stat.S_ISVTX" in _TMP_LEASE_INIT
    assert "unsafe CCM runtime parent directory" in _TMP_LEASE_INIT


def test_container_tmp_pressure_cleanup_requires_idle_exclusive_lease(
    tmp_path,
):
    namespace = _tmp_pressure_namespace()
    private_tmp = tmp_path / "private-tmp"
    private_tmp.mkdir()
    stale = private_tmp / "agent-cache"
    stale.mkdir()
    (stale / "large.bin").write_bytes(b"x" * 1024)
    lease = _create_test_lease(tmp_path)
    usage = iter(
        [
            (0.80, 0.10),  # exact threshold must trigger
            (0.80, 0.10),  # exclusive-lease recheck
            (0.70, 0.10),  # cleanup moved below the trigger
            (0.60, 0.10),  # final shared-lease check
        ]
    )
    namespace["_usage_ratios"] = lambda _root: next(usage)
    namespace["_unexpected_processes"] = lambda: []

    lease_fd = namespace["acquire_agent_tmp_lease"](
        str(private_tmp),
        str(lease),
        os.geteuid(),
        0.80,
    )
    try:
        assert not stale.exists()
        competing_fd = os.open(lease, os.O_RDONLY)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(
                    competing_fd, fcntl.LOCK_EX | fcntl.LOCK_NB
                )
        finally:
            os.close(competing_fd)
    finally:
        os.close(lease_fd)


def test_container_tmp_pressure_busy_agent_blocks_launch_without_deleting(
    tmp_path,
):
    namespace = _tmp_pressure_namespace()
    private_tmp = tmp_path / "private-tmp"
    private_tmp.mkdir()
    active_file = private_tmp / "active-agent-state"
    active_file.write_text("keep", encoding="utf-8")
    lease = _create_test_lease(tmp_path)
    active_lease_fd = os.open(lease, os.O_RDONLY)
    fcntl.flock(active_lease_fd, fcntl.LOCK_SH)
    namespace["_usage_ratios"] = lambda _root: (0.85, 0.10)

    try:
        with pytest.raises(
            namespace["TmpPressureGateError"],
            match="while an Agent is active",
        ) as caught:
            namespace["acquire_agent_tmp_lease"](
                str(private_tmp),
                str(lease),
                os.geteuid(),
                0.80,
            )
    finally:
        os.close(active_lease_fd)

    assert caught.value.exit_code == namespace["TMP_BUSY_EXIT"]
    assert active_file.read_text(encoding="utf-8") == "keep"


def test_container_tmp_pressure_unknown_process_fails_closed(tmp_path):
    namespace = _tmp_pressure_namespace()
    private_tmp = tmp_path / "private-tmp"
    private_tmp.mkdir()
    evidence = private_tmp / "unknown-owner"
    evidence.write_text("keep", encoding="utf-8")
    lease = _create_test_lease(tmp_path)
    namespace["_usage_ratios"] = lambda _root: (0.85, 0.10)
    namespace["_unexpected_processes"] = lambda: [43210]

    with pytest.raises(
        namespace["TmpPressureGateError"],
        match="unexpected pids: 43210",
    ):
        namespace["acquire_agent_tmp_lease"](
            str(private_tmp),
            str(lease),
            os.geteuid(),
            0.80,
        )

    assert evidence.read_text(encoding="utf-8") == "keep"


def test_container_tmp_idle_proof_accepts_init_with_single_tail(monkeypatch):
    namespace = _tmp_pressure_namespace()
    commands = {
        1: [b"/sbin/docker-init", b"--", b"tail", b"-f", b"/dev/null"],
        7: [b"/usr/bin/tail", b"-f", b"/dev/null"],
    }
    monkeypatch.setattr(namespace["os"], "getpid", lambda: 42)
    monkeypatch.setattr(
        namespace["os"],
        "listdir",
        lambda path: ["1", "7", "42"] if path == "/proc" else [],
    )
    namespace["_read_process_command"] = lambda pid: commands[pid]
    namespace["_read_process_parent"] = lambda pid: 1

    assert namespace["_unexpected_processes"]() == []


@pytest.mark.asyncio
async def test_container_tmp_preflight_uses_configured_threshold():
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"
    manager._run = AsyncMock(return_value=(75, "Agent is active"))

    with (
        patch(
            "backend.services.container_manager.settings.tmp_cleanup_enabled",
            True,
        ),
        patch(
            "backend.services.container_manager.settings.tmp_cleanup_usage_threshold",
            0.80,
        ),
        pytest.raises(
            ContainerTmpPressureError,
            match="Agent is active",
        ),
    ):
        await manager.ensure_tmp_capacity(7)

    command = manager._run.await_args.args[0]
    assert command[:5] == [
        "docker",
        "exec",
        "-e",
        "CCM_CONTAINER_EXEC_ROLE=tmp-gate",
        "ccm-project-7",
    ]
    assert command[-4:] == [
        "/tmp",
        "/home/sandbox/.ccm-runtime/tmp-pressure.lock",
        "0",
        "0.8",
    ]


@pytest.mark.asyncio
async def test_container_run_cancellation_waits_for_gate_to_exit(monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()

    class FakeProcess:
        returncode = None
        killed = False

        async def communicate(self):
            started.set()
            await release.wait()
            self.returncode = 0
            return b"done", None

        def kill(self):
            self.killed = True
            self.returncode = -signal.SIGKILL
            release.set()

        async def wait(self):
            await release.wait()
            return self.returncode

    process = FakeProcess()
    monkeypatch.setattr(
        asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=process),
    )

    operation = asyncio.create_task(
        ContainerManager._run(["docker", "exec", "test"], timeout=1)
    )
    await started.wait()
    operation.cancel()
    await asyncio.sleep(0)
    assert operation.done() is False

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert process.killed is False


@pytest.mark.asyncio
async def test_new_container_initializes_root_lease_before_tmp_preflight(
    tmp_path,
):
    manager = ContainerManager()
    manager._run = AsyncMock(
        side_effect=[
            (1, ""),  # inspect: absent
            (0, ""),  # defensive rm
            (0, "container-id"),  # docker run
            (0, ""),  # root-owned lease initialization
            (0, ""),  # pressure preflight
        ]
    )

    with patch(
        "backend.services.container_manager.settings.tmp_cleanup_enabled",
        True,
    ):
        await manager.ensure_container(11, str(tmp_path / "project"))

    commands = [item.args[0] for item in manager._run.await_args_list]
    run_command = next(command for command in commands if command[:3] == [
        "docker",
        "run",
        "-d",
    ])
    assert "--init" in run_command
    assert commands[-2][:5] == [
        "docker",
        "exec",
        "-u",
        "0",
        "ccm-project-11",
    ]
    assert commands[-1][:5] == [
        "docker",
        "exec",
        "-e",
        "CCM_CONTAINER_EXEC_ROLE=tmp-gate",
        "ccm-project-11",
    ]


@pytest.mark.asyncio
async def test_running_legacy_container_is_not_recreated_just_for_init(
    tmp_path,
):
    manager = ContainerManager()
    manager._run = AsyncMock(
        side_effect=[
            (0, "true"),  # running container
            (0, ""),  # matching empty API account mount
            (0, ""),  # root-owned lease initialization
            (0, ""),  # pressure preflight
        ]
    )

    with patch(
        "backend.services.container_manager.settings.tmp_cleanup_enabled",
        True,
    ):
        await manager.ensure_container(12, str(tmp_path / "project"))

    commands = [item.args[0] for item in manager._run.await_args_list]
    assert ["docker", "rm", "-f", "ccm-project-12"] not in commands
    assert not any(command[:3] == ["docker", "run", "-d"] for command in commands)
    assert commands[-2][:5] == [
        "docker",
        "exec",
        "-u",
        "0",
        "ccm-project-12",
    ]
    assert commands[-1][:5] == [
        "docker",
        "exec",
        "-e",
        "CCM_CONTAINER_EXEC_ROLE=tmp-gate",
        "ccm-project-12",
    ]


@pytest.mark.asyncio
async def test_shared_project_launch_is_rejected_before_container_effect(
    db_factory,
    tmp_path,
):
    async with db_factory() as db:
        project = Project(
            name="pressured-container",
            local_path=str(tmp_path),
            status="ready",
        )
        instance = Instance(name="container-slot", status="running")
        db.add_all([project, instance])
        await db.flush()
        task = Task(
            title="must stay isolated",
            status="executing",
            provider="claude",
            project_id=project.id,
            instance_id=instance.id,
            target_repo=str(tmp_path),
        )
        db.add(task)
        await db.commit()
        await db.refresh(instance)
        await db.refresh(task)

    manager = InstanceManager(db_factory, MagicMock())
    manager._pty_enabled = True
    manager._pty_backend = MagicMock()
    container_manager = MagicMock()
    container_manager.ensure_container = AsyncMock(
        side_effect=ContainerTmpPressureError("container /tmp busy")
    )
    manager._container_mgr = container_manager

    with (
        patch(
            "backend.services.container_manager.is_shared_project",
            new=AsyncMock(return_value=True),
        ),
        patch.object(
            ContainerManager, "is_docker_available", return_value=True
        ) as docker_available,
        patch(
            "backend.services.mcp_config.generate_mcp_config",
            return_value=None,
        ),
        patch(
            "backend.services.ask_user_settings.ensure_ask_user_hook"
        ),
        patch.object(
            manager, "_launch_pty", new_callable=AsyncMock
        ) as launch_pty,
        patch.object(manager, "_build_command") as build_command,
        pytest.raises(
            SharedProjectAgentLaunchDisabledError,
            match="is shared",
        ),
    ):
        await manager._launch_locked(
            instance.id,
            "do isolated work",
            task_id=task.id,
            cwd=str(tmp_path),
            provider="claude",
        )

    docker_available.assert_not_called()
    container_manager.ensure_container.assert_not_awaited()
    container_manager.create_pty_wrapper.assert_not_called()
    launch_pty.assert_not_awaited()
    build_command.assert_not_called()
    assert instance.id not in manager.processes


@pytest.mark.asyncio
async def test_api_account_root_is_mounted_once_read_only_for_shared_project(
    tmp_path,
):
    manager = ContainerManager()
    project = tmp_path / "project"
    config = tmp_path / "cloudrouter-1" / "claude"
    api_root = config.parent
    config.mkdir(parents=True)
    manager._run = AsyncMock(side_effect=[
        (1, ""),  # container does not exist
        (0, ""),  # docker rm
        (0, "container-id"),  # docker run
    ])

    await manager.ensure_container(
        7,
        str(project),
        str(config),
        api_account_root=str(api_root),
    )

    run_command = manager._run.await_args_list[-1].args[0]
    assert (
        f"{api_root.resolve()}:/home/sandbox/.ccm-api-account:ro"
        in run_command
    )
    assert run_command.count(
        f"{api_root.resolve()}:/home/sandbox/.ccm-api-account:ro"
    ) == 1


@pytest.mark.asyncio
async def test_changed_api_account_mount_recreates_running_container(tmp_path):
    manager = ContainerManager()
    project = tmp_path / "project"
    config = tmp_path / "new" / "claude"
    api_root = config.parent
    config.mkdir(parents=True)
    manager._run = AsyncMock(side_effect=[
        (0, "true"),
        (0, str(tmp_path / "old")),
        (0, ""),
        (0, "container-id"),
    ])

    await manager.ensure_container(
        8,
        str(project),
        str(config),
        api_account_root=str(api_root),
    )

    commands = [call.args[0] for call in manager._run.await_args_list]
    assert ["docker", "rm", "-f", "ccm-project-8"] in commands


@pytest.mark.asyncio
async def test_exec_command_uses_tokenized_supervisor_and_host_session():
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"
    process = MagicMock()

    with (
        patch(
            "backend.services.container_manager.secrets.token_hex",
            return_value="fixed-token",
        ),
        patch(
            "backend.services.container_manager.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ) as spawn,
    ):
        returned = await manager.exec_command(
            7,
            ["claude", "-p", "literal; not shell"],
            env={"SAFE": "value with spaces"},
        )

    assert returned is process
    assert manager.owns_exec(process)
    args = spawn.await_args.args
    assert args[:5] == ("docker", "exec", "-i", "-w", "/workspace")
    assert "CCM_CONTAINER_EXEC_TOKEN=fixed-token" in args
    assert "CCM_CONTAINER_EXEC_ROLE=supervisor" in args
    assert args[-3:] == ("claude", "-p", "literal; not shell")
    assert _EXEC_SUPERVISOR in args
    supervisor_index = args.index(_EXEC_SUPERVISOR)
    assert args[supervisor_index + 2 : supervisor_index + 6] == (
        "/tmp",
        "/home/sandbox/.ccm-runtime/tmp-pressure.lock",
        "0",
        "0.8",
    )
    if os.name == "posix":
        assert spawn.await_args.kwargs["start_new_session"] is True


@pytest.mark.asyncio
async def test_cancelled_exec_spawn_reaps_host_and_tokenized_inner():
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"
    process = MagicMock(pid=54_340, returncode=None)
    process.wait = AsyncMock(return_value=-signal.SIGKILL)
    spawn_started = asyncio.Event()
    release_spawn = asyncio.Event()

    async def delayed_spawn(*args, **kwargs):
        spawn_started.set()
        await release_spawn.wait()
        return process

    def kill_host_group(pid, sig):
        assert pid == process.pid
        if sig == signal.SIGKILL:
            process.returncode = -signal.SIGKILL

    with (
        patch(
            "backend.services.container_manager.secrets.token_hex",
            return_value="cancelled-token",
        ),
        patch(
            "backend.services.container_manager.asyncio.create_subprocess_exec",
            side_effect=delayed_spawn,
        ),
        patch(
            "backend.services.container_manager.os.killpg",
            side_effect=kill_host_group,
        ) as killpg,
        patch.object(
            manager,
            "_control_spec",
            new_callable=AsyncMock,
            side_effect=[0, 3],
        ) as control,
    ):
        execution = asyncio.create_task(
            manager.exec_command(7, ["claude", "-p", "work"])
        )
        await asyncio.wait_for(spawn_started.wait(), timeout=2.0)
        execution.cancel()
        release_spawn.set()
        with pytest.raises(asyncio.CancelledError):
            await execution

    assert (process.pid, signal.SIGKILL) in [
        call.args for call in killpg.call_args_list
    ]
    assert control.await_args_list[0].kwargs == {
        "action": "signal",
        "sig": signal.SIGKILL,
        "wait_seconds": 2.0,
    }
    assert control.await_args_list[1].kwargs == {"action": "check"}
    assert not manager.owns_exec(process)


@pytest.mark.asyncio
async def test_cancelled_exec_spawn_cleanup_failure_exposes_exact_process():
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"
    process = MagicMock(pid=54_341, returncode=None)
    process.wait = AsyncMock(return_value=-signal.SIGKILL)
    spawn_started = asyncio.Event()
    release_spawn = asyncio.Event()

    async def delayed_spawn(*args, **kwargs):
        spawn_started.set()
        await release_spawn.wait()
        return process

    def kill_host_group(pid, sig):
        if sig == signal.SIGKILL:
            process.returncode = -signal.SIGKILL

    with (
        patch(
            "backend.services.container_manager.asyncio.create_subprocess_exec",
            side_effect=delayed_spawn,
        ),
        patch(
            "backend.services.container_manager.os.killpg",
            side_effect=kill_host_group,
        ),
        patch.object(
            manager,
            "_control_spec",
            new_callable=AsyncMock,
            side_effect=RuntimeError("docker control unavailable"),
        ),
    ):
        execution = asyncio.create_task(
            manager.exec_command(7, ["claude", "-p", "work"])
        )
        await asyncio.wait_for(spawn_started.wait(), timeout=2.0)
        execution.cancel()
        release_spawn.set()
        with pytest.raises(ContainerExecSpawnCleanupError) as caught:
            await execution

    assert caught.value.process is process
    assert manager.owns_exec(process)


@pytest.mark.parametrize("unsafe_pid", [None, -1, 0, 1, False, True])
@pytest.mark.asyncio
async def test_cancelled_exec_cleanup_rejects_unsafe_host_group_without_signal(
    unsafe_pid,
):
    manager = ContainerManager()
    process = MagicMock(pid=unsafe_pid, returncode=None)
    process.kill = MagicMock()
    process.wait = AsyncMock()
    spec = ContainerExecSpec(
        container_name="ccm-project-7",
        token="unsafe-host-group",
        pid_file="/tmp/ccm-exec-unsafe-host-group.pid",
    )

    with (
        patch("backend.services.container_manager.os.killpg") as killpg,
        patch.object(
            manager,
            "_control_spec",
            new_callable=AsyncMock,
        ) as control,
    ):
        with pytest.raises(
            UnsafeProcessGroupError,
            match="Refusing unsafe process group identity",
        ):
            await manager._cleanup_cancelled_exec_spawn(process, spec)

    killpg.assert_not_called()
    process.kill.assert_not_called()
    process.wait.assert_not_awaited()
    control.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_exec_targets_exact_tokenized_container_generation():
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"
    process = MagicMock(returncode=None)

    with (
        patch(
            "backend.services.container_manager.secrets.token_hex",
            return_value="exact-token",
        ),
        patch(
            "backend.services.container_manager.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ),
    ):
        await manager.exec_command(7, ["claude"])

    manager._run = AsyncMock(return_value=(0, ""))
    assert await manager.signal_exec(process, signal.SIGKILL) is True

    control_cmd = manager._run.await_args.args[0]
    assert control_cmd[:3] == ["docker", "exec", "ccm-project-7"]
    assert "exact-token" in control_cmd
    assert str(int(signal.SIGKILL)) in control_cmd


def test_pty_wrapper_uses_supervisor_and_is_instance_unique(tmp_path):
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"

    with (
        patch(
            "backend.services.container_manager.secrets.token_hex",
            return_value="pty-token",
        ),
        patch(
            "backend.services.container_manager.tempfile.gettempdir",
            return_value=str(tmp_path),
        ),
    ):
        wrapper_path, spec = manager.create_pty_wrapper(7, 19)

    try:
        assert wrapper_path.endswith(
            "ccm-docker-claude-19-pty-token.sh"
        )
        wrapper = Path(wrapper_path).read_text(encoding="utf-8")
        assert "CCM_CONTAINER_EXEC_TOKEN=pty-token" in wrapper
        assert "CCM_CONTAINER_EXEC_ROLE=supervisor" in wrapper
        assert "\"$@\"" in wrapper
        assert oct(os.stat(wrapper_path).st_mode & 0o777) == "0o700"
    finally:
        manager.discard_spec(spec)
    assert not os.path.exists(wrapper_path)


@pytest.mark.asyncio
async def test_container_control_failure_is_fail_closed():
    manager = ContainerManager()
    manager._containers[7] = "ccm-project-7"
    process = MagicMock(returncode=None)

    with patch(
        "backend.services.container_manager.asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=process),
    ):
        await manager.exec_command(7, ["claude"])

    manager._run = AsyncMock(return_value=(125, "docker daemon unavailable"))
    with pytest.raises(RuntimeError, match="docker daemon unavailable"):
        await manager.signal_exec(process, signal.SIGKILL)
    assert manager.owns_exec(process)


@pytest.mark.asyncio
async def test_instance_manager_signals_inner_exec_before_host_group():
    manager = InstanceManager(MagicMock(), MagicMock())
    process = MagicMock(pid=43210, returncode=None)
    container_manager = MagicMock()
    container_manager.owns_exec.return_value = True
    container_manager.signal_exec = AsyncMock(return_value=True)
    manager._container_mgr = container_manager
    manager._container_exec_processes[9] = process
    manager._container_tasks[9] = 7
    manager._process_groups[9] = process

    with patch("backend.services.instance_manager.os.killpg") as killpg:
        await manager._signal_managed_process_tree(
            9, process, signal.SIGTERM
        )

    container_manager.signal_exec.assert_awaited_once_with(
        process, signal.SIGTERM
    )
    killpg.assert_called_once_with(process.pid, signal.SIGTERM)


@pytest.mark.asyncio
async def test_instance_manager_waits_for_inner_group_before_forgetting_exec():
    manager = InstanceManager(MagicMock(), MagicMock())
    process = MagicMock(pid=43210, returncode=0)
    process.wait = AsyncMock(return_value=0)
    container_manager = MagicMock()
    container_manager.owns_exec.return_value = True
    container_manager.exec_is_alive = AsyncMock(side_effect=[True, False])
    manager._container_mgr = container_manager
    manager._container_exec_processes[9] = process
    manager._container_tasks[9] = 7

    with patch.object(manager, "_process_group_alive", return_value=False):
        await manager._wait_process_tree(9, process, 1.0)

    assert container_manager.exec_is_alive.await_count == 2
    container_manager.forget_exec.assert_called_once_with(process)
    assert 9 not in manager._container_exec_processes
    assert 9 not in manager._container_tasks


@pytest.mark.asyncio
async def test_inner_signal_failure_retains_generation_evidence():
    manager = InstanceManager(MagicMock(), MagicMock())
    process = MagicMock(pid=43210, returncode=None)
    container_manager = MagicMock()
    container_manager.owns_exec.return_value = True
    container_manager.signal_exec = AsyncMock(
        side_effect=RuntimeError("inner state unknown")
    )
    manager._container_mgr = container_manager
    manager._container_exec_processes[9] = process
    manager._container_tasks[9] = 7
    manager._process_groups[9] = process

    with (
        patch("backend.services.instance_manager.os.killpg"),
        pytest.raises(RuntimeError, match="inner state unknown"),
    ):
        await manager._signal_managed_process_tree(
            9, process, signal.SIGKILL
        )

    assert manager._container_exec_processes[9] is process
    assert manager._process_groups[9] is process


@pytest.mark.asyncio
async def test_pty_exit_kills_inner_survivors_before_forgetting_generation():
    manager = InstanceManager(MagicMock(), MagicMock())
    process = MagicMock(pid=43210, returncode=0)
    container_manager = MagicMock()
    container_manager.owns_exec.return_value = True
    container_manager.exec_is_alive = AsyncMock(side_effect=[True, False])
    container_manager.signal_exec = AsyncMock(return_value=True)
    manager._container_mgr = container_manager
    manager._container_exec_processes[9] = process
    manager._container_tasks[9] = 7

    await manager.finalize_pty_container_exec(9)

    container_manager.signal_exec.assert_awaited_once_with(
        process, signal.SIGKILL
    )
    container_manager.forget_exec.assert_called_once_with(process)
    assert 9 not in manager._container_exec_processes
    assert 9 not in manager._container_tasks


@pytest.mark.asyncio
async def test_stale_pty_exit_does_not_signal_replacement_container_generation():
    manager = InstanceManager(MagicMock(), MagicMock())
    old_process = MagicMock(pid=43209, returncode=0)
    replacement = MagicMock(pid=43210, returncode=None)
    container_manager = MagicMock()
    container_manager.owns_exec.return_value = True
    container_manager.exec_is_alive = AsyncMock(return_value=True)
    container_manager.signal_exec = AsyncMock()
    manager._container_mgr = container_manager
    manager._container_exec_processes[9] = replacement

    await manager.finalize_pty_container_exec(
        9, expected_process=old_process
    )

    container_manager.exec_is_alive.assert_not_awaited()
    container_manager.signal_exec.assert_not_awaited()
    assert manager._container_exec_processes[9] is replacement


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="requires POSIX sessions")
async def test_inner_supervisor_kills_descendants_after_agent_leader_exits(
    tmp_path,
):
    """Exercise the real supervisor without requiring a Docker daemon."""

    pid_file = tmp_path / "agent.pid"
    descendant_file = tmp_path / "descendant.pid"
    leader = (
        "import pathlib,signal,subprocess,sys;"
        "p=subprocess.Popen([sys.executable,'-c',"
        "'import signal,time;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)']);"
        f"pathlib.Path({str(descendant_file)!r}).write_text(str(p.pid))"
    )
    env = os.environ.copy()
    env["CCM_CONTAINER_EXEC_TOKEN"] = "supervisor-test"
    env["CCM_CONTAINER_EXEC_ROLE"] = "supervisor"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        _EXEC_SUPERVISOR,
        "0",
        str(tmp_path),
        str(tmp_path / "unused-lease"),
        str(os.geteuid()),
        "1.0",
        str(pid_file),
        sys.executable,
        "-c",
        leader,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    await asyncio.wait_for(process.wait(), timeout=5.0)
    descendant_pid = int(descendant_file.read_text())

    def descendant_is_live() -> bool:
        try:
            state = Path(f"/proc/{descendant_pid}/stat").read_text(
                encoding="utf-8"
            ).split()[2]
            return state != "Z"
        except FileNotFoundError:
            return False

    deadline = asyncio.get_running_loop().time() + 2.0
    while descendant_is_live() and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.02)
    assert not descendant_is_live()
    assert not pid_file.exists()


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="requires POSIX /proc")
async def test_exact_token_controller_stops_live_inner_group(tmp_path):
    """Validate the control protocol against real local processes."""

    pid_file = tmp_path / "controlled.pid"
    token = "exact-controller-test"
    env = os.environ.copy()
    env["CCM_CONTAINER_EXEC_TOKEN"] = token
    env["CCM_CONTAINER_EXEC_ROLE"] = "supervisor"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        _EXEC_SUPERVISOR,
        "0",
        str(tmp_path),
        str(tmp_path / "unused-lease"),
        str(os.geteuid()),
        "1.0",
        str(pid_file),
        sys.executable,
        "-c",
        "import time; time.sleep(60)",
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        deadline = asyncio.get_running_loop().time() + 2.0
        while (
            not pid_file.exists()
            and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(0.02)
        assert pid_file.exists()

        controller = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            _EXEC_CONTROL,
            token,
            str(pid_file),
            "signal",
            str(int(signal.SIGKILL)),
            "1.0",
        )
        assert await asyncio.wait_for(controller.wait(), timeout=3.0) == 0
        assert await asyncio.wait_for(process.wait(), timeout=3.0) == (
            128 + int(signal.SIGKILL)
        )
        assert not pid_file.exists()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
