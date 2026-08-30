"""Real-Git tests for the idempotent Delivery workspace boundary."""

import asyncio
from pathlib import Path
import shutil
import shlex
import subprocess
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.services import delivery_workspace
from backend.services.delivery_workspace import (
    DeliveryWorkspaceConflict,
    DeliveryWorkspaceError,
    DeliveryWorkspaceManager,
)


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, Path]:
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    repo = tmp_path / "repo"
    remote.mkdir()
    _git(remote, "init", "--bare")
    seed.mkdir()
    _git(seed, "init")
    _git(seed, "config", "user.name", "Delivery Test")
    _git(seed, "config", "user.email", "delivery@example.test")
    (seed / "README.md").write_text("base\n", encoding="utf-8")
    _git(seed, "add", "README.md")
    _git(seed, "commit", "-m", "base")
    _git(seed, "branch", "-M", "main")
    _git(seed, "remote", "add", "origin", str(remote))
    _git(seed, "push", "-u", "origin", "main")
    _git(remote, "symbolic-ref", "HEAD", "refs/heads/main")
    _git(tmp_path, "clone", str(remote), str(repo))
    return repo, remote


@pytest.mark.asyncio
async def test_prepare_is_idempotent_and_adopts_exact_workspace(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)

    first = await manager.prepare(
        repo_path=str(repo),
        run_id=17,
        branch="ccm/delivery/17-timeout",
        base_branch="main",
    )
    second = await manager.prepare(
        repo_path=str(repo),
        run_id=17,
        branch="ccm/delivery/17-timeout",
        base_branch="main",
    )

    assert second == first
    assert first.head_sha == first.base_sha
    assert Path(first.worktree_path).is_dir()
    assert (
        _git(Path(first.worktree_path), "symbolic-ref", "--short", "HEAD")
        == "ccm/delivery/17-timeout"
    )


@pytest.mark.asyncio
async def test_prepare_allows_safe_controller_worktree_config(tmp_path, monkeypatch):
    repo, _remote = _repository(tmp_path)
    _git(repo, "config", "core.hooksPath", str(repo / ".git" / "hooks"))
    managed_keys = tmp_path / ".ccm-task-git-credentials"
    managed_keys.mkdir()
    deploy_key = managed_keys / "repo-github"
    deploy_key.write_text("test-key\n", encoding="utf-8")
    deploy_key.chmod(0o600)
    monkeypatch.setattr(
        delivery_workspace,
        "_CCM_GIT_CREDENTIALS_DIR",
        managed_keys,
    )
    _git(
        repo,
        "config",
        "core.sshCommand",
        f"ssh -i {deploy_key} -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes",
    )
    worktree_config = repo / ".git" / "config.worktree"
    _git(repo, "config", "--file", str(worktree_config), "user.name", "Local User")

    result = await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
        repo_path=str(repo),
        run_id=176,
        branch="ccm/delivery/176-safe-worktree-config",
        base_branch="main",
    )

    assert Path(result.worktree_path).is_dir()


@pytest.mark.asyncio
async def test_prepare_rejects_unsafe_controller_worktree_config(tmp_path):
    repo, _remote = _repository(tmp_path)
    worktree_config = repo / ".git" / "config.worktree"
    _git(repo, "config", "--file", str(worktree_config), "core.hooksPath", "/tmp/hooks")

    with pytest.raises(DeliveryWorkspaceConflict, match="core.hookspath"):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=177,
            branch="ccm/delivery/177-unsafe-worktree-config",
            base_branch="main",
        )


@pytest.mark.asyncio
async def test_prepare_rejects_ssh_key_outside_ccm_managed_vault(tmp_path):
    repo, _remote = _repository(tmp_path)
    outside_key = tmp_path / "outside-key"
    outside_key.write_text("test-key\n", encoding="utf-8")
    outside_key.chmod(0o600)
    _git(
        repo,
        "config",
        "core.sshCommand",
        f"ssh -i {outside_key} -o IdentitiesOnly=yes",
    )

    with pytest.raises(DeliveryWorkspaceConflict, match="core.sshcommand"):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=178,
            branch="ccm/delivery/178-outside-key",
            base_branch="main",
        )


@pytest.mark.asyncio
async def test_production_manager_rejects_local_remote_without_explicit_test_flag(
    tmp_path,
):
    repo, _remote = _repository(tmp_path)

    with pytest.raises(DeliveryWorkspaceConflict, match="Local Delivery remotes"):
        await DeliveryWorkspaceManager().prepare(
            repo_path=str(repo),
            run_id=170,
            branch="ccm/delivery/170-no-local-production",
            base_branch="main",
        )


@pytest.mark.asyncio
async def test_prepare_fetches_from_validated_explicit_remote_url(
    tmp_path,
    monkeypatch,
):
    repo, remote = _repository(tmp_path)
    managed_keys = tmp_path / ".ccm-task-git-credentials"
    managed_keys.mkdir()
    deploy_key = managed_keys / "repo-github"
    deploy_key.write_text("test-key\n", encoding="utf-8")
    deploy_key.chmod(0o600)
    monkeypatch.setattr(
        delivery_workspace,
        "_CCM_GIT_CREDENTIALS_DIR",
        managed_keys,
    )
    ssh_command = f"ssh -i {deploy_key} -o IdentitiesOnly=yes"
    _git(repo, "config", "core.sshCommand", ssh_command)
    calls: list[tuple[tuple[str, ...], dict[str, str] | None]] = []
    original_git = delivery_workspace._git

    async def recording_git(cwd, args, **kwargs):
        calls.append((tuple(args), kwargs.get("env")))
        return await original_git(cwd, args, **kwargs)

    monkeypatch.setattr(delivery_workspace, "_git", recording_git)

    await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
        repo_path=str(repo),
        run_id=171,
        branch="ccm/delivery/171-explicit-remote",
        base_branch="main",
    )

    fetch, fetch_env = next(call for call in calls if "fetch" in call[0])
    assert str(remote) in fetch
    assert "origin" not in fetch
    assert fetch_env is not None
    assert fetch_env["GIT_SSH_COMMAND"] == shlex.join(shlex.split(ssh_command))


@pytest.mark.asyncio
async def test_prepare_never_executes_untrusted_remote_helper(tmp_path):
    repo, _remote = _repository(tmp_path)
    marker = tmp_path / "remote-helper-ran"
    helper = tmp_path / "remote-helper.sh"
    helper.write_text(
        f"#!/bin/sh\n: > {marker}\nexit 1\n",
        encoding="utf-8",
    )
    helper.chmod(0o700)
    _git(repo, "remote", "set-url", "origin", f"ext::{helper}")
    _git(repo, "config", "protocol.ext.allow", "always")

    with pytest.raises(DeliveryWorkspaceError):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=172,
            branch="ccm/delivery/172-untrusted-remote",
            base_branch="main",
        )

    assert not marker.exists()


@pytest.mark.asyncio
async def test_prepare_rejects_plain_http_github_remote(tmp_path):
    repo, _remote = _repository(tmp_path)
    _git(
        repo,
        "remote",
        "set-url",
        "origin",
        "http://github.com/acme/insecure-delivery.git",
    )

    with pytest.raises(DeliveryWorkspaceConflict, match="explicit GitHub"):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=174,
            branch="ccm/delivery/174-http-remote",
            base_branch="main",
        )


@pytest.mark.asyncio
async def test_prepare_rejects_github_origin_outside_frozen_repo_identity(tmp_path):
    repo, _remote = _repository(tmp_path)
    _git(
        repo,
        "remote",
        "set-url",
        "origin",
        "git@github.com:acme/actual-origin.git",
    )

    with pytest.raises(DeliveryWorkspaceConflict, match="monitored GitHub"):
        await DeliveryWorkspaceManager().prepare(
            repo_path=str(repo),
            run_id=175,
            branch="ccm/delivery/175-wrong-repo",
            base_branch="main",
            expected_repo_full_name="acme/frozen-repo",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config_key,config_value",
    [
        ("core.hooksPath", "COMMAND"),
        ("credential.helper", "!COMMAND"),
        ("core.sshCommand", "COMMAND"),
        ("filter.delivery.clean", "COMMAND"),
        ("filter.delivery.smudge", "COMMAND"),
        ("filter.delivery.process", "COMMAND"),
        ("diff.external", "COMMAND"),
        ("diff.delivery.command", "COMMAND"),
        ("diff.delivery.textconv", "COMMAND"),
        ("merge.delivery.driver", "COMMAND"),
        ("core.fsmonitor", "COMMAND"),
        ("core.alternateRefsCommand", "COMMAND"),
        ("gpg.program", "COMMAND"),
        ("push.gpgSign", "true"),
        ("http.proxy", "http://attacker.invalid:8080"),
        ("http.sslVerify", "false"),
        ("http.extraHeader", "Authorization: leaked"),
        ("https.proxy", "http://attacker.invalid:8080"),
        ("remote.origin.proxy", "http://attacker.invalid:8080"),
        ("remote.origin.uploadpack", "COMMAND"),
        ("include.path", "COMMAND"),
        ("url.https://attacker.invalid/.insteadOf", "https://github.com/"),
    ],
)
async def test_prepare_rejects_repo_local_external_command_configuration(
    tmp_path,
    config_key,
    config_value,
):
    repo, _remote = _repository(tmp_path)
    marker = tmp_path / "local-config-command-ran"
    command = tmp_path / "local-config-command.sh"
    command.write_text(
        f"#!/bin/sh\n: > {marker}\nexit 1\n",
        encoding="utf-8",
    )
    command.chmod(0o700)
    _git(repo, "config", config_key, config_value.replace("COMMAND", str(command)))

    with pytest.raises(DeliveryWorkspaceConflict, match="unsafe Git configuration"):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=173,
            branch="ccm/delivery/173-unsafe-config",
            base_branch="main",
        )

    assert not marker.exists()


@pytest.mark.asyncio
async def test_prepare_preserves_existing_branch_commits_on_recovery(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=18,
        branch="ccm/delivery/18-recovery",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    _git(workspace, "config", "user.name", "Delivery Test")
    _git(workspace, "config", "user.email", "delivery@example.test")
    (workspace / "fix.txt").write_text("fixed\n", encoding="utf-8")
    _git(workspace, "add", "fix.txt")
    _git(workspace, "commit", "-m", "fix")

    recovered = await manager.prepare(
        repo_path=str(repo),
        run_id=18,
        branch="ccm/delivery/18-recovery",
        base_branch="main",
    )

    assert recovered.base_sha == initial.base_sha
    assert recovered.head_sha != initial.head_sha
    assert recovered.head_sha == _git(workspace, "rev-parse", "HEAD")


@pytest.mark.asyncio
async def test_controller_commit_is_exact_and_crash_recoverable(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=25,
        branch="ccm/delivery/25-controller-commit",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    (workspace / "README.md").write_text("updated\n", encoding="utf-8")
    (workspace / "new.txt").write_text("new\n", encoding="utf-8")

    committed = await manager.commit_changes(
        repo_path=str(repo),
        worktree_path=str(workspace),
        branch=initial.branch,
        base_branch="main",
        expected_head_sha=initial.head_sha,
        run_id=25,
        turn_generation=3,
        title="Implement controlled delivery",
    )
    recovered = await manager.commit_changes(
        repo_path=str(repo),
        worktree_path=str(workspace),
        branch=initial.branch,
        base_branch="main",
        expected_head_sha=initial.head_sha,
        run_id=25,
        turn_generation=3,
        title="Implement controlled delivery",
    )

    assert recovered == committed
    assert committed.head_sha != initial.head_sha
    assert _git(workspace, "rev-parse", "HEAD^1") == initial.head_sha
    body = _git(workspace, "show", "-s", "--format=%B", "HEAD")
    assert "CCM-Delivery-Run: 25" in body
    assert "CCM-Delivery-Turn: 3" in body
    assert _git(workspace, "show", "-s", "--format=%an <%ae>", "HEAD") == (
        "CCM Delivery Controller <delivery@ccm.local>"
    )
    assert _git(workspace, "status", "--porcelain") == ""


@pytest.mark.asyncio
async def test_controller_commit_removes_only_empty_untracked_isolation_placeholders(
    tmp_path,
):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=251,
        branch="ccm/delivery/251-placeholders",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    for name in (".env", "package.json", "pnpm-lock.yaml", ".gitmodules"):
        (workspace / name).touch()
    (workspace / ".npmrc").write_text("registry=https://example.test\n")
    (workspace / "feature.txt").write_text("intended\n")

    committed = await manager.commit_changes(
        repo_path=str(repo),
        worktree_path=str(workspace),
        branch=initial.branch,
        base_branch="main",
        expected_head_sha=initial.head_sha,
        run_id=251,
        turn_generation=1,
        title="Keep intended output",
    )

    assert committed.head_sha != initial.head_sha
    assert _git(
        workspace, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"
    ).splitlines() == [
        ".npmrc",
        "feature.txt",
    ]
    assert (workspace / ".npmrc").read_text() == "registry=https://example.test\n"


@pytest.mark.asyncio
async def test_controller_commit_preserves_tracked_empty_placeholder_name(tmp_path):
    repo, _remote = _repository(tmp_path)
    (repo / ".env").touch()
    _git(repo, "add", ".env")
    _git(repo, "config", "user.name", "Delivery Test")
    _git(repo, "config", "user.email", "delivery@example.test")
    _git(repo, "commit", "-m", "track empty env")
    _git(repo, "push", "origin", "main")
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=252,
        branch="ccm/delivery/252-tracked-placeholder",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    (workspace / "feature.txt").write_text("intended\n")

    committed = await manager.commit_changes(
        repo_path=str(repo),
        worktree_path=str(workspace),
        branch=initial.branch,
        base_branch="main",
        expected_head_sha=initial.head_sha,
        run_id=252,
        turn_generation=1,
        title="Preserve tracked config",
    )

    assert committed.head_sha != initial.head_sha
    assert (workspace / ".env").is_file()
    assert _git(workspace, "ls-files", ".env") == ".env"


@pytest.mark.asyncio
async def test_controller_commit_returns_same_head_for_no_change_turn(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=26,
        branch="ccm/delivery/26-no-change",
        base_branch="main",
    )

    unchanged = await manager.commit_changes(
        repo_path=str(repo),
        worktree_path=initial.worktree_path,
        branch=initial.branch,
        base_branch="main",
        expected_head_sha=initial.head_sha,
        run_id=26,
        turn_generation=1,
        title="No-op cycle",
    )

    assert unchanged == initial


@pytest.mark.asyncio
async def test_controller_commit_rejects_tampered_git_pointer(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=27,
        branch="ccm/delivery/27-pointer",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    (workspace / "README.md").write_text("updated\n", encoding="utf-8")
    (workspace / ".git").write_text(
        f"gitdir: {repo / '.git'}\n",
        encoding="utf-8",
    )

    with pytest.raises(DeliveryWorkspaceConflict, match="pointer escaped"):
        await manager.commit_changes(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=initial.branch,
            base_branch="main",
            expected_head_sha=initial.head_sha,
            run_id=27,
            turn_generation=1,
            title="Tampered pointer",
        )


@pytest.mark.asyncio
async def test_controller_commit_rejects_external_clean_filter(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=28,
        branch="ccm/delivery/28-filter",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    _git(workspace, "config", "filter.delivery-evil.clean", "/bin/false")
    (workspace / ".gitattributes").write_text(
        "*.txt filter=delivery-evil\n",
        encoding="utf-8",
    )
    (workspace / "payload.txt").write_text("payload\n", encoding="utf-8")

    with pytest.raises(DeliveryWorkspaceConflict, match="external Git filters"):
        await manager.commit_changes(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=initial.branch,
            base_branch="main",
            expected_head_sha=initial.head_sha,
            run_id=28,
            turn_generation=1,
            title="Unsafe filter",
        )


@pytest.mark.asyncio
async def test_controller_commit_drops_command_scope_filter_environment(
    tmp_path,
    monkeypatch,
):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=29,
        branch="ccm/delivery/29-env-filter",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    (workspace / ".gitattributes").write_text(
        "payload.txt filter=delivery-env\n",
        encoding="utf-8",
    )
    (workspace / "payload.txt").write_text("payload\n", encoding="utf-8")
    # Command-scope config is not returned by ``git config --local``.  If the
    # Controller inherits these variables, ``git add`` executes /bin/false.
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "filter.delivery-env.clean")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "/bin/false")

    committed = await manager.commit_changes(
        repo_path=str(repo),
        worktree_path=str(workspace),
        branch=initial.branch,
        base_branch="main",
        expected_head_sha=initial.head_sha,
        run_id=29,
        turn_generation=1,
        title="Ignore inherited Git config",
    )

    assert committed.head_sha != initial.head_sha
    assert _git(workspace, "status", "--porcelain") == ""


@pytest.mark.asyncio
async def test_controller_commit_recovery_rejects_empty_marker_commit(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=30,
        branch="ccm/delivery/30-empty-recovery",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    _git(workspace, "config", "user.name", "Delivery Test")
    _git(workspace, "config", "user.email", "delivery@example.test")
    forged = _git(
        workspace,
        "commit-tree",
        _git(workspace, "rev-parse", "HEAD^{tree}"),
        "-p",
        initial.head_sha,
        "-m",
        "Forged empty recovery\n\nCCM-Delivery-Run: 30\nCCM-Delivery-Turn: 1",
    )
    _git(workspace, "reset", "--hard", forged)

    with pytest.raises(DeliveryWorkspaceConflict, match="outside the Controller"):
        await manager.commit_changes(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=initial.branch,
            base_branch="main",
            expected_head_sha=initial.head_sha,
            run_id=30,
            turn_generation=1,
            title="Forged empty recovery",
        )


@pytest.mark.asyncio
async def test_controller_commit_recovery_rejects_merge_marker_commit(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    initial = await manager.prepare(
        repo_path=str(repo),
        run_id=31,
        branch="ccm/delivery/31-merge-recovery",
        base_branch="main",
    )
    workspace = Path(initial.worktree_path)
    _git(workspace, "config", "user.name", "Delivery Test")
    _git(workspace, "config", "user.email", "delivery@example.test")
    unrelated = _git(
        workspace,
        "commit-tree",
        _git(workspace, "rev-parse", "HEAD^{tree}"),
        "-m",
        "unrelated root",
    )
    (workspace / "payload.txt").write_text("payload\n", encoding="utf-8")
    _git(workspace, "add", "payload.txt")
    forged = _git(
        workspace,
        "commit-tree",
        _git(workspace, "write-tree"),
        "-p",
        initial.head_sha,
        "-p",
        unrelated,
        "-m",
        "Forged merge recovery\n\nCCM-Delivery-Run: 31\nCCM-Delivery-Turn: 1",
    )
    _git(workspace, "reset", "--hard", forged)

    with pytest.raises(DeliveryWorkspaceConflict, match="outside the Controller"):
        await manager.commit_changes(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=initial.branch,
            base_branch="main",
            expected_head_sha=initial.head_sha,
            run_id=31,
            turn_generation=1,
            title="Forged merge recovery",
        )


@pytest.mark.asyncio
async def test_git_repeated_cancellation_waits_for_exact_process_reap(
    tmp_path,
    monkeypatch,
):
    spawned = asyncio.Event()
    wait_started = asyncio.Event()
    release_wait = asyncio.Event()
    stdout = asyncio.StreamReader()
    stderr = asyncio.StreamReader()
    process = MagicMock(pid=54_321, returncode=None, stdout=stdout, stderr=stderr)

    async def wait():
        wait_started.set()
        await release_wait.wait()
        process.returncode = -15
        return -15

    process.wait = AsyncMock(side_effect=wait)

    async def create_subprocess_exec(*args, **kwargs):
        del args, kwargs
        spawned.set()
        return process

    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)
    monkeypatch.setattr(
        delivery_workspace.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    command = asyncio.create_task(delivery_workspace._git(tmp_path, ["status"]))
    await spawned.wait()
    await asyncio.sleep(0)
    command.cancel()
    await wait_started.wait()
    command.cancel()
    await asyncio.sleep(0)

    assert not command.done()
    release_wait.set()
    with pytest.raises(asyncio.CancelledError):
        await command
    assert (54_321, delivery_workspace.signal.SIGTERM) in signals


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode", [0, 1])
async def test_git_reaps_process_group_descendants_after_parent_exit(
    tmp_path,
    monkeypatch,
    returncode,
):
    stdout = asyncio.StreamReader()
    stderr = asyncio.StreamReader()
    stdout.feed_eof()
    stderr.feed_eof()
    process = MagicMock(
        pid=54_322,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )
    process.wait = AsyncMock(return_value=returncode)

    async def create_subprocess_exec(*args, **kwargs):
        del args, kwargs
        return process

    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)
    monkeypatch.setattr(
        delivery_workspace.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    if returncode:
        with pytest.raises(DeliveryWorkspaceError, match="Git command failed"):
            await delivery_workspace._git(tmp_path, ["status"])
    else:
        await delivery_workspace._git(tmp_path, ["status"])

    assert (54_322, delivery_workspace.signal.SIGTERM) in signals
    assert (54_322, delivery_workspace.signal.SIGKILL) in signals


@pytest.mark.asyncio
async def test_prepare_rejects_branch_owned_by_another_worktree(tmp_path):
    repo, _remote = _repository(tmp_path)
    other = tmp_path / "other"
    branch = "ccm/delivery/19-conflict"
    _git(repo, "worktree", "add", "-b", branch, str(other), "origin/main")

    with pytest.raises(DeliveryWorkspaceConflict, match="another worktree"):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=19,
            branch=branch,
            base_branch="main",
        )


@pytest.mark.asyncio
async def test_prepare_recovers_branch_only_crash_at_exact_base(tmp_path):
    repo, _remote = _repository(tmp_path)
    branch = "ccm/delivery/191-branch-only"
    _git(repo, "branch", branch, "origin/main")

    recovered = await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
        repo_path=str(repo),
        run_id=191,
        branch=branch,
        base_branch="main",
    )

    assert recovered.head_sha == recovered.base_sha
    assert Path(recovered.worktree_path).is_dir()
    assert _git(Path(recovered.worktree_path), "branch", "--show-current") == branch


@pytest.mark.asyncio
async def test_prepare_rejects_branch_only_crash_at_wrong_base(tmp_path):
    repo, _remote = _repository(tmp_path)
    branch = "ccm/delivery/192-wrong-base"
    base = _git(repo, "rev-parse", "origin/main")
    wrong = _git(
        repo,
        "commit-tree",
        f"{base}^{{tree}}",
        "-p",
        base,
        "-m",
        "wrong base",
    )
    _git(repo, "update-ref", f"refs/heads/{branch}", wrong)

    with pytest.raises(DeliveryWorkspaceConflict, match="does not match.*base"):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=192,
            branch=branch,
            base_branch="main",
        )

    assert not (repo / ".claude-manager" / "worktrees" / "delivery-192").exists()


@pytest.mark.asyncio
async def test_prepare_recovers_registered_worktree_with_missing_path(tmp_path):
    repo, _remote = _repository(tmp_path)
    branch = "ccm/delivery/193-missing-path"
    workspace = repo / ".claude-manager" / "worktrees" / "delivery-193"
    workspace.parent.mkdir(parents=True)
    _git(repo, "worktree", "add", "-b", branch, str(workspace), "origin/main")
    shutil.rmtree(workspace)

    recovered = await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
        repo_path=str(repo),
        run_id=193,
        branch=branch,
        base_branch="main",
    )

    assert Path(recovered.worktree_path) == workspace
    assert recovered.head_sha == recovered.base_sha
    assert _git(workspace, "branch", "--show-current") == branch


@pytest.mark.asyncio
async def test_prepare_repairs_registered_worktree_missing_git_pointer(tmp_path):
    repo, _remote = _repository(tmp_path)
    branch = "ccm/delivery/194-missing-pointer"
    workspace = repo / ".claude-manager" / "worktrees" / "delivery-194"
    workspace.parent.mkdir(parents=True)
    _git(repo, "worktree", "add", "-b", branch, str(workspace), "origin/main")
    (workspace / ".git").unlink()

    recovered = await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
        repo_path=str(repo),
        run_id=194,
        branch=branch,
        base_branch="main",
    )

    assert recovered.head_sha == recovered.base_sha
    assert (workspace / ".git").is_file()


@pytest.mark.asyncio
async def test_prepare_rejects_symlinked_managed_ancestry(tmp_path):
    repo, _remote = _repository(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (repo / ".claude-manager").symlink_to(outside, target_is_directory=True)

    with pytest.raises(DeliveryWorkspaceConflict, match="ancestry"):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=20,
            branch="ccm/delivery/20-symlink",
            base_branch="main",
        )

    assert list(outside.iterdir()) == []


@pytest.mark.asyncio
async def test_prepare_requires_exact_remote_base(tmp_path):
    repo, _remote = _repository(tmp_path)

    with pytest.raises(DeliveryWorkspaceError, match="Git command failed"):
        await DeliveryWorkspaceManager(allow_local_remotes=True).prepare(
            repo_path=str(repo),
            run_id=21,
            branch="ccm/delivery/21-missing-base",
            base_branch="does-not-exist",
        )


@pytest.mark.asyncio
async def test_inspect_rejects_branch_switch(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    snapshot = await manager.prepare(
        repo_path=str(repo),
        run_id=22,
        branch="ccm/delivery/22-branch",
        base_branch="main",
    )
    workspace = Path(snapshot.worktree_path)
    _git(workspace, "checkout", "--detach", "HEAD")

    with pytest.raises(DeliveryWorkspaceError):
        await manager.inspect(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=snapshot.branch,
            base_branch="main",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("change_kind", ["tracked", "untracked"])
async def test_inspect_rejects_dirty_or_untracked_workspace(tmp_path, change_kind):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    snapshot = await manager.prepare(
        repo_path=str(repo),
        run_id=23,
        branch="ccm/delivery/23-dirty",
        base_branch="main",
    )
    workspace = Path(snapshot.worktree_path)
    if change_kind == "tracked":
        (workspace / "README.md").write_text("dirty\n", encoding="utf-8")
    else:
        (workspace / "untracked.txt").write_text("untracked\n", encoding="utf-8")

    with pytest.raises(
        DeliveryWorkspaceConflict,
        match="uncommitted or untracked changes",
    ):
        await manager.inspect(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=snapshot.branch,
            base_branch="main",
        )


@pytest.mark.asyncio
async def test_inspect_rejects_head_unrelated_to_frozen_base(tmp_path):
    repo, _remote = _repository(tmp_path)
    manager = DeliveryWorkspaceManager(allow_local_remotes=True)
    snapshot = await manager.prepare(
        repo_path=str(repo),
        run_id=24,
        branch="ccm/delivery/24-unrelated",
        base_branch="main",
    )
    workspace = Path(snapshot.worktree_path)
    _git(workspace, "config", "user.name", "Delivery Test")
    _git(workspace, "config", "user.email", "delivery@example.test")
    tree_sha = _git(workspace, "rev-parse", "HEAD^{tree}")
    unrelated_root = _git(
        workspace,
        "commit-tree",
        tree_sha,
        "-m",
        "unrelated root",
    )
    _git(workspace, "reset", "--hard", unrelated_root)

    with pytest.raises(
        DeliveryWorkspaceConflict,
        match="no longer descends from the frozen base",
    ):
        await manager.inspect(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=snapshot.branch,
            base_branch="main",
        )
