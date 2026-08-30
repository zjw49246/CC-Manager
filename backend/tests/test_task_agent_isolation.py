import json
import os
import shutil
import stat
import shlex
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.config import settings
from backend.services import task_agent_isolation as task_agent_isolation_module
from backend.services.task_agent_isolation import (
    CLAUDE_DELIVERY_BUILTIN_TOOLS,
    CLAUDE_MONITOR_BUILTIN_TOOLS,
    CLAUDE_SUBPROCESS_ENV_SCRUB,
    CLAUDE_SUB_AGENT_BUILTIN_TOOLS,
    CLAUDE_TASK_BUILTIN_TOOLS,
    CLAUDE_UNRESTRICTED_BUILTIN_TOOLS,
    CLAUDE_UNRESTRICTED_PERMISSION_TOOLS,
    TaskAgentIsolationError,
    claude_permission_allow_rules,
    discover_linked_worktree_git_read_boundary,
    generate_claude_aux_isolation_settings,
    generate_claude_delivery_isolation_settings,
    generate_claude_task_isolation_settings,
    generate_claude_unrestricted_task_settings,
    generate_claude_zero_tool_isolation_settings,
    prepare_task_working_directory,
    require_claude_apply_seccomp,
    require_task_security_boundary_configured,
    scrub_task_model_environment,
    task_model_tool_environment,
    validate_claude_delivery_isolation_settings,
    validate_claude_task_isolation_settings,
    validate_claude_unrestricted_task_settings,
)
from backend.services import trusted_runtime
from backend.services.trusted_runtime import (
    RUNNING_CCM_CHECKOUT,
    require_trusted_python_runtime,
    trusted_runtime_protected_roots,
    verify_materialized_trusted_python_asset,
)
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    _protected_path_variants,
    manager_secret_protected_paths,
)
from backend.services.mcp_config import (
    CCM_BROWSER_REVIEW_TOOLS,
    CCM_FRONTEND_REVIEW_TOOLS,
    CCM_MONITOR_AGENT_TOOLS,
    CCM_SKILLS_TOOLS,
    CCM_SSH_TOOLS,
    CCM_SUB_AGENT_TOOLS,
    CCM_WORKSPACE_REVIEW_TOOLS,
)


# Capture the production entrypoint during collection.  The InstanceManager
# suite intentionally stubs it, but must restore the module attribute before
# another test module runs; otherwise exact filesystem validation is silently
# bypassed and negative sandbox tests can produce false passes.
_COLLECTED_CLAUDE_TASK_ISOLATION_VALIDATOR = (
    task_agent_isolation_module.validate_claude_task_isolation_settings
)


def test_task_model_tool_environment_includes_standard_user_tool_directories():
    environment = task_model_tool_environment({"LANG": "C.UTF-8"})

    path_entries = environment["PATH"].split(os.pathsep)
    assert str(Path.home() / ".local" / "bin") in path_entries
    assert str(Path.home() / ".cargo" / "bin") in path_entries
    assert environment["HOME"] == str(Path.home())
    assert environment["LANG"] == "C.UTF-8"


def test_claude_task_isolation_validator_does_not_leak_between_modules():
    assert (
        task_agent_isolation_module.validate_claude_task_isolation_settings
        is _COLLECTED_CLAUDE_TASK_ISOLATION_VALIDATOR
    )


def _git(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )


def _linked_git_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git("init", "-q", cwd=repository)
    _git("config", "user.name", "CCM Test", cwd=repository)
    _git("config", "user.email", "ccm@example.invalid", cwd=repository)
    (repository / "tracked.txt").write_text("baseline\n", encoding="utf-8")
    _git("add", "tracked.txt", cwd=repository)
    _git("commit", "-qm", "baseline", cwd=repository)
    linked = tmp_path / "linked"
    _git(
        "worktree",
        "add",
        "-qb",
        "task-branch",
        str(linked),
        cwd=repository,
    )
    common = repository / ".git"
    return repository, linked, common


def _private_scratch(tmp_path: Path, name: str = "scratch") -> Path:
    tmp_path.chmod(0o700)
    scratch = tmp_path / name
    scratch.mkdir(mode=0o700)
    scratch.chmod(0o700)
    return scratch


def test_linked_worktree_git_boundary_is_exact_read_only_projection(tmp_path):
    repository, linked, common = _linked_git_fixture(tmp_path)
    other = tmp_path / "other"
    _git(
        "worktree",
        "add",
        "-qb",
        "other-branch",
        str(other),
        cwd=repository,
    )

    boundary = discover_linked_worktree_git_read_boundary(linked)

    assert boundary is not None
    git_dir = Path(boundary.git_dir)
    assert Path(boundary.common_dir) == common
    assert boundary.head_ref == "refs/heads/task-branch"
    assert set(boundary.read_paths) == {
        str(linked / ".git"),
        str(git_dir / "HEAD"),
        str(git_dir / "commondir"),
        str(git_dir / "index"),
        str(common / "objects"),
        str(common / "refs" / "heads" / "task-branch"),
    }
    forbidden = {
        common / "config",
        common / "hooks",
        common / "packed-refs",
        common / "refs" / "heads" / "other-branch",
    }
    assert all(str(path) not in boundary.read_paths for path in forbidden)
    assert str(common / "worktrees" / "other") not in boundary.read_paths


def test_normal_git_directory_has_no_linked_read_projection(tmp_path):
    repository, _linked, _common = _linked_git_fixture(tmp_path)

    assert discover_linked_worktree_git_read_boundary(repository) is None


def test_claude_delivery_policy_has_exact_networkless_git_projection(
    tmp_path,
    monkeypatch,
):
    _repository, linked, _common = _linked_git_fixture(tmp_path)
    runtime_root = tmp_path / "runtime"
    manager_secret = tmp_path / "manager-secret"
    scratch = _private_scratch(tmp_path)
    sibling_scratch = _private_scratch(tmp_path, "sibling-scratch")
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(runtime_root),
    )

    path, boundary = generate_claude_delivery_isolation_settings(
        91,
        [str(manager_secret)],
        working_directory=linked,
        private_tmpdir=scratch,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    filesystem = payload["sandbox"]["filesystem"]
    git_denies = {
        str(linked / ".git"),
        boundary.git_dir,
        boundary.common_dir,
    }

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.name == "claude-delivery-security.json"
    assert "hooks" not in payload
    assert payload["permissions"]["allow"] == list(
        CLAUDE_DELIVERY_BUILTIN_TOOLS
    )
    assert not any(
        tool.startswith("mcp__")
        for tool in payload["permissions"]["allow"]
    )
    assert "AskUserQuestion" not in CLAUDE_DELIVERY_BUILTIN_TOOLS
    assert "Agent" not in CLAUDE_DELIVERY_BUILTIN_TOOLS
    assert "Task" not in CLAUDE_DELIVERY_BUILTIN_TOOLS
    assert "Workflow" not in CLAUDE_DELIVERY_BUILTIN_TOOLS
    assert payload["sandbox"]["network"]["allowedDomains"] == []
    assert filesystem["denyRead"] == filesystem["denyWrite"]
    assert git_denies.issubset(filesystem["denyRead"])
    assert str(tmp_path) in filesystem["denyRead"]
    assert filesystem["allowRead"] == sorted(
        (*boundary.read_paths, str(scratch))
    )
    assert filesystem["allowWrite"] == sorted((str(linked), str(scratch)))
    assert str(sibling_scratch) not in filesystem["allowRead"]
    assert str(sibling_scratch) not in filesystem["allowWrite"]
    credential_denies = {
        entry["path"]
        for entry in payload["sandbox"]["credentials"]["files"]
    }
    assert git_denies.isdisjoint(credential_denies)
    assert str(manager_secret) in credential_denies
    for git_path in git_denies:
        permission_path = f"//{git_path.lstrip('/')}"
        assert f"Read({permission_path}/**)" in payload["permissions"]["deny"]
        assert f"Edit({permission_path}/**)" in payload["permissions"]["deny"]


def test_claude_delivery_policy_requires_linked_worktree(tmp_path, monkeypatch):
    repository, _linked, _common = _linked_git_fixture(tmp_path)
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    scratch = _private_scratch(tmp_path)

    with pytest.raises(TaskAgentIsolationError, match="linked-worktree"):
        generate_claude_delivery_isolation_settings(
            92,
            [str(tmp_path / "manager-secret")],
            working_directory=repository,
            private_tmpdir=scratch,
        )


def test_claude_delivery_validation_rechecks_git_identity_before_cli(
    tmp_path,
    monkeypatch,
):
    _repository, linked, _common = _linked_git_fixture(tmp_path)
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    scratch = _private_scratch(tmp_path)
    path, boundary = generate_claude_delivery_isolation_settings(
        93,
        [str(tmp_path / "manager-secret")],
        working_directory=linked,
        private_tmpdir=scratch,
    )
    branch_ref = Path(boundary.common_dir).joinpath(
        *(boundary.head_ref or "").split("/")
    )
    branch_ref.write_text("0" * 40 + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.subprocess.run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("Git identity failure must precede CLI")
        ),
    )

    with pytest.raises(TaskAgentIsolationError, match="changed before launch"):
        validate_claude_delivery_isolation_settings(
            path,
            claude_binary="claude",
            working_directory=linked,
            private_tmpdir=scratch,
            expected_git_boundary=boundary,
        )


def test_claude_delivery_validation_rejects_extra_git_read_override(
    tmp_path,
    monkeypatch,
):
    _repository, linked, _common = _linked_git_fixture(tmp_path)
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    scratch = _private_scratch(tmp_path)
    path, boundary = generate_claude_delivery_isolation_settings(
        94,
        [str(tmp_path / "manager-secret")],
        working_directory=linked,
        private_tmpdir=scratch,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["sandbox"]["filesystem"]["allowRead"].append("/etc/passwd")
    payload["sandbox"]["filesystem"]["allowRead"].sort()
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.subprocess.run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("settings failure must precede CLI")
        ),
    )

    with pytest.raises(TaskAgentIsolationError, match="filesystem paths"):
        validate_claude_delivery_isolation_settings(
            path,
            claude_binary="claude",
            working_directory=linked,
            private_tmpdir=scratch,
            expected_git_boundary=boundary,
        )


def test_linked_worktree_git_boundary_rejects_pointer_symlink(tmp_path):
    _repository, linked, _common = _linked_git_fixture(tmp_path)
    pointer = linked / ".git"
    saved = linked / ".git.saved"
    pointer.rename(saved)
    pointer.symlink_to(saved)

    with pytest.raises(TaskAgentIsolationError, match="safe regular file"):
        discover_linked_worktree_git_read_boundary(linked)


def test_linked_worktree_git_boundary_rejects_wrong_backlink(tmp_path):
    _repository, linked, _common = _linked_git_fixture(tmp_path)
    initial = discover_linked_worktree_git_read_boundary(linked)
    assert initial is not None
    (Path(initial.git_dir) / "gitdir").write_text(
        str(tmp_path / "other" / ".git"),
        encoding="utf-8",
    )

    with pytest.raises(TaskAgentIsolationError, match="backlink"):
        discover_linked_worktree_git_read_boundary(linked)


def test_linked_worktree_git_boundary_rejects_unsafe_head_ref(tmp_path):
    _repository, linked, _common = _linked_git_fixture(tmp_path)
    initial = discover_linked_worktree_git_read_boundary(linked)
    assert initial is not None
    (Path(initial.git_dir) / "HEAD").write_text(
        "ref: refs/heads/../escape\n",
        encoding="utf-8",
    )

    with pytest.raises(TaskAgentIsolationError, match="safe local branch"):
        discover_linked_worktree_git_read_boundary(linked)


def test_linked_worktree_git_boundary_rejects_packed_only_branch(tmp_path):
    repository, linked, _common = _linked_git_fixture(tmp_path)
    _git("pack-refs", "--all", "--prune", cwd=repository)

    with pytest.raises(TaskAgentIsolationError, match="exact loose ref"):
        discover_linked_worktree_git_read_boundary(linked)


def test_linked_worktree_git_boundary_rejects_symlink_index(tmp_path):
    _repository, linked, _common = _linked_git_fixture(tmp_path)
    initial = discover_linked_worktree_git_read_boundary(linked)
    assert initial is not None
    index = Path(initial.git_dir) / "index"
    saved = Path(initial.git_dir) / "index.saved"
    index.rename(saved)
    index.symlink_to(saved)

    with pytest.raises(TaskAgentIsolationError, match="index"):
        discover_linked_worktree_git_read_boundary(linked)


def _sandbox_loading_canary_result():
    return SimpleNamespace(
        returncode=1,
        stdout=json.dumps({
            "type": "result",
            "subtype": "error_during_execution",
            "is_error": True,
            "num_turns": 0,
            "total_cost_usd": 0,
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_read_input_tokens": 0,
            },
        }),
        stderr=(
            "Sandbox required but unavailable because "
            "sandbox.failIfUnavailable is enabled"
        ),
    )


def test_sandbox_loading_canary_accepts_claude_2_1_168_bwrap_error():
    from backend.services.task_agent_isolation import (
        _validate_sandbox_loading_canary,
    )

    _validate_sandbox_loading_canary(SimpleNamespace(
        returncode=1,
        stdout="",
        stderr=(
            "error: bubblewrap is required for subprocess env scrubbing and "
            "isolation. Install with: sudo apt-get install -y bubblewrap"
        ),
    ))


def test_protected_path_variants_expand_environment_variables(
    tmp_path,
    monkeypatch,
):
    credential_root = tmp_path / "credentials"
    monkeypatch.setenv("CCM_TEST_CREDENTIAL_ROOT", str(credential_root))

    variants = _protected_path_variants("$CCM_TEST_CREDENTIAL_ROOT/ssh")

    assert str(credential_root / "ssh") in variants


def test_task_process_env_scrubs_ambient_credentials_before_exact_git_overlay():
    parent = {
        "PATH": os.environ.get("PATH", ""),
        "AUTH_TOKEN": "deployment-secret",
        "CCM_INTERNAL_SERVICE_TOKEN": "internal-secret",
        "GH_TOKEN": "ambient-gh-secret",
        "GITHUB_TOKEN": "ambient-github-secret",
        "GIT_ASKPASS": "/ambient/askpass",
        "GIT_SSH_COMMAND": "ssh -i /ambient/key",
        "SSH_AUTH_SOCK": "/ambient/agent.sock",
        "GIT_AUTHOR_NAME": "Committer",
        "ANTHROPIC_API_KEY": "provider-parent-secret",
        "DATABASE_URL": "postgresql://manager-secret",
        "OPENAI_API_KEY": "whisper-secret",
        "FEISHU_APP_SECRET": "feishu-secret",
        "FEISHU_OAUTH_STATE_SECRET": "oauth-state-secret",
        "SMTP_PASSWORD": "mail-secret",
        "BACKUP_S3_ACCESS_KEY": "backup-access",
        "BACKUP_S3_SECRET_KEY": "backup-secret",
        "BACKUP_OSS_ACCESS_KEY": "oss-access",
        "BACKUP_OSS_SECRET_KEY": "oss-secret",
        "CCM_TEST_ARBITRARY_SECRET": "must-not-leak",
    }
    child_env = scrub_task_model_environment(parent, provider="claude")
    child_env.update({
        "GIT_ASKPASS": "/project/askpass",
        "GIT_SSH_COMMAND": "ssh -i /project/key",
        "GIT_AUTHOR_NAME": "Project Committer",
    })

    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json, os; "
                "print(json.dumps({"
                "'gh': 'GH_TOKEN' in os.environ, "
                "'github': 'GITHUB_TOKEN' in os.environ, "
                "'agent': 'SSH_AUTH_SOCK' in os.environ, "
                "'askpass': os.environ.get('GIT_ASKPASS'), "
                "'ssh': os.environ.get('GIT_SSH_COMMAND')}))"
            ),
        ],
        env=child_env,
        check=True,
        capture_output=True,
        text=True,
    )
    observed = json.loads(probe.stdout)

    assert observed == {
        "gh": False,
        "github": False,
        "agent": False,
        "askpass": "/project/askpass",
        "ssh": "ssh -i /project/key",
    }
    assert child_env["GIT_AUTHOR_NAME"] == "Project Committer"
    assert child_env["ANTHROPIC_API_KEY"] == "provider-parent-secret"
    assert child_env[CLAUDE_SUBPROCESS_ENV_SCRUB] == "1"
    for secret_name in (
        "DATABASE_URL",
        "OPENAI_API_KEY",
        "FEISHU_APP_SECRET",
        "FEISHU_OAUTH_STATE_SECRET",
        "SMTP_PASSWORD",
        "BACKUP_S3_ACCESS_KEY",
        "BACKUP_S3_SECRET_KEY",
        "BACKUP_OSS_ACCESS_KEY",
        "BACKUP_OSS_SECRET_KEY",
        "CCM_TEST_ARBITRARY_SECRET",
    ):
        assert secret_name not in child_env


def test_agent_workloads_require_nonempty_control_plane_auth(monkeypatch):
    monkeypatch.setattr(settings, "auth_token", "  ")
    with pytest.raises(TaskAgentIsolationError, match="AUTH_TOKEN"):
        require_task_security_boundary_configured()

    monkeypatch.setattr(settings, "auth_token", "deployment-secret")
    require_task_security_boundary_configured()


def test_trusted_runtime_protects_live_assets_but_not_complete_checkout():
    roots = {Path(value) for value in trusted_runtime_protected_roots()}
    checkout = Path(RUNNING_CCM_CHECKOUT)

    assert checkout not in roots
    assert (checkout / "backend").resolve() in roots
    assert Path(os.path.sep) not in roots


def test_system_python_prefix_is_not_a_global_task_deny(monkeypatch):
    monkeypatch.setattr(trusted_runtime.sys, "prefix", "/usr")
    monkeypatch.setattr(trusted_runtime.sys, "base_prefix", "/usr")

    roots = set(trusted_runtime._startup_protected_roots())

    assert "/usr" not in roots


def test_writable_system_python_fails_child_admission_not_manager_import(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(trusted_runtime.sys, "prefix", str(tmp_path))
    monkeypatch.setattr(trusted_runtime.sys, "base_prefix", str(tmp_path))
    monkeypatch.setattr(trusted_runtime.os, "access", lambda *_args: True)

    with pytest.raises(
        trusted_runtime.TrustedRuntimeError,
        match="writable non-venv",
    ):
        require_trusted_python_runtime()


def _fake_running_checkout(tmp_path, monkeypatch):
    live = tmp_path / "live-ccm"
    backend = live / "backend"
    backend.mkdir(parents=True)
    managed = live / ".claude-manager" / "worktrees" / "task-worktree"
    managed.mkdir(parents=True)
    workspaces = tmp_path / "workspaces"
    workspaces.mkdir()
    monkeypatch.setattr(settings, "workspace_dir", str(workspaces))
    monkeypatch.setattr(trusted_runtime, "RUNNING_CCM_CHECKOUT", str(live))
    monkeypatch.setattr(
        trusted_runtime,
        "TRUSTED_RUNTIME_PROTECTED_ROOTS",
        (str(backend),),
    )
    return live, managed, workspaces


def test_working_directory_rejects_live_checkout_but_allows_managed_worktree(
    tmp_path,
    monkeypatch,
):
    live, managed, _workspaces = _fake_running_checkout(tmp_path, monkeypatch)
    incarnation = "a" * 32

    with pytest.raises(TaskAgentIsolationError, match="overlaps"):
        prepare_task_working_directory(
            7,
            incarnation,
            str(live),
            has_explicit_workspace=True,
        )

    assert prepare_task_working_directory(
        7,
        incarnation,
        str(managed),
        has_explicit_workspace=True,
    ) == str(managed)


def test_projectless_workspace_is_incarnation_scoped_and_resumable(
    tmp_path,
    monkeypatch,
):
    live, _managed, workspaces = _fake_running_checkout(tmp_path, monkeypatch)
    first_incarnation = "1" * 32
    second_incarnation = "2" * 32

    first = Path(prepare_task_working_directory(
        9,
        first_incarnation,
        str(live),
        has_explicit_workspace=False,
    ))
    assert first == (
        workspaces
        / ".ccm-task-workspaces"
        / f"task-9-{first_incarnation}"
    )
    assert stat.S_IMODE(first.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(first.stat().st_mode) == 0o700
    (first / "old-incarnation.txt").write_text("old", encoding="utf-8")

    assert Path(prepare_task_working_directory(
        9,
        first_incarnation,
        str(first),
        has_explicit_workspace=False,
    )) == first
    second = Path(prepare_task_working_directory(
        9,
        second_incarnation,
        str(first),
        has_explicit_workspace=False,
    ))
    assert second != first
    assert not (second / "old-incarnation.txt").exists()


def test_projectless_workspace_rejects_symlink_leaf(tmp_path, monkeypatch):
    live, _managed, workspaces = _fake_running_checkout(tmp_path, monkeypatch)
    incarnation = "3" * 32
    private_root = workspaces / ".ccm-task-workspaces"
    private_root.mkdir(mode=0o700)
    target = tmp_path / "attacker-target"
    target.mkdir()
    expected = private_root / f"task-11-{incarnation}"
    expected.symlink_to(target, target_is_directory=True)

    with pytest.raises(TaskAgentIsolationError, match="safe directory"):
        prepare_task_working_directory(
            11,
            incarnation,
            str(expected),
            has_explicit_workspace=False,
        )

    assert not (target / "CLAUDE.md").exists()


@pytest.mark.parametrize("incarnation", ("", "A" * 32, "f" * 31))
def test_working_directory_requires_valid_task_incarnation(
    tmp_path,
    incarnation,
):
    with pytest.raises(TaskAgentIsolationError, match="incarnation"):
        prepare_task_working_directory(
            12,
            incarnation,
            str(tmp_path),
            has_explicit_workspace=True,
        )


def test_claude_zero_tool_policy_has_no_tool_mcp_hook_or_network_authority(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_zero_tool_isolation_settings(
        "goal",
        17,
        [str(tmp_path / "manager-secrets")],
    )
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["permissions"]["allow"] == []
    assert "hooks" not in payload
    assert payload["sandbox"]["network"]["allowedDomains"] == []
    assert str(tmp_path / "manager-secrets") in payload["sandbox"][
        "filesystem"
    ]["denyRead"]


def test_manager_secret_paths_cover_custom_pool_homes_and_backup_roots(
    tmp_path,
    monkeypatch,
):
    claude_config = tmp_path / "claude-inventory" / "accounts.json"
    codex_config = tmp_path / "codex-inventory" / "accounts.json"
    claude_config.parent.mkdir()
    codex_config.parent.mkdir()
    claude_home = tmp_path / "custom-claude-home"
    disabled_claude_home = tmp_path / "disabled-claude-home"
    codex_home = tmp_path / "custom-codex-home"
    retired_codex_home = tmp_path / "retired-codex-home"
    claude_config.write_text(json.dumps({"accounts": [
        {"id": "a", "config_dir": str(claude_home), "enabled": True},
        {
            "id": "b",
            "config_dir": str(disabled_claude_home),
            "enabled": False,
            "retired": True,
        },
    ]}), encoding="utf-8")
    codex_config.write_text(json.dumps({"accounts": [
        {"id": "a", "codex_home": str(codex_home), "enabled": True},
        {
            "id": "b",
            "codex_home": str(retired_codex_home),
            "enabled": False,
            "retired": True,
        },
    ]}), encoding="utf-8")
    backup_temp = tmp_path / "backup-temp"
    backup_destination = tmp_path / "backup-destination"
    monkeypatch.setattr(settings, "pool_config_path", str(claude_config))
    monkeypatch.setattr(settings, "codex_pool_config_path", str(codex_config))
    monkeypatch.setattr(settings, "backup_temp_dir", str(backup_temp))
    monkeypatch.setattr(
        settings,
        "backup_destination_path",
        str(backup_destination),
    )

    protected = set(manager_secret_protected_paths())

    assert str(claude_config.parent) in protected
    assert str(codex_config.parent) in protected
    assert str(claude_home) in protected
    assert str(disabled_claude_home) in protected
    assert str(codex_home) in protected
    assert str(retired_codex_home) in protected
    assert str(backup_temp) in protected
    assert str(backup_destination) in protected


def test_manager_secret_paths_fail_closed_on_one_bad_pool_record(
    tmp_path,
    monkeypatch,
):
    claude_config = tmp_path / "claude-pool" / "accounts.json"
    claude_config.parent.mkdir()
    claude_config.write_text(json.dumps({"accounts": [
        {"id": "valid", "config_dir": str(tmp_path / "valid-home")},
        {"id": "broken", "config_dir": "relative/home"},
        {"id": "later", "config_dir": str(tmp_path / "later-home")},
    ]}), encoding="utf-8")
    codex_config = tmp_path / "codex-pool" / "accounts.json"
    codex_config.parent.mkdir()
    codex_config.write_text('{"accounts": []}', encoding="utf-8")
    monkeypatch.setattr(settings, "pool_config_path", str(claude_config))
    monkeypatch.setattr(settings, "codex_pool_config_path", str(codex_config))

    with pytest.raises(TaskSSHAccessError, match="inventory is invalid"):
        manager_secret_protected_paths()


@pytest.mark.parametrize(
    ("namespace", "identifier", "generation"),
    [("monitor", 8, 3), ("sub-agent", 9, None)],
)
def test_claude_aux_isolation_has_no_main_task_hooks_and_can_close_network(
    tmp_path,
    monkeypatch,
    namespace,
    identifier,
    generation,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_aux_isolation_settings(
        namespace=namespace,
        identifier=identifier,
        protected_paths=["/Users/operator/.ssh"],
        turn_generation=generation,
        disable_direct_network=True,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert "hooks" not in payload
    assert payload["sandbox"]["network"]["allowedDomains"] == []
    assert "/Users/operator/.ssh" in payload["sandbox"]["filesystem"][
        "denyRead"
    ]
    if generation is not None:
        assert path.name == f"claude-security-{generation}.json"
    assert set(CLAUDE_MONITOR_BUILTIN_TOOLS) == {"Bash", "Glob", "Grep", "Read"}
    assert "Agent" not in CLAUDE_SUB_AGENT_BUILTIN_TOOLS


def test_claude_task_isolation_denies_credentials_and_direct_ssh_network(
    tmp_path,
    monkeypatch,
):
    runtime_root = tmp_path / "runtime"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(runtime_root))
    monkeypatch.setattr(settings, "ask_user_enabled", True)

    path = generate_claude_task_isolation_settings(
        31,
        ["/Users/operator/.ssh", "/private/ccm/profile.pem"],
        ssh_capabilities={"read"},
    )
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert payload["permissions"]["defaultMode"] == "acceptEdits"
    assert payload["permissions"]["disableBypassPermissionsMode"] == "disable"
    assert "Read(//Users/operator/.ssh/**)" in payload["permissions"]["deny"]
    assert payload["sandbox"]["failIfUnavailable"] is True
    assert payload["sandbox"]["allowUnsandboxedCommands"] is False
    assert payload["sandbox"]["network"] == {
        "strictAllowlist": True,
        "allowedDomains": [],
        "deniedDomains": [],
        "allowAllUnixSockets": False,
        "allowLocalBinding": False,
    }
    assert "/Users/operator/.ssh" in payload["sandbox"]["filesystem"][
        "denyRead"
    ]
    assert str(runtime_root) in payload["sandbox"]["filesystem"]["denyRead"]
    commands = [
        hook["command"]
        for entry in payload["hooks"]["PreToolUse"]
        for hook in entry["hooks"]
    ]
    script_paths = {Path(shlex.split(command)[1]) for command in commands}
    ask_user_script = next(
        path for path in script_paths if path.name.startswith("ask-user-hook-")
    )
    ssh_guard_script = next(
        path
        for path in script_paths
        if path.name.startswith("task-ssh-guard-hook-")
    )
    verify_materialized_trusted_python_asset(
        "ask_user_hook",
        ask_user_script,
    )
    verify_materialized_trusted_python_asset(
        "task_ssh_guard_hook",
        ssh_guard_script,
    )
    assert all("AUTH_TOKEN" not in command for command in commands)
    assert all("--auth-token" not in command for command in commands)


@pytest.mark.parametrize("stale_leaf_kind", ["missing", "directory"])
def test_claude_isolation_collapses_stale_key_below_denied_managed_root(
    tmp_path,
    monkeypatch,
    stale_leaf_kind,
):
    """A stale SSH Profile must not prevent the Bash sandbox from starting."""

    runtime_root = tmp_path / "runtime"
    managed_root = tmp_path / ".ccm" / "ssh-keys" / "managed"
    managed_root.mkdir(parents=True)
    missing_key = managed_root / "165ded4541cf2299dedaaa094a90d9de"
    if stale_leaf_kind == "directory":
        missing_key.mkdir()
    git_parent = tmp_path / ".ssh"
    git_parent.mkdir()
    allowed_git_key = git_parent / "project-git-key"
    allowed_git_key.write_text("private", encoding="utf-8")
    allowed_git_key.chmod(0o600)
    sibling_prefix = tmp_path / ".ccm" / "ssh-keys" / "managed-backup"
    assert missing_key.exists() is (stale_leaf_kind == "directory")
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(runtime_root))

    path = generate_claude_task_isolation_settings(
        32,
        [
            str(managed_root),
            str(missing_key),
            str(git_parent),
            str(allowed_git_key),
            str(sibling_prefix),
        ],
        allowed_read_paths=[str(allowed_git_key)],
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    filesystem = payload["sandbox"]["filesystem"]
    credentials = {
        entry["path"]
        for entry in payload["sandbox"]["credentials"]["files"]
    }

    assert str(managed_root) in filesystem["denyRead"]
    assert str(managed_root) in filesystem["denyWrite"]
    assert str(managed_root) in credentials
    assert str(missing_key) not in filesystem["denyRead"]
    assert str(missing_key) not in filesystem["denyWrite"]
    assert str(missing_key) not in credentials
    assert str(allowed_git_key) not in filesystem["denyRead"]
    assert str(allowed_git_key) in filesystem["allowRead"]
    assert str(sibling_prefix) in filesystem["denyRead"]
    assert payload["sandbox"]["autoAllowBashIfSandboxed"] is True


def test_claude_isolation_keeps_required_runtime_root_under_denied_parent(
    tmp_path,
    monkeypatch,
):
    """The default ~/.ccm layout must still satisfy the exact preflight."""

    ccm_root = tmp_path / ".ccm"
    runtime_root = ccm_root / "task-runtime-secrets"
    managed_root = ccm_root / "ssh-keys" / "managed"
    managed_root.mkdir(parents=True)
    missing_key = managed_root / "stale-profile-key"
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(runtime_root),
    )

    path = generate_claude_task_isolation_settings(
        33,
        [str(ccm_root), str(managed_root), str(missing_key)],
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    filesystem = payload["sandbox"]["filesystem"]

    assert str(ccm_root) in filesystem["denyRead"]
    assert str(runtime_root) in filesystem["denyRead"]
    assert str(managed_root) not in filesystem["denyRead"]
    assert str(missing_key) not in filesystem["denyRead"]


@pytest.mark.parametrize("stale_leaf_kind", ["missing", "directory"])
def test_claude_isolation_stale_key_projection_starts_bubblewrap(
    tmp_path,
    monkeypatch,
    stale_leaf_kind,
):
    """Exercise the Linux mount ordering that previously broke every Bash."""

    bwrap = shutil.which("bwrap")
    bash_binary = shutil.which("bash")
    if bwrap is None or bash_binary is None:
        pytest.skip("bubblewrap integration probe is unavailable")

    bash_exit = [bash_binary, "--noprofile", "--norc", "-c", "exit 0"]

    preflight = subprocess.run(
        [bwrap, "--ro-bind", "/", "/", "--", *bash_exit],
        check=False,
        capture_output=True,
        text=True,
    )
    if preflight.returncode != 0:
        pytest.skip(f"bubblewrap namespaces are unavailable: {preflight.stderr}")

    runtime_root = tmp_path / "runtime"
    managed_root = tmp_path / ".ccm" / "ssh-keys" / "managed"
    managed_root.mkdir(parents=True)
    missing_key = managed_root / "stale-profile-key"
    if stale_leaf_kind == "directory":
        missing_key.mkdir()
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(runtime_root))

    settings_path = generate_claude_task_isolation_settings(
        34,
        [str(managed_root), str(missing_key)],
    )
    filesystem = json.loads(
        settings_path.read_text(encoding="utf-8")
    )["sandbox"]["filesystem"]

    command = [bwrap, "--ro-bind", "/", "/"]
    for raw_path in filesystem["denyRead"]:
        path = Path(raw_path)
        try:
            path.relative_to(tmp_path)
        except ValueError:
            continue
        command.extend(("--ro-bind", str(path), str(path)))
    command.extend(("--", *bash_exit))

    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_claude_task_isolation_allows_every_injected_task_mcp_tool(
    tmp_path,
    monkeypatch,
):
    """Scrub forces default mode, so injected MCP tools need exact rules."""

    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_task_isolation_settings(32, [])
    payload = json.loads(path.read_text(encoding="utf-8"))
    allowed = set(payload["permissions"]["allow"])

    expected_servers = {
        "ccm_skills": CCM_SKILLS_TOOLS,
        "ccm_ssh": CCM_SSH_TOOLS,
        "ccm_frontend_review": CCM_FRONTEND_REVIEW_TOOLS,
        "ccm_workspace_review": CCM_WORKSPACE_REVIEW_TOOLS,
        "ccm_browser_review": CCM_BROWSER_REVIEW_TOOLS,
        "ccm_monitor_agent": CCM_MONITOR_AGENT_TOOLS,
        "ccm_sub_agent": CCM_SUB_AGENT_TOOLS,
    }
    expected_mcp = {
        f"mcp__{server}__{tool}"
        for server, tools in expected_servers.items()
        for tool in tools
    }
    assert allowed == set(CLAUDE_TASK_BUILTIN_TOOLS) | expected_mcp


def test_claude_unrestricted_task_settings_are_private_exact_and_keep_ask_user(
    tmp_path,
    monkeypatch,
):
    runtime_root = tmp_path / "runtime"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(runtime_root))
    monkeypatch.setattr(settings, "ask_user_enabled", True)

    path = generate_claude_unrestricted_task_settings(
        121,
        turn_generation=7,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert path.name == "claude-unrestricted-security.json"
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode)
    assert info.st_nlink == 1
    assert stat.S_IMODE(info.st_mode) == 0o600
    assert "sandbox" not in payload
    assert payload["permissions"] == {
        "defaultMode": "bypassPermissions",
        "allow": list(
            claude_permission_allow_rules(
                CLAUDE_UNRESTRICTED_PERMISSION_TOOLS
            )
        ),
        "deny": [],
    }
    assert payload["permissions"]["allow"][:len(
        CLAUDE_UNRESTRICTED_PERMISSION_TOOLS
    )] == list(CLAUDE_UNRESTRICTED_PERMISSION_TOOLS)
    assert all(
        not rule.startswith("mcp__")
        for rule in CLAUDE_UNRESTRICTED_PERMISSION_TOOLS
    )
    ask_user_command = payload["hooks"]["PreToolUse"][0]["hooks"][0][
        "command"
    ]
    ask_user_script = Path(shlex.split(ask_user_command)[1])
    verify_materialized_trusted_python_asset("ask_user_hook", ask_user_script)
    assert ask_user_script.parent == path.parent

    validate_claude_unrestricted_task_settings(path)


def test_claude_unrestricted_task_settings_validator_rejects_sandbox_field(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    monkeypatch.setattr(settings, "ask_user_enabled", False)
    path = generate_claude_unrestricted_task_settings(
        122,
        turn_generation=0,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["sandbox"] = {"enabled": False}
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)

    with pytest.raises(TaskAgentIsolationError, match="unexpected shape"):
        validate_claude_unrestricted_task_settings(path)


def test_claude_task_isolation_keeps_general_network_without_ssh_grant(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_task_isolation_settings(
        32,
        ["/Users/operator/.ssh"],
        ssh_capabilities=(),
    )

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["sandbox"]["network"]["allowedDomains"] == ["*"]
    assert not any(
        "task_ssh_guard_hook.py" in hook["command"]
        for entry in payload.get("hooks", {}).get("PreToolUse", [])
        for hook in entry["hooks"]
    )


def test_claude_task_isolation_rejects_root_as_protected_path(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )

    with pytest.raises(TaskAgentIsolationError, match="filesystem root"):
        generate_claude_task_isolation_settings(33, ["/"])


def test_claude_isolation_preflight_scrubs_manager_tokens(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_task_isolation_settings(
        34,
        ["/Users/operator/.ssh"],
    )
    monkeypatch.setenv("AUTH_TOKEN", "deployment-secret")
    monkeypatch.setenv("CCM_INTERNAL_SERVICE_TOKEN", "internal-secret")
    monkeypatch.setenv("CCM_ASK_USER_TOKEN", "task-secret")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "model-secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "cloud-secret")
    captured = []

    binaries = {
        "claude": "/opt/claude/bin/claude",
        "bwrap": "/usr/bin/bwrap",
        "socat": "/usr/bin/socat",
    }
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.shutil.which",
        lambda name: binaries.get(name),
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.require_claude_apply_seccomp",
        lambda _binary: Path("/opt/sandbox-runtime/x64/apply-seccomp"),
    )

    def fake_run(argv, **kwargs):
        captured.append({"argv": argv, "env": kwargs["env"]})
        if len(captured) == 1:
            return _sandbox_loading_canary_result()
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps({
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "num_turns": 0,
                "total_cost_usd": 0,
                "usage": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_read_input_tokens": 0,
                },
            }),
            stderr="",
        )

    monkeypatch.setattr(
        "backend.services.task_agent_isolation.subprocess.run",
        fake_run,
    )

    validate_claude_task_isolation_settings(path, claude_binary="claude")

    assert len(captured) == 2
    assert captured[0]["argv"] == captured[1]["argv"] == [
        "/usr/bin/bwrap",
        "--unshare-net",
        "--die-with-parent",
        "--ro-bind",
        "/",
        "/",
        "--dev-bind",
        "/dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--ro-bind",
        str(path),
        "/tmp/ccm-claude-isolation-settings.json",
        "--chdir",
        "/",
        "--",
        "/opt/claude/bin/claude",
        "--bare",
        "--settings",
        "/tmp/ccm-claude-isolation-settings.json",
        "--setting-sources",
        "",
        "--strict-mcp-config",
        "--permission-mode",
        "acceptEdits",
        "--disable-slash-commands",
        "--no-chrome",
        "--tools",
        ",".join(CLAUDE_TASK_BUILTIN_TOOLS),
        "--allowedTools",
        ",".join(CLAUDE_TASK_BUILTIN_TOOLS),
        "--no-session-persistence",
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    assert captured[0]["env"]["PATH"] == "/nonexistent/ccm-claude-sandbox-canary"
    for invocation in captured:
        assert "AUTH_TOKEN" not in invocation["env"]
        assert "CCM_INTERNAL_SERVICE_TOKEN" not in invocation["env"]
        assert "CCM_ASK_USER_TOKEN" not in invocation["env"]
        assert "ANTHROPIC_API_KEY" not in invocation["env"]
        assert "AWS_ACCESS_KEY_ID" not in invocation["env"]
        assert invocation["env"][CLAUDE_SUBPROCESS_ENV_SCRUB] == "1"


def test_claude_isolation_preflight_rejects_weakened_local_contract(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_task_isolation_settings(
        35,
        ["/Users/operator/.ssh"],
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["sandbox"]["failIfUnavailable"] = False
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.subprocess.run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("local contract failure must precede CLI")
        ),
    )

    with pytest.raises(TaskAgentIsolationError, match="fail-closed"):
        validate_claude_task_isolation_settings(path, claude_binary="claude")


def test_claude_isolation_preflight_requires_sandbox_dependencies(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_aux_isolation_settings(
        namespace="monitor",
        identifier=36,
        protected_paths=["/Users/operator/.ssh"],
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.shutil.which",
        lambda name: None if name == "socat" else f"/usr/bin/{name}",
    )

    with pytest.raises(TaskAgentIsolationError, match="bubblewrap, and socat"):
        validate_claude_task_isolation_settings(
            path,
            claude_binary="claude",
            tools=CLAUDE_MONITOR_BUILTIN_TOOLS,
        )


def test_claude_isolation_preflight_requires_apply_seccomp(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_aux_isolation_settings(
        namespace="monitor",
        identifier=39,
        protected_paths=["/Users/operator/.ssh"],
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.require_claude_apply_seccomp",
        lambda _binary: (_ for _ in ()).throw(
            TaskAgentIsolationError("matching apply-seccomp helper is missing")
        ),
    )

    with pytest.raises(TaskAgentIsolationError, match="apply-seccomp"):
        validate_claude_task_isolation_settings(
            path,
            claude_binary="claude",
            tools=CLAUDE_MONITOR_BUILTIN_TOOLS,
        )


def test_apply_seccomp_resolution_uses_matching_global_architecture(
    tmp_path,
    monkeypatch,
):
    prefix = tmp_path / "npm-prefix"
    helper = (
        prefix
        / "lib/node_modules/@anthropic-ai/sandbox-runtime"
        / "vendor/seccomp/x64/apply-seccomp"
    )
    helper.parent.mkdir(parents=True)
    helper.write_bytes(b"official helper fixture")
    helper.chmod(0o755)
    monkeypatch.setenv("NPM_CONFIG_PREFIX", str(prefix))
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.platform.system",
        lambda: "Linux",
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.platform.machine",
        lambda: "x86_64",
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.shutil.which",
        lambda name: "/opt/claude" if name == "claude" else None,
    )

    assert require_claude_apply_seccomp("claude") == helper.resolve()


def test_claude_isolation_preflight_accepts_real_cli_empty_zero_turn_shape(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_aux_isolation_settings(
        namespace="monitor",
        identifier=38,
        protected_paths=["/Users/operator/.ssh"],
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.require_claude_apply_seccomp",
        lambda _binary: Path("/opt/sandbox-runtime/x64/apply-seccomp"),
    )
    results = iter((
        _sandbox_loading_canary_result(),
        SimpleNamespace(returncode=0, stdout="", stderr=""),
    ))
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.subprocess.run",
        lambda *_args, **_kwargs: next(results),
    )

    validate_claude_task_isolation_settings(
        path,
        claude_binary="claude",
        tools=CLAUDE_MONITOR_BUILTIN_TOOLS,
    )


@pytest.mark.parametrize(
    "result_event",
    [
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "num_turns": 1,
            "total_cost_usd": 0,
            "usage": {"input_tokens": 0},
        },
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "num_turns": 0,
            "total_cost_usd": 0.01,
            "usage": {"input_tokens": 1},
        },
    ],
)
def test_claude_isolation_preflight_rejects_model_execution(
    tmp_path,
    monkeypatch,
    result_event,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_aux_isolation_settings(
        namespace="sub-agent",
        identifier=37,
        protected_paths=["/Users/operator/.ssh"],
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.require_claude_apply_seccomp",
        lambda _binary: Path("/opt/sandbox-runtime/x64/apply-seccomp"),
    )
    results = iter((
        _sandbox_loading_canary_result(),
        SimpleNamespace(
            returncode=0,
            stdout=json.dumps(result_event),
            stderr="",
        ),
    ))
    monkeypatch.setattr(
        "backend.services.task_agent_isolation.subprocess.run",
        lambda *_args, **_kwargs: next(results),
    )

    with pytest.raises(TaskAgentIsolationError, match="model turn"):
        validate_claude_task_isolation_settings(
            path,
            claude_binary="claude",
            tools=CLAUDE_SUB_AGENT_BUILTIN_TOOLS,
        )


def test_task_claude_wrapper_is_private_and_uses_exact_cli_boundary():
    wrapper = Path(__file__).resolve().parents[1] / "services" / "task_claude_wrapper.sh"

    text = wrapper.read_text(encoding="utf-8")

    assert os.access(wrapper, os.X_OK)
    assert stat.S_IMODE(wrapper.stat().st_mode) & 0o022 == 0
    assert "--setting-sources \"\"" in text
    assert "--strict-mcp-config" in text
    assert "--permission-mode acceptEdits" in text
    assert "--dangerously-skip-permissions" not in text
    assert text.count("--disable-slash-commands") == 3
    assert set(CLAUDE_TASK_BUILTIN_TOOLS) == {
        "AskUserQuestion",
        "Bash",
        "Edit",
        "Glob",
        "Grep",
        "MultiEdit",
        "NotebookEdit",
        "Read",
        "Write",
    }
    prepare_task_working_directory,
