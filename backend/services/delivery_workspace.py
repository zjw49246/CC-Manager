"""Idempotent, non-destructive Git workspace ownership for Delivery Runs."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import signal
import stat
from typing import Iterable
from urllib.parse import urlsplit

from backend.services.cancellation import await_task_completion


class DeliveryWorkspaceError(RuntimeError):
    pass


class DeliveryWorkspaceConflict(DeliveryWorkspaceError):
    pass


_SHA_RE = re.compile(r"[0-9a-f]{40,64}\Z")
_BRANCH_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,199}\Z")
_MAX_GIT_OUTPUT = 2 * 1024 * 1024
_MAX_GITDIR_POINTER_BYTES = 4096
_MAX_GIT_CONFIG_BYTES = 1024 * 1024
_CCM_GIT_CREDENTIALS_DIR = Path.home() / ".ccm-task-git-credentials"
_GITHUB_REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
_SCP_GITHUB_RE = re.compile(
    r"(?:[A-Za-z0-9_.-]+@)?github\.com:"
    r"(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?/?\Z",
    re.IGNORECASE,
)
_SAFE_GIT_CONFIG = (
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
    "-c",
    "core.sshCommand=ssh -F /dev/null",
    "-c",
    "ssh.variant=ssh",
    "-c",
    "core.gitProxy=",
    "-c",
    "core.askPass=/bin/false",
    "-c",
    "commit.gpgSign=false",
    "-c",
    "tag.gpgSign=false",
    "-c",
    "push.gpgSign=false",
    "-c",
    "credential.helper=",
    "-c",
    "credential.interactive=false",
    "-c",
    "diff.external=",
    "-c",
    "interactive.diffFilter=",
    "-c",
    "gc.auto=0",
    "-c",
    "maintenance.auto=false",
    "-c",
    "protocol.ext.allow=never",
    "-c",
    "protocol.file.allow=always",
)


@dataclass(frozen=True, slots=True)
class _ValidatedGitRepository:
    common_git_dir: Path
    fetch_url: str
    push_url: str
    repo_full_name: str | None
    ssh_command: str | None


@dataclass(frozen=True, slots=True)
class DeliveryWorkspaceSnapshot:
    repo_path: str
    worktree_path: str
    branch: str
    base_branch: str
    base_sha: str
    head_sha: str
    head_tree_sha: str


@dataclass(frozen=True, slots=True)
class _WorktreeEntry:
    path: str
    branch_ref: str | None
    head_sha: str | None


def _validate_branch(value: str, *, field: str) -> str:
    if not isinstance(value, str) or _BRANCH_RE.fullmatch(value) is None:
        raise DeliveryWorkspaceError(f"Invalid {field}")
    if value.startswith("-") or ".." in value or "@{" in value:
        raise DeliveryWorkspaceError(f"Invalid {field}")
    return value


def _git_env() -> dict[str, str]:
    # Git accepts repository, index, object-store, hooks, and arbitrary
    # command-scope config overrides through ``GIT_*`` environment variables.
    # In particular, ``GIT_CONFIG_COUNT`` can install a clean/process filter
    # which is invisible to ``git config --local`` and would execute while the
    # privileged Controller stages an untrusted Developer working tree.  Start
    # from the service environment (so PATH/SSH_AUTH_SOCK remain available),
    # but never inherit Git's control plane.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_")
        and key not in {"SSH_ASKPASS", "SSH_ASKPASS_REQUIRE"}
    }
    env.update(
        {
            "GIT_TERMINAL_PROMPT": "0",
            "GCM_INTERACTIVE": "never",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_ASKPASS": "/bin/false",
            "SSH_ASKPASS": "/bin/false",
            "SSH_ASKPASS_REQUIRE": "never",
            "GIT_EDITOR": "/bin/false",
            "GIT_SEQUENCE_EDITOR": "/bin/false",
            "GIT_PAGER": "cat",
            "GIT_SSH_COMMAND": "ssh -F /dev/null",
            "GIT_SSH_VARIANT": "ssh",
            # Controller Git must not inherit a service-user credential helper,
            # signing program, filter driver, or include from global config.
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "LC_ALL": "C",
        }
    )
    return env


async def _await_shielded_cleanup(
    cleanup: asyncio.Task[None],
    *,
    delayed_cancel: asyncio.CancelledError | None = None,
) -> None:
    """Finish one exact cleanup task before propagating any cancellation.

    A second ``Task.cancel()`` also interrupts a plain ``await shield(...)``.
    Keep the cleanup task identity and continue waiting so a Run lease can
    never be released while its Controller Git process is still alive.
    """

    cancellation = await await_task_completion(cleanup)
    cancellation = cancellation or delayed_cancel
    cleanup.result()
    if cancellation is not None:
        raise cancellation


async def _git(
    cwd: Path,
    args: Iterable[str],
    *,
    timeout: float = 60,
    allowed_returncodes: frozenset[int] = frozenset({0}),
    env: dict[str, str] | None = None,
) -> bytes:
    argv = ["git", *_SAFE_GIT_CONFIG, *list(args)]
    spawn = asyncio.create_task(
        asyncio.create_subprocess_exec(
            *argv,
            cwd=str(cwd),
            env=env or _git_env(),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
    )
    delayed_cancel: asyncio.CancelledError | None = None
    try:
        delayed_cancel = await await_task_completion(spawn)
        process = spawn.result()
    except OSError as exc:
        if delayed_cancel is not None:
            raise delayed_cancel
        raise DeliveryWorkspaceError(f"Unable to start Git: {exc}") from exc
    except BaseException:
        if delayed_cancel is not None:
            raise delayed_cancel
        raise

    async def read_limited(stream: asyncio.StreamReader) -> bytes:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = await stream.read(64 * 1024)
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if total > _MAX_GIT_OUTPUT:
                raise DeliveryWorkspaceError(
                    "Git command output exceeded the safety limit"
                )
            chunks.append(chunk)

    async def terminate() -> None:
        if os.name != "posix":
            if process.returncode is not None:
                await process.wait()
                return
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
                return
            except TimeoutError:
                pass
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
            return

        # The session leader may already have exited while a helper it spawned
        # remains alive with inherited descriptors closed.  Always signal the
        # *group*, even after a successful/failed parent exit, and always issue
        # the final SIGKILL before allowing the Controller lease to be released.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        if process.returncode is None:
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                pass
        else:
            await process.wait()
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if process.returncode is None:
            await process.wait()

    async def cleanup_readers() -> None:
        await terminate()
        for task in (stdout_task, stderr_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

    if delayed_cancel is not None:
        # No readers exist yet when cancellation lands during spawn.
        await _await_shielded_cleanup(
            asyncio.create_task(terminate()),
            delayed_cancel=delayed_cancel,
        )

    stdout_task = asyncio.create_task(read_limited(process.stdout))
    stderr_task = asyncio.create_task(read_limited(process.stderr))
    try:
        stdout, stderr = await asyncio.wait_for(
            asyncio.gather(stdout_task, stderr_task),
            timeout,
        )
        await process.wait()
        await _await_shielded_cleanup(asyncio.create_task(terminate()))
    except TimeoutError as exc:
        await _await_shielded_cleanup(asyncio.create_task(cleanup_readers()))
        raise DeliveryWorkspaceError(
            f"Git command timed out: {' '.join(argv)}"
        ) from exc
    except BaseException as exc:
        await _await_shielded_cleanup(
            asyncio.create_task(cleanup_readers()),
            delayed_cancel=(exc if isinstance(exc, asyncio.CancelledError) else None),
        )
        raise
    if process.returncode not in allowed_returncodes:
        message = stderr.decode("utf-8", errors="replace").strip()
        raise DeliveryWorkspaceError(
            f"Git command failed ({' '.join(argv)}): {message[:2000]}"
        )
    return stdout


async def list_delivery_changed_paths(
    *,
    worktree_path: str,
    base_sha: str,
    head_sha: str,
) -> list[str]:
    """Return the exact committed path manifest used for Preview routing."""

    if _SHA_RE.fullmatch(base_sha) is None or _SHA_RE.fullmatch(head_sha) is None:
        raise DeliveryWorkspaceError("Invalid Delivery diff identity")
    workspace = Path(os.path.abspath(worktree_path))
    if not workspace.is_dir() or not (workspace / ".git").exists():
        raise DeliveryWorkspaceError("Delivery worktree is unavailable")
    raw = await _git(
        workspace,
        [
            "diff",
            "--name-only",
            "--no-renames",
            "-z",
            f"{base_sha}..{head_sha}",
            "--",
        ],
    )
    paths: list[str] = []
    for value in raw.split(b"\0"):
        if not value:
            continue
        try:
            path = value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DeliveryWorkspaceError(
                "Delivery changed path is not valid UTF-8"
            ) from exc
        normalized = str(PurePosixPath(path))
        if (
            not normalized
            or normalized.startswith("/")
            or ".." in PurePosixPath(normalized).parts
        ):
            raise DeliveryWorkspaceError("Delivery changed path is invalid")
        paths.append(normalized)
    return sorted(set(paths))


def _read_gitdir_pointer(path: Path, *, label: str) -> Path:
    """Parse one linked-worktree pointer without following an attacker symlink."""

    if path.is_symlink() or not path.is_file():
        raise DeliveryWorkspaceConflict(f"{label} is not a regular file")
    try:
        if path.stat().st_size > _MAX_GITDIR_POINTER_BYTES:
            raise DeliveryWorkspaceConflict(f"{label} is too large")
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise DeliveryWorkspaceConflict(f"{label} cannot be read safely") from exc
    lines = raw.splitlines()
    if len(lines) != 1 or not lines[0].startswith("gitdir: "):
        raise DeliveryWorkspaceConflict(f"{label} is malformed")
    target = Path(lines[0][len("gitdir: ") :])
    if not target.is_absolute():
        target = path.parent / target
    try:
        resolved = target.resolve(strict=True)
    except OSError as exc:
        raise DeliveryWorkspaceConflict(f"{label} target is unavailable") from exc
    if resolved.is_symlink() or not resolved.is_dir():
        raise DeliveryWorkspaceConflict(f"{label} target is not a safe directory")
    return resolved


def _repository_git_dir(repo: Path) -> Path:
    marker = repo / ".git"
    if marker.is_symlink():
        raise DeliveryWorkspaceConflict("Project Git metadata is a symlink")
    if marker.is_dir():
        return marker.resolve(strict=True)
    return _read_gitdir_pointer(marker, label="Project .git pointer")


def _common_git_dir(repo: Path) -> Path:
    git_dir = _repository_git_dir(repo)
    if (repo / ".git").is_dir():
        return git_dir
    commondir = git_dir / "commondir"
    if not commondir.is_file() or commondir.is_symlink():
        raise DeliveryWorkspaceConflict("Project worktree has no safe commondir")
    try:
        raw = commondir.read_text(encoding="utf-8").strip()
        common = (git_dir / raw).resolve(strict=True)
    except (OSError, UnicodeError) as exc:
        raise DeliveryWorkspaceConflict("Project commondir is unavailable") from exc
    if not common.is_dir() or common.is_symlink():
        raise DeliveryWorkspaceConflict("Project common Git directory is unsafe")
    return common


def _unsafe_git_config_key(key: str, value: str) -> bool:
    """Reject repository config that can select an external executable.

    Command-scope overrides cover scalar settings as defense in depth, but
    dynamic namespaces such as ``filter.<name>`` and ``url.<base>`` cannot be
    globally cleared.  Delivery therefore treats their repository-local
    presence as an ownership violation instead of trying to guess whether a
    particular Git subcommand would consult them.
    """

    exact = {
        "core.askpass",
        "core.alternaterefscommand",
        "core.editor",
        "core.fsmonitor",
        "core.gitproxy",
        "core.hookspath",
        "core.pager",
        "core.sshcommand",
        "core.worktree",
        "credential.helper",
        "diff.external",
        "gpg.program",
        "interactive.difffilter",
        "sequence.editor",
        "user.signingkey",
        "commit.gpgsign",
        "tag.gpgsign",
        "push.gpgsign",
        "extensions.worktreeconfig",
    }
    if key in exact:
        return True
    if key.startswith(
        (
            "alias.",
            "credential.",
            "filter.",
            "gpg.",
            "http.",
            "https.",
            "include.",
            "includeif.",
            "difftool.",
            "mergetool.",
            "protocol.",
            "url.",
        )
    ):
        return True
    if key.startswith("diff.") and key.endswith((".command", ".textconv")):
        return True
    if key.startswith("merge.") and key.endswith(".driver"):
        return True
    if key.startswith("remote.") and key.endswith(
        (
            ".uploadpack",
            ".receivepack",
            ".vcs",
            ".proxy",
            ".promisor",
            ".partialclonefilter",
        )
    ):
        return True
    if key.startswith("submodule.") and key.endswith(".update"):
        return value.lstrip().startswith("!")
    return key.startswith("tar.") and key.endswith(".command")


def _canonical_ccm_managed_ssh_command(value: str) -> str | None:
    """Return a shell-safe CCM-managed SSH command, or reject it."""

    try:
        argv = shlex.split(value, posix=True)
    except ValueError:
        return None
    if len(argv) < 3 or argv[0] != "ssh" or argv[1] != "-i":
        return None
    key_path = Path(argv[2])
    if (
        not key_path.is_absolute()
        or key_path.parent != _CCM_GIT_CREDENTIALS_DIR
        or key_path.is_symlink()
        or not key_path.is_file()
        or key_path.stat().st_uid != os.geteuid()
        or stat.S_IMODE(key_path.stat().st_mode) & 0o077
    ):
        return None
    allowed_options = {
        "BatchMode=yes",
        "IdentitiesOnly=yes",
        "StrictHostKeyChecking=yes",
        "StrictHostKeyChecking=no",
    }
    remainder = argv[3:]
    if len(remainder) % 2:
        return None
    if not all(
        remainder[index] == "-o" and remainder[index + 1] in allowed_options
        for index in range(0, len(remainder), 2)
    ):
        return None
    return shlex.join(argv)


async def _read_safe_git_config(
    config_path: Path,
    *,
    allowed_hooks_path: Path | None = None,
) -> list[tuple[str, str]]:
    """Parse one repository-owned config file without executing includes."""

    if (
        config_path.is_symlink()
        or not config_path.is_file()
        or config_path.stat().st_size > _MAX_GIT_CONFIG_BYTES
    ):
        raise DeliveryWorkspaceConflict("Repository Git configuration is unsafe")
    raw = await _git(
        Path(os.path.abspath(os.sep)),
        [
            "config",
            "--no-includes",
            "--null",
            "--file",
            str(config_path),
            "--list",
        ],
    )
    entries: list[tuple[str, str]] = []
    try:
        fields = raw.decode("utf-8", errors="strict").split("\0")
    except UnicodeDecodeError as exc:
        raise DeliveryWorkspaceConflict(
            "Repository Git configuration is not valid UTF-8"
        ) from exc
    for field in fields:
        if not field:
            continue
        key, separator, value = field.partition("\n")
        key = key.lower()
        if not separator or not key:
            raise DeliveryWorkspaceConflict("Repository Git configuration is malformed")
        canonical_default_hooks = (
            key == "core.hookspath"
            and allowed_hooks_path is not None
            and os.path.abspath(value) == str(allowed_hooks_path)
        )
        managed_ssh_command = (
            key == "core.sshcommand"
            and _canonical_ccm_managed_ssh_command(value) is not None
        )
        if (
            not canonical_default_hooks
            and not managed_ssh_command
            and _unsafe_git_config_key(key, value)
        ):
            category = "external Git filters" if key.startswith("filter.") else key
            raise DeliveryWorkspaceConflict(
                f"Repository contains unsafe Git configuration: {category} ({key})"
            )
        entries.append((key, value))
    return entries


def _validate_remote_endpoint(
    value: str,
    *,
    allow_local_remotes: bool,
) -> tuple[str, str | None]:
    if not isinstance(value, str) or not value or any(ch in value for ch in "\r\n\x00"):
        raise DeliveryWorkspaceConflict("Origin remote URL is malformed")

    # Real-Git unit tests use a local bare repository.  Absolute, canonical
    # directory paths are data-only transports and are safe to pass explicitly.
    local = Path(value)
    if local.is_absolute():
        if not allow_local_remotes:
            raise DeliveryWorkspaceConflict(
                "Local Delivery remotes are disabled outside explicit tests"
            )
        if (
            local.is_symlink()
            or not local.is_dir()
            or os.path.realpath(local) != str(local)
        ):
            raise DeliveryWorkspaceConflict(
                "Origin local remote is not a safe directory"
            )
        return value, None

    scp_match = _SCP_GITHUB_RE.fullmatch(value)
    if scp_match is not None:
        return value, scp_match.group("repo")
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise DeliveryWorkspaceConflict("Origin remote URL is malformed") from exc
    if (
        parsed.scheme.lower() not in {"https", "ssh"}
        or (parsed.hostname or "").lower() != "github.com"
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise DeliveryWorkspaceConflict(
            "Origin must be an explicit GitHub or safe local remote"
        )
    path = parsed.path.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    if _GITHUB_REPO_RE.fullmatch(path) is None:
        raise DeliveryWorkspaceConflict("Origin GitHub repository is malformed")
    return value, path


async def _validate_controller_git_repository(
    repo: Path,
    *,
    expected_repo_full_name: str | None = None,
    allow_local_remotes: bool = False,
) -> _ValidatedGitRepository:
    """Read repository config without includes and return explicit safe URLs."""

    common = _common_git_dir(repo)
    git_dir = _repository_git_dir(repo)
    config_path = common / "config"
    if (
        config_path.is_symlink()
        or not config_path.is_file()
        or config_path.stat().st_size > _MAX_GIT_CONFIG_BYTES
    ):
        raise DeliveryWorkspaceConflict("Repository Git configuration is unsafe")
    allowed_hooks_path = common / "hooks"
    entries = await _read_safe_git_config(
        config_path,
        allowed_hooks_path=allowed_hooks_path,
    )
    worktree_config = git_dir / "config.worktree"
    if os.path.lexists(worktree_config):
        # Worktree-specific settings are legitimate, but receive the same
        # executable/credential/transport safety validation as common config.
        await _read_safe_git_config(
            worktree_config,
            allowed_hooks_path=allowed_hooks_path,
        )

    fetch_values = [value for key, value in entries if key == "remote.origin.url"]
    push_values = [value for key, value in entries if key == "remote.origin.pushurl"]
    ssh_values = [value for key, value in entries if key == "core.sshcommand"]
    if len(fetch_values) != 1 or len(push_values) > 1:
        raise DeliveryWorkspaceConflict(
            "Origin must define exactly one fetch and at most one push URL"
        )
    if len(ssh_values) > 1:
        raise DeliveryWorkspaceConflict("Repository defines multiple SSH commands")
    ssh_command = (
        _canonical_ccm_managed_ssh_command(ssh_values[0]) if ssh_values else None
    )
    fetch_url, fetch_repo = _validate_remote_endpoint(
        fetch_values[0],
        allow_local_remotes=allow_local_remotes,
    )
    push_url, push_repo = _validate_remote_endpoint(
        push_values[0] if push_values else fetch_values[0],
        allow_local_remotes=allow_local_remotes,
    )
    if fetch_repo is None or push_repo is None:
        if (
            not allow_local_remotes
            or fetch_repo is not None
            or push_repo is not None
            or fetch_url != push_url
        ):
            raise DeliveryWorkspaceConflict(
                "Origin fetch and push remotes have different identities"
            )
        normalized_repo = None
    else:
        if fetch_repo.lower() != push_repo.lower():
            raise DeliveryWorkspaceConflict(
                "Origin fetch and push remotes have different GitHub identities"
            )
        if (
            not isinstance(expected_repo_full_name, str)
            or _GITHUB_REPO_RE.fullmatch(expected_repo_full_name) is None
        ):
            raise DeliveryWorkspaceConflict(
                "Expected monitored GitHub repository identity is required"
            )
        if fetch_repo.lower() != expected_repo_full_name.lower():
            raise DeliveryWorkspaceConflict(
                "Origin does not match the monitored GitHub repository"
            )
        normalized_repo = fetch_repo
    return _ValidatedGitRepository(
        common_git_dir=common,
        fetch_url=fetch_url,
        push_url=push_url,
        repo_full_name=normalized_repo,
        ssh_command=ssh_command,
    )


def _restore_missing_worktree_pointer(
    *,
    repository: _ValidatedGitRepository,
    workspace: Path,
    branch_ref: str,
) -> None:
    """Restore only the exact pointer from an intact Git registration."""

    pointer = workspace / ".git"
    if os.path.lexists(pointer):
        raise DeliveryWorkspaceConflict(
            "Incomplete Delivery .git pointer exists and cannot be replaced"
        )
    registrations = repository.common_git_dir / "worktrees"
    if registrations.is_symlink() or not registrations.is_dir():
        raise DeliveryWorkspaceConflict("Project worktree registrations are unsafe")

    matches: list[Path] = []
    try:
        candidates = list(registrations.iterdir())
    except OSError as exc:
        raise DeliveryWorkspaceConflict(
            "Project worktree registrations cannot be inspected"
        ) from exc
    for candidate in candidates:
        if candidate.is_symlink() or not candidate.is_dir():
            continue
        backlink = candidate / "gitdir"
        if backlink.is_symlink() or not backlink.is_file():
            continue
        try:
            if backlink.stat().st_size > _MAX_GITDIR_POINTER_BYTES:
                continue
            target = Path(backlink.read_text(encoding="utf-8").strip())
        except (OSError, UnicodeError):
            continue
        if not target.is_absolute():
            target = candidate / target
        if Path(os.path.abspath(target)) == pointer:
            matches.append(candidate.resolve(strict=True))
    if len(matches) != 1:
        raise DeliveryWorkspaceConflict(
            "Delivery worktree registration is missing or ambiguous"
        )

    control = matches[0]
    commondir = control / "commondir"
    head = control / "HEAD"
    if (
        commondir.is_symlink()
        or not commondir.is_file()
        or head.is_symlink()
        or not head.is_file()
    ):
        raise DeliveryWorkspaceConflict("Delivery worktree control data is unsafe")
    try:
        common_target = (
            control / commondir.read_text(encoding="utf-8").strip()
        ).resolve(strict=True)
        head_value = head.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise DeliveryWorkspaceConflict(
            "Delivery worktree control data cannot be read"
        ) from exc
    if common_target != repository.common_git_dir or head_value != f"ref: {branch_ref}":
        raise DeliveryWorkspaceConflict(
            "Delivery worktree control data does not match its branch"
        )

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(pointer, flags, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(f"gitdir: {control}\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise DeliveryWorkspaceConflict(
            "Delivery .git pointer changed during repair"
        ) from exc
    except OSError as exc:
        raise DeliveryWorkspaceConflict(
            "Delivery .git pointer could not be restored"
        ) from exc


async def _validate_linked_worktree_control(repo: Path, workspace: Path) -> Path:
    """Prove the writable worktree still points at this repository's Git dir.

    A workspace-write Developer can edit the in-worktree ``.git`` pointer but
    cannot write the out-of-tree linked-worktree metadata.  Validate that
    pointer *before* invoking Git so a completed model turn cannot redirect a
    privileged Controller command to another repository.
    """

    common = _common_git_dir(repo)
    git_dir = _read_gitdir_pointer(
        workspace / ".git",
        label="Delivery .git pointer",
    )
    worktrees = common / "worktrees"
    try:
        relative = git_dir.relative_to(worktrees)
    except ValueError as exc:
        raise DeliveryWorkspaceConflict(
            "Delivery .git pointer escaped the Project repository"
        ) from exc
    if len(relative.parts) != 1:
        raise DeliveryWorkspaceConflict(
            "Delivery .git pointer is not a direct managed worktree"
        )

    commondir = git_dir / "commondir"
    backlink = git_dir / "gitdir"
    if (
        commondir.is_symlink()
        or not commondir.is_file()
        or backlink.is_symlink()
        or not backlink.is_file()
    ):
        raise DeliveryWorkspaceConflict("Delivery Git control files are unsafe")
    try:
        common_target = (
            git_dir / commondir.read_text(encoding="utf-8").strip()
        ).resolve(strict=True)
        backlink_target = Path(backlink.read_text(encoding="utf-8").strip()).resolve(
            strict=True
        )
    except (OSError, UnicodeError) as exc:
        raise DeliveryWorkspaceConflict(
            "Delivery Git control files cannot be resolved"
        ) from exc
    if common_target != common or backlink_target != (workspace / ".git").resolve():
        raise DeliveryWorkspaceConflict("Delivery Git ownership changed")
    worktree_config = git_dir / "config.worktree"
    if os.path.lexists(worktree_config):
        await _read_safe_git_config(
            worktree_config,
            allowed_hooks_path=common / "hooks",
        )
    return git_dir


def _parse_worktree_list(raw: bytes) -> list[_WorktreeEntry]:
    entries: list[_WorktreeEntry] = []
    current: dict[str, str] = {}
    fields = raw.decode("utf-8", errors="strict").split("\0")

    def finish() -> None:
        nonlocal current
        if not current:
            return
        path = current.get("worktree")
        if not path:
            raise DeliveryWorkspaceError("Malformed git worktree output")
        entries.append(
            _WorktreeEntry(
                path=os.path.abspath(path),
                branch_ref=current.get("branch"),
                head_sha=current.get("HEAD"),
            )
        )
        current = {}

    for line in fields:
        if not line:
            continue
        key, separator, value = line.partition(" ")
        if key == "worktree" and current:
            finish()
        if key in {"worktree", "HEAD", "branch"}:
            if not separator or key in current:
                raise DeliveryWorkspaceError("Malformed git worktree output")
            current[key] = value
    finish()
    return entries


def _safe_repo_root(repo_path: str) -> Path:
    absolute = Path(os.path.abspath(repo_path))
    if (
        not absolute.is_absolute()
        or absolute.is_symlink()
        or not absolute.is_dir()
        or os.path.realpath(absolute) != str(absolute)
    ):
        raise DeliveryWorkspaceError("Project repository path is not a safe directory")
    return absolute


def _ensure_managed_parent(repo: Path) -> Path:
    current = repo
    for component in (".claude-manager", "worktrees"):
        current = current / component
        if os.path.lexists(current):
            if current.is_symlink() or not current.is_dir():
                raise DeliveryWorkspaceConflict(
                    "Managed Delivery workspace ancestry is not a safe directory"
                )
        else:
            current.mkdir(mode=0o700)
    return current


class DeliveryWorkspaceManager:
    """Prepare and re-verify one fixed worktree without ever deleting it."""

    def __init__(
        self,
        *,
        fetch_timeout: float = 120,
        allow_local_remotes: bool = False,
    ) -> None:
        self.fetch_timeout = fetch_timeout
        self.allow_local_remotes = bool(allow_local_remotes)

    async def list_changed_paths(
        self,
        *,
        worktree_path: str,
        base_sha: str,
        head_sha: str,
    ) -> list[str]:
        return await list_delivery_changed_paths(
            worktree_path=worktree_path,
            base_sha=base_sha,
            head_sha=head_sha,
        )

    async def prepare(
        self,
        *,
        repo_path: str,
        run_id: int,
        branch: str,
        base_branch: str,
        expected_repo_full_name: str | None = None,
    ) -> DeliveryWorkspaceSnapshot:
        if isinstance(run_id, bool) or run_id <= 0:
            raise DeliveryWorkspaceError("run_id must be a positive integer")
        branch = _validate_branch(branch, field="delivery branch")
        base_branch = _validate_branch(base_branch, field="base branch")
        repo = _safe_repo_root(repo_path)
        repository = await _validate_controller_git_repository(
            repo,
            expected_repo_full_name=expected_repo_full_name,
            allow_local_remotes=self.allow_local_remotes,
        )
        top_level = Path(
            (await _git(repo, ["rev-parse", "--show-toplevel"])).decode().strip()
        )
        if Path(os.path.abspath(top_level)) != repo:
            raise DeliveryWorkspaceError(
                "Project path must be the exact Git worktree root"
            )
        managed_parent = _ensure_managed_parent(repo)
        expected = managed_parent / f"delivery-{run_id}"
        expected_abs = Path(os.path.abspath(expected))
        try:
            expected_abs.relative_to(repo)
        except ValueError as exc:
            raise DeliveryWorkspaceError(
                "Delivery workspace escaped the repository"
            ) from exc

        entries = _parse_worktree_list(
            await _git(repo, ["worktree", "list", "--porcelain", "-z"])
        )
        branch_ref = f"refs/heads/{branch}"
        path_entry = next(
            (item for item in entries if item.path == str(expected_abs)),
            None,
        )
        branch_entries = [item for item in entries if item.branch_ref == branch_ref]
        if path_entry is not None:
            if path_entry.branch_ref != branch_ref:
                raise DeliveryWorkspaceConflict(
                    "Delivery workspace path belongs to a different branch"
                )
            if len(branch_entries) != 1 or branch_entries[0].path != str(expected_abs):
                raise DeliveryWorkspaceConflict(
                    "Delivery branch is registered to an unexpected worktree"
                )
            if not os.path.lexists(expected_abs):
                # Git can persist the linked-worktree administration before
                # checkout creates the path.  Remove only this exact stale
                # registration; the branch ref is intentionally retained and
                # reattached below after exact-base verification.
                try:
                    await _git(
                        repo,
                        ["worktree", "remove", "--force", str(expected_abs)],
                    )
                except DeliveryWorkspaceError as exc:
                    raise DeliveryWorkspaceConflict(
                        "Incomplete Delivery worktree registration cannot be repaired"
                    ) from exc
                entries = _parse_worktree_list(
                    await _git(repo, ["worktree", "list", "--porcelain", "-z"])
                )
                if any(
                    item.path == str(expected_abs) or item.branch_ref == branch_ref
                    for item in entries
                ):
                    raise DeliveryWorkspaceConflict(
                        "Incomplete Delivery worktree registration remains active"
                    )
                path_entry = None
                branch_entries = []
            else:
                if expected_abs.is_symlink() or not expected_abs.is_dir():
                    raise DeliveryWorkspaceConflict(
                        "Delivery workspace path is not a safe directory"
                    )
                if not os.path.lexists(expected_abs / ".git"):
                    _restore_missing_worktree_pointer(
                        repository=repository,
                        workspace=expected_abs,
                        branch_ref=branch_ref,
                    )
                repaired = _parse_worktree_list(
                    await _git(repo, ["worktree", "list", "--porcelain", "-z"])
                )
                repaired_entries = [
                    item
                    for item in repaired
                    if item.path == str(expected_abs) or item.branch_ref == branch_ref
                ]
                if (
                    len(repaired_entries) != 1
                    or repaired_entries[0].path != str(expected_abs)
                    or repaired_entries[0].branch_ref != branch_ref
                ):
                    raise DeliveryWorkspaceConflict(
                        "Delivery worktree repair did not restore exact ownership"
                    )
                return await self.inspect(
                    repo_path=str(repo),
                    worktree_path=str(expected_abs),
                    branch=branch,
                    base_branch=base_branch,
                    expected_repo_full_name=expected_repo_full_name,
                )
        if branch_entries:
            raise DeliveryWorkspaceConflict(
                "Delivery branch already belongs to another worktree"
            )
        if os.path.lexists(expected_abs):
            if expected_abs.is_symlink() or not expected_abs.is_dir():
                raise DeliveryWorkspaceConflict(
                    "Delivery workspace path exists but is not a safe directory"
                )
            try:
                empty = next(expected_abs.iterdir(), None) is None
            except OSError as exc:
                raise DeliveryWorkspaceConflict(
                    "Delivery workspace path cannot be inspected safely"
                ) from exc
            if not empty:
                raise DeliveryWorkspaceConflict(
                    "Delivery workspace path exists but is not registered by Git"
                )
            expected_abs.rmdir()

        remote_ref = f"refs/remotes/origin/{base_branch}"
        fetch_env = _git_env()
        if repository.ssh_command is not None:
            # GIT_SSH_COMMAND has precedence over the inert command-scope
            # core.sshCommand override. The value was reconstructed only from
            # CCM's owned 0600 key and a closed set of SSH options.
            fetch_env["GIT_SSH_COMMAND"] = repository.ssh_command
        await _git(
            repo,
            [
                "fetch",
                "--no-tags",
                "--no-recurse-submodules",
                "--no-auto-maintenance",
                "--upload-pack=git-upload-pack",
                repository.fetch_url,
                f"refs/heads/{base_branch}:{remote_ref}",
            ],
            timeout=self.fetch_timeout,
            env=fetch_env,
        )
        base_sha = (
            (await _git(repo, ["rev-parse", "--verify", f"{remote_ref}^{{commit}}"]))
            .decode()
            .strip()
        )
        if _SHA_RE.fullmatch(base_sha) is None:
            raise DeliveryWorkspaceError("Remote base did not resolve to a commit")
        existing_branch = (
            (
                await _git(
                    repo,
                    ["rev-parse", "--verify", "--quiet", f"{branch_ref}^{{commit}}"],
                    allowed_returncodes=frozenset({0, 1}),
                )
            )
            .decode()
            .strip()
        )
        if existing_branch and (
            _SHA_RE.fullmatch(existing_branch) is None or existing_branch != base_sha
        ):
            raise DeliveryWorkspaceConflict(
                "Existing Delivery branch does not match the fetched base"
            )
        add_args = ["worktree", "add"]
        if existing_branch:
            add_args.extend([str(expected_abs), branch])
        else:
            add_args.extend(["--no-track", "-b", branch, str(expected_abs), remote_ref])
        await _git(
            repo,
            add_args,
            timeout=self.fetch_timeout,
        )
        snapshot = await self.inspect(
            repo_path=str(repo),
            worktree_path=str(expected_abs),
            branch=branch,
            base_branch=base_branch,
            expected_repo_full_name=expected_repo_full_name,
        )
        if snapshot.base_sha != base_sha or snapshot.head_sha != base_sha:
            raise DeliveryWorkspaceConflict(
                "New Delivery workspace did not start at the fetched base"
            )
        return snapshot

    async def inspect(
        self,
        *,
        repo_path: str,
        worktree_path: str,
        branch: str,
        base_branch: str,
        expected_repo_full_name: str | None = None,
    ) -> DeliveryWorkspaceSnapshot:
        branch = _validate_branch(branch, field="delivery branch")
        base_branch = _validate_branch(base_branch, field="base branch")
        repo = _safe_repo_root(repo_path)
        await _validate_controller_git_repository(
            repo,
            expected_repo_full_name=expected_repo_full_name,
            allow_local_remotes=self.allow_local_remotes,
        )
        workspace = Path(os.path.abspath(worktree_path))
        if workspace.is_symlink() or not workspace.is_dir():
            raise DeliveryWorkspaceConflict(
                "Delivery workspace is not a safe directory"
            )
        managed_parent = _ensure_managed_parent(repo)
        try:
            workspace.relative_to(managed_parent)
        except ValueError as exc:
            raise DeliveryWorkspaceConflict(
                "Delivery workspace is outside the managed project directory"
            ) from exc
        await _validate_linked_worktree_control(repo, workspace)
        top_level = Path(
            (await _git(workspace, ["rev-parse", "--show-toplevel"])).decode().strip()
        )
        if Path(os.path.abspath(top_level)) != workspace:
            raise DeliveryWorkspaceConflict("Delivery workspace root changed")
        current_branch = (
            (await _git(workspace, ["symbolic-ref", "--short", "HEAD"]))
            .decode()
            .strip()
        )
        if current_branch != branch:
            raise DeliveryWorkspaceConflict(
                f"Delivery workspace is on {current_branch!r}, expected {branch!r}"
            )
        head_sha = (
            (await _git(workspace, ["rev-parse", "--verify", "HEAD^{commit}"]))
            .decode()
            .strip()
        )
        tree_sha = (
            (await _git(workspace, ["rev-parse", "--verify", "HEAD^{tree}"]))
            .decode()
            .strip()
        )
        base_sha = (
            (
                await _git(
                    workspace,
                    [
                        "rev-parse",
                        "--verify",
                        f"refs/remotes/origin/{base_branch}^{{commit}}",
                    ],
                )
            )
            .decode()
            .strip()
        )
        if not all(
            _SHA_RE.fullmatch(value) for value in (head_sha, tree_sha, base_sha)
        ):
            raise DeliveryWorkspaceError(
                "Delivery workspace returned an invalid object ID"
            )
        status = await _git(
            workspace,
            ["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        )
        if status:
            raise DeliveryWorkspaceConflict(
                "Delivery workspace contains uncommitted or untracked changes"
            )
        try:
            merge_base = (
                (await _git(workspace, ["merge-base", base_sha, head_sha]))
                .decode()
                .strip()
            )
        except DeliveryWorkspaceError as exc:
            raise DeliveryWorkspaceConflict(
                "Delivery branch no longer descends from the frozen base"
            ) from exc
        if merge_base != base_sha:
            raise DeliveryWorkspaceConflict(
                "Delivery branch no longer descends from the frozen base"
            )
        return DeliveryWorkspaceSnapshot(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=branch,
            base_branch=base_branch,
            base_sha=base_sha,
            head_sha=head_sha,
            head_tree_sha=tree_sha,
        )

    async def commit_changes(
        self,
        *,
        repo_path: str,
        worktree_path: str,
        branch: str,
        base_branch: str,
        expected_head_sha: str,
        run_id: int,
        turn_generation: int,
        title: str,
        expected_repo_full_name: str | None = None,
    ) -> DeliveryWorkspaceSnapshot:
        """Create the Controller-owned commit for one completed Developer turn.

        The Developer receives a network-disabled workspace-write sandbox and
        deliberately cannot write the linked worktree's out-of-tree Git
        metadata.  It leaves a reviewed working tree; this method validates the
        pointer, stages it with hooks/global config disabled, and records the
        exact Run/Turn marker.  If CCM crashes after ``git commit`` but before
        the database transition, the marker makes the effect idempotently
        recoverable instead of committing the same files twice.
        """

        if (
            _SHA_RE.fullmatch(expected_head_sha or "") is None
            or isinstance(run_id, bool)
            or run_id <= 0
            or isinstance(turn_generation, bool)
            or turn_generation <= 0
        ):
            raise DeliveryWorkspaceError("Invalid Delivery commit subject")
        branch = _validate_branch(branch, field="delivery branch")
        base_branch = _validate_branch(base_branch, field="base branch")
        repo = _safe_repo_root(repo_path)
        await _validate_controller_git_repository(
            repo,
            expected_repo_full_name=expected_repo_full_name,
            allow_local_remotes=self.allow_local_remotes,
        )
        workspace = Path(os.path.abspath(worktree_path))
        if workspace.is_symlink() or not workspace.is_dir():
            raise DeliveryWorkspaceConflict(
                "Delivery workspace is not a safe directory"
            )
        managed_parent = _ensure_managed_parent(repo)
        try:
            workspace.relative_to(managed_parent)
        except ValueError as exc:
            raise DeliveryWorkspaceConflict(
                "Delivery workspace is outside the managed project directory"
            ) from exc
        if workspace != managed_parent / f"delivery-{run_id}":
            raise DeliveryWorkspaceConflict(
                "Delivery workspace path does not match its owning Run"
            )
        await _validate_linked_worktree_control(repo, workspace)

        current_branch = (
            (await _git(workspace, ["symbolic-ref", "--short", "HEAD"]))
            .decode()
            .strip()
        )
        if current_branch != branch:
            raise DeliveryWorkspaceConflict(
                f"Delivery workspace is on {current_branch!r}, expected {branch!r}"
            )
        current_head = (
            (await _git(workspace, ["rev-parse", "--verify", "HEAD^{commit}"]))
            .decode()
            .strip()
        )
        marker = f"CCM-Delivery-Run: {run_id}\nCCM-Delivery-Turn: {turn_generation}"

        if current_head != expected_head_sha:
            # Recovery is accepted only for the exact single commit this method
            # could have created for the old head.  Any other advance remains a
            # subject violation, even if its tree happens to look plausible.
            parents = (
                (await _git(workspace, ["show", "-s", "--format=%P", "HEAD"]))
                .decode()
                .strip()
                .split()
            )
            body = (
                await _git(workspace, ["show", "-s", "--format=%B", "HEAD"])
            ).decode("utf-8", errors="strict")
            current_tree = (
                (await _git(workspace, ["rev-parse", "--verify", "HEAD^{tree}"]))
                .decode()
                .strip()
            )
            previous_tree = (
                (
                    await _git(
                        workspace,
                        ["rev-parse", "--verify", f"{expected_head_sha}^{{tree}}"],
                    )
                )
                .decode()
                .strip()
            )
            if (
                parents != [expected_head_sha]
                or current_tree == previous_tree
                or marker not in body
            ):
                raise DeliveryWorkspaceConflict(
                    "Delivery workspace head changed outside the Controller commit"
                )
            return await self.inspect(
                repo_path=str(repo),
                worktree_path=str(workspace),
                branch=branch,
                base_branch=base_branch,
                expected_repo_full_name=expected_repo_full_name,
            )

        # Git filters are arbitrary executables.  Project-local filter drivers
        # would let an untrusted .gitattributes file execute outside the model's
        # sandbox during Controller staging, so V1 fails closed when configured.
        filters = await _git(
            workspace,
            [
                *_SAFE_GIT_CONFIG,
                "config",
                "--local",
                "--get-regexp",
                r"^filter\..*\.(clean|process)$",
            ],
            allowed_returncodes=frozenset({0, 1}),
        )
        if filters.strip():
            raise DeliveryWorkspaceConflict(
                "Delivery Controller commit does not allow external Git filters"
            )

        # Provider isolation can materialize exact zero-byte deny placeholders
        # for credential/config names inside a writable workspace. They are a
        # runtime boundary, not Developer output, and presence alone can alter
        # package-manager or deployment behavior. Remove only the explicit
        # isolation inventory when it is still untracked, regular, unsymlinked
        # and empty; never apply a pattern or delete a tracked/user file.
        from backend.services.task_agent_isolation import (
            DELIVERY_ISOLATION_PLACEHOLDER_NAMES,
        )

        untracked = {
            value.decode("utf-8", errors="strict")
            for value in (
                await _git(
                    workspace,
                    ["ls-files", "--others", "--exclude-standard", "-z"],
                )
            ).split(b"\0")
            if value
        }
        for relative in sorted(DELIVERY_ISOLATION_PLACEHOLDER_NAMES & untracked):
            candidate = workspace / relative
            try:
                info = candidate.lstat()
            except FileNotFoundError:
                continue
            if (
                stat.S_ISREG(info.st_mode)
                and not stat.S_ISLNK(info.st_mode)
                and info.st_size == 0
            ):
                candidate.unlink()

        await _git(workspace, [*_SAFE_GIT_CONFIG, "add", "--all"])
        staged_tree = (
            (await _git(workspace, [*_SAFE_GIT_CONFIG, "write-tree"])).decode().strip()
        )
        previous_tree = (
            (
                await _git(
                    workspace,
                    ["rev-parse", "--verify", f"{expected_head_sha}^{{tree}}"],
                )
            )
            .decode()
            .strip()
        )
        if staged_tree != previous_tree:
            normalized_title = " ".join((title or "Delivery update").split())[:160]
            message = f"{normalized_title or 'Delivery update'}\n\n{marker}\n"
            env = _git_env()
            env.update(
                {
                    "GIT_AUTHOR_NAME": "CCM Delivery Controller",
                    "GIT_AUTHOR_EMAIL": "delivery@ccm.local",
                    "GIT_COMMITTER_NAME": "CCM Delivery Controller",
                    "GIT_COMMITTER_EMAIL": "delivery@ccm.local",
                }
            )
            await _git(
                workspace,
                [
                    *_SAFE_GIT_CONFIG,
                    "commit",
                    "--no-verify",
                    "--no-gpg-sign",
                    "-m",
                    message,
                ],
                env=env,
            )

        return await self.inspect(
            repo_path=str(repo),
            worktree_path=str(workspace),
            branch=branch,
            base_branch=base_branch,
            expected_repo_full_name=expected_repo_full_name,
        )
