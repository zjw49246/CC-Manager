"""Fail-closed filesystem/network policy for local model Task processes."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import shlex
import stat
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from backend.services.mcp_config import (
    CCM_BROWSER_REVIEW_TOOLS,
    CCM_FRONTEND_REVIEW_TOOLS,
    CCM_MONITOR_AGENT_TOOLS,
    CCM_SKILLS_TOOLS,
    CCM_SSH_TOOLS,
    CCM_SUB_AGENT_TOOLS,
    CCM_WORKSPACE_REVIEW_TOOLS,
)
from backend.services.task_runtime_secrets import (
    runtime_secret_root,
    write_private_json,
)


CLAUDE_TASK_BUILTIN_TOOLS = (
    "AskUserQuestion",
    "Bash",
    "Edit",
    "Glob",
    "Grep",
    "MultiEdit",
    "NotebookEdit",
    "Read",
    "Write",
)

# An unrestricted administrator turn historically used Claude's complete
# built-in inventory.  It still needs an explicit permission allowlist because
# subprocess environment scrubbing can make an interactive PTY report effective
# ``default`` mode, but that compatibility profile must not silently remove
# native web, agent, workflow, or skill tools.  ``default`` is Claude's own
# stable token for the complete built-in inventory. Permission allow rules are
# separate and concrete below.
CLAUDE_UNRESTRICTED_BUILTIN_TOOLS = ("default",)

# ``default`` is expanded only by Claude's base-tool parser.  The permission
# parser treats it as a literal (unknown) tool name, so unrestricted turns need
# concrete allow rules even though their inventory remains future-compatible.
# These are the canonical built-ins registered by the pinned production CLI
# (2.1.168), including conditionally enabled tools.  Keep canonical registry
# names here rather than legacy aliases (for example ``Task`` or
# ``MultiEdit``): ``--tools default`` controls availability, while this list
# only prevents an available built-in from falling into a hidden permission
# prompt after Claude's effective-mode fallback.
CLAUDE_UNRESTRICTED_PERMISSION_TOOLS = (
    "Agent",
    "AskUserQuestion",
    "Bash",
    "CronCreate",
    "CronDelete",
    "CronList",
    "DesignSync",
    "Edit",
    "EnterPlanMode",
    "EnterWorktree",
    "ExitPlanMode",
    "ExitWorktree",
    "Glob",
    "Grep",
    "ListMcpResourcesTool",
    "LSP",
    "Monitor",
    "NotebookEdit",
    "PowerShell",
    "PushNotification",
    "Read",
    "ReadMcpResourceTool",
    "RemoteTrigger",
    "REPL",
    "ScheduleWakeup",
    "SendMessage",
    "SendUserFile",
    "SendUserMessage",
    "ShareOnboardingGuide",
    "Skill",
    "TaskCreate",
    "TaskGet",
    "TaskList",
    "TaskOutput",
    "TaskStop",
    "TaskUpdate",
    "TeamCreate",
    "TeamDelete",
    "TodoWrite",
    "ToolSearch",
    "WaitForMcpServers",
    "WebFetch",
    "WebSearch",
    "Workflow",
    "Write",
)

# Native Claude delegation and team-management tools must stay unavailable
# while the CCM Sub-Agent skill is active. Keep legacy ``Task`` alongside the
# current task/team tool names because Claude versions expose both shapes.
CLAUDE_NATIVE_SUB_AGENT_TOOLS = (
    "Agent",
    "Task",
    "TaskCreate",
    "TaskGet",
    "TaskList",
    "TaskOutput",
    "TaskStop",
    "TaskUpdate",
    "TeamCreate",
    "TeamDelete",
    "SendMessage",
)

CLAUDE_DELIVERY_BUILTIN_TOOLS = (
    "Bash",
    "Edit",
    "Glob",
    "Grep",
    "MultiEdit",
    "NotebookEdit",
    "Read",
    "Write",
)

CLAUDE_MONITOR_BUILTIN_TOOLS = (
    "Bash",
    "Glob",
    "Grep",
    "Read",
)

CLAUDE_SUB_AGENT_BUILTIN_TOOLS = (
    "Bash",
    "Edit",
    "Glob",
    "Grep",
    "MultiEdit",
    "NotebookEdit",
    "Read",
    "Write",
)

CLAUDE_READ_ONLY_BUILTIN_TOOLS = (
    "Glob",
    "Grep",
    "Read",
)

# Bubblewrap/Codex may create empty mount targets for denied workspace-local
# configuration paths. These exact names are runtime scaffolding only when
# they remain untracked, zero-byte regular files; the Delivery Controller uses
# this closed inventory before staging a reviewed Developer tree.
DELIVERY_ISOLATION_PLACEHOLDER_NAMES = frozenset({
    ".env",
    ".env.development",
    ".env.development.local",
    ".env.local",
    ".env.production",
    ".env.production.local",
    ".env.test",
    ".env.test.local",
    ".gitmodules",
    ".npmrc",
    ".yarnrc",
    ".yarnrc.yml",
    "bunfig.toml",
    "package-lock.json",
    "package.json",
    "pnpm-lock.yaml",
    "yarn.lock",
})


class TaskAgentIsolationError(RuntimeError):
    """The provider could not prove the required Task isolation boundary."""


class TaskWorkingDirectoryMissingError(TaskAgentIsolationError):
    """The Task's explicit working directory does not exist on this host.

    Typical cause: the Project clone failed (or a worktree was removed) while
    ``target_repo``/``last_cwd`` still points at the path. Raising a typed,
    human-readable error here replaces the bare ``[Errno 2]`` the OS would
    otherwise produce at subprocess spawn time.
    """

    def __init__(self, path: str):
        super().__init__(
            f"Task working directory {path} does not exist — the project may "
            "not have been cloned successfully; check the Project status and "
            "re-clone it if needed"
        )
        self.path = path


def require_existing_task_cwd(cwd: str) -> str:
    """Preflight for launch paths that spawn without the full isolation check.

    Monitor and Sub-Agent turns spawn their processes directly and never pass
    through ``prepare_task_working_directory``; give them the same typed error
    instead of a bare OS-level ENOENT.
    """
    if not os.path.isdir(cwd):
        raise TaskWorkingDirectoryMissingError(cwd)
    return cwd


@dataclass(frozen=True)
class LinkedWorktreeGitReadBoundary:
    """Exact read-only metadata needed by Git in one linked worktree.

    The common object store is necessarily shared, but config, hooks, packed
    refs, credentials, other branch refs, and every other worktree gitdir stay
    outside this set.  No shared Git path is ever writable by the model.
    """

    git_dir: str
    common_dir: str
    head_ref: str | None
    read_paths: tuple[str, ...]
    identity_fingerprint: tuple[tuple[object, ...], ...]


@dataclass(frozen=True)
class _StableGitControlFile:
    text: str
    identity: tuple[object, ...]


def _read_stable_git_control_file(
    path: Path,
    *,
    label: str,
    maximum_bytes: int = 4096,
) -> _StableGitControlFile:
    try:
        path_info = path.lstat()
    except OSError as exc:
        raise TaskAgentIsolationError(
            f"Linked worktree {label} is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(path_info.st_mode)
        or not stat.S_ISREG(path_info.st_mode)
        or path_info.st_uid != os.geteuid()
        or path_info.st_size < 1
        or path_info.st_size > maximum_bytes
    ):
        raise TaskAgentIsolationError(
            f"Linked worktree {label} is not a safe regular file"
        )
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise TaskAgentIsolationError(
            f"Linked worktree {label} could not be opened safely"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            opened.st_dev != path_info.st_dev
            or opened.st_ino != path_info.st_ino
            or opened.st_size != path_info.st_size
        ):
            raise TaskAgentIsolationError(
                f"Linked worktree {label} changed while opening"
            )
        payload = os.read(descriptor, maximum_bytes + 1)
        if len(payload) != opened.st_size or os.read(descriptor, 1):
            raise TaskAgentIsolationError(
                f"Linked worktree {label} changed while reading"
            )
        finished = os.fstat(descriptor)
        if (
            finished.st_dev != opened.st_dev
            or finished.st_ino != opened.st_ino
            or finished.st_size != opened.st_size
            or finished.st_mtime_ns != opened.st_mtime_ns
        ):
            raise TaskAgentIsolationError(
                f"Linked worktree {label} changed while reading"
            )
    finally:
        os.close(descriptor)
    try:
        text = payload.decode("utf-8")
    except UnicodeError as exc:
        raise TaskAgentIsolationError(
            f"Linked worktree {label} is not UTF-8"
        ) from exc
    return _StableGitControlFile(
        text=text,
        identity=(
            str(path),
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_uid,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
            hashlib.sha256(payload).hexdigest(),
        ),
    )


def _git_path_identity(path: Path) -> tuple[object, ...]:
    """Snapshot one path without following links, including absence."""

    try:
        info = path.lstat()
    except FileNotFoundError:
        return (str(path), "missing")
    except OSError as exc:
        raise TaskAgentIsolationError(
            "Linked worktree identity boundary is unavailable"
        ) from exc
    return (
        str(path),
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
        "",
    )


def _require_real_git_directory(path: Path, *, label: str) -> Path:
    try:
        lexical = Path(os.path.abspath(path))
        resolved = lexical.resolve(strict=True)
        info = lexical.lstat()
    except (OSError, RuntimeError) as exc:
        raise TaskAgentIsolationError(
            f"Linked worktree {label} is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or lexical != resolved
    ):
        raise TaskAgentIsolationError(
            f"Linked worktree {label} must be a service-owned real directory"
        )
    return resolved


def _safe_linked_head_ref(value: str) -> bool:
    if (
        not value.startswith("refs/heads/")
        or len(value) > 512
        or "@{" in value
        or "//" in value
        or "\\" in value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    parts = value.split("/")
    return all(
        part
        and part not in {".", ".."}
        and not part.startswith(".")
        and not part.endswith(".")
        and not part.endswith(".lock")
        for part in parts
    )


def discover_linked_worktree_git_read_boundary(
    working_directory: str | os.PathLike[str],
) -> LinkedWorktreeGitReadBoundary | None:
    """Prove the exact read-only Git projection for one linked worktree.

    A normal checkout's ``.git`` directory is deliberately unsupported: its
    config, hooks and writable object/ref database live together.  A linked
    worktree gives CCM a separate pointer and per-worktree control files, so
    status/diff/log can be restored with exact read mounts while every Git
    write (including commit) remains denied.
    """

    try:
        workspace = Path(working_directory).expanduser().resolve(strict=True)
        workspace_info = workspace.lstat()
    except (OSError, RuntimeError) as exc:
        raise TaskAgentIsolationError(
            "Task workspace is unavailable for linked Git discovery"
        ) from exc
    if not stat.S_ISDIR(workspace_info.st_mode):
        raise TaskAgentIsolationError("Task workspace is not a directory")
    pointer_path = workspace / ".git"
    try:
        pointer_info = pointer_path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise TaskAgentIsolationError(
            "Task Git entry is unavailable"
        ) from exc
    if stat.S_ISDIR(pointer_info.st_mode) and not stat.S_ISLNK(pointer_info.st_mode):
        return None
    identity_fingerprint: list[tuple[object, ...]] = []
    pointer_snapshot = _read_stable_git_control_file(
        pointer_path,
        label=".git pointer",
    )
    pointer = pointer_snapshot.text
    identity_fingerprint.append(pointer_snapshot.identity)
    if "\x00" in pointer or not pointer.startswith("gitdir:"):
        raise TaskAgentIsolationError("Task .git pointer is malformed")
    raw_git_dir = pointer[len("gitdir:"):].strip()
    if not raw_git_dir or "\n" in raw_git_dir or "\r" in raw_git_dir:
        raise TaskAgentIsolationError("Task .git pointer is malformed")
    git_dir_path = Path(raw_git_dir).expanduser()
    if not git_dir_path.is_absolute():
        git_dir_path = workspace / git_dir_path
    git_dir = _require_real_git_directory(
        git_dir_path,
        label="per-worktree gitdir",
    )
    if git_dir.parent.name != "worktrees":
        raise TaskAgentIsolationError(
            "Task .git pointer does not target linked-worktree metadata"
        )

    commondir_path = git_dir / "commondir"
    commondir_snapshot = _read_stable_git_control_file(
        commondir_path,
        label="commondir",
    )
    commondir_value = commondir_snapshot.text.strip()
    identity_fingerprint.append(commondir_snapshot.identity)
    if not commondir_value or "\n" in commondir_value or "\r" in commondir_value:
        raise TaskAgentIsolationError("Linked worktree commondir is malformed")
    common_candidate = Path(commondir_value).expanduser()
    if not common_candidate.is_absolute():
        common_candidate = git_dir / common_candidate
    common_dir = _require_real_git_directory(
        common_candidate,
        label="common gitdir",
    )
    if git_dir.parent.parent != common_dir:
        raise TaskAgentIsolationError(
            "Linked worktree gitdir is outside its common metadata root"
        )

    backlink_path = git_dir / "gitdir"
    backlink_snapshot = _read_stable_git_control_file(
        backlink_path,
        label="gitdir backlink",
    )
    backlink = backlink_snapshot.text.strip()
    identity_fingerprint.append(backlink_snapshot.identity)
    backlink_candidate = Path(backlink).expanduser()
    if not backlink_candidate.is_absolute():
        backlink_candidate = git_dir / backlink_candidate
    try:
        backlink_resolved = backlink_candidate.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise TaskAgentIsolationError(
            "Linked worktree gitdir backlink is unavailable"
        ) from exc
    if backlink_resolved != pointer_path:
        raise TaskAgentIsolationError(
            "Linked worktree gitdir backlink targets another workspace"
        )

    head_path = git_dir / "HEAD"
    head_snapshot = _read_stable_git_control_file(
        head_path,
        label="HEAD",
    )
    head_value = head_snapshot.text.strip()
    identity_fingerprint.append(head_snapshot.identity)
    head_ref: str | None = None
    read_paths = {
        str(pointer_path),
        str(head_path),
        str(commondir_path),
        str(git_dir / "index"),
    }
    if head_value.startswith("ref:"):
        head_ref = head_value[len("ref:"):].strip()
        if not _safe_linked_head_ref(head_ref):
            raise TaskAgentIsolationError(
                "Linked worktree HEAD is not a safe local branch ref"
            )
        ref_path = common_dir.joinpath(*head_ref.split("/"))
        try:
            ref_info = ref_path.lstat()
        except FileNotFoundError:
            # An unborn branch is safe only when no packed ref database can
            # silently supply a different shared branch value.
            if (common_dir / "packed-refs").exists():
                raise TaskAgentIsolationError(
                    "Linked worktree branch is not an exact loose ref"
                )
            identity_fingerprint.append(_git_path_identity(ref_path))
        except OSError as exc:
            raise TaskAgentIsolationError(
                "Linked worktree branch ref is unavailable"
            ) from exc
        else:
            if (
                stat.S_ISLNK(ref_info.st_mode)
                or not stat.S_ISREG(ref_info.st_mode)
                or ref_info.st_uid != os.geteuid()
                or ref_path.resolve(strict=True) != ref_path
            ):
                raise TaskAgentIsolationError(
                    "Linked worktree branch ref is not a safe regular file"
                )
            ref_snapshot = _read_stable_git_control_file(
                ref_path,
                label="branch ref",
            )
            if re.fullmatch(
                r"(?:[0-9a-f]{40}|[0-9a-f]{64})\n?",
                ref_snapshot.text,
            ) is None:
                raise TaskAgentIsolationError(
                    "Linked worktree branch ref is malformed"
                )
            identity_fingerprint.append(ref_snapshot.identity)
        read_paths.add(str(ref_path))
    elif re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", head_value) is None:
        raise TaskAgentIsolationError("Linked worktree HEAD is malformed")

    object_dir = _require_real_git_directory(
        common_dir / "objects",
        label="object database",
    )
    read_paths.add(str(object_dir))
    identity_fingerprint.extend((
        _git_path_identity(workspace),
        _git_path_identity(git_dir),
        _git_path_identity(common_dir),
        _git_path_identity(object_dir),
        _git_path_identity(common_dir / "packed-refs"),
    ))
    index_path = git_dir / "index"
    try:
        index_info = index_path.lstat()
    except FileNotFoundError:
        identity_fingerprint.append(_git_path_identity(index_path))
    except OSError as exc:
        raise TaskAgentIsolationError(
            "Linked worktree index is unavailable"
        ) from exc
    else:
        if (
            stat.S_ISLNK(index_info.st_mode)
            or not stat.S_ISREG(index_info.st_mode)
            or index_info.st_uid != os.geteuid()
            or index_path.resolve(strict=True) != index_path
        ):
            raise TaskAgentIsolationError(
                "Linked worktree index is not a safe regular file"
            )
        identity_fingerprint.append(_git_path_identity(index_path))
    shallow_path = common_dir / "shallow"
    try:
        shallow_info = shallow_path.lstat()
    except FileNotFoundError:
        identity_fingerprint.append(_git_path_identity(shallow_path))
    except OSError as exc:
        raise TaskAgentIsolationError(
            "Linked worktree shallow boundary is unavailable"
        ) from exc
    else:
        if (
            stat.S_ISLNK(shallow_info.st_mode)
            or not stat.S_ISREG(shallow_info.st_mode)
            or shallow_info.st_uid != os.geteuid()
            or shallow_path.resolve(strict=True) != shallow_path
        ):
            raise TaskAgentIsolationError(
                "Linked worktree shallow boundary is unsafe"
            )
        read_paths.add(str(shallow_path))
        shallow_snapshot = _read_stable_git_control_file(
            shallow_path,
            label="shallow boundary",
            maximum_bytes=1024 * 1024,
        )
        identity_fingerprint.append(shallow_snapshot.identity)
    return LinkedWorktreeGitReadBoundary(
        git_dir=str(git_dir),
        common_dir=str(common_dir),
        head_ref=head_ref,
        read_paths=tuple(sorted(read_paths)),
        identity_fingerprint=tuple(sorted(identity_fingerprint, key=str)),
    )


def explicit_git_credential_paths(
    git_env: Mapping[str, str] | None,
) -> tuple[str, ...]:
    """Extract the exact credential files authorized by CCM's Git env."""

    values: set[str] = set()
    askpass = (git_env or {}).get("GIT_ASKPASS")
    if askpass and askpass != os.devnull:
        values.add(askpass)
    command = (git_env or {}).get("GIT_SSH_COMMAND")
    if command:
        try:
            parts = shlex.split(command)
        except ValueError as exc:
            raise TaskAgentIsolationError(
                "Task Git SSH command is malformed"
            ) from exc
        index = 0
        while index < len(parts):
            part = parts[index]
            candidate: str | None = None
            if part == "-i" and index + 1 < len(parts):
                index += 1
                candidate = parts[index]
            elif part.startswith("-i") and len(part) > 2:
                candidate = part[2:]
            elif part == "-o" and index + 1 < len(parts):
                option = parts[index + 1]
                if option.lower().startswith("identityfile="):
                    index += 1
                    candidate = option.split("=", 1)[1]
            elif part.lower().startswith("-oidentityfile="):
                candidate = part.split("=", 1)[1]
            if candidate:
                values.add(candidate)
            index += 1
    normalized: set[str] = set()
    for value in values:
        expanded = os.path.abspath(
            os.path.expandvars(os.path.expanduser(value))
        )
        normalized.add(expanded)
        normalized.add(os.path.realpath(expanded))
    return tuple(sorted(normalized))


def _canonical_exact_credential_paths(
    values: Iterable[str],
    *,
    require_private_regular_file: bool,
) -> tuple[str, ...]:
    """Normalize exact files without adding implicit protected roots."""

    paths: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        expanded = os.path.abspath(
            os.path.expandvars(os.path.expanduser(value))
        )
        if expanded == os.path.sep:
            raise TaskAgentIsolationError(
                "Task Git credential read override cannot target filesystem root"
            )
        candidates = {expanded, os.path.realpath(expanded)}
        if require_private_regular_file:
            try:
                info = os.lstat(expanded)
            except OSError as exc:
                raise TaskAgentIsolationError(
                    "Task Git credential is unavailable for exact isolation"
                ) from exc
            if (
                stat.S_ISLNK(info.st_mode)
                or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) & 0o077
            ):
                raise TaskAgentIsolationError(
                    "Task Git credential must be a service-owned, private "
                    "regular file and must not be a symlink"
                )
        paths.update(candidates)
    return tuple(sorted(paths))


def require_git_credentials_outside_protected_paths(
    git_env: Mapping[str, str] | None,
    protected_paths: Iterable[str],
    *,
    allowed_read_paths: Iterable[str] = (),
    non_overridable_paths: Iterable[str] = (),
) -> tuple[str, ...]:
    """Fail before launch when an explicit Git credential is Manager-secret.

    Exempting an exact Profile path would still fail when the credential sits
    below the permanently denied managed-key root. Copying it into a Task
    runtime file would broaden credential handling. Reject the conflicting
    configuration explicitly and keep the stable Manager deny roots intact.
    """

    credentials = _canonical_exact_credential_paths(
        explicit_git_credential_paths(git_env),
        require_private_regular_file=True,
    )
    if not credentials:
        return ()
    boundaries = _canonical_protected_paths(protected_paths)
    allowed = set(_canonical_exact_credential_paths(
        allowed_read_paths,
        require_private_regular_file=True,
    ))
    if not set(credentials).issubset(allowed):
        raise TaskAgentIsolationError(
            "Task Git credentials require exact provider read overrides"
        )
    hard_boundaries = _canonical_protected_paths(non_overridable_paths)
    for credential in credentials:
        for boundary in hard_boundaries:
            try:
                overlaps = os.path.commonpath((credential, boundary)) == boundary
            except ValueError:
                overlaps = False
            if overlaps:
                raise TaskAgentIsolationError(
                    "Task Git credential overlaps a Manager-protected SSH "
                    "Profile/key root; rotate the Project/global Git key to "
                    "a separate path before running this Task"
                )
        # A same-path deny is removed by task_ssh_protected_paths; parent
        # ambient roots remain denied and are overridden only for this exact
        # file by Claude allowRead / Codex longest-path permission mapping.
        if any(
            credential == boundary
            for boundary in boundaries
        ):
            raise TaskAgentIsolationError(
                "Task Git credential has a contradictory exact deny rule"
            )
    return tuple(sorted(allowed))


_SECURITY_TOP_LEVEL_KEYS = {
    "showThinkingSummaries",
    "disableAutoMode",
    "disableAgentView",
    "disableRemoteControl",
    "disableSkillShellExecution",
    "permissions",
    "sandbox",
    "hooks",
}
_MODEL_CREDENTIAL_ENV_KEYS = {
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_BEARER_TOKEN_BEDROCK",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "ANTHROPIC_VERTEX_PROJECT_ID",
    "CLOUD_ML_REGION",
}
_PREFLIGHT_SETTINGS_PATH = "/tmp/ccm-claude-isolation-settings.json"
CLAUDE_SUBPROCESS_ENV_SCRUB = "CLAUDE_CODE_SUBPROCESS_ENV_SCRUB"
_SANDBOX_RUNTIME_PACKAGE_PARTS = (
    "@anthropic-ai",
    "sandbox-runtime",
    "vendor",
    "seccomp",
)
_TASK_GIT_IDENTITY_ENV_KEYS = frozenset({
    "GIT_AUTHOR_NAME",
    "GIT_AUTHOR_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_COMMITTER_EMAIL",
})
_TASK_AMBIENT_CREDENTIAL_ENV_PREFIXES = (
    "GIT_",
    "GH_",
    "GITHUB_",
    "SSH_",
)
_TASK_PARENT_SECRET_ENV_KEYS = frozenset({
    "AUTH_TOKEN",
    "CCM_INTERNAL_SERVICE_TOKEN",
    "CCM_ASK_USER_TOKEN",
    "CCM_TASK_SSH_GUARD",
    "CLAUDECODE",
    "CLAUDE_CODE",
    "DATABASE_URL",
    "OPENAI_API_KEY",
    "FEISHU_APP_SECRET",
    "FEISHU_OAUTH_STATE_SECRET",
    "SMTP_PASSWORD",
    "BACKUP_S3_ACCESS_KEY",
    "BACKUP_S3_SECRET_KEY",
    "BACKUP_OSS_ACCESS_KEY",
    "BACKUP_OSS_SECRET_KEY",
})
_TASK_PROCESS_CORE_ENV_KEYS = frozenset({
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "TEMP",
    "TMP",
    "LANG",
    "LANGUAGE",
    "TERM",
    "COLORTERM",
    "TZ",
    "NO_COLOR",
    "FORCE_COLOR",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "BASH_MAX_TIMEOUT_MS",
})
_CLAUDE_PROVIDER_PROCESS_ENV_KEYS = frozenset({
    *_MODEL_CREDENTIAL_ENV_KEYS,
    "ANTHROPIC_BASE_URL",
    "CLAUDE_CONFIG_DIR",
    "DISABLE_AUTO_COMPACT",
    "MAX_THINKING_TOKENS",
})
_CODEX_PROVIDER_PROCESS_ENV_KEYS = frozenset({
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
    "CODEX_API_KEY",
    "CODEX_HOME",
    "CLOUDROUTER_API_KEY",
    "APEX_CODEX_GATEWAY_KEY",
    "APEX_CODEX_API_KEY",
    "APEXROUTER_API_KEY",
    "APEXROUTER_CODEX_API_KEY",
})


def require_task_security_boundary_configured() -> None:
    """Refuse every model workload while CCM's control plane is unauthenticated.

    Filesystem policy cannot protect Manager secrets when an Agent can call an
    otherwise-open local CCM API over the network.  Keep this admission check
    at every provider-effect boundary so queued/recovered work and auxiliary
    Agents cannot bypass API-time validation.
    """

    from backend.config import settings

    if not isinstance(settings.auth_token, str) or not settings.auth_token.strip():
        raise TaskAgentIsolationError(
            "AUTH_TOKEN must be configured before running Agent workloads"
        )


def prepare_task_working_directory(
    task_id: int,
    task_incarnation_id: str,
    working_directory: str | None,
    *,
    has_explicit_workspace: bool,
) -> str:
    """Keep Task writes outside the live Manager checkout/runtime.

    Legacy project-less Tasks used ``os.getcwd()``, which is often the running
    CCM checkout. Move only that implicit case to a dedicated workspace. An
    explicitly configured Project/target that overlaps trusted runtime is a
    configuration error and must be repaired or materialized as a worktree.
    """

    from backend.config import settings
    from backend.services.trusted_runtime import (
        RUNNING_CCM_CHECKOUT,
        trusted_runtime_protected_roots,
    )

    if (
        isinstance(task_id, bool)
        or not isinstance(task_id, int)
        or task_id <= 0
        or not isinstance(task_incarnation_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", task_incarnation_id) is None
    ):
        raise TaskAgentIsolationError(
            "Task launch requires a valid stable incarnation identity"
        )

    workspace_root = Path(
        os.path.abspath(
            os.path.expandvars(os.path.expanduser(settings.workspace_dir))
        )
    )
    try:
        workspace_root.mkdir(parents=True, exist_ok=True)
        workspace_root = workspace_root.resolve(strict=True)
    except OSError as exc:
        raise TaskAgentIsolationError(
            "WORKSPACE_DIR could not be resolved safely"
        ) from exc
    task_root = workspace_root / ".ccm-task-workspaces"
    expected_workspace = (
        task_root / f"task-{task_id}-{task_incarnation_id}"
    )

    lexical_candidate = Path(
        os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(working_directory or os.getcwd())
            )
        )
    )

    def _inside(path: Path, root: Path) -> bool:
        try:
            return os.path.commonpath((str(path), str(root))) == str(root)
        except ValueError:
            return False

    def _private_directory_fd(
        parent_fd: int,
        name: str,
        *,
        label: str,
    ) -> int:
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
        except FileExistsError:
            pass
        except OSError as exc:
            raise TaskAgentIsolationError(
                f"The private {label} could not be created safely"
            ) from exc
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(name, flags, dir_fd=parent_fd)
        except OSError as exc:
            raise TaskAgentIsolationError(
                f"The private {label} is not a safe directory"
            ) from exc
        info = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            os.close(descriptor)
            raise TaskAgentIsolationError(
                f"The private {label} must be service-owned with mode 0700"
            )
        return descriptor

    def _prepare_private_workspace() -> str:
        root_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            workspace_fd = os.open(workspace_root, root_flags)
        except OSError as exc:
            raise TaskAgentIsolationError(
                "WORKSPACE_DIR is not a safe directory"
            ) from exc
        task_root_fd: int | None = None
        leaf_fd: int | None = None
        try:
            task_root_fd = _private_directory_fd(
                workspace_fd,
                task_root.name,
                label="Task workspace root",
            )
            leaf_fd = _private_directory_fd(
                task_root_fd,
                expected_workspace.name,
                label="Task workspace",
            )
            path_info = expected_workspace.lstat()
            opened_info = os.fstat(leaf_fd)
            if (
                stat.S_ISLNK(path_info.st_mode)
                or not stat.S_ISDIR(path_info.st_mode)
                or path_info.st_dev != opened_info.st_dev
                or path_info.st_ino != opened_info.st_ino
            ):
                raise TaskAgentIsolationError(
                    "The private Task workspace identity changed"
                )
            resolved = expected_workspace.resolve(strict=True)
            if resolved != expected_workspace:
                raise TaskAgentIsolationError(
                    "The private Task workspace must not use symlinks"
                )
            return str(resolved)
        except TaskAgentIsolationError:
            raise
        except OSError as exc:
            raise TaskAgentIsolationError(
                "A safe workspace for the project-less Task could not be created"
            ) from exc
        finally:
            if leaf_fd is not None:
                os.close(leaf_fd)
            if task_root_fd is not None:
                os.close(task_root_fd)
            os.close(workspace_fd)

    # A resumed project-less Task must use the exact incarnation-specific
    # lexical path.  Never resolve a stale/symlinked leaf first and accidentally
    # accept its target as an ordinary external workspace.
    if lexical_candidate == expected_workspace:
        return _prepare_private_workspace()
    candidate_in_private_root = _inside(lexical_candidate, task_root)
    try:
        candidate = lexical_candidate.resolve(strict=False)
    except OSError as exc:
        raise TaskAgentIsolationError(
            "Task working directory could not be resolved safely"
        ) from exc

    def _inside_trusted_root(path: Path) -> bool:
        for raw_root in trusted_runtime_protected_roots():
            root = Path(raw_root)
            if _inside(path, root):
                return True
        return False

    running_checkout = Path(RUNNING_CCM_CHECKOUT)
    managed_worktree_root = (
        running_checkout / ".claude-manager" / "worktrees"
    )
    is_nested_managed_worktree = (
        _inside(lexical_candidate, managed_worktree_root)
        and lexical_candidate != managed_worktree_root
        and _inside(candidate, managed_worktree_root)
        and candidate != managed_worktree_root
    )
    overlaps_live_checkout = (
        _inside(candidate, running_checkout)
        and not is_nested_managed_worktree
    )
    overlaps_boundary = (
        candidate_in_private_root
        or overlaps_live_checkout
        or _inside_trusted_root(candidate)
    )
    if not overlaps_boundary:
        if has_explicit_workspace and not candidate.is_dir():
            raise TaskWorkingDirectoryMissingError(str(candidate))
        return str(candidate)
    if has_explicit_workspace:
        raise TaskAgentIsolationError(
            "Task workspace overlaps the running CCM checkout or trusted "
            "runtime; configure an isolated git worktree"
        )

    safe_workspace = Path(_prepare_private_workspace())
    if (
        _inside_trusted_root(safe_workspace)
        or _inside(safe_workspace, running_checkout)
    ):
        raise TaskAgentIsolationError(
            "WORKSPACE_DIR overlaps the running CCM checkout or trusted runtime"
        )
    return str(safe_workspace)


def require_claude_apply_seccomp(
    claude_binary: str,
) -> Path | None:
    """Resolve the architecture-matched Unix-socket seccomp helper.

    Claude's Linux sandbox deliberately continues with a warning when this
    optional helper is absent, even when ``failIfUnavailable`` is true. CCM's
    policy forbids Unix sockets, so that upstream degradation is not safe for
    a Task process and must become a local preflight failure.
    """

    if platform.system() != "Linux":
        return None
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        architecture = "x64"
    elif machine in {"aarch64", "arm64"}:
        architecture = "arm64"
    else:
        raise TaskAgentIsolationError(
            "Claude Task isolation has no apply-seccomp build for "
            f"architecture {machine or 'unknown'}"
        )

    candidates: list[Path] = []
    direct = shutil.which("apply-seccomp")
    if direct:
        candidates.append(Path(direct))

    npm_roots: set[Path] = {
        Path("/usr/lib/node_modules"),
        Path("/usr/local/lib/node_modules"),
        Path.home() / ".npm-global" / "lib" / "node_modules",
    }
    npm_prefix = os.environ.get("NPM_CONFIG_PREFIX")
    if npm_prefix and os.path.isabs(npm_prefix):
        npm_roots.add(Path(npm_prefix) / "lib" / "node_modules")
    resolved_claude = shutil.which(claude_binary)
    if resolved_claude:
        try:
            claude_path = Path(resolved_claude).resolve(strict=True)
        except OSError:
            claude_path = Path(resolved_claude)
        npm_roots.update(
            parent
            for parent in claude_path.parents
            if parent.name == "node_modules"
        )
    for root in sorted(npm_roots, key=str):
        candidates.append(
            root.joinpath(
                *_SANDBOX_RUNTIME_PACKAGE_PARTS,
                architecture,
                "apply-seccomp",
            )
        )

    for candidate in candidates:
        try:
            info = candidate.lstat()
        except OSError:
            continue
        if (
            stat.S_ISREG(info.st_mode)
            and not stat.S_ISLNK(info.st_mode)
            and info.st_mode & stat.S_IXUSR
            and not info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            return candidate.resolve(strict=True)
    raise TaskAgentIsolationError(
        "Claude Task isolation requires the matching apply-seccomp helper; "
        "install @anthropic-ai/sandbox-runtime globally"
    )


def scrub_task_model_environment(
    source: Mapping[str, str],
    *,
    provider: str,
) -> dict[str, str]:
    """Drop Manager credentials before constructing a Task model process.

    Explicit per-project Git variables are overlaid by the caller afterwards.
    Claude still receives provider credentials needed by the CLI itself; its
    subprocess scrub keeps those credentials out of Bash/hooks/MCP children.
    """

    normalized_provider = (provider or "claude").lower()
    provider_keys = (
        _CLAUDE_PROVIDER_PROCESS_ENV_KEYS
        if normalized_provider == "claude"
        else _CODEX_PROVIDER_PROCESS_ENV_KEYS
        if normalized_provider == "codex"
        else frozenset()
    )
    allowed_keys = _TASK_PROCESS_CORE_ENV_KEYS | provider_keys
    result: dict[str, str] = {}
    for key, value in source.items():
        upper_key = key.upper()
        if upper_key in allowed_keys or upper_key.startswith("LC_"):
            result[key] = value
    if normalized_provider == "claude":
        # Claude 2.1.168 otherwise passes provider/API credentials from its own
        # process environment straight through to Bash subprocesses.
        result[CLAUDE_SUBPROCESS_ENV_SCRUB] = "1"
    return result


def task_model_tool_environment(source: Mapping[str, str]) -> dict[str, str]:
    """Return the non-secret core environment safe for model shell tools."""

    home = Path.home()
    tool_path = os.pathsep.join(
        (
            str(home / ".local" / "bin"),
            str(home / ".cargo" / "bin"),
            os.defpath,
        )
    )
    result = {
        "PATH": tool_path,
        "HOME": str(home),
    }
    for key in ("LANG", "LANGUAGE", "TZ"):
        value = source.get(key)
        if isinstance(value, str) and value:
            result[key] = value
    return result


def _canonical_protected_paths(values: Iterable[str]) -> tuple[str, ...]:
    paths: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        expanded = os.path.expandvars(os.path.expanduser(value))
        if not os.path.isabs(expanded):
            raise TaskAgentIsolationError(
                "Task credential protection requires absolute paths"
            )
        path = os.path.abspath(expanded)
        if path == os.path.sep:
            raise TaskAgentIsolationError(
                "Task credential protection cannot target filesystem root"
            )
        paths.add(path)
        try:
            paths.add(os.path.realpath(path))
        except OSError:
            pass
    runtime_root = str(runtime_secret_root())
    paths.add(runtime_root)
    # These are validated as exact fail-closed boundaries below. The runtime
    # root is materialized before Claude starts, so retaining it beneath a
    # broader deny cannot reproduce the stale missing-leaf mount failure.
    required_exact_paths = {runtime_root}
    if os.name == "posix" and Path("/proc").is_dir():
        paths.add("/proc")
        required_exact_paths.add("/proc")

    # Claude's Linux sandbox materializes each deny entry as a bubblewrap
    # mount target. A redundant missing leaf below an already-denied directory
    # cannot be created once that parent is read-only, which makes every Bash
    # command fail before it starts. Keep the minimal ancestor set instead:
    # denying the parent already protects all existing and future descendants,
    # including stale/missing managed SSH key paths.
    def covered_by(candidate: str, root: str) -> bool:
        try:
            return os.path.commonpath((candidate, root)) == root
        except ValueError:
            # Different Windows drives have no common path.
            return False

    roots: list[str] = []
    for path in sorted(paths, key=lambda value: (len(Path(value).parts), value)):
        if (
            path not in required_exact_paths
            and any(covered_by(path, root) for root in roots)
        ):
            continue
        roots.append(path)
    return tuple(sorted(roots))


def _canonical_exact_filesystem_paths(
    values: Iterable[str],
    *,
    label: str,
) -> tuple[str, ...]:
    """Normalize exact non-root paths without adding ambient deny roots."""

    paths: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise TaskAgentIsolationError(
                f"Claude Task isolation {label} contains an invalid path"
            )
        expanded = os.path.expandvars(os.path.expanduser(value))
        if not os.path.isabs(expanded):
            raise TaskAgentIsolationError(
                f"Claude Task isolation {label} requires absolute paths"
            )
        path = os.path.abspath(expanded)
        if path == os.path.sep:
            raise TaskAgentIsolationError(
                f"Claude Task isolation {label} cannot target filesystem root"
            )
        paths.add(path)
    return tuple(sorted(paths))


def _delivery_git_projection(
    working_directory: str | os.PathLike[str],
    *,
    expected_boundary: LinkedWorktreeGitReadBoundary | None = None,
) -> tuple[LinkedWorktreeGitReadBoundary, tuple[str, ...]]:
    """Discover and validate the immutable Git view for one Delivery turn."""

    boundary = discover_linked_worktree_git_read_boundary(working_directory)
    if boundary is None:
        raise TaskAgentIsolationError(
            "Claude Delivery requires a linked-worktree Git boundary"
        )
    if expected_boundary is not None and boundary != expected_boundary:
        raise TaskAgentIsolationError(
            "Claude Delivery linked-worktree Git boundary changed before launch"
        )
    try:
        workspace = Path(working_directory).expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise TaskAgentIsolationError(
            "Claude Delivery workspace is unavailable"
        ) from exc
    pointer = str(workspace / ".git")
    deny_roots = _canonical_exact_filesystem_paths(
        (pointer, boundary.git_dir, boundary.common_dir),
        label="Delivery Git deny projection",
    )
    read_paths = _canonical_exact_filesystem_paths(
        boundary.read_paths,
        label="Delivery Git read projection",
    )
    if pointer not in read_paths:
        raise TaskAgentIsolationError(
            "Claude Delivery Git projection lost its worktree pointer"
        )
    for path in read_paths:
        if path == pointer:
            continue
        try:
            covered = any(
                os.path.commonpath((path, root)) == root
                for root in (boundary.git_dir, boundary.common_dir)
            )
        except ValueError:
            covered = False
        if not covered:
            raise TaskAgentIsolationError(
                "Claude Delivery Git read projection escaped its metadata roots"
            )
    return boundary, deny_roots


def _delivery_private_tmp_projection(
    private_tmpdir: str | os.PathLike[str],
) -> tuple[str, str]:
    """Prove one service-owned 0700 scratch leaf and its private parent."""

    try:
        lexical = Path(os.path.abspath(
            os.path.expandvars(os.path.expanduser(os.fspath(private_tmpdir)))
        ))
        scratch = lexical.resolve(strict=True)
        scratch_info = lexical.lstat()
        root = scratch.parent
        root_info = root.lstat()
    except (OSError, RuntimeError) as exc:
        raise TaskAgentIsolationError(
            "Claude Delivery private TMPDIR is unavailable"
        ) from exc
    if (
        lexical != scratch
        or scratch == root
        or root == Path(os.path.sep)
        or stat.S_ISLNK(scratch_info.st_mode)
        or not stat.S_ISDIR(scratch_info.st_mode)
        or scratch_info.st_uid != os.geteuid()
        or stat.S_IMODE(scratch_info.st_mode) != 0o700
        or stat.S_ISLNK(root_info.st_mode)
        or not stat.S_ISDIR(root_info.st_mode)
        or root_info.st_uid != os.geteuid()
        or stat.S_IMODE(root_info.st_mode) != 0o700
    ):
        raise TaskAgentIsolationError(
            "Claude Delivery private TMPDIR must be a service-owned 0700 leaf"
        )
    return str(root), str(scratch)


def _permission_path(path: str) -> str:
    return f"//{path.lstrip('/')}"


def _mcp_allow_rules() -> list[str]:
    # CLAUDE_CODE_SUBPROCESS_ENV_SCRUB makes Claude 2.1.168 enforce its
    # explicit allow rules in effective ``default`` mode. Keep the complete
    # CCM-owned inventory here: ``--strict-mcp-config`` still exposes only the
    # private per-turn servers, while an omitted rule would open an invisible
    # interactive permission prompt in headless/PTTY execution.
    servers = {
        "ccm_skills": CCM_SKILLS_TOOLS,
        "ccm_ssh": CCM_SSH_TOOLS,
        "ccm_monitor_agent": CCM_MONITOR_AGENT_TOOLS,
        "ccm_sub_agent": CCM_SUB_AGENT_TOOLS,
        "ccm_frontend_review": CCM_FRONTEND_REVIEW_TOOLS,
        "ccm_workspace_review": CCM_WORKSPACE_REVIEW_TOOLS,
        "ccm_browser_review": CCM_BROWSER_REVIEW_TOOLS,
    }
    return [
        f"mcp__{server}__{tool}"
        for server, tools in servers.items()
        for tool in tools
    ]


def claude_permission_allow_rules(
    builtin_tools: Iterable[str] = CLAUDE_TASK_BUILTIN_TOOLS,
    *,
    include_mcp_tools: bool = True,
) -> tuple[str, ...]:
    """Return exact permission rules separately from Claude's tool inventory.

    ``--tools`` accepts built-in tool names, while ``permissions.allow`` and
    ``--allowedTools`` also accept MCP matchers.  Keeping this union public
    lets direct and PTY launchers share one permission policy without putting
    ``mcp__...`` rules into the built-in tool inventory.
    """

    selected_tools = tuple(builtin_tools)
    if (
        any(
            not isinstance(tool, str) or not tool or "," in tool
            for tool in selected_tools
        )
        or len(selected_tools) != len(set(selected_tools))
    ):
        raise TaskAgentIsolationError(
            "Claude Task permissions require a unique exact tool allowlist"
        )
    return (
        *selected_tools,
        *(tuple(_mcp_allow_rules()) if include_mcp_tools else ()),
    )


def _generate_claude_isolation_settings(
    *,
    namespace: str,
    identifier: int,
    filename: str,
    protected_paths: Iterable[str],
    allowed_read_paths: Iterable[str] = (),
    ssh_capabilities: Iterable[str] = (),
    disable_direct_network: bool = False,
    include_task_hooks: bool,
    builtin_tools: Iterable[str],
    include_mcp_tools: bool = True,
    read_only_deny_paths: Iterable[str] = (),
    read_only_allow_paths: Iterable[str] = (),
    allowed_write_paths: Iterable[str] | None = None,
) -> Path:
    from backend.config import settings
    from backend.services.ask_user_settings import (
        ask_user_hook_entry,
        task_ssh_guard_hook_entry,
    )
    from backend.services.trusted_runtime import (
        materialize_trusted_python_asset,
    )

    credential_paths = _canonical_protected_paths(protected_paths)
    credential_read_overrides = _canonical_exact_credential_paths(
        allowed_read_paths,
        require_private_regular_file=True,
    )
    read_only_denies = _canonical_exact_filesystem_paths(
        read_only_deny_paths,
        label="read-only deny projection",
    )
    read_only_overrides = _canonical_exact_filesystem_paths(
        read_only_allow_paths,
        label="read-only allow projection",
    )
    write_overrides = (
        None
        if allowed_write_paths is None
        else _canonical_exact_filesystem_paths(
            allowed_write_paths,
            label="write allow projection",
        )
    )
    write_denies = tuple(sorted({*credential_paths, *read_only_denies}))
    read_denies = write_denies
    read_overrides = tuple(sorted({
        *credential_read_overrides,
        *read_only_overrides,
    }))
    if set(credential_paths) & set(read_only_denies):
        raise TaskAgentIsolationError(
            "Claude Task isolation cannot classify a credential path as a "
            "read-only projection"
        )
    immutable_denies = _canonical_protected_paths(())
    if any(
        path == os.path.sep
        or not os.path.isabs(path)
        or any(
            os.path.commonpath((path, boundary)) == boundary
            for boundary in immutable_denies
        )
        for path in read_overrides
    ):
        raise TaskAgentIsolationError(
            "Claude Task Git credential read overrides must be exact paths"
        )
    selected_tools = tuple(builtin_tools)
    permission_allow_rules = claude_permission_allow_rules(
        selected_tools,
        include_mcp_tools=include_mcp_tools,
    )
    capabilities = set(ssh_capabilities) & {"exec", "read", "write"}
    hooks = []
    if include_task_hooks and settings.ask_user_enabled:
        ask_user_script = materialize_trusted_python_asset(
            "ask_user_hook",
            namespace=namespace,
            identifier=identifier,
        )
        hooks.append(ask_user_hook_entry(script_path=ask_user_script))
    if include_task_hooks and (capabilities or disable_direct_network):
        guard_script = materialize_trusted_python_asset(
            "task_ssh_guard_hook",
            namespace=namespace,
            identifier=identifier,
        )
        hooks.append(
            task_ssh_guard_hook_entry(
                read_denies,
                script_path=guard_script,
            )
        )

    permission_denies: list[str] = []
    for path in read_denies:
        rule_path = _permission_path(path)
        permission_denies.extend((
            f"Read({rule_path})",
            f"Read({rule_path}/**)",
            f"Edit({rule_path})",
            f"Edit({rule_path}/**)",
        ))

    filesystem: dict[str, object] = {
        "denyRead": list(read_denies),
        "denyWrite": list(write_denies),
        # Claude 2.1.168 documents allowRead as an exact re-allow
        # inside denyRead regions. Parent credential roots remain
        # denied; only the selected Git key/askpass is readable by
        # the git subprocess for this turn.
        "allowRead": list(read_overrides),
    }
    if write_overrides is not None:
        filesystem["allowWrite"] = list(write_overrides)

    payload: dict[str, object] = {
        "showThinkingSummaries": True,
        "disableAutoMode": "disable",
        "disableAgentView": True,
        "disableRemoteControl": True,
        "disableSkillShellExecution": True,
        "permissions": {
            "defaultMode": "acceptEdits",
            "disableBypassPermissionsMode": "disable",
            "allow": list(permission_allow_rules),
            "deny": permission_denies,
        },
        "sandbox": {
            "enabled": True,
            "failIfUnavailable": True,
            "autoAllowBashIfSandboxed": True,
            "allowUnsandboxedCommands": False,
            "excludedCommands": [],
            "filesystem": filesystem,
            "credentials": {
                "files": [
                    {"path": path, "mode": "deny"}
                    for path in credential_paths
                ],
            },
            "network": {
                "strictAllowlist": True,
                "allowedDomains": (
                    [] if disable_direct_network or capabilities else ["*"]
                ),
                "deniedDomains": [],
                "allowAllUnixSockets": False,
                "allowLocalBinding": False,
            },
        },
    }
    if hooks:
        payload["hooks"] = {"PreToolUse": hooks}
    return write_private_json(
        namespace,
        identifier,
        filename,
        payload,
    )


def generate_claude_task_isolation_settings(
    task_id: int,
    protected_paths: Iterable[str],
    *,
    allowed_read_paths: Iterable[str] = (),
    read_only_allow_paths: Iterable[str] = (),
    ssh_capabilities: Iterable[str] = (),
    disable_direct_network: bool = False,
) -> Path:
    """Write exact CLI settings for one direct Claude Task turn.

    Account/project/local settings are disabled separately on argv. These
    settings therefore cannot be weakened by a repository-controlled
    ``allowRead`` entry or an ambient hook/plugin/MCP server.
    """

    return _generate_claude_isolation_settings(
        namespace="task",
        identifier=task_id,
        filename="claude-security.json",
        protected_paths=protected_paths,
        allowed_read_paths=allowed_read_paths,
        read_only_allow_paths=read_only_allow_paths,
        ssh_capabilities=ssh_capabilities,
        disable_direct_network=disable_direct_network,
        include_task_hooks=True,
        builtin_tools=CLAUDE_TASK_BUILTIN_TOOLS,
    )


def generate_claude_unrestricted_task_settings(
    task_id: int,
    *,
    turn_generation: int,
    builtin_tools: Iterable[str] = CLAUDE_UNRESTRICTED_PERMISSION_TOOLS,
) -> Path:
    """Write one private permission profile for an unrestricted admin turn.

    Claude 2.1.168 can report effective ``default`` mode when subprocess
    credential scrubbing is enabled even though the launcher requested bypass
    mode.  Exact built-in and CCM MCP allow rules prevent that compatibility
    behavior from opening an invisible interactive permission dialog.  This
    profile deliberately contains no filesystem or network sandbox; role and
    execution-mode admission remain the launcher's responsibility.
    """

    if (
        isinstance(turn_generation, bool)
        or not isinstance(turn_generation, int)
        or turn_generation < 0
    ):
        raise ValueError("Claude unrestricted Task turn generation is invalid")

    from backend.config import settings
    from backend.services.ask_user_settings import ask_user_hook_entry
    from backend.services.trusted_runtime import materialize_trusted_python_asset

    selected_tools = tuple(builtin_tools)
    allow_rules = claude_permission_allow_rules(selected_tools)
    payload: dict[str, object] = {
        "showThinkingSummaries": True,
        "disableAutoMode": "disable",
        "disableAgentView": True,
        "disableRemoteControl": True,
        "disableSkillShellExecution": True,
        "permissions": {
            "defaultMode": "bypassPermissions",
            "allow": list(allow_rules),
            "deny": [],
        },
    }
    if settings.ask_user_enabled:
        ask_user_script = materialize_trusted_python_asset(
            "ask_user_hook",
            namespace="task",
            identifier=task_id,
        )
        payload["hooks"] = {
            "PreToolUse": [
                ask_user_hook_entry(script_path=ask_user_script)
            ]
        }
    # Keep one stable, atomically-replaced profile per Task.  A hot PTY
    # session retains the Task scope for its complete native-process lifetime;
    # using the turn generation in the filename would otherwise accumulate
    # one stale owner-only file per chat turn until that session is released.
    return write_private_json(
        "task",
        task_id,
        "claude-unrestricted-security.json",
        payload,
    )


def generate_claude_delivery_isolation_settings(
    task_id: int,
    protected_paths: Iterable[str],
    *,
    working_directory: str | os.PathLike[str],
    private_tmpdir: str | os.PathLike[str],
) -> tuple[Path, LinkedWorktreeGitReadBoundary]:
    """Write the networkless, no-MCP policy for a Delivery Developer turn.

    Git control metadata is broadly denied for both reads and writes. Only the
    exact linked-worktree projection proven by
    :func:`discover_linked_worktree_git_read_boundary` is re-opened for
    sandboxed subprocess reads, so ``git status/diff/log`` can work while the
    model cannot commit, reset, update refs, inspect config/hooks, or push.
    """

    boundary, deny_roots = _delivery_git_projection(working_directory)
    workspace = str(Path(working_directory).expanduser().resolve(strict=True))
    scratch_root, scratch = _delivery_private_tmp_projection(private_tmpdir)
    settings_path = _generate_claude_isolation_settings(
        namespace="task",
        identifier=task_id,
        filename="claude-delivery-security.json",
        protected_paths=protected_paths,
        allowed_read_paths=(),
        ssh_capabilities=(),
        disable_direct_network=True,
        include_task_hooks=False,
        builtin_tools=CLAUDE_DELIVERY_BUILTIN_TOOLS,
        include_mcp_tools=False,
        read_only_deny_paths=(*deny_roots, scratch_root),
        read_only_allow_paths=(*boundary.read_paths, scratch),
        allowed_write_paths=(workspace, scratch),
    )
    return settings_path, boundary


def generate_claude_aux_isolation_settings(
    *,
    namespace: str,
    identifier: int,
    protected_paths: Iterable[str],
    turn_generation: int | None = None,
    disable_direct_network: bool = False,
) -> Path:
    """Write exact settings for a Monitor or Sub-Agent Claude child.

    Auxiliary children use their own scoped MCP callback and never inherit the
    main Task's AskUser/SSH hooks.  A parent Task with managed SSH grants also
    disables direct child networking, so the grant cannot be bypassed through
    an independently launched child.
    """

    if namespace not in {"monitor", "sub-agent"}:
        raise ValueError("Unsupported Claude auxiliary isolation namespace")
    filename = (
        f"claude-security-{turn_generation}.json"
        if turn_generation is not None
        else "claude-security.json"
    )
    return _generate_claude_isolation_settings(
        namespace=namespace,
        identifier=identifier,
        filename=filename,
        protected_paths=protected_paths,
        allowed_read_paths=(),
        ssh_capabilities=(),
        disable_direct_network=disable_direct_network,
        include_task_hooks=False,
        builtin_tools=(
            CLAUDE_MONITOR_BUILTIN_TOOLS
            if namespace == "monitor"
            else CLAUDE_SUB_AGENT_BUILTIN_TOOLS
        ),
    )


def generate_claude_zero_tool_isolation_settings(
    namespace: str,
    identifier: int,
    protected_paths: Iterable[str],
) -> Path:
    """Write an exact no-tool/no-MCP/no-network policy for text-only Agents."""

    return _generate_claude_isolation_settings(
        namespace=namespace,
        identifier=identifier,
        filename="claude-zero-tool-security.json",
        protected_paths=protected_paths,
        allowed_read_paths=(),
        ssh_capabilities=(),
        disable_direct_network=True,
        include_task_hooks=False,
        builtin_tools=(),
        include_mcp_tools=False,
    )


def generate_claude_read_only_isolation_settings(
    namespace: str,
    identifier: int,
    protected_paths: Iterable[str],
) -> Path:
    """Write a no-MCP/no-network policy with only repository read tools."""

    return _generate_claude_isolation_settings(
        namespace=namespace,
        identifier=identifier,
        filename="claude-read-only-security.json",
        protected_paths=protected_paths,
        allowed_read_paths=(),
        ssh_capabilities=(),
        disable_direct_network=True,
        include_task_hooks=False,
        builtin_tools=CLAUDE_READ_ONLY_BUILTIN_TOOLS,
        include_mcp_tools=False,
    )


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _require_exact_keys(value: object, expected: set[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != expected:
        raise TaskAgentIsolationError(
            f"Claude Task isolation {label} has an unexpected shape"
        )
    return value


def validate_claude_unrestricted_task_settings(
    settings_path: Path,
    *,
    builtin_tools: Iterable[str] = CLAUDE_UNRESTRICTED_PERMISSION_TOOLS,
) -> None:
    """Validate one CCM-owned unrestricted permission profile without CLI I/O."""

    try:
        info = settings_path.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ValueError("settings path is not a private regular file")
        payload = json.loads(
            settings_path.read_text(encoding="utf-8"),
            object_pairs_hook=_strict_json_object,
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise TaskAgentIsolationError(
            "Claude unrestricted Task settings are not a private strict JSON file"
        ) from exc

    from backend.config import settings

    require_ask_user_hook = bool(settings.ask_user_enabled)
    expected_top_level = {
        "showThinkingSummaries",
        "disableAutoMode",
        "disableAgentView",
        "disableRemoteControl",
        "disableSkillShellExecution",
        "permissions",
    }
    if require_ask_user_hook:
        expected_top_level.add("hooks")
    if not isinstance(payload, dict) or set(payload) != expected_top_level:
        raise TaskAgentIsolationError(
            "Claude unrestricted Task settings have an unexpected shape"
        )
    exact_values = {
        "showThinkingSummaries": True,
        "disableAutoMode": "disable",
        "disableAgentView": True,
        "disableRemoteControl": True,
        "disableSkillShellExecution": True,
    }
    if any(payload.get(key) != value for key, value in exact_values.items()):
        raise TaskAgentIsolationError(
            "Claude unrestricted Task settings weaken autonomous controls"
        )

    selected_tools = tuple(builtin_tools)
    expected_allow = list(claude_permission_allow_rules(selected_tools))
    permissions = _require_exact_keys(
        payload.get("permissions"),
        {"defaultMode", "allow", "deny"},
        "unrestricted permissions",
    )
    if (
        permissions["defaultMode"] != "bypassPermissions"
        or permissions["allow"] != expected_allow
        or permissions["deny"] != []
    ):
        raise TaskAgentIsolationError(
            "Claude unrestricted Task permission policy is not exact"
        )

    if not require_ask_user_hook:
        return
    from backend.services.ask_user_settings import ask_user_hook_entry
    from backend.services.trusted_runtime import (
        trusted_python_asset_filename,
        verify_materialized_trusted_python_asset,
    )

    hooks = _require_exact_keys(payload.get("hooks"), {"PreToolUse"}, "hooks")
    ask_user_script = (
        settings_path.parent / trusted_python_asset_filename("ask_user_hook")
    )
    verify_materialized_trusted_python_asset(
        "ask_user_hook",
        ask_user_script,
    )
    if hooks["PreToolUse"] != [
        ask_user_hook_entry(script_path=ask_user_script)
    ]:
        raise TaskAgentIsolationError(
            "Claude unrestricted Task AskUser hook is not the CCM-owned entry"
        )


def _validate_claude_security_contract(
    settings_path: Path,
    *,
    expected_tools: tuple[str, ...],
    include_mcp_tools: bool = True,
    expected_read_only_denies: tuple[str, ...] = (),
    expected_allow_read: tuple[str, ...] | None = None,
    expected_allow_write: tuple[str, ...] | None = None,
    require_network_disabled: bool = False,
    require_no_hooks: bool = False,
) -> None:
    """Validate the exact CCM-owned settings contract without trusting CLI."""

    try:
        info = settings_path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise ValueError("settings path is not a regular file")
        if stat.S_IMODE(info.st_mode) & 0o077:
            raise ValueError("settings file is not private")
        payload = json.loads(
            settings_path.read_text(encoding="utf-8"),
            object_pairs_hook=_strict_json_object,
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise TaskAgentIsolationError(
            "Claude Task isolation settings are not a private strict JSON file"
        ) from exc

    if not isinstance(payload, dict):
        raise TaskAgentIsolationError(
            "Claude Task isolation settings must be a JSON object"
        )
    allowed_top_level = set(_SECURITY_TOP_LEVEL_KEYS)
    if "hooks" not in payload:
        allowed_top_level.remove("hooks")
    if set(payload) != allowed_top_level:
        raise TaskAgentIsolationError(
            "Claude Task isolation settings contain unexpected fields"
        )
    exact_values = {
        "showThinkingSummaries": True,
        "disableAutoMode": "disable",
        "disableAgentView": True,
        "disableRemoteControl": True,
        "disableSkillShellExecution": True,
    }
    if any(payload.get(key) != value for key, value in exact_values.items()):
        raise TaskAgentIsolationError(
            "Claude Task isolation settings weaken autonomous feature controls"
        )

    permissions = _require_exact_keys(
        payload.get("permissions"),
        {"defaultMode", "disableBypassPermissionsMode", "allow", "deny"},
        "permissions",
    )
    if (
        permissions["defaultMode"] != "acceptEdits"
        or permissions["disableBypassPermissionsMode"] != "disable"
        or permissions["allow"] != [
            *expected_tools,
            *(_mcp_allow_rules() if include_mcp_tools else []),
        ]
    ):
        raise TaskAgentIsolationError(
            "Claude Task isolation permission policy is not exact"
        )

    sandbox = _require_exact_keys(
        payload.get("sandbox"),
        {
            "enabled",
            "failIfUnavailable",
            "autoAllowBashIfSandboxed",
            "allowUnsandboxedCommands",
            "excludedCommands",
            "filesystem",
            "credentials",
            "network",
        },
        "sandbox",
    )
    if (
        sandbox["enabled"] is not True
        or sandbox["failIfUnavailable"] is not True
        or sandbox["autoAllowBashIfSandboxed"] is not True
        or sandbox["allowUnsandboxedCommands"] is not False
        or sandbox["excludedCommands"] != []
    ):
        raise TaskAgentIsolationError(
            "Claude Task isolation sandbox fail-closed policy is not exact"
        )
    expected_filesystem_keys = {"denyRead", "denyWrite", "allowRead"}
    if expected_allow_write is not None:
        expected_filesystem_keys.add("allowWrite")
    filesystem = _require_exact_keys(
        sandbox.get("filesystem"),
        expected_filesystem_keys,
        "sandbox filesystem",
    )
    paths = filesystem["denyRead"]
    read_only_denies = _canonical_exact_filesystem_paths(
        expected_read_only_denies,
        label="validated read-only deny projection",
    )
    if (
        not isinstance(paths, list)
        or not paths
        or paths != sorted(set(paths))
        or filesystem["denyWrite"] != paths
        or not isinstance(filesystem["allowRead"], list)
        or filesystem["allowRead"] != sorted(set(filesystem["allowRead"]))
        or any(
            not isinstance(path, str)
            or not os.path.isabs(path)
            or path == os.path.sep
            for path in filesystem["allowRead"]
        )
        or any(
            not isinstance(path, str)
            or not os.path.isabs(path)
            or path == os.path.sep
            for path in paths
        )
        or str(runtime_secret_root()) not in paths
        or (
            os.name == "posix"
            and Path("/proc").is_dir()
            and "/proc" not in paths
        )
        or not set(read_only_denies).issubset(paths)
        or (
            expected_allow_read is not None
            and filesystem["allowRead"]
            != list(_canonical_exact_filesystem_paths(
                expected_allow_read,
                label="validated read-only allow projection",
            ))
        )
        or (
            expected_allow_write is not None
            and filesystem["allowWrite"]
            != list(_canonical_exact_filesystem_paths(
                expected_allow_write,
                label="validated write allow projection",
            ))
        )
    ):
        raise TaskAgentIsolationError(
            "Claude Task isolation protected filesystem paths are not exact"
        )
    expected_denies = [
        rule
        for path in paths
        for rule in (
            f"Read({_permission_path(path)})",
            f"Read({_permission_path(path)}/**)",
            f"Edit({_permission_path(path)})",
            f"Edit({_permission_path(path)}/**)",
        )
    ]
    if permissions["deny"] != expected_denies:
        raise TaskAgentIsolationError(
            "Claude Task isolation permission denies do not match the sandbox"
        )
    credentials = _require_exact_keys(
        sandbox.get("credentials"),
        {"files"},
        "sandbox credentials",
    )
    credential_paths = [
        path for path in paths if path not in set(read_only_denies)
    ]
    if credentials["files"] != [
        {"path": path, "mode": "deny"} for path in credential_paths
    ]:
        raise TaskAgentIsolationError(
            "Claude Task isolation credential denies are not exact"
        )
    network = _require_exact_keys(
        sandbox.get("network"),
        {
            "strictAllowlist",
            "allowedDomains",
            "deniedDomains",
            "allowAllUnixSockets",
            "allowLocalBinding",
        },
        "sandbox network",
    )
    if (
        network["strictAllowlist"] is not True
        or network["allowedDomains"] not in ([], ["*"])
        or (require_network_disabled and network["allowedDomains"] != [])
        or network["deniedDomains"] != []
        or network["allowAllUnixSockets"] is not False
        or network["allowLocalBinding"] is not False
    ):
        raise TaskAgentIsolationError(
            "Claude Task isolation network policy is not exact"
        )

    hooks = payload.get("hooks")
    if require_no_hooks and hooks is not None:
        raise TaskAgentIsolationError(
            "Claude Task isolation unexpectedly includes hooks"
        )
    if hooks is not None:
        from backend.services.ask_user_settings import (
            ask_user_hook_entry,
            task_ssh_guard_hook_entry,
        )
        from backend.services.trusted_runtime import (
            trusted_python_asset_filename,
            verify_materialized_trusted_python_asset,
        )

        hooks = _require_exact_keys(hooks, {"PreToolUse"}, "hooks")
        entries = hooks["PreToolUse"]
        ask_user_script = (
            settings_path.parent
            / trusted_python_asset_filename("ask_user_hook")
        )
        allowed_entries = []
        if ask_user_script.exists():
            verify_materialized_trusted_python_asset(
                "ask_user_hook",
                ask_user_script,
            )
            allowed_entries.append(
                ask_user_hook_entry(script_path=ask_user_script)
            )
        if network["allowedDomains"] == []:
            guard_script = (
                settings_path.parent
                / trusted_python_asset_filename("task_ssh_guard_hook")
            )
            verify_materialized_trusted_python_asset(
                "task_ssh_guard_hook",
                guard_script,
            )
            allowed_entries.append(
                task_ssh_guard_hook_entry(
                    tuple(paths),
                    script_path=guard_script,
                )
            )
        if (
            not isinstance(entries, list)
            or not entries
            or len(entries) != len({json.dumps(entry, sort_keys=True) for entry in entries})
            or any(entry not in allowed_entries for entry in entries)
        ):
            raise TaskAgentIsolationError(
                "Claude Task isolation hooks are not CCM-owned exact entries"
            )


def _zero_usage(value: object) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if key.endswith(("_tokens", "_requests")):
                if isinstance(item, bool) or not isinstance(item, (int, float)):
                    return False
                if item != 0:
                    return False
            elif isinstance(item, (dict, list)) and not _zero_usage(item):
                return False
        return True
    if isinstance(value, list):
        return all(_zero_usage(item) for item in value)
    return True


def _validate_zero_turn_probe(result: subprocess.CompletedProcess[str]) -> None:
    if result.returncode != 0:
        raise TaskAgentIsolationError(
            "Claude Task isolation zero-turn probe failed"
        )
    try:
        events = [
            json.loads(line)
            for line in (result.stdout or "").splitlines()
            if line.strip()
        ]
    except (TypeError, json.JSONDecodeError) as exc:
        raise TaskAgentIsolationError(
            "Claude Task isolation zero-turn probe returned invalid output"
        ) from exc
    # Claude 2.1.168 exits successfully without emitting a result event when
    # stream-json stdin reaches EOF before the first user message. That is the
    # intended zero-turn path: model credentials are absent and the outer
    # network namespace makes a provider request impossible. Older builds emit
    # an explicit zero-usage result, which is verified below when present.
    if not events:
        return
    results = [
        event
        for event in events
        if isinstance(event, dict) and event.get("type") == "result"
    ]
    if len(results) != 1:
        raise TaskAgentIsolationError(
            "Claude Task isolation zero-turn probe was not conclusive"
        )
    final = results[0]
    usage = final.get("usage")
    if (
        final.get("subtype") != "success"
        or final.get("is_error") is not False
        or final.get("num_turns") != 0
        or final.get("total_cost_usd") not in (0, 0.0)
        or not isinstance(usage, dict)
        or usage.get("input_tokens") != 0
        or usage.get("output_tokens") != 0
        or not _zero_usage(usage)
    ):
        raise TaskAgentIsolationError(
            "Claude Task isolation preflight unexpectedly executed a model turn"
        )


def _validate_sandbox_loading_canary(
    result: subprocess.CompletedProcess[str],
) -> None:
    combined = "\n".join((result.stdout or "", result.stderr or "")).lower()
    compact = combined.replace(".", "")
    # Claude 2.1.168 fails before emitting stream-json when PATH deliberately
    # hides bwrap.  Older builds emitted a zero-turn result and named
    # failIfUnavailable.  Both are valid fail-closed outcomes: the local
    # contract above has already verified the exact settings file and the CLI
    # is invoked with --settings plus an empty setting source list.
    recognized_unavailable = bool(
        (
            "sandbox required but unavailable" in combined
            and "failifunavailable" in compact
        )
        or (
            "bubblewrap is required for subprocess env scrubbing and isolation"
            in combined
            and "install" in combined
        )
    )
    if (
        result.returncode == 0
        or not recognized_unavailable
    ):
        raise TaskAgentIsolationError(
            "Claude did not prove that sandbox.failIfUnavailable was loaded"
        )
    try:
        events = [
            json.loads(line)
            for line in (result.stdout or "").splitlines()
            if line.strip().startswith("{")
        ]
    except json.JSONDecodeError as exc:
        raise TaskAgentIsolationError(
            "Claude sandbox loading canary returned invalid output"
        ) from exc
    results = [
        event
        for event in events
        if isinstance(event, dict) and event.get("type") == "result"
    ]
    if not results:
        # Current Claude exits before the stream-json renderer is initialized.
        # The outer bwrap has no network and stdin is /dev/null, so this path
        # cannot have crossed a provider boundary or consumed a model turn.
        return
    if len(results) != 1:
        raise TaskAgentIsolationError(
            "Claude sandbox loading canary did not prove zero turns"
        )
    final = results[0]
    usage = final.get("usage")
    if (
        final.get("is_error") is not True
        or final.get("num_turns") != 0
        or final.get("total_cost_usd") not in (0, 0.0)
        or not isinstance(usage, dict)
        or usage.get("input_tokens") != 0
        or usage.get("output_tokens") != 0
        or not _zero_usage(usage)
    ):
        raise TaskAgentIsolationError(
            "Claude sandbox loading canary unexpectedly executed a model turn"
        )


def validate_claude_task_isolation_settings(
    settings_path: Path,
    *,
    claude_binary: str,
    tools: Iterable[str] = CLAUDE_TASK_BUILTIN_TOOLS,
    timeout_seconds: float = 5.0,
    include_mcp_tools: bool = True,
    _expected_read_only_denies: tuple[str, ...] = (),
    _expected_allow_read: tuple[str, ...] | None = None,
    _expected_allow_write: tuple[str, ...] | None = None,
    _require_network_disabled: bool = False,
    _require_no_hooks: bool = False,
) -> None:
    """Strictly validate settings, then run a networkless zero-turn CLI probe."""

    selected_tools = tuple(tools)
    if any(
        not isinstance(tool, str) or not tool or "," in tool
        for tool in selected_tools
    ):
        raise TaskAgentIsolationError(
            "Claude Task isolation requires an exact tool allowlist"
        )
    _validate_claude_security_contract(
        settings_path,
        expected_tools=selected_tools,
        include_mcp_tools=include_mcp_tools,
        expected_read_only_denies=_expected_read_only_denies,
        expected_allow_read=_expected_allow_read,
        expected_allow_write=_expected_allow_write,
        require_network_disabled=_require_network_disabled,
        require_no_hooks=_require_no_hooks,
    )
    resolved_claude = shutil.which(claude_binary)
    bubblewrap = shutil.which("bwrap")
    socat = shutil.which("socat")
    if not resolved_claude or not bubblewrap or not socat:
        raise TaskAgentIsolationError(
            "Claude Task isolation requires claude, bubblewrap, and socat"
        )
    require_claude_apply_seccomp(resolved_claude)

    env = {
        key: value
        for key, value in os.environ.items()
        if key.upper() not in {
            "AUTH_TOKEN",
            "CCM_INTERNAL_SERVICE_TOKEN",
            "CCM_ASK_USER_TOKEN",
            "CLAUDECODE",
            "CLAUDE_CODE",
            *_MODEL_CREDENTIAL_ENV_KEYS,
        }
    }
    env[CLAUDE_SUBPROCESS_ENV_SCRUB] = "1"
    claude_argv = [
        resolved_claude,
        "--bare",
        "--settings",
        _PREFLIGHT_SETTINGS_PATH,
        "--setting-sources",
        "",
        "--strict-mcp-config",
        "--permission-mode",
        "acceptEdits",
        "--disable-slash-commands",
        "--no-chrome",
        "--tools",
        ",".join(selected_tools),
        "--allowedTools",
        ",".join(selected_tools),
        "--no-session-persistence",
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    outer_argv = [
        bubblewrap,
        "--unshare-net",
        "--die-with-parent",
        "--ro-bind",
        os.path.sep,
        os.path.sep,
        "--dev-bind",
        "/dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--ro-bind",
        str(settings_path),
        _PREFLIGHT_SETTINGS_PATH,
        "--chdir",
        os.path.sep,
        "--",
        *claude_argv,
    ]
    deadline = time.monotonic() + timeout_seconds

    def run_probe(probe_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(outer_argv, timeout_seconds)
        return subprocess.run(
            outer_argv,
            cwd=os.path.sep,
            env=probe_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=remaining,
            check=False,
        )

    try:
        canary_env = dict(env)
        canary_env["PATH"] = "/nonexistent/ccm-claude-sandbox-canary"
        _validate_sandbox_loading_canary(run_probe(canary_env))
        result = run_probe(env)
    except (OSError, subprocess.SubprocessError) as exc:
        raise TaskAgentIsolationError(
            "Claude Task isolation settings could not be validated"
        ) from exc
    _validate_zero_turn_probe(result)


def validate_claude_delivery_isolation_settings(
    settings_path: Path,
    *,
    claude_binary: str,
    working_directory: str | os.PathLike[str],
    private_tmpdir: str | os.PathLike[str],
    expected_git_boundary: LinkedWorktreeGitReadBoundary,
    timeout_seconds: float = 5.0,
) -> None:
    """Re-prove the Delivery Git identity and validate its exact policy."""

    current_boundary, deny_roots = _delivery_git_projection(
        working_directory,
        expected_boundary=expected_git_boundary,
    )
    workspace = str(Path(working_directory).expanduser().resolve(strict=True))
    scratch_root, scratch = _delivery_private_tmp_projection(private_tmpdir)
    validate_claude_task_isolation_settings(
        settings_path,
        claude_binary=claude_binary,
        tools=CLAUDE_DELIVERY_BUILTIN_TOOLS,
        timeout_seconds=timeout_seconds,
        include_mcp_tools=False,
        _expected_read_only_denies=(*deny_roots, scratch_root),
        _expected_allow_read=(*current_boundary.read_paths, scratch),
        _expected_allow_write=(workspace, scratch),
        _require_network_disabled=True,
        _require_no_hooks=True,
    )


def validate_claude_zero_tool_isolation_settings(
    settings_path: Path,
    *,
    claude_binary: str,
    timeout_seconds: float = 5.0,
) -> None:
    """Validate a text-only Claude policy with no tool or MCP authority."""

    validate_claude_task_isolation_settings(
        settings_path,
        claude_binary=claude_binary,
        tools=(),
        timeout_seconds=timeout_seconds,
        include_mcp_tools=False,
    )
