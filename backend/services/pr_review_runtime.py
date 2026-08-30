"""Runtime isolation helpers for GitHub PR review tasks.

PR review tasks inspect a remote repository through ``gh``.  They must not
inherit project instructions from the CCM checkout merely because the backend
process happens to run there.
"""

from __future__ import annotations

import os
import secrets
import stat
from pathlib import Path


PR_REVIEW_TAG = "pr-review"
PR_REVIEW_FIX_TAG = "pr-review-fix"
PRE_PR_CODE_REVIEW_TAG = "pre-pr-code-review"
# v3 extends the tool-free sandbox contract to ``pr-review-fix`` tasks.  A
# v2 Worker only understands the original ``pr-review`` tag and must therefore
# fail closed instead of accepting a fix task with inherited tools.
PR_REVIEW_SNAPSHOT_CONTEXT_VERSION = 3
PR_REVIEW_TERMINAL_CHAT_VERSION = 1
PR_REVIEW_RUNTIME_DIR_ENV = "CCM_PR_REVIEW_RUNTIME_DIR"
PR_REVIEW_TERMINAL_CHAT_HEADER = "X-CCM-PR-Review-Terminal-Chat"
PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE = "confirmed"
_PR_REVIEW_CWD_PREFIX = "ccm-pr-review-"
_REVIEW_CONTEXT_DOC_NAMES = ("CLAUDE.md", "AGENTS.md", "PROGRESS.md")
_CREATE_ATTEMPTS = 32


def _has_tag(task: object, tag: str) -> bool:
    tags = getattr(task, "tags", None)
    return isinstance(tags, (list, tuple, set, dict)) and tag in tags


def _has_int_metadata_marker(task: object, key: str) -> bool:
    metadata = getattr(task, "metadata_", None)
    return isinstance(metadata, dict) and type(metadata.get(key)) is int


def is_pr_review_task(task: object) -> bool:
    """Return whether a Task belongs to the automated review workflow.

    Manager Tasks retain the durable ``pr_review_id`` marker even if their
    presentation tags are edited through an older client.  Worker mirrors do
    not receive Manager metadata, so the preserved tag remains a deliberate
    fail-closed compatibility marker there.
    """

    return _has_tag(task, PR_REVIEW_TAG) or _has_int_metadata_marker(
        task,
        "pr_review_id",
    )


def is_pr_review_fix_task(task: object) -> bool:
    """Return whether a Task is an automated finding-repair generation."""

    return _has_tag(task, PR_REVIEW_FIX_TAG) or _has_int_metadata_marker(
        task,
        "pr_finding_action_id",
    )


def is_pre_pr_code_review_task(task: object) -> bool:
    """Return whether a Task is a Controller-owned pre-PR review turn.

    The tag survives on older Worker mirrors, while Manager rows retain the
    exact Capability/Run identity tuple even if a legacy client edits tags.
    Either representation must preserve the sandbox boundary.
    """

    if _has_tag(task, PRE_PR_CODE_REVIEW_TAG):
        return True
    metadata = getattr(task, "metadata_", None)
    return bool(
        isinstance(metadata, dict)
        and type(metadata.get("code_review_run_id")) is int
        and type(metadata.get("capability_invocation_id")) is int
        and type(metadata.get("capability_execution_id")) is int
    )


def is_pr_sandbox_task(task: object) -> bool:
    """Return whether a review/fix Task needs the tool-free runtime boundary."""

    return (
        is_pr_review_task(task)
        or is_pr_review_fix_task(task)
        or is_pre_pr_code_review_task(task)
    )


def _trusted_runtime_anchor() -> Path:
    """Return the user-owned boundary that protects review runtime paths."""

    return Path(os.path.abspath(os.fspath(Path.home().expanduser())))


def _configured_runtime_root() -> tuple[Path, Path]:
    """Resolve the managed root without following any filesystem symlinks."""

    anchor = _trusted_runtime_anchor()
    configured = os.environ.get(PR_REVIEW_RUNTIME_DIR_ENV)
    root = (
        Path(configured).expanduser()
        if configured
        else anchor / ".cache" / "ccm" / "pr-review-runtime"
    )
    if not root.is_absolute():
        raise RuntimeError(
            f"{PR_REVIEW_RUNTIME_DIR_ENV} must be an absolute path"
        )
    root = Path(os.path.abspath(os.fspath(root)))

    try:
        relative = root.relative_to(anchor)
    except ValueError as exc:
        raise RuntimeError(
            f"{PR_REVIEW_RUNTIME_DIR_ENV} must be below the current user's "
            "trusted home directory"
        ) from exc
    if not relative.parts:
        raise RuntimeError(
            f"{PR_REVIEW_RUNTIME_DIR_ENV} cannot be the trusted home directory"
        )
    return root, anchor


def _has_review_context_doc_in_ancestry(
    path: Path,
    *,
    stop_at: Path | None = None,
) -> bool:
    for directory in (path, *path.parents):
        for name in _REVIEW_CONTEXT_DOC_NAMES:
            if os.path.lexists(directory / name):
                return True
        if stop_at is not None and directory == stop_at:
            break
    return False


def _validate_posix_directory(
    metadata: os.stat_result,
    *,
    exact_private_mode: bool,
) -> None:
    getuid = getattr(os, "getuid", None)
    if getuid is None:
        raise RuntimeError("Cannot verify PR review runtime ownership")
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError("PR review runtime path is not a directory")
    if metadata.st_uid != getuid():
        raise RuntimeError("PR review runtime path is not owned by CCM")
    mode = stat.S_IMODE(metadata.st_mode)
    if exact_private_mode:
        if mode != 0o700:
            raise RuntimeError("PR review runtime directory must have mode 0700")
    elif mode & 0o022:
        raise RuntimeError(
            "PR review runtime ancestry must not be group/world writable"
        )


def _open_posix_runtime_root(root: Path, anchor: Path) -> int:
    """Securely create/open the root relative to its trusted home anchor."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)

    try:
        anchor_metadata = anchor.lstat()
    except OSError as exc:
        raise RuntimeError(
            f"Cannot inspect PR review trusted home directory: {exc}"
        ) from exc
    if anchor.is_symlink():
        raise RuntimeError("PR review trusted home directory cannot be a symlink")
    _validate_posix_directory(anchor_metadata, exact_private_mode=False)

    try:
        current_fd = os.open(anchor, flags)
    except OSError as exc:
        raise RuntimeError(
            f"Cannot open PR review trusted home directory: {exc}"
        ) from exc

    relative_parts = root.relative_to(anchor).parts
    try:
        for index, part in enumerate(relative_parts):
            created = False
            try:
                child_fd = os.open(part, flags, dir_fd=current_fd)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=current_fd)
                    created = True
                    child_fd = os.open(part, flags, dir_fd=current_fd)
                except OSError as exc:
                    raise RuntimeError(
                        f"Cannot create private PR review runtime root: {exc}"
                    ) from exc
            except OSError as exc:
                raise RuntimeError(
                    f"Cannot securely open PR review runtime ancestry: {exc}"
                ) from exc

            try:
                metadata = os.fstat(child_fd)
                is_root = index == len(relative_parts) - 1
                _validate_posix_directory(
                    metadata,
                    exact_private_mode=is_root or created,
                )
            except BaseException:
                os.close(child_fd)
                raise
            os.close(current_fd)
            current_fd = child_fd

        root_metadata = os.fstat(current_fd)
        path_metadata = root.lstat()
        if (
            root_metadata.st_dev,
            root_metadata.st_ino,
        ) != (
            path_metadata.st_dev,
            path_metadata.st_ino,
        ):
            raise RuntimeError(
                "PR review runtime root changed while it was being verified"
            )
        if _has_review_context_doc_in_ancestry(root, stop_at=anchor):
            raise RuntimeError(
                "PR review runtime ancestry contains "
                "CLAUDE.md/AGENTS.md/PROGRESS.md"
            )
        return current_fd
    except BaseException:
        os.close(current_fd)
        raise


def _prepare_runtime_root() -> tuple[Path, int | None]:
    root, anchor = _configured_runtime_root()
    if os.name == "posix":
        return root, _open_posix_runtime_root(root, anchor)

    # Windows ACLs do not map reliably to POSIX uid/mode checks.  Keep the
    # managed root below the user's home, reject reparse-point symlinks, and
    # avoid the system-wide temporary directory.
    anchor.mkdir(parents=True, exist_ok=True)
    root.mkdir(parents=True, exist_ok=True)
    for directory in (root, *root.parents):
        if directory.is_symlink() or not directory.is_dir():
            raise RuntimeError(
                "PR review runtime ancestry must contain directories only"
            )
        if directory == anchor:
            break
    if _has_review_context_doc_in_ancestry(root, stop_at=anchor):
        raise RuntimeError(
            "PR review runtime ancestry contains "
            "CLAUDE.md/AGENTS.md/PROGRESS.md"
        )
    return root, None


def _is_reusable_review_cwd(path: str | None, task_id: int) -> bool:
    if not isinstance(path, str) or not path:
        return False
    candidate = Path(path)
    if not candidate.is_absolute():
        return False
    if not candidate.name.startswith(f"{_PR_REVIEW_CWD_PREFIX}{task_id}-"):
        return False

    try:
        root, root_fd = _prepare_runtime_root()
    except (OSError, RuntimeError, ValueError):
        return False
    if candidate.parent != root:
        if root_fd is not None:
            os.close(root_fd)
        return False

    if root_fd is not None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        child_fd: int | None = None
        try:
            child_fd = os.open(candidate.name, flags, dir_fd=root_fd)
            metadata = os.fstat(child_fd)
            _validate_posix_directory(metadata, exact_private_mode=True)
            path_metadata = candidate.lstat()
            if (
                metadata.st_dev,
                metadata.st_ino,
            ) != (
                path_metadata.st_dev,
                path_metadata.st_ino,
            ):
                return False
            for name in _REVIEW_CONTEXT_DOC_NAMES:
                try:
                    os.stat(name, dir_fd=child_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                except OSError:
                    return False
                return False
            return True
        except (OSError, RuntimeError):
            return False
        finally:
            if child_fd is not None:
                os.close(child_fd)
            os.close(root_fd)

    try:
        metadata = candidate.lstat()
    except OSError:
        return False
    return (
        stat.S_ISDIR(metadata.st_mode)
        and not candidate.is_symlink()
        and not _has_review_context_doc_in_ancestry(
            candidate,
            stop_at=root,
        )
    )


def _create_private_review_cwd(root: Path, root_fd: int | None, task_id: int) -> Path:
    prefix = f"{_PR_REVIEW_CWD_PREFIX}{task_id}-"
    for _ in range(_CREATE_ATTEMPTS):
        name = f"{prefix}{secrets.token_hex(8)}"
        candidate = root / name
        try:
            if root_fd is None:
                candidate.mkdir(mode=0o700)
                if candidate.is_symlink() or not candidate.is_dir():
                    raise RuntimeError(
                        "Created PR review runtime path is not a directory"
                    )
                return candidate

            os.mkdir(name, mode=0o700, dir_fd=root_fd)
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            child_fd = os.open(name, flags, dir_fd=root_fd)
            try:
                metadata = os.fstat(child_fd)
                _validate_posix_directory(metadata, exact_private_mode=True)
                path_metadata = candidate.lstat()
                if (
                    metadata.st_dev,
                    metadata.st_ino,
                ) != (
                    path_metadata.st_dev,
                    path_metadata.st_ino,
                ):
                    raise RuntimeError(
                        "PR review task directory changed during creation"
                    )
            finally:
                os.close(child_fd)
            return candidate
        except FileExistsError:
            continue
    raise RuntimeError("Unable to allocate a unique PR review task directory")


def isolated_pr_review_cwd(task: object) -> str:
    """Return a task-private cwd whose ancestry has no project agent docs.

    A valid previous cwd is reused so provider-native resume keeps the same
    directory.  Unsafe/stale values (including the CCM checkout used by older
    tasks) are deliberately ignored and replaced with a fresh secure temp dir.
    """

    task_id = getattr(task, "id", None)
    if type(task_id) is not int or task_id <= 0:
        raise RuntimeError("PR review task must have a persisted positive id")

    previous = getattr(task, "last_cwd", None)
    if _is_reusable_review_cwd(previous, task_id):
        return previous

    root, root_fd = _prepare_runtime_root()
    try:
        cwd = _create_private_review_cwd(root, root_fd, task_id)
    finally:
        if root_fd is not None:
            os.close(root_fd)

    if not _is_reusable_review_cwd(str(cwd), task_id):
        try:
            cwd.rmdir()
        except OSError:
            pass
        raise RuntimeError(
            "Unable to create an isolated PR review directory in the trusted "
            "private runtime root"
        )
    return str(cwd)
