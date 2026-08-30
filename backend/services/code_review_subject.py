"""Capture and re-verify immutable Git material for pre-PR code review.

The working tree is used only as an admission gate (it must be clean and not
mid-operation).  Review bytes themselves are read from Git's object database
at two explicit, full commit IDs.  In particular, this module never follows a
changed path through the filesystem.
"""

from __future__ import annotations

import base64
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass
from typing import Final, Iterator, Mapping, TypeAlias

from backend.services.structured_code_review import CommitRangeSubject


Pathish: TypeAlias = str | os.PathLike[str]

PATCH_HASH_DESCRIPTION: Final = "sha256(raw git-diff stdout bytes)"
PATCH_DIFF_ARGUMENTS: Final[tuple[str, ...]] = (
    "diff",
    "--no-ext-diff",
    "--no-textconv",
    "--binary",
    "--full-index",
    "--find-renames",
    "--diff-algorithm=myers",
    "--no-indent-heuristic",
    "--unified=3",
)
MAX_STRUCTURED_PROMPT_SECTION_BYTES: Final = 2 * 1024 * 1024
MAX_PATCH_BYTES: Final = 1024 * 1024
MAX_CHANGED_PATH_DATA_BYTES: Final = 256 * 1024
MAX_TREE_DATA_BYTES: Final = 2 * 1024 * 1024
MAX_REVIEW_FILE_BYTES: Final = 256 * 1024
MAX_REVIEW_FILE_BYTES_TOTAL: Final = 768 * 1024
MAX_GUIDANCE_FILE_BYTES: Final = 64 * 1024
MAX_GUIDANCE_BYTES_TOTAL: Final = 128 * 1024
MAX_GIT_PATH_BYTES: Final = 4096
MAX_SMALL_GIT_STDOUT_BYTES: Final = 64 * 1024
MAX_GIT_STDERR_BYTES: Final = 64 * 1024
GIT_COMMAND_TIMEOUT_SECONDS: Final = 60.0

_SHA1_RE = re.compile(r"[0-9a-f]{40}\Z")
_STATUS_RE = re.compile(rb"[A-Z](?:[0-9]{1,3})?\Z")
_OPERATION_MARKERS: Final[tuple[str, ...]] = (
    "MERGE_HEAD",
    "CHERRY_PICK_HEAD",
    "REVERT_HEAD",
    "BISECT_START",
    "rebase-merge",
    "rebase-apply",
    "sequencer",
)
_GUIDANCE_NAMES: Final[tuple[bytes, ...]] = (b"AGENTS.md", b"CLAUDE.md")


class CodeReviewSubjectError(ValueError):
    """The repository cannot safely produce the requested review subject."""


class GitCommandError(CodeReviewSubjectError):
    """A read-only Git command failed or returned malformed output."""


class RepositoryStateError(CodeReviewSubjectError):
    """The repository is dirty, moving, or in an intermediate operation."""


class SubjectChangedError(CodeReviewSubjectError):
    """A captured immutable subject no longer matches the repository HEAD."""


@dataclass(frozen=True, slots=True)
class GitPath:
    """A repository-relative Git path with lossless prompt serialization."""

    raw: bytes

    def __post_init__(self) -> None:
        _validate_git_path(self.raw)

    @property
    def utf8(self) -> str | None:
        try:
            return self.raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return None

    @property
    def display(self) -> str:
        # ``backslashreplace`` is readable while ``base64`` below remains the
        # source of truth for invalid UTF-8 and control-heavy names.
        return self.raw.decode("utf-8", errors="backslashreplace")

    def as_material(self) -> dict[str, object]:
        return {
            "display": self.display,
            "utf8": self.utf8,
            "base64": base64.b64encode(self.raw).decode("ascii"),
        }


@dataclass(frozen=True, slots=True)
class ChangedPath:
    status: str
    path: GitPath
    old_path: GitPath | None = None

    def as_material(self) -> dict[str, object]:
        return {
            "status": self.status,
            "path": self.path.as_material(),
            "old_path": (
                None if self.old_path is None else self.old_path.as_material()
            ),
        }


@dataclass(frozen=True, slots=True)
class CapturedGitObject:
    """One exact object-db entry, optionally including bounded blob bytes."""

    path: GitPath
    mode: str
    object_type: str
    object_id: str
    byte_length: int | None
    content_sha256: str | None
    content: bytes | None
    content_kind: str
    omitted_reason: str | None = None

    def as_material(self) -> dict[str, object]:
        value: dict[str, object] = {
            "path": self.path.as_material(),
            "mode": self.mode,
            "object_type": self.object_type,
            "object_id": self.object_id,
            "byte_length": self.byte_length,
            "content_sha256": self.content_sha256,
            "content_kind": self.content_kind,
            "omitted_reason": self.omitted_reason,
            "content": None,
        }
        if self.content is None:
            return value
        if self.content_kind in {"utf-8", "symlink_target_utf-8"}:
            value["content"] = {
                "encoding": "utf-8",
                "text": self.content.decode("utf-8", errors="strict"),
            }
        else:
            value["content"] = {
                "encoding": "base64",
                "data": base64.b64encode(self.content).decode("ascii"),
            }
        return value


@dataclass(frozen=True, slots=True)
class CapturedCommitRangeSubject:
    """Immutable subject plus bounded, object-db-derived review material."""

    repo_path: str
    base_sha: str
    head_sha: str
    head_tree_sha: str
    patch_sha256: str
    patch: bytes
    head_ref: str | None
    detached_head: bool
    changed_paths: tuple[ChangedPath, ...]
    files: tuple[CapturedGitObject, ...]
    guidance: tuple[CapturedGitObject, ...]

    @property
    def subject(self) -> CommitRangeSubject:
        return CommitRangeSubject(
            base_sha=self.base_sha,
            head_sha=self.head_sha,
            head_tree_sha=self.head_tree_sha,
            patch_sha256=self.patch_sha256,
        )

    def prompt_material(self) -> dict[str, object]:
        """Return JSON-safe, lossless material for the structured reviewer."""

        if hashlib.sha256(self.patch).hexdigest() != self.patch_sha256:
            raise SubjectChangedError(
                "captured patch bytes do not match the exact subject"
            )

        patch_payload: dict[str, object]
        try:
            patch_text = self.patch.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            patch_payload = {
                "encoding": "base64",
                "data": base64.b64encode(self.patch).decode("ascii"),
            }
        else:
            patch_payload = {"encoding": "utf-8", "text": patch_text}
        material = {
            "subject": self.subject.as_dict(),
            "patch_hash_semantics": PATCH_HASH_DESCRIPTION,
            "patch_byte_length": len(self.patch),
            "patch_payload": patch_payload,
            "changed_paths": [item.as_material() for item in self.changed_paths],
            "head_files": [item.as_material() for item in self.files],
        }
        _assert_prompt_section_fits(material, "review material")
        return material

    def prompt_guidance(self) -> dict[str, object]:
        guidance = {
            "source": "exact head commit object database",
            "subject": self.subject.as_dict(),
            "documents": [item.as_material() for item in self.guidance],
        }
        _assert_prompt_section_fits(guidance, "review guidance")
        return guidance


@dataclass(frozen=True, slots=True)
class _TreeEntry:
    mode: str
    object_type: str
    object_id: str
    path: GitPath


@dataclass(frozen=True, slots=True)
class _HeadState:
    sha: str
    ref: str | None
    detached: bool


@dataclass(frozen=True, slots=True)
class _ObjectDatabaseView:
    """An attribute/config-isolated Git directory over immutable objects."""

    git_dir: Path
    worktree_root: Path
    attr_source: str


def _assert_prompt_section_fits(value: object, label: str) -> None:
    try:
        rendered = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CodeReviewSubjectError(f"{label} is not safely serializable") from exc
    if len(rendered) > MAX_STRUCTURED_PROMPT_SECTION_BYTES:
        raise CodeReviewSubjectError(
            f"{label} exceeds the structured review prompt limit"
        )


def _git_environment(
    *,
    object_view: _ObjectDatabaseView | None = None,
) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_COMMON_DIR",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        }
        and not key.startswith("GIT_CONFIG_KEY_")
        and not key.startswith("GIT_CONFIG_VALUE_")
        and key != "GIT_CONFIG_COUNT"
    }
    env.update(
        {
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_COUNT": "2",
            "GIT_CONFIG_KEY_0": "core.fsmonitor",
            "GIT_CONFIG_VALUE_0": "false",
            "GIT_CONFIG_KEY_1": "core.attributesFile",
            "GIT_CONFIG_VALUE_1": os.devnull,
            "LC_ALL": "C",
        }
    )
    if object_view is not None:
        env.update(
            {
                "GIT_DIR": os.fspath(object_view.git_dir),
                "GIT_ATTR_SOURCE": object_view.attr_source,
            }
        )
    return env


def _terminate_git_process(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "posix" and proc.pid > 1:
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=1)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        if os.name == "posix" and proc.pid > 1:
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        # There is no safe result to consume, but leave the original failure
        # intact if the platform cannot reap an already-killed process.
        pass


def _read_bounded_git_output(
    proc: subprocess.Popen[bytes],
    *,
    max_stdout_bytes: int,
    timeout: float,
) -> tuple[bytes, bytes]:
    """Drain both pipes without ever retaining an unbounded Git response."""

    if proc.stdout is None or proc.stderr is None:
        raise RuntimeError("Git process pipes were not configured")
    stdout = bytearray()
    stderr = bytearray()
    stdout_exceeded = False
    deadline = time.monotonic() + timeout
    selector = selectors.DefaultSelector()
    try:
        selector.register(proc.stdout, selectors.EVENT_READ, "stdout")
        selector.register(proc.stderr, selectors.EVENT_READ, "stderr")
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _terminate_git_process(proc)
                raise GitCommandError("Git command timed out")
            events = selector.select(min(remaining, 0.25))
            if not events:
                if proc.poll() is not None:
                    # EOF may trail process exit by one selector iteration.
                    continue
                continue
            for key, _mask in events:
                try:
                    chunk = os.read(key.fd, 64 * 1024)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                if key.data == "stdout":
                    retain = max_stdout_bytes + 1 - len(stdout)
                    if retain > 0:
                        stdout.extend(chunk[:retain])
                    if len(stdout) > max_stdout_bytes:
                        stdout_exceeded = True
                else:
                    retain = MAX_GIT_STDERR_BYTES + 1 - len(stderr)
                    if retain > 0:
                        stderr.extend(chunk[:retain])
                if stdout_exceeded:
                    _terminate_git_process(proc)
                    raise GitCommandError(
                        f"Git stdout exceeds the {max_stdout_bytes}-byte limit"
                    )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            _terminate_git_process(proc)
            raise GitCommandError("Git command timed out")
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            _terminate_git_process(proc)
            raise GitCommandError("Git command timed out") from exc
    except BaseException:
        _terminate_git_process(proc)
        raise
    finally:
        selector.close()
        proc.stdout.close()
        proc.stderr.close()
    if len(stderr) > MAX_GIT_STDERR_BYTES:
        del stderr[MAX_GIT_STDERR_BYTES:]
    return bytes(stdout), bytes(stderr)


def _run_git(
    repo: Path,
    args: tuple[str, ...] | list[str],
    *,
    allowed_returncodes: frozenset[int] = frozenset({0}),
    max_stdout_bytes: int = MAX_SMALL_GIT_STDOUT_BYTES,
    object_view: _ObjectDatabaseView | None = None,
) -> subprocess.CompletedProcess[bytes]:
    if (
        not isinstance(max_stdout_bytes, int)
        or isinstance(max_stdout_bytes, bool)
        or max_stdout_bytes < 0
    ):
        raise ValueError("max_stdout_bytes must be a non-negative integer")
    argv = ["git", "-C", os.fspath(repo), *args]
    try:
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            close_fds=True,
            start_new_session=(os.name == "posix"),
            env=_git_environment(object_view=object_view),
        )
        stdout, stderr = _read_bounded_git_output(
            proc,
            max_stdout_bytes=max_stdout_bytes,
            timeout=GIT_COMMAND_TIMEOUT_SECONDS,
        )
    except GitCommandError:
        raise
    except OSError as exc:
        raise GitCommandError(f"Git command could not run: {args[0]}") from exc
    result = subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)
    if result.returncode not in allowed_returncodes:
        detail = result.stderr[:1000].decode("utf-8", errors="replace").strip()
        suffix = f": {detail}" if detail else ""
        raise GitCommandError(
            f"Git command failed ({args[0]}, rc={result.returncode}){suffix}"
        )
    return result


def _canonical_repo_path(repo: Pathish) -> Path:
    try:
        candidate = Path(repo).resolve(strict=True)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise CodeReviewSubjectError("repository path is invalid") from exc
    if not candidate.is_dir():
        raise CodeReviewSubjectError("repository path is not a directory")

    inside = _run_git(
        candidate,
        ["rev-parse", "--is-inside-work-tree"],
        max_stdout_bytes=16,
    )
    if inside.stdout.strip() != b"true":
        raise CodeReviewSubjectError("repository is not a Git working tree")
    top = _run_git(
        candidate,
        ["rev-parse", "--show-toplevel"],
        max_stdout_bytes=MAX_GIT_PATH_BYTES + 2,
    ).stdout
    try:
        top_path = Path(os.fsdecode(top.rstrip(b"\r\n"))).resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise CodeReviewSubjectError("Git returned an invalid worktree root") from exc
    if not top_path.is_dir():
        raise CodeReviewSubjectError("Git worktree root is not a directory")
    return top_path


def _git_path(repo: Path, name: str) -> Path:
    raw = _run_git(
        repo,
        ["rev-parse", "--git-path", name],
        max_stdout_bytes=MAX_GIT_PATH_BYTES + 2,
    ).stdout.rstrip(b"\r\n")
    if not raw or b"\x00" in raw or b"\n" in raw or b"\r" in raw:
        raise GitCommandError("Git returned an invalid metadata path")
    value = os.fsdecode(raw)
    path = Path(value) if os.path.isabs(value) else repo / value
    try:
        return path.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise GitCommandError("Git returned an unavailable metadata path") from exc


@contextmanager
def _isolated_object_database(
    repo: Path,
    head_sha: str,
) -> Iterator[_ObjectDatabaseView]:
    """Expose repository objects without mutable repository Git metadata.

    The temporary bare Git directory has no ``info/attributes`` and no local
    diff-driver configuration.  Global and system attributes/configuration are
    disabled by :func:`_git_environment`; committed ``.gitattributes`` are read
    from the exact head tree via ``GIT_ATTR_SOURCE``.
    """

    object_format = _run_git(
        repo,
        ["rev-parse", "--show-object-format"],
        max_stdout_bytes=16,
    ).stdout.strip()
    if object_format != b"sha1":
        raise CodeReviewSubjectError("only SHA-1 Git object databases are supported")
    objects_path = _git_path(repo, "objects")
    if not objects_path.is_dir():
        raise GitCommandError("Git object database is not a directory")
    raw_objects_path = os.fsencode(objects_path)
    if b"\x00" in raw_objects_path or b"\n" in raw_objects_path or b"\r" in raw_objects_path:
        raise CodeReviewSubjectError("Git object database path is unsupported")

    try:
        with tempfile.TemporaryDirectory(prefix="ccm-review-object-db-") as temp:
            git_dir = Path(temp) / "git"
            (git_dir / "objects" / "info").mkdir(parents=True, mode=0o700)
            (git_dir / "refs" / "heads").mkdir(parents=True, mode=0o700)
            (git_dir / "info").mkdir(mode=0o700)
            # A detached exact HEAD is a compatibility fallback for Git
            # versions that predate GIT_ATTR_SOURCE.
            (git_dir / "HEAD").write_bytes(head_sha.encode("ascii") + b"\n")
            (git_dir / "config").write_bytes(
                b"[core]\n"
                b"\trepositoryformatversion = 0\n"
                b"\tbare = true\n"
            )
            (git_dir / "objects" / "info" / "alternates").write_bytes(
                raw_objects_path + b"\n"
            )
            yield _ObjectDatabaseView(
                git_dir=git_dir,
                worktree_root=repo,
                attr_source=head_sha,
            )
    except CodeReviewSubjectError:
        raise
    except OSError as exc:
        raise GitCommandError("could not create isolated Git object view") from exc


def _validate_sha(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA1_RE.fullmatch(value) is None:
        raise CodeReviewSubjectError(f"{field} must be a full lowercase 40-byte SHA")
    return value


def _resolve_exact_commit(
    repo: Path,
    sha: str,
    field: str,
    *,
    object_view: _ObjectDatabaseView | None = None,
) -> str:
    resolved = _run_git(
        repo,
        ["rev-parse", "--verify", "--end-of-options", f"{sha}^{{commit}}"],
        max_stdout_bytes=64,
        object_view=object_view,
    ).stdout.strip()
    try:
        result = resolved.decode("ascii")
    except UnicodeDecodeError as exc:
        raise GitCommandError(f"Git returned an invalid {field}") from exc
    if _SHA1_RE.fullmatch(result) is None or result != sha:
        raise CodeReviewSubjectError(f"{field} does not name that exact commit")
    return result


def _current_head(repo: Path) -> str:
    raw = _run_git(
        repo,
        ["rev-parse", "--verify", "--end-of-options", "HEAD^{commit}"],
        max_stdout_bytes=64,
    ).stdout.strip()
    try:
        value = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise GitCommandError("Git returned an invalid HEAD") from exc
    return _validate_sha(value, "HEAD")


def _tree_sha(
    repo: Path,
    head_sha: str,
    *,
    object_view: _ObjectDatabaseView | None = None,
) -> str:
    raw = _run_git(
        repo,
        ["rev-parse", "--verify", "--end-of-options", f"{head_sha}^{{tree}}"],
        max_stdout_bytes=64,
        object_view=object_view,
    ).stdout.strip()
    try:
        value = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise GitCommandError("Git returned an invalid tree SHA") from exc
    return _validate_sha(value, "head tree")


def _operation_marker_path(repo: Path, marker: str) -> str:
    raw = _run_git(
        repo,
        ["rev-parse", "--git-path", marker],
        max_stdout_bytes=MAX_GIT_PATH_BYTES + 2,
    ).stdout.rstrip(b"\r\n")
    value = os.fsdecode(raw)
    if not os.path.isabs(value):
        value = os.path.join(os.fspath(repo), value)
    return value


def _assert_stable_worktree(repo: Path) -> None:
    for marker in _OPERATION_MARKERS:
        marker_path = _operation_marker_path(repo, marker)
        if os.path.lexists(marker_path):
            raise RepositoryStateError(
                f"repository is in an intermediate Git operation ({marker})"
            )
    status = _run_git(
        repo,
        [
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ],
        max_stdout_bytes=MAX_CHANGED_PATH_DATA_BYTES,
    ).stdout
    if status:
        raise RepositoryStateError(
            "repository must be clean, including tracked and untracked files"
        )


def _head_ref(repo: Path) -> tuple[str | None, bool]:
    result = _run_git(
        repo,
        ["symbolic-ref", "--quiet", "--short", "HEAD"],
        allowed_returncodes=frozenset({0, 1}),
        max_stdout_bytes=MAX_GIT_PATH_BYTES + 2,
    )
    if result.returncode == 1:
        return None, True
    try:
        value = result.stdout.rstrip(b"\r\n").decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise GitCommandError("Git returned a non-UTF-8 branch name") from exc
    if not value:
        raise GitCommandError("Git returned an empty branch name")
    return value, False


def _head_state(repo: Path) -> _HeadState:
    sha_before = _current_head(repo)
    head_ref, detached = _head_ref(repo)
    sha_after = _current_head(repo)
    if sha_after != sha_before:
        raise SubjectChangedError("repository HEAD changed while it was inspected")
    return _HeadState(sha=sha_before, ref=head_ref, detached=detached)


def _assert_ancestor(
    repo: Path,
    base_sha: str,
    head_sha: str,
    *,
    object_view: _ObjectDatabaseView | None = None,
) -> None:
    result = _run_git(
        repo,
        ["merge-base", "--is-ancestor", base_sha, head_sha],
        allowed_returncodes=frozenset({0, 1}),
        max_stdout_bytes=0,
        object_view=object_view,
    )
    if result.returncode == 1:
        raise CodeReviewSubjectError("base commit is not an ancestor of HEAD")


def _validate_git_path(raw: object) -> bytes:
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_GIT_PATH_BYTES:
        raise GitCommandError("Git returned an invalid repository path")
    if b"\x00" in raw or raw.startswith(b"/"):
        raise GitCommandError("Git returned an unsafe repository path")
    if any(part in {b"", b".", b".."} for part in raw.split(b"/")):
        raise GitCommandError("Git returned an unsafe repository path")
    return raw


def _parse_changed_paths(raw: bytes) -> tuple[ChangedPath, ...]:
    if not raw:
        return ()
    if not raw.endswith(b"\x00"):
        raise GitCommandError("Git returned unterminated changed-path data")
    fields = raw[:-1].split(b"\x00")
    result: list[ChangedPath] = []
    index = 0
    while index < len(fields):
        status_raw = fields[index]
        index += 1
        if _STATUS_RE.fullmatch(status_raw) is None:
            raise GitCommandError("Git returned an invalid changed-path status")
        status = status_raw.decode("ascii")
        path_count = 2 if status[0] in {"R", "C"} else 1
        if index + path_count > len(fields):
            raise GitCommandError("Git returned incomplete changed-path data")
        if path_count == 2:
            old_path = GitPath(fields[index])
            path = GitPath(fields[index + 1])
            index += 2
        else:
            old_path = None
            path = GitPath(fields[index])
            index += 1
        result.append(ChangedPath(status=status, path=path, old_path=old_path))
    return tuple(result)


def _tree_entries(
    repo: Path,
    head_sha: str,
    *,
    object_view: _ObjectDatabaseView,
) -> Mapping[bytes, _TreeEntry]:
    raw = _run_git(
        repo,
        ["ls-tree", "-r", "-z", "--full-tree", head_sha],
        max_stdout_bytes=MAX_TREE_DATA_BYTES,
        object_view=object_view,
    ).stdout
    if raw and not raw.endswith(b"\x00"):
        raise GitCommandError("Git returned unterminated tree data")
    entries: dict[bytes, _TreeEntry] = {}
    for record in raw.rstrip(b"\x00").split(b"\x00") if raw else ():
        try:
            metadata, raw_path = record.split(b"\t", 1)
            mode_raw, type_raw, oid_raw = metadata.split(b" ", 2)
            mode = mode_raw.decode("ascii")
            object_type = type_raw.decode("ascii")
            object_id = oid_raw.decode("ascii")
        except (ValueError, UnicodeDecodeError) as exc:
            raise GitCommandError("Git returned malformed tree data") from exc
        path = GitPath(raw_path)
        if not re.fullmatch(r"[0-7]{6}", mode):
            raise GitCommandError("Git returned an invalid tree mode")
        if object_type not in {"blob", "commit"}:
            raise GitCommandError("Git returned an unsupported tree object type")
        if _SHA1_RE.fullmatch(object_id) is None:
            raise GitCommandError("Git returned an invalid tree object ID")
        if raw_path in entries:
            raise GitCommandError("Git returned duplicate tree paths")
        entries[raw_path] = _TreeEntry(
            mode=mode,
            object_type=object_type,
            object_id=object_id,
            path=path,
        )
    return entries


def _blob_size(
    repo: Path,
    object_id: str,
    *,
    object_view: _ObjectDatabaseView,
) -> int:
    raw = _run_git(
        repo,
        ["cat-file", "-s", object_id],
        max_stdout_bytes=64,
        object_view=object_view,
    ).stdout.strip()
    try:
        value = int(raw.decode("ascii"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise GitCommandError("Git returned an invalid blob size") from exc
    if value < 0:
        raise GitCommandError("Git returned a negative blob size")
    return value


def _content_kind(mode: str, content: bytes) -> str:
    symlink_prefix = "symlink_target_" if mode == "120000" else ""
    if b"\x00" in content:
        return symlink_prefix + "binary"
    try:
        content.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return symlink_prefix + "non_utf8"
    return symlink_prefix + "utf-8"


def _capture_tree_object(
    repo: Path,
    entry: _TreeEntry,
    *,
    object_view: _ObjectDatabaseView,
    per_file_limit: int,
    remaining_total: int,
) -> tuple[CapturedGitObject, int]:
    if entry.object_type == "commit" or entry.mode == "160000":
        return (
            CapturedGitObject(
                path=entry.path,
                mode=entry.mode,
                object_type=entry.object_type,
                object_id=entry.object_id,
                byte_length=None,
                content_sha256=None,
                content=None,
                content_kind="gitlink",
                omitted_reason="gitlink content is not part of the parent object db",
            ),
            remaining_total,
        )
    size = _blob_size(repo, entry.object_id, object_view=object_view)
    if size > per_file_limit:
        return (
            CapturedGitObject(
                path=entry.path,
                mode=entry.mode,
                object_type=entry.object_type,
                object_id=entry.object_id,
                byte_length=size,
                content_sha256=None,
                content=None,
                content_kind="omitted",
                omitted_reason="per-file review material limit exceeded",
            ),
            remaining_total,
        )
    if size > remaining_total:
        return (
            CapturedGitObject(
                path=entry.path,
                mode=entry.mode,
                object_type=entry.object_type,
                object_id=entry.object_id,
                byte_length=size,
                content_sha256=None,
                content=None,
                content_kind="omitted",
                omitted_reason="total review material limit exceeded",
            ),
            remaining_total,
        )
    content = _run_git(
        repo,
        ["cat-file", "blob", entry.object_id],
        max_stdout_bytes=size,
        object_view=object_view,
    ).stdout
    if len(content) != size:
        raise GitCommandError("Git blob size changed while it was captured")
    return (
        CapturedGitObject(
            path=entry.path,
            mode=entry.mode,
            object_type=entry.object_type,
            object_id=entry.object_id,
            byte_length=size,
            content_sha256=hashlib.sha256(content).hexdigest(),
            content=content,
            content_kind=_content_kind(entry.mode, content),
        ),
        remaining_total - size,
    )


def _capture_changed_files(
    repo: Path,
    changed_paths: tuple[ChangedPath, ...],
    tree: Mapping[bytes, _TreeEntry],
    *,
    object_view: _ObjectDatabaseView,
) -> tuple[CapturedGitObject, ...]:
    remaining = MAX_REVIEW_FILE_BYTES_TOTAL
    result: list[CapturedGitObject] = []
    seen: set[bytes] = set()
    for changed in changed_paths:
        if changed.status.startswith("D") or changed.path.raw in seen:
            continue
        entry = tree.get(changed.path.raw)
        if entry is None:
            raise GitCommandError("changed path is missing from the exact HEAD tree")
        captured, remaining = _capture_tree_object(
            repo,
            entry,
            object_view=object_view,
            per_file_limit=MAX_REVIEW_FILE_BYTES,
            remaining_total=remaining,
        )
        result.append(captured)
        seen.add(changed.path.raw)
    return tuple(result)


def _guidance_candidates(
    changed_paths: tuple[ChangedPath, ...],
) -> tuple[bytes, ...]:
    candidates: set[bytes] = set(_GUIDANCE_NAMES)
    for changed in changed_paths:
        relevant = [changed.path]
        if changed.old_path is not None:
            relevant.append(changed.old_path)
        for path in relevant:
            parents = path.raw.split(b"/")[:-1]
            for depth in range(1, len(parents) + 1):
                prefix = b"/".join(parents[:depth])
                for name in _GUIDANCE_NAMES:
                    candidates.add(prefix + b"/" + name)
    return tuple(sorted(candidates))


def _capture_guidance(
    repo: Path,
    changed_paths: tuple[ChangedPath, ...],
    tree: Mapping[bytes, _TreeEntry],
    *,
    object_view: _ObjectDatabaseView,
) -> tuple[CapturedGitObject, ...]:
    remaining = MAX_GUIDANCE_BYTES_TOTAL
    result: list[CapturedGitObject] = []
    for raw_path in _guidance_candidates(changed_paths):
        entry = tree.get(raw_path)
        if entry is None:
            continue
        captured, remaining = _capture_tree_object(
            repo,
            entry,
            object_view=object_view,
            per_file_limit=MAX_GUIDANCE_FILE_BYTES,
            remaining_total=remaining,
        )
        result.append(captured)
    return tuple(result)


def _patch_bytes(
    repo: Path,
    base_sha: str,
    head_sha: str,
    *,
    object_view: _ObjectDatabaseView,
) -> bytes:
    return _run_git(
        repo,
        [*PATCH_DIFF_ARGUMENTS, base_sha, head_sha, "--"],
        max_stdout_bytes=MAX_PATCH_BYTES,
        object_view=object_view,
    ).stdout


def _assert_capture_still_current(
    repo: Path,
    *,
    head_state: _HeadState,
    head_tree_sha: str,
    object_view: _ObjectDatabaseView,
) -> None:
    _assert_stable_worktree(repo)
    if _head_state(repo) != head_state:
        raise SubjectChangedError(
            "repository HEAD metadata changed while subject was captured"
        )
    if (
        _tree_sha(repo, head_state.sha, object_view=object_view)
        != head_tree_sha
    ):
        raise SubjectChangedError("HEAD tree changed while subject was captured")


def capture_commit_range_subject(
    repo: Pathish,
    base_sha: str,
    expected_head_sha: str | None = None,
) -> CapturedCommitRangeSubject:
    """Capture one clean ``base..HEAD`` range entirely from the object DB.

    Detached HEAD is accepted and represented explicitly.  ``base_sha`` and an
    optional ``expected_head_sha`` must be full lowercase SHA-1 commit IDs; no
    ref names, abbreviations, tags, or branch names are accepted.
    """

    root = _canonical_repo_path(repo)
    base_sha = _validate_sha(base_sha, "base_sha")
    if expected_head_sha is not None:
        expected_head_sha = _validate_sha(expected_head_sha, "expected_head_sha")
    _assert_stable_worktree(root)

    initial_head = _head_state(root)
    head_sha = initial_head.sha
    if expected_head_sha is not None and head_sha != expected_head_sha:
        raise SubjectChangedError("repository HEAD does not match expected_head_sha")
    if base_sha == head_sha:
        raise CodeReviewSubjectError("base and head commits must be different")
    with _isolated_object_database(root, head_sha) as object_view:
        _resolve_exact_commit(
            root,
            base_sha,
            "base_sha",
            object_view=object_view,
        )
        _resolve_exact_commit(
            root,
            head_sha,
            "head_sha",
            object_view=object_view,
        )
        _assert_ancestor(
            root,
            base_sha,
            head_sha,
            object_view=object_view,
        )

        head_tree_sha = _tree_sha(
            root,
            head_sha,
            object_view=object_view,
        )
        patch = _patch_bytes(
            root,
            base_sha,
            head_sha,
            object_view=object_view,
        )
        changed_paths = _parse_changed_paths(
            _run_git(
                root,
                [
                    "diff",
                    "--name-status",
                    "-z",
                    "--find-renames",
                    base_sha,
                    head_sha,
                    "--",
                ],
                max_stdout_bytes=MAX_CHANGED_PATH_DATA_BYTES,
                object_view=object_view,
            ).stdout
        )
        tree = _tree_entries(root, head_sha, object_view=object_view)
        files = _capture_changed_files(
            root,
            changed_paths,
            tree,
            object_view=object_view,
        )
        guidance = _capture_guidance(
            root,
            changed_paths,
            tree,
            object_view=object_view,
        )
        _assert_capture_still_current(
            root,
            head_state=initial_head,
            head_tree_sha=head_tree_sha,
            object_view=object_view,
        )

        captured = CapturedCommitRangeSubject(
            repo_path=os.fspath(root),
            base_sha=base_sha,
            head_sha=head_sha,
            head_tree_sha=head_tree_sha,
            patch_sha256=hashlib.sha256(patch).hexdigest(),
            patch=patch,
            head_ref=initial_head.ref,
            detached_head=initial_head.detached,
            changed_paths=changed_paths,
            files=files,
            guidance=guidance,
        )
        # Keep the capture budget aligned with the structured review service's
        # 2 MiB per-section admission limit, including JSON/base64 expansion.
        captured.prompt_material()
        captured.prompt_guidance()
        return captured


def verify_commit_range_subject(
    repo: Pathish,
    subject: CapturedCommitRangeSubject | CommitRangeSubject,
) -> CommitRangeSubject:
    """Fail closed unless HEAD, tree, cleanliness, and raw patch still match."""

    root = _canonical_repo_path(repo)
    if isinstance(subject, CapturedCommitRangeSubject):
        captured_root = _canonical_repo_path(subject.repo_path)
        if root != captured_root:
            raise SubjectChangedError("subject is bound to a different repository")
        exact_subject = subject.subject
    elif isinstance(subject, CommitRangeSubject):
        exact_subject = subject
    else:
        raise TypeError("subject must be a captured or structured commit range")

    # Re-enter the full capture path so verification cannot silently drift from
    # capture semantics (limits, isolated attributes, clean/operation gates, or
    # HEAD fencing).  Compare every content-addressed subject field.
    recomputed = capture_commit_range_subject(
        root,
        exact_subject.base_sha,
        expected_head_sha=exact_subject.head_sha,
    )
    if recomputed.head_tree_sha != exact_subject.head_tree_sha:
        raise SubjectChangedError("HEAD tree no longer matches review subject")
    if recomputed.patch_sha256 != exact_subject.patch_sha256:
        raise SubjectChangedError("raw patch no longer matches review subject")
    if recomputed.subject != exact_subject:
        raise SubjectChangedError("recomputed commit range no longer matches subject")
    if isinstance(subject, CapturedCommitRangeSubject) and recomputed != subject:
        raise SubjectChangedError(
            "captured review material no longer matches the exact subject"
        )
    return exact_subject
