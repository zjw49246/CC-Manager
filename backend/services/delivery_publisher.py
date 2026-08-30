"""Fail-closed Git/GitHub publisher for one immutable Delivery subject.

The controller owns the durable outbox/lease.  This module owns the narrow
remote boundary: publish one exact reviewed commit with a non-force push,
recover or create the matching open pull request, and idempotently attach the
normal PR Monitor lifecycle.  No shell is involved; Git and ``gh api`` receive
validated argv components and request bodies respectively.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
from typing import Any, Protocol
from urllib.parse import urlencode, urlsplit

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.delivery import DeliveryAction, DeliveryRun
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRFindingRebuttal,
    PRMonitorRun,
    PRReview,
    PRReviewerRun,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.services.code_review_subject import verify_commit_range_subject
from backend.services.cancellation import await_task_completion
from backend.services.delivery_controller import (
    DeliveryEffectFence,
    DeliveryPublisherNoEffectPreflightError,
    DeliveryPublisherPermanentError,
    DeliverySubjectChanged,
    PublishedPullRequest,
)
from backend.services.delivery_workspace import (
    DeliveryWorkspaceError,
    _SAFE_GIT_CONFIG,
    _git_env as _controller_git_environment,
    _validate_controller_git_repository,
)
from backend.services.pr_monitor_loop import attach_review_to_run
from backend.services.pr_review_actions import (
    FindingActionConflict,
    lock_pr_repo_action_boundary,
)
from backend.services.pr_review_service import (
    GhError,
    _ACTION_NONCE_RE,
    _find_merge_evidence,
    _gh_api_json,
    _gh_api_value,
    _terminal_publication_error,
    create_pr_review_task,
)
from backend.services.structured_code_review import CommitRangeSubject


_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
_BRANCH_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,199}\Z")
_SCP_GITHUB_RE = re.compile(
    r"(?:[^/@:]+@)?github\.com:(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?/?\Z",
    re.IGNORECASE,
)
_MAX_GIT_OUTPUT = 128 * 1024
_MAX_GITHUB_LIST_BYTES = 2 * 1024 * 1024
_MAX_PR_BODY_BYTES = 60 * 1024
_LEGACY_BOUND_STALE_HEAD_REASON = (
    "Delivery pull-request history is ambiguous: "
    "Pull request does not match the exact Delivery subject"
)


class DeliveryGitError(RuntimeError):
    """A Git read/write failed without proving a permanent ref conflict."""


class DeliveryGitAuthenticationError(DeliveryGitError):
    """A GitHub credential failure proved that no Git write was attempted."""


class DeliveryNonFastForwardError(DeliveryGitError):
    """The remote rejected the deliberately non-force ref update."""


@dataclass(frozen=True, slots=True)
class _PublishingSubject:
    run_id: int
    project_id: int
    monitored_repo_id: int
    developer_task_id: int | None
    repo_path: str
    workspace_path: str
    repo_full_name: str
    project_git_url: str
    git_ssh_key_path: str | None
    title: str
    requirements: str
    base_branch: str
    delivery_branch: str
    base_sha: str
    head_sha: str
    head_tree_sha: str
    patch_sha256: str
    phase: str
    activity: str
    policy_hash: str
    policy_snapshot: dict
    pr_number: int | None
    pr_url: str | None
    pr_monitor_run_id: int | None

    @property
    def delivery_id(self) -> str:
        return f"delivery:{self.run_id}:{self.head_sha}"

    @property
    def publish_key(self) -> str:
        return (
            f"delivery:{self.run_id}:publish:{self.base_sha}:{self.head_sha}"
        )


@dataclass(frozen=True, slots=True)
class _MergedMonitorEvidence:
    nonce: str
    actor: str
    publishing_started_at: datetime
    merge_method: str


@dataclass(frozen=True, slots=True)
class _PullRequestSnapshot:
    pull_request: PublishedPullRequest
    # ``merged`` is derived from a closed GitHub PR's immutable merge evidence.
    # Callers need the distinction only for the durable conflict receipt; both
    # terminal states are ineligible for Delivery V1 and must block replacement
    # PR creation forever.
    state: str


class DeliveryGitGateway(Protocol):
    async def verify_local(self, subject: _PublishingSubject) -> None: ...

    async def origin_repo_full_name(self, subject: _PublishingSubject) -> str: ...

    async def remote_ref_sha(
        self,
        subject: _PublishingSubject,
        branch: str,
    ) -> str | None: ...

    async def push_exact(self, subject: _PublishingSubject) -> None: ...


class DeliveryGitHubGateway(Protocol):
    async def list_pull_requests(
        self,
        *,
        repo_full_name: str,
        owner: str,
        head_branch: str,
    ) -> list[dict]: ...

    async def get_pull_request(
        self,
        *,
        repo_full_name: str,
        pr_number: int,
    ) -> dict: ...

    async def create_pull_request(
        self,
        *,
        repo_full_name: str,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
    ) -> dict: ...


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _value_hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _valid_branch(value: object) -> bool:
    components = value.split("/") if isinstance(value, str) else []
    return bool(
        isinstance(value, str)
        and _BRANCH_RE.fullmatch(value)
        and not value.startswith("-")
        and ".." not in value
        and "@{" not in value
        and not value.endswith(("/", ".", ".lock"))
        and "//" not in value
        and all(
            component
            and not component.startswith(".")
            and not component.endswith(".lock")
            for component in components
        )
    )


def _github_repo_from_url(value: object) -> str | None:
    """Return an owner/repo only for an unambiguous GitHub remote URL."""

    if not isinstance(value, str) or not value or any(ch in value for ch in "\r\n\x00"):
        return None
    scp_match = _SCP_GITHUB_RE.fullmatch(value)
    if scp_match is not None:
        return scp_match.group("repo")
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme.lower() not in {"https", "ssh"}:
        return None
    if (parsed.hostname or "").lower() != "github.com":
        return None
    path = parsed.path.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    if _REPO_RE.fullmatch(path) is None:
        return None
    return path


def _git_environment(ssh_key_path: str | None = None) -> dict[str, str]:
    env = _controller_git_environment()
    if ssh_key_path:
        key = Path(ssh_key_path)
        try:
            stat_result = key.stat()
            resolved = key.resolve(strict=True)
        except OSError as exc:
            raise DeliveryGitAuthenticationError(
                "Delivery Project SSH key is unavailable"
            ) from exc
        if (
            not resolved.is_file()
            or resolved != key.absolute()
            or stat_result.st_uid != os.geteuid()
            or stat_result.st_mode & 0o077
        ):
            raise DeliveryGitAuthenticationError(
                "Delivery Project SSH key does not satisfy the private-key boundary"
            )
        env["GIT_SSH_COMMAND"] = (
            f"ssh -i {shlex.quote(str(resolved))} -F /dev/null "
            "-o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "
            "-o BatchMode=yes"
        )
    return env


def _github_credential_config(remote_url: str) -> tuple[str, ...]:
    """Return one fixed GitHub-only credential helper for HTTPS transport.

    Controller Git deliberately ignores ambient/global credential helpers.  An
    HTTPS push still needs a credential source, so bind Git to the already
    authenticated GitHub CLI without placing its token in argv, an environment
    variable, the repository config, or a temporary plaintext file.  SSH
    remotes continue to use the separately constrained SSH transport.
    """

    try:
        parsed = urlsplit(remote_url)
    except ValueError as exc:
        raise DeliveryGitAuthenticationError(
            "Delivery GitHub credential scope is invalid"
        ) from exc
    if parsed.scheme.lower() != "https":
        return ()
    executable = shutil.which("gh")
    if executable is None:
        raise DeliveryGitAuthenticationError(
            "GitHub CLI is unavailable for Delivery HTTPS authentication"
        )
    try:
        resolved = Path(executable).resolve(strict=True)
    except OSError as exc:
        raise DeliveryGitAuthenticationError(
            "GitHub CLI cannot be resolved for Delivery HTTPS authentication"
        ) from exc
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise DeliveryGitAuthenticationError(
            "GitHub CLI is not executable for Delivery HTTPS authentication"
        )
    helper = f"!{shlex.quote(str(resolved))} auth git-credential"
    return (
        "-c",
        "credential.https://github.com.helper=",
        "-c",
        f"credential.https://github.com.helper={helper}",
    )


def _is_authentication_failure(diagnostic: str) -> bool:
    return any(
        marker in diagnostic
        for marker in (
            "authentication failed",
            "could not read username",
            "terminal prompts disabled",
            "unable to get password",
            "no oauth token",
            "not logged into any github hosts",
        )
    )


def _is_write_permission_failure(diagnostic: str) -> bool:
    """Return only for a remote refusal which proves no ref update occurred."""

    return any(
        marker in diagnostic
        for marker in (
            "write access to repository not granted",
            "permission to ",
            "the requested url returned error: 403",
            "remote: permission denied",
            "remote: access denied",
        )
    )


async def _await_cleanup_settled(
    cleanup: asyncio.Task[None],
    *,
    delayed_cancel: asyncio.CancelledError | None = None,
) -> None:
    cancellation = await await_task_completion(cleanup)
    cancellation = cancellation or delayed_cancel
    cleanup.result()
    if cancellation is not None:
        raise cancellation


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
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


async def _run_git(
    cwd: str,
    *args: str,
    timeout: float = 120,
    ssh_key_path: str | None = None,
) -> tuple[int, bytes, bytes]:
    """Run a bounded argv-only Git process with cancellation-safe reaping."""

    spawn = asyncio.create_task(
        asyncio.create_subprocess_exec(
            "git",
            *_SAFE_GIT_CONFIG,
            *args,
            cwd=cwd,
            env=_git_environment(ssh_key_path),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=(os.name == "posix"),
        )
    )
    delayed_cancel: asyncio.CancelledError | None = None
    try:
        delayed_cancel = await await_task_completion(spawn)
        process = spawn.result()
    except OSError as exc:
        raise DeliveryGitError("Unable to start Git") from exc
    if delayed_cancel is not None:
        await _await_cleanup_settled(
            asyncio.create_task(_terminate_process(process)),
            delayed_cancel=delayed_cancel,
        )

    async def read_limited(stream: asyncio.StreamReader) -> bytes:
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = await stream.read(32 * 1024)
            if not chunk:
                return b"".join(chunks)
            size += len(chunk)
            if size > _MAX_GIT_OUTPUT:
                raise DeliveryGitError("Git output exceeded the safety limit")
            chunks.append(chunk)

    stdout_task = asyncio.create_task(read_limited(process.stdout))
    stderr_task = asyncio.create_task(read_limited(process.stderr))

    async def cleanup() -> None:
        await _terminate_process(process)
        for task in (stdout_task, stderr_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

    try:
        stdout, stderr = await asyncio.wait_for(
            asyncio.gather(stdout_task, stderr_task),
            timeout=timeout,
        )
        await process.wait()
        await _await_cleanup_settled(asyncio.create_task(_terminate_process(process)))
        return process.returncode or 0, stdout, stderr
    except BaseException as exc:
        await _await_cleanup_settled(
            asyncio.create_task(cleanup()),
            delayed_cancel=(exc if isinstance(exc, asyncio.CancelledError) else None),
        )
        raise


class GitDeliveryGateway:
    """Production Git transport.  It never constructs a shell command."""

    async def _validated_remote_urls(
        self,
        subject: _PublishingSubject,
    ) -> tuple[str, str]:
        try:
            repository = await _validate_controller_git_repository(
                Path(subject.workspace_path),
                expected_repo_full_name=subject.repo_full_name,
            )
        except DeliveryWorkspaceError as exc:
            raise DeliveryGitError(
                "Repository Git configuration is not safe for publication"
            ) from exc
        fetch_repo = _github_repo_from_url(repository.fetch_url)
        push_repo = _github_repo_from_url(repository.push_url)
        project_repo = _github_repo_from_url(subject.project_git_url)
        expected = subject.repo_full_name.lower()
        if (
            fetch_repo is None
            or push_repo is None
            or project_repo is None
            or fetch_repo.lower() != expected
            or push_repo.lower() != expected
            or project_repo.lower() != expected
        ):
            raise DeliveryGitError(
                "Origin fetch/push URLs do not match the monitored GitHub repository"
            )
        return repository.fetch_url, repository.push_url

    async def verify_local(self, subject: _PublishingSubject) -> None:
        repo = Path(os.path.abspath(subject.repo_path))
        workspace = Path(os.path.abspath(subject.workspace_path))
        managed_root = repo / ".claude-manager" / "worktrees"
        try:
            for path in (
                repo,
                repo / ".claude-manager",
                managed_root,
                workspace,
            ):
                if (
                    path.is_symlink()
                    or not path.is_dir()
                    or os.path.realpath(path) != str(path)
                ):
                    raise DeliverySubjectChanged(
                        "Delivery workspace ancestry is not a safe directory"
                    )
            workspace.relative_to(managed_root)
            await _validate_controller_git_repository(
                workspace,
                expected_repo_full_name=subject.repo_full_name,
            )

            async def git_value(cwd: Path, *args: str) -> str:
                returncode, stdout, _stderr = await _run_git(str(cwd), *args)
                if returncode != 0:
                    raise DeliveryGitError("Unable to inspect Delivery workspace")
                try:
                    value = stdout.decode("utf-8").strip()
                except UnicodeDecodeError as exc:
                    raise DeliveryGitError(
                        "Delivery workspace returned non-UTF-8 metadata"
                    ) from exc
                if not value:
                    raise DeliveryGitError("Delivery workspace metadata is empty")
                return value

            repo_top, workspace_top, repo_common, workspace_common = (
                await asyncio.gather(
                    git_value(repo, "rev-parse", "--show-toplevel"),
                    git_value(workspace, "rev-parse", "--show-toplevel"),
                    git_value(
                        repo,
                        "rev-parse",
                        "--path-format=absolute",
                        "--git-common-dir",
                    ),
                    git_value(
                        workspace,
                        "rev-parse",
                        "--path-format=absolute",
                        "--git-common-dir",
                    ),
                )
            )
            if (
                Path(os.path.abspath(repo_top)) != repo
                or Path(os.path.abspath(workspace_top)) != workspace
                or Path(os.path.abspath(repo_common))
                != Path(os.path.abspath(workspace_common))
            ):
                raise DeliverySubjectChanged(
                    "Delivery workspace is no longer owned by the project repository"
                )
            branch, base_sha, head_sha, tree_sha = await asyncio.gather(
                git_value(workspace, "symbolic-ref", "--short", "HEAD"),
                git_value(
                    workspace,
                    "rev-parse",
                    "--verify",
                    f"refs/remotes/origin/{subject.base_branch}^{{commit}}",
                ),
                git_value(workspace, "rev-parse", "--verify", "HEAD^{commit}"),
                git_value(workspace, "rev-parse", "--verify", "HEAD^{tree}"),
            )
            if branch != subject.delivery_branch or (
                base_sha != subject.base_sha
                or head_sha != subject.head_sha
                or tree_sha != subject.head_tree_sha
            ):
                raise DeliverySubjectChanged(
                    "Delivery workspace no longer matches the reviewed subject"
                )
            exact = CommitRangeSubject(
                base_sha=subject.base_sha,
                head_sha=subject.head_sha,
                head_tree_sha=subject.head_tree_sha,
                patch_sha256=subject.patch_sha256,
            )
            await asyncio.to_thread(
                verify_commit_range_subject,
                subject.workspace_path,
                exact,
            )
        except DeliverySubjectChanged:
            raise
        except (DeliveryGitError, ValueError) as exc:
            raise DeliverySubjectChanged(
                "Delivery workspace failed exact-subject verification"
            ) from exc

    async def origin_repo_full_name(self, subject: _PublishingSubject) -> str:
        fetch_url, _push_url = await self._validated_remote_urls(subject)
        repo = _github_repo_from_url(fetch_url)
        if repo is None:
            raise DeliveryGitError("Origin is not an unambiguous GitHub remote")
        return repo

    async def remote_ref_sha(
        self,
        subject: _PublishingSubject,
        branch: str,
    ) -> str | None:
        if not _valid_branch(branch):
            raise DeliveryGitError("Invalid remote branch")
        fetch_url, _push_url = await self._validated_remote_urls(subject)
        ref = f"refs/heads/{branch}"
        returncode, stdout, stderr = await _run_git(
            subject.workspace_path,
            *_github_credential_config(fetch_url),
            "ls-remote",
            "--upload-pack=git-upload-pack",
            "--refs",
            fetch_url,
            ref,
            ssh_key_path=subject.git_ssh_key_path,
        )
        if returncode != 0:
            diagnostic = (stdout + b"\n" + stderr).decode(
                "utf-8", errors="replace"
            ).lower()
            if _is_authentication_failure(diagnostic):
                raise DeliveryGitAuthenticationError(
                    "GitHub authentication is unavailable for Delivery ref reads"
                )
            raise DeliveryGitError("Unable to read the exact remote ref")
        if not stdout:
            return None
        try:
            lines = stdout.decode("ascii").splitlines()
        except UnicodeDecodeError as exc:
            raise DeliveryGitError("Remote ref response is malformed") from exc
        if len(lines) != 1:
            raise DeliveryGitError("Remote returned an ambiguous ref")
        sha, separator, returned_ref = lines[0].partition("\t")
        sha = sha.lower()
        if not separator or returned_ref != ref or _SHA_RE.fullmatch(sha) is None:
            raise DeliveryGitError("Remote returned a malformed ref")
        return sha

    async def push_exact(self, subject: _PublishingSubject) -> None:
        _fetch_url, push_url = await self._validated_remote_urls(subject)
        refspec = f"{subject.head_sha}:refs/heads/{subject.delivery_branch}"
        returncode, stdout, stderr = await _run_git(
            subject.workspace_path,
            *_github_credential_config(push_url),
            "push",
            "--porcelain",
            "--no-verify",
            "--receive-pack=git-receive-pack",
            push_url,
            refspec,
            ssh_key_path=subject.git_ssh_key_path,
        )
        if returncode == 0:
            return
        diagnostic = (stdout + b"\n" + stderr).decode(
            "utf-8", errors="replace"
        ).lower()
        if _is_authentication_failure(diagnostic):
            raise DeliveryGitAuthenticationError(
                "GitHub authentication is unavailable for Delivery push"
            )
        if any(
            marker in diagnostic
            for marker in (
                "non-fast-forward",
                "[rejected]",
                "fetch first",
                "stale info",
            )
        ):
            raise DeliveryNonFastForwardError(
                "Remote delivery branch rejected the non-force update"
            )
        if _is_write_permission_failure(diagnostic):
            # Delivery has no durable field for a cross-repository head route.
            # Treat a proven upstream refusal as a no-effect preflight failure
            # instead of pushing to an unrecorded fork which cannot be recovered
            # safely after a crash or across a later repair cycle.
            raise DeliveryGitAuthenticationError(
                "GitHub write permission is unavailable for Delivery push"
            )
        # Do not include stderr: remote URLs and credential helpers may expose
        # sensitive material in diagnostics.
        raise DeliveryGitError("Git push did not report success")


class GhDeliveryGateway:
    """GitHub REST adapter using CCM's existing authenticated ``gh api`` path."""

    async def list_pull_requests(
        self,
        *,
        repo_full_name: str,
        owner: str,
        head_branch: str,
    ) -> list[dict]:
        query = urlencode(
            {
                # Recovery must include closed/merged history.  GitHub only
                # enforces uniqueness among *open* PRs, so querying ``open``
                # would permit a response-loss retry to create a replacement
                # after the first PR was closed.
                "state": "all",
                "head": f"{owner}:{head_branch}",
                "per_page": "100",
            }
        )
        value = await _gh_api_value(
            f"repos/{repo_full_name}/pulls?{query}",
            max_output_bytes=_MAX_GITHUB_LIST_BYTES,
        )
        if not isinstance(value, list) or any(
            not isinstance(item, dict) for item in value
        ):
            raise GhError("Malformed GitHub pull-request list response")
        return value

    async def get_pull_request(
        self,
        *,
        repo_full_name: str,
        pr_number: int,
    ) -> dict:
        return await _gh_api_json(
            f"repos/{repo_full_name}/pulls/{pr_number}",
            max_output_bytes=_MAX_GITHUB_LIST_BYTES,
        )

    async def create_pull_request(
        self,
        *,
        repo_full_name: str,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
    ) -> dict:
        return await _gh_api_json(
            f"repos/{repo_full_name}/pulls",
            method="POST",
            payload={
                "title": title,
                "body": body,
                "head": head_branch,
                "base": base_branch,
                "maintainer_can_modify": True,
            },
            max_output_bytes=_MAX_GITHUB_LIST_BYTES,
        )


ReviewCreator = Callable[
    [AsyncSession, MonitoredRepo, dict],
    Awaitable[PRReview],
]
ReviewAttacher = Callable[..., Awaitable[PRMonitorRun]]


class GitHubDeliveryPublisher:
    """Idempotent production implementation of ``DeliveryPublisher``."""

    def __init__(
        self,
        db_factory: Callable[[], Any],
        *,
        git: DeliveryGitGateway | None = None,
        github: DeliveryGitHubGateway | None = None,
        review_creator: ReviewCreator = create_pr_review_task,
        review_attacher: ReviewAttacher = attach_review_to_run,
    ) -> None:
        self._db_factory = db_factory
        self._git = git or GitDeliveryGateway()
        self._github = github or GhDeliveryGateway()
        self._review_creator = review_creator
        self._review_attacher = review_attacher

    @staticmethod
    def _validate_effect_fence_argument(
        subject: _PublishingSubject,
        fence: DeliveryEffectFence,
    ) -> None:
        if (
            not isinstance(fence, DeliveryEffectFence)
            or fence.run_id != subject.run_id
            or not isinstance(fence.controller_owner, str)
            or not fence.controller_owner
            or isinstance(fence.controller_generation, bool)
            or fence.controller_generation < 1
            or isinstance(fence.action_id, bool)
            or fence.action_id < 1
            or not isinstance(fence.action_token, str)
            or not fence.action_token
            or fence.expected_base_sha != subject.base_sha
            or fence.expected_head_sha != subject.head_sha
        ):
            raise DeliverySubjectChanged(
                "Delivery publication fence does not match the exact subject"
            )

    async def _assert_effect_fence(
        self,
        subject: _PublishingSubject,
        fence: DeliveryEffectFence,
        *,
        db: AsyncSession | None = None,
    ) -> None:
        """Prove the exact Controller and Action leases before an effect.

        Git's non-force ref update and GitHub's branch/base PR natural key make
        every remote write idempotent.  This durable fence additionally stops a
        stale Controller generation before it can begin another write after a
        lease takeover.
        """

        self._validate_effect_fence_argument(subject, fence)

        async def verify(session: AsyncSession) -> None:
            run = await session.get(
                DeliveryRun,
                subject.run_id,
                populate_existing=True,
            )
            action = await session.get(
                DeliveryAction,
                fence.action_id,
                populate_existing=True,
            )
            now = datetime.utcnow()
            if (
                run is None
                or action is None
                or run.phase != "publishing"
                or run.activity != "running"
                or run.outcome is not None
                or run.lease_owner != fence.controller_owner
                or run.controller_generation != fence.controller_generation
                or run.lease_expires_at is None
                or run.lease_expires_at <= now
                or run.base_sha != subject.base_sha
                or run.head_sha != subject.head_sha
                or run.head_tree_sha != subject.head_tree_sha
                or action.run_id != run.id
                or action.cycle_id != run.current_cycle_id
                or action.active_run_id != run.id
                or action.action_type != "ensure_pull_request"
                or action.idempotency_key != subject.publish_key
                or action.status != "leased"
                or action.lease_owner != fence.action_token
                or action.lease_expires_at is None
                or action.lease_expires_at <= now
                or action.expected_base_sha != subject.base_sha
                or action.expected_head_sha != subject.head_sha
                or not isinstance(action.payload, dict)
                or action.payload_hash != _value_hash(action.payload)
                or action.payload.get("run_id") != run.id
                or action.payload.get("cycle_id") != run.current_cycle_id
                or action.payload.get("repo_id") != run.monitored_repo_id
                or action.payload.get("base_sha") != subject.base_sha
                or action.payload.get("head_sha") != subject.head_sha
                or action.payload.get("head_tree_sha") != subject.head_tree_sha
                or action.payload.get("patch_sha256") != subject.patch_sha256
                or action.payload.get("base_branch") != subject.base_branch
                or action.payload.get("delivery_branch")
                != subject.delivery_branch
            ):
                raise DeliverySubjectChanged(
                    "Delivery publication lease or action fence changed"
                )

        if db is not None:
            await verify(db)
            return
        async with self._db_factory() as owned_db:
            await verify(owned_db)
            await owned_db.rollback()

    async def _load_subject(
        self,
        run_id: int,
        *,
        expected_states: frozenset[tuple[str, str]] = frozenset(
            {("publishing", "running")}
        ),
    ) -> _PublishingSubject:
        if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
            raise DeliveryPublisherPermanentError("Invalid Delivery Run id")
        async with self._db_factory() as db:
            run = await db.get(DeliveryRun, run_id, populate_existing=True)
            if run is None:
                raise DeliveryPublisherPermanentError("Delivery Run disappeared")
            project = await db.get(Project, run.project_id, populate_existing=True)
            repo = (
                await db.get(
                    MonitoredRepo,
                    run.monitored_repo_id,
                    populate_existing=True,
                )
                if run.monitored_repo_id is not None
                else None
            )
            if project is None or repo is None:
                raise DeliveryPublisherPermanentError(
                    "Delivery publishing scope disappeared"
                )
            policy = run.policy_snapshot
            monitor_policy = (
                policy.get("pr_monitor") if isinstance(policy, dict) else None
            )
            auto_merge = (
                policy.get("auto_merge") if isinstance(policy, dict) else None
            )
            terminal = (
                policy.get("terminal") if isinstance(policy, dict) else None
            )
            if (
                (run.phase, run.activity) not in expected_states
                or run.outcome is not None
                or not isinstance(policy, dict)
                or _value_hash(policy) != run.policy_hash
                or type(auto_merge) is not bool
                or terminal
                != ("merged" if auto_merge else "ready_to_merge")
                or not isinstance(monitor_policy, dict)
                or monitor_policy.get("repo_id") != repo.id
                or monitor_policy.get("repo_full_name") != repo.repo_full_name
                or monitor_policy.get("review_mode") != repo.review_mode
                or monitor_policy.get("wait_for_ci") is not bool(repo.wait_for_ci)
                or monitor_policy.get("required_checks") != repo.required_checks
            ):
                raise DeliveryPublisherPermanentError(
                    "Delivery policy or publishing state changed"
                )
            if (
                project.worker_id is not None
                or repo.worker_id is not None
                or repo.project_id != project.id
                or not project.has_remote
                or not project.local_path
                or not project.git_url
                or not repo.enabled
                or (repo.merge_queue_mode or "manual") != "manual"
                or (repo.review_mode or "single") != "panel"
                or bool(repo.wait_for_ci) != bool(repo.required_checks)
                or (auto_merge and not repo.wait_for_ci)
            ):
                raise DeliveryPublisherPermanentError(
                    "Delivery repository is no longer eligible for publishing"
                )
            if (
                _REPO_RE.fullmatch(repo.repo_full_name) is None
                or _github_repo_from_url(project.git_url) is None
                or _github_repo_from_url(project.git_url).lower()
                != repo.repo_full_name.lower()
                or not _valid_branch(run.base_branch)
                or not _valid_branch(run.delivery_branch)
                or not all(
                    isinstance(value, str) and _SHA_RE.fullmatch(value)
                    for value in (
                        run.base_sha,
                        run.head_sha,
                        run.head_tree_sha,
                    )
                )
                or not isinstance(run.patch_sha256, str)
                or _HASH_RE.fullmatch(run.patch_sha256) is None
                or not run.workspace_path
                or not run.title
            ):
                raise DeliveryPublisherPermanentError(
                    "Delivery exact subject is incomplete or invalid"
                )
            return _PublishingSubject(
                run_id=run.id,
                project_id=project.id,
                monitored_repo_id=repo.id,
                developer_task_id=run.developer_task_id,
                repo_path=str(Path(project.local_path).absolute()),
                workspace_path=str(Path(run.workspace_path).absolute()),
                repo_full_name=repo.repo_full_name,
                project_git_url=project.git_url,
                git_ssh_key_path=(
                    project.git_ssh_key_path
                    if project.git_credential_type == "ssh"
                    else None
                ),
                title=run.title,
                requirements=run.requirements,
                base_branch=run.base_branch,
                delivery_branch=run.delivery_branch,
                base_sha=run.base_sha,
                head_sha=run.head_sha,
                head_tree_sha=run.head_tree_sha,
                patch_sha256=run.patch_sha256,
                phase=run.phase,
                activity=run.activity,
                policy_hash=run.policy_hash,
                policy_snapshot=json.loads(_canonical_json(policy)),
                pr_number=run.pr_number,
                pr_url=run.pr_url,
                pr_monitor_run_id=run.pr_monitor_run_id,
            )

    async def _assert_local_subject(
        self,
        subject: _PublishingSubject,
    ) -> None:
        current = await self._load_subject(
            subject.run_id,
            expected_states=frozenset({(subject.phase, subject.activity)}),
        )
        if current != subject:
            raise DeliverySubjectChanged(
                "Delivery Run changed during publication"
            )
        await self._git.verify_local(subject)
        try:
            origin_repo = await self._git.origin_repo_full_name(subject)
        except DeliveryGitError as exc:
            raise DeliveryPublisherPermanentError(
                "Project origin cannot be proven to be the monitored repository"
            ) from exc
        if origin_repo.lower() != subject.repo_full_name.lower():
            raise DeliveryPublisherPermanentError(
                "Project origin does not match the monitored repository"
            )

    async def _assert_remote_base(self, subject: _PublishingSubject) -> None:
        remote_base = await self._git.remote_ref_sha(
            subject, subject.base_branch
        )
        if remote_base != subject.base_sha:
            raise DeliveryPublisherPermanentError(
                "Remote base branch changed after the reviewed subject was frozen"
            )

    async def _assert_subject(
        self,
        subject: _PublishingSubject,
        *,
        require_remote_head: bool,
    ) -> None:
        await self._assert_local_subject(subject)
        await self._assert_remote_base(subject)
        if require_remote_head:
            remote_head = await self._git.remote_ref_sha(
                subject, subject.delivery_branch
            )
            if remote_head != subject.head_sha:
                raise DeliverySubjectChanged(
                    "Remote delivery branch does not expose the reviewed head"
                )

    def _validate_idempotency_key(
        self,
        subject: _PublishingSubject,
        value: str,
        *,
        monitor: bool,
    ) -> None:
        expected = subject.publish_key + (":monitor" if monitor else "")
        if value != expected:
            raise DeliveryPublisherPermanentError(
                "Delivery publication idempotency key does not match the subject"
            )

    def _validate_pull_request_snapshot(
        self,
        subject: _PublishingSubject,
        value: object,
        *,
        allow_bound_open_stale_head: bool = False,
    ) -> _PullRequestSnapshot:
        if not isinstance(value, dict):
            raise DeliveryPublisherPermanentError(
                "GitHub returned a malformed pull request"
            )
        base = value.get("base")
        head = value.get("head")
        base_repo = base.get("repo") if isinstance(base, dict) else None
        head_repo = head.get("repo") if isinstance(head, dict) else None
        number = value.get("number")
        url = value.get("html_url")
        state = value.get("state")
        base_sha = base.get("sha") if isinstance(base, dict) else None
        base_ref = base.get("ref") if isinstance(base, dict) else None
        head_sha = head.get("sha") if isinstance(head, dict) else None
        head_ref = head.get("ref") if isinstance(head, dict) else None
        base_repo_name = (
            base_repo.get("full_name") if isinstance(base_repo, dict) else None
        )
        head_repo_name = (
            head_repo.get("full_name") if isinstance(head_repo, dict) else None
        )
        normalized_head_sha = head_sha.lower() if isinstance(head_sha, str) else None
        bound_open_stale_head = (
            allow_bound_open_stale_head
            and state == "open"
            and subject.pr_number is not None
            and subject.pr_url is not None
            and number == subject.pr_number
            and isinstance(url, str)
            and url.rstrip("/") == subject.pr_url.rstrip("/")
            and isinstance(normalized_head_sha, str)
            and _SHA_RE.fullmatch(normalized_head_sha) is not None
            and isinstance(head_repo_name, str)
            and head_repo_name.lower() == subject.repo_full_name.lower()
        )
        if (
            isinstance(number, bool)
            or not isinstance(number, int)
            or number <= 0
            or not isinstance(url, str)
            or state not in {"open", "closed"}
            or not isinstance(base_sha, str)
            or base_sha.lower() != subject.base_sha
            or base_ref != subject.base_branch
            or not isinstance(head_sha, str)
            or (
                normalized_head_sha != subject.head_sha
                and not bound_open_stale_head
            )
            or head_ref != subject.delivery_branch
            or not isinstance(base_repo_name, str)
            or base_repo_name.lower() != subject.repo_full_name.lower()
            or not isinstance(head_repo_name, str)
            # Delivery pushes its fenced branch to the Project origin. A PR
            # from a same-named branch in a fork is a different subject.
            or head_repo_name.lower() != subject.repo_full_name.lower()
        ):
            raise DeliveryPublisherPermanentError(
                "Pull request does not match the exact Delivery subject"
            )
        try:
            parsed_url = urlsplit(url)
        except ValueError as exc:
            raise DeliveryPublisherPermanentError(
                "GitHub returned an invalid pull-request URL"
            ) from exc
        expected_path = f"/{subject.repo_full_name}/pull/{number}".lower()
        if (
            parsed_url.scheme != "https"
            or (parsed_url.hostname or "").lower() != "github.com"
            or parsed_url.path.rstrip("/").lower() != expected_path
            or parsed_url.query
            or parsed_url.fragment
        ):
            raise DeliveryPublisherPermanentError(
                "Pull-request URL does not match its repository and number"
            )
        if subject.pr_number is not None and subject.pr_number != number:
            raise DeliveryPublisherPermanentError(
                "Delivery Run is already bound to another pull request"
            )
        if subject.pr_url is not None and subject.pr_url.rstrip("/") != url.rstrip("/"):
            raise DeliveryPublisherPermanentError(
                "Delivery Run pull-request URL changed"
            )
        merged = value.get("merged")
        merged_at = value.get("merged_at")
        if (
            merged not in (None, True, False)
            or (merged_at is not None and not isinstance(merged_at, str))
            or (isinstance(merged_at, str) and not merged_at)
            or (merged is False and merged_at is not None)
            or (state == "open" and (merged is True or merged_at is not None))
        ):
            raise DeliveryPublisherPermanentError(
                "GitHub returned inconsistent pull-request terminal evidence"
            )
        terminal_state = (
            "merged"
            if state == "closed" and (merged is True or merged_at is not None)
            else state
        )
        return _PullRequestSnapshot(
            pull_request=PublishedPullRequest(
                repo_id=subject.monitored_repo_id,
                pr_number=number,
                url=url,
                base_sha=subject.base_sha,
                head_sha=normalized_head_sha,
                head_branch=subject.delivery_branch,
                head_repo_full_name=head_repo_name,
            ),
            state=terminal_state,
        )

    def _validate_pull_request(
        self,
        subject: _PublishingSubject,
        value: object,
    ) -> PublishedPullRequest:
        snapshot = self._validate_pull_request_snapshot(subject, value)
        if snapshot.state != "open":
            raise DeliveryPublisherPermanentError(
                "Pull request is no longer open for Delivery"
            )
        return snapshot.pull_request

    async def _find_existing_pull_request(
        self,
        subject: _PublishingSubject,
        *,
        allow_bound_open_stale_head: bool = False,
    ) -> _PullRequestSnapshot | None:
        owner = subject.repo_full_name.split("/", 1)[0]
        candidates = await self._github.list_pull_requests(
            repo_full_name=subject.repo_full_name,
            owner=owner,
            head_branch=subject.delivery_branch,
        )
        if not candidates:
            return None
        if len(candidates) != 1:
            raise DeliveryPublisherPermanentError(
                "GitHub returned ambiguous PR history for one Delivery branch"
            )
        return self._validate_pull_request_snapshot(
            subject,
            candidates[0],
            allow_bound_open_stale_head=allow_bound_open_stale_head,
        )

    @staticmethod
    def _publish_progress_subject(subject: _PublishingSubject) -> dict[str, object]:
        return {
            "run_id": subject.run_id,
            "repo_id": subject.monitored_repo_id,
            "repo_full_name": subject.repo_full_name,
            "base_branch": subject.base_branch,
            "delivery_branch": subject.delivery_branch,
            "base_sha": subject.base_sha,
            "head_sha": subject.head_sha,
            "head_tree_sha": subject.head_tree_sha,
            "patch_sha256": subject.patch_sha256,
        }

    def _create_intent_receipt(self, subject: _PublishingSubject) -> dict[str, object]:
        return {
            "schema_version": 2,
            "kind": "pull_request_create_intent",
            "subject": self._publish_progress_subject(subject),
        }

    def _is_bound_history_reconciliation_receipt(
        self,
        subject: _PublishingSubject,
        value: object,
    ) -> bool:
        """Recognize an old ambiguous receipt that is safe to reconcile.

        A Run already bound to a PR can never enter the PR-create path below,
        so replay may only inspect that identity and idempotently advance its
        frozen branch.  This specifically recovers receipts produced when an
        older publisher compared a repair cycle's new head with the open PR's
        previous head before pushing the branch.
        """

        return (
            subject.pr_number is not None
            and subject.pr_url is not None
            and isinstance(value, dict)
            and set(value) == {"schema_version", "kind", "subject", "reason"}
            and value.get("schema_version") == 2
            and value.get("kind") == "pull_request_history_ambiguous"
            and value.get("subject") == self._publish_progress_subject(subject)
            and value.get("reason") == _LEGACY_BOUND_STALE_HEAD_REASON
        )

    async def _load_create_intent(
        self,
        subject: _PublishingSubject,
        fence: DeliveryEffectFence,
    ) -> bool:
        """Return whether this exact Action has crossed the PR-create boundary.

        Once the intent is durable, an empty GitHub list is no longer proof that
        creating another PR is safe: the first response may have been lost and
        list visibility may lag.  Only reconciliation may follow an intent.
        """

        async with self._db_factory() as db:
            await self._assert_effect_fence(subject, fence, db=db)
            action = await db.get(
                DeliveryAction,
                fence.action_id,
                populate_existing=True,
            )
            if action is None:
                raise DeliverySubjectChanged("Delivery publish action disappeared")
            result = action.result
            if result is None and action.remote_id is None and action.remote_url is None:
                await db.rollback()
                return False
            if (
                action.remote_id is None
                and action.remote_url is None
                and self._is_bound_history_reconciliation_receipt(subject, result)
            ):
                await db.rollback()
                return False
            expected = self._create_intent_receipt(subject)
            if (
                result != expected
                or action.remote_id is not None
                or action.remote_url is not None
            ):
                raise DeliverySubjectChanged(
                    "Delivery publish action has an incompatible remote receipt"
                )
            await db.rollback()
            return True

    async def _record_create_intent(
        self,
        subject: _PublishingSubject,
        fence: DeliveryEffectFence,
    ) -> None:
        """Commit the no-recreate barrier before invoking GitHub's create API."""

        async with self._db_factory() as db:
            run = (
                await db.execute(
                    select(DeliveryRun)
                    .where(DeliveryRun.id == subject.run_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            action = (
                await db.execute(
                    select(DeliveryAction)
                    .where(DeliveryAction.id == fence.action_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            await self._assert_effect_fence(subject, fence, db=db)
            if run is None or action is None:
                raise DeliverySubjectChanged("Delivery publish action disappeared")
            expected = self._create_intent_receipt(subject)
            if action.result is None:
                if action.remote_id is not None or action.remote_url is not None:
                    raise DeliverySubjectChanged(
                        "Delivery publish action has incomplete remote evidence"
                    )
                action.result = expected
            elif (
                action.result != expected
                or action.remote_id is not None
                or action.remote_url is not None
            ):
                raise DeliverySubjectChanged(
                    "Delivery publish action already crossed another effect boundary"
                )
            await db.commit()

    async def _record_terminal_pull_request_conflict(
        self,
        subject: _PublishingSubject,
        fence: DeliveryEffectFence,
        *,
        kind: str,
        reason: str,
        snapshot: _PullRequestSnapshot | None = None,
    ) -> None:
        """Persist evidence which makes replacement PR creation fail closed.

        The Action intentionally remains leased here.  The Controller owns its
        status transition, while this schema-v2 receipt is already sufficient
        for crash-before-catch recovery to distinguish the row from an unused
        publish Action.
        """

        if kind not in {
            "pull_request_history_conflict",
            "pull_request_history_ambiguous",
            "pull_request_create_unresolved",
        }:
            raise ValueError("unsupported Delivery PR conflict kind")
        async with self._db_factory() as db:
            run = (
                await db.execute(
                    select(DeliveryRun)
                    .where(DeliveryRun.id == subject.run_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            action = (
                await db.execute(
                    select(DeliveryAction)
                    .where(DeliveryAction.id == fence.action_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            await self._assert_effect_fence(subject, fence, db=db)
            if run is None or action is None:
                raise DeliverySubjectChanged("Delivery publish action disappeared")
            bound_reconciliation = (
                kind == "pull_request_history_ambiguous"
                and snapshot is None
                and action.remote_id is None
                and action.remote_url is None
                and self._is_bound_history_reconciliation_receipt(
                    subject,
                    action.result,
                )
            )
            if (
                action.result not in (None, self._create_intent_receipt(subject))
                and not bound_reconciliation
            ):
                raise DeliverySubjectChanged(
                    "Delivery publish action already has different remote evidence"
                )

            stored_reason = (
                f"Bound PR reconciliation failed after legacy receipt recovery: "
                f"{reason}"
                if bound_reconciliation
                else reason
            )
            receipt: dict[str, object] = {
                "schema_version": 2,
                "kind": kind,
                "subject": self._publish_progress_subject(subject),
                "reason": stored_reason[:1000],
            }
            if kind == "pull_request_history_conflict" and snapshot is None:
                raise DeliverySubjectChanged(
                    "Historical PR conflict is missing its exact remote identity"
                )
            if kind != "pull_request_history_conflict" and snapshot is not None:
                raise DeliverySubjectChanged(
                    "Only an exact historical PR conflict may bind remote identity"
                )
            if snapshot is not None:
                pull_request = snapshot.pull_request
                if snapshot.state not in {"closed", "merged"}:
                    raise DeliverySubjectChanged(
                        "Only terminal PR history may become a conflict receipt"
                    )
                if (run.pr_number, run.pr_url) not in (
                    (None, None),
                    (pull_request.pr_number, pull_request.url),
                ):
                    raise DeliverySubjectChanged(
                        "Delivery Run is already bound to another remote PR"
                    )
                receipt["remote"] = {
                    "state": snapshot.state,
                    "repo_id": pull_request.repo_id,
                    "pr_number": pull_request.pr_number,
                    "url": pull_request.url,
                    "base_sha": pull_request.base_sha,
                    "head_sha": pull_request.head_sha,
                    "head_branch": pull_request.head_branch,
                    "head_repo_full_name": pull_request.head_repo_full_name,
                }
                action.remote_id = str(pull_request.pr_number)
                action.remote_url = pull_request.url
                run.pr_number = pull_request.pr_number
                run.pr_url = pull_request.url
                run.updated_at = datetime.utcnow()
            elif action.remote_id is not None or action.remote_url is not None:
                raise DeliverySubjectChanged(
                    "Unresolved PR creation has unexpected remote identity"
                )
            action.result = receipt
            action.last_error = stored_reason[:2000]
            await db.commit()

    def _pull_request_body(self, subject: _PublishingSubject) -> str:
        marker = (
            f"<!-- ccm-delivery run={subject.run_id} "
            f"head={subject.head_sha} -->"
        )
        prefix = f"{marker}\n\n"
        suffix = "\n\n---\nCreated by CCM Delivery Loop."
        budget = _MAX_PR_BODY_BYTES - len((prefix + suffix).encode("utf-8"))
        raw = subject.requirements.encode("utf-8")
        if len(raw) > budget:
            raw = raw[: max(0, budget - len("\n\n[truncated]".encode()))]
            while True:
                try:
                    requirements = raw.decode("utf-8")
                    break
                except UnicodeDecodeError:
                    raw = raw[:-1]
            requirements += "\n\n[truncated]"
        else:
            requirements = subject.requirements
        return prefix + requirements + suffix

    async def _open_pull_request_or_record_conflict(
        self,
        subject: _PublishingSubject,
        fence: DeliveryEffectFence,
        snapshot: _PullRequestSnapshot,
    ) -> PublishedPullRequest:
        if snapshot.state == "open":
            return snapshot.pull_request
        reason = (
            f"Exact Delivery pull request #{snapshot.pull_request.pr_number} "
            f"is already {snapshot.state}; replacement creation is forbidden"
        )
        await self._record_terminal_pull_request_conflict(
            subject,
            fence,
            kind="pull_request_history_conflict",
            reason=reason,
            snapshot=snapshot,
        )
        raise DeliveryPublisherPermanentError(reason)

    async def ensure_pull_request(
        self,
        *,
        run_id: int,
        idempotency_key: str,
        fence: DeliveryEffectFence,
    ) -> PublishedPullRequest:
        try:
            subject = await self._load_subject(run_id)
            self._validate_idempotency_key(subject, idempotency_key, monitor=False)
            await self._assert_effect_fence(subject, fence)
            await self._assert_local_subject(subject)
            create_intent = await self._load_create_intent(subject, fence)
        except DeliveryPublisherNoEffectPreflightError:
            raise
        except DeliveryPublisherPermanentError as exc:
            raise DeliveryPublisherNoEffectPreflightError(str(exc)) from exc

        # First inspect all historical PRs before changing the remote branch.
        # This prevents a retry from recreating a branch merely to replace a PR
        # which was already closed after CCM lost the creation response.
        try:
            await self._assert_remote_base(subject)
        except DeliveryGitAuthenticationError as exc:
            raise DeliveryPublisherNoEffectPreflightError(
                "GitHub credentials are unavailable before Delivery publication"
            ) from exc
        except DeliveryPublisherPermanentError as base_error:
            try:
                historical = await self._find_existing_pull_request(subject)
            except Exception:
                raise DeliveryPublisherNoEffectPreflightError(
                    str(base_error)
                ) from base_error
            if historical is not None and historical.state != "open":
                return await self._open_pull_request_or_record_conflict(
                    subject,
                    fence,
                    historical,
                )
            raise DeliveryPublisherNoEffectPreflightError(
                str(base_error)
            ) from base_error

        try:
            existing = await self._find_existing_pull_request(
                subject,
                # A later repair cycle is already durably bound to this PR.
                # Its open PR necessarily exposes the previous cycle's head
                # until the exact non-force push below advances the branch.
                # Number, URL, repo and branch identity remain strict here;
                # every post-push read uses exact-head validation again.
                allow_bound_open_stale_head=True,
            )
        except DeliveryPublisherPermanentError as exc:
            reason = f"Delivery pull-request history is ambiguous: {exc}"
            await self._record_terminal_pull_request_conflict(
                subject,
                fence,
                kind="pull_request_history_ambiguous",
                reason=reason,
            )
            raise DeliveryPublisherPermanentError(reason) from exc
        preexisting_open: PublishedPullRequest | None = None
        if existing is not None:
            preexisting_open = await self._open_pull_request_or_record_conflict(
                subject,
                fence,
                existing,
            )
        if create_intent:
            if preexisting_open is not None:
                await self._assert_subject(subject, require_remote_head=True)
                await self._assert_effect_fence(subject, fence)
                return preexisting_open
            reason = (
                "A prior GitHub pull-request creation attempt has no uniquely "
                "recoverable remote identity; replacement creation is forbidden"
            )
            await self._record_terminal_pull_request_conflict(
                subject,
                fence,
                kind="pull_request_create_unresolved",
                reason=reason,
            )
            raise DeliveryPublisherPermanentError(reason)

        # Side effect 1: exact non-force branch publication.  A lost response
        # is recovered by reading the exact remote ref before deciding whether
        # the error is still relevant.
        try:
            remote_head = await self._git.remote_ref_sha(
                subject, subject.delivery_branch
            )
        except DeliveryGitAuthenticationError as exc:
            raise DeliveryPublisherNoEffectPreflightError(
                "GitHub credentials are unavailable before Delivery publication"
            ) from exc
        if remote_head != subject.head_sha:
            await self._assert_subject(subject, require_remote_head=False)
            await self._assert_effect_fence(subject, fence)
            try:
                await self._git.push_exact(subject)
            except DeliveryGitAuthenticationError as exc:
                raise DeliveryPublisherNoEffectPreflightError(
                    "GitHub credentials are unavailable for Delivery publication"
                ) from exc
            except DeliveryNonFastForwardError as exc:
                # A rejected non-force ref update proves that this attempt did
                # not mutate the remote branch.
                raise DeliveryPublisherNoEffectPreflightError(
                    "Remote delivery branch cannot be advanced without force"
                ) from exc
            except DeliveryGitError:
                recovered = await self._git.remote_ref_sha(
                    subject, subject.delivery_branch
                )
                if recovered != subject.head_sha:
                    raise
            await self._assert_subject(subject, require_remote_head=True)
            try:
                existing = await self._find_existing_pull_request(subject)
            except DeliveryPublisherPermanentError as exc:
                reason = f"Delivery pull-request history is ambiguous: {exc}"
                await self._record_terminal_pull_request_conflict(
                    subject,
                    fence,
                    kind="pull_request_history_ambiguous",
                    reason=reason,
                )
                raise DeliveryPublisherPermanentError(reason) from exc
            if existing is not None:
                pull_request = await self._open_pull_request_or_record_conflict(
                    subject,
                    fence,
                    existing,
                )
                await self._assert_subject(subject, require_remote_head=True)
                await self._assert_effect_fence(subject, fence)
                return pull_request
        else:
            await self._assert_subject(subject, require_remote_head=True)
        await self._assert_effect_fence(subject, fence)
        if preexisting_open is not None:
            if preexisting_open.head_sha != subject.head_sha:
                # The remote ref may already have advanced while GitHub's PR
                # list response was stale.  Never return the relaxed snapshot
                # as publication evidence: re-read and require the exact head.
                try:
                    refreshed = await self._find_existing_pull_request(subject)
                except DeliveryPublisherPermanentError as exc:
                    reason = f"Delivery pull-request history is ambiguous: {exc}"
                    await self._record_terminal_pull_request_conflict(
                        subject,
                        fence,
                        kind="pull_request_history_ambiguous",
                        reason=reason,
                    )
                    raise DeliveryPublisherPermanentError(reason) from exc
                if refreshed is None:
                    raise DeliveryPublisherPermanentError(
                        "Bound pull request did not expose the published head"
                    )
                return await self._open_pull_request_or_record_conflict(
                    subject,
                    fence,
                    refreshed,
                )
            return preexisting_open
        if subject.pr_number is not None or subject.pr_url is not None:
            raise DeliveryPublisherPermanentError(
                "Bound pull request cannot be recovered by its frozen branch"
            )

        # Side effect 2: PR creation.  GitHub has no caller-provided idempotency
        # key and closed PRs do not retain its open-PR uniqueness constraint.
        # Commit an intent before the request: after this point only state=all
        # reconciliation may run, and an empty result is terminally ambiguous.
        await self._assert_subject(subject, require_remote_head=True)
        await self._assert_effect_fence(subject, fence)
        await self._record_create_intent(subject, fence)
        await self._assert_effect_fence(subject, fence)
        try:
            created = await self._github.create_pull_request(
                repo_full_name=subject.repo_full_name,
                title=subject.title,
                body=self._pull_request_body(subject),
                head_branch=subject.delivery_branch,
                base_branch=subject.base_branch,
            )
            created_snapshot = self._validate_pull_request_snapshot(subject, created)
            published = await self._open_pull_request_or_record_conflict(
                subject,
                fence,
                created_snapshot,
            )
        except asyncio.CancelledError:
            raise
        except DeliveryPublisherPermanentError:
            raise
        except Exception as create_error:
            try:
                await self._assert_subject(subject, require_remote_head=True)
                await self._assert_effect_fence(subject, fence)
                recovered = await self._find_existing_pull_request(subject)
            except asyncio.CancelledError:
                raise
            except Exception as recovery_error:
                reason = (
                    "GitHub PR creation crossed its durable intent but remote "
                    f"identity reconciliation failed: {type(recovery_error).__name__}: "
                    f"{str(recovery_error)[:500]}"
                )
                await self._record_terminal_pull_request_conflict(
                    subject,
                    fence,
                    kind="pull_request_create_unresolved",
                    reason=reason,
                )
                raise DeliveryPublisherPermanentError(reason) from create_error
            if recovered is None:
                reason = (
                    "GitHub PR creation crossed its durable intent but no unique "
                    "remote identity can be proven; replacement creation is forbidden"
                )
                await self._record_terminal_pull_request_conflict(
                    subject,
                    fence,
                    kind="pull_request_create_unresolved",
                    reason=reason,
                )
                raise DeliveryPublisherPermanentError(reason) from create_error
            published = await self._open_pull_request_or_record_conflict(
                subject,
                fence,
                recovered,
            )

        await self._assert_subject(subject, require_remote_head=True)
        await self._assert_effect_fence(subject, fence)
        confirmed_snapshot = self._validate_pull_request_snapshot(
            subject,
            await self._github.get_pull_request(
                repo_full_name=subject.repo_full_name,
                pr_number=published.pr_number,
            ),
        )
        confirmed = await self._open_pull_request_or_record_conflict(
            subject,
            fence,
            confirmed_snapshot,
        )
        await self._assert_subject(subject, require_remote_head=True)
        await self._assert_effect_fence(subject, fence)
        return confirmed

    def _assert_published_argument(
        self,
        subject: _PublishingSubject,
        pull_request: PublishedPullRequest,
    ) -> None:
        if (
            pull_request.repo_id != subject.monitored_repo_id
            or pull_request.pr_number <= 0
            or pull_request.base_sha != subject.base_sha
            or pull_request.head_sha != subject.head_sha
            or pull_request.head_branch != subject.delivery_branch
            or pull_request.head_repo_full_name is None
            or pull_request.head_repo_full_name.lower()
            != subject.repo_full_name.lower()
        ):
            raise DeliveryPublisherPermanentError(
                "PR Monitor request does not match the published subject"
            )

    async def _exact_review_and_monitor(
        self,
        subject: _PublishingSubject,
        pull_request: PublishedPullRequest,
        *,
        attach_missing: bool,
        fence: DeliveryEffectFence,
    ) -> int | None:
        async with self._db_factory() as db:
            # FindingAction/Rebuttal writers take this portable repository
            # fence before locking the Review/Finding rows.  Adoption must use
            # the same order; locking only the Review would still allow a
            # legacy effect to commit between the empty-effect query and the
            # Delivery marker update.
            try:
                repo = await lock_pr_repo_action_boundary(
                    db,
                    subject.monitored_repo_id,
                )
            except FindingActionConflict as exc:
                raise DeliveryPublisherPermanentError(
                    "Monitored repository disappeared"
                ) from exc
            await self._assert_effect_fence(subject, fence, db=db)
            marker_review = (
                await db.execute(
                    select(PRReview)
                    .where(
                        PRReview.repo_id == repo.id,
                        PRReview.delivery_id == subject.delivery_id,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if marker_review is not None and (
                marker_review.pr_number != pull_request.pr_number
                or marker_review.base_ref != subject.base_branch
                or marker_review.base_sha != subject.base_sha
                or marker_review.head_sha != subject.head_sha
            ):
                raise DeliveryPublisherPermanentError(
                    "Delivery review id is bound to another PR subject"
                )
            review = (
                await db.execute(
                    select(PRReview)
                    .where(
                        PRReview.repo_id == repo.id,
                        PRReview.pr_number == pull_request.pr_number,
                        PRReview.base_ref == subject.base_branch,
                        PRReview.base_sha == subject.base_sha,
                        PRReview.head_sha == subject.head_sha,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if review is None:
                return None
            if review.pr_url.rstrip("/") != pull_request.url.rstrip("/"):
                raise DeliveryPublisherPermanentError(
                    "Exact PR Review URL belongs to another pull request"
                )
            if marker_review is not None and marker_review.id != review.id:
                raise DeliveryPublisherPermanentError(
                    "Delivery review id is already owned by another Review"
                )
            if review.delivery_id != subject.delivery_id:
                if (
                    isinstance(review.delivery_id, str)
                    and review.delivery_id.startswith("delivery:")
                ):
                    raise DeliveryPublisherPermanentError(
                        "Exact PR Review belongs to another Delivery Run"
                    )
                if (
                    review.status not in {"pending", "waiting_ci", "reviewing"}
                    or review.action_taken is not None
                    or review.pending_action is not None
                    or review.pending_review_body is not None
                    or review.publishing_actor is not None
                    or review.publishing_retry_count is not None
                    or review.publishing_task_started_at is not None
                    or review.publishing_started_at is not None
                    or review.publishing_lease_token is not None
                    or review.publishing_lease_expires_at is not None
                    or review.superseding_snapshot is not None
                    or review.superseding_token is not None
                    or review.superseding_started_at is not None
                    or review.completed_at is not None
                ):
                    # A legacy publisher may already be between its durable
                    # outbox claim and a GitHub mutation.  Changing ownership
                    # after that point cannot revoke an in-flight external
                    # effect, so fail closed instead of retroactively applying
                    # Delivery's no-merge marker.
                    raise DeliveryPublisherPermanentError(
                        "Webhook Review already started a legacy publication "
                        "effect and cannot be adopted by Delivery"
                    )
                legacy_action = await db.scalar(
                    select(PRFindingAction.id)
                    .join(PRFinding, PRFinding.id == PRFindingAction.finding_id)
                    .where(PRFinding.pr_review_id == review.id)
                    .limit(1)
                )
                legacy_rebuttal = await db.scalar(
                    select(PRFindingRebuttal.id)
                    .where(PRFindingRebuttal.pr_review_id == review.id)
                    .limit(1)
                )
                if legacy_action is not None or legacy_rebuttal is not None:
                    raise DeliveryPublisherPermanentError(
                        "Webhook Review already has a legacy Finding effect and "
                        "cannot be adopted by Delivery"
                    )
                reviewer_task_ids = set(
                    (
                        await db.execute(
                            select(PRReviewerRun.task_id).where(
                                PRReviewerRun.pr_review_id == review.id,
                                PRReviewerRun.task_id.is_not(None),
                            )
                        )
                    ).scalars()
                )
                if review.task_id is not None:
                    reviewer_task_ids.add(review.task_id)
                if reviewer_task_ids:
                    reviewer_tasks = list(
                        (
                            await db.execute(
                                select(Task).where(Task.id.in_(reviewer_task_ids))
                            )
                        ).scalars()
                    )
                    expected_auto_merge = subject.policy_snapshot.get(
                        "auto_merge"
                    )
                    if len(reviewer_tasks) != len(reviewer_task_ids) or any(
                        (
                            (task.metadata_ or {}).get("pr_review_id")
                            != review.id
                            or (task.metadata_ or {}).get("pr_base_ref")
                            != subject.base_branch
                            or (task.metadata_ or {}).get("pr_base_sha")
                            != subject.base_sha
                            or (task.metadata_ or {}).get("pr_head_sha")
                            != subject.head_sha
                            or (task.metadata_ or {}).get("pr_auto_merge")
                            is not expected_auto_merge
                        )
                        for task in reviewer_tasks
                    ):
                        raise DeliveryPublisherPermanentError(
                            "Webhook Review Task policy does not match the "
                            "frozen Delivery merge policy"
                        )
                # An opened webhook can win the natural-key race and create
                # this exact Review with its opaque GitHub delivery id.  Adopt
                # it under the row lock before attaching/returning so every
                # later PR publication path is permanently constrained by the
                # owning Run's frozen merge policy.  The exact-subject and
                # delivery-id unique constraints make concurrent adoption
                # fail closed.
                review.delivery_id = subject.delivery_id
                await db.flush()
            monitor = (
                await db.get(
                    PRMonitorRun,
                    review.monitor_run_id,
                    populate_existing=True,
                )
                if review.monitor_run_id is not None
                else None
            )
            if monitor is not None and (
                monitor.repo_id != repo.id
                or monitor.pr_number != pull_request.pr_number
                or monitor.current_base_sha != subject.base_sha
                or monitor.current_head_sha != subject.head_sha
                or monitor.current_review_id != review.id
            ):
                raise DeliveryPublisherPermanentError(
                    "Exact PR Review points at a different Monitor subject"
                )
            if (
                review.monitor_run_id is None
                or monitor is None
                or monitor.head_repo_full_name is None
                or monitor.head_branch is None
            ):
                if not attach_missing:
                    return None
                monitor = await self._review_attacher(
                    db,
                    repo=repo,
                    review=review,
                    pr_data={
                        "head_repo_full_name": subject.repo_full_name,
                        "head_branch": subject.delivery_branch,
                    },
                )
            if monitor is None:
                raise DeliveryPublisherPermanentError(
                    "Exact PR Review lost its Monitor Run"
                )
            if (
                monitor.repo_id != repo.id
                or monitor.pr_number != pull_request.pr_number
                or monitor.current_base_sha != subject.base_sha
                or monitor.current_head_sha != subject.head_sha
                or monitor.current_review_id != review.id
                or (
                    monitor.head_repo_full_name is not None
                    and monitor.head_repo_full_name.lower()
                    != subject.repo_full_name.lower()
                )
                or monitor.head_branch not in (None, subject.delivery_branch)
                or (
                    monitor.developer_task_id is not None
                    and monitor.developer_task_id != subject.developer_task_id
                )
            ):
                raise DeliveryPublisherPermanentError(
                    "PR Monitor Run belongs to another exact subject"
                )
            if (
                subject.pr_monitor_run_id is not None
                and subject.pr_monitor_run_id != monitor.id
            ):
                raise DeliveryPublisherPermanentError(
                    "Delivery Run is already bound to another Monitor Run"
                )
            # ``attach_review_to_run`` commits when it had work to do.  The
            # exact Monitor may already have been attached by the webhook, in
            # which case this commit is what makes Delivery ownership durable.
            await db.commit()
            return monitor.id

    async def ensure_monitor(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        idempotency_key: str,
        fence: DeliveryEffectFence,
    ) -> int:
        try:
            subject = await self._load_subject(run_id)
            self._validate_idempotency_key(subject, idempotency_key, monitor=True)
            self._assert_published_argument(subject, pull_request)
            await self._assert_effect_fence(subject, fence)
            await self._assert_subject(subject, require_remote_head=True)
            github_pr = await self._github.get_pull_request(
                repo_full_name=subject.repo_full_name,
                pr_number=pull_request.pr_number,
            )
            exact_pr = self._validate_pull_request(subject, github_pr)
            if exact_pr != pull_request:
                raise DeliveryPublisherPermanentError(
                    "Published PR argument is not the current GitHub subject"
                )
            remote_title = github_pr.get("title")
            remote_user = github_pr.get("user")
            remote_author = (
                remote_user.get("login") if isinstance(remote_user, dict) else None
            )
            if (
                not isinstance(remote_title, str)
                or not remote_title
                or len(remote_title) > 500
                or not isinstance(remote_author, str)
                or not remote_author
                or len(remote_author) > 200
            ):
                raise DeliveryPublisherPermanentError(
                    "GitHub PR metadata is incomplete"
                )
        except DeliveryPublisherNoEffectPreflightError:
            raise
        except DeliveryPublisherPermanentError as exc:
            # The PR receipt already exists, but no Review/Monitor mutation has
            # begun in this call.  This lets the controller fail a deterministic
            # bad policy/argument while retaining that receipt for operators.
            raise DeliveryPublisherNoEffectPreflightError(str(exc)) from exc

        monitor_id = await self._exact_review_and_monitor(
            subject,
            pull_request,
            attach_missing=True,
            fence=fence,
        )
        if monitor_id is not None:
            await self._assert_subject(subject, require_remote_head=True)
            await self._assert_effect_fence(subject, fence)
            return monitor_id

        pr_data = {
            "number": pull_request.pr_number,
            "base_ref": subject.base_branch,
            "base_sha": subject.base_sha,
            "head_sha": subject.head_sha,
            "delivery_id": subject.delivery_id,
            "title": remote_title,
            "author": remote_author,
            "url": pull_request.url,
            "head_repo_full_name": subject.repo_full_name,
            "head_branch": subject.delivery_branch,
        }
        await self._assert_subject(subject, require_remote_head=True)
        await self._assert_effect_fence(subject, fence)
        try:
            async with self._db_factory() as db:
                await self._assert_effect_fence(subject, fence, db=db)
                repo = await db.get(
                    MonitoredRepo,
                    subject.monitored_repo_id,
                    populate_existing=True,
                )
                if repo is None:
                    raise DeliveryPublisherPermanentError(
                        "Monitored repository disappeared"
                    )
                await self._review_creator(db, repo, pr_data)
        except IntegrityError:
            # Concurrent webhook/controller creation is resolved by the exact
            # database natural keys below, never by creating a second review.
            pass

        await self._assert_subject(subject, require_remote_head=True)
        await self._assert_effect_fence(subject, fence)
        monitor_id = await self._exact_review_and_monitor(
            subject,
            pull_request,
            attach_missing=True,
            fence=fence,
        )
        if monitor_id is None:
            raise DeliverySubjectChanged(
                "PR Review creation did not establish an exact Monitor Run"
            )
        await self._assert_subject(subject, require_remote_head=True)
        await self._assert_effect_fence(subject, fence)
        return monitor_id

    async def _assert_ready_monitor_snapshot(
        self,
        subject: _PublishingSubject,
        *,
        monitor_run_id: int,
        expected_monitor_state_version: int,
    ) -> None:
        if (
            isinstance(monitor_run_id, bool)
            or not isinstance(monitor_run_id, int)
            or monitor_run_id <= 0
            or isinstance(expected_monitor_state_version, bool)
            or not isinstance(expected_monitor_state_version, int)
            or expected_monitor_state_version < 1
        ):
            raise DeliveryPublisherPermanentError(
                "Invalid ready-to-merge Monitor snapshot"
            )
        async with self._db_factory() as db:
            monitor = await db.get(
                PRMonitorRun,
                monitor_run_id,
                populate_existing=True,
            )
            review = (
                await db.get(
                    PRReview,
                    monitor.current_review_id,
                    populate_existing=True,
                )
                if monitor is not None and monitor.current_review_id is not None
                else None
            )
            if (
                subject.pr_number is None
                or subject.pr_url is None
                or subject.pr_monitor_run_id != monitor_run_id
                or monitor is None
                or monitor.state_version != expected_monitor_state_version
                or monitor.status != "ready_to_merge"
                or monitor.repo_id != subject.monitored_repo_id
                or monitor.pr_number != subject.pr_number
                or monitor.current_base_sha != subject.base_sha
                or monitor.current_head_sha != subject.head_sha
                or monitor.head_repo_full_name is None
                or monitor.head_repo_full_name.lower()
                != subject.repo_full_name.lower()
                or monitor.head_branch != subject.delivery_branch
                or review is None
                or review.delivery_id != subject.delivery_id
                or review.monitor_run_id != monitor.id
                or review.repo_id != subject.monitored_repo_id
                or review.pr_number != subject.pr_number
                or review.base_ref != subject.base_branch
                or review.base_sha != subject.base_sha
                or review.head_sha != subject.head_sha
                or review.pr_url.rstrip("/") != subject.pr_url.rstrip("/")
            ):
                raise DeliverySubjectChanged(
                    "PR Monitor ready snapshot no longer matches the Delivery subject"
                )

    async def verify_ready_to_merge(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        monitor_run_id: int,
        expected_monitor_state_version: int,
    ) -> PublishedPullRequest:
        """Read-only remote proof for one locally ready-to-merge snapshot.

        The controller must still re-lock and compare the Monitor state version
        after this method returns.  The checks here close the webhook-delay gap:
        a locally green H1 cannot finish after either the remote ref or GitHub
        PR has already advanced to H2.
        """

        subject = await self._load_subject(
            run_id,
            expected_states=frozenset({("monitoring", "waiting")}),
        )
        if not isinstance(pull_request, PublishedPullRequest):
            raise DeliveryPublisherPermanentError(
                "Invalid ready-to-merge PR snapshot"
            )
        self._assert_published_argument(subject, pull_request)
        if (
            subject.pr_number != pull_request.pr_number
            or subject.pr_url is None
            or subject.pr_url.rstrip("/") != pull_request.url.rstrip("/")
            or subject.pr_monitor_run_id != monitor_run_id
        ):
            raise DeliverySubjectChanged(
                "Delivery Run PR binding changed before remote verification"
            )
        await self._assert_ready_monitor_snapshot(
            subject,
            monitor_run_id=monitor_run_id,
            expected_monitor_state_version=expected_monitor_state_version,
        )
        await self._assert_subject(subject, require_remote_head=True)
        remote_pr = await self._github.get_pull_request(
            repo_full_name=subject.repo_full_name,
            pr_number=pull_request.pr_number,
        )
        try:
            verified = self._validate_pull_request(subject, remote_pr)
        except DeliveryPublisherPermanentError as exc:
            # A synchronize/close webhook may simply be behind GitHub.  This
            # exact local Monitor snapshot is stale, but the publisher itself
            # is not permanently misconfigured.
            raise DeliverySubjectChanged(
                "GitHub PR no longer matches the locally ready subject"
            ) from exc
        if verified != pull_request:
            raise DeliverySubjectChanged(
                "GitHub PR changed during ready-to-merge verification"
            )
        # Re-read both external refs and durable Monitor state after GitHub.
        # This does not replace the controller's final locked CAS; it ensures
        # the gateway never returns a snapshot it already knows is stale.
        await self._assert_subject(subject, require_remote_head=True)
        await self._assert_ready_monitor_snapshot(
            subject,
            monitor_run_id=monitor_run_id,
            expected_monitor_state_version=expected_monitor_state_version,
        )
        return verified

    async def _assert_merged_monitor_snapshot(
        self,
        subject: _PublishingSubject,
        *,
        monitor_run_id: int,
        expected_monitor_state_version: int,
    ) -> _MergedMonitorEvidence:
        """Return the exact frozen evidence policy for one merged terminal."""

        if (
            isinstance(monitor_run_id, bool)
            or not isinstance(monitor_run_id, int)
            or monitor_run_id <= 0
            or isinstance(expected_monitor_state_version, bool)
            or not isinstance(expected_monitor_state_version, int)
            or expected_monitor_state_version < 1
        ):
            raise DeliveryPublisherPermanentError(
                "Invalid merged Monitor snapshot"
            )
        async with self._db_factory() as db:
            monitor = await db.get(
                PRMonitorRun,
                monitor_run_id,
                populate_existing=True,
            )
            review = (
                await db.get(
                    PRReview,
                    monitor.current_review_id,
                    populate_existing=True,
                )
                if monitor is not None and monitor.current_review_id is not None
                else None
            )
            task = (
                await db.get(Task, review.task_id, populate_existing=True)
                if review is not None and review.task_id is not None
                else None
            )
            nonce = review.action_nonce if review is not None else None
            actor = review.publishing_actor if review is not None else None
            publishing_started_at = (
                review.publishing_started_at if review is not None else None
            )
            merge_method = review.merge_method if review is not None else None
            if (
                subject.policy_snapshot.get("auto_merge") is not True
                or subject.policy_snapshot.get("terminal") != "merged"
                or subject.pr_number is None
                or subject.pr_url is None
                or subject.pr_monitor_run_id != monitor_run_id
                or monitor is None
                or monitor.state_version != expected_monitor_state_version
                or monitor.status != "merged"
                or monitor.repo_id != subject.monitored_repo_id
                or monitor.pr_number != subject.pr_number
                or monitor.current_base_sha != subject.base_sha
                or monitor.current_head_sha != subject.head_sha
                or monitor.head_repo_full_name is None
                or monitor.head_repo_full_name.lower()
                != subject.repo_full_name.lower()
                or monitor.head_branch != subject.delivery_branch
                or review is None
                or review.delivery_id != subject.delivery_id
                or review.monitor_run_id != monitor.id
                or review.repo_id != subject.monitored_repo_id
                or review.pr_number != subject.pr_number
                or review.base_ref != subject.base_branch
                or review.base_sha != subject.base_sha
                or review.head_sha != subject.head_sha
                or review.pr_url.rstrip("/") != subject.pr_url.rstrip("/")
                or review.status != "merged"
                or review.action_taken != "approved_merged"
                or review.completed_at is None
                or review.pending_action is not None
                or review.publishing_lease_token is not None
                or not isinstance(nonce, str)
                or _ACTION_NONCE_RE.fullmatch(nonce) is None
                or not isinstance(actor, str)
                or not actor
                or not isinstance(publishing_started_at, datetime)
                or merge_method not in {"merge", "squash", "fast-forward"}
                or task is None
                or task.status != "completed"
                or (task.metadata_ or {}).get("pr_review_id") != review.id
                or (task.metadata_ or {}).get("pr_base_ref")
                != subject.base_branch
                or (task.metadata_ or {}).get("pr_base_sha")
                != subject.base_sha
                or (task.metadata_ or {}).get("pr_head_sha")
                != subject.head_sha
                or (task.metadata_ or {}).get("pr_auto_merge") is not True
                or (task.metadata_ or {}).get("pr_action_nonce") != nonce
            ):
                raise DeliverySubjectChanged(
                    "PR Monitor merged snapshot no longer matches the "
                    "Delivery subject"
                )
            return _MergedMonitorEvidence(
                nonce=nonce,
                actor=actor,
                publishing_started_at=publishing_started_at,
                merge_method=merge_method,
            )

    async def verify_merged(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        monitor_run_id: int,
        expected_monitor_state_version: int,
    ) -> PublishedPullRequest:
        """Verify the exact CCM merge after base/head refs may have moved."""

        subject = await self._load_subject(
            run_id,
            expected_states=frozenset({("monitoring", "waiting")}),
        )
        if not isinstance(pull_request, PublishedPullRequest):
            raise DeliveryPublisherPermanentError(
                "Invalid merged PR snapshot"
            )
        self._assert_published_argument(subject, pull_request)
        if (
            subject.pr_number != pull_request.pr_number
            or subject.pr_url is None
            or subject.pr_url.rstrip("/") != pull_request.url.rstrip("/")
            or subject.pr_monitor_run_id != monitor_run_id
        ):
            raise DeliverySubjectChanged(
                "Delivery Run PR binding changed before merge verification"
            )
        evidence = await self._assert_merged_monitor_snapshot(
            subject,
            monitor_run_id=monitor_run_id,
            expected_monitor_state_version=expected_monitor_state_version,
        )
        # The merge legitimately advances the base ref and repositories may
        # delete the head branch.  Verify the immutable PR/merge-commit
        # evidence instead of reusing the open-PR ref checks.
        try:
            merge_confirmed = await _find_merge_evidence(
                repo_name=subject.repo_full_name,
                pr_number=pull_request.pr_number,
                base_ref=subject.base_branch,
                base_sha=subject.base_sha,
                head_sha=subject.head_sha,
                nonce=evidence.nonce,
                actor=evidence.actor,
                publishing_started_at=evidence.publishing_started_at,
                merge_method=evidence.merge_method,
            )
        except GhError as exc:
            if _terminal_publication_error(exc):
                raise DeliverySubjectChanged(
                    "GitHub Delivery merge evidence is invalid"
                ) from exc
            raise
        if not merge_confirmed:
            raise DeliverySubjectChanged(
                "GitHub does not expose the exact Delivery merge evidence"
            )
        current = await self._load_subject(
            run_id,
            expected_states=frozenset({("monitoring", "waiting")}),
        )
        if current != subject:
            raise DeliverySubjectChanged(
                "Delivery Run changed during merge verification"
            )
        repeated_evidence = await self._assert_merged_monitor_snapshot(
            subject,
            monitor_run_id=monitor_run_id,
            expected_monitor_state_version=expected_monitor_state_version,
        )
        if repeated_evidence != evidence:
            raise DeliverySubjectChanged(
                "Delivery merge evidence policy changed during verification"
            )
        return pull_request


__all__ = [
    "DeliveryGitAuthenticationError",
    "DeliveryGitError",
    "DeliveryNonFastForwardError",
    "GhDeliveryGateway",
    "GitDeliveryGateway",
    "GitHubDeliveryPublisher",
]
