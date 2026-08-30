"""Immutable public-GitHub target resolution for Test Harness runs.

This module performs metadata-only reads.  It never invokes Git, reads a local
checkout, or executes repository content.  Source acquisition is a separate
sandbox operation which must verify the exact SHA returned here.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote, urlparse

import httpx

from backend.models.project import Project


_REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_MAX_CHANGED_FILES_RESPONSE_BYTES = 16 * 1024 * 1024
_MAX_CHANGED_FILES = 300


class GitTargetResolutionError(RuntimeError):
    """A requested remote target could not be frozen safely."""


class GitHubMetadataClient(Protocol):
    async def get(self, path: str, *, max_bytes: int = _MAX_RESPONSE_BYTES) -> Any:
        ...


class PublicGitHubMetadataClient:
    """Bounded unauthenticated reader fixed to GitHub's public REST origin."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 20.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.timeout_seconds = max(1.0, min(float(timeout_seconds), 60.0))
        self.transport = transport

    async def get(self, path: str, *, max_bytes: int = _MAX_RESPONSE_BYTES) -> Any:
        if not path.startswith("/") or "\x00" in path or len(path) > 2000:
            raise GitTargetResolutionError("GitHub metadata path is invalid")
        if not 1 <= max_bytes <= _MAX_CHANGED_FILES_RESPONSE_BYTES:
            raise GitTargetResolutionError("GitHub metadata response limit is invalid")
        url = "https://api.github.com" + path
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "CCM-Test-Harness/1",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout_seconds,
                follow_redirects=False,
                trust_env=False,
                transport=self.transport,
            ) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    if response.is_redirect:
                        raise GitTargetResolutionError(
                            "GitHub metadata redirects are not accepted"
                        )
                    if response.status_code != 200:
                        if response.status_code == 404:
                            raise GitTargetResolutionError(
                                "Public GitHub target was not found"
                            )
                        if response.status_code in {401, 403, 429}:
                            raise GitTargetResolutionError(
                                "Public GitHub metadata rate limit or access gate was reached"
                            )
                        raise GitTargetResolutionError(
                            f"GitHub metadata request failed with HTTP {response.status_code}"
                        )
                    declared = response.headers.get("content-length")
                    if declared is not None:
                        try:
                            if int(declared) > max_bytes:
                                raise GitTargetResolutionError(
                                    "GitHub metadata response exceeds the safety limit"
                                )
                        except ValueError as exc:
                            raise GitTargetResolutionError(
                                "GitHub metadata returned an invalid content length"
                            ) from exc
                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > max_bytes:
                            raise GitTargetResolutionError(
                                "GitHub metadata response exceeds the safety limit"
                            )
                        chunks.append(chunk)
        except GitTargetResolutionError:
            raise
        except (httpx.HTTPError, OSError) as exc:
            raise GitTargetResolutionError(
                f"GitHub metadata request failed: {type(exc).__name__}"
            ) from exc
        try:
            return json.loads(b"".join(chunks).decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise GitTargetResolutionError(
                "GitHub metadata response is not valid JSON"
            ) from exc


@dataclass(frozen=True, slots=True)
class ResolvedGitTarget:
    kind: str
    repository: str
    clone_url: str
    base_sha: str | None
    head_sha: str
    fetch_ref: str
    source_repository: str
    source_ref: str | None
    pr_number: int | None
    changed_files: tuple[dict[str, Any], ...]
    fingerprint: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "repository": self.repository,
            "clone_url": self.clone_url,
            "base_sha": self.base_sha,
            "head_sha": self.head_sha,
            "fetch_ref": self.fetch_ref,
            "source_repository": self.source_repository,
            "source_ref": self.source_ref,
            "pr_number": self.pr_number,
            "changed_files": [dict(item) for item in self.changed_files],
            "fingerprint": self.fingerprint,
        }


def github_repository_from_project(project: Project | None) -> str:
    if project is None or not isinstance(project.git_url, str):
        raise GitTargetResolutionError(
            "PR/ref tests require a Task Project with a GitHub remote"
        )
    raw = project.git_url.strip()
    if not raw or len(raw) > 500 or "\x00" in raw:
        raise GitTargetResolutionError("Project GitHub remote is invalid")
    path: str | None = None
    if raw.startswith("git@github.com:"):
        path = raw[len("git@github.com:") :]
    else:
        parsed = urlparse(raw)
        if (
            parsed.scheme not in {"https", "ssh"}
            or (parsed.hostname or "").lower() != "github.com"
            or parsed.query
            or parsed.fragment
            or (parsed.username not in {None, "git"})
            or parsed.password is not None
        ):
            raise GitTargetResolutionError(
                "Only canonical GitHub HTTPS/SSH Project remotes are supported"
            )
        path = parsed.path.lstrip("/")
    # Project remotes saved by the UI or copied from GitHub commonly retain the
    # repository URL's optional trailing slash.  Normalize exactly one suffix
    # before validating the owner/repository identity; repeated/path-internal
    # slashes still fail the strict two-component regex below.
    if path.endswith("/"):
        path = path[:-1]
    if path.endswith(".git"):
        path = path[:-4]
    if not path or _REPO_RE.fullmatch(path) is None:
        raise GitTargetResolutionError("Project GitHub repository identity is invalid")
    return path


def _require_sha(value: object, label: str) -> str:
    normalized = value.lower() if isinstance(value, str) else ""
    if _SHA_RE.fullmatch(normalized) is None:
        raise GitTargetResolutionError(f"GitHub {label} SHA is invalid")
    return normalized


def _require_repo(value: object, label: str) -> str:
    if not isinstance(value, str) or _REPO_RE.fullmatch(value) is None:
        raise GitTargetResolutionError(f"GitHub {label} repository is invalid")
    return value


def _require_ref(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 300
        or "\x00" in value
        or value.startswith("-")
    ):
        raise GitTargetResolutionError(f"GitHub {label} ref is invalid")
    return value


def _fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(b"ccm-public-git-target-v1\0" + encoded).hexdigest()


class PublicGitTargetResolver:
    def __init__(self, client: GitHubMetadataClient | None = None) -> None:
        self.client = client or PublicGitHubMetadataClient()

    async def resolve(
        self,
        *,
        project: Project | None,
        kind: str,
        target: dict[str, Any],
    ) -> ResolvedGitTarget:
        repository = github_repository_from_project(project)
        if kind == "pull_request":
            return await self._resolve_pull_request(repository, target)
        if kind == "git_ref":
            return await self._resolve_git_ref(repository, target)
        raise GitTargetResolutionError("unsupported public Git target kind")

    async def _resolve_pull_request(
        self,
        repository: str,
        target: dict[str, Any],
    ) -> ResolvedGitTarget:
        number = target.get("pr_number")
        if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
            raise GitTargetResolutionError("pull request number is invalid")
        payload = await self.client.get(f"/repos/{repository}/pulls/{number}")
        if not isinstance(payload, dict):
            raise GitTargetResolutionError("GitHub pull request response is malformed")
        state = payload.get("state")
        if state not in {"open", "closed"} or payload.get("draft") is not False:
            raise GitTargetResolutionError(
                "pull request must be a public non-draft GitHub PR"
            )
        base = payload.get("base")
        head = payload.get("head")
        base_repo = base.get("repo") if isinstance(base, dict) else None
        head_repo = head.get("repo") if isinstance(head, dict) else None
        if not isinstance(base_repo, dict) or not isinstance(head_repo, dict):
            raise GitTargetResolutionError(
                "pull request source or base repository is unavailable"
            )
        canonical_repository = _require_repo(base_repo.get("full_name"), "base")
        source_repository = _require_repo(head_repo.get("full_name"), "source")
        if canonical_repository.lower() != repository.lower():
            raise GitTargetResolutionError(
                "pull request base repository does not match the Task Project"
            )
        if base_repo.get("private") is not False or head_repo.get("private") is not False:
            raise GitTargetResolutionError(
                "private pull requests are not supported by the public sandbox MVP"
            )
        base_sha = _require_sha(base.get("sha"), "base")
        head_sha = _require_sha(head.get("sha"), "head")
        source_ref = _require_ref(head.get("ref"), "source")
        changed_count = payload.get("changed_files")
        if (
            isinstance(changed_count, bool)
            or not isinstance(changed_count, int)
            or not 0 <= changed_count <= _MAX_CHANGED_FILES
        ):
            raise GitTargetResolutionError(
                "pull request changed-file count exceeds the safety limit"
            )
        changed_files = await self._changed_files(
            canonical_repository,
            number,
            expected_count=changed_count,
        )
        material = {
            "kind": "pull_request",
            "repository": canonical_repository,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "source_repository": source_repository,
            "source_ref": source_ref,
            "pr_number": number,
            "changed_files": list(changed_files),
        }
        return ResolvedGitTarget(
            kind="pull_request",
            repository=canonical_repository,
            clone_url=f"https://github.com/{canonical_repository}.git",
            base_sha=base_sha,
            head_sha=head_sha,
            fetch_ref=f"refs/pull/{number}/head",
            source_repository=source_repository,
            source_ref=source_ref,
            pr_number=number,
            changed_files=changed_files,
            fingerprint=_fingerprint(material),
        )

    async def _changed_files(
        self,
        repository: str,
        number: int,
        *,
        expected_count: int,
    ) -> tuple[dict[str, Any], ...]:
        results: list[dict[str, Any]] = []
        for page in range(1, 5):
            payload = await self.client.get(
                f"/repos/{repository}/pulls/{number}/files?per_page=100&page={page}",
                max_bytes=_MAX_CHANGED_FILES_RESPONSE_BYTES,
            )
            if not isinstance(payload, list):
                raise GitTargetResolutionError(
                    "GitHub changed-file response is malformed"
                )
            for item in payload:
                if not isinstance(item, dict):
                    raise GitTargetResolutionError(
                        "GitHub changed-file entry is malformed"
                    )
                filename = item.get("filename")
                status = item.get("status")
                if (
                    not isinstance(filename, str)
                    or not filename
                    or len(filename) > 1000
                    or "\x00" in filename
                    or filename.startswith("/")
                    or ".." in filename.split("/")
                    or status
                    not in {
                        "added",
                        "modified",
                        "removed",
                        "renamed",
                        "copied",
                        "changed",
                        "unchanged",
                    }
                ):
                    raise GitTargetResolutionError(
                        "GitHub changed-file entry is unsafe"
                    )
                record: dict[str, Any] = {
                    "path": filename,
                    "status": status,
                }
                previous = item.get("previous_filename")
                if previous is not None:
                    if (
                        not isinstance(previous, str)
                        or not previous
                        or len(previous) > 1000
                        or "\x00" in previous
                        or previous.startswith("/")
                        or ".." in previous.split("/")
                    ):
                        raise GitTargetResolutionError(
                            "GitHub previous filename is unsafe"
                        )
                    record["previous_path"] = previous
                for key in ("additions", "deletions", "changes"):
                    value = item.get(key)
                    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                        raise GitTargetResolutionError(
                            "GitHub changed-file counters are malformed"
                        )
                    record[key] = value
                results.append(record)
                if len(results) > _MAX_CHANGED_FILES:
                    raise GitTargetResolutionError(
                        "pull request has too many changed files"
                    )
            if len(payload) < 100:
                break
        if len(results) != expected_count:
            raise GitTargetResolutionError(
                "GitHub changed-file manifest is incomplete"
            )
        return tuple(results)

    async def _resolve_git_ref(
        self,
        repository: str,
        target: dict[str, Any],
    ) -> ResolvedGitTarget:
        ref = _require_ref(target.get("ref"), "target")
        payload = await self.client.get(
            f"/repos/{repository}/commits/{quote(ref, safe='')}"
        )
        if not isinstance(payload, dict):
            raise GitTargetResolutionError("GitHub commit response is malformed")
        head_sha = _require_sha(payload.get("sha"), "target")
        material = {
            "kind": "git_ref",
            "repository": repository,
            "head_sha": head_sha,
            "source_ref": ref,
        }
        return ResolvedGitTarget(
            kind="git_ref",
            repository=repository,
            clone_url=f"https://github.com/{repository}.git",
            base_sha=None,
            head_sha=head_sha,
            fetch_ref=ref,
            source_repository=repository,
            source_ref=ref,
            pr_number=None,
            changed_files=(),
            fingerprint=_fingerprint(material),
        )


public_git_target_resolver = PublicGitTargetResolver()
