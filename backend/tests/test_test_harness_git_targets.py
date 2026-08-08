from __future__ import annotations

import json

import httpx
import pytest

from backend.models.project import Project
from backend.services.test_harness_git_targets import (
    GitTargetResolutionError as ResolutionError,
    PublicGitHubMetadataClient,
    PublicGitTargetResolver,
    github_repository_from_project,
)


class _Client:
    def __init__(self, responses: dict[str, object]):
        self.responses = responses
        self.calls: list[tuple[str, int]] = []

    async def get(self, path: str, *, max_bytes: int = 2 * 1024 * 1024):
        self.calls.append((path, max_bytes))
        try:
            return self.responses[path]
        except KeyError as exc:
            raise AssertionError(path) from exc


def _project(url: str = "https://github.com/zjw49246/CC-Manager.git") -> Project:
    return Project(name="CCM", git_url=url)


def _pull_payload(**changes):
    payload = {
        "state": "open",
        "draft": False,
        "changed_files": 2,
        "base": {
            "sha": "a" * 40,
            "ref": "main",
            "repo": {
                "full_name": "zjw49246/CC-Manager",
                "private": False,
            },
        },
        "head": {
            "sha": "b" * 40,
            "ref": "browser-review",
            "repo": {
                "full_name": "fork-owner/CC-Manager",
                "private": False,
            },
        },
    }
    payload.update(changes)
    return payload


def _file(path: str, status: str = "modified") -> dict[str, object]:
    return {
        "filename": path,
        "status": status,
        "additions": 10,
        "deletions": 3,
        "changes": 13,
    }


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/zjw49246/CC-Manager.git",
        "https://github.com/zjw49246/CC-Manager/",
        "https://github.com/zjw49246/CC-Manager.git/",
        "git@github.com:zjw49246/CC-Manager.git",
        "ssh://git@github.com/zjw49246/CC-Manager.git",
    ],
)
def test_project_remote_resolves_only_canonical_github_identity(url):
    assert github_repository_from_project(_project(url)) == "zjw49246/CC-Manager"


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/zjw49246/CC-Manager.git",
        "https://token@github.com/zjw49246/CC-Manager.git",
        "file:///tmp/repo",
        "https://github.com/zjw49246/CC-Manager.git?ref=main",
        "https://github.com/zjw49246/too/many",
    ],
)
def test_project_remote_rejects_noncanonical_or_credentialed_urls(url):
    with pytest.raises(ResolutionError):
        github_repository_from_project(_project(url))


@pytest.mark.asyncio
async def test_public_pr_resolution_freezes_exact_sha_and_changed_manifest():
    client = _Client(
        {
            "/repos/zjw49246/CC-Manager/pulls/99": _pull_payload(),
            "/repos/zjw49246/CC-Manager/pulls/99/files?per_page=100&page=1": [
                _file("frontend/src/App.tsx"),
                _file("frontend/src/App.test.tsx", "added"),
            ],
        }
    )
    resolver = PublicGitTargetResolver(client)

    target = await resolver.resolve(
        project=_project(),
        kind="pull_request",
        target={"remote": "origin", "pr_number": 99},
    )

    assert target.repository == "zjw49246/CC-Manager"
    assert target.base_sha == "a" * 40
    assert target.head_sha == "b" * 40
    assert target.fetch_ref == "refs/pull/99/head"
    assert target.source_repository == "fork-owner/CC-Manager"
    assert target.source_ref == "browser-review"
    assert [item["path"] for item in target.changed_files] == [
        "frontend/src/App.tsx",
        "frontend/src/App.test.tsx",
    ]
    assert len(target.fingerprint) == 64
    assert target.as_dict()["head_sha"] == "b" * 40


@pytest.mark.asyncio
async def test_public_pr_resolution_allows_closed_pr_at_its_frozen_head():
    client = _Client(
        {
            "/repos/zjw49246/CC-Manager/pulls/99": _pull_payload(
                state="closed",
                changed_files=1,
            ),
            "/repos/zjw49246/CC-Manager/pulls/99/files?per_page=100&page=1": [
                _file("frontend/src/App.tsx"),
            ],
        }
    )

    target = await PublicGitTargetResolver(client).resolve(
        project=_project(),
        kind="pull_request",
        target={"pr_number": 99},
    )

    assert target.head_sha == "b" * 40
    assert target.fetch_ref == "refs/pull/99/head"


@pytest.mark.asyncio
async def test_public_pr_resolution_rejects_private_or_mismatched_base():
    private = _pull_payload()
    private["head"]["repo"]["private"] = True
    mismatched = _pull_payload()
    mismatched["base"]["repo"]["full_name"] = "someone/else"

    for payload, message in (
        (private, "private"),
        (mismatched, "does not match"),
    ):
        resolver = PublicGitTargetResolver(
            _Client({"/repos/zjw49246/CC-Manager/pulls/99": payload})
        )
        with pytest.raises(ResolutionError, match=message):
            await resolver.resolve(
                project=_project(),
                kind="pull_request",
                target={"pr_number": 99},
            )


@pytest.mark.asyncio
async def test_public_pr_resolution_rejects_incomplete_manifest():
    resolver = PublicGitTargetResolver(
        _Client(
            {
                "/repos/zjw49246/CC-Manager/pulls/99": _pull_payload(),
                "/repos/zjw49246/CC-Manager/pulls/99/files?per_page=100&page=1": [
                    _file("frontend/src/App.tsx")
                ],
            }
        )
    )

    with pytest.raises(ResolutionError, match="incomplete"):
        await resolver.resolve(
            project=_project(),
            kind="pull_request",
            target={"pr_number": 99},
        )


@pytest.mark.asyncio
async def test_public_git_ref_is_resolved_to_full_commit_sha():
    resolver = PublicGitTargetResolver(
        _Client(
            {
                "/repos/zjw49246/CC-Manager/commits/feature%2Fbrowser": {
                    "sha": "c" * 40
                }
            }
        )
    )

    target = await resolver.resolve(
        project=_project(),
        kind="git_ref",
        target={"ref": "feature/browser", "fetch": True},
    )

    assert target.head_sha == "c" * 40
    assert target.fetch_ref == "feature/browser"
    assert target.base_sha is None


@pytest.mark.asyncio
async def test_public_metadata_client_rejects_redirects_and_oversized_bodies():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("redirect"):
            return httpx.Response(302, headers={"location": "http://127.0.0.1/"})
        payload = json.dumps({"value": "x" * 100}).encode()
        return httpx.Response(200, content=payload)

    client = PublicGitHubMetadataClient(
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(ResolutionError, match="redirect"):
        await client.get("/redirect")
    with pytest.raises(ResolutionError, match="exceeds"):
        await client.get("/oversized", max_bytes=10)
