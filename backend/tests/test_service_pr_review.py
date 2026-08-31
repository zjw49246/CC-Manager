"""Security and generation tests for PR Monitor review orchestration."""

import asyncio
import base64
from copy import deepcopy
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import pytest_asyncio
from sqlalchemy import select, update

from backend.models.delivery import DeliveryRun
from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRMonitorRun,
    PRReview,
    PRReviewerRun,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.models.team_share import TeamProjectShare
from backend.models.worker import Worker
from backend.services import pr_review_service
from backend.services.delivery_service import value_hash
from backend.services.pr_review_service import (
    GhError,
    PR_MONITOR_INTERNAL_PROJECT_TAG,
    PR_MONITOR_PROJECT_NAME,
    _get_or_create_pr_monitor_project,
    build_review_prompt,
    check_and_update_review,
    create_pr_review_task,
)
from backend.tests.worker_termination_helpers import (
    persist_active_manager_receipt,
)


PR_DATA = {
    "number": 7,
    "base_sha": "a" * 40,
    "head_sha": "b" * 40,
    "delivery_id": "delivery-7",
    "title": "Fix bug",
    "author": "alice",
    "url": "https://github.com/owner/repo/pull/7",
}
TREE_SHA = "c" * 40
CLAUDE_BLOB_SHA = "d" * 40
CCM_TREE_SHA = "e" * 40
GUIDANCE_MANIFEST_SHA = "f" * 40
ACTION_NONCE = "f" * 48
REVIEW_EVIDENCE_MARKER = (
    f"<!-- ccm-pr-review-evidence:nonce={ACTION_NONCE} -->"
)
LEGACY_REVIEW_EVIDENCE_MARKER = f"CCM review nonce: {ACTION_NONCE}"
PUBLISHING_STARTED_AT = datetime(2026, 7, 31, 0, 0, 0)
ACTOR = "ccm-bot"


def _published_action_mock(
    status: str,
    action: str,
    *,
    review_id: int,
) -> AsyncMock:
    """Return a publisher mock with the immutable GitHub receipt it owes."""

    async def publish(**kwargs):
        kwargs["evidence_sink"].update({
            "github_review_id": review_id,
            "github_review_url": (
                "https://github.com/owner/repo/pull/7"
                f"#pullrequestreview-{review_id}"
            ),
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": kwargs["publishing_started_at"],
        })
        return status, action

    return AsyncMock(side_effect=publish)


@pytest.fixture(autouse=True)
def _default_fresh_github_identity(monkeypatch):
    """Keep legacy direct-publisher tests explicit about the fresh fence.

    Individual identity-rotation/failure tests replace this default with their
    own ordered AsyncMock.  Without a default, old publisher unit tests would
    accidentally route their mocked Review POST response through ``GET user``.
    """

    monkeypatch.setattr(
        pr_review_service,
        "_gh_authenticated_login",
        AsyncMock(return_value=ACTOR),
    )


def _make_repo(**overrides) -> MonitoredRepo:
    values = {
        "repo_full_name": "owner/repo",
        "webhook_secret": "s" * 64,
        "auto_merge": False,
        "default_branch": "main",
        "allowed_authors": [],
        "review_model": "claude-sonnet-4-6",
        "provider": "claude",
    }
    values.update(overrides)
    return MonitoredRepo(**values)


async def _link_legacy_review_task(
    db,
    *,
    project: Project,
    repo_name: str,
) -> Task:
    task = Task(
        title="legacy reviewer",
        description="internal review protocol",
        project_id=project.id,
        tags=[],
    )
    repo = _make_repo(repo_full_name=repo_name)
    db.add_all([task, repo])
    await db.flush()
    db.add(PRReview(
        repo_id=repo.id,
        pr_number=7,
        base_ref="main",
        base_sha="a" * 40,
        head_sha="b" * 40,
        pr_title="Legacy review",
        pr_author="alice",
        pr_url=f"https://github.com/{repo_name}/pull/7",
        task_id=task.id,
        status="reviewing",
    ))
    await db.flush()
    return task


@pytest.mark.asyncio
async def test_legacy_pr_monitor_project_is_adopted_only_with_internal_evidence(
    db_session,
):
    project = Project(name=PR_MONITOR_PROJECT_NAME)
    db_session.add(project)
    await db_session.flush()
    await _link_legacy_review_task(
        db_session,
        project=project,
        repo_name="owner/exclusive-legacy-monitor",
    )

    project_id = await _get_or_create_pr_monitor_project(db_session)

    assert project_id == project.id
    assert PR_MONITOR_INTERNAL_PROJECT_TAG in (project.tags or [])
    assert project.show_in_selector is False


@pytest.mark.asyncio
async def test_legacy_name_collision_with_ordinary_state_gets_fallback_project(
    db_session,
):
    project = Project(
        name=PR_MONITOR_PROJECT_NAME,
        local_path="/tmp/real-pr-monitor-project",
        status="ready",
    )
    db_session.add(project)
    await db_session.flush()
    db_session.add_all([
        Task(
            title="ordinary project task",
            description="member work",
            project_id=project.id,
        ),
        TeamProjectShare(
            project_id=project.id,
            target_type="user",
            target_id=101,
            shared_by=1,
        ),
    ])
    await _link_legacy_review_task(
        db_session,
        project=project,
        repo_name="owner/colliding-legacy-monitor",
    )

    project_id = await _get_or_create_pr_monitor_project(db_session)
    internal = await db_session.get(Project, project_id)

    assert project_id != project.id
    assert PR_MONITOR_INTERNAL_PROJECT_TAG not in (project.tags or [])
    assert PR_MONITOR_INTERNAL_PROJECT_TAG in (internal.tags or [])
    assert internal.show_in_selector is False


def _snapshot(
    *,
    state="OPEN",
    base_ref="main",
    base_sha=PR_DATA["base_sha"],
    head_sha=PR_DATA["head_sha"],
    is_draft=False,
    merged_at=None,
    merged_by=None,
    merge_commit_sha=None,
):
    return {
        "state": state,
        "baseRefName": base_ref,
        "baseRefOid": base_sha,
        "headRefOid": head_sha,
        "isDraft": is_draft,
        "mergedAt": merged_at,
        "mergedBy": (
            {"login": merged_by}
            if merged_by is not None
            else None
        ),
        "mergeCommit": (
            {"oid": merge_commit_sha}
            if merge_commit_sha is not None
            else None
        ),
    }


def _review_response(
    *,
    state="APPROVED",
    head_sha=PR_DATA["head_sha"],
    body=f"review\n\n{REVIEW_EVIDENCE_MARKER}",
):
    return {
        "id": 91,
        "state": state,
        "commit_id": head_sha,
        "body": body,
        "user": {"login": ACTOR},
        "submitted_at": "2026-07-31T00:00:01Z",
    }


def _comment_response(
    *,
    body=f"comment\n\n{REVIEW_EVIDENCE_MARKER}",
):
    return {
        "id": 92,
        "state": "COMMENTED",
        "commit_id": PR_DATA["head_sha"],
        "body": body,
        "user": {"login": ACTOR},
        "submitted_at": "2026-07-31T00:00:01Z",
    }


def _merged_issue_comment_response(
    *,
    head_sha=PR_DATA["head_sha"],
    nonce=ACTION_NONCE,
    actor=ACTOR,
    created_at="2026-07-31T00:00:01Z",
):
    return {
        "id": 93,
        "body": pr_review_service._merged_comment_body(
            nonce=nonce,
            head_sha=head_sha,
        ),
        "user": {"login": actor},
        "created_at": created_at,
        "html_url": "https://github.com/owner/repo/pull/7#issuecomment-93",
    }


def _terminal_output(result="lgtm_comment", body="Looks good."):
    return (
        "PR_REVIEW_BODY_BEGIN\n"
        f"{body}\n"
        "PR_REVIEW_BODY_END\n"
        f"PR_REVIEW_RESULT: {result}"
    )


def _blob_payload(sha: str, content: bytes) -> dict:
    return {
        "sha": sha,
        "size": len(content),
        "encoding": "base64",
        "content": base64.b64encode(content).decode("ascii"),
    }


def _guidance_api_side_effect(
    claude: bytes = b"# Rules\nUse tests.",
):
    manifest = json.dumps({
        "version": 1,
        "documents": [{
            "path": "CLAUDE.md",
            "roles": sorted(pr_review_service._GUIDANCE_ROLES),
        }],
    }).encode()
    return [
        {"sha": PR_DATA["base_sha"], "tree": {"sha": TREE_SHA}},
        {"sha": TREE_SHA, "truncated": False, "tree": [
            {
                "path": ".ccm",
                "type": "tree",
                "mode": "040000",
                "sha": CCM_TREE_SHA,
            },
            {
                "path": "CLAUDE.md",
                "type": "blob",
                "mode": "100644",
                "sha": CLAUDE_BLOB_SHA,
                "size": len(claude),
            },
        ]},
        {"sha": CCM_TREE_SHA, "truncated": False, "tree": [{
            "path": "review-guides.json",
            "type": "blob",
            "mode": "100644",
            "sha": GUIDANCE_MANIFEST_SHA,
            "size": len(manifest),
        }]},
        _blob_payload(GUIDANCE_MANIFEST_SHA, manifest),
        {"sha": TREE_SHA, "truncated": False, "tree": [{
            "path": "CLAUDE.md",
            "type": "blob",
            "mode": "100644",
            "sha": CLAUDE_BLOB_SHA,
            "size": len(claude),
        }]},
        _blob_payload(CLAUDE_BLOB_SHA, claude),
    ]


def _prepared_context(
    guidance: dict[str, object] | None = None,
) -> dict:
    return {
        "repo_name": "owner/repo",
        "pr_number": PR_DATA["number"],
        "base_ref": "main",
        "base_sha": PR_DATA["base_sha"],
        "head_sha": PR_DATA["head_sha"],
        "guidance": guidance or {},
        "material": {
            "number": PR_DATA["number"],
            "title": PR_DATA["title"],
            "body": "Description",
            "author": PR_DATA["author"],
            "base_ref": "main",
            "head_ref": "feature",
            "files": [{
                "path": "backend/app.py",
                "additions": 2,
                "deletions": 1,
            }],
            "patch": "diff --git a/backend/app.py b/backend/app.py\n",
        },
    }


def _publisher_kwargs(**overrides) -> dict:
    values = {
        "repo_name": "owner/repo",
        "pr_number": PR_DATA["number"],
        "base_ref": "main",
        "base_sha": PR_DATA["base_sha"],
        "head_sha": PR_DATA["head_sha"],
        "result": "lgtm_comment",
        "review_body": "",
        "auto_merge": False,
        "merge_method": None,
        "nonce": ACTION_NONCE,
        "actor": ACTOR,
        "current_actor": ACTOR,
        "publishing_started_at": PUBLISHING_STARTED_AT,
        "ensure_current": AsyncMock(return_value=True),
    }
    values.update(overrides)
    if "merge_method" not in overrides:
        values["merge_method"] = (
            "fast-forward" if values["auto_merge"] else None
        )
    return values


def _direct_ref_repo_info(**overrides) -> dict:
    values = {
        "full_name": "owner/repo",
        "archived": False,
        "disabled": False,
        "permissions": {"push": True},
    }
    values.update(overrides)
    return values


def _safe_direct_merge_protection(**overrides) -> dict:
    values = {
        "required_status_checks": {
            "strict": True,
            "contexts": ["tests"],
            "checks": [{"context": "tests", "app_id": 1}],
        },
        "enforce_admins": {"enabled": True},
        "allow_force_pushes": {"enabled": False},
        "allow_deletions": {"enabled": False},
        "required_conversation_resolution": {"enabled": False},
        "required_pull_request_reviews": {
            "bypass_pull_request_allowances": {
                "users": [],
                "teams": [],
                "apps": [],
            },
        },
        "required_linear_history": {"enabled": False},
    }
    values.update(overrides)
    return values


def _exact_ci_coverage(*, app_id: int = 1) -> tuple[list[dict], dict]:
    required = [{
        "kind": "check_run",
        "name": "tests",
        "app_slug": "github-actions",
    }]
    return required, {
        "head_sha": PR_DATA["head_sha"],
        "required": required,
        "observed": [{
            **required[0],
            "state": "passed",
            "app_id": app_id,
        }],
    }


def _standard_collaborator_permission(
    *,
    role="write",
    actor=ACTOR,
    user_type="User",
) -> dict:
    permissions = {"push": True}
    if role == "maintain":
        permissions["maintain"] = True
    if role == "admin":
        permissions["admin"] = True
    return {
        "permission": "write" if role == "maintain" else role,
        "role_name": role,
        "user": {
            "login": actor,
            "type": user_type,
            "role_name": role,
            "permissions": permissions,
        },
    }


def _ahead_compare_response(ancestor: str, descendant: str) -> dict:
    return {
        "url": (
            "https://api.github.com/repos/owner/repo/compare/"
            f"{ancestor}...{descendant}"
        ),
        "base_commit": {"sha": ancestor},
        "merge_base_commit": {"sha": ancestor},
        "status": "ahead",
        "ahead_by": 1,
        "behind_by": 0,
        "total_commits": 1,
        "commits": [{"sha": descendant}],
    }


@pytest_asyncio.fixture
async def repo(db_session):
    value = _make_repo()
    db_session.add(value)
    await db_session.commit()
    await db_session.refresh(value)
    return value


@pytest.fixture
def no_broadcast():
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    with patch("backend.main.broadcaster", broadcaster):
        yield broadcaster


async def _make_review(
    db,
    repo,
    *,
    auto_merge=False,
    retry_count=2,
    task_status="completed",
    nonce=ACTION_NONCE,
):
    started_at = datetime.utcnow()
    task = Task(
        title="PR review task",
        description="review",
        status=task_status,
        retry_count=retry_count,
        started_at=started_at,
        completed_at=(
            started_at + timedelta(seconds=2)
            if task_status == "completed"
            else None
        ),
        metadata_={
            "pr_auto_merge": auto_merge,
            "pr_base_ref": "main",
            "pr_action_nonce": nonce,
        },
    )
    db.add(task)
    await db.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=PR_DATA["number"],
        base_ref="main",
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        delivery_id=PR_DATA["delivery_id"],
        pr_title=PR_DATA["title"],
        pr_author=PR_DATA["author"],
        pr_url=PR_DATA["url"],
        status="reviewing",
        task_id=task.id,
        action_nonce=nonce,
    )
    db.add(review)
    await db.commit()
    await db.refresh(task)
    await db.refresh(review)
    return review, task


async def _add_terminal_log(
    db,
    task: Task,
    *,
    result="lgtm_comment",
    body="Looks good.",
    retry_count: int | None = None,
) -> LogEntry:
    entry = LogEntry(
        task_id=task.id,
        task_retry_count=(
            task.retry_count if retry_count is None else retry_count
        ),
        event_type="result",
        content=_terminal_output(result, body),
        timestamp=task.started_at + timedelta(seconds=1),
    )
    db.add(entry)
    await db.commit()
    return entry


async def _arm_publishing(
    db,
    review: PRReview,
    task: Task,
    *,
    action="lgtm_comment",
    body="Looks good.",
) -> None:
    review.status = "publishing"
    review.review_summary = body
    review.pending_action = action
    review.pending_review_body = body
    review.publishing_actor = ACTOR
    review.publishing_retry_count = task.retry_count
    review.publishing_task_started_at = task.started_at
    review.publishing_started_at = PUBLISHING_STARTED_AT
    review.merge_method = (
        "merge"
        if (task.metadata_ or {}).get("pr_auto_merge") is True
        and action == "approved_merged"
        else None
    )
    await db.commit()
    await db.refresh(review)


async def _bind_current_reviewing_run(db, review: PRReview) -> PRMonitorRun:
    run = PRMonitorRun(
        repo_id=review.repo_id,
        pr_number=review.pr_number,
        current_base_sha=review.base_sha,
        current_head_sha=review.head_sha,
        current_review_id=review.id,
        status="reviewing",
    )
    db.add(run)
    await db.flush()
    review.monitor_run_id = run.id
    return run


async def _bind_delivery_review(
    db,
    *,
    delivery_run: DeliveryRun,
    review: PRReview,
) -> PRMonitorRun:
    """Create the exact durable Run/Monitor/Review ownership triangle."""

    monitor = PRMonitorRun(
        repo_id=review.repo_id,
        pr_number=review.pr_number,
        current_base_sha=review.base_sha,
        current_head_sha=review.head_sha,
        current_review_id=review.id,
        status="reviewing",
    )
    db.add(monitor)
    await db.flush()
    review.monitor_run_id = monitor.id
    delivery_run.pr_monitor_run_id = monitor.id
    await db.commit()
    await db.refresh(review)
    await db.refresh(delivery_run)
    return monitor


# ---------------------------------------------------------------------------
# Prompt and backend-fetched base guidance
# ---------------------------------------------------------------------------


def test_build_review_prompt_injects_verified_documents_as_json():
    documents = {
        "CLAUDE.md": "Rule: use tests.\n`$(never-run)`",
        "PROGRESS.md": "Lesson: keep snapshots pinned.",
        pr_review_service._GUIDANCE_ROLE_MAP_KEY: {
            "CLAUDE.md": ["principal_engineer"],
            "PROGRESS.md": ["principal_engineer"],
        },
    }
    prompt = build_review_prompt(
        _make_repo(auto_merge=False),
        PR_DATA,
        guidance_documents=documents,
    )

    assert f"Captured base commit: `{PR_DATA['base_sha']}`" in prompt
    assert f"Captured head commit: `{PR_DATA['head_sha']}`" in prompt
    assert "Do not read `CLAUDE.md`, `AGENTS.md`, or `PROGRESS.md`" in prompt
    assert "CCM already fetched the exact root tree" in prompt
    assert "Do not run `gh pr review`, `gh pr comment`, `gh pr merge`" in prompt
    assert "PR_REVIEW_RESULT: lgtm_comment" in prompt

    injected = prompt.split(
        "<ccm_verified_base_guidance>\n", 1
    )[1].split("\n</ccm_verified_base_guidance>", 1)[0]
    records = [json.loads(line) for line in injected.splitlines()]
    assert [record["name"] for record in records] == [
        "CLAUDE.md",
        "PROGRESS.md",
    ]
    assert records[0]["content"] == documents["CLAUDE.md"]
    assert records[0]["byte_length"] == len(
        documents["CLAUDE.md"].encode()
    )
    assert records[0]["sha256"] == hashlib.sha256(
        documents["CLAUDE.md"].encode()
    ).hexdigest()


def test_build_review_prompt_allows_empty_pack_and_ignores_legacy_documents():
    prompt = build_review_prompt(
        _make_repo(auto_merge=True),
        PR_DATA,
        guidance_documents={
            "CLAUDE.md": "LEGACY_IMPLICIT_CLAUDE_SENTINEL",
            "PROGRESS.md": "LEGACY_IMPLICIT_PROGRESS_SENTINEL",
        },
    )
    injected = prompt.split(
        "<ccm_verified_base_guidance>\n", 1
    )[1].split("\n</ccm_verified_base_guidance>", 1)[0]
    assert injected == ""
    assert "LEGACY_IMPLICIT_CLAUDE_SENTINEL" not in prompt
    assert "LEGACY_IMPLICIT_PROGRESS_SENTINEL" not in prompt
    assert "This block may\nbe empty" in prompt
    assert "PR_REVIEW_RESULT: approved_merged" in prompt


def test_review_prompt_budget_is_utf8_bounded_and_provider_specific():
    assert (
        pr_review_service._CODEX_PR_REVIEW_PROMPT_MAX_CHARS
        + pr_review_service._CODEX_PR_REVIEW_RUNTIME_RESERVE_CHARS
        == pr_review_service._CODEX_INPUT_MAX_CHARS
    )
    within_budget = (
        "界" * pr_review_service._CODEX_PR_REVIEW_PROMPT_MAX_CHARS
    )
    pr_review_service.validate_review_prompt_budget(
        within_budget,
        provider="codex",
        label="single reviewer",
    )

    with pytest.raises(
        pr_review_service.PRReviewInputTooLarge,
        match=r"unsupported_input_size: single reviewer.*codex",
    ):
        pr_review_service.validate_review_prompt_budget(
            within_budget + "界",
            provider="codex",
            label="single reviewer",
        )

    with pytest.raises(
        pr_review_service.PRReviewInputTooLarge,
        match=r"unsupported_input_size: single reviewer.*claude",
    ):
        pr_review_service.validate_review_prompt_budget(
            "界" * (
                pr_review_service._CLAUDE_PR_REVIEW_PROMPT_MAX_BYTES // 3 + 1
            ),
            provider="claude",
            label="single reviewer",
        )


def test_input_rejection_evidence_enforces_json_safe_integer_boundary():
    maximum = pr_review_service.PRReviewInputTooLarge.max_safe_integer
    accepted = pr_review_service.PRReviewInputTooLarge(
        "bounded",
        measured=maximum,
        limit=maximum - 1,
        unit="characters",
    )
    assert accepted.measured == maximum

    for measured, limit in (
        (maximum + 1, 1),
        (maximum + 1, maximum),
        (maximum + 2, maximum + 1),
    ):
        with pytest.raises(
            ValueError,
            match="invalid PR review input-size evidence",
        ):
            pr_review_service.PRReviewInputTooLarge(
                "out of range",
                measured=measured,
                limit=limit,
                unit="characters",
            )


def test_single_prompt_keeps_the_complete_patch_within_budget():
    exact_patch = "PATCH_BEGIN\n" + ("changed line\n" * 2_000) + "PATCH_END"
    material = _prepared_context()["material"]
    material["patch"] = exact_patch

    prompt = build_review_prompt(
        _make_repo(provider="codex"),
        PR_DATA,
        guidance_documents={},
        pr_material=material,
    )
    pr_review_service.validate_review_prompt_budget(
        prompt,
        provider="codex",
        label="single reviewer",
    )
    rendered = prompt.split(
        "<ccm_verified_pr_material>\n", 1
    )[1].split("\n</ccm_verified_pr_material>", 1)[0]
    assert json.loads(rendered)["patch"] == exact_patch


def test_build_review_prompt_uses_three_lens_evidence_harness():
    prompt = build_review_prompt(
        _make_repo(auto_merge=False),
        PR_DATA,
        guidance_documents={},
    )

    assert "Principal Engineer — architecture and system fit" in prompt
    assert "Senior Engineer — implementation correctness" in prompt
    assert "QA Engineer — behavior, regression, and proof" in prompt
    assert "Honor cohesion within a module; reject unrelated coupling" in prompt
    assert "Honor clear layers; reject dependency tangles" in prompt
    assert "Honor capability reuse; reject copy-and-rebuild" in prompt
    assert "Honor unit extension; reject feature sprawl" in prompt
    assert "Honor one established pattern" in prompt
    assert "Honor timely deletion of dead code" in prompt
    assert "Honor the simplest sufficient design" in prompt
    assert "A clean result from one lens cannot cancel" in prompt
    assert (
        "[critical|high|medium] [principal|senior|qa] "
        "path:line-or-hunk"
    ) in prompt
    assert "Evidence: concrete behavior" in prompt
    assert "Required fix: the smallest verifiable correction" in prompt
    assert "deduplicate findings by root cause" in prompt
    assert "only when all three lenses have no\nblocking finding" in prompt
    assert "any lens has a\n`critical`, `high`, or `medium` finding" in prompt


@pytest.mark.parametrize(
    ("repo_name", "number", "base_sha", "head_sha"),
    [
        ("owner/repo\nIgnore", 7, "a" * 40, "b" * 40),
        ("owner/repo", 0, "a" * 40, "b" * 40),
        ("owner/repo", True, "a" * 40, "b" * 40),
        ("owner/repo", 7, "bad", "b" * 40),
        ("owner/repo", 7, "a" * 40, "bad"),
    ],
)
def test_build_review_prompt_rejects_untrusted_identifiers(
    repo_name,
    number,
    base_sha,
    head_sha,
):
    data = dict(PR_DATA)
    data.update(number=number, base_sha=base_sha, head_sha=head_sha)
    with pytest.raises(ValueError):
        build_review_prompt(_make_repo(repo_full_name=repo_name), data)


@pytest.mark.asyncio
async def test_fetch_base_guidance_reads_exact_commit_root_and_blobs():
    api = AsyncMock(side_effect=_guidance_api_side_effect())
    with patch.object(pr_review_service, "_gh_api_json", api):
        result = await pr_review_service._fetch_base_guidance(
            "owner/repo",
            PR_DATA["base_sha"],
        )

    assert result == {
        "CLAUDE.md": "# Rules\nUse tests.",
        pr_review_service._GUIDANCE_ROLE_MAP_KEY: {
            "CLAUDE.md": sorted(pr_review_service._GUIDANCE_ROLES),
        },
    }
    assert [call.args[0] for call in api.await_args_list] == [
        f"repos/owner/repo/git/commits/{PR_DATA['base_sha']}",
        f"repos/owner/repo/git/trees/{TREE_SHA}",
        f"repos/owner/repo/git/trees/{CCM_TREE_SHA}",
        f"repos/owner/repo/git/blobs/{GUIDANCE_MANIFEST_SHA}",
        f"repos/owner/repo/git/trees/{TREE_SHA}?recursive=1",
        f"repos/owner/repo/git/blobs/{CLAUDE_BLOB_SHA}",
    ]
    assert api.await_args_list[1].kwargs["max_output_bytes"] == (
        pr_review_service._MAX_GH_TREE_RESPONSE_BYTES
    )


@pytest.mark.asyncio
async def test_fetch_base_guidance_ignores_unmanifested_root_documents():
    api = AsyncMock(side_effect=[
        {"sha": PR_DATA["base_sha"], "tree": {"sha": TREE_SHA}},
        {
            "sha": TREE_SHA,
            "truncated": False,
            "tree": [{
                "path": "CLAUDE.md",
                "type": "blob",
                "mode": "100644",
                "sha": CLAUDE_BLOB_SHA,
                "size": 200_000,
            }],
        },
    ])
    with patch.object(pr_review_service, "_gh_api_json", api):
        result = await pr_review_service._fetch_base_guidance(
            "owner/repo",
            PR_DATA["base_sha"],
        )
    assert result == {}
    assert api.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "commit",
    [
        {},
        {"sha": "9" * 40, "tree": {"sha": TREE_SHA}},
        {"sha": PR_DATA["base_sha"], "tree": {"sha": "bad"}},
    ],
)
async def test_fetch_base_guidance_rejects_mismatched_commit(commit):
    with patch.object(
        pr_review_service,
        "_gh_api_json",
        AsyncMock(return_value=commit),
    ):
        with pytest.raises(GhError, match="commit response"):
            await pr_review_service._fetch_base_guidance(
                "owner/repo",
                PR_DATA["base_sha"],
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tree",
    [
        {"sha": TREE_SHA, "truncated": True, "tree": []},
        {"sha": "9" * 40, "truncated": False, "tree": []},
        {"sha": TREE_SHA, "truncated": False, "tree": "not-a-list"},
    ],
)
async def test_fetch_base_guidance_rejects_unproven_tree(tree):
    api = AsyncMock(side_effect=[
        {"sha": PR_DATA["base_sha"], "tree": {"sha": TREE_SHA}},
        tree,
    ])
    with patch.object(pr_review_service, "_gh_api_json", api):
        with pytest.raises(GhError, match="tree response"):
            await pr_review_service._fetch_base_guidance(
                "owner/repo",
                PR_DATA["base_sha"],
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["120000", "160000", "040000"])
async def test_fetch_base_guidance_rejects_symlink_or_non_regular(mode):
    manifest = json.dumps({
        "version": 1,
        "documents": [{
            "path": "CLAUDE.md",
            "roles": ["principal_engineer"],
        }],
    }).encode()
    api = AsyncMock(side_effect=[
        {"sha": PR_DATA["base_sha"], "tree": {"sha": TREE_SHA}},
        {
            "sha": TREE_SHA,
            "truncated": False,
            "tree": [{
                "path": ".ccm",
                "type": "tree",
                "mode": "040000",
                "sha": CCM_TREE_SHA,
            }],
        },
        {"sha": CCM_TREE_SHA, "truncated": False, "tree": [{
            "path": "review-guides.json",
            "type": "blob",
            "mode": "100644",
            "sha": GUIDANCE_MANIFEST_SHA,
            "size": len(manifest),
        }]},
        _blob_payload(GUIDANCE_MANIFEST_SHA, manifest),
        {
            "sha": TREE_SHA,
            "truncated": False,
            "tree": [{
                "path": "CLAUDE.md",
                "type": "blob",
                "mode": mode,
                "sha": CLAUDE_BLOB_SHA,
                "size": 1,
            }],
        },
    ])
    with patch.object(pr_review_service, "_gh_api_json", api):
        with pytest.raises(GhError, match="unsafe review guidance"):
            await pr_review_service._fetch_base_guidance(
                "owner/repo",
                PR_DATA["base_sha"],
            )


@pytest.mark.parametrize(
    ("entry_overrides", "blob_overrides", "error"),
    [
        ({}, {"content": "%%%"}, "base64"),
        (
            {"size": 4},
            {"content": base64.b64encode(b"bad\x00").decode(), "size": 4},
            "NUL",
        ),
        ({}, {"content": base64.b64encode(b"\xff").decode(), "size": 1}, "UTF-8"),
        ({}, {"sha": "9" * 40}, "malformed"),
        ({"size": 2}, {"size": 2}, "declared size"),
        (
            {"size": pr_review_service._MAX_GUIDANCE_FILE_BYTES + 1},
            {},
            "oversized",
        ),
    ],
)
def test_decode_guidance_blob_fails_closed(
    entry_overrides,
    blob_overrides,
    error,
):
    content = b"x"
    entry = {
        "sha": CLAUDE_BLOB_SHA,
        "size": len(content),
        **entry_overrides,
    }
    blob = {
        **_blob_payload(CLAUDE_BLOB_SHA, content),
        **blob_overrides,
    }
    with pytest.raises(GhError, match=error):
        pr_review_service._decode_guidance_blob(
            name="CLAUDE.md",
            entry=entry,
            blob=blob,
            max_bytes=pr_review_service._MAX_GUIDANCE_FILE_BYTES,
        )


def test_decode_guidance_manifest_has_an_independent_small_limit():
    content = b"x" * (pr_review_service._MAX_GUIDANCE_MANIFEST_BYTES + 1)
    with pytest.raises(GhError, match="oversized"):
        pr_review_service._decode_guidance_blob(
            name=pr_review_service._GUIDANCE_MANIFEST_PATH,
            entry={"sha": GUIDANCE_MANIFEST_SHA, "size": len(content)},
            blob=_blob_payload(GUIDANCE_MANIFEST_SHA, content),
            max_bytes=pr_review_service._MAX_GUIDANCE_MANIFEST_BYTES,
        )


def test_render_guidance_documents_enforces_combined_limit():
    each = "x" * pr_review_service._MAX_GUIDANCE_FILE_BYTES
    with pytest.raises(ValueError, match="combined"):
        pr_review_service._render_guidance_documents({
            "docs/one.md": each,
            "docs/two.md": each,
            "docs/three.md": each,
            pr_review_service._GUIDANCE_ROLE_MAP_KEY: {
                "docs/one.md": ["senior_engineer"],
                "docs/two.md": ["senior_engineer"],
                "docs/three.md": ["senior_engineer"],
            },
        }, role="senior_engineer")


def test_render_guidance_documents_enforces_manifest_document_count():
    role_map = {
        f"docs/guide-{index}.md": ["qa_engineer"]
        for index in range(pr_review_service._MAX_GUIDANCE_DOCUMENTS + 1)
    }
    with pytest.raises(ValueError, match="role map"):
        pr_review_service._render_guidance_documents({
            **{name: "guide" for name in role_map},
            pr_review_service._GUIDANCE_ROLE_MAP_KEY: role_map,
        }, role="qa_engineer")


def _compare_identity(
    *,
    base_sha=PR_DATA["base_sha"],
    head_sha=PR_DATA["head_sha"],
    total_commits=1,
    commits=None,
    url=None,
):
    endpoint = (
        "repos/owner/repo/compare/"
        f"{PR_DATA['base_sha']}...{PR_DATA['head_sha']}"
    )
    return {
        "base_commit": {"sha": base_sha},
        "commits": commits if commits is not None else [{"sha": head_sha}],
        "total_commits": total_commits,
        "url": url or f"https://api.github.com/{endpoint}",
    }


@pytest.mark.asyncio
async def test_fetch_patch_uses_immutable_captured_sha_endpoint():
    endpoint = (
        "repos/owner/repo/compare/"
        f"{PR_DATA['base_sha']}...{PR_DATA['head_sha']}"
    )
    patch_bytes = (
        f"From {PR_DATA['head_sha']} Mon Sep 17 00:00:00 2001\n"
        "Subject: [PATCH] pinned\n\n"
        "diff --git a/app.py b/app.py\n"
    ).encode()
    api = AsyncMock(return_value=_compare_identity())
    runner = AsyncMock(return_value=(0, patch_bytes, b""))
    with (
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(pr_review_service, "_run_gh", runner),
    ):
        result = await pr_review_service._fetch_immutable_compare_patch(
            repo_name="owner/repo",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
        )

    assert result == patch_bytes.decode()
    api.assert_awaited_once_with(
        f"{endpoint}?per_page=100&page=1",
        max_output_bytes=pr_review_service._MAX_GH_COMPARE_RESPONSE_BYTES,
    )
    runner.assert_awaited_once_with(
        "api",
        endpoint,
        "-H",
        "Accept: application/vnd.github.v3.patch",
        timeout=60,
    )
    assert str(PR_DATA["number"]) not in runner.await_args.args


@pytest.mark.asyncio
async def test_fetch_patch_accepts_update_branch_merge_head_omitted_from_mbox():
    previous_head = "8" * 40
    identity = _compare_identity(
        total_commits=2,
        commits=[
            {"sha": previous_head},
            {
                "sha": PR_DATA["head_sha"],
                "parents": [
                    {"sha": previous_head},
                    {"sha": PR_DATA["base_sha"]},
                ],
            },
        ],
    )
    patch_bytes = (
        f"From {previous_head} Mon Sep 17 00:00:00 2001\n"
        "Subject: [PATCH] branch changes\n\n"
        "diff --git a/app.py b/app.py\n"
    ).encode()
    with (
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(return_value=identity),
        ),
        patch.object(
            pr_review_service,
            "_run_gh",
            AsyncMock(return_value=(0, patch_bytes, b"")),
        ),
    ):
        result = await pr_review_service._fetch_immutable_compare_patch(
            repo_name="owner/repo",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
        )

    assert result == patch_bytes.decode()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identity", "error"),
    [
        (_compare_identity(base_sha="9" * 40), "identity response"),
        (_compare_identity(head_sha="9" * 40), "captured head"),
        (
            _compare_identity(
                url="https://api.github.com/repos/owner/repo/"
                f"compare/{PR_DATA['base_sha']}...{'9' * 40}"
            ),
            "identity response",
        ),
    ],
)
async def test_fetch_patch_rejects_compare_identity_mismatch(
    identity,
    error,
):
    runner = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(return_value=identity),
        ),
        patch.object(pr_review_service, "_run_gh", runner),
    ):
        with pytest.raises(GhError, match=error):
            await pr_review_service._fetch_immutable_compare_patch(
                repo_name="owner/repo",
                base_sha=PR_DATA["base_sha"],
                head_sha=PR_DATA["head_sha"],
            )
    runner.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_patch_rejects_patch_for_a_different_head():
    patch_bytes = (
        f"From {'9' * 40} Mon Sep 17 00:00:00 2001\n"
        "Subject: [PATCH] wrong\n"
    ).encode()
    with (
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(return_value=_compare_identity()),
        ),
        patch.object(
            pr_review_service,
            "_run_gh",
            AsyncMock(return_value=(0, patch_bytes, b"")),
        ),
    ):
        with pytest.raises(GhError, match="patch identity"):
            await pr_review_service._fetch_immutable_compare_patch(
                repo_name="owner/repo",
                base_sha=PR_DATA["base_sha"],
                head_sha=PR_DATA["head_sha"],
            )


@pytest.mark.asyncio
async def test_fetch_pr_material_does_not_fetch_or_store_full_file_copies():
    metadata = {
        **_snapshot(),
        "number": PR_DATA["number"],
        "title": PR_DATA["title"],
        "body": "Description",
        "author": {"login": PR_DATA["author"]},
        "headRefName": "feature",
        "changedFiles": 1,
    }
    files = [{
        "path": "backend/app.py",
        "additions": 2,
        "deletions": 1,
    }]
    exact_patch = "diff --git a/backend/app.py b/backend/app.py\n"
    runner = AsyncMock(return_value=(0, json.dumps(metadata).encode(), b""))
    fetch_files = AsyncMock(return_value=files)
    fetch_patch = AsyncMock(return_value=exact_patch)
    with (
        patch.object(pr_review_service, "_run_gh", runner),
        patch.object(pr_review_service, "_fetch_pr_files", fetch_files),
        patch.object(
            pr_review_service,
            "_fetch_immutable_compare_patch",
            fetch_patch,
        ),
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
    ):
        material = await pr_review_service._fetch_pr_material(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            base_ref="main",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
        )

    assert material["files"] == files
    assert material["patch"] == exact_patch
    assert "changed_file_contents" not in material
    fetch_files.assert_awaited_once()
    fetch_patch.assert_awaited_once()


# ---------------------------------------------------------------------------
# Task creation: prefetch first, inject exact docs, freeze nonce/policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_pr_review_task_prefetches_guidance_and_freezes_nonce(
    db_session,
    repo,
):
    documents = {
        "CLAUDE.md": "Always test.",
        "PROGRESS.md": "Never trust head docs.",
        pr_review_service._GUIDANCE_ROLE_MAP_KEY: {
            "CLAUDE.md": ["principal_engineer"],
            "PROGRESS.md": ["senior_engineer"],
        },
    }
    prepared = _prepared_context(documents)
    broadcaster = MagicMock(broadcast=AsyncMock())
    dispatcher = MagicMock(wake=MagicMock())
    token_calls = 0

    def deterministic_token_hex(length: int) -> str:
        nonlocal token_calls
        token_calls += 1
        if length == 24:
            return ACTION_NONCE
        return f"{token_calls:0{length * 2}x}"[-length * 2:]

    with (
        patch.object(
            pr_review_service,
            "prepare_pr_review_context",
            AsyncMock(return_value=prepared),
        ) as prepare,
        patch.object(
            pr_review_service.secrets,
            "token_hex",
            side_effect=deterministic_token_hex,
        ),
        patch("backend.main.broadcaster", broadcaster),
        patch("backend.main.dispatcher", dispatcher),
    ):
        review = await create_pr_review_task(db_session, repo, PR_DATA)

    task = await db_session.get(Task, review.task_id)
    assert review.status == "reviewing"
    assert review.base_sha == PR_DATA["base_sha"]
    assert review.head_sha == PR_DATA["head_sha"]
    assert review.action_nonce == ACTION_NONCE
    assert task.tags == ["pr-review"]
    assert task.status == "pending"
    assert task.archived is True
    assert task.execution_user_id is None
    assert task.execution_user_role == "member"
    assert task.execution_mode == "sandbox"
    assert task.execution_principal_kind == "system"
    assert task.metadata_ == {
        "pr_review_id": review.id,
        "pr_base_ref": "main",
        "pr_base_sha": PR_DATA["base_sha"],
        "pr_head_sha": PR_DATA["head_sha"],
        "pr_auto_merge": False,
        "pr_wait_for_ci": False,
        "pr_required_checks": [],
        "pr_action_nonce": ACTION_NONCE,
    }
    assert "Always test." in task.description
    assert "Never trust head docs." in task.description
    prepare.assert_awaited_once_with(repo, PR_DATA)
    dispatcher.wake.assert_called_once()
    broadcaster.broadcast.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_pr_review_task_fetch_failure_stages_nothing(
    db_session,
    repo,
):
    with patch.object(
        pr_review_service,
        "prepare_pr_review_context",
        AsyncMock(side_effect=GhError("tree unavailable")),
    ):
        with pytest.raises(GhError, match="tree unavailable"):
            await create_pr_review_task(db_session, repo, PR_DATA)

    assert (await db_session.execute(select(PRReview))).scalars().all() == []
    assert (await db_session.execute(select(Task))).scalars().all() == []


@pytest.mark.asyncio
async def test_create_pr_review_task_oversize_stages_nothing(
    db_session,
):
    repo = _make_repo(provider="codex")
    db_session.add(repo)
    await db_session.commit()
    prepared = _prepared_context()
    prepared["material"]["patch"] = "x" * (
        pr_review_service._CODEX_PR_REVIEW_PROMPT_MAX_CHARS + 1
    )

    with pytest.raises(
        pr_review_service.PRReviewInputTooLarge,
        match="unsupported_input_size",
    ):
        await create_pr_review_task(
            db_session,
            repo,
            PR_DATA,
            prepared_context=prepared,
        )

    assert not db_session.new
    assert (await db_session.execute(select(PRReview))).scalars().all() == []
    assert (await db_session.execute(select(Task))).scalars().all() == []
    assert (await db_session.execute(select(Project))).scalars().all() == []


@pytest.mark.asyncio
async def test_create_pr_review_task_codex_uses_codex_default(
    db_session,
):
    repo = _make_repo(provider="codex", review_model=None)
    db_session.add(repo)
    await db_session.commit()
    with (
        patch.object(
            pr_review_service,
            "prepare_pr_review_context",
            AsyncMock(return_value=_prepared_context()),
        ),
        patch.object(
            pr_review_service.secrets,
            "token_hex",
            side_effect=[ACTION_NONCE, "1" * 32, "2" * 32],
        ),
    ):
        review = await create_pr_review_task(db_session, repo, PR_DATA)
    task = await db_session.get(Task, review.task_id)
    assert task.provider == "codex"
    assert task.model


# ---------------------------------------------------------------------------
# Strict terminal recommendation and exact retry generation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("content", "expected_result", "expected_body", "error_fragment"),
    [
        (
            _terminal_output("lgtm_comment", "Looks good."),
            "lgtm_comment",
            "Looks good.",
            None,
        ),
        (
            _terminal_output("review_comments", "Fix race."),
            "review_comments",
            "Fix race.",
            None,
        ),
        (
            _terminal_output("approved_merged", ""),
            "approved_merged",
            "",
            None,
        ),
        ("PR_REVIEW_RESULT: lgtm_comment", None, None, "exactly one"),
        (
            _terminal_output() + "\n",
            None,
            None,
            "exactly one",
        ),
        (
            _terminal_output() + "\n" + _terminal_output(),
            None,
            None,
            "exactly one",
        ),
        (
            _terminal_output("review_comments", ""),
            None,
            None,
            "non-empty",
        ),
        (
            _terminal_output("lgtm_comment", "bad\x00body"),
            None,
            None,
            "NUL",
        ),
    ],
)
def test_parse_pr_review_output_is_strict(
    content,
    expected_result,
    expected_body,
    error_fragment,
):
    result, body, error = pr_review_service._parse_pr_review_output(content)
    assert result == expected_result
    assert body == expected_body
    if error_fragment is None:
        assert error is None
    else:
        assert error_fragment in error


def test_parse_pr_review_output_rejects_oversized_body():
    result, body, error = pr_review_service._parse_pr_review_output(
        _terminal_output(
            "review_comments",
            "x" * (pr_review_service._MAX_REVIEW_BODY_BYTES + 1),
        )
    )
    assert result is None and body is None
    assert "61440-byte" in error


@pytest.mark.asyncio
async def test_read_terminal_output_uses_only_exact_retry_generation(
    db_session,
    repo,
):
    review, task = await _make_review(
        db_session,
        repo,
        retry_count=5,
    )
    db_session.add_all([
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="message",
            role="assistant",
            content=_terminal_output("lgtm_comment", "Current."),
            timestamp=task.started_at + timedelta(seconds=1),
        ),
        # Higher id and a current-looking timestamp must not let retry 4 win.
        LogEntry(
            task_id=task.id,
            task_retry_count=4,
            event_type="result",
            content=_terminal_output("review_comments", "Stale."),
            timestamp=task.started_at + timedelta(seconds=2),
        ),
    ])
    await db_session.commit()

    result, body, error = (
        await pr_review_service._read_terminal_pr_review_result(
            db_session,
            task.id,
            5,
        )
    )
    assert (result, body, error) == ("lgtm_comment", "Current.", None)
    assert review.task_id == task.id


@pytest.mark.asyncio
async def test_read_terminal_output_ignores_late_backfilled_chatter(
    db_session,
    repo,
):
    _review, task = await _make_review(
        db_session,
        repo,
        retry_count=5,
    )
    db_session.add_all([
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="result",
            role="assistant",
            content=_terminal_output("lgtm_comment", "Verified."),
            timestamp=task.started_at + timedelta(seconds=2),
        ),
        # An older Worker message may be appended later with a higher local id.
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="message",
            role="assistant",
            content="Still checking the patch.",
            timestamp=task.started_at + timedelta(seconds=1),
        ),
    ])
    await db_session.commit()

    output = await pr_review_service._read_terminal_pr_review_result(
        db_session,
        task.id,
        5,
    )

    assert output == ("lgtm_comment", "Verified.", None)


@pytest.mark.asyncio
async def test_read_terminal_output_rejects_conflicting_strict_blocks(
    db_session,
    repo,
):
    _review, task = await _make_review(
        db_session,
        repo,
        retry_count=5,
    )
    db_session.add_all([
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="message",
            role="assistant",
            content=_terminal_output("lgtm_comment", "First."),
            timestamp=task.started_at + timedelta(seconds=1),
        ),
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="result",
            role="assistant",
            content=_terminal_output("review_comments", "Second."),
            timestamp=task.started_at + timedelta(seconds=2),
        ),
    ])
    await db_session.commit()

    result, body, error = (
        await pr_review_service._read_terminal_pr_review_result(
            db_session,
            task.id,
            5,
        )
    )

    assert result is None and body is None
    assert "conflicting terminal outputs" in error


@pytest.mark.asyncio
async def test_read_terminal_output_rejects_unscoped_legacy_log(
    db_session,
    repo,
):
    _review, task = await _make_review(
        db_session,
        repo,
        retry_count=6,
    )
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=None,
        event_type="result",
        content=_terminal_output(),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()

    result, body, error = (
        await pr_review_service._read_terminal_pr_review_result(
            db_session,
            task.id,
            6,
        )
    )
    assert result is None and body is None
    assert "no terminal output" in error


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_count", [None, -1, True])
async def test_read_terminal_output_requires_explicit_retry_generation(
    db_session,
    repo,
    retry_count,
):
    _review, task = await _make_review(db_session, repo)
    result, body, error = (
        await pr_review_service._read_terminal_pr_review_result(
            db_session,
            task.id,
            retry_count,
        )
    )
    assert result is None and body is None
    assert "missing or invalid" in error


# ---------------------------------------------------------------------------
# Backend-only GitHub publishing (structured stdin JSON, pinned commit/nonce)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gh_api_json_sends_dynamic_body_only_over_stdin():
    malicious = "Review `touch /tmp/nope` and $(touch /tmp/nope)"
    process = SimpleNamespace(returncode=0)
    seen = {}

    async def communicate(value=None):
        seen["stdin"] = value
        return b'{"id":1}', b""

    process.communicate = communicate
    spawn = AsyncMock(return_value=process)
    with patch.object(
        pr_review_service.asyncio,
        "create_subprocess_exec",
        spawn,
    ):
        result = await pr_review_service._gh_api_json(
            "repos/owner/repo/pulls/7/reviews",
            method="POST",
            payload={"body": malicious},
        )

    assert result == {"id": 1}
    argv = spawn.await_args.args
    assert argv == (
        "gh",
        "api",
        "--method",
        "POST",
        "repos/owner/repo/pulls/7/reviews",
        "--input",
        "-",
    )
    assert malicious not in " ".join(argv)
    assert json.loads(seen["stdin"]) == {"body": malicious}
    assert spawn.await_args.kwargs["stdin"] is asyncio.subprocess.PIPE


@pytest.mark.asyncio
async def test_update_pr_branch_uses_expected_head_and_update_branch_endpoint():
    snapshot = {
        "state": "OPEN",
        "isDraft": False,
        "baseRefName": "main",
        "baseRefOid": "b" * 40,
        "headRefOid": "a" * 40,
        "mergedAt": None,
        "mergedBy": None,
        "mergeCommit": None,
    }
    api = AsyncMock(return_value={
        "message": "Updating pull request branch.",
        "url": "https://github.com/owner/repo/pull/7",
    })
    with (
        patch.object(pr_review_service, "_gh_pr_view", AsyncMock(return_value=snapshot)),
        patch.object(pr_review_service, "_gh_api_json", api),
    ):
        result = await pr_review_service.update_pr_branch(
            repo_name="owner/repo",
            pr_number=7,
            base_ref="main",
            expected_base_sha="1" * 40,
            expected_head_sha="a" * 40,
        )
    assert result == {
        "message": "Updating pull request branch.",
        "sha": None,
        "ref": "main",
    }
    api.assert_awaited_once_with(
        "repos/owner/repo/pulls/7/update-branch",
        method="PUT",
        payload={"expected_head_sha": "a" * 40, "update_method": "merge"},
        max_output_bytes=pr_review_service._MAX_GH_PR_VIEW_RESPONSE_BYTES,
    )


@pytest.mark.asyncio
async def test_update_pr_branch_rejects_stale_expected_head_before_write():
    from backend.services.pr_review_service import PRBranchUpdateConflict

    snapshot = {
        "state": "OPEN",
        "isDraft": False,
        "baseRefName": "main",
        "baseRefOid": "b" * 40,
        "headRefOid": "d" * 40,
        "mergedAt": None,
        "mergedBy": None,
        "mergeCommit": None,
    }
    api = AsyncMock()
    with (
        patch.object(pr_review_service, "_gh_pr_view", AsyncMock(return_value=snapshot)),
        patch.object(pr_review_service, "_gh_api_json", api),
    ):
        with pytest.raises(PRBranchUpdateConflict, match="head changed"):
            await pr_review_service.update_pr_branch(
                repo_name="owner/repo",
                pr_number=7,
                base_ref="main",
                expected_base_sha="1" * 40,
                expected_head_sha="a" * 40,
            )
    api.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stdout", "stderr", "returncode", "match"),
    [
        (b"not-json", b"", 0, "invalid gh output"),
        (b"[]", b"", 0, "expected a JSON object"),
        (b"", b"HTTP 401: bad credentials", 1, "HTTP 401"),
    ],
)
async def test_gh_api_json_fails_closed(
    stdout,
    stderr,
    returncode,
    match,
):
    process = SimpleNamespace(returncode=returncode)

    async def communicate(_value=None):
        return stdout, stderr

    process.communicate = communicate
    with patch.object(
        pr_review_service.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=process),
    ):
        with pytest.raises(GhError, match=match):
            await pr_review_service._gh_api_json("repos/owner/repo")


def test_review_evidence_marker_is_hidden_from_rendered_human_body():
    body = pr_review_service._review_body_with_evidence(
        "Review passed with no blocking findings.",
        ACTION_NONCE,
    )

    assert body.endswith(f"\n\n{REVIEW_EVIDENCE_MARKER}")
    assert LEGACY_REVIEW_EVIDENCE_MARKER not in body
    # GitHub Markdown does not render HTML comments. The text before the
    # final comment is therefore the complete human-visible review body.
    visible_markdown = body.removesuffix(REVIEW_EVIDENCE_MARKER).rstrip()
    assert visible_markdown == "Review passed with no blocking findings."
    for internal_protocol in (
        ACTION_NONCE,
        "nonce",
        "PR_REVIEW_",
        "schema_version",
        "{",
        "}",
    ):
        assert internal_protocol not in visible_markdown


def test_review_evidence_reader_requires_an_exact_final_marker():
    hidden = f"Readable review.\n\n{REVIEW_EVIDENCE_MARKER}"
    legacy = f"Readable legacy review.\n\n{LEGACY_REVIEW_EVIDENCE_MARKER}"

    assert pr_review_service._review_body_has_evidence(hidden, ACTION_NONCE)
    assert pr_review_service._review_body_has_evidence(legacy, ACTION_NONCE)
    assert not pr_review_service._review_body_has_evidence(
        f"{hidden}\nvisible trailing text",
        ACTION_NONCE,
    )
    assert not pr_review_service._review_body_has_evidence(
        f"quoted {REVIEW_EVIDENCE_MARKER} inline",
        ACTION_NONCE,
    )


@pytest.mark.asyncio
async def test_publish_changes_review_uses_pinned_commit_nonce_and_json():
    gh_view = AsyncMock(return_value=_snapshot())
    api = AsyncMock(return_value=_comment_response(
        body=f"Fix the race.\n\n{REVIEW_EVIDENCE_MARKER}",
    ))
    find_review = AsyncMock(return_value=None)
    find_merge = AsyncMock()
    kwargs = _publisher_kwargs(
        result="review_comments",
        review_body="Fix the race.",
    )
    with (
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("commented", "review_comments")
    find_review.assert_awaited_once()
    find_merge.assert_not_awaited()
    assert kwargs["ensure_current"].await_count == 2
    assert api.await_args.args == ("repos/owner/repo/pulls/7/reviews",)
    assert api.await_args.kwargs == {
        "method": "POST",
        "payload": {
            "body": (
                "Fix the race.\n\n"
                f"Reviewed commit: `{PR_DATA['head_sha']}`.\n\n"
                f"{REVIEW_EVIDENCE_MARKER}"
            ),
            "commit_id": PR_DATA["head_sha"],
            "event": "COMMENT",
        },
    }


@pytest.mark.asyncio
async def test_publish_lgtm_creates_non_authorizing_backend_comment():
    api = AsyncMock(return_value=_comment_response())
    find_review = AsyncMock(return_value=None)
    find_merge = AsyncMock()
    kwargs = _publisher_kwargs(
        review_body="agent body is not approval evidence",
    )
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)
    assert result == ("approved", "lgtm_comment")
    find_merge.assert_not_awaited()
    payload = api.await_args.kwargs["payload"]
    assert payload["event"] == "COMMENT"
    assert payload["commit_id"] == PR_DATA["head_sha"]
    assert REVIEW_EVIDENCE_MARKER in payload["body"]
    assert LEGACY_REVIEW_EVIDENCE_MARKER not in payload["body"]
    assert "ready to merge" in payload["body"]
    assert PR_DATA["head_sha"] in payload["body"]
    assert "agent body" not in payload["body"]


@pytest.mark.asyncio
async def test_publish_comment_does_not_retry_an_impossible_self_approval_error():
    api = AsyncMock(side_effect=GhError("Can not approve your own pull request"))
    find_review = AsyncMock(return_value=None)
    find_merge = AsyncMock()
    kwargs = _publisher_kwargs()
    with (
            patch.object(
                pr_review_service,
                "_gh_pr_view",
                AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        with pytest.raises(GhError, match="Can not approve"):
            await pr_review_service._publish_review_action(**kwargs)
    assert api.await_count == 1
    assert api.await_args.kwargs["payload"]["event"] == "COMMENT"
    assert kwargs["ensure_current"].await_count == 1
    find_merge.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_finding_comment_does_not_retry_state_change_error():
    api = AsyncMock(
        side_effect=GhError(
            "Review Can not request changes on your own pull request"
        )
    )
    find_review = AsyncMock(return_value=None)
    kwargs = _publisher_kwargs(
        result="review_comments",
        review_body="blocking findings",
    )
    with (
            patch.object(
                pr_review_service,
                "_gh_pr_view",
                AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
    ):
        with pytest.raises(GhError, match="request changes"):
            await pr_review_service._publish_review_action(**kwargs)
    assert api.await_count == 1
    payload = api.await_args.kwargs["payload"]
    assert payload["event"] == "COMMENT"
    assert payload["commit_id"] == PR_DATA["head_sha"]
    assert "blocking findings" in payload["body"]
    assert REVIEW_EVIDENCE_MARKER in payload["body"]
    assert LEGACY_REVIEW_EVIDENCE_MARKER not in payload["body"]
    assert kwargs["ensure_current"].await_count == 1


@pytest.mark.asyncio
async def test_publish_auto_merge_pins_slash_base_ref_and_confirms_merge():
    base_ref = "release/2026"
    api = AsyncMock(side_effect=[
        _comment_response(),
        {
            "ref": f"refs/heads/{base_ref}",
            "object": {"type": "commit", "sha": PR_DATA["head_sha"]},
        },
    ])
    gh_view = AsyncMock(return_value=_snapshot(base_ref=base_ref))
    find_review = AsyncMock(return_value=None)
    find_merge = AsyncMock(side_effect=[False, True])
    publish_merged_comment = AsyncMock()
    kwargs = _publisher_kwargs(
        result="approved_merged",
        auto_merge=True,
        base_ref=base_ref,
    )
    with (
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
        patch.object(
            pr_review_service,
            "_publish_merged_comment",
            publish_merged_comment,
        ),
        patch.object(
            pr_review_service,
            "_require_direct_merge_protection",
            AsyncMock(),
        ),
        patch.object(
            pr_review_service,
            "_require_safe_base_chain",
            AsyncMock(),
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)
    assert result == ("merged", "approved_merged")
    merge_call = api.await_args_list[1]
    assert merge_call.args == (
        "repos/owner/repo/git/refs/heads/release%2F2026",
    )
    assert merge_call.kwargs["method"] == "PATCH"
    assert merge_call.kwargs["payload"]["sha"] == PR_DATA["head_sha"]
    assert merge_call.kwargs["payload"]["force"] is False
    assert not any(
        call_.args[0] == "repos/owner/repo/pulls/7/merge"
        for call_ in api.await_args_list
    )
    assert find_merge.await_count == 2
    assert kwargs["ensure_current"].await_count == 3
    publish_merged_comment.assert_awaited_once_with(
        repo_name="owner/repo",
        pr_number=PR_DATA["number"],
        base_ref=base_ref,
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        nonce=ACTION_NONCE,
        actor=ACTOR,
        current_actor=ACTOR,
        publishing_started_at=PUBLISHING_STARTED_AT,
        merge_method="fast-forward",
        ensure_current=kwargs["ensure_current"],
    )


@pytest.mark.asyncio
async def test_legacy_squash_outbox_without_evidence_never_replays_merge():
    api = AsyncMock()
    kwargs = _publisher_kwargs(
        result="approved_merged",
        auto_merge=True,
        merge_method="squash",
    )
    with (
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(return_value=False),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        pytest.raises(GhError, match="automatic replay is disabled") as raised,
    ):
        await pr_review_service._publish_review_action(**kwargs)
    assert pr_review_service._terminal_publication_error(raised.value)
    api.assert_not_awaited()


@pytest.mark.asyncio
async def test_auto_merge_blocking_review_does_not_require_merge_method():
    kwargs = _publisher_kwargs(
        result="review_comments",
        review_body="Blocking issue.",
        auto_merge=True,
        merge_method=None,
    )
    with patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="CHANGES_REQUESTED"),
        ), patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ):
        assert await pr_review_service._publish_review_action(**kwargs) == (
            "commented",
            "review_comments",
        )


@pytest.mark.asyncio
async def test_auto_merge_revalidates_frozen_required_ci_before_put():
    api = AsyncMock()
    ci = AsyncMock(return_value=(
        "failed",
        "Failed: tests (github-actions)",
        {"head_sha": PR_DATA["head_sha"]},
    ))
    kwargs = _publisher_kwargs(
        result="approved_merged",
        auto_merge=True,
        wait_for_ci=True,
        required_checks=[{
            "kind": "check_run",
            "name": "tests",
            "app_slug": "github-actions",
        }],
        ensure_zero_threads=AsyncMock(return_value=True),
    )
    with (
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(return_value=False),
        ),
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch(
            "backend.services.pr_review_panel.fetch_exact_head_ci",
            ci,
        ),
        patch.object(
            pr_review_service,
            "_require_direct_merge_protection",
            AsyncMock(),
        ),
        patch.object(
            pr_review_service,
            "_require_safe_base_chain",
            AsyncMock(),
        ),
    ):
        with pytest.raises(GhError, match="required CI is not passed"):
            await pr_review_service._publish_review_action(**kwargs)

    # A deterministic exact-head CI failure must stop the durable direct
    # merge action immediately; retrying the same failed check cannot help.
    assert pr_review_service._terminal_publication_error(
        GhError(
            "Exact-head required CI is not passed before merge: "
            "failed: tests"
        )
    )

    ci.assert_awaited_once_with(
        "owner/repo",
        PR_DATA["head_sha"],
        kwargs["required_checks"],
    )
    api.assert_not_awaited()
    kwargs["ensure_zero_threads"].assert_not_awaited()


@pytest.mark.asyncio
async def test_auto_merge_ci_pass_after_review_ack_does_not_repeat_review():
    api = AsyncMock(return_value={
        "ref": "refs/heads/main",
        "object": {"type": "commit", "sha": PR_DATA["head_sha"]},
    })
    ci = AsyncMock(return_value=(
        "passed",
        "1 required exact-head CI checks passed",
        {"head_sha": PR_DATA["head_sha"]},
    ))
    find_merge = AsyncMock(side_effect=[False, True])
    publish_comment = AsyncMock()
    kwargs = _publisher_kwargs(
        result="approved_merged",
        auto_merge=True,
        wait_for_ci=True,
        required_checks=[{
            "kind": "check_run",
            "name": "tests",
            "app_slug": "github-actions",
        }],
        ensure_zero_threads=AsyncMock(return_value=True),
    )
    with (
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(pr_review_service, "_find_merge_evidence", find_merge),
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_publish_merged_comment",
            publish_comment,
        ),
        patch(
            "backend.services.pr_review_panel.fetch_exact_head_ci",
            ci,
        ),
        patch.object(
            pr_review_service,
            "_require_direct_merge_protection",
            AsyncMock(),
        ),
        patch.object(
            pr_review_service,
            "_require_safe_base_chain",
            AsyncMock(),
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("merged", "approved_merged")
    api.assert_awaited_once()
    assert api.await_args.args == (
        "repos/owner/repo/git/refs/heads/main",
    )
    assert api.await_args.kwargs["method"] == "PATCH"
    ci.assert_awaited_once()
    kwargs["ensure_zero_threads"].assert_awaited_once()
    publish_comment.assert_awaited_once()


@pytest.mark.asyncio
async def test_pre_merge_zero_thread_gate_rejects_open_unpublished_blocker(
    db_session,
    repo,
):
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=PR_DATA["number"],
        current_base_sha=PR_DATA["base_sha"],
        current_head_sha=PR_DATA["head_sha"],
        status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=PR_DATA["number"],
        base_ref="main",
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        pr_title=PR_DATA["title"],
        pr_author=PR_DATA["author"],
        pr_url=PR_DATA["url"],
        status="publishing",
        pending_action="approved_merged",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="qa_engineer",
        provider="claude",
        status="changes_required",
        prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="3" * 64,
        role="qa_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=8,
        title="not yet published blocker",
        evidence="The state transition is unsafe.",
        impact="The merge would preserve the bug.",
        required_fix="Fix the transition.",
        test="Exercise the failure interleaving.",
        status="open",
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        thread_nonce="4" * 48,
        thread_status="pending",
    ))
    await db_session.commit()

    assert not await pr_review_service._publication_has_zero_blocking_threads(
        db_session,
        review_id=review.id,
        monitor_run_id=run.id,
        head_sha=PR_DATA["head_sha"],
        expected_pending_action="approved_merged",
    )


@pytest.mark.asyncio
async def test_publish_auto_merge_reconciles_lost_merge_ack_before_comment():
    api = AsyncMock(side_effect=[
        _comment_response(),
        GhError("merge response timed out"),
    ])
    find_merge = AsyncMock(side_effect=[False, True])
    publish_merged_comment = AsyncMock()
    kwargs = _publisher_kwargs(
        result="approved_merged",
        auto_merge=True,
    )
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
        patch.object(
            pr_review_service,
            "_publish_merged_comment",
            publish_merged_comment,
        ),
        patch.object(
            pr_review_service,
            "_require_direct_merge_protection",
            AsyncMock(),
        ),
        patch.object(
            pr_review_service,
            "_require_safe_base_chain",
            AsyncMock(),
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("merged", "approved_merged")
    assert api.await_count == 2
    assert find_merge.await_count == 2
    publish_merged_comment.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_existing_merge_evidence_still_requires_merged_comment():
    api = AsyncMock()
    gh_view = AsyncMock(return_value=_snapshot())
    publish_merged_comment = AsyncMock()
    kwargs = _publisher_kwargs(
        result="approved_merged",
        auto_merge=True,
    )
    with (
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(return_value=True),
        ),
        patch.object(
            pr_review_service,
            "_publish_merged_comment",
            publish_merged_comment,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("merged", "approved_merged")
    api.assert_not_awaited()
    gh_view.assert_not_awaited()
    publish_merged_comment.assert_awaited_once()


@pytest.mark.asyncio
async def test_find_review_evidence_ignores_forged_nonce_marker():
    forged = {
        **_comment_response(),
        "id": 94,
        "user": {"login": "untrusted-user"},
    }
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(return_value=[[forged]]),
    ):
        evidence = await pr_review_service._find_review_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            head_sha=PR_DATA["head_sha"],
            result="lgtm_comment",
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
        )

    assert evidence is None


@pytest.mark.asyncio
async def test_find_review_evidence_accepts_valid_among_forged_markers():
    forged = {
        **_comment_response(),
        "id": 94,
        "user": {"login": "untrusted-user"},
    }
    valid = _review_response()
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(return_value=[[forged, valid]]),
    ):
        evidence = await pr_review_service._find_review_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            head_sha=PR_DATA["head_sha"],
            result="lgtm_comment",
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
        )

    assert evidence == "APPROVED"


@pytest.mark.asyncio
async def test_find_review_evidence_reads_legacy_visible_marker_for_recovery():
    legacy = _review_response(
        body=f"legacy review\n\n{LEGACY_REVIEW_EVIDENCE_MARKER}",
    )
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(return_value=[[legacy]]),
    ):
        evidence = await pr_review_service._find_review_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            head_sha=PR_DATA["head_sha"],
            result="lgtm_comment",
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
        )

    assert evidence == "APPROVED"


@pytest.mark.parametrize(
    "merge_preferences",
    [
        {"allow_merge_commit": True, "allow_squash_merge": True},
        {"allow_merge_commit": False, "allow_squash_merge": True},
        {
            "allow_merge_commit": False,
            "allow_squash_merge": False,
            "allow_rebase_merge": True,
        },
        {},
    ],
)
def test_select_safe_merge_method_uses_direct_ref_capability_not_preferences(
    merge_preferences,
):
    repo_info = _direct_ref_repo_info(**merge_preferences)
    assert pr_review_service._select_safe_merge_method(
        repo_info,
        expected_repo_name="OWNER/REPO",
    ) == "fast-forward"


@pytest.mark.parametrize(
    "repo_info",
    [
        None,
        {},
        _direct_ref_repo_info(full_name=None),
        _direct_ref_repo_info(archived=None),
        _direct_ref_repo_info(disabled=0),
        _direct_ref_repo_info(permissions=None),
        _direct_ref_repo_info(permissions={}),
        _direct_ref_repo_info(permissions={"push": 1}),
    ],
)
def test_select_safe_merge_method_fails_closed_on_malformed_capability(
    repo_info,
):
    with pytest.raises(
        pr_review_service.GhRepositoryCapabilityError,
        match="response is malformed",
    ):
        pr_review_service._select_safe_merge_method(
            repo_info,
            expected_repo_name="owner/repo",
        )


@pytest.mark.parametrize(
    ("repo_info", "match"),
    [
        (
            _direct_ref_repo_info(full_name="other/repo"),
            "repository identity mismatched",
        ),
        (_direct_ref_repo_info(archived=True), "archived or disabled"),
        (_direct_ref_repo_info(disabled=True), "archived or disabled"),
        (
            _direct_ref_repo_info(permissions={"push": False}),
            "lacks push permission",
        ),
    ],
)
def test_select_safe_merge_method_rejects_unavailable_direct_ref(
    repo_info,
    match,
):
    with pytest.raises(
        pr_review_service.GhRepositoryCapabilityError,
        match=match,
    ):
        pr_review_service._select_safe_merge_method(
            repo_info,
            expected_repo_name="owner/repo",
        )


@pytest.mark.asyncio
async def test_freeze_safe_merge_method_binds_repository_capability_response():
    api = AsyncMock(return_value=_direct_ref_repo_info())
    with patch.object(pr_review_service, "_gh_api_json", api):
        assert await pr_review_service._freeze_safe_merge_method(
            "owner/repo"
        ) == "fast-forward"
    api.assert_awaited_once_with("repos/owner/repo")


@pytest.mark.asyncio
async def test_freeze_safe_merge_method_preserves_retryable_api_error_class():
    transport_error = GhError("temporary GitHub API timeout")
    with (
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(side_effect=transport_error),
        ),
        pytest.raises(GhError) as raised,
    ):
        await pr_review_service._freeze_safe_merge_method("owner/repo")
    assert raised.value is transport_error
    assert not isinstance(
        raised.value,
        pr_review_service.GhRepositoryCapabilityError,
    )


@pytest.mark.parametrize("role", ("write", "maintain", "admin"))
@pytest.mark.parametrize(
    "merge_method",
    ("merge", "squash", "fast-forward"),
)
def test_direct_merge_protection_accepts_only_standard_non_bypass_roles(
    role,
    merge_method,
):
    protection = _safe_direct_merge_protection()
    if merge_method == "fast-forward":
        protection["required_pull_request_reviews"] = None
    pr_review_service._validate_direct_merge_protection(
        protection,
        _standard_collaborator_permission(role=role),
        [],
        actor=ACTOR,
        merge_method=merge_method,
    )


@pytest.mark.parametrize(
    "unsafe_case",
    (
        "active_ruleset",
        "non_strict_checks",
        "empty_checks",
        "admins_not_enforced",
        "force_pushes",
        "branch_deletion",
        "conversation_resolution",
        "malformed_conversation_resolution",
        "missing_bypass_allowances",
        "nonempty_bypass_allowances",
        "linear_merge_commit",
        "fast_forward_required_reviews",
        "custom_role",
        "actor_mismatch",
        "non_user_actor",
    ),
)
def test_direct_merge_protection_fails_closed_for_unsafe_controls(unsafe_case):
    protection = deepcopy(_safe_direct_merge_protection())
    permission = _standard_collaborator_permission()
    rules = []
    merge_method = "merge"
    if unsafe_case == "active_ruleset":
        rules = [{"type": "required_status_checks"}]
    elif unsafe_case == "non_strict_checks":
        protection["required_status_checks"]["strict"] = False
    elif unsafe_case == "empty_checks":
        protection["required_status_checks"]["contexts"] = []
        protection["required_status_checks"]["checks"] = []
    elif unsafe_case == "admins_not_enforced":
        protection["enforce_admins"]["enabled"] = False
    elif unsafe_case == "force_pushes":
        protection["allow_force_pushes"]["enabled"] = True
    elif unsafe_case == "branch_deletion":
        protection["allow_deletions"]["enabled"] = True
    elif unsafe_case == "conversation_resolution":
        protection["required_conversation_resolution"]["enabled"] = True
    elif unsafe_case == "malformed_conversation_resolution":
        protection["required_conversation_resolution"] = None
    elif unsafe_case == "missing_bypass_allowances":
        protection["required_pull_request_reviews"] = {}
    elif unsafe_case == "nonempty_bypass_allowances":
        protection["required_pull_request_reviews"][
            "bypass_pull_request_allowances"
        ]["apps"] = [{"slug": "merge-bot"}]
    elif unsafe_case == "linear_merge_commit":
        protection["required_linear_history"]["enabled"] = True
    elif unsafe_case == "fast_forward_required_reviews":
        merge_method = "fast-forward"
    elif unsafe_case == "custom_role":
        permission["permission"] = "write"
        permission["role_name"] = "release-manager"
        permission["user"]["role_name"] = "release-manager"
    elif unsafe_case == "actor_mismatch":
        permission = _standard_collaborator_permission(actor="other-bot")
    elif unsafe_case == "non_user_actor":
        permission = _standard_collaborator_permission(user_type="Bot")

    with pytest.raises(GhError, match="protection policy is unsafe"):
        pr_review_service._validate_direct_merge_protection(
            protection,
            permission,
            rules,
            actor=ACTOR,
            merge_method=merge_method,
        )


def test_direct_merge_protection_binds_frozen_ci_to_protected_app():
    protection = _safe_direct_merge_protection()
    protection["required_pull_request_reviews"] = None
    required, exact_ci = _exact_ci_coverage()
    pr_review_service._validate_direct_merge_protection(
        protection,
        _standard_collaborator_permission(),
        [],
        actor=ACTOR,
        merge_method="fast-forward",
        frozen_required_checks=required,
        exact_ci=exact_ci,
        head_sha=PR_DATA["head_sha"],
    )


@pytest.mark.parametrize("mismatch", ("context", "app", "status_policy"))
def test_direct_merge_protection_rejects_unprotected_frozen_ci(mismatch):
    protection = _safe_direct_merge_protection()
    protection["required_pull_request_reviews"] = None
    required, exact_ci = _exact_ci_coverage()
    if mismatch == "context":
        protection["required_status_checks"]["checks"][0]["context"] = "lint"
    elif mismatch == "app":
        protection["required_status_checks"]["checks"][0]["app_id"] = 2
    else:
        required[0]["kind"] = "status"
        exact_ci["required"][0]["kind"] = "status"
        exact_ci["observed"][0]["kind"] = "status"
    with pytest.raises(GhError, match="protection policy is unsafe"):
        pr_review_service._validate_direct_merge_protection(
            protection,
            _standard_collaborator_permission(),
            [],
            actor=ACTOR,
            merge_method="fast-forward",
            frozen_required_checks=required,
            exact_ci=exact_ci,
            head_sha=PR_DATA["head_sha"],
        )


@pytest.mark.asyncio
async def test_require_direct_merge_protection_reads_exact_classic_controls():
    classic = _safe_direct_merge_protection()
    permission = _standard_collaborator_permission()
    json_read = AsyncMock(side_effect=[classic, permission])
    rules_read = AsyncMock(return_value=[])
    with (
        patch.object(pr_review_service, "_gh_api_json", json_read),
        patch.object(pr_review_service, "_gh_api_value", rules_read),
    ):
        await pr_review_service._require_direct_merge_protection(
            repo_name="owner/repo",
            base_ref="release/2026",
            actor=ACTOR,
            merge_method="squash",
        )

    assert [call.args[0] for call in json_read.await_args_list] == [
        "repos/owner/repo/branches/release%2F2026/protection",
        f"repos/owner/repo/collaborators/{ACTOR}/permission",
    ]
    rules_read.assert_awaited_once_with(
        "repos/owner/repo/rules/branches/release%2F2026?per_page=1&page=1"
    )


@pytest.mark.asyncio
async def test_trusted_direct_merge_requires_only_exact_actor_permission():
    permission = _standard_collaborator_permission()
    json_read = AsyncMock(return_value=permission)
    rules_read = AsyncMock()
    with (
        patch.object(pr_review_service, "_gh_api_json", json_read),
        patch.object(pr_review_service, "_gh_api_value", rules_read),
    ):
        await pr_review_service._require_direct_merge_protection(
            repo_name="owner/repo",
            base_ref="main",
            actor=ACTOR,
            merge_method="fast-forward",
            strict_branch_protection=False,
        )

    json_read.assert_awaited_once_with(
        f"repos/owner/repo/collaborators/{ACTOR}/permission"
    )
    rules_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_require_direct_merge_protection_rejects_unverifiable_rulesets():
    with (
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(side_effect=[
                _safe_direct_merge_protection(),
                _standard_collaborator_permission(),
            ]),
        ),
        patch.object(
            pr_review_service,
            "_gh_api_value",
            AsyncMock(side_effect=GhError("gh: Not Found (HTTP 404)")),
        ),
    ):
        with pytest.raises(GhError, match="could not be verified"):
            await pr_review_service._require_direct_merge_protection(
                repo_name="owner/repo",
                base_ref="main",
                actor=ACTOR,
                merge_method="merge",
            )


@pytest.mark.asyncio
async def test_commit_ancestry_requires_exact_ahead_compare_response():
    ancestor = "1" * 40
    descendant = "2" * 40
    api = AsyncMock(return_value=_ahead_compare_response(ancestor, descendant))
    with patch.object(pr_review_service, "_gh_api_json", api):
        await pr_review_service._require_commit_ancestor(
            repo_name="owner/repo",
            ancestor=ancestor,
            descendant=descendant,
        )
    api.assert_awaited_once_with(
        f"repos/owner/repo/compare/{ancestor}...{descendant}?per_page=1&page=1",
        max_output_bytes=pr_review_service._MAX_GH_COMPARE_RESPONSE_BYTES,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ("diverged", "wrong_url", "wrong_merge_base", "truncated_count"),
)
async def test_commit_ancestry_rejects_ambiguous_compare_response(mutation):
    ancestor = "1" * 40
    descendant = "2" * 40
    response = _ahead_compare_response(ancestor, descendant)
    if mutation == "diverged":
        response["status"] = "diverged"
    elif mutation == "wrong_url":
        response["url"] = "https://api.github.com/repos/other/repo/compare/x...y"
    elif mutation == "wrong_merge_base":
        response["merge_base_commit"]["sha"] = "3" * 40
    elif mutation == "truncated_count":
        response["ahead_by"] = 2
        response["total_commits"] = 1
    with patch.object(
        pr_review_service,
        "_gh_api_json",
        AsyncMock(return_value=response),
    ):
        with pytest.raises(GhError, match="ancestry"):
            await pr_review_service._require_commit_ancestor(
                repo_name="owner/repo",
                ancestor=ancestor,
                descendant=descendant,
            )


@pytest.mark.asyncio
async def test_safe_base_chain_proves_captured_actual_and_head_edges():
    captured = "1" * 40
    actual = "2" * 40
    head = "3" * 40
    ancestor = AsyncMock()
    with patch.object(
        pr_review_service,
        "_require_commit_ancestor",
        ancestor,
    ):
        await pr_review_service._require_safe_base_chain(
            repo_name="owner/repo",
            captured_base=captured,
            actual_base=actual,
            head_sha=head,
        )
    assert ancestor.await_args_list == [
        call(
            repo_name="owner/repo",
            ancestor=captured,
            descendant=actual,
        ),
        call(
            repo_name="owner/repo",
            ancestor=actual,
            descendant=head,
        ),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("merge_method", "parents"),
    [
        ("merge", [PR_DATA["base_sha"], PR_DATA["head_sha"]]),
        ("squash", [PR_DATA["base_sha"]]),
    ],
)
async def test_find_merge_evidence_binds_exact_subject_actor_time_and_method(
    merge_method,
    parents,
):
    merge_sha = "9" * 40
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(
                state="MERGED",
                merged_at="2026-07-31T00:00:01Z",
                merged_by=ACTOR,
                merge_commit_sha=merge_sha,
            )),
        ),
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(return_value={
                "sha": merge_sha,
                "commit": {
                    "message": (
                        "Automated review\n\n"
                        f"CCM review nonce: {ACTION_NONCE}"
                    ),
                },
                "parents": [{"sha": sha} for sha in parents],
            }),
        ),
        patch.object(
            pr_review_service,
            "_require_safe_base_chain",
            AsyncMock(),
        ),
    ):
        assert await pr_review_service._find_merge_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            base_ref="main",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
            merge_method=merge_method,
        )


@pytest.mark.asyncio
async def test_find_fast_forward_merge_evidence_binds_frozen_subject():
    base_ref = "release/2026"
    current_base = "7" * 40
    gh_view = AsyncMock(return_value=_snapshot(
        state="MERGED",
        base_ref=base_ref,
        base_sha=current_base,
        merged_at="2026-07-31T00:00:01Z",
        merged_by=ACTOR,
        merge_commit_sha=PR_DATA["head_sha"],
    ))
    ancestor = AsyncMock()
    commit_read = AsyncMock()
    with (
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(pr_review_service, "_require_commit_ancestor", ancestor),
        patch.object(pr_review_service, "_gh_api_json", commit_read),
    ):
        assert await pr_review_service._find_merge_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            base_ref=base_ref,
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
            merge_method="fast-forward",
        )

    gh_view.assert_awaited_once_with(PR_DATA["number"], "owner/repo")
    assert ancestor.await_args_list == [
        call(
            repo_name="owner/repo",
            ancestor=PR_DATA["base_sha"],
            descendant=PR_DATA["head_sha"],
        ),
        call(
            repo_name="owner/repo",
            ancestor=PR_DATA["head_sha"],
            descendant=current_base,
        ),
    ]
    # Fast-forward evidence is the exact PR snapshot plus ancestry; it must
    # never be reconciled through a legacy merge-commit message lookup.
    commit_read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("snapshot_override", "started_at", "match"),
    [
        ({"base_ref": "main"}, PUBLISHING_STARTED_AT, "matching merge evidence"),
        ({"head_sha": "8" * 40}, PUBLISHING_STARTED_AT, "matching merge evidence"),
        ({"merged_by": "other-bot"}, PUBLISHING_STARTED_AT, "matching merge evidence"),
        (
            {"merged_at": "2026-07-30T23:59:54Z"},
            PUBLISHING_STARTED_AT,
            "matching merge evidence",
        ),
        (
            {"merge_commit_sha": "9" * 40},
            PUBLISHING_STARTED_AT,
            "malformed or mismatched",
        ),
    ],
)
async def test_find_fast_forward_merge_evidence_rejects_subject_drift(
    snapshot_override,
    started_at,
    match,
):
    snapshot_values = {
        "state": "MERGED",
        "base_ref": "release/2026",
        "base_sha": "7" * 40,
        "merged_at": "2026-07-31T00:00:01Z",
        "merged_by": ACTOR,
        "merge_commit_sha": PR_DATA["head_sha"],
    }
    snapshot_values.update(snapshot_override)
    ancestor = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(**snapshot_values)),
        ),
        patch.object(pr_review_service, "_require_commit_ancestor", ancestor),
        pytest.raises(GhError, match=match),
    ):
        await pr_review_service._find_merge_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            base_ref="release/2026",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=started_at,
            merge_method="fast-forward",
        )
    ancestor.assert_not_awaited()


@pytest.mark.asyncio
async def test_find_fast_forward_merge_evidence_rejects_unproven_frozen_base():
    rejected = GhError("GitHub PR base ancestry is unsafe for direct auto-merge")
    ancestor = AsyncMock(side_effect=rejected)
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(
                state="MERGED",
                base_ref="release/2026",
                base_sha=PR_DATA["head_sha"],
                merged_at="2026-07-31T00:00:01Z",
                merged_by=ACTOR,
                merge_commit_sha=PR_DATA["head_sha"],
            )),
        ),
        patch.object(pr_review_service, "_require_commit_ancestor", ancestor),
        pytest.raises(GhError, match="base ancestry is unsafe"),
    ):
        await pr_review_service._find_merge_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            base_ref="release/2026",
            base_sha="8" * 40,
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
            merge_method="fast-forward",
        )
    ancestor.assert_awaited_once_with(
        repo_name="owner/repo",
        ancestor="8" * 40,
        descendant=PR_DATA["head_sha"],
    )


@pytest.mark.asyncio
async def test_find_merge_evidence_rejects_public_nonce_copied_by_other_actor():
    merge_sha = "9" * 40
    commit = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(
                state="MERGED",
                merged_at="2026-07-31T00:00:01Z",
                merged_by="untrusted-maintainer",
                merge_commit_sha=merge_sha,
            )),
        ),
        patch.object(pr_review_service, "_gh_api_json", commit),
    ):
        with pytest.raises(GhError, match="without matching merge evidence"):
            await pr_review_service._find_merge_evidence(
                repo_name="owner/repo",
                pr_number=PR_DATA["number"],
                base_ref="main",
                base_sha=PR_DATA["base_sha"],
                head_sha=PR_DATA["head_sha"],
                nonce=ACTION_NONCE,
                actor=ACTOR,
                publishing_started_at=PUBLISHING_STARTED_AT,
                merge_method="merge",
            )
    commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_find_merge_evidence_rejects_same_sha_base_retarget():
    merge_sha = "9" * 40
    commit = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(
                state="MERGED",
                base_ref="release/2026",
                merged_at="2026-07-31T00:00:01Z",
                merged_by=ACTOR,
                merge_commit_sha=merge_sha,
            )),
        ),
        patch.object(pr_review_service, "_gh_api_json", commit),
        pytest.raises(GhError, match="without matching merge evidence"),
    ):
        await pr_review_service._find_merge_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            base_ref="main",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
            merge_method="merge",
        )

    commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_find_merge_evidence_rejects_wrong_captured_base_parent():
    merge_sha = "9" * 40
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(
                state="MERGED",
                merged_at="2026-07-31T00:00:01Z",
                merged_by=ACTOR,
                merge_commit_sha=merge_sha,
            )),
        ),
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(return_value={
                "sha": merge_sha,
                "commit": {
                    "message": f"CCM review nonce: {ACTION_NONCE}",
                },
                "parents": [
                    {"sha": "8" * 40},
                    {"sha": PR_DATA["head_sha"]},
                ],
            }),
        ),
        patch.object(
            pr_review_service,
            "_require_safe_base_chain",
            AsyncMock(side_effect=GhError(
                "GitHub PR base ancestry is unsafe for direct auto-merge"
            )),
        ),
    ):
        with pytest.raises(GhError, match="base ancestry is unsafe"):
            await pr_review_service._find_merge_evidence(
                repo_name="owner/repo",
                pr_number=PR_DATA["number"],
                base_ref="main",
                base_sha=PR_DATA["base_sha"],
                head_sha=PR_DATA["head_sha"],
                nonce=ACTION_NONCE,
                actor=ACTOR,
                publishing_started_at=PUBLISHING_STARTED_AT,
                merge_method="merge",
            )


def test_merged_comment_evidence_pins_actor_head_nonce_and_time():
    valid = _merged_issue_comment_response()
    assert pr_review_service._validate_merged_comment_evidence(
        valid,
        head_sha=PR_DATA["head_sha"],
        nonce=ACTION_NONCE,
        actor=ACTOR,
        publishing_started_at=PUBLISHING_STARTED_AT,
    ) == 93

    invalid_values = [
        {**valid, "user": {"login": "another-bot"}},
        _merged_issue_comment_response(head_sha="c" * 40),
        _merged_issue_comment_response(nonce="d" * 48),
        _merged_issue_comment_response(created_at="2026-07-30T23:59:00Z"),
    ]
    for invalid in invalid_values:
        with pytest.raises(GhError, match="malformed or mismatched"):
            pr_review_service._validate_merged_comment_evidence(
                invalid,
                head_sha=PR_DATA["head_sha"],
                nonce=ACTION_NONCE,
                actor=ACTOR,
                publishing_started_at=PUBLISHING_STARTED_AT,
            )


@pytest.mark.asyncio
async def test_find_merged_comment_evidence_ignores_forged_marker():
    forged = _merged_issue_comment_response(actor="untrusted-user")
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(return_value=[[forged]]),
    ):
        evidence = await pr_review_service._find_merged_comment_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
        )

    assert evidence is None


@pytest.mark.asyncio
async def test_find_merged_comment_evidence_accepts_valid_among_forged_markers():
    forged = {
        **_merged_issue_comment_response(actor="untrusted-user"),
        "id": 94,
    }
    valid = _merged_issue_comment_response()
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(return_value=[[forged, valid]]),
    ):
        evidence = await pr_review_service._find_merged_comment_evidence(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
        )

    assert evidence == valid["id"]


@pytest.mark.asyncio
async def test_publish_merged_comment_is_exact_and_reconciles_lost_ack():
    find_comment = AsyncMock(side_effect=[None, 93])
    find_merge = AsyncMock(return_value=True)
    api = AsyncMock(side_effect=GhError("comment response timed out"))
    ensure_current = AsyncMock(return_value=True)
    with (
        patch.object(
            pr_review_service,
            "_find_merged_comment_evidence",
            find_comment,
        ),
        patch.object(
            pr_review_service,
            "_require_safe_base_chain",
            AsyncMock(),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
    ):
        await pr_review_service._publish_merged_comment(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            base_ref="main",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            current_actor=ACTOR,
            publishing_started_at=PUBLISHING_STARTED_AT,
            merge_method="merge",
            ensure_current=ensure_current,
        )

    assert find_comment.await_count == 2
    find_merge.assert_awaited_once()
    assert ensure_current.await_count == 2
    api.assert_awaited_once()
    assert api.await_args.args == ("repos/owner/repo/issues/7/comments",)
    assert api.await_args.kwargs["method"] == "POST"
    comment_body = api.await_args.kwargs["payload"]["body"]
    assert "merged the exact reviewed head" in comment_body
    assert PR_DATA["head_sha"] in comment_body
    assert ACTION_NONCE in comment_body


@pytest.mark.asyncio
async def test_publish_merged_comment_existing_evidence_skips_writes():
    api = AsyncMock()
    find_merge = AsyncMock(return_value=True)
    ensure_current = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_find_merged_comment_evidence",
            AsyncMock(return_value=93),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
    ):
        await pr_review_service._publish_merged_comment(
            repo_name="owner/repo",
            pr_number=PR_DATA["number"],
            base_ref="main",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
            nonce=ACTION_NONCE,
            actor=ACTOR,
            current_actor="rotated-bot",
            publishing_started_at=PUBLISHING_STARTED_AT,
            merge_method="merge",
            ensure_current=ensure_current,
        )

    api.assert_not_awaited()
    find_merge.assert_awaited_once()
    ensure_current.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_merge_actor_rotation_stays_recoverable():
    find_merge = AsyncMock(return_value=True)
    ensure_current = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_find_merged_comment_evidence",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value="rotated-bot"),
        ),
    ):
        with pytest.raises(
            GhError,
            match="publishing identity changed.*merged comment",
        ) as exc:
            await pr_review_service._publish_merged_comment(
                repo_name="owner/repo",
                pr_number=PR_DATA["number"],
                base_ref="main",
                base_sha=PR_DATA["base_sha"],
                head_sha=PR_DATA["head_sha"],
                nonce=ACTION_NONCE,
                actor=ACTOR,
                current_actor="rotated-bot",
                publishing_started_at=PUBLISHING_STARTED_AT,
                merge_method="merge",
                ensure_current=ensure_current,
            )

    assert not pr_review_service._terminal_publication_error(exc.value)
    find_merge.assert_awaited_once()
    ensure_current.assert_awaited_once()


@pytest.mark.asyncio
async def test_post_merge_actor_rotation_keeps_durable_outbox_pending(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(
        db_session,
        repo,
        auto_merge=True,
    )
    await _arm_publishing(
        db_session,
        review,
        task,
        action="approved_merged",
    )
    lease_token = "9" * 48
    review.publishing_lease_token = lease_token
    review.publishing_lease_expires_at = (
        datetime.utcnow() + timedelta(minutes=5)
    )
    await db_session.commit()
    review_id = review.id
    repo_name = repo.repo_full_name

    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value="rotated-bot"),
        ),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(return_value=True),
        ),
        patch.object(
            pr_review_service,
            "_find_merged_comment_evidence",
            AsyncMock(return_value=None),
        ),
    ):
        await pr_review_service._resume_publishing_review_under_lease(
            db_session,
            review_id,
            repo_name,
            lease_token=lease_token,
        )

    refreshed = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    assert refreshed.status == "publishing"
    assert refreshed.pending_action == "approved_merged"
    assert refreshed.publishing_actor == ACTOR
    assert refreshed.publishing_lease_token is None
    assert refreshed.review_summary == "Looks good."
    assert refreshed.publication_state == "reconciling"
    assert refreshed.failure_stage == "github_identity"
    assert "publishing identity changed" in refreshed.publication_error
    assert "merged comment evidence" in refreshed.publication_error


@pytest.mark.asyncio
async def test_publish_existing_nonce_evidence_does_not_repeat_write():
    api = AsyncMock()
    gh_view = AsyncMock(return_value=_snapshot())
    find_merge = AsyncMock()
    kwargs = _publisher_kwargs()
    with (
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("approved", "lgtm_comment")
    api.assert_not_awaited()
    gh_view.assert_awaited_once()
    find_merge.assert_not_awaited()
    kwargs["ensure_current"].assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("result", ("lgtm_comment", "review_comments"))
async def test_recovered_review_evidence_rejects_same_sha_base_retarget(result):
    api = AsyncMock()
    kwargs = _publisher_kwargs(
        result=result,
        review_body="Blocking evidence" if result == "review_comments" else "",
    )
    with (
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value=(
                "CHANGES_REQUESTED"
                if result == "review_comments"
                else "APPROVED"
            )),
        ),
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(base_ref="release/2026")),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        pytest.raises(GhError, match="snapshot changed"),
    ):
        await pr_review_service._publish_review_action(**kwargs)

    api.assert_not_awaited()
    kwargs["ensure_current"].assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_rotated_actor_reconciles_old_actor_evidence():
    api = AsyncMock()
    kwargs = _publisher_kwargs(current_actor="replacement-bot")
    with (
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(),
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("approved", "lgtm_comment")
    api.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_rotated_actor_without_evidence_refuses_new_write():
    api = AsyncMock()
    kwargs = _publisher_kwargs(current_actor="replacement-bot")
    with (
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value="replacement-bot"),
        ),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value=None),
        ),
    ):
        with pytest.raises(GhError, match="identity changed"):
            await pr_review_service._publish_review_action(**kwargs)

    api.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_snapshot_change_blocks_write():
    api = AsyncMock()
    kwargs = _publisher_kwargs()
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(head_sha="9" * 40)),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(),
        ),
    ):
        with pytest.raises(GhError, match="snapshot changed"):
            await pr_review_service._publish_review_action(**kwargs)
    api.assert_not_awaited()
    kwargs["ensure_current"].assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_rejects_mismatched_created_review_evidence():
    api = AsyncMock(return_value=_review_response(head_sha="9" * 40))
    kwargs = _publisher_kwargs()
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(),
        ),
    ):
        with pytest.raises(GhError, match="mismatched review evidence"):
            await pr_review_service._publish_review_action(**kwargs)


# ---------------------------------------------------------------------------
# Completion orchestration: exact generation + frozen policy/nonce
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_review_reads_exact_terminal_body_and_publishes(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=3)
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=3,
        event_type="result",
        content=_terminal_output("lgtm_comment", "No blocking findings."),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()
    expected_task_id = task.id
    expected_task_started_at = task.started_at

    async def publish_after_durable_claim(**kwargs):
        await db_session.refresh(review)
        assert review.status == "publishing"
        assert review.pending_action == "lgtm_comment"
        assert review.pending_review_body == "No blocking findings."
        assert review.publishing_actor == ACTOR
        assert review.publishing_retry_count == 3
        assert review.publishing_task_started_at == expected_task_started_at
        assert review.publishing_started_at is not None
        kwargs["evidence_sink"].update({
            "github_review_id": 731,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-731",
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": review.publishing_started_at,
        })
        return "approved", "lgtm_comment"

    publish = AsyncMock(side_effect=publish_after_durable_claim)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=expected_task_id,
            terminal_task_retry_count=3,
        )

    await db_session.refresh(review)
    assert review.status == "approved"
    assert review.action_taken == "lgtm_comment"
    assert review.code_verdict == "pass"
    assert review.code_verdict_task_id == expected_task_id
    assert review.code_verdict_retry_count == 3
    assert review.code_verdict_task_started_at == expected_task_started_at
    assert review.code_verdict_recorded_at is not None
    assert review.completed_at is not None
    assert review.review_summary == "No blocking findings."
    assert "PR_REVIEW_BODY_BEGIN" not in review.review_summary
    assert "PR_REVIEW_BODY_END" not in review.review_summary
    assert "PR_REVIEW_RESULT:" not in review.review_summary
    assert review.pending_action is None
    assert review.pending_review_body is None
    assert review.publishing_actor is None
    assert review.publishing_retry_count is None
    assert review.publishing_task_started_at is None
    assert review.publishing_started_at is None
    assert review.publishing_lease_token is None
    assert review.publishing_lease_expires_at is None
    publish.assert_awaited_once()
    assert publish.await_args.kwargs["review_body"] == "No blocking findings."
    assert publish.await_args.kwargs["actor"] == ACTOR
    assert callable(publish.await_args.kwargs["ensure_current"])
    # Immutable code verdict, armed publication, and published receipt.
    assert no_broadcast.broadcast.await_count == 3


@pytest.mark.asyncio
async def test_check_review_changes_requested_persists_clean_agent_body(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=5)
    body = (
        "Authorization can be bypassed.\n\n"
        "Require the project ACL check before dispatching the task."
    )
    await _add_terminal_log(
        db_session,
        task,
        result="review_comments",
        body=body,
        retry_count=5,
    )
    expected_task_id = task.id
    expected_task_started_at = task.started_at

    async def publish_with_evidence(**kwargs):
        kwargs["evidence_sink"].update({
            "github_review_id": 732,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-732",
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": kwargs["publishing_started_at"],
        })
        return "commented", "review_comments"

    publish = AsyncMock(side_effect=publish_with_evidence)
    publish_findings = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
        patch.object(
            pr_review_service,
            "_publish_blocking_finding_threads",
            publish_findings,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            repo.repo_full_name,
            terminal_task_id=expected_task_id,
            terminal_task_retry_count=5,
        )

    await db_session.refresh(review)
    assert review.status == "commented"
    assert review.action_taken == "review_comments"
    assert review.code_verdict == "changes_required"
    assert review.code_verdict_task_id == expected_task_id
    assert review.code_verdict_retry_count == 5
    assert review.code_verdict_task_started_at == expected_task_started_at
    assert review.code_verdict_recorded_at is not None
    assert review.review_summary == body
    assert "PR_REVIEW_BODY_BEGIN" not in review.review_summary
    assert "PR_REVIEW_BODY_END" not in review.review_summary
    assert "PR_REVIEW_RESULT:" not in review.review_summary
    assert review.pending_action is None
    assert review.pending_review_body is None
    assert review.publishing_actor is None
    assert review.publishing_retry_count is None
    assert review.publishing_task_started_at is None
    assert review.publishing_started_at is None
    assert review.publishing_lease_token is None
    assert review.publishing_lease_expires_at is None
    publish.assert_awaited_once()
    assert publish.await_args.kwargs["review_body"] == body
    publish_findings.assert_awaited_once()
    assert no_broadcast.broadcast.await_count == 3


@pytest.mark.asyncio
async def test_background_terminal_arms_review_without_publishing_effect(
    db_session,
    repo,
    no_broadcast,
):
    """An exact PTY marker may stage the outbox but not write GitHub yet."""

    review, task = await _make_review(db_session, repo, retry_count=4)
    generation = "review-background-generation"
    task.pty_background_generation = generation
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=4,
        event_type="result",
        content=_terminal_output("lgtm_comment", "No blocking findings."),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()

    resume = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_resume_publishing_review",
            resume,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            repo.repo_full_name,
            terminal_task_id=task.id,
            terminal_task_retry_count=4,
            expected_background_generation=generation,
        )

    await db_session.refresh(review)
    await db_session.refresh(task)
    assert review.status == "publishing"
    assert review.pending_action == "lgtm_comment"
    assert task.pty_background_generation == generation
    resume.assert_not_awaited()
    # The immutable code verdict and durable publishing transition are
    # announced. The actual GitHub completion belongs to marker-free recovery.
    assert no_broadcast.broadcast.await_count == 2


@pytest.mark.asyncio
async def test_check_review_retries_transient_identity_read(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    review_id = review.id
    task_id = task.id
    retry_count = task.retry_count
    task_started_at = task.started_at
    repo_name = repo.repo_full_name
    await _add_terminal_log(
        db_session,
        task,
        result="lgtm_comment",
        body="No blocking findings.",
    )
    identity = AsyncMock(
        side_effect=[GhError("GitHub API timeout"), ACTOR]
    )
    async def publish_with_evidence(**kwargs):
        kwargs["evidence_sink"].update({
            "github_review_id": 733,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-733",
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": kwargs["publishing_started_at"],
        })
        return "approved", "lgtm_comment"

    publish = AsyncMock(side_effect=publish_with_evidence)
    with (
        patch.object(pr_review_service, "_github_publisher_login", identity),
        patch.object(pr_review_service, "_publish_review_action", publish),
    ):
        await check_and_update_review(
            db_session,
            review_id,
            repo_name,
            terminal_task_id=task_id,
            terminal_task_retry_count=retry_count,
        )
        await db_session.refresh(review)
        assert review.status == "reviewing"
        assert review.code_verdict == "pass"
        assert review.review_summary == "No blocking findings."
        assert review.code_verdict_task_id == task_id
        assert review.code_verdict_retry_count == retry_count
        assert review.code_verdict_task_started_at == task_started_at
        assert review.code_verdict_recorded_at is not None
        frozen_at = review.code_verdict_recorded_at
        assert review.publication_state == "reconciling"
        assert review.failure_stage == "github_identity"
        assert review.publication_error == "GitHub API timeout"
        publish.assert_not_awaited()

        await check_and_update_review(
            db_session,
            review_id,
            repo_name,
            terminal_task_id=task_id,
            terminal_task_retry_count=retry_count,
        )

    await db_session.refresh(review)
    assert review.status == "approved"
    assert review.code_verdict == "pass"
    assert review.code_verdict_recorded_at == frozen_at
    assert identity.await_count == 2
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_code_verdict_freezes_in_committed_generation_before_identity(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=8)
    body = "The exact head has no blocking findings."
    await _add_terminal_log(
        db_session,
        task,
        result="lgtm_comment",
        body=body,
        retry_count=8,
    )
    review_id = review.id
    task_id = task.id
    task_started_at = task.started_at
    repo_name = repo.repo_full_name

    async def identity_after_verdict_commit():
        # The GitHub boundary is reached only after a separate commit made the
        # immutable code result and complete generation provenance visible.
        await db_session.refresh(review)
        assert review.status == "reviewing"
        assert review.code_verdict == "pass"
        assert review.review_summary == body
        assert review.code_verdict_task_id == task_id
        assert review.code_verdict_retry_count == 8
        assert review.code_verdict_task_started_at == task_started_at
        assert review.code_verdict_recorded_at is not None
        return ACTOR

    identity = AsyncMock(side_effect=identity_after_verdict_commit)

    async def publish_with_evidence(**kwargs):
        kwargs["evidence_sink"].update({
            "github_review_id": 734,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-734",
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": kwargs["publishing_started_at"],
        })
        return "approved", "lgtm_comment"

    publish = AsyncMock(side_effect=publish_with_evidence)
    with (
        patch.object(pr_review_service, "_github_publisher_login", identity),
        patch.object(pr_review_service, "_publish_review_action", publish),
    ):
        await check_and_update_review(
            db_session,
            review_id,
            repo_name,
            terminal_task_id=task_id,
            terminal_task_retry_count=8,
        )

    identity.assert_awaited()
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_code_verdict_cas_rejects_conflicting_result_or_body(
    db_session,
    repo,
):
    review, task = await _make_review(db_session, repo, retry_count=9)
    review_id = review.id
    task_id = task.id
    task_started_at = task.started_at
    assert await pr_review_service._commit_exact_code_verdict(
        db_session,
        review_id=review_id,
        task_id=task_id,
        retry_count=9,
        task_started_at=task_started_at,
        code_verdict="pass",
        review_summary="Stable result.",
        background_handoff_pending=None,
        expected_background_generation=None,
    )
    await db_session.refresh(review)
    recorded_at = review.code_verdict_recorded_at

    assert await pr_review_service._commit_exact_code_verdict(
        db_session,
        review_id=review_id,
        task_id=task_id,
        retry_count=9,
        task_started_at=task_started_at,
        code_verdict="pass",
        review_summary="Stable result.",
        background_handoff_pending=None,
        expected_background_generation=None,
    )
    assert not await pr_review_service._commit_exact_code_verdict(
        db_session,
        review_id=review_id,
        task_id=task_id,
        retry_count=9,
        task_started_at=task_started_at,
        code_verdict="changes_required",
        review_summary="Stable result.",
        background_handoff_pending=None,
        expected_background_generation=None,
    )
    assert not await pr_review_service._commit_exact_code_verdict(
        db_session,
        review_id=review_id,
        task_id=task_id,
        retry_count=9,
        task_started_at=task_started_at,
        code_verdict="pass",
        review_summary="Changed result.",
        background_handoff_pending=None,
        expected_background_generation=None,
    )
    await db_session.refresh(review)
    assert review.code_verdict == "pass"
    assert review.review_summary == "Stable result."
    assert review.code_verdict_recorded_at == recorded_at


@pytest.mark.asyncio
async def test_terminal_intent_before_task_completion_keeps_verdict_without_github(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=10)
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=review.pr_number,
        current_base_sha=review.base_sha,
        current_head_sha=review.head_sha,
        current_review_id=review.id,
        status="reviewing",
        terminal_intent_status="closed",
        terminal_intent_base_ref=review.base_ref,
        terminal_intent_head_sha=review.head_sha,
        terminal_intent_delivery_id="closed-before-result",
        terminal_intent_observed_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    review.monitor_run_id = run.id
    body = "Review completed while the signed close was settling."
    await _add_terminal_log(
        db_session,
        task,
        result="lgtm_comment",
        body=body,
        retry_count=10,
    )
    review_id = review.id
    task_id = task.id
    task_started_at = task.started_at
    repo_name = repo.repo_full_name
    identity = AsyncMock(return_value=ACTOR)
    capability = AsyncMock()
    publish = AsyncMock()
    with (
        patch.object(pr_review_service, "_gh_authenticated_login", identity),
        patch.object(pr_review_service, "_freeze_safe_merge_method", capability),
        patch.object(pr_review_service, "_publish_review_action", publish),
    ):
        await check_and_update_review(
            db_session,
            review_id,
            repo_name,
            terminal_task_id=task_id,
            terminal_task_retry_count=10,
        )

    await db_session.refresh(review)
    assert review.status == "approved"
    assert review.code_verdict == "pass"
    assert review.review_summary == body
    assert review.code_verdict_task_id == task_id
    assert review.code_verdict_retry_count == 10
    assert review.code_verdict_task_started_at == task_started_at
    assert review.code_verdict_recorded_at is not None
    assert review.publication_state == "not_applicable"
    assert review.failure_stage == "lifecycle"
    assert review.pending_action is None
    identity.assert_not_awaited()
    capability.assert_not_awaited()
    publish.assert_not_awaited()
    assert no_broadcast.broadcast.await_count == 2


@pytest.mark.asyncio
async def test_check_auto_merge_rebase_only_arms_fast_forward_publication(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(
        db_session,
        repo,
        auto_merge=True,
    )
    await _add_terminal_log(
        db_session,
        task,
        result="approved_merged",
        body="No blocking findings.",
    )
    actor = AsyncMock(return_value=ACTOR)

    async def publish_after_direct_ref_claim(**kwargs):
        await db_session.refresh(review)
        assert review.status == "publishing"
        assert review.merge_method == "fast-forward"
        assert kwargs["merge_method"] == "fast-forward"
        kwargs["evidence_sink"].update({
            "github_review_id": 735,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-735",
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": kwargs["publishing_started_at"],
        })
        return "merged", "approved_merged"

    publish = AsyncMock(side_effect=publish_after_direct_ref_claim)
    repo_capability = AsyncMock(return_value=_direct_ref_repo_info(
        allow_merge_commit=False,
        allow_squash_merge=False,
        allow_rebase_merge=True,
    ))
    with (
        patch.object(
            pr_review_service,
            "_gh_api_json",
            repo_capability,
        ),
        patch.object(pr_review_service, "_gh_authenticated_login", actor),
        patch.object(pr_review_service, "_publish_review_action", publish),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            repo.repo_full_name,
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "merged"
    assert review.action_taken == "approved_merged"
    assert review.merge_method == "fast-forward"
    repo_capability.assert_awaited_once_with("repos/owner/repo")
    actor.assert_awaited_once()
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_auto_merge_retries_transient_capability_read(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, auto_merge=True)
    review_id = review.id
    task_id = task.id
    retry_count = task.retry_count
    repo_name = repo.repo_full_name
    await _add_terminal_log(
        db_session,
        task,
        result="approved_merged",
        body="No blocking findings.",
    )
    freeze = AsyncMock(side_effect=[
        GhError("GitHub API HTTP 503"),
        "fast-forward",
    ])

    async def publish_with_evidence(**kwargs):
        kwargs["evidence_sink"].update({
            "github_review_id": 736,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-736",
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": kwargs["publishing_started_at"],
        })
        return "merged", "approved_merged"

    publish = AsyncMock(side_effect=publish_with_evidence)
    with (
        patch.object(pr_review_service, "_freeze_safe_merge_method", freeze),
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(pr_review_service, "_publish_review_action", publish),
    ):
        await check_and_update_review(
            db_session,
            review_id,
            repo_name,
            terminal_task_id=task_id,
            terminal_task_retry_count=retry_count,
        )
        await db_session.refresh(review)
        assert review.status == "reviewing"
        assert review.action_taken is None
        publish.assert_not_awaited()

        await check_and_update_review(
            db_session,
            review_id,
            repo_name,
            terminal_task_id=task_id,
            terminal_task_retry_count=retry_count,
        )

    await db_session.refresh(review)
    assert review.status == "merged"
    assert review.merge_method == "fast-forward"
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_auto_merge_blocker_publishes_on_rebase_only_repository(
    db_session,
    repo,
    no_broadcast,
):
    """Merge capabilities cannot suppress a blocking review publication."""

    review, task = await _make_review(
        db_session,
        repo,
        auto_merge=True,
    )
    await _add_terminal_log(
        db_session,
        task,
        result="review_comments",
        body="A blocking correctness issue remains.",
    )
    freeze = AsyncMock(side_effect=AssertionError(
        "blocking reviews must not inspect auto-merge capability"
    ))
    publish = _published_action_mock(
        "commented",
        "review_comments",
        review_id=737,
    )
    with (
        patch.object(
            pr_review_service,
            "_freeze_safe_merge_method",
            freeze,
        ),
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            repo.repo_full_name,
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )

    await db_session.refresh(review)
    assert review.status == "commented"
    assert review.action_taken == "review_comments"
    assert review.merge_method is None
    freeze.assert_not_awaited()
    assert publish.await_args.kwargs["merge_method"] is None


@pytest.mark.asyncio
async def test_check_review_ignores_newer_output_from_old_retry(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=5)
    db_session.add_all([
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="message",
            role="assistant",
            content=_terminal_output("lgtm_comment", "Current."),
            timestamp=task.started_at + timedelta(seconds=1),
        ),
        LogEntry(
            task_id=task.id,
            task_retry_count=4,
            event_type="result",
            content=_terminal_output("review_comments", "Stale."),
            timestamp=task.started_at + timedelta(seconds=2),
        ),
    ])
    await db_session.commit()
    publish = _published_action_mock(
        "approved",
        "lgtm_comment",
        review_id=738,
    )
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=5,
        )
    assert publish.await_args.kwargs["review_body"] == "Current."


@pytest.mark.asyncio
async def test_check_review_unscoped_log_fails_without_github_write(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=6)
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=None,
        event_type="result",
        content=_terminal_output(),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=6,
        )
    await db_session.refresh(review)
    assert review.status == "error"
    assert "no terminal output" in review.review_summary
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_review_missing_retry_generation_fails_closed(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=None,
        )
    await db_session.refresh(review)
    # Without an exact generation even the terminal error CAS is rejected.
    assert review.status == "reviewing"
    publish.assert_not_awaited()
    no_broadcast.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_review_missing_nonce_fails_without_publish(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, nonce="bad")
    await _add_terminal_log(db_session, task)
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "error"
    assert "one-time action nonce" in review.review_summary
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("auto_merge", "terminal_result"),
    [(False, "approved_merged"), (True, "lgtm_comment")],
)
async def test_check_review_enforces_frozen_action_policy(
    db_session,
    repo,
    no_broadcast,
    auto_merge,
    terminal_result,
):
    review, task = await _make_review(
        db_session,
        repo,
        auto_merge=auto_merge,
    )
    await _add_terminal_log(
        db_session,
        task,
        result=terminal_result,
    )
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "error"
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_delivery_durable_publication_never_resumes_approved_merged(
    db_session,
    repo,
    no_broadcast,
):
    """A corrupted/replayed outbox cannot broaden a Delivery policy."""

    project = Project(
        name="delivery-publication-policy",
        local_path="/srv/repos/delivery-publication-policy",
        git_url="git@github.com:owner/repo.git",
        has_remote=True,
        default_branch="main",
        status="ready",
    )
    db_session.add(project)
    await db_session.flush()
    policy = {
        "schema_version": 1,
        "terminal": "ready_to_merge",
        "auto_merge": False,
        "pr_monitor": {
            "repo_id": repo.id,
            "repo_full_name": repo.repo_full_name,
            "review_mode": "panel",
            "wait_for_ci": True,
            "required_checks": [{
                "kind": "check_run",
                "name": "tests",
                "app_slug": "github-actions",
            }],
        },
    }
    run = DeliveryRun(
        admission_scope="system",
        idempotency_key="service-pr-review-publication",
        request_hash="f" * 64,
        project_id=project.id,
        monitored_repo_id=repo.id,
        title="Delivery publication fence",
        requirements="Never merge automatically.",
        requirements_hash="1" * 64,
        policy_snapshot=policy,
        policy_hash=value_hash(policy),
        base_branch="main",
        delivery_branch="ccm/delivery/publication-fence",
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        pr_number=PR_DATA["number"],
        phase="monitoring",
        activity="waiting",
        wait_reason="pr_monitor",
    )
    db_session.add(run)
    await db_session.flush()
    review, task = await _make_review(
        db_session,
        repo,
        auto_merge=True,
    )
    task.metadata_ = {
        **task.metadata_,
        "pr_wait_for_ci": True,
        "pr_required_checks": [{
            "kind": "check_run",
            "name": "tests",
            "app_slug": "github-actions",
        }],
    }
    review.delivery_id = f"delivery:{run.id}:{PR_DATA['head_sha']}"
    await _bind_delivery_review(
        db_session,
        delivery_run=run,
        review=review,
    )
    await _arm_publishing(
        db_session,
        review,
        task,
        action="approved_merged",
    )
    lease_token = "1" * 48
    review.publishing_lease_token = lease_token
    review.publishing_lease_expires_at = datetime.utcnow() + timedelta(minutes=5)
    await db_session.commit()

    publish = AsyncMock()
    identity = AsyncMock(return_value=ACTOR)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            identity,
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        await pr_review_service._resume_publishing_review_under_lease(
            db_session,
            review.id,
            repo.repo_full_name,
            lease_token=lease_token,
        )

    await db_session.refresh(review)
    assert review.status == "error"
    assert review.action_taken == "error"
    assert review.review_summary == "Looks good."
    assert review.publication_state == "failed"
    assert review.failure_stage == "publication"
    assert "Durable PR publication state is invalid" in review.publication_error
    identity.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_delivery_durable_publication_resumes_frozen_auto_merge(
    db_session,
    repo,
    no_broadcast,
):
    """A frozen Delivery auto-merge policy authorizes exact recovery."""

    project = Project(
        name="delivery-auto-merge-publication",
        local_path="/srv/repos/delivery-auto-merge-publication",
        git_url="git@github.com:owner/repo.git",
        has_remote=True,
        default_branch="main",
        status="ready",
    )
    db_session.add(project)
    await db_session.flush()
    policy = {
        "schema_version": 1,
        "terminal": "merged",
        "auto_merge": True,
        "pr_monitor": {
            "repo_id": repo.id,
            "repo_full_name": repo.repo_full_name,
            "review_mode": "panel",
            "wait_for_ci": True,
            "required_checks": [{
                "kind": "check_run",
                "name": "tests",
                "app_slug": "github-actions",
            }],
        },
    }
    run = DeliveryRun(
        admission_scope="system",
        idempotency_key="service-pr-review-auto-merge-publication",
        request_hash="e" * 64,
        project_id=project.id,
        monitored_repo_id=repo.id,
        title="Delivery auto-merge publication recovery",
        requirements="Merge and publish the durable outcome.",
        requirements_hash="2" * 64,
        policy_snapshot=policy,
        policy_hash=value_hash(policy),
        base_branch="main",
        delivery_branch="ccm/delivery/auto-merge-publication",
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        pr_number=PR_DATA["number"],
        phase="monitoring",
        activity="waiting",
        wait_reason="pr_monitor",
    )
    db_session.add(run)
    await db_session.flush()
    review, task = await _make_review(
        db_session,
        repo,
        auto_merge=True,
    )
    task.metadata_ = {
        **task.metadata_,
        "pr_wait_for_ci": True,
        "pr_required_checks": [{
            "kind": "check_run",
            "name": "tests",
            "app_slug": "github-actions",
        }],
    }
    review.delivery_id = f"delivery:{run.id}:{PR_DATA['head_sha']}"
    await _bind_delivery_review(
        db_session,
        delivery_run=run,
        review=review,
    )
    await _arm_publishing(
        db_session,
        review,
        task,
        action="approved_merged",
    )
    lease_token = "2" * 48
    review.publishing_lease_token = lease_token
    review.publishing_lease_expires_at = datetime.utcnow() + timedelta(minutes=5)
    await db_session.commit()

    publish = _published_action_mock(
        "merged",
        "approved_merged",
        review_id=739,
    )
    identity = AsyncMock(return_value=ACTOR)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            identity,
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        await pr_review_service._resume_publishing_review_under_lease(
            db_session,
            review.id,
            repo.repo_full_name,
            lease_token=lease_token,
        )

    await db_session.refresh(review)
    assert review.status == "merged"
    assert review.action_taken == "approved_merged"
    assert review.completed_at is not None
    assert review.pending_action is None
    assert review.publishing_lease_token is None
    identity.assert_not_awaited()
    publish.assert_awaited_once()
    assert publish.await_args.kwargs["result"] == "approved_merged"
    assert publish.await_args.kwargs["auto_merge"] is True


@pytest.mark.asyncio
async def test_check_review_discards_non_owner_task(
    db_session,
    repo,
    no_broadcast,
):
    review, _task = await _make_review(db_session, repo)
    other = Task(
        title="other",
        status="completed",
        retry_count=1,
        started_at=datetime.utcnow(),
    )
    db_session.add(other)
    await db_session.commit()
    await db_session.refresh(other)
    await _add_terminal_log(db_session, other, retry_count=1)
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=other.id,
            terminal_task_retry_count=1,
        )
    await db_session.refresh(review)
    assert review.status == "reviewing"
    publish.assert_not_awaited()
    no_broadcast.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_review_unconfirmed_publish_error_stays_publishing(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    await _add_terminal_log(db_session, task)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            AsyncMock(side_effect=GhError("network timeout")),
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "publishing"
    assert review.action_taken is None
    assert review.pending_action == "lgtm_comment"
    assert review.action_nonce == ACTION_NONCE
    assert review.review_summary == "Looks good."
    assert review.publication_state == "reconciling"
    assert review.failure_stage == "recovery"
    assert "nonce reconciliation" in review.publication_error


@pytest.mark.asyncio
async def test_check_review_terminal_snapshot_error_finishes_error(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    await _add_terminal_log(db_session, task)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            AsyncMock(side_effect=GhError(
                "GitHub PR snapshot changed before the backend action"
            )),
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "error"
    assert review.action_taken == "error"
    assert review.review_summary == "Looks good."
    assert review.publication_state == "not_applicable"
    assert review.failure_stage == "lifecycle"
    assert "snapshot changed" in review.publication_error


@pytest.mark.asyncio
async def test_check_review_actor_rotation_without_evidence_stays_recoverable(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    await _add_terminal_log(db_session, task)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(side_effect=[ACTOR, "replacement-bot"]),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            AsyncMock(
                side_effect=GhError(
                    "GitHub publishing identity changed before durable "
                    "review evidence was found"
                )
            ),
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )

    await db_session.refresh(review)
    assert review.status == "publishing"
    assert review.action_taken is None
    assert review.pending_action == "lgtm_comment"
    assert review.review_summary == "Looks good."
    assert review.publication_state == "reconciling"
    assert review.failure_stage == "github_identity"
    assert "identity changed" in review.publication_error


@pytest.mark.asyncio
async def test_check_review_cannot_commit_after_background_handoff_arms(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    await _add_terminal_log(db_session, task)
    task_id = task.id
    task_retry_count = task.retry_count
    pending = False

    async def publish_then_arm(**kwargs):
        nonlocal pending
        pending = True
        kwargs["evidence_sink"].update({
            "github_review_id": 742,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-742",
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": kwargs["publishing_started_at"],
        })
        await db_session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(pty_background_generation="bg-new")
        )
        await db_session.commit()
        return "approved", "lgtm_comment"

    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            side_effect=publish_then_arm,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task_id,
            terminal_task_retry_count=task_retry_count,
            background_handoff_pending=lambda: pending,
        )
    await db_session.refresh(review)
    assert review.status == "publishing"
    assert review.pending_action == "lgtm_comment"
    assert review.completed_at is None
    assert no_broadcast.broadcast.await_count == 2


@pytest.mark.asyncio
async def test_recover_publishing_pr_reviews_reuses_nonce_without_write(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        review_id = review.id

    api = AsyncMock()
    gh_view = AsyncMock(return_value=_snapshot())
    async def find_existing_review(**kwargs):
        kwargs["evidence_sink"].update({
            "github_review_id": 743,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-743",
            "github_review_state": "APPROVED",
            "published_actor": ACTOR,
            "published_at": PUBLISHING_STARTED_AT,
        })
        return "APPROVED"

    find_review = AsyncMock(side_effect=find_existing_review)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(),
        ),
    ):
        recovered = await pr_review_service.recover_publishing_pr_reviews(
            session_factory
        )

    assert recovered == 1
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "approved"
        assert stored.action_taken == "lgtm_comment"
        assert stored.review_summary == "Looks good."
        assert "PR_REVIEW_BODY_BEGIN" not in stored.review_summary
        assert "PR_REVIEW_BODY_END" not in stored.review_summary
        assert "PR_REVIEW_RESULT:" not in stored.review_summary
        assert stored.pending_action is None
        assert stored.pending_review_body is None
        assert stored.publishing_actor is None
        assert stored.publishing_retry_count is None
        assert stored.publishing_task_started_at is None
        assert stored.publishing_started_at is None
        assert stored.publishing_lease_token is None
        assert stored.publishing_lease_expires_at is None
    find_review.assert_awaited_once()
    api.assert_not_awaited()
    gh_view.assert_awaited_once()


@pytest.mark.asyncio
async def test_recovery_uses_frozen_review_base_ref_after_repo_config_drift(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo(default_branch="main")
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        repo.default_branch = "release/next"
        await db.commit()

    publish = _published_action_mock(
        "approved",
        "lgtm_comment",
        review_id=740,
    )
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(pr_review_service, "_publish_review_action", publish),
    ):
        assert await pr_review_service.recover_publishing_pr_reviews(
            session_factory
        ) == 1

    publish.assert_awaited_once()
    assert publish.await_args.kwargs["base_ref"] == "main"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing_evidence", "finalized"),
    [
        ({"github_review_id": 745}, True),
        ({
            "github_review_id": 745,
            "github_review_url": (
                "https://github.com/owner/repo/pull/7#pullrequestreview-745"
            ),
            "github_review_state": "COMMENTED",
            "published_actor": ACTOR,
            "published_at": PUBLISHING_STARTED_AT,
        }, True),
        ({"github_review_id": 999}, False),
        ({
            "github_review_url": (
                "https://github.com/owner/repo/pull/7#pullrequestreview-999"
            ),
        }, False),
        ({"github_review_state": "APPROVED"}, False),
        ({"published_actor": "other-bot"}, False),
        ({"published_at": datetime(2026, 7, 30, 23, 59, 0)}, False),
    ],
    ids=(
        "same-partial",
        "same-complete",
        "conflicting-id",
        "conflicting-url",
        "conflicting-state",
        "conflicting-actor",
        "conflicting-time",
    ),
)
async def test_terminal_publication_cas_preserves_immutable_evidence(
    session_factory,
    no_broadcast,
    existing_evidence,
    finalized,
):
    """Same evidence may fill NULLs; a conflicting partial tuple never changes."""

    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        for field, value in existing_evidence.items():
            setattr(review, field, value)
        await db.commit()
        review_id = review.id

    async with session_factory() as db:
        lease_token = await pr_review_service._acquire_publication_lease(
            db,
            review_id,
        )
    assert lease_token is not None

    publish = _published_action_mock(
        "approved",
        "lgtm_comment",
        review_id=745,
    )
    async with session_factory() as db:
        with patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ):
            await pr_review_service._resume_publishing_review_under_lease(
                db,
                review_id,
                repo.repo_full_name,
                lease_token=lease_token,
            )

    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        if finalized:
            assert stored.status == "approved"
            assert stored.publication_state == "published"
            assert stored.github_review_id == 745
            assert stored.github_review_url == (
                "https://github.com/owner/repo/pull/7#pullrequestreview-745"
            )
            assert stored.github_review_state == "COMMENTED"
            assert stored.published_actor == ACTOR
            assert stored.published_at == PUBLISHING_STARTED_AT
        else:
            assert stored.status == "publishing"
            for field in pr_review_service._PUBLICATION_EVIDENCE_FIELDS:
                assert getattr(stored, field) == existing_evidence.get(field)


@pytest.mark.asyncio
async def test_recover_migration_window_merge_outbox_after_lost_merge_ack(
    session_factory,
    no_broadcast,
):
    """An old binary's post-migration NULL outbox is frozen and reconciled."""

    async with session_factory() as db:
        repo = _make_repo(auto_merge=True)
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(
            db,
            repo,
            auto_merge=True,
        )
        await _arm_publishing(
            db,
            review,
            task,
            action="approved_merged",
        )
        # Simulate the online migration window: the migration backfill already
        # ran, then an old binary inserted its otherwise complete implicit
        # merge-commit outbox and left the new column NULL.
        review.merge_method = None
        await db.commit()
        review_id = review.id

    merge_sha = "9" * 40

    async def github_api(path, *, method="GET", payload=None, **_kwargs):
        if path == f"repos/owner/repo/commits/{merge_sha}":
            assert method == "GET"
            assert payload is None
            return {
                "sha": merge_sha,
                "commit": {
                    "message": (
                        "Automated review\n\n"
                        f"CCM review nonce: {ACTION_NONCE}"
                    ),
                },
                "parents": [
                    {"sha": PR_DATA["base_sha"]},
                    {"sha": PR_DATA["head_sha"]},
                ],
            }
        if path == "repos/owner/repo/issues/7/comments":
            assert method == "POST"
            assert payload == {
                "body": pr_review_service._merged_comment_body(
                    nonce=ACTION_NONCE,
                    head_sha=PR_DATA["head_sha"],
                ),
            }
            return _merged_issue_comment_response()
        raise AssertionError(f"unexpected GitHub mutation/read: {path}")

    api = AsyncMock(side_effect=github_api)
    gh_view = AsyncMock(return_value=_snapshot(
        state="MERGED",
        merged_at="2026-07-31T00:00:01Z",
        merged_by=ACTOR,
        merge_commit_sha=merge_sha,
    ))
    async def find_existing_review(**kwargs):
        kwargs["evidence_sink"].update({
            "github_review_id": 744,
            "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-744",
            "github_review_state": "APPROVED",
            "published_actor": ACTOR,
            "published_at": PUBLISHING_STARTED_AT,
        })
        return "APPROVED"

    find_review = AsyncMock(side_effect=find_existing_review)
    find_comment = AsyncMock(return_value=None)
    find_merge = AsyncMock(wraps=pr_review_service._find_merge_evidence)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
        patch.object(
            pr_review_service,
            "_find_merged_comment_evidence",
            find_comment,
        ),
        patch.object(
            pr_review_service,
            "_require_safe_base_chain",
            AsyncMock(),
        ),
    ):
        recovered = await pr_review_service.recover_publishing_pr_reviews(
            session_factory
        )

    assert recovered == 1
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "merged"
        assert stored.action_taken == "approved_merged"
        assert stored.pending_action is None
        assert stored.merge_method == "merge"
        assert stored.publishing_actor == ACTOR
        assert stored.publishing_started_at == PUBLISHING_STARTED_AT

    find_review_kwargs = dict(find_review.await_args.kwargs)
    assert find_review_kwargs.pop("evidence_sink") == {
        "github_review_id": 744,
        "github_review_url": "https://github.com/owner/repo/pull/7#pullrequestreview-744",
        "github_review_state": "APPROVED",
        "published_actor": ACTOR,
        "published_at": PUBLISHING_STARTED_AT,
    }
    assert find_review_kwargs == {
        "repo_name": "owner/repo",
        "pr_number": PR_DATA["number"],
        "head_sha": PR_DATA["head_sha"],
        "result": "approved_merged",
        "nonce": ACTION_NONCE,
        "actor": ACTOR,
        "publishing_started_at": PUBLISHING_STARTED_AT,
    }
    assert find_merge.await_count == 2
    for call in find_merge.await_args_list:
            assert call.kwargs == {
                "repo_name": "owner/repo",
                "pr_number": PR_DATA["number"],
                "base_ref": "main",
                "base_sha": PR_DATA["base_sha"],
            "head_sha": PR_DATA["head_sha"],
            "nonce": ACTION_NONCE,
            "actor": ACTOR,
            "publishing_started_at": PUBLISHING_STARTED_AT,
            "merge_method": "merge",
        }
    assert gh_view.await_count == 2
    assert find_comment.await_count == 1
    assert not any("pulls/7/merge" in call.args[0] for call in api.await_args_list)


@pytest.mark.asyncio
async def test_recover_publishing_pr_reviews_counts_only_terminal_rows(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        review_id = review.id

    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            AsyncMock(side_effect=GhError("temporary network failure")),
        ),
    ):
        recovered = await pr_review_service.recover_publishing_pr_reviews(
            session_factory
        )

    assert recovered == 0
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "publishing"
        assert stored.review_summary == "Looks good."
        assert stored.publication_state == "reconciling"
        assert stored.failure_stage == "recovery"
        assert "nonce reconciliation" in stored.publication_error


@pytest.mark.asyncio
async def test_publication_lease_fences_concurrent_processes(
    session_factory,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at

    async with session_factory() as first:
        first_token = await pr_review_service._acquire_publication_lease(
            first,
            review_id,
        )
    assert first_token is not None

    async with session_factory() as second:
        assert (
            await pr_review_service._acquire_publication_lease(
                second,
                review_id,
            )
            is None
        )

    async with session_factory() as db:
        await db.execute(
            update(PRReview)
            .where(PRReview.id == review_id)
            .values(
                publishing_lease_expires_at=(
                    datetime.utcnow() - timedelta(seconds=1)
                )
            )
        )
        await db.commit()

    async with session_factory() as second:
        second_token = await pr_review_service._acquire_publication_lease(
            second,
            review_id,
        )
        assert second_token is not None
        assert second_token != first_token
        assert not await pr_review_service._publication_is_current(
            second,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
                nonce=ACTION_NONCE,
                lease_token=first_token,
                expected_delivery_id=PR_DATA["delivery_id"],
                base_ref="main",
        )


@pytest.mark.asyncio
async def test_publication_guard_rejects_delivery_ownership_change(
    session_factory,
):
    """A legacy publication cannot mutate GitHub after Delivery adoption."""

    async with session_factory() as db:
        repo = _make_repo(auto_merge=True)
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo, auto_merge=True)
        await _arm_publishing(
            db,
            review,
            task,
            action="approved_merged",
        )
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at
        legacy_delivery_id = review.delivery_id

    async with session_factory() as db:
        lease_token = await pr_review_service._acquire_publication_lease(
            db,
            review_id,
        )
    assert lease_token is not None

    async with session_factory() as db:
        await db.execute(
            update(PRReview)
            .where(PRReview.id == review_id)
            .values(delivery_id=f"delivery:91:{PR_DATA['head_sha']}")
        )
        await db.commit()

        assert not await pr_review_service._publication_is_current(
            db,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
                nonce=ACTION_NONCE,
                lease_token=lease_token,
                expected_delivery_id=legacy_delivery_id,
                base_ref="main",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "auto_merge", "merge_method"),
    [
        ("lgtm_comment", False, None),
        ("review_comments", False, None),
        ("approved_merged", True, "merge"),
    ],
)
async def test_publication_guard_accepts_current_reviewing_run_effect_paths(
    session_factory,
    action,
    auto_merge,
    merge_method,
):
    """COMMENT, Finding, and merge effects share the exact reviewing fence."""

    async with session_factory() as db:
        repo = _make_repo(auto_merge=auto_merge)
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo, auto_merge=auto_merge)
        await _bind_current_reviewing_run(db, review)
        await _arm_publishing(db, review, task, action=action)
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at
        expected_delivery_id = review.delivery_id

    async with session_factory() as db:
        lease_token = await pr_review_service._acquire_publication_lease(
            db,
            review_id,
        )
    assert lease_token is not None

    async with session_factory() as db:
        assert await pr_review_service._publication_is_current(
            db,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
            nonce=ACTION_NONCE,
            lease_token=lease_token,
            expected_delivery_id=expected_delivery_id,
            base_ref="main",
            merge_method=merge_method,
        )


@pytest.mark.asyncio
async def test_publication_guard_preserves_legacy_review_without_run(
    session_factory,
):
    """Pre-Run durable outboxes retain their existing safe recovery path."""

    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at
        expected_delivery_id = review.delivery_id

    async with session_factory() as db:
        lease_token = await pr_review_service._acquire_publication_lease(
            db,
            review_id,
        )
    assert lease_token is not None

    async with session_factory() as db:
        assert await pr_review_service._publication_is_current(
            db,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
            nonce=ACTION_NONCE,
            lease_token=lease_token,
            expected_delivery_id=expected_delivery_id,
            base_ref="main",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["closed", "merged"])
async def test_publication_guard_rejects_committed_terminal_intent(
    session_factory,
    terminal_status,
):
    """A signed terminal intent revokes an already-armed merge publisher."""

    async with session_factory() as db:
        repo = _make_repo(auto_merge=True)
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo, auto_merge=True)
        run = await _bind_current_reviewing_run(db, review)
        await _arm_publishing(
            db,
            review,
            task,
            action="approved_merged",
        )
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at
        run_id = run.id
        expected_delivery_id = review.delivery_id

    async with session_factory() as db:
        lease_token = await pr_review_service._acquire_publication_lease(
            db,
            review_id,
        )
    assert lease_token is not None

    async with session_factory() as db:
        assert await pr_review_service._publication_is_current(
            db,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
            nonce=ACTION_NONCE,
            lease_token=lease_token,
            expected_delivery_id=expected_delivery_id,
            base_ref="main",
            merge_method="merge",
        )

    # This is the webhook commit boundary.  Publication remains armed while
    # terminal recovery drains it, but no later GitHub mutation is authorized.
    async with session_factory() as db:
        run = await db.get(PRMonitorRun, run_id)
        run.terminal_intent_status = terminal_status
        run.terminal_intent_base_ref = "main"
        run.terminal_intent_head_sha = PR_DATA["head_sha"]
        run.terminal_intent_delivery_id = f"terminal-{terminal_status}"
        run.terminal_intent_observed_at = datetime.utcnow()
        await db.commit()

    async with session_factory() as db:
        assert not await pr_review_service._publication_is_current(
            db,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
            nonce=ACTION_NONCE,
            lease_token=lease_token,
            expected_delivery_id=expected_delivery_id,
            base_ref="main",
            merge_method="merge",
        )


@pytest.mark.asyncio
async def test_publication_guard_rejects_partial_terminal_intent(
    session_factory,
):
    """An interrupted webhook intent write fails closed even without status."""

    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        run = await _bind_current_reviewing_run(db, review)
        await _arm_publishing(db, review, task)
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at
        run_id = run.id
        expected_delivery_id = review.delivery_id

    async with session_factory() as db:
        lease_token = await pr_review_service._acquire_publication_lease(
            db,
            review_id,
        )
    assert lease_token is not None

    async with session_factory() as db:
        run = await db.get(PRMonitorRun, run_id)
        run.terminal_intent_head_sha = PR_DATA["head_sha"]
        await db.commit()

    async with session_factory() as db:
        assert not await pr_review_service._publication_is_current(
            db,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
            nonce=ACTION_NONCE,
            lease_token=lease_token,
            expected_delivery_id=expected_delivery_id,
            base_ref="main",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("current_review_id", None),
        ("current_base_sha", "8" * 40),
        ("current_head_sha", "9" * 40),
        ("status", "paused"),
    ],
)
async def test_publication_guard_rejects_monitor_binding_mismatch(
    session_factory,
    field,
    value,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        run = await _bind_current_reviewing_run(db, review)
        await _arm_publishing(db, review, task)
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at
        run_id = run.id
        expected_delivery_id = review.delivery_id

    async with session_factory() as db:
        lease_token = await pr_review_service._acquire_publication_lease(
            db,
            review_id,
        )
    assert lease_token is not None

    async with session_factory() as db:
        run = await db.get(PRMonitorRun, run_id)
        setattr(run, field, value)
        await db.commit()

    async with session_factory() as db:
        assert not await pr_review_service._publication_is_current(
            db,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
            nonce=ACTION_NONCE,
            lease_token=lease_token,
            expected_delivery_id=expected_delivery_id,
            base_ref="main",
        )


@pytest.mark.asyncio
async def test_publication_guard_rejects_active_manager_termination_receipt(
    session_factory,
):
    """A pending remote stop revokes publication authority before GitHub."""

    lease_token = "9" * 48
    async with session_factory() as db:
        repo = _make_repo()
        worker = Worker(name="publication-receipt-worker", status="ready")
        db.add_all((repo, worker))
        await db.flush()
        review, task = await _make_review(db, repo)
        task.worker_id = worker.id
        await _arm_publishing(db, review, task)
        review.publishing_lease_token = lease_token
        review.publishing_lease_expires_at = (
            datetime.utcnow() + timedelta(minutes=5)
        )
        await db.commit()
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at

    await persist_active_manager_receipt(session_factory, task_id)

    async with session_factory() as db:
        assert not await pr_review_service._publication_is_current(
            db,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
                nonce=ACTION_NONCE,
                lease_token=lease_token,
                expected_delivery_id=PR_DATA["delivery_id"],
                base_ref="main",
        )

    authenticated_login = AsyncMock(return_value=ACTOR)
    publish = AsyncMock()
    gh_api = AsyncMock()
    gh_view = AsyncMock()
    find_evidence = AsyncMock()
    async with session_factory() as db:
        with (
            patch.object(
                pr_review_service,
                "_gh_authenticated_login",
                authenticated_login,
            ),
            patch.object(
                pr_review_service,
                "_publish_review_action",
                publish,
            ),
            patch.object(pr_review_service, "_gh_api_json", gh_api),
            patch.object(pr_review_service, "_gh_pr_view", gh_view),
            patch.object(
                pr_review_service,
                "_find_review_evidence",
                find_evidence,
            ),
        ):
            await pr_review_service._resume_publishing_review_under_lease(
                db,
                review_id,
                "owner/repo",
                lease_token=lease_token,
            )

    authenticated_login.assert_not_awaited()
    publish.assert_not_awaited()
    gh_api.assert_not_awaited()
    gh_view.assert_not_awaited()
    find_evidence.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "publishing"
        assert stored.pending_action == "lgtm_comment"


@pytest.mark.asyncio
async def test_recover_incomplete_review_claims_exact_completed_generation(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _add_terminal_log(db, task)
        review_id = review.id

    publish = _published_action_mock(
        "approved",
        "lgtm_comment",
        review_id=741,
    )
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 1
    publish.assert_awaited_once()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "approved"


@pytest.mark.asyncio
async def test_full_recovery_projects_terminal_error_to_monitor_once(
    session_factory,
    no_broadcast,
):
    """The periodic entry point closes and then leaves the error gap closed."""

    async with session_factory() as db:
        repo = _make_repo(review_mode="panel")
        db.add(repo)
        await db.flush()
        monitor = PRMonitorRun(
            repo_id=repo.id,
            pr_number=PR_DATA["number"],
            current_base_sha=PR_DATA["base_sha"],
            current_head_sha=PR_DATA["head_sha"],
            status="reviewing",
        )
        db.add(monitor)
        await db.flush()
        review = PRReview(
            monitor_run_id=monitor.id,
            repo_id=repo.id,
            pr_number=PR_DATA["number"],
            base_ref="main",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
            delivery_id=PR_DATA["delivery_id"],
            pr_title=PR_DATA["title"],
            pr_author=PR_DATA["author"],
            pr_url=PR_DATA["url"],
            status="error",
            action_taken="error",
            review_summary="review transport failed permanently",
            completed_at=datetime.utcnow(),
        )
        db.add(review)
        await db.flush()
        monitor.current_review_id = review.id
        await db.commit()
        monitor_id = monitor.id
        review_id = review.id

    assert (
        await pr_review_service.recover_incomplete_pr_reviews(session_factory)
        == 1
    )
    async with session_factory() as db:
        recovered_monitor = await db.get(PRMonitorRun, monitor_id)
        recovered_review = await db.get(PRReview, review_id)
        assert recovered_review.status == "error"
        assert recovered_review.action_taken == "error"
        assert recovered_monitor.status == "paused"
        assert recovered_monitor.pause_reason == (
            f"review_error:{review_id}:review transport failed permanently"
        )
        recovered_version = recovered_monitor.state_version

    assert (
        await pr_review_service.recover_incomplete_pr_reviews(session_factory)
        == 0
    )
    async with session_factory() as db:
        unchanged_monitor = await db.get(PRMonitorRun, monitor_id)
        assert unchanged_monitor.status == "paused"
        assert unchanged_monitor.state_version == recovered_version


@pytest.mark.asyncio
async def test_periodic_pr_recovery_invokes_finding_action_recovery(
    session_factory,
    no_broadcast,
    monkeypatch,
):
    import backend.main as main_module
    from backend.services import pr_review_fix

    relay = object()
    recover_finding_actions = AsyncMock(return_value=3)
    monkeypatch.setattr(main_module, "worker_relay", relay)
    monkeypatch.setattr(
        pr_review_fix,
        "recover_incomplete_finding_actions",
        recover_finding_actions,
        raising=False,
    )

    recovered = await pr_review_service.recover_incomplete_pr_reviews(
        session_factory
    )

    assert recovered == 3
    recover_finding_actions.assert_awaited_once_with(
        session_factory,
        worker_relay=relay,
    )


@pytest.mark.asyncio
async def test_periodic_pr_recovery_reconciles_cancelled_panel_reviewers(
    session_factory,
    no_broadcast,
    monkeypatch,
):
    from backend.services import pr_review_panel

    reconcile_cancelled = AsyncMock(return_value=2)
    monkeypatch.setattr(
        pr_review_panel,
        "reconcile_cancelled_reviewer_tasks",
        reconcile_cancelled,
    )

    recovered = await pr_review_service.recover_incomplete_pr_reviews(
        session_factory
    )

    assert recovered == 2
    reconcile_cancelled.assert_awaited_once_with(session_factory)


@pytest.mark.asyncio
async def test_recover_incomplete_worker_review_defers_missing_history(
    session_factory,
    no_broadcast,
    caplog,
):
    database_now = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)
    async with session_factory() as db:
        repo = _make_repo()
        worker = Worker(name="recent-review-history", status="ready")
        db.add_all((repo, worker))
        await db.flush()
        review, task = await _make_review(db, repo)
        task.worker_id = worker.id
        task.started_at = database_now.replace(tzinfo=None) - timedelta(
            seconds=4
        )
        task.completed_at = database_now.replace(tzinfo=None) - timedelta(
            seconds=2
        )
        await db.commit()
        review_id = review.id

    publish = AsyncMock()
    with (
        caplog.at_level(logging.INFO),
        patch.object(
            pr_review_service,
            "_database_now",
            AsyncMock(return_value=database_now),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 0
    publish.assert_not_awaited()
    assert "Recovered 0 of 1 incomplete PR review action(s)" not in caplog.text
    no_broadcast.broadcast.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "reviewing"
        assert stored.completed_at is None


@pytest.mark.asyncio
async def test_recover_worker_review_defers_early_assistant_chatter(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        review_id = review.id
        task.worker_id = 44
        db.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            event_type="message",
            role="assistant",
            content="I am still reviewing the patch.",
            timestamp=task.started_at + timedelta(seconds=1),
        ))
        await db.commit()

    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 0
    publish.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "reviewing"


@pytest.mark.asyncio
async def test_expired_legacy_worker_review_is_quarantined_once_without_github(
    session_factory,
    no_broadcast,
    caplog,
):
    """Unscoped legacy output is preserved but never replayed as authority."""

    database_now = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)
    completed_at = database_now.replace(tzinfo=None) - timedelta(minutes=6)
    started_at = completed_at - timedelta(seconds=2)
    legacy_content = _terminal_output(
        "lgtm_comment",
        "This legacy result must not be published.",
    )
    async with session_factory() as db:
        repo = _make_repo()
        worker = Worker(name="expired-review-history", status="ready")
        db.add_all((repo, worker))
        await db.flush()
        review, task = await _make_review(db, repo, retry_count=7)
        task.worker_id = worker.id
        task.started_at = started_at
        task.completed_at = completed_at
        monitor = PRMonitorRun(
            repo_id=repo.id,
            pr_number=review.pr_number,
            current_base_sha=review.base_sha,
            current_head_sha=review.head_sha,
            status="reviewing",
        )
        db.add(monitor)
        await db.flush()
        review.monitor_run_id = monitor.id
        monitor.current_review_id = review.id
        legacy_log = LogEntry(
            task_id=task.id,
            task_retry_count=None,
            event_type="result",
            content=legacy_content,
            timestamp=started_at + timedelta(seconds=1),
        )
        db.add(legacy_log)
        await db.commit()
        review_id = review.id
        task_id = task.id
        monitor_id = monitor.id
        legacy_log_id = legacy_log.id
        task_incarnation = task.incarnation_id
        turn_generation = task.turn_generation

    database_clock = AsyncMock(return_value=database_now)
    authenticated_login = AsyncMock()
    gh_api = AsyncMock()
    gh_view = AsyncMock()
    find_evidence = AsyncMock()
    publish = AsyncMock()
    with (
        caplog.at_level(logging.INFO),
        patch.object(
            pr_review_service,
            "_database_now",
            database_clock,
        ),
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            authenticated_login,
        ),
        patch.object(pr_review_service, "_gh_api_json", gh_api),
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_evidence,
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        first = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )
        second = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert first == 1
    assert second == 0
    assert database_clock.await_count >= 1
    authenticated_login.assert_not_awaited()
    gh_api.assert_not_awaited()
    gh_view.assert_not_awaited()
    find_evidence.assert_not_awaited()
    publish.assert_not_awaited()
    assert "Recovered 0 of 1 incomplete PR review action(s)" not in caplog.text
    no_broadcast.broadcast.assert_awaited_once()

    async with session_factory() as db:
        stored_review = await db.get(PRReview, review_id)
        stored_task = await db.get(Task, task_id)
        stored_monitor = await db.get(PRMonitorRun, monitor_id)
        stored_log = await db.get(LogEntry, legacy_log_id)
        assert stored_review.status == "error"
        assert stored_review.action_taken == "error"
        assert "infrastructure quarantine" in stored_review.review_summary
        assert "no GitHub action was attempted" in stored_review.review_summary
        assert stored_review.completed_at == database_now.replace(tzinfo=None)
        assert stored_monitor.status == "paused"
        assert stored_monitor.state_version == 2
        assert stored_monitor.pause_reason.startswith(
            f"review_error:{review_id}:PR review infrastructure quarantine"
        )
        assert stored_task.status == "completed"
        assert stored_task.retry_count == 7
        assert stored_task.incarnation_id == task_incarnation
        assert stored_task.turn_generation == turn_generation
        assert stored_task.started_at == started_at
        assert stored_task.completed_at == completed_at
        assert stored_log.task_retry_count is None
        assert stored_log.content == legacy_content


@pytest.mark.asyncio
async def test_expired_missing_history_yields_to_new_task_generation(
    session_factory,
    no_broadcast,
):
    """The quarantine CAS cannot consume a generation that changed mid-scan."""

    database_now = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.flush()
        review, task = await _make_review(db, repo, retry_count=7)
        task.started_at = (
            database_now.replace(tzinfo=None) - timedelta(minutes=7)
        )
        task.completed_at = (
            database_now.replace(tzinfo=None) - timedelta(minutes=6)
        )
        await db.commit()
        review_id = review.id
        task_id = task.id
        original_turn_generation = task.turn_generation

    mutated = False

    async def mutate_before_quarantine(_db):
        nonlocal mutated
        if not mutated:
            mutated = True
            async with session_factory() as concurrent_db:
                current = await concurrent_db.get(Task, task_id)
                current.retry_count += 1
                current.turn_generation += 1
                await concurrent_db.commit()
        return database_now

    publish = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_database_now",
            side_effect=mutate_before_quarantine,
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert mutated is True
    assert recovered == 0
    publish.assert_not_awaited()
    no_broadcast.broadcast.assert_not_awaited()
    async with session_factory() as db:
        stored_review = await db.get(PRReview, review_id)
        stored_task = await db.get(Task, task_id)
        assert stored_review.status == "reviewing"
        assert stored_review.action_taken is None
        assert stored_review.completed_at is None
        assert stored_task.retry_count == 8
        assert stored_task.turn_generation == original_turn_generation + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("task_status", ["failed", "cancelled", "conflict"])
async def test_recover_incomplete_terminal_failure_finishes_review_error(
    session_factory,
    no_broadcast,
    task_status,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, _task = await _make_review(
            db,
            repo,
            task_status=task_status,
        )
        review_id = review.id

    recovered = await pr_review_service.recover_incomplete_pr_reviews(
        session_factory
    )

    assert recovered == 1
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "error"
        assert stored.action_taken == "error"
        assert task_status in stored.review_summary


@pytest.mark.asyncio
@pytest.mark.parametrize("task_status", ["failed", "cancelled", "conflict"])
async def test_recover_terminal_review_yields_to_manager_termination_receipt(
    session_factory,
    no_broadcast,
    task_status,
):
    """Receipt ownership fences reviewing terminal projection."""

    async with session_factory() as db:
        repo = _make_repo()
        worker = Worker(name=f"recovery-receipt-{task_status}", status="ready")
        db.add_all((repo, worker))
        await db.flush()
        review, task = await _make_review(
            db,
            repo,
            task_status=task_status,
        )
        task.worker_id = worker.id
        await db.commit()
        review_id = review.id
        task_id = task.id

    await persist_active_manager_receipt(session_factory, task_id)

    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 0
    publish.assert_not_awaited()
    no_broadcast.broadcast.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "reviewing"
        assert stored.action_taken is None
        assert stored.review_summary is None
        assert stored.completed_at is None


@pytest.mark.asyncio
async def test_recover_incomplete_superseded_terminal_never_publishes(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(
            db,
            repo,
            task_status="failed",
        )
        task.metadata_ = {
            **(task.metadata_ or {}),
            "pr_review_superseded": True,
        }
        review_id = review.id
        await db.commit()

    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 1
    publish.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "superseded"


@pytest.mark.asyncio
async def test_recover_superseding_intent_creates_replacement_after_cleanup(
    session_factory,
    no_broadcast,
):
    from backend.services.task_termination import TaskTerminationResult

    replacement = {
        **PR_DATA,
        "head_sha": "9" * 40,
        "delivery_id": "delivery-replacement",
    }
    context = _prepared_context()
    context["head_sha"] = replacement["head_sha"]
    context["guidance"] = {
        "CLAUDE.md": "LEGACY_RECOVERY_CLAUDE_SENTINEL",
        "PROGRESS.md": "LEGACY_RECOVERY_PROGRESS_SENTINEL",
    }
    context["material"]["changed_file_contents"] = [{
        "path": "backend/legacy.py",
        "base": {"content": "LEGACY_RECOVERY_BASE_FILE_SENTINEL"},
        "head": {"content": "LEGACY_RECOVERY_HEAD_FILE_SENTINEL"},
    }]
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(
            db,
            repo,
            task_status="pending",
        )
        task_id = task.id
        review_id = review.id
        review.status = "superseding"
        review.superseding_snapshot = {
            "version": 2,
            "pr_data": replacement,
            "prepared_context": context,
        }
        review.superseding_token = "1" * 48
        review.superseding_started_at = (
            datetime.utcnow() - timedelta(minutes=5)
        )
        await db.commit()

    async def terminate(task_id_arg, db, **_kwargs):
        assert task_id_arg == task_id
        current = await db.get(Task, task_id)
        previous = current.status
        current.status = "completed"
        current.completed_at = datetime.utcnow()
        await db.commit()
        return TaskTerminationResult(
            task_id=task_id,
            previous_status=previous,
            terminal_status="completed",
            transitioned=True,
            stopped=False,
            cleared_messages=0,
            retry_count=current.retry_count,
            turn_generation=current.turn_generation,
            instance_id=current.instance_id,
            started_at=current.started_at,
            completed_at=current.completed_at,
            pty_background_generation=None,
        )

    with patch(
            "backend.services.task_termination."
            "terminate_authoritative_task_generation",
            side_effect=terminate,
        ):
        recovered = await pr_review_service.recover_superseding_pr_reviews(
            session_factory,
            grace_seconds=0,
        )

    assert recovered == 1
    async with session_factory() as db:
        old = await db.get(PRReview, review_id)
        reviews = (
            await db.execute(
                select(PRReview).where(PRReview.repo_id == old.repo_id)
            )
        ).scalars().all()
        assert old.status == "superseded"
        assert old.superseding_snapshot is None
        assert old.superseding_token is None
        assert old.superseding_started_at is None
        assert len(reviews) == 2
        new = next(review for review in reviews if review.id != review_id)
        assert new.status == "reviewing"
        replacement_task = await db.get(Task, new.task_id)
        assert replacement_task is not None
        for sentinel in (
            "LEGACY_RECOVERY_CLAUDE_SENTINEL",
            "LEGACY_RECOVERY_PROGRESS_SENTINEL",
            "LEGACY_RECOVERY_BASE_FILE_SENTINEL",
            "LEGACY_RECOVERY_HEAD_FILE_SENTINEL",
        ):
            assert sentinel not in replacement_task.description
    assert new.head_sha == replacement["head_sha"]


@pytest.mark.asyncio
async def test_recover_superseding_input_rejection_after_cleanup_is_idempotent(
    session_factory,
    no_broadcast,
):
    """Snapshot v4 rechecks current policy and creates no Reviewer Task."""

    from backend.services.task_termination import TaskTerminationResult

    replacement = {
        **PR_DATA,
        "base_ref": "main",
        "head_sha": "9" * 40,
        "delivery_id": "delivery-input-rejection-recovery",
    }
    detail = (
        "unsupported_input_size: recovered review input exceeds the safe model "
        "limit; no reviewer Task was created."
    )
    measured = 140_001
    limit = 140_000
    context = _prepared_context()
    context["head_sha"] = replacement["head_sha"]
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(
            db,
            repo,
            task_status="pending",
        )
        repo_id = repo.id
        task_id = task.id
        review_id = review.id
        review.status = "superseding"
        review.superseding_snapshot = {
            "version": 4,
            "pr_data": replacement,
            "prepared_context": context,
            "input_rejection": {
                "category": "unsupported_input_size",
                "measured": measured,
                "limit": limit,
                "unit": "UTF-8 bytes",
            },
        }
        review.superseding_token = "3" * 48
        review.superseding_started_at = (
            datetime.utcnow() - timedelta(minutes=5)
        )
        await db.commit()

    terminated_ids = []

    async def terminate(task_id_arg, db, **_kwargs):
        terminated_ids.append(task_id_arg)
        current = await db.get(Task, task_id_arg)
        previous = current.status
        current.status = "completed"
        current.completed_at = datetime.utcnow()
        current.metadata_ = {
            **(current.metadata_ or {}),
            "pr_review_superseded": True,
        }
        await db.commit()
        return TaskTerminationResult(
            task_id=task_id_arg,
            previous_status=previous,
            terminal_status="completed",
            transitioned=True,
            stopped=False,
            cleared_messages=0,
            retry_count=current.retry_count,
            turn_generation=current.turn_generation,
            instance_id=current.instance_id,
            started_at=current.started_at,
            completed_at=current.completed_at,
            pty_background_generation=None,
        )

    def reject_current_policy(*_args, **_kwargs):
        raise pr_review_service.PRReviewInputTooLarge(
            detail,
            measured=measured,
            limit=limit,
            unit="UTF-8 bytes",
        )

    with (
        patch(
            "backend.services.task_termination."
            "terminate_authoritative_task_generation",
            side_effect=terminate,
        ),
        patch.object(
            pr_review_service,
            "preflight_pr_review_prompts",
            side_effect=reject_current_policy,
        ),
    ):
        recovered = await pr_review_service.recover_superseding_pr_reviews(
            session_factory,
            grace_seconds=0,
        )
        repeated = await pr_review_service.recover_superseding_pr_reviews(
            session_factory,
            grace_seconds=0,
        )

    assert recovered == 1
    assert repeated == 0
    assert terminated_ids == [task_id]
    async with session_factory() as db:
        reviews = list((await db.execute(
            select(PRReview)
            .where(PRReview.repo_id == repo_id)
            .order_by(PRReview.id)
        )).scalars())
        tasks = list((await db.execute(
            select(Task).order_by(Task.id)
        )).scalars())
        assert len(reviews) == 2
        old, rejected = reviews
        assert old.id == review_id
        assert old.status == "superseded"
        assert old.superseding_snapshot is None
        assert old.superseding_token is None
        assert old.superseding_started_at is None
        assert len(tasks) == 2
        assert any(task.id == task_id for task in tasks)
        display_tasks = [
            task for task in tasks
            if (task.metadata_ or {}).get("pr_monitor_display") is True
        ]
        assert len(display_tasks) == 1
        assert display_tasks[0].status == "completed"
        assert tasks[0].status == "completed"
        assert tasks[0].metadata_["pr_review_superseded"] is True

        assert rejected.status == "error"
        assert rejected.task_id is None
        assert rejected.base_sha == replacement["base_sha"]
        assert rejected.head_sha == replacement["head_sha"]
        assert rejected.publication_state == "not_applicable"
        assert rejected.failure_stage == "reviewer"
        assert rejected.error_category == "unsupported_input_size"
        assert rejected.error_measured == measured
        assert rejected.error_limit == limit
        assert rejected.error_unit == "UTF-8 bytes"
        run = await db.get(PRMonitorRun, rejected.monitor_run_id)
        assert run is not None
        assert run.current_review_id == rejected.id
        assert run.current_base_sha == rejected.base_sha
        assert run.current_head_sha == rejected.head_sha
        assert run.status == "paused"
        assert run.pause_reason == "review_input_too_large"


@pytest.mark.asyncio
async def test_recover_invalid_superseding_input_rejection_fails_closed(
    session_factory,
    no_broadcast,
):
    """Malformed v3 evidence is quarantined and never creates new work."""

    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(
            db,
            repo,
            task_status="pending",
        )
        repo_id = repo.id
        task_id = task.id
        review_id = review.id
        review.status = "superseding"
        review.superseding_snapshot = {
            "version": 3,
            "kind": "input_rejection",
            "repo_name": repo.repo_full_name,
            "pr_data": {
                **PR_DATA,
                "base_ref": "main",
                "head_sha": "8" * 40,
            },
            "input_rejection": {
                "category": "unsupported_input_size",
                "message": "unsupported_input_size: malformed evidence",
                "measured": 10,
                "limit": 20,
                "unit": "characters",
            },
        }
        review.superseding_token = "4" * 48
        review.superseding_started_at = (
            datetime.utcnow() - timedelta(minutes=5)
        )
        await db.commit()

    terminate = AsyncMock()
    create_rejection = AsyncMock()
    with (
        patch(
            "backend.services.task_termination."
            "terminate_authoritative_task_generation",
            terminate,
        ),
        patch.object(
            pr_review_service,
            "create_pr_review_input_rejection",
            create_rejection,
        ),
    ):
        recovered = await pr_review_service.recover_superseding_pr_reviews(
            session_factory,
            grace_seconds=0,
        )

    assert recovered == 1
    terminate.assert_not_awaited()
    create_rejection.assert_not_awaited()
    async with session_factory() as db:
        reviews = list((await db.execute(
            select(PRReview).where(PRReview.repo_id == repo_id)
        )).scalars())
        tasks = list((await db.execute(select(Task))).scalars())
        assert len(reviews) == 1
        stored = reviews[0]
        assert stored.id == review_id
        assert stored.status == "error"
        assert stored.action_taken == "error"
        assert stored.review_summary == (
            "Durable PR synchronize snapshot is invalid"
        )
        assert stored.completed_at is not None
        assert stored.superseding_snapshot is None
        assert stored.superseding_token is None
        assert stored.superseding_started_at is None
        assert stored.task_id == task_id
        assert tasks == [await db.get(Task, task_id)]
        assert tasks[0].status == "pending"


@pytest.mark.asyncio
async def test_recover_superseding_intent_different_base_ref_is_not_self_target(
    session_factory,
    no_broadcast,
):
    from backend.services.task_termination import TaskTerminationResult

    replacement = {
        **PR_DATA,
        "base_ref": "release/2026",
        "delivery_id": "delivery-base-retarget",
    }
    context = _prepared_context()
    context["base_ref"] = replacement["base_ref"]
    context["material"]["base_ref"] = replacement["base_ref"]
    async with session_factory() as db:
        repo = _make_repo(default_branch="release/2026")
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(
            db,
            repo,
            task_status="pending",
        )
        task_id = task.id
        review_id = review.id
        review.status = "superseding"
        review.superseding_snapshot = {
            "version": 2,
            "pr_data": replacement,
            "prepared_context": context,
        }
        review.superseding_token = "2" * 48
        review.superseding_started_at = (
            datetime.utcnow() - timedelta(minutes=5)
        )
        await db.commit()

    terminated_ids = []

    async def terminate(task_id_arg, db, **_kwargs):
        terminated_ids.append(task_id_arg)
        current = await db.get(Task, task_id_arg)
        previous = current.status
        current.status = "completed"
        current.completed_at = datetime.utcnow()
        await db.commit()
        return TaskTerminationResult(
            task_id=task_id_arg,
            previous_status=previous,
            terminal_status="completed",
            transitioned=True,
            stopped=False,
            cleared_messages=0,
            retry_count=current.retry_count,
            turn_generation=current.turn_generation,
            instance_id=current.instance_id,
            started_at=current.started_at,
            completed_at=current.completed_at,
            pty_background_generation=None,
        )

    with patch(
        "backend.services.task_termination."
        "terminate_authoritative_task_generation",
        side_effect=terminate,
    ):
        recovered = await pr_review_service.recover_superseding_pr_reviews(
            session_factory,
            grace_seconds=0,
        )

    assert recovered == 1
    assert terminated_ids == [task_id]
    async with session_factory() as db:
        reviews = list((await db.execute(
            select(PRReview).where(PRReview.repo_id == repo.id)
        )).scalars())
        assert len(reviews) == 2
        old = next(review for review in reviews if review.id == review_id)
        new = next(review for review in reviews if review.id != review_id)
        assert old.status == "superseded"
        assert old.base_ref == "main"
        assert new.status == "reviewing"
        assert new.base_ref == "release/2026"
        assert old.base_sha == new.base_sha == PR_DATA["base_sha"]
        assert old.head_sha == new.head_sha == PR_DATA["head_sha"]


def _thread_finding(*, line=12):
    return PRFinding(
        id=41,
        pr_review_id=1,
        reviewer_run_id=2,
        fingerprint="1" * 64,
        thread_nonce="2" * 48,
        role="senior_engineer",
        severity="high",
        category="correctness",
        path="backend/app.py",
        line=line,
        title="Broken validation",
        evidence="The invalid branch returns success.",
        impact="Bad input is persisted.",
        required_fix="Return a validation error.",
        test="Exercise the invalid branch.",
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("include_valid", (False, True))
async def test_finding_marker_forgery_cannot_block_ack_reconciliation(
    include_valid,
):
    finding = _thread_finding()
    marker_body = pr_review_service._finding_thread_body(finding)
    forged = {
        "id": 90,
        "body": marker_body,
        "user": {"login": "untrusted-user"},
        "html_url": "https://github.test/comment/90",
        "commit_id": PR_DATA["head_sha"],
        "path": finding.path,
    }
    valid = {
        "id": 91,
        "body": marker_body,
        "user": {"login": ACTOR},
        "html_url": "https://github.test/comment/91",
        "commit_id": PR_DATA["head_sha"],
        "path": finding.path,
    }
    post = AsyncMock(return_value=valid)
    with (
        patch.object(
            pr_review_service,
            "_gh_api_value",
            AsyncMock(return_value=[[forged, *([valid] if include_valid else [])]]),
        ),
        patch.object(pr_review_service, "_gh_api_json", post),
    ):
        result = await pr_review_service._publish_one_finding_thread(
            repo_name="owner/repo",
            pr_number=7,
            finding=finding,
            actor=ACTOR,
            ensure_current=AsyncMock(return_value=True),
        )

    assert result == (
        "published_inline",
        91,
        "https://github.test/comment/91",
        None,
    )
    assert post.await_count == (0 if include_valid else 1)


@pytest.mark.asyncio
async def test_blocking_finding_publishes_independent_inline_thread():
    finding = _thread_finding()
    response = {
        "id": 99,
        "body": pr_review_service._finding_thread_body(finding),
        "user": {"login": ACTOR},
        "html_url": "https://github.test/comment/99",
        "commit_id": PR_DATA["head_sha"],
        "path": finding.path,
    }
    with (
        patch.object(pr_review_service, "_gh_api_value", AsyncMock(return_value=[[]])),
        patch.object(pr_review_service, "_gh_api_json", AsyncMock(return_value=response)) as post,
    ):
        result = await pr_review_service._publish_one_finding_thread(
            repo_name="owner/repo",
            pr_number=7,
            finding=finding,
            actor=ACTOR,
            ensure_current=AsyncMock(return_value=True),
        )
    assert result == ("published_inline", 99, "https://github.test/comment/99", None)
    assert post.await_args.kwargs["payload"]["line"] == 12
    assert post.await_args.kwargs["payload"]["commit_id"] == PR_DATA["head_sha"]


@pytest.mark.asyncio
async def test_unlocatable_finding_falls_back_without_clearing_blocker():
    finding = _thread_finding(line=None)
    response = {
        "id": 100,
        "body": pr_review_service._finding_thread_body(finding),
        "user": {"login": ACTOR},
        "html_url": "https://github.test/comment/100",
    }
    with (
        patch.object(pr_review_service, "_gh_api_value", AsyncMock(return_value=[[]])),
        patch.object(pr_review_service, "_gh_api_json", AsyncMock(return_value=response)) as post,
    ):
        result = await pr_review_service._publish_one_finding_thread(
            repo_name="owner/repo",
            pr_number=7,
            finding=finding,
            actor=ACTOR,
            ensure_current=AsyncMock(return_value=True),
        )
    assert result[0] == "published_fallback"
    assert "blocker remains open" in result[3]
    assert "/issues/7/comments" in post.await_args.args[0]


@pytest.mark.asyncio
async def test_finding_publication_survives_exact_guard_rollback(db_session):
    """A fresh publication guard rolls back and expires ORM state by design."""

    repo = _make_repo(review_mode="panel")
    db_session.add(repo)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=7,
        base_ref="main",
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        pr_title="fixture",
        pr_author="alice",
        pr_url="https://github.test/owner/repo/pull/7",
        status="publishing",
    )
    db_session.add(review)
    await db_session.flush()
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="3" * 64,
        guide_pack_hash="4" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = _thread_finding(line=8)
    finding.id = None
    finding.pr_review_id = review.id
    finding.reviewer_run_id = reviewer.id
    db_session.add(finding)
    await db_session.commit()
    review_id = review.id
    finding_id = finding.id

    async def exact_guard():
        await db_session.rollback()
        return True

    async def list_comments(*_args, **_kwargs):
        assert not db_session.in_transaction()
        return [[]]

    async def post_comment(_endpoint, *, payload=None, **_kwargs):
        assert not db_session.in_transaction()
        return {
            "id": 101,
            "body": payload["body"],
            "user": {"login": ACTOR},
            "html_url": "https://github.test/comment/101",
            "commit_id": PR_DATA["head_sha"],
            "path": "backend/app.py",
        }

    with (
        patch.object(pr_review_service, "_gh_api_value", side_effect=list_comments),
        patch.object(pr_review_service, "_gh_api_json", side_effect=post_comment),
    ):
        await pr_review_service._publish_blocking_finding_threads(
            db_session,
            review_id=review_id,
            repo_name="owner/repo",
            pr_number=7,
            actor=ACTOR,
            ensure_current=exact_guard,
        )

    published = await db_session.get(PRFinding, finding_id, populate_existing=True)
    assert published.thread_status == "published_inline"
    assert published.github_comment_id == 101
