"""Exact-head and idempotency tests for the Delivery publishing boundary."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs, urlsplit

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.database import Base
from backend.models.delivery import DeliveryAction, DeliveryRun
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRFindingRebuttal,
    PRMergeQueueAction,
    PRMonitorRun,
    PRRepairWake,
    PRReview,
    PRReviewerRun,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.services import delivery_publisher as delivery_publisher_service
from backend.services import pr_review_actions as pr_review_actions_service
from backend.services.delivery_controller import (
    DeliveryEffectFence,
    DeliveryPublisherNoEffectPreflightError,
    DeliveryPublisherPermanentError,
    DeliverySubjectChanged,
    PublishedPullRequest,
)
from backend.services.delivery_publisher import (
    DeliveryGitAuthenticationError,
    DeliveryGitError,
    DeliveryNonFastForwardError,
    GhDeliveryGateway,
    GitDeliveryGateway,
    GitHubDeliveryPublisher,
    _value_hash,
)
from backend.services.pr_monitor_loop import attach_review_to_run, record_gate_pass
from backend.services.pr_review_actions import FindingActionConflict
from backend.services.pr_review_service import GhError


BASE = "1" * 40
HEAD = "2" * 40
TREE = "3" * 40
PATCH = "4" * 64
HEAD2 = "7" * 40
TREE2 = "8" * 40
PATCH2 = "9" * 64
MERGE_PUBLISHED_AT = datetime(2026, 8, 1, 0, 0, 0)


@pytest.mark.asyncio
async def test_github_history_query_is_head_only_and_includes_terminal_prs(
    monkeypatch,
):
    api = AsyncMock(return_value=[])
    monkeypatch.setattr(delivery_publisher_service, "_gh_api_value", api)

    gateway = GhDeliveryGateway()
    assert await gateway.list_pull_requests(
        repo_full_name="acme/widgets",
        owner="acme",
        head_branch="ccm/delivery-41",
    ) == []

    path = api.await_args.args[0]
    parsed = urlsplit(path)
    assert parsed.path == "repos/acme/widgets/pulls"
    assert parse_qs(parsed.query) == {
        "state": ["all"],
        "head": ["acme:ccm/delivery-41"],
        "per_page": ["100"],
    }
    assert "base" not in parse_qs(parsed.query)


@pytest_asyncio.fixture
async def concurrent_db_factory(tmp_path):
    """File-backed SQLite supplies distinct connections for writer races."""

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'publisher-race.db'}",
        echo=False,
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    try:
        yield factory
    finally:
        await engine.dispose()


@dataclass
class FakeGit:
    repo_full_name: str
    remote_refs: dict[str, str | None]
    expected_base: str = BASE
    expected_head: str = HEAD
    expected_tree: str = TREE
    expected_patch: str = PATCH
    push_error: Exception | None = None
    push_response_lost: bool = False
    verify_calls: int = 0
    push_calls: int = 0

    async def verify_local(self, subject) -> None:
        self.verify_calls += 1
        assert subject.base_sha == self.expected_base
        assert subject.head_sha == self.expected_head
        assert subject.head_tree_sha == self.expected_tree
        assert subject.patch_sha256 == self.expected_patch

    async def origin_repo_full_name(self, subject) -> str:
        return self.repo_full_name

    async def remote_ref_sha(self, subject, branch: str) -> str | None:
        return self.remote_refs.get(branch)

    async def push_exact(self, subject):
        self.push_calls += 1
        if self.push_error is not None:
            raise self.push_error
        self.remote_refs[subject.delivery_branch] = subject.head_sha
        if self.push_response_lost:
            raise DeliveryGitError("response lost")


@dataclass
class FakeGitHub:
    pulls: list[dict] = field(default_factory=list)
    create_response_lost: bool = False
    create_calls: int = 0
    list_calls: int = 0
    get_calls: int = 0
    created_payload: dict | None = None

    async def list_pull_requests(self, **_kwargs) -> list[dict]:
        self.list_calls += 1
        return list(self.pulls)

    async def get_pull_request(self, *, pr_number: int, **_kwargs) -> dict:
        self.get_calls += 1
        for pull in self.pulls:
            if pull["number"] == pr_number:
                return pull
        raise GhError("not found")

    async def create_pull_request(self, **kwargs) -> dict:
        self.create_calls += 1
        assert self.created_payload is not None
        assert kwargs["head_branch"] == self.created_payload["head"]["ref"]
        assert kwargs["base_branch"] == self.created_payload["base"]["ref"]
        assert "ccm-delivery" in kwargs["body"]
        self.pulls.append(self.created_payload)
        if self.create_response_lost:
            raise GhError("connection closed after request")
        return self.created_payload


def _pull(
    *,
    branch: str,
    repo_full_name: str,
    number: int = 17,
    base_sha: str = BASE,
    head_sha: str = HEAD,
    base_branch: str = "main",
    head_branch: str | None = None,
    base_repo: str | None = None,
    head_repo: str | None = None,
    state: str = "open",
    merged: bool | None = None,
) -> dict:
    payload = {
        "number": number,
        "html_url": f"https://github.com/{repo_full_name}/pull/{number}",
        "state": state,
        "title": "Deliver exact head",
        "user": {"login": "delivery-bot"},
        "base": {
            "sha": base_sha,
            "ref": base_branch,
            "repo": {"full_name": base_repo or repo_full_name},
        },
        "head": {
            "sha": head_sha,
            "ref": head_branch or branch,
            "repo": {"full_name": head_repo or repo_full_name},
        },
    }
    if merged is not None:
        payload["merged"] = merged
        payload["merged_at"] = "2026-08-06T00:00:00Z" if merged else None
    return payload


async def _delivery_scope(
    db_session,
    *,
    suffix: str,
    auto_merge: bool = False,
    wait_for_ci: bool = True,
):
    assert wait_for_ci or not auto_merge
    repo_name = f"acme/delivery-{suffix}"
    project = Project(
        name=f"publisher-{suffix}",
        worker_id=None,
        local_path=f"/srv/repos/publisher-{suffix}",
        git_url=f"git@github.com:{repo_name}.git",
        has_remote=True,
        default_branch="main",
        status="ready",
    )
    db_session.add(project)
    await db_session.flush()
    required_checks = [
        {"kind": "check_run", "name": "tests", "app_slug": "github-actions"}
    ] if wait_for_ci else []
    monitored = MonitoredRepo(
        repo_full_name=repo_name,
        project_id=project.id,
        worker_id=None,
        webhook_secret="secret",
        enabled=True,
        auto_merge=auto_merge,
        review_mode="panel",
        wait_for_ci=wait_for_ci,
        required_checks=required_checks,
        merge_queue_mode="manual",
        default_branch="main",
    )
    db_session.add(monitored)
    await db_session.flush()
    policy = {
        "schema_version": 1,
        "terminal": "merged" if auto_merge else "ready_to_merge",
        "auto_merge": auto_merge,
        "max_cycles": 10,
        "max_no_progress": 3,
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "default",
        "effort_level": "high",
        "timeout_hours": 4,
        "pr_monitor": {
            "repo_id": monitored.id,
            "repo_full_name": monitored.repo_full_name,
            "review_mode": "panel",
            "wait_for_ci": wait_for_ci,
            "required_checks": required_checks,
        },
    }
    branch = f"ccm/delivery/{suffix}"
    run = DeliveryRun(
        admission_scope="system",
        idempotency_key=f"publisher-{suffix}",
        request_hash="f" * 64,
        project_id=project.id,
        monitored_repo_id=monitored.id,
        title="Deliver exact head",
        requirements="Implement the change and preserve exact-head safety.",
        requirements_hash="5" * 64,
        policy_snapshot=policy,
        policy_hash=_value_hash(policy),
        base_branch="main",
        delivery_branch=branch,
        workspace_path=f"/srv/repos/publisher-{suffix}/.claude-manager/worktrees/delivery-1",
        base_sha=BASE,
        head_sha=HEAD,
        head_tree_sha=TREE,
        patch_sha256=PATCH,
        phase="publishing",
        activity="running",
    )
    db_session.add(run)
    await db_session.commit()
    return run, project, monitored


def _key(run: DeliveryRun, *, monitor: bool = False) -> str:
    value = f"delivery:{run.id}:publish:{run.base_sha}:{run.head_sha}"
    return value + (":monitor" if monitor else "")


async def _fence(db_factory, run: DeliveryRun) -> DeliveryEffectFence:
    """Install the exact durable Controller/Action lease used by publisher tests."""

    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert stored is not None
        action = await db.scalar(
            select(DeliveryAction)
            .where(
                DeliveryAction.run_id == stored.id,
                DeliveryAction.expected_base_sha == stored.base_sha,
                DeliveryAction.expected_head_sha == stored.head_sha,
            )
            .order_by(DeliveryAction.id.desc())
            .limit(1)
        )
        if action is None:
            active = await db.scalar(
                select(DeliveryAction).where(
                    DeliveryAction.active_run_id == stored.id
                )
            )
            if active is not None:
                active.status = "succeeded"
                active.active_run_id = None
                active.lease_owner = None
                active.lease_expires_at = None
                active.completed_at = datetime.utcnow()
            payload = {
                "schema_version": 1,
                "run_id": stored.id,
                "cycle_id": stored.current_cycle_id,
                "repo_id": stored.monitored_repo_id,
                "base_sha": stored.base_sha,
                "head_sha": stored.head_sha,
                "head_tree_sha": stored.head_tree_sha,
                "patch_sha256": stored.patch_sha256,
                "base_branch": stored.base_branch,
                "delivery_branch": stored.delivery_branch,
            }
            action = DeliveryAction(
                run_id=stored.id,
                cycle_id=stored.current_cycle_id,
                active_run_id=stored.id,
                action_type="ensure_pull_request",
                idempotency_key=_key(stored),
                desired_version=stored.state_version,
                expected_base_sha=stored.base_sha,
                expected_head_sha=stored.head_sha,
                payload=payload,
                payload_hash=_value_hash(payload),
                status="leased",
            )
            db.add(action)
            await db.flush()
        owner = "publisher-test-controller"
        token = "a" * 64
        expires = datetime.utcnow() + timedelta(minutes=5)
        stored.lease_owner = owner
        stored.lease_expires_at = expires
        stored.controller_generation += 1
        action.status = "leased"
        action.active_run_id = stored.id
        action.lease_owner = token
        action.lease_expires_at = expires
        action.next_attempt_at = None
        await db.commit()
        return DeliveryEffectFence(
            run_id=stored.id,
            controller_owner=owner,
            controller_generation=stored.controller_generation,
            action_id=action.id,
            action_token=token,
            expected_base_sha=stored.base_sha,
            expected_head_sha=stored.head_sha,
        )


async def _ready_delivery_scope(db_session, db_factory, *, suffix: str):
    run, _project, repo = await _delivery_scope(db_session, suffix=suffix)
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    payload = _pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)
    github = FakeGitHub(pulls=[payload])

    async def create_review(db, monitored_repo, pr_data):
        review = PRReview(
            repo_id=monitored_repo.id,
            pr_number=pr_data["number"],
            base_ref=pr_data["base_ref"],
            base_sha=pr_data["base_sha"],
            head_sha=pr_data["head_sha"],
            delivery_id=pr_data["delivery_id"],
            pr_title=pr_data["title"],
            pr_author=pr_data["author"],
            pr_url=pr_data["url"],
            status="waiting_ci",
            action_nonce="c" * 48,
            ci_status="pending",
        )
        db.add(review)
        await db.flush()
        await attach_review_to_run(
            db,
            repo=monitored_repo,
            review=review,
            pr_data=pr_data,
        )
        return review

    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=git,
        github=github,
        review_creator=create_review,
    )
    pull_request = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )
    monitor_id = await publisher.ensure_monitor(
        run_id=run.id,
        pull_request=pull_request,
        idempotency_key=_key(run, monitor=True),
        fence=await _fence(db_factory, run),
    )
    async with db_factory() as db:
        stored_run = await db.get(DeliveryRun, run.id, populate_existing=True)
        monitor = await db.get(PRMonitorRun, monitor_id, populate_existing=True)
        review = await db.get(PRReview, monitor.current_review_id, populate_existing=True)
        stored_run.pr_number = pull_request.pr_number
        stored_run.pr_url = pull_request.url
        stored_run.pr_monitor_run_id = monitor.id
        stored_run.phase = "monitoring"
        stored_run.activity = "waiting"
        stored_run.wait_reason = "pr_monitor"
        monitor.status = "ready_to_merge"
        monitor.state_version += 1
        review.status = "approved"
        review.action_taken = "lgtm_comment"
        review.ci_status = "passed"
        await db.commit()
        monitor_version = monitor.state_version
    return run, repo, git, github, publisher, pull_request, monitor_id, monitor_version


async def _merged_delivery_scope(db_session, db_factory, *, suffix: str):
    run, _project, repo = await _delivery_scope(
        db_session,
        suffix=suffix,
        auto_merge=True,
    )
    nonce = "e" * 48
    review_task = Task(
        title="Merged Delivery reviewer",
        description="Exact merged review evidence.",
        mode="auto",
        status="completed",
        metadata_={
            "pr_review_id": None,
            "pr_base_ref": "main",
            "pr_base_sha": BASE,
            "pr_head_sha": HEAD,
            "pr_auto_merge": True,
            "pr_action_nonce": nonce,
        },
        completed_at=datetime.utcnow(),
    )
    db_session.add(review_task)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=17,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        delivery_id=f"delivery:{run.id}:{HEAD}",
        pr_title="Deliver exact head",
        pr_author="delivery-bot",
        pr_url=f"https://github.com/{repo.repo_full_name}/pull/17",
        task_id=review_task.id,
        status="merged",
        action_nonce=nonce,
        action_taken="approved_merged",
        publishing_actor="ccm-bot",
        publishing_started_at=MERGE_PUBLISHED_AT,
        merge_method="fast-forward",
        completed_at=datetime.utcnow(),
    )
    db_session.add(review)
    await db_session.flush()
    review_task.metadata_ = {
        **(review_task.metadata_ or {}),
        "pr_review_id": review.id,
    }
    monitor = PRMonitorRun(
        repo_id=repo.id,
        pr_number=17,
        status="merged",
        current_base_sha=BASE,
        current_head_sha=HEAD,
        current_review_id=review.id,
        head_repo_full_name=repo.repo_full_name,
        head_branch=run.delivery_branch,
        completed_at=datetime.utcnow(),
    )
    db_session.add(monitor)
    await db_session.flush()
    review.monitor_run_id = monitor.id
    run.pr_number = 17
    run.pr_url = review.pr_url
    run.pr_monitor_run_id = monitor.id
    run.phase = "monitoring"
    run.activity = "waiting"
    run.wait_reason = "pr_monitor"
    await db_session.commit()
    pull_request = PublishedPullRequest(
        repo_id=repo.id,
        pr_number=17,
        url=review.pr_url,
        base_sha=BASE,
        head_sha=HEAD,
        head_branch=run.delivery_branch,
        head_repo_full_name=repo.repo_full_name,
    )
    # A successful merge may advance main and delete the delivery branch.
    git = FakeGit(
        repo.repo_full_name,
        {"main": HEAD2, run.delivery_branch: None},
    )
    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=git,
        github=FakeGitHub(),
    )
    return run, repo, publisher, pull_request, monitor.id, monitor.state_version, nonce


@pytest.mark.asyncio
async def test_create_response_loss_recovers_exact_open_pr(db_session, db_factory):
    run, _project, repo = await _delivery_scope(db_session, suffix="response-loss")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    github = FakeGitHub(create_response_lost=True)
    github.created_payload = _pull(
        branch=run.delivery_branch,
        repo_full_name=repo.repo_full_name,
    )
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    published = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )

    assert published.pr_number == 17
    assert published.base_sha == BASE
    assert published.head_sha == HEAD
    assert git.remote_refs[run.delivery_branch] == HEAD
    assert git.push_calls == 1
    assert github.create_calls == 1
    # state=all is checked before branch publication, after publication, and
    # once more to reconcile the lost create response.
    assert github.list_calls == 3


@pytest.mark.asyncio
async def test_panel_only_policy_can_publish_exact_pr(db_session, db_factory):
    run, _project, repo = await _delivery_scope(
        db_session,
        suffix="panel-only",
        wait_for_ci=False,
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    github = FakeGitHub()
    github.created_payload = _pull(
        branch=run.delivery_branch,
        repo_full_name=repo.repo_full_name,
    )
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    published = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )

    assert published.pr_number == 17
    assert git.remote_refs[run.delivery_branch] == HEAD
    assert github.create_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("merged", "terminal_state"),
    [(False, "closed"), (True, "merged")],
)
async def test_terminal_historical_pr_is_persisted_and_never_recreated(
    db_session,
    db_factory,
    merged,
    terminal_state,
):
    run, _project, repo = await _delivery_scope(
        db_session,
        suffix=f"historical-{terminal_state}",
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    github = FakeGitHub(
        pulls=[
            _pull(
                branch=run.delivery_branch,
                repo_full_name=repo.repo_full_name,
                state="closed",
                merged=merged,
            )
        ]
    )
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    fence = await _fence(db_factory, run)

    with pytest.raises(
        DeliveryPublisherPermanentError,
        match=rf"already {terminal_state}",
    ):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=fence,
        )

    async with db_factory() as db:
        action = await db.get(DeliveryAction, fence.action_id)
        stored_run = await db.get(DeliveryRun, run.id)
        assert action.result["schema_version"] == 2
        assert action.result["kind"] == "pull_request_history_conflict"
        assert action.result["remote"]["state"] == terminal_state
        assert action.remote_id == "17"
        assert stored_run.pr_number == 17
        assert stored_run.pr_url == action.remote_url

    # Even before the Controller projects the terminal receipt, a same-owner
    # retry must reject it locally and must not issue another remote mutation.
    with pytest.raises(DeliverySubjectChanged, match="incompatible remote receipt"):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=fence,
        )
    assert git.push_calls == 0
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_create_response_loss_then_close_never_creates_replacement(
    db_session,
    db_factory,
):
    run, _project, repo = await _delivery_scope(
        db_session,
        suffix="response-loss-closed",
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    github = FakeGitHub()

    async def create_then_lose_response_and_close(**_kwargs):
        github.create_calls += 1
        github.pulls.append(
            _pull(
                branch=run.delivery_branch,
                repo_full_name=repo.repo_full_name,
                state="closed",
                merged=False,
            )
        )
        raise GhError("connection closed after request")

    github.create_pull_request = create_then_lose_response_and_close
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    fence = await _fence(db_factory, run)

    with pytest.raises(DeliveryPublisherPermanentError, match="already closed"):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=fence,
        )

    async with db_factory() as db:
        action = await db.get(DeliveryAction, fence.action_id)
        assert action.result["kind"] == "pull_request_history_conflict"
        assert action.result["remote"]["state"] == "closed"
    assert github.create_calls == 1

    with pytest.raises(DeliverySubjectChanged, match="incompatible remote receipt"):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=fence,
        )
    assert github.create_calls == 1


@pytest.mark.asyncio
async def test_durable_create_intent_with_no_remote_identity_never_calls_create(
    db_session,
    db_factory,
):
    run, _project, repo = await _delivery_scope(
        db_session,
        suffix="intent-crash",
    )
    git = FakeGit(
        repo.repo_full_name,
        {"main": BASE, run.delivery_branch: HEAD},
    )
    github = FakeGitHub()
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    fence = await _fence(db_factory, run)
    subject = await publisher._load_subject(run.id)

    # Models a process death after the at-most-once barrier commits and before
    # GitHub returns (or even receives) the create request.
    await publisher._record_create_intent(subject, fence)

    with pytest.raises(
        DeliveryPublisherPermanentError,
        match="replacement creation is forbidden",
    ):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=fence,
        )

    async with db_factory() as db:
        action = await db.get(DeliveryAction, fence.action_id)
        assert action.result["kind"] == "pull_request_create_unresolved"
        assert action.remote_id is None
        assert action.remote_url is None
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_git_push_response_loss_recovers_remote_ref(db_session, db_factory):
    run, _project, repo = await _delivery_scope(db_session, suffix="push-loss")
    git = FakeGit(
        repo.repo_full_name,
        {"main": BASE, run.delivery_branch: None},
        push_response_lost=True,
    )
    existing = _pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)
    github = FakeGitHub(pulls=[existing])
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    published = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )

    assert published.pr_number == 17
    assert git.push_calls == 1
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_existing_exact_pr_is_reused_without_push_or_create(
    db_session, db_factory
):
    run, _project, repo = await _delivery_scope(db_session, suffix="existing")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    github = FakeGitHub(
        pulls=[_pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)]
    )
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    first = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )
    second = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )

    assert second == first
    assert git.push_calls == 0
    assert github.create_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["base", "head", "branch", "repo"])
async def test_existing_pr_with_wrong_exact_subject_is_refused(
    db_session, db_factory, mismatch
):
    run, _project, repo = await _delivery_scope(
        db_session, suffix=f"wrong-{mismatch}"
    )
    values = {
        "base_sha": "9" * 40 if mismatch == "base" else BASE,
        "head_sha": "8" * 40 if mismatch == "head" else HEAD,
        "head_branch": "somebody/elses-branch" if mismatch == "branch" else None,
        "head_repo": "other/repository" if mismatch == "repo" else None,
    }
    github = FakeGitHub(
        pulls=[
            _pull(
                branch=run.delivery_branch,
                repo_full_name=repo.repo_full_name,
                **values,
            )
        ]
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    with pytest.raises(
        DeliveryPublisherPermanentError,
        match="exact Delivery subject",
    ) as raised:
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=await _fence(db_factory, run),
        )

    assert not isinstance(raised.value, DeliveryPublisherNoEffectPreflightError)
    # Ambiguous historical identity is rejected before any branch mutation.
    assert git.push_calls == 0
    assert github.create_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["number", "url", "branch", "repo"])
async def test_bound_pr_stale_head_does_not_relax_remote_identity(
    db_session,
    db_factory,
    mismatch,
):
    run, _project, repo = await _delivery_scope(
        db_session,
        suffix=f"bound-stale-{mismatch}",
    )
    run.pr_number = 17
    run.pr_url = f"https://github.com/{repo.repo_full_name}/pull/17"
    await db_session.commit()

    number = 18 if mismatch == "number" else 17
    payload = _pull(
        branch=run.delivery_branch,
        repo_full_name=repo.repo_full_name,
        number=number,
        head_sha=HEAD2,
        head_branch="somebody/elses-branch" if mismatch == "branch" else None,
        head_repo="other/repository" if mismatch == "repo" else None,
    )
    if mismatch == "url":
        payload["html_url"] = (
            f"https://github.com/{repo.repo_full_name}/pull/18"
        )
    github = FakeGitHub(pulls=[payload])
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD2})
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    with pytest.raises(
        DeliveryPublisherPermanentError,
        match="exact Delivery subject",
    ):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=await _fence(db_factory, run),
        )

    assert git.push_calls == 0
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_non_fast_forward_is_never_retried_with_force(db_session, db_factory):
    run, _project, repo = await _delivery_scope(db_session, suffix="non-ff")
    git = FakeGit(
        repo.repo_full_name,
        {"main": BASE, run.delivery_branch: "7" * 40},
        push_error=DeliveryNonFastForwardError("rejected"),
    )
    github = FakeGitHub()
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    with pytest.raises(
        DeliveryPublisherNoEffectPreflightError,
        match="without force",
    ):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=await _fence(db_factory, run),
        )

    assert git.push_calls == 1
    assert github.list_calls == 1
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_proven_git_auth_failure_is_a_no_effect_terminal_error(
    db_session,
    db_factory,
):
    run, _project, repo = await _delivery_scope(db_session, suffix="git-auth")
    git = FakeGit(
        repo.repo_full_name,
        {"main": BASE, run.delivery_branch: None},
        push_error=DeliveryGitAuthenticationError("credentials unavailable"),
    )
    github = FakeGitHub()
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    with pytest.raises(
        DeliveryPublisherNoEffectPreflightError,
        match="credentials are unavailable",
    ):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=await _fence(db_factory, run),
        )

    assert git.push_calls == 1
    assert git.remote_refs[run.delivery_branch] is None
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_production_git_gateway_uses_plain_refspec_without_force(
    db_session, db_factory, monkeypatch
):
    run, _project, repo = await _delivery_scope(db_session, suffix="push-argv")
    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=FakeGit(repo.repo_full_name, {"main": BASE}),
        github=FakeGitHub(),
    )
    subject = await publisher._load_subject(run.id)
    calls = []

    async def rejected(_cwd, *args, **_kwargs):
        calls.append(args)
        return 1, b"!\trefs/heads/x\t[rejected] (non-fast-forward)\n", b""

    async def validated_urls(_self, current):
        return current.project_git_url, current.project_git_url

    monkeypatch.setattr("backend.services.delivery_publisher._run_git", rejected)
    monkeypatch.setattr(
        delivery_publisher_service,
        "_github_credential_config",
        lambda _url: (
            "-c",
            "credential.https://github.com.helper=",
            "-c",
            "credential.https://github.com.helper=!/trusted/gh auth git-credential",
        ),
    )
    monkeypatch.setattr(
        GitDeliveryGateway,
        "_validated_remote_urls",
        validated_urls,
    )

    with pytest.raises(DeliveryNonFastForwardError):
        await GitDeliveryGateway().push_exact(subject)

    assert len(calls) == 1
    argv = calls[0]
    assert "--force" not in argv
    assert "--force-with-lease" not in argv
    assert (
        "credential.https://github.com.helper=!/trusted/gh auth git-credential"
        in argv
    )
    assert f"{HEAD}:refs/heads/{run.delivery_branch}" in argv


@pytest.mark.asyncio
async def test_production_git_gateway_uses_explicit_verified_remote_urls(
    db_session,
    db_factory,
    monkeypatch,
):
    run, project, repo = await _delivery_scope(db_session, suffix="explicit-url")
    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=FakeGit(repo.repo_full_name, {"main": BASE}),
        github=FakeGitHub(),
    )
    subject = await publisher._load_subject(run.id)
    calls: list[tuple[str, ...]] = []

    async def successful(_cwd, *args, **_kwargs):
        calls.append(args)
        if "ls-remote" in args:
            ref = args[-1]
            return 0, f"{BASE}\t{ref}\n".encode(), b""
        return 0, b"", b""

    async def validated_urls(_self, current):
        return current.project_git_url, current.project_git_url

    monkeypatch.setattr(delivery_publisher_service, "_run_git", successful)
    monkeypatch.setattr(
        delivery_publisher_service,
        "_github_credential_config",
        lambda _url: (
            "-c",
            "credential.https://github.com.helper=",
            "-c",
            "credential.https://github.com.helper=!/trusted/gh auth git-credential",
        ),
    )
    monkeypatch.setattr(
        GitDeliveryGateway,
        "_validated_remote_urls",
        validated_urls,
    )

    assert await GitDeliveryGateway().remote_ref_sha(subject, "main") == BASE
    await GitDeliveryGateway().push_exact(subject)

    assert len(calls) == 2
    for argv in calls:
        assert project.git_url in argv
        assert "origin" not in argv


def test_github_https_credential_helper_is_fixed_and_tokenless(
    tmp_path,
    monkeypatch,
):
    gh = tmp_path / "trusted gh"
    gh.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    gh.chmod(0o700)
    monkeypatch.setattr(
        delivery_publisher_service.shutil,
        "which",
        lambda command: str(gh) if command == "gh" else None,
    )

    config = delivery_publisher_service._github_credential_config(
        "https://github.com/acme/widgets.git"
    )

    assert config[:2] == (
        "-c",
        "credential.https://github.com.helper=",
    )
    assert config[2] == "-c"
    assert "auth git-credential" in config[3]
    assert str(gh.resolve()) in config[3]
    assert "token" not in config[3].lower()


def test_delivery_git_environment_projects_private_ssh_key(tmp_path):
    key = tmp_path / "delivery_key"
    key.write_text("test-only-key", encoding="utf-8")
    key.chmod(0o600)

    env = delivery_publisher_service._git_environment(str(key))

    assert env["GIT_SSH_COMMAND"] == (
        f"ssh -i {key} -F /dev/null -o IdentitiesOnly=yes "
        "-o StrictHostKeyChecking=accept-new -o BatchMode=yes"
    )


def test_delivery_git_environment_rejects_group_readable_key(tmp_path):
    key = tmp_path / "delivery_key"
    key.write_text("test-only-key", encoding="utf-8")
    key.chmod(0o640)

    with pytest.raises(
        DeliveryGitAuthenticationError,
        match="private-key boundary",
    ):
        delivery_publisher_service._git_environment(str(key))


def test_github_ssh_transport_does_not_require_https_credential_helper(
    monkeypatch,
):
    monkeypatch.setattr(
        delivery_publisher_service.shutil,
        "which",
        lambda _command: pytest.fail("SSH transport must not resolve gh"),
    )

    assert (
        delivery_publisher_service._github_credential_config(
            "git@github.com:acme/widgets.git"
        )
        == ()
    )


@pytest.mark.asyncio
async def test_production_git_gateway_classifies_missing_https_credentials(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _project, repo = await _delivery_scope(db_session, suffix="push-auth")
    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=FakeGit(repo.repo_full_name, {"main": BASE}),
        github=FakeGitHub(),
    )
    subject = await publisher._load_subject(run.id)

    async def rejected(_cwd, *args, **_kwargs):
        assert "push" in args
        return 128, b"", b"fatal: unable to get password from user\n"

    async def validated_urls(_self, _current):
        remote = "https://github.com/acme/widgets.git"
        return remote, remote

    monkeypatch.setattr(delivery_publisher_service, "_run_git", rejected)
    monkeypatch.setattr(
        delivery_publisher_service,
        "_github_credential_config",
        lambda _url: (
            "-c",
            "credential.https://github.com.helper=",
            "-c",
            "credential.https://github.com.helper=!/trusted/gh auth git-credential",
        ),
    )
    monkeypatch.setattr(
        GitDeliveryGateway,
        "_validated_remote_urls",
        validated_urls,
    )

    with pytest.raises(
        DeliveryGitAuthenticationError,
        match="authentication is unavailable",
    ):
        await GitDeliveryGateway().push_exact(subject)


@pytest.mark.asyncio
async def test_production_git_gateway_refuses_unpersisted_fork_fallback(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _project, repo = await _delivery_scope(
        db_session,
        suffix="push-permission",
    )
    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=FakeGit(repo.repo_full_name, {"main": BASE}),
        github=FakeGitHub(),
    )
    subject = await publisher._load_subject(run.id)
    calls: list[tuple[str, ...]] = []

    async def rejected(_cwd, *args, **_kwargs):
        calls.append(args)
        return (
            1,
            b"",
            b"remote: Permission to acme/widgets denied to ccm-bot.\n",
        )

    async def validated_urls(_self, _current):
        remote = "https://github.com/acme/widgets.git"
        return remote, remote

    monkeypatch.setattr(delivery_publisher_service, "_run_git", rejected)
    monkeypatch.setattr(
        delivery_publisher_service,
        "_github_credential_config",
        lambda _url: (),
    )
    monkeypatch.setattr(
        GitDeliveryGateway,
        "_validated_remote_urls",
        validated_urls,
    )

    with pytest.raises(
        DeliveryGitAuthenticationError,
        match="write permission is unavailable",
    ):
        await GitDeliveryGateway().push_exact(subject)

    assert len(calls) == 1
    assert "push" in calls[0]


def test_publisher_rejects_plain_http_github_remote_identity():
    assert (
        delivery_publisher_service._github_repo_from_url(
            "http://github.com/acme/insecure.git"
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode", [0, 1])
async def test_publisher_git_reaps_process_group_descendants_after_parent_exit(
    tmp_path,
    monkeypatch,
    returncode,
):
    stdout = asyncio.StreamReader()
    stderr = asyncio.StreamReader()
    stdout.feed_eof()
    stderr.feed_eof()
    process = MagicMock(
        pid=64_322,
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
        delivery_publisher_service.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    actual, _stdout, _stderr = await delivery_publisher_service._run_git(
        str(tmp_path),
        "status",
    )

    assert actual == returncode
    assert (64_322, delivery_publisher_service.signal.SIGTERM) in signals
    assert (64_322, delivery_publisher_service.signal.SIGKILL) in signals


@pytest.mark.asyncio
async def test_publisher_git_repeated_cancellation_waits_for_group_reap(
    tmp_path,
    monkeypatch,
):
    spawned = asyncio.Event()
    wait_started = asyncio.Event()
    release_wait = asyncio.Event()
    stdout = asyncio.StreamReader()
    stderr = asyncio.StreamReader()
    process = MagicMock(pid=64_323, returncode=None, stdout=stdout, stderr=stderr)

    async def wait():
        wait_started.set()
        await release_wait.wait()
        process.returncode = -9
        return -9

    process.wait = AsyncMock(side_effect=wait)

    async def create_subprocess_exec(*args, **kwargs):
        del args, kwargs
        spawned.set()
        return process

    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)
    monkeypatch.setattr(
        delivery_publisher_service.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    command = asyncio.create_task(
        delivery_publisher_service._run_git(str(tmp_path), "status")
    )
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
    assert (64_323, delivery_publisher_service.signal.SIGTERM) in signals
    assert (64_323, delivery_publisher_service.signal.SIGKILL) in signals


@pytest.mark.asyncio
async def test_wrong_origin_repository_is_refused_before_push(db_session, db_factory):
    run, _project, _repo = await _delivery_scope(db_session, suffix="wrong-origin")
    git = FakeGit("other/repository", {"main": BASE, run.delivery_branch: None})
    github = FakeGitHub()
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    with pytest.raises(DeliveryPublisherNoEffectPreflightError, match="origin"):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=await _fence(db_factory, run),
        )

    assert git.push_calls == 0
    assert github.list_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("expired_lease", ["run", "action"])
async def test_expired_effect_lease_is_refused_at_last_fence_before_push(
    db_session,
    db_factory,
    expired_lease,
):
    """A lease lost during preflight cannot authorize the following push."""

    run, _project, repo = await _delivery_scope(
        db_session,
        suffix=f"expired-{expired_lease}",
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    github = FakeGitHub()
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    fence = await _fence(db_factory, run)
    original_remote_ref_sha = git.remote_ref_sha
    remote_reads = 0

    async def expire_during_last_preflight(subject, branch):
        nonlocal remote_reads
        result = await original_remote_ref_sha(subject, branch)
        remote_reads += 1
        # Read 1 proves base, read 2 observes the absent delivery branch, and
        # read 3 is the repeated base proof immediately before the push fence.
        if remote_reads == 3:
            async with db_factory() as db:
                if expired_lease == "run":
                    row = await db.get(DeliveryRun, run.id, populate_existing=True)
                else:
                    row = await db.get(
                        DeliveryAction,
                        fence.action_id,
                        populate_existing=True,
                    )
                assert row is not None
                row.lease_expires_at = datetime.utcnow() - timedelta(seconds=1)
                await db.commit()
        return result

    git.remote_ref_sha = expire_during_last_preflight

    with pytest.raises(DeliverySubjectChanged, match="lease or action fence"):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=fence,
        )

    assert remote_reads == 3
    assert git.push_calls == 0
    assert git.remote_refs[run.delivery_branch] is None
    assert github.list_calls == 1
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_controller_takeover_after_push_prevents_pr_creation(
    db_session,
    db_factory,
):
    """A stale Controller may finish an idempotent push but no later effect."""

    run, _project, repo = await _delivery_scope(db_session, suffix="push-takeover")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    github = FakeGitHub()
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    fence = await _fence(db_factory, run)
    original_push_exact = git.push_exact

    async def push_then_take_over(subject):
        await original_push_exact(subject)
        async with db_factory() as db:
            stored_run = await db.get(
                DeliveryRun,
                run.id,
                populate_existing=True,
            )
            action = await db.get(
                DeliveryAction,
                fence.action_id,
                populate_existing=True,
            )
            assert stored_run is not None
            assert action is not None
            stored_run.lease_owner = "replacement-controller"
            stored_run.controller_generation += 1
            stored_run.lease_expires_at = datetime.utcnow() + timedelta(minutes=5)
            action.lease_owner = "b" * 64
            action.lease_expires_at = datetime.utcnow() + timedelta(minutes=5)
            await db.commit()

    git.push_exact = push_then_take_over

    with pytest.raises(DeliverySubjectChanged, match="lease or action fence"):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=fence,
        )

    assert git.push_calls == 1
    assert git.remote_refs[run.delivery_branch] == HEAD
    assert github.list_calls == 2
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_wrong_action_token_is_refused_before_remote_access(
    db_session,
    db_factory,
):
    run, _project, repo = await _delivery_scope(db_session, suffix="wrong-token")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    github = FakeGitHub()
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    fence = replace(await _fence(db_factory, run), action_token="b" * 64)

    with pytest.raises(DeliverySubjectChanged, match="lease or action fence"):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=fence,
        )

    assert git.verify_calls == 0
    assert git.push_calls == 0
    assert github.list_calls == 0
    assert github.create_calls == 0


@pytest.mark.asyncio
async def test_cancelled_ambiguous_push_is_recovered_by_next_controller(
    db_session,
    db_factory,
):
    """Cancellation never continues to PR creation; the next lease recovers."""

    run, _project, repo = await _delivery_scope(db_session, suffix="push-cancel")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: None})
    created = _pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)
    github = FakeGitHub(created_payload=created)
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)

    async def push_then_cancel(subject):
        git.push_calls += 1
        git.remote_refs[subject.delivery_branch] = subject.head_sha
        raise asyncio.CancelledError

    git.push_exact = push_then_cancel
    with pytest.raises(asyncio.CancelledError):
        await publisher.ensure_pull_request(
            run_id=run.id,
            idempotency_key=_key(run),
            fence=await _fence(db_factory, run),
        )

    assert git.remote_refs[run.delivery_branch] == HEAD
    assert github.list_calls == 1
    assert github.create_calls == 0

    published = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )

    assert published.pr_number == created["number"]
    assert git.push_calls == 1
    assert github.create_calls == 1


@pytest.mark.asyncio
async def test_monitor_creation_is_exact_and_idempotent(db_session, db_factory):
    run, _project, repo = await _delivery_scope(db_session, suffix="monitor")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    pr_payload = _pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)
    github = FakeGitHub(pulls=[pr_payload])
    create_calls = 0

    async def create_review(db, monitored_repo, pr_data):
        nonlocal create_calls
        create_calls += 1
        assert pr_data["base_ref"] == run.base_branch
        review = PRReview(
            repo_id=monitored_repo.id,
            pr_number=pr_data["number"],
            base_ref=pr_data["base_ref"],
            base_sha=pr_data["base_sha"],
            head_sha=pr_data["head_sha"],
            delivery_id=pr_data["delivery_id"],
            pr_title=pr_data["title"],
            pr_author=pr_data["author"],
            pr_url=pr_data["url"],
            status="waiting_ci",
            action_nonce="6" * 48,
        )
        db.add(review)
        await db.flush()
        await attach_review_to_run(
            db,
            repo=monitored_repo,
            review=review,
            pr_data=pr_data,
        )
        return review

    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=git,
        github=github,
        review_creator=create_review,
    )
    pull_request = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )

    first = await publisher.ensure_monitor(
        run_id=run.id,
        pull_request=pull_request,
        idempotency_key=_key(run, monitor=True),
        fence=await _fence(db_factory, run),
    )
    second = await publisher.ensure_monitor(
        run_id=run.id,
        pull_request=pull_request,
        idempotency_key=_key(run, monitor=True),
        fence=await _fence(db_factory, run),
    )

    assert second == first
    assert create_calls == 1
    async with db_factory() as db:
        reviews = list((await db.execute(select(PRReview))).scalars())
        assert len(reviews) == 1
        assert reviews[0].base_ref == run.base_branch
        assert reviews[0].base_sha == BASE
        assert reviews[0].head_sha == HEAD
        assert reviews[0].monitor_run_id == first


@pytest.mark.asyncio
async def test_monitor_creator_isolates_same_sha_review_for_different_base_ref(
    db_session,
    db_factory,
):
    """A webhook Review for another target branch is never adopted."""

    run, _project, repo = await _delivery_scope(
        db_session,
        suffix="monitor-base-ref-isolation",
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    pr_payload = _pull(
        branch=run.delivery_branch,
        repo_full_name=repo.repo_full_name,
    )
    github = FakeGitHub(pulls=[pr_payload])
    wrong_ref_review = PRReview(
        repo_id=repo.id,
        pr_number=pr_payload["number"],
        base_ref="release/1.x",
        base_sha=BASE,
        head_sha=HEAD,
        delivery_id="github-webhook-other-base",
        pr_title=pr_payload["title"],
        pr_author=pr_payload["user"]["login"],
        pr_url=pr_payload["html_url"],
        status="waiting_ci",
        action_nonce="5" * 48,
    )
    db_session.add(wrong_ref_review)
    await db_session.commit()

    created_payloads: list[dict] = []

    async def create_review(db, monitored_repo, pr_data):
        created_payloads.append(dict(pr_data))
        review = PRReview(
            repo_id=monitored_repo.id,
            pr_number=pr_data["number"],
            base_ref=pr_data["base_ref"],
            base_sha=pr_data["base_sha"],
            head_sha=pr_data["head_sha"],
            delivery_id=pr_data["delivery_id"],
            pr_title=pr_data["title"],
            pr_author=pr_data["author"],
            pr_url=pr_data["url"],
            status="waiting_ci",
            action_nonce="6" * 48,
        )
        db.add(review)
        await db.flush()
        await attach_review_to_run(
            db,
            repo=monitored_repo,
            review=review,
            pr_data=pr_data,
        )
        return review

    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=git,
        github=github,
        review_creator=create_review,
    )
    pull_request = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )
    monitor_id = await publisher.ensure_monitor(
        run_id=run.id,
        pull_request=pull_request,
        idempotency_key=_key(run, monitor=True),
        fence=await _fence(db_factory, run),
    )

    assert len(created_payloads) == 1
    assert created_payloads[0]["base_ref"] == "main"
    assert created_payloads[0]["base_sha"] == BASE
    assert created_payloads[0]["head_sha"] == HEAD
    async with db_factory() as db:
        reviews = list(
            (
                await db.execute(
                    select(PRReview)
                    .where(PRReview.repo_id == repo.id)
                    .order_by(PRReview.id.asc())
                )
            ).scalars()
        )
        assert len(reviews) == 2
        other_base, exact = reviews
        assert (
            other_base.base_ref,
            other_base.base_sha,
            other_base.head_sha,
            other_base.delivery_id,
            other_base.monitor_run_id,
        ) == (
            "release/1.x",
            BASE,
            HEAD,
            "github-webhook-other-base",
            None,
        )
        assert (
            exact.base_ref,
            exact.base_sha,
            exact.head_sha,
            exact.delivery_id,
            exact.monitor_run_id,
        ) == (
            "main",
            BASE,
            HEAD,
            f"delivery:{run.id}:{HEAD}",
            monitor_id,
        )


@pytest.mark.asyncio
async def test_webhook_review_is_atomically_adopted_before_cancelled_run_policy_drift(
    db_session,
    db_factory,
):
    """A webhook race can never turn a Delivery review into Merge Queue work."""

    run, _project, repo = await _delivery_scope(db_session, suffix="webhook-race")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    payload = _pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)
    github = FakeGitHub(pulls=[payload])

    # Model the opened webhook winning the exact Review natural key before the
    # Delivery publisher gets to create its reserved marker.
    review = PRReview(
        repo_id=repo.id,
        pr_number=payload["number"],
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        delivery_id="github-webhook-delivery-opaque",
        pr_title=payload["title"],
        pr_author=payload["user"]["login"],
        pr_url=payload["html_url"],
        status="reviewing",
        action_nonce="d" * 48,
        ci_status="passed",
    )
    db_session.add(review)
    await db_session.flush()
    monitor = await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data={
            "head_repo_full_name": repo.repo_full_name,
            "head_branch": run.delivery_branch,
        },
    )

    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    published = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )
    adopted_monitor_id = await publisher.ensure_monitor(
        run_id=run.id,
        pull_request=published,
        idempotency_key=_key(run, monitor=True),
        fence=await _fence(db_factory, run),
    )
    assert adopted_monitor_id == monitor.id

    # Once the Run is cancelled the repository-wide freeze is released.  Even
    # if an administrator then enables automatic Merge Queue, the immutable
    # Review marker must continue to enforce the Delivery no-merge policy.
    async with db_factory() as db:
        stored_run = await db.get(DeliveryRun, run.id, populate_existing=True)
        stored_repo = await db.get(MonitoredRepo, repo.id, populate_existing=True)
        stored_review = await db.get(PRReview, review.id, populate_existing=True)
        stored_monitor = await db.get(
            PRMonitorRun,
            adopted_monitor_id,
            populate_existing=True,
        )
        assert stored_review.delivery_id == f"delivery:{run.id}:{HEAD}"
        stored_run.phase = "done"
        stored_run.activity = "terminal"
        stored_run.outcome = "cancelled"
        stored_run.completed_at = datetime.utcnow()
        stored_repo.merge_queue_mode = "auto"
        stored_review.status = "approved"
        stored_review.action_taken = "lgtm_comment"
        stored_monitor.status = "reviewing"
        await db.commit()

    async with db_factory() as db:
        await record_gate_pass(db, review.id)

    async with db_factory() as db:
        stored_monitor = await db.get(
            PRMonitorRun,
            adopted_monitor_id,
            populate_existing=True,
        )
        actions = list((await db.execute(select(PRMergeQueueAction))).scalars())
        assert stored_monitor.status == "ready_to_merge"
        assert actions == []


@pytest.mark.asyncio
async def test_webhook_adoption_serializes_with_legacy_finding_action_writer(
    concurrent_db_factory,
    monkeypatch,
):
    """The repo writer fence makes adoption and legacy effects indivisible."""

    db_factory = concurrent_db_factory
    db_session = db_factory()
    run, _project, repo = await _delivery_scope(
        db_session,
        suffix="adoption-writer-race",
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    payload = _pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)
    github = FakeGitHub(pulls=[payload])
    review = PRReview(
        repo_id=repo.id,
        pr_number=payload["number"],
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        delivery_id="github-webhook-adoption-writer-race",
        pr_title=payload["title"],
        pr_author=payload["user"]["login"],
        pr_url=payload["html_url"],
        status="reviewing",
        action_nonce="b" * 48,
        ci_status="passed",
    )
    db_session.add(review)
    await db_session.flush()
    monitor = await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data={
            "head_repo_full_name": repo.repo_full_name,
            "head_branch": run.delivery_branch,
        },
    )
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="completed",
        prompt_policy_hash="c" * 64,
        guide_pack_hash="d" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="e" * 64,
        role=reviewer.role,
        severity="high",
        category="concurrency",
        path="backend/example.py",
        line=7,
        title="Writer ordering must be stable",
        evidence="Adoption and finding effects share one repository.",
        impact="A legacy action could escape Delivery policy.",
        required_fix="Use the common repository writer fence.",
        test="Race adoption against an immediate finding action.",
        thread_nonce="f" * 48,
        base_sha=BASE,
        head_sha=HEAD,
    )
    db_session.add(finding)
    await db_session.commit()
    await db_session.close()

    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    published = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )
    monitor_fence = await _fence(db_factory, run)
    publisher_has_writer_fence = asyncio.Event()
    release_publisher = asyncio.Event()
    legacy_writer_attempted = asyncio.Event()
    original_lock = pr_review_actions_service.lock_pr_repo_action_boundary

    async def pause_publisher_after_lock(db, repo_id):
        locked_repo = await original_lock(db, repo_id)
        publisher_has_writer_fence.set()
        await release_publisher.wait()
        return locked_repo

    async def observe_legacy_lock_attempt(db, repo_id):
        legacy_writer_attempted.set()
        return await original_lock(db, repo_id)

    monkeypatch.setattr(
        delivery_publisher_service,
        "lock_pr_repo_action_boundary",
        pause_publisher_after_lock,
    )
    monkeypatch.setattr(
        pr_review_actions_service,
        "lock_pr_repo_action_boundary",
        observe_legacy_lock_attempt,
    )

    adoption = asyncio.create_task(
        publisher.ensure_monitor(
            run_id=run.id,
            pull_request=published,
            idempotency_key=_key(run, monitor=True),
            fence=monitor_fence,
        )
    )
    await asyncio.wait_for(publisher_has_writer_fence.wait(), timeout=5)

    async def create_legacy_action():
        async with db_factory() as db:
            return await pr_review_actions_service.create_immediate_finding_action(
                db,
                finding_id=finding.id,
                review_id=review.id,
                action_type="human_advice",
                idempotency_key="adoption-writer-race-action",
                actor_user_id=None,
                human_advice="This must lose to Delivery adoption.",
            )

    legacy_action = asyncio.create_task(create_legacy_action())
    await asyncio.wait_for(legacy_writer_attempted.wait(), timeout=5)
    assert not legacy_action.done()
    release_publisher.set()

    assert await asyncio.wait_for(adoption, timeout=5) == monitor.id
    with pytest.raises(FindingActionConflict, match="Delivery-owned"):
        await asyncio.wait_for(legacy_action, timeout=5)

    async with db_factory() as db:
        stored_review = await db.get(PRReview, review.id, populate_existing=True)
        actions = list((await db.execute(select(PRFindingAction))).scalars())
        assert stored_review is not None
        assert stored_review.delivery_id == f"delivery:{run.id}:{HEAD}"
        assert actions == []


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_effect", ["finding_action", "rebuttal"])
async def test_webhook_review_with_legacy_finding_effect_is_not_adopted(
    db_session,
    db_factory,
    legacy_effect,
):
    """Delivery ownership cannot be applied after a legacy write escaped."""

    run, project, repo = await _delivery_scope(
        db_session,
        suffix=f"legacy-{legacy_effect}",
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    payload = _pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)
    github = FakeGitHub(pulls=[payload])
    developer = Task(
        title=f"Legacy developer {legacy_effect}",
        description="Existing legacy PR Monitor owner",
        status="pending",
        project_id=project.id,
    )
    db_session.add(developer)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=payload["number"],
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        delivery_id=f"github-webhook-{legacy_effect}",
        pr_title=payload["title"],
        pr_author=payload["user"]["login"],
        pr_url=payload["html_url"],
        status="reviewing",
        action_nonce="e" * 48,
        ci_status="passed",
    )
    db_session.add(review)
    await db_session.flush()
    monitor = await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data={
            "head_repo_full_name": repo.repo_full_name,
            "head_branch": run.delivery_branch,
        },
    )
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="completed",
        prompt_policy_hash="5" * 64,
        guide_pack_hash="6" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="7" * 64,
        role=reviewer.role,
        severity="high",
        category="correctness",
        path="backend/example.py",
        line=12,
        title="Legacy finding already acted upon",
        evidence="A legacy workflow already produced a durable effect.",
        impact="Changing ownership now would hide the escaped effect.",
        required_fix="Keep the review under legacy ownership.",
        test="Delivery adoption must fail closed.",
        thread_nonce="8" * 48,
        base_sha=BASE,
        head_sha=HEAD,
    )
    db_session.add(finding)
    await db_session.flush()
    if legacy_effect == "finding_action":
        db_session.add(
            PRFindingAction(
                finding_id=finding.id,
                action_type="human_advice",
                status="completed",
                idempotency_key=f"legacy-{review.id}-action",
                human_advice="This action predates Delivery ownership.",
                expected_head_sha=HEAD,
            )
        )
    else:
        db_session.add(
            PRFindingRebuttal(
                finding_id=finding.id,
                pr_review_id=review.id,
                monitor_run_id=monitor.id,
                developer_task_id=developer.id,
                attempt=1,
                base_sha=BASE,
                head_sha=HEAD,
                evidence="This rebuttal predates Delivery ownership.",
                evidence_hash="9" * 64,
                status="pending",
                resolution_nonce="a" * 48,
            )
        )
    await db_session.commit()

    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    published = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )

    with pytest.raises(
        DeliveryPublisherPermanentError,
        match="legacy Finding effect",
    ):
        await publisher.ensure_monitor(
            run_id=run.id,
            pull_request=published,
            idempotency_key=_key(run, monitor=True),
            fence=await _fence(db_factory, run),
        )

    async with db_factory() as db:
        stored_review = await db.get(PRReview, review.id, populate_existing=True)
        stored_monitor = await db.get(
            PRMonitorRun,
            monitor.id,
            populate_existing=True,
        )
        assert stored_review is not None
        assert stored_review.delivery_id == f"github-webhook-{legacy_effect}"
        assert stored_monitor is not None
        assert stored_monitor.current_review_id == review.id


@pytest.mark.asyncio
async def test_webhook_review_with_active_legacy_publication_is_not_adopted(
    db_session,
    db_factory,
):
    """Delivery cannot revoke an already-claimed legacy GitHub effect."""

    run, _project, repo = await _delivery_scope(
        db_session,
        suffix="legacy-publication",
    )
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    payload = _pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)
    github = FakeGitHub(pulls=[payload])
    review = PRReview(
        repo_id=repo.id,
        pr_number=payload["number"],
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        delivery_id="github-webhook-legacy-publication",
        pr_title=payload["title"],
        pr_author=payload["user"]["login"],
        pr_url=payload["html_url"],
        status="publishing",
        action_nonce="1" * 48,
        pending_action="approved_merged",
        pending_review_body="Legacy publication is already claimed.",
        publishing_actor="legacy-bot",
        publishing_retry_count=0,
        publishing_task_started_at=datetime.utcnow(),
        publishing_started_at=datetime.utcnow(),
        publishing_lease_token="2" * 48,
        publishing_lease_expires_at=datetime.utcnow() + timedelta(minutes=5),
        ci_status="passed",
    )
    db_session.add(review)
    await db_session.flush()
    await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data={
            "head_repo_full_name": repo.repo_full_name,
            "head_branch": run.delivery_branch,
        },
    )
    await db_session.commit()

    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    published = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )

    with pytest.raises(
        DeliveryPublisherPermanentError,
        match="legacy publication effect",
    ):
        await publisher.ensure_monitor(
            run_id=run.id,
            pull_request=published,
            idempotency_key=_key(run, monitor=True),
            fence=await _fence(db_factory, run),
        )

    async with db_factory() as db:
        stored = await db.get(PRReview, review.id, populate_existing=True)
        assert stored is not None
        assert stored.delivery_id == "github-webhook-legacy-publication"
        assert stored.status == "publishing"


@pytest.mark.asyncio
async def test_new_cycle_head_creates_review_and_advances_same_monitor_run(
    db_session, db_factory
):
    """One Delivery PR keeps one Monitor Run while immutable Reviews advance."""

    run, _project, repo = await _delivery_scope(db_session, suffix="monitor-head2")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    github = FakeGitHub(
        pulls=[_pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)]
    )
    create_calls = 0

    async def create_review(db, monitored_repo, pr_data):
        nonlocal create_calls
        create_calls += 1
        review = PRReview(
            repo_id=monitored_repo.id,
            pr_number=pr_data["number"],
            base_ref=pr_data["base_ref"],
            base_sha=pr_data["base_sha"],
            head_sha=pr_data["head_sha"],
            delivery_id=pr_data["delivery_id"],
            pr_title=pr_data["title"],
            pr_author=pr_data["author"],
            pr_url=pr_data["url"],
            status="waiting_ci",
            action_nonce=f"{create_calls:048x}",
            ci_status="pending",
            review_summary="Waiting for exact-head CI before starting reviewers",
        )
        db.add(review)
        await db.flush()
        await attach_review_to_run(
            db,
            repo=monitored_repo,
            review=review,
            pr_data=pr_data,
        )
        return review

    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=git,
        github=github,
        review_creator=create_review,
    )

    head1_pr = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )
    monitor_id = await publisher.ensure_monitor(
        run_id=run.id,
        pull_request=head1_pr,
        idempotency_key=_key(run, monitor=True),
        fence=await _fence(db_factory, run),
    )

    # Mirror the real boundary after head1 was commented/blocked, then the
    # controller completed another Plan/Code/Review cycle on the same branch.
    async with db_factory() as db:
        stored_run = await db.get(DeliveryRun, run.id, populate_existing=True)
        monitor = await db.get(PRMonitorRun, monitor_id, populate_existing=True)
        old_review = await db.get(
            PRReview, monitor.current_review_id, populate_existing=True
        )
        old_review.status = "commented"
        old_review.action_taken = "review_comments"
        old_review.review_summary = "Fix the exact-head finding"
        monitor.status = "waiting_for_fix"
        monitor.pause_reason = None
        db.add(
            PRRepairWake(
                monitor_run_id=monitor.id,
                review_id=old_review.id,
                developer_task_id=None,
                trigger_base_sha=BASE,
                trigger_head_sha=HEAD,
                reason_kind="review_blocked",
                evidence_hash="a" * 64,
                evidence={"subject": {"base_sha": BASE, "head_sha": HEAD}},
                status="shadow",
                attempt=1,
                delivery_token="b" * 48,
            )
        )
        stored_run.pr_number = head1_pr.pr_number
        stored_run.pr_url = head1_pr.url
        stored_run.pr_monitor_run_id = monitor.id
        stored_run.head_sha = HEAD2
        stored_run.head_tree_sha = TREE2
        stored_run.patch_sha256 = PATCH2
        stored_run.head_generation += 1
        await db.commit()
        run_head2 = stored_run

    git.expected_head = HEAD2
    git.expected_tree = TREE2
    git.expected_patch = PATCH2
    original_push_exact = git.push_exact

    async def push_and_advance_bound_pr(subject):
        result = await original_push_exact(subject)
        # GitHub advances the already-open PR when its bound branch moves.
        github.pulls[0]["head"]["sha"] = subject.head_sha
        return result

    git.push_exact = push_and_advance_bound_pr

    head2_pr = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run_head2),
        fence=await _fence(db_factory, run_head2),
    )
    advanced_id = await publisher.ensure_monitor(
        run_id=run.id,
        pull_request=head2_pr,
        idempotency_key=_key(run_head2, monitor=True),
        fence=await _fence(db_factory, run_head2),
    )
    repeated_id = await publisher.ensure_monitor(
        run_id=run.id,
        pull_request=head2_pr,
        idempotency_key=_key(run_head2, monitor=True),
        fence=await _fence(db_factory, run_head2),
    )

    assert advanced_id == monitor_id
    assert repeated_id == monitor_id
    assert create_calls == 2
    assert git.push_calls == 1
    assert git.remote_refs[run.delivery_branch] == HEAD2
    assert head2_pr.head_sha == HEAD2
    async with db_factory() as db:
        reviews = list(
            (
                await db.execute(
                    select(PRReview)
                    .where(PRReview.repo_id == repo.id)
                    .order_by(PRReview.id)
                )
            ).scalars()
        )
        monitors = list(
            (
                await db.execute(
                    select(PRMonitorRun).where(PRMonitorRun.repo_id == repo.id)
                )
            ).scalars()
        )
        wake = (
            await db.execute(
                select(PRRepairWake).where(PRRepairWake.monitor_run_id == monitor_id)
            )
        ).scalar_one()

        assert [(item.head_sha, item.monitor_run_id) for item in reviews] == [
            (HEAD, monitor_id),
            (HEAD2, monitor_id),
        ]
        assert len(monitors) == 1
        assert monitors[0].id == monitor_id
        assert monitors[0].current_review_id == reviews[1].id
        assert monitors[0].current_base_sha == BASE
        assert monitors[0].current_head_sha == HEAD2
        assert monitors[0].head_repo_full_name == repo.repo_full_name
        assert monitors[0].head_branch == run.delivery_branch
        assert monitors[0].status == "waiting_ci"
        assert wake.status == "superseded"


@pytest.mark.asyncio
async def test_merged_verifier_uses_exact_nonce_without_requiring_live_refs(
    db_session,
    db_factory,
    monkeypatch,
):
    (
        run,
        repo,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
        nonce,
    ) = await _merged_delivery_scope(
        db_session,
        db_factory,
        suffix="merged-exact",
    )
    evidence_calls: list[dict] = []

    async def exact_merge_evidence(
        *,
        repo_name,
        pr_number,
        base_ref,
        base_sha,
        head_sha,
        nonce,
        actor,
        publishing_started_at,
        merge_method,
    ):
        # Keep this explicit signature in the regression: unlike a permissive
        # AsyncMock, it raises immediately if Delivery drops the frozen base_ref
        # argument while wiring the real merge-evidence helper.
        evidence_calls.append(
            {
                "repo_name": repo_name,
                "pr_number": pr_number,
                "base_ref": base_ref,
                "base_sha": base_sha,
                "head_sha": head_sha,
                "nonce": nonce,
                "actor": actor,
                "publishing_started_at": publishing_started_at,
                "merge_method": merge_method,
            }
        )
        return True

    monkeypatch.setattr(
        delivery_publisher_service,
        "_find_merge_evidence",
        exact_merge_evidence,
    )
    # Repository edits affect only newly admitted Runs.  This exact Run keeps
    # the auto-merge terminal frozen in its hashed policy snapshot.
    repo.auto_merge = False
    await db_session.commit()

    verified = await publisher.verify_merged(
        run_id=run.id,
        pull_request=pull_request,
        monitor_run_id=monitor_id,
        expected_monitor_state_version=monitor_version,
    )

    assert verified == pull_request
    assert evidence_calls == [
        {
            "repo_name": repo.repo_full_name,
            "pr_number": 17,
            "base_ref": "main",
            "base_sha": BASE,
            "head_sha": HEAD,
            "nonce": nonce,
            "actor": "ccm-bot",
            "publishing_started_at": MERGE_PUBLISHED_AT,
            "merge_method": "fast-forward",
        }
    ]


@pytest.mark.asyncio
async def test_merged_verifier_rejects_same_sha_review_for_different_base_ref(
    db_session,
    db_factory,
    monkeypatch,
):
    (
        run,
        _repo,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
        _nonce,
    ) = await _merged_delivery_scope(
        db_session,
        db_factory,
        suffix="merged-other-base-ref",
    )
    async with db_factory() as db:
        monitor = await db.get(PRMonitorRun, monitor_id, populate_existing=True)
        review = await db.get(
            PRReview,
            monitor.current_review_id,
            populate_existing=True,
        )
        assert (review.base_sha, review.head_sha) == (BASE, HEAD)
        review.base_ref = "release/1.x"
        await db.commit()

    evidence = AsyncMock(side_effect=AssertionError("remote evidence must not run"))
    monkeypatch.setattr(
        delivery_publisher_service,
        "_find_merge_evidence",
        evidence,
    )

    with pytest.raises(DeliverySubjectChanged, match="merged snapshot"):
        await publisher.verify_merged(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )
    evidence.assert_not_awaited()


@pytest.mark.asyncio
async def test_merged_verifier_rejects_missing_remote_merge_evidence(
    db_session,
    db_factory,
    monkeypatch,
):
    (
        run,
        _repo,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
        _nonce,
    ) = await _merged_delivery_scope(
        db_session,
        db_factory,
        suffix="merged-missing",
    )
    monkeypatch.setattr(
        delivery_publisher_service,
        "_find_merge_evidence",
        AsyncMock(return_value=False),
    )

    with pytest.raises(DeliverySubjectChanged, match="merge evidence"):
        await publisher.verify_merged(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )


@pytest.mark.asyncio
async def test_merged_verifier_pauses_for_terminal_evidence_mismatch(
    db_session,
    db_factory,
    monkeypatch,
):
    (
        run,
        _repo,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
        _nonce,
    ) = await _merged_delivery_scope(
        db_session,
        db_factory,
        suffix="merged-terminal-mismatch",
    )
    monkeypatch.setattr(
        delivery_publisher_service,
        "_find_merge_evidence",
        AsyncMock(
            side_effect=GhError(
                "GitHub merge commit evidence is malformed or mismatched"
            )
        ),
    )

    with pytest.raises(DeliverySubjectChanged, match="evidence is invalid"):
        await publisher.verify_merged(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )


@pytest.mark.asyncio
async def test_merged_verifier_preserves_transient_evidence_error_for_retry(
    db_session,
    db_factory,
    monkeypatch,
):
    (
        run,
        _repo,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
        _nonce,
    ) = await _merged_delivery_scope(
        db_session,
        db_factory,
        suffix="merged-transient-error",
    )
    transient = GhError("GitHub API request failed: HTTP 503")
    monkeypatch.setattr(
        delivery_publisher_service,
        "_find_merge_evidence",
        AsyncMock(side_effect=transient),
    )

    # GhError is deliberately not converted to DeliverySubjectChanged: the
    # controller's generic fault path leaves the Run waiting for a retry.
    with pytest.raises(GhError) as caught:
        await publisher.verify_merged(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )
    assert caught.value is transient


@pytest.mark.asyncio
async def test_merged_verifier_rechecks_exact_monitor_generation(
    db_session,
    db_factory,
    monkeypatch,
):
    (
        run,
        _repo,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
        _nonce,
    ) = await _merged_delivery_scope(
        db_session,
        db_factory,
        suffix="merged-race",
    )

    async def race_merge_evidence(**_kwargs):
        async with db_factory() as db:
            monitor = await db.get(
                PRMonitorRun,
                monitor_id,
                populate_existing=True,
            )
            monitor.state_version += 1
            await db.commit()
        return True

    monkeypatch.setattr(
        delivery_publisher_service,
        "_find_merge_evidence",
        AsyncMock(side_effect=race_merge_evidence),
    )

    with pytest.raises(DeliverySubjectChanged, match="merged snapshot"):
        await publisher.verify_merged(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )


@pytest.mark.asyncio
async def test_ready_verifier_proves_exact_remote_pr_without_writes(
    db_session, db_factory
):
    (
        run,
        repo,
        _git,
        _github,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
    ) = await _ready_delivery_scope(db_session, db_factory, suffix="ready-exact")
    # Repository edits affect only newly admitted Runs.  This exact Run keeps
    # the manual terminal frozen in its hashed policy snapshot.
    repo.auto_merge = True
    await db_session.commit()

    verified = await publisher.verify_ready_to_merge(
        run_id=run.id,
        pull_request=pull_request,
        monitor_run_id=monitor_id,
        expected_monitor_state_version=monitor_version,
    )

    assert verified == pull_request
    async with db_factory() as db:
        monitors = list(
            (
                await db.execute(
                    select(PRMonitorRun).where(PRMonitorRun.repo_id == repo.id)
                )
            ).scalars()
        )
        reviews = list(
            (
                await db.execute(
                    select(PRReview).where(PRReview.repo_id == repo.id)
                )
            ).scalars()
        )
        assert len(monitors) == 1
        assert len(reviews) == 1
        assert monitors[0].id == monitor_id
        assert monitors[0].state_version == monitor_version
        assert monitors[0].status == "ready_to_merge"
        assert reviews[0].id == monitors[0].current_review_id


@pytest.mark.asyncio
async def test_ready_verifier_rejects_same_sha_review_for_different_base_ref(
    db_session,
    db_factory,
):
    (
        run,
        _repo,
        git,
        github,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
    ) = await _ready_delivery_scope(
        db_session,
        db_factory,
        suffix="ready-other-local-base-ref",
    )
    async with db_factory() as db:
        monitor = await db.get(PRMonitorRun, monitor_id, populate_existing=True)
        review = await db.get(
            PRReview,
            monitor.current_review_id,
            populate_existing=True,
        )
        assert (review.base_sha, review.head_sha) == (BASE, HEAD)
        review.base_ref = "release/1.x"
        await db.commit()
    verify_calls = git.verify_calls
    get_calls = github.get_calls

    with pytest.raises(DeliverySubjectChanged, match="ready snapshot"):
        await publisher.verify_ready_to_merge(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )

    assert git.verify_calls == verify_calls
    assert github.get_calls == get_calls


@pytest.mark.asyncio
async def test_ready_verifier_rejects_remote_retarget_with_same_shas(
    db_session,
    db_factory,
):
    (
        run,
        repo,
        _git,
        github,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
    ) = await _ready_delivery_scope(
        db_session,
        db_factory,
        suffix="ready-remote-retarget",
    )
    github.pulls[:] = [
        _pull(
            branch=run.delivery_branch,
            repo_full_name=repo.repo_full_name,
            base_branch="release/1.x",
            base_sha=BASE,
            head_sha=HEAD,
        )
    ]

    with pytest.raises(DeliverySubjectChanged, match="locally ready subject"):
        await publisher.verify_ready_to_merge(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )


@pytest.mark.asyncio
async def test_ready_verifier_rejects_delayed_webhook_when_remote_ref_is_head2(
    db_session, db_factory
):
    (
        run,
        _repo,
        git,
        _github,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
    ) = await _ready_delivery_scope(db_session, db_factory, suffix="ready-remote-h2")
    # The DB/webhook still says H1 is ready, but the source branch has already
    # advanced.  The verifier must not trust the local Monitor terminal state.
    git.remote_refs[run.delivery_branch] = HEAD2

    with pytest.raises(DeliverySubjectChanged, match="Remote delivery branch"):
        await publisher.verify_ready_to_merge(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )


@pytest.mark.asyncio
async def test_ready_verifier_rejects_remote_base_advance(
    db_session, db_factory
):
    (
        run,
        _repo,
        git,
        _github,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
    ) = await _ready_delivery_scope(db_session, db_factory, suffix="ready-base-h2")
    git.remote_refs["main"] = HEAD2

    with pytest.raises(DeliveryPublisherPermanentError, match="Remote base branch"):
        await publisher.verify_ready_to_merge(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )


@pytest.mark.asyncio
async def test_ready_verifier_rejects_github_pr_head2_before_webhook_arrives(
    db_session, db_factory
):
    (
        run,
        repo,
        _git,
        github,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
    ) = await _ready_delivery_scope(db_session, db_factory, suffix="ready-pr-h2")
    github.pulls[:] = [
        _pull(
            branch=run.delivery_branch,
            repo_full_name=repo.repo_full_name,
            head_sha=HEAD2,
        )
    ]

    with pytest.raises(DeliverySubjectChanged, match="locally ready subject"):
        await publisher.verify_ready_to_merge(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )


@pytest.mark.asyncio
async def test_ready_verifier_requires_github_pr_to_still_be_open(
    db_session, db_factory
):
    (
        run,
        _repo,
        _git,
        github,
        publisher,
        pull_request,
        monitor_id,
        monitor_version,
    ) = await _ready_delivery_scope(db_session, db_factory, suffix="ready-closed")
    github.pulls[0] = {**github.pulls[0], "state": "closed"}

    with pytest.raises(DeliverySubjectChanged, match="locally ready subject"):
        await publisher.verify_ready_to_merge(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )


@pytest.mark.asyncio
async def test_ready_verifier_rechecks_monitor_version_after_remote_read(
    db_session, db_factory
):
    (
        run,
        repo,
        git,
        github,
        _publisher,
        pull_request,
        monitor_id,
        monitor_version,
    ) = await _ready_delivery_scope(db_session, db_factory, suffix="ready-monitor-race")

    class RacingGitHub(FakeGitHub):
        async def get_pull_request(self, **kwargs):
            value = await super().get_pull_request(**kwargs)
            async with db_factory() as db:
                monitor = await db.get(
                    PRMonitorRun, monitor_id, populate_existing=True
                )
                monitor.status = "waiting_ci"
                monitor.state_version += 1
                await db.commit()
            return value

    racing_github = RacingGitHub(pulls=list(github.pulls))
    publisher = GitHubDeliveryPublisher(
        db_factory,
        git=git,
        github=racing_github,
    )

    with pytest.raises(DeliverySubjectChanged, match="ready snapshot"):
        await publisher.verify_ready_to_merge(
            run_id=run.id,
            pull_request=pull_request,
            monitor_run_id=monitor_id,
            expected_monitor_state_version=monitor_version,
        )

    async with db_factory() as db:
        reviews = list(
            (
                await db.execute(
                    select(PRReview).where(PRReview.repo_id == repo.id)
                )
            ).scalars()
        )
        assert len(reviews) == 1


@pytest.mark.asyncio
async def test_monitor_refuses_pull_request_argument_from_another_branch(
    db_session, db_factory
):
    run, _project, repo = await _delivery_scope(db_session, suffix="monitor-wrong")
    git = FakeGit(repo.repo_full_name, {"main": BASE, run.delivery_branch: HEAD})
    github = FakeGitHub(
        pulls=[_pull(branch=run.delivery_branch, repo_full_name=repo.repo_full_name)]
    )
    publisher = GitHubDeliveryPublisher(db_factory, git=git, github=github)
    exact = await publisher.ensure_pull_request(
        run_id=run.id,
        idempotency_key=_key(run),
        fence=await _fence(db_factory, run),
    )
    wrong = exact.__class__(
        repo_id=exact.repo_id,
        pr_number=exact.pr_number,
        url=exact.url,
        base_sha=exact.base_sha,
        head_sha=exact.head_sha,
        head_branch="wrong/branch",
        head_repo_full_name=exact.head_repo_full_name,
    )

    with pytest.raises(DeliveryPublisherPermanentError, match="does not match"):
        await publisher.ensure_monitor(
            run_id=run.id,
            pull_request=wrong,
            idempotency_key=_key(run, monitor=True),
            fence=await _fence(db_factory, run),
        )
