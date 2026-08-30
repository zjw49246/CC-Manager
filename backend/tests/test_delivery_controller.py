"""Recovery and exact-subject tests for the Delivery Controller."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from backend.config import settings
from backend.models.capability import CapabilityInvocation
from backend.models.delivery import (
    DeliveryAction,
    DeliveryCycle,
    DeliveryRun,
    DeliveryTurn,
)
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.plan import Plan, PlanVersion
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMonitorRun,
    PRRepairWake,
    PRReview,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessAttempt as HarnessAttempt,
    TestHarnessEvidence as HarnessEvidence,
    TestHarnessFinding as HarnessFinding,
    TestHarnessRun as HarnessRun,
)
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.delivery_controller import (
    CapabilitySnapshot,
    CoreDeliveryCapabilityGateway,
    DeliveryController,
    DeliveryEffectFence,
    DeliveryPublisherNoEffectPreflightError,
    DeliveryPublisherPermanentError,
    PublishedPullRequest,
)
from backend.services.capability_service import (
    CapabilityConflictError,
    CapabilityDisabledError,
    CapabilityValidationError,
    create_controller_invocation,
)
from backend.services.capability_registry import (
    register_capability,
    resolve_capability,
    unregister_capability,
)
from backend.services.delivery_reducer import DeliveryReducerEvent
from backend.services.delivery_progress import _stage_progress
from backend.services.delivery_service import (
    DeliveryConflictError,
    DeliveryCreateSpec,
    apply_run_event,
    create_delivery_run,
    lock_run,
)
from backend.services.delivery_workspace import (
    DeliveryWorkspaceError,
    DeliveryWorkspaceSnapshot,
)
from backend.services.dispatcher import GlobalDispatcher
from backend.services.plan_capability import plan_capability_definition


BASE_SHA = "1" * 40
HEAD_ONE = "2" * 40
TREE_ONE = "3" * 40
HEAD_TWO = "4" * 40
TREE_TWO = "5" * 40
PATCH_SHA = "6" * 64


@pytest.fixture
def registered_plan_capability():
    previous = resolve_capability("plan")
    register_capability(plan_capability_definition(), replace=True)
    yield
    if previous is None:
        unregister_capability("plan")
    else:
        register_capability(previous, replace=True)


class FakeWorkspace:
    def __init__(self) -> None:
        self.repo_path = "/srv/repos/delivery-controller"
        self.worktree_path = f"{self.repo_path}/.claude-manager/worktrees/delivery-1"
        self.branch = ""
        self.base_branch = "main"
        self.base_sha = BASE_SHA
        self.head_sha = BASE_SHA
        self.tree_sha = "7" * 40
        self.prepare_calls = 0
        self.inspect_calls = 0
        self.commit_calls = 0
        self.pending_head_sha: str | None = None
        self.pending_tree_sha: str | None = None
        self.last_commit_subject: tuple[str, int, int] | None = None
        self.changed_paths = ["frontend/src/App.tsx"]

    def advance(self, head: str, tree: str) -> None:
        self.pending_head_sha = head
        self.pending_tree_sha = tree

    def snapshot(self) -> DeliveryWorkspaceSnapshot:
        return DeliveryWorkspaceSnapshot(
            repo_path=self.repo_path,
            worktree_path=self.worktree_path,
            branch=self.branch,
            base_branch=self.base_branch,
            base_sha=self.base_sha,
            head_sha=self.head_sha,
            head_tree_sha=self.tree_sha,
        )

    async def list_changed_paths(self, *, worktree_path, base_sha, head_sha):
        assert worktree_path == self.worktree_path
        assert base_sha == self.base_sha
        assert head_sha == self.head_sha
        return list(self.changed_paths)

    async def prepare(
        self,
        *,
        repo_path,
        run_id,
        branch,
        base_branch,
        expected_repo_full_name,
    ):
        assert repo_path == self.repo_path
        assert expected_repo_full_name == "acme/delivery-controller"
        self.prepare_calls += 1
        self.branch = branch
        self.base_branch = base_branch
        self.worktree_path = (
            f"{self.repo_path}/.claude-manager/worktrees/delivery-{run_id}"
        )
        return self.snapshot()

    async def inspect(
        self,
        *,
        repo_path,
        worktree_path,
        branch,
        base_branch,
        expected_repo_full_name,
    ):
        assert repo_path == self.repo_path
        assert expected_repo_full_name == "acme/delivery-controller"
        assert worktree_path == self.worktree_path
        assert branch == self.branch
        assert base_branch == self.base_branch
        self.inspect_calls += 1
        return self.snapshot()

    async def commit_changes(
        self,
        *,
        repo_path,
        worktree_path,
        branch,
        base_branch,
        expected_head_sha,
        run_id,
        turn_generation,
        title,
        expected_repo_full_name,
    ):
        assert repo_path == self.repo_path
        assert expected_repo_full_name == "acme/delivery-controller"
        assert worktree_path == self.worktree_path
        assert branch == self.branch
        assert base_branch == self.base_branch
        assert run_id > 0
        assert turn_generation > 0
        assert title
        self.commit_calls += 1
        subject = (expected_head_sha, run_id, turn_generation)
        if expected_head_sha != self.head_sha:
            # Model DeliveryWorkspaceManager's trailer-based recovery after a
            # crash between the Git commit and durable Controller finalize.
            assert self.last_commit_subject == subject
            return self.snapshot()
        if self.pending_head_sha is not None:
            self.head_sha = self.pending_head_sha
            self.tree_sha = self.pending_tree_sha or self.tree_sha
            self.pending_head_sha = None
            self.pending_tree_sha = None
            self.last_commit_subject = subject
        return self.snapshot()


class FailingWorkspace(FakeWorkspace):
    async def prepare(
        self,
        *,
        repo_path,
        run_id,
        branch,
        base_branch,
        expected_repo_full_name,
    ):
        del repo_path, run_id, branch, base_branch, expected_repo_full_name
        raise DeliveryWorkspaceError("worktree ownership changed")


class FailingCapabilities:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def create(self, **kwargs):
        del kwargs
        raise self.error

    async def observe(self, invocation_id: int):
        del invocation_id
        raise AssertionError("observe must not run after create fails")

    async def consume(self, snapshot):
        del snapshot
        raise AssertionError("consume must not run after create fails")


class FakeCapabilities:
    def __init__(self, db_factory, *, review_verdicts=None) -> None:
        self.db_factory = db_factory
        self.review_verdicts = list(review_verdicts or ["approved"])
        self.snapshots: dict[int, CapabilitySnapshot] = {}
        self.created: list[tuple[str, str]] = []
        self.next_id = 1000

    async def create(
        self,
        *,
        task_id,
        capability_key,
        request_payload,
        idempotency_key,
    ) -> CapabilitySnapshot:
        for key, existing_key in self.created:
            if existing_key == idempotency_key:
                invocation_id = int(key.split(":", 1)[1])
                return self.snapshots[invocation_id]
        self.next_id += 1
        invocation_id = self.next_id
        if capability_key == "plan":
            async with self.db_factory() as db:
                plan = Plan(
                    title=request_payload["title"],
                    initial_request=request_payload["prompt"],
                    target_task_id=task_id,
                    pipeline_config=default_plan_pipeline_config().model_dump(),
                )
                db.add(plan)
                await db.flush()
                version = PlanVersion(
                    plan_id=plan.id,
                    version_number=1,
                    content=f"Approved fake plan {invocation_id}",
                    review_verdict="approve",
                    human_decision="pending",
                )
                db.add(version)
                await db.flush()
                plan.current_version_id = version.id
                await db.commit()
                result_id = version.id
            snapshot = CapabilitySnapshot(
                invocation_id=invocation_id,
                status="ready",
                state_version=3,
                result_kind="plan_version",
                result_id=result_id,
                result_hash="8" * 64,
            )
        else:
            verdict = self.review_verdicts.pop(0)
            async with self.db_factory() as db:
                task = await db.get(Task, task_id)
                run = await db.get(DeliveryRun, task.delivery_run_id)
                tree_sha = run.head_tree_sha
            snapshot = CapabilitySnapshot(
                invocation_id=invocation_id,
                status="ready",
                state_version=3,
                result_kind="code_review_result",
                result_id=invocation_id + 10_000,
                result_hash="9" * 64,
                verdict=verdict,
                summary=(
                    "Looks good" if verdict == "approved" else "Add regression test"
                ),
                findings=(
                    ()
                    if verdict == "approved"
                    else ({"severity": "high", "required_fix": "add a test"},)
                ),
                subject_ref={
                    "base_sha": request_payload["base_sha"],
                    "head_sha": request_payload["head_sha"],
                    "head_tree_sha": tree_sha,
                    "patch_sha256": PATCH_SHA,
                },
            )
        self.snapshots[invocation_id] = snapshot
        self.created.append((f"inv:{invocation_id}", idempotency_key))
        return snapshot

    async def observe(self, invocation_id: int) -> CapabilitySnapshot:
        return self.snapshots[invocation_id]

    async def consume(self, snapshot: CapabilitySnapshot) -> CapabilitySnapshot:
        consumed = replace(
            snapshot,
            status="completed",
            state_version=snapshot.state_version + 1,
        )
        self.snapshots[snapshot.invocation_id] = consumed
        return consumed


class FakeDispatcher:
    def __init__(self) -> None:
        self.wake_count = 0

    def wake(self) -> None:
        self.wake_count += 1


class FakeFrontendHarness:
    """Durable Harness double used to exercise the Delivery Browser gate."""

    def __init__(self, db_factory) -> None:
        self.db_factory = db_factory
        self.start_calls = 0
        self.cancel_calls = 0
        self.run_id = "f" * 32
        self.last_spec = None
        self.preview_config_overrides = []

    async def start_task_run(
        self,
        *,
        task_id,
        spec,
        owner_user_id=None,
        owner_identity,
        preview_config_override=None,
    ):
        del owner_user_id
        self.start_calls += 1
        self.last_spec = spec
        self.preview_config_overrides.append(preview_config_override)
        self.run_id = ("f" if self.start_calls == 1 else "d") * 32
        assert spec.target_kind == "current_workspace"
        assert spec.idempotency_key
        async with self.db_factory() as db:
            existing = await db.get(HarnessRun, self.run_id)
            if existing is not None:
                return existing
            task = await db.get(Task, task_id)
            delivery = await db.get(DeliveryRun, task.delivery_run_id)
            assert owner_identity.task_id == task.id
            assert owner_identity.incarnation_id == task.incarnation_id
            assert owner_identity.retry_count == task.retry_count
            assert owner_identity.turn_generation == task.turn_generation
            assert owner_identity.status == task.status == "delivery_waiting"
            harness = HarnessRun(
                id=self.run_id,
                task_id=task.id,
                owner_task_incarnation_id=task.incarnation_id,
                owner_task_retry_count=task.retry_count,
                owner_task_turn_generation=task.turn_generation,
                owner_task_status=task.status,
                project_id=task.project_id,
                target_kind="current_workspace",
                target_spec={
                    "kind": "current_workspace",
                    **spec.target,
                },
                test_plan={"objective": "Validate Delivery"},
                runtime_config={"provider": "codex"},
                request_fingerprint="a" * 64,
                idempotency_scope=f"task:{task.id}",
                idempotency_key=spec.idempotency_key,
                root_run_id=self.run_id,
                status="running",
                stage="reviewing",
                source_git_head=delivery.head_sha,
                cleanup_status="pending",
            )
            db.add(harness)
            await db.commit()
            await db.refresh(harness)
            return harness

    async def finish(
        self,
        *,
        verdict: str,
        report: str,
        finding: bool = False,
        complete_evidence: bool = True,
    ) -> None:
        async with self.db_factory() as db:
            harness = await db.get(HarnessRun, self.run_id)
            harness.status = "completed"
            harness.stage = "completed"
            harness.verdict = verdict
            harness.report = report
            harness.cleanup_status = "completed"
            harness.completed_at = datetime.utcnow()
            evidence_prefix = "e" if self.start_calls == 1 else "c"
            attempt = HarnessAttempt(
                id=evidence_prefix * 32,
                run_id=harness.id,
                ordinal=1,
                status="completed",
                stage="completed",
                provider="codex",
                model="gpt-test",
                reasoning_effort="medium",
                codex_service_tier="default",
                archive_state="complete" if complete_evidence else "incomplete",
                result_data={"verdict": verdict},
            )
            db.add(attempt)
            if complete_evidence:
                for evidence_id, kind, name, content_type in (
                    (
                        ("1" if self.start_calls == 1 else "5") * 32,
                        "screenshot",
                        "final.png",
                        "image/png",
                    ),
                    (
                        ("2" if self.start_calls == 1 else "6") * 32,
                        "report",
                        "report.md",
                        "text/markdown",
                    ),
                ):
                    db.add(
                        HarnessEvidence(
                            id=evidence_id,
                            run_id=harness.id,
                            attempt_id=attempt.id,
                            kind=kind,
                            name=name,
                            content_type=content_type,
                            storage_path=f"archive/{name}",
                            sha256=evidence_id * 2,
                            byte_size=8,
                            metadata_={},
                        )
                    )
            if finding:
                db.add(
                    HarnessFinding(
                        id="3" * 32,
                        run_id=harness.id,
                        ordinal=1,
                        fingerprint="4" * 64,
                        scenario_id="delivery-flow",
                        severity="high",
                        category="functionality",
                        title="Save action does not complete",
                        expected="The change is saved",
                        actual="The page reports an error",
                        reproduction=["Open the form", "Press Save"],
                        evidence_names=["final.png"],
                    )
                )
            await db.commit()

    async def cancel(self, run_id, *, expected_identity) -> None:
        assert run_id == self.run_id
        assert expected_identity.task_id > 0
        self.cancel_calls += 1


def _real_dispatcher(db_factory) -> GlobalDispatcher:
    instance_manager = MagicMock()
    lifecycle_locks: dict[int, asyncio.Lock] = {}
    instance_manager._instance_lifecycle_lock.side_effect = lambda instance_id: (
        lifecycle_locks.setdefault(instance_id, asyncio.Lock())
    )
    instance_manager.is_running.return_value = False
    instance_manager.processes = {}
    instance_manager.reconcile_dead_reverse_task_owner = AsyncMock()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    return GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )


class FakePublisher:
    def __init__(self, db_factory, *, gate: asyncio.Event | None = None) -> None:
        self.db_factory = db_factory
        self.gate = gate
        self.entered = asyncio.Event()
        self.pr_calls = 0
        self.monitor_calls = 0
        self.verify_calls = 0

    async def ensure_pull_request(self, *, run_id, idempotency_key, fence):
        del idempotency_key
        assert isinstance(fence, DeliveryEffectFence)
        assert fence.run_id == run_id
        self.pr_calls += 1
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, run_id)
            repo = await db.get(MonitoredRepo, run.monitored_repo_id)
            return PublishedPullRequest(
                repo_id=repo.id,
                pr_number=73,
                url=f"https://github.com/{repo.repo_full_name}/pull/73",
                base_sha=run.base_sha,
                head_sha=run.head_sha,
                head_branch=run.delivery_branch,
                head_repo_full_name=repo.repo_full_name,
            )

    async def ensure_monitor(
        self,
        *,
        run_id,
        pull_request,
        idempotency_key,
        fence,
    ):
        del idempotency_key
        assert isinstance(fence, DeliveryEffectFence)
        assert fence.run_id == run_id
        self.monitor_calls += 1
        async with self.db_factory() as db:
            delivery = await db.get(DeliveryRun, run_id)
            existing = await db.scalar(
                select(PRMonitorRun).where(
                    PRMonitorRun.repo_id == pull_request.repo_id,
                    PRMonitorRun.pr_number == pull_request.pr_number,
                )
            )
            if existing is not None:
                existing.current_base_sha = pull_request.base_sha
                existing.current_head_sha = pull_request.head_sha
                existing.head_branch = pull_request.head_branch
                existing.head_repo_full_name = pull_request.head_repo_full_name
                existing.status = "reviewing"
                existing.state_version += 1
                await db.commit()
                return existing.id
            monitor = PRMonitorRun(
                repo_id=pull_request.repo_id,
                pr_number=pull_request.pr_number,
                status="reviewing",
                current_base_sha=pull_request.base_sha,
                current_head_sha=pull_request.head_sha,
                head_branch=pull_request.head_branch,
                head_repo_full_name=pull_request.head_repo_full_name,
                developer_task_id=delivery.developer_task_id,
            )
            db.add(monitor)
            await db.flush()
            db.add(
                PRRepairWake(
                    monitor_run_id=monitor.id,
                    developer_task_id=delivery.developer_task_id,
                    trigger_base_sha=pull_request.base_sha,
                    trigger_head_sha=pull_request.head_sha,
                    reason_kind="review_changes_requested",
                    evidence_hash="a" * 64,
                    evidence={"findings": [{"required_fix": "legacy wake"}]},
                    status="pending",
                    delivery_token="legacy-delivery-token",
                )
            )
            monitor.status = "repair_pending"
            await db.commit()
            return monitor.id

    async def verify_ready_to_merge(
        self,
        *,
        run_id,
        pull_request,
        monitor_run_id,
        expected_monitor_state_version,
    ):
        self.verify_calls += 1
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, run_id)
            monitor = await db.get(PRMonitorRun, monitor_run_id)
            review = await db.get(PRReview, monitor.current_review_id)
            assert run.pr_number == pull_request.pr_number
            assert run.pr_url == pull_request.url
            assert run.base_sha == pull_request.base_sha
            assert run.head_sha == pull_request.head_sha
            assert monitor.status == "ready_to_merge"
            assert monitor.state_version == expected_monitor_state_version
            assert review.monitor_run_id == monitor.id
            assert review.base_ref == run.base_branch
            assert review.base_sha == run.base_sha
            assert review.head_sha == run.head_sha
        return pull_request

    async def verify_merged(
        self,
        *,
        run_id,
        pull_request,
        monitor_run_id,
        expected_monitor_state_version,
    ):
        self.verify_calls += 1
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, run_id)
            monitor = await db.get(PRMonitorRun, monitor_run_id)
            review = await db.get(PRReview, monitor.current_review_id)
            assert run.pr_number == pull_request.pr_number
            assert run.pr_url == pull_request.url
            assert run.base_sha == pull_request.base_sha
            assert run.head_sha == pull_request.head_sha
            assert monitor.status == "merged"
            assert monitor.state_version == expected_monitor_state_version
            assert review.status == "merged"
            assert review.action_taken == "approved_merged"
            assert review.base_ref == run.base_branch
            assert review.base_sha == run.base_sha
            assert review.head_sha == run.head_sha
        return pull_request


class IndeterminatePublisher(FakePublisher):
    async def ensure_pull_request(self, *, run_id, idempotency_key, fence):
        del run_id, idempotency_key, fence
        self.pr_calls += 1
        raise RuntimeError("connection dropped after an indeterminate write")


class PermanentAfterWritePublisher(FakePublisher):
    def __init__(self, db_factory) -> None:
        super().__init__(db_factory)
        self.failed_once = False

    async def ensure_pull_request(self, **kwargs):
        if not self.failed_once:
            self.failed_once = True
            self.pr_calls += 1
            raise DeliveryPublisherPermanentError(
                "PR may exist but its confirmation exposed an identity conflict"
            )
        return await super().ensure_pull_request(**kwargs)


class NoEffectPreflightPublisher(FakePublisher):
    async def ensure_pull_request(self, **kwargs):
        del kwargs
        self.pr_calls += 1
        raise DeliveryPublisherNoEffectPreflightError(
            "frozen publishing policy is invalid"
        )


class IntentBeforeSuccessPublisher(FakePublisher):
    """Model the real publisher's durable at-most-once create barrier."""

    async def ensure_pull_request(self, *, run_id, idempotency_key, fence):
        pull_request = await super().ensure_pull_request(
            run_id=run_id,
            idempotency_key=idempotency_key,
            fence=fence,
        )
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, run_id)
            repo = await db.get(MonitoredRepo, run.monitored_repo_id)
            action = await db.get(DeliveryAction, fence.action_id)
            action.result = {
                "schema_version": 2,
                "kind": "pull_request_create_intent",
                "subject": {
                    "run_id": run.id,
                    "repo_id": repo.id,
                    "repo_full_name": repo.repo_full_name,
                    "base_branch": run.base_branch,
                    "delivery_branch": run.delivery_branch,
                    "base_sha": run.base_sha,
                    "head_sha": run.head_sha,
                    "head_tree_sha": run.head_tree_sha,
                    "patch_sha256": run.patch_sha256,
                },
            }
            await db.commit()
        return pull_request


class TerminalHistoryPublisher(FakePublisher):
    def __init__(self, db_factory, *, state: str = "closed") -> None:
        super().__init__(db_factory)
        assert state in {"closed", "merged"}
        self.state = state

    async def ensure_pull_request(self, *, run_id, idempotency_key, fence):
        del idempotency_key
        self.pr_calls += 1
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, run_id)
            repo = await db.get(MonitoredRepo, run.monitored_repo_id)
            action = await db.get(DeliveryAction, fence.action_id)
            url = f"https://github.com/{repo.repo_full_name}/pull/73"
            subject = {
                "run_id": run.id,
                "repo_id": repo.id,
                "repo_full_name": repo.repo_full_name,
                "base_branch": run.base_branch,
                "delivery_branch": run.delivery_branch,
                "base_sha": run.base_sha,
                "head_sha": run.head_sha,
                "head_tree_sha": run.head_tree_sha,
                "patch_sha256": run.patch_sha256,
            }
            action.remote_id = "73"
            action.remote_url = url
            action.result = {
                "schema_version": 2,
                "kind": "pull_request_history_conflict",
                "subject": subject,
                "reason": (f"Exact Delivery pull request #73 is already {self.state}"),
                "remote": {
                    "state": self.state,
                    "repo_id": repo.id,
                    "pr_number": 73,
                    "url": url,
                    "base_sha": run.base_sha,
                    "head_sha": run.head_sha,
                    "head_branch": run.delivery_branch,
                    "head_repo_full_name": repo.repo_full_name,
                },
            }
            run.pr_number = 73
            run.pr_url = url
            await db.commit()
        raise DeliveryPublisherPermanentError(
            f"Exact Delivery pull request #73 is already {self.state}"
        )


class CrashAfterTerminalHistoryPublisher(TerminalHistoryPublisher):
    async def ensure_pull_request(self, **kwargs):
        try:
            await super().ensure_pull_request(**kwargs)
        except DeliveryPublisherPermanentError:
            # Model process cancellation after the terminal receipt commits,
            # but before the Controller's permanent-error catch can run.
            raise asyncio.CancelledError


class UnresolvedTerminalReceiptPublisher(FakePublisher):
    def __init__(self, db_factory, *, kind: str) -> None:
        super().__init__(db_factory)
        assert kind in {
            "pull_request_history_ambiguous",
            "pull_request_create_unresolved",
        }
        self.kind = kind

    async def ensure_pull_request(self, *, run_id, idempotency_key, fence):
        del idempotency_key
        self.pr_calls += 1
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, run_id)
            repo = await db.get(MonitoredRepo, run.monitored_repo_id)
            action = await db.get(DeliveryAction, fence.action_id)
            action.result = {
                "schema_version": 2,
                "kind": self.kind,
                "subject": {
                    "run_id": run.id,
                    "repo_id": repo.id,
                    "repo_full_name": repo.repo_full_name,
                    "base_branch": run.base_branch,
                    "delivery_branch": run.delivery_branch,
                    "base_sha": run.base_sha,
                    "head_sha": run.head_sha,
                    "head_tree_sha": run.head_tree_sha,
                    "patch_sha256": run.patch_sha256,
                },
                "reason": f"terminal evidence: {self.kind}",
            }
            await db.commit()
        raise DeliveryPublisherPermanentError(f"terminal evidence: {self.kind}")


class BoundHistoryAmbiguityThenSuccessPublisher(FakePublisher):
    def __init__(self, db_factory) -> None:
        super().__init__(db_factory)
        self.failed_once = False

    async def ensure_pull_request(self, *, run_id, idempotency_key, fence):
        if self.failed_once:
            return await super().ensure_pull_request(
                run_id=run_id,
                idempotency_key=idempotency_key,
                fence=fence,
            )
        self.failed_once = True
        self.pr_calls += 1
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, run_id)
            repo = await db.get(MonitoredRepo, run.monitored_repo_id)
            action = await db.get(DeliveryAction, fence.action_id)
            url = f"https://github.com/{repo.repo_full_name}/pull/73"
            action.result = {
                "schema_version": 2,
                "kind": "pull_request_history_ambiguous",
                "subject": {
                    "run_id": run.id,
                    "repo_id": repo.id,
                    "repo_full_name": repo.repo_full_name,
                    "base_branch": run.base_branch,
                    "delivery_branch": run.delivery_branch,
                    "base_sha": run.base_sha,
                    "head_sha": run.head_sha,
                    "head_tree_sha": run.head_tree_sha,
                    "patch_sha256": run.patch_sha256,
                },
                "reason": (
                    "Delivery pull-request history is ambiguous: Pull request "
                    "does not match the exact Delivery subject"
                ),
            }
            run.pr_number = 73
            run.pr_url = url
            await db.commit()
        raise DeliveryPublisherPermanentError(
            "old publisher compared the PR before its branch push"
        )


class MonitorFailsOncePublisher(FakePublisher):
    async def ensure_monitor(self, **kwargs):
        if self.monitor_calls == 0:
            self.monitor_calls += 1
            raise DeliveryPublisherPermanentError(
                "monitor binding rejected after PR publication"
            )
        return await super().ensure_monitor(**kwargs)


class RacingReadyPublisher(FakePublisher):
    async def verify_ready_to_merge(self, **kwargs):
        pull_request = await super().verify_ready_to_merge(**kwargs)
        async with self.db_factory() as db:
            monitor = await db.get(PRMonitorRun, kwargs["monitor_run_id"])
            monitor.status = "waiting_ci"
            monitor.state_version += 1
            await db.commit()
        return pull_request


class RacingReviewBaseRefPublisher(FakePublisher):
    """Retarget the same-SHA Review after verifier success, before final CAS."""

    async def _retarget_review(self, monitor_run_id: int) -> None:
        async with self.db_factory() as db:
            monitor = await db.get(
                PRMonitorRun,
                monitor_run_id,
                populate_existing=True,
            )
            review = await db.get(
                PRReview,
                monitor.current_review_id,
                populate_existing=True,
            )
            assert review.base_ref == "main"
            assert review.base_sha == monitor.current_base_sha
            assert review.head_sha == monitor.current_head_sha
            review.base_ref = "release/1.x"
            await db.commit()

    async def verify_ready_to_merge(self, **kwargs):
        pull_request = await super().verify_ready_to_merge(**kwargs)
        await self._retarget_review(kwargs["monitor_run_id"])
        return pull_request

    async def verify_merged(self, **kwargs):
        pull_request = await super().verify_merged(**kwargs)
        await self._retarget_review(kwargs["monitor_run_id"])
        return pull_request


async def _scope(
    db_session,
    *,
    max_cycles: int = 4,
    max_no_progress: int = 2,
    auto_merge: bool = False,
    frontend_review: str = "off",
):
    project = Project(
        name="delivery-controller-project",
        local_path="/srv/repos/delivery-controller",
        git_url="git@github.com:acme/delivery-controller.git",
        has_remote=True,
        default_branch="main",
        status="ready",
    )
    db_session.add(project)
    await db_session.flush()
    repo = MonitoredRepo(
        repo_full_name="acme/delivery-controller",
        project_id=project.id,
        webhook_secret="secret",
        enabled=True,
        auto_merge=auto_merge,
        auto_repair=True,
        review_mode="panel",
        wait_for_ci=True,
        required_checks=[
            {
                "kind": "check_run",
                "name": "test",
                "app_slug": "github-actions",
            }
        ],
        merge_queue_mode="manual",
        default_branch="main",
    )
    db_session.add(repo)
    await db_session.commit()
    run = await create_delivery_run(
        db_session,
        DeliveryCreateSpec(
            idempotency_key="controller-scope-run",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Delivery controller test",
            requirements="Implement the change and prove it with tests.",
            max_cycles=max_cycles,
            max_no_progress=max_no_progress,
            frontend_review=frontend_review,
        ),
    )
    return run, repo


def _controller(
    db_factory,
    workspace,
    capabilities,
    publisher,
    *,
    owner="controller-a",
    dispatcher=None,
    enabled=True,
    test_harness_service=None,
):
    return DeliveryController(
        db_factory=db_factory,
        workspace_manager=workspace,
        capability_gateway=capabilities,
        publisher=publisher,
        dispatcher=dispatcher,
        test_harness_service=test_harness_service,
        owner_id=owner,
        enabled=enabled,
        poll_interval_seconds=0.01,
        reconcile_interval_seconds=0.01,
        lease_seconds=30,
        action_lease_seconds=30,
    )


async def _to_coding_pending(controller, run_id):
    assert await controller.reconcile_run(run_id)  # create Plan invocation
    assert await controller.reconcile_run(run_id)  # consume Plan
    assert await controller.reconcile_run(run_id)  # dispatch Developer turn


def test_plan_prompt_preserves_explicit_report_only_requirements():
    context = MagicMock(requirements="Inspect the stress tests; do not write anything")

    prompt = DeliveryController._plan_prompt(context, {"kind": "operator_retry"})

    assert "report-only plan" in prompt
    assert "GIT_OPTIONAL_LOCKS=0" in prompt
    assert "final repository-state audit" in prompt
    assert "Do not invent implementation or test work" in prompt
    assert context.requirements in prompt


def test_report_only_terminal_progress_skips_code_and_pr_gates():
    now = datetime.utcnow()
    run = MagicMock(
        phase="done",
        activity="terminal",
        outcome="success",
        created_at=now,
        turn_count=1,
        pr_number=None,
        delivery_branch="ccm/delivery/1-task",
    )
    transition = MagicMock(
        cause="report_completed",
        before_state={"phase": "coding", "activity": "running"},
        after_state={"phase": "done", "activity": "terminal"},
        created_at=now,
    )
    frontend = MagicMock(
        policy="off",
        skip_reason=None,
        run_id=None,
        status=None,
    )

    stages = _stage_progress(
        run=run,
        cycle=None,
        transitions=[transition],
        monitor=None,
        frontend=frontend,
    )

    assert [stage.state for stage in stages] == [
        "completed",
        "completed",
        "skipped",
        "skipped",
        "skipped",
        "skipped",
    ]


async def _complete_code(db_factory, workspace, run_id, head, tree):
    workspace.advance(head, tree)
    now = datetime.utcnow()
    async with db_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        task = await db.get(Task, run.developer_task_id)
        historical_instance = Instance(
            name=f"settled-delivery-{run.id}-{run.turn_count}",
            status="idle",
            pid=None,
            current_task_id=None,
        )
        db.add(historical_instance)
        await db.flush()
        task.status = "completed"
        # Dispatcher preserves this as execution history after releasing the
        # reverse Instance owner; Controller must not wait for it to be NULL.
        task.instance_id = historical_instance.id
        task.started_at = now
        task.completed_at = now + timedelta(seconds=1)
        task.session_id = task.session_id or f"session-{run.cycle_count}"
        await db.commit()


async def _complete_report_only(db_factory, workspace, run_id):
    await _complete_code(
        db_factory,
        workspace,
        run_id,
        workspace.head_sha,
        workspace.tree_sha,
    )
    async with db_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        task = await db.get(Task, run.developer_task_id)
        db.add(
            LogEntry(
                instance_id=task.instance_id,
                task_id=task.id,
                task_retry_count=task.retry_count,
                task_turn_generation=run.turn_count,
                event_type="message",
                role="assistant",
                content=(
                    "Read-only inspection completed with findings.\n\n"
                    "DELIVERY_RESULT: REPORT_COMPLETE"
                ),
                is_error=False,
            )
        )
        await db.commit()


async def _add_active_harness_run(
    db_factory,
    task_id: int,
    *,
    run_id: str,
) -> None:
    """Persist the graph shape admitted by the public terminal-Task API."""

    async with db_factory() as db:
        task = await db.get(Task, task_id, populate_existing=True)
        assert task is not None
        assert task.status in {"completed", "failed", "cancelled", "conflict"}
        db.add(
            HarnessRun(
                id=run_id,
                task_id=task.id,
                owner_task_incarnation_id=task.incarnation_id,
                owner_task_retry_count=task.retry_count,
                owner_task_turn_generation=task.turn_generation,
                owner_task_status=task.status,
                target_kind="fixed_url",
                target_spec={"url": "https://example.com"},
                test_plan={"objective": "Verify the terminal Delivery output"},
                runtime_config={"provider": "codex"},
                request_fingerprint="8" * 64,
                root_run_id=run_id,
                status="running",
                stage="waiting_for_browser",
            )
        )
        await db.commit()


async def _complete_harness_run(db_factory, run_id: str) -> None:
    async with db_factory() as db:
        run = await db.get(HarnessRun, run_id, populate_existing=True)
        assert run is not None
        run.status = "completed"
        run.stage = "completed"
        run.cleanup_status = "completed"
        run.completed_at = datetime.utcnow()
        await db.commit()


async def _finish_code_with_dispatcher(
    dispatcher: GlobalDispatcher,
    db_factory,
    workspace,
    run_id: int,
    *,
    success: bool,
) -> tuple[int, int]:
    workspace.advance(HEAD_ONE, TREE_ONE)
    started_at = datetime.utcnow()
    async with db_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        task = await db.get(Task, run.developer_task_id)
        instance = Instance(
            name=f"dispatcher-delivery-{run.id}",
            status="running",
            current_task_id=task.id,
            pid=None,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task.status = "executing"
        task.instance_id = instance.id
        task.started_at = started_at
        task.completed_at = None
        task.max_retries = task.retry_count
        await db.commit()
        await db.refresh(task)
        task_id, instance_id = task.id, instance.id
        generation = dispatcher._task_lifecycle_generation(task)

    if success:
        assert await dispatcher._complete_owned_task_result(generation) == (
            True,
            False,
        )
    else:
        assert (
            await dispatcher._retry_or_fail_mode_task(
                generation,
                "Exit code: 1",
            )
            == "failed"
        )
    await dispatcher._reset_instance_if_stale(instance_id, generation)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.instance_id == instance_id
        assert instance.current_task_id is None
        assert instance.status == "idle"
        assert instance.pid is None
    return task_id, instance_id


async def _to_publishing(
    controller, db_factory, workspace, run_id, head=HEAD_ONE, tree=TREE_ONE
):
    await _to_coding_pending(controller, run_id)
    await _complete_code(db_factory, workspace, run_id, head, tree)
    assert await controller.reconcile_run(run_id)  # exact terminal -> pre-review
    assert await controller.reconcile_run(run_id)  # create Review invocation
    assert await controller.reconcile_run(run_id)  # consume Review -> publishing


async def _to_frontend_ready(
    controller,
    db_factory,
    workspace,
    run_id,
    *,
    head=HEAD_ONE,
    tree=TREE_ONE,
):
    await _to_coding_pending(controller, run_id)
    await _complete_code(db_factory, workspace, run_id, head, tree)
    assert await controller.reconcile_run(run_id)  # exact terminal -> pre-review
    assert await controller.reconcile_run(run_id)  # create Review invocation
    assert await controller.reconcile_run(run_id)  # approved -> frontend ready
    async with db_factory() as db:
        run = await db.get(DeliveryRun, run_id, populate_existing=True)
        assert (run.phase, run.activity) == ("frontend_review", "ready")


async def _to_monitoring(controller, db_factory, workspace, run_id):
    await _to_publishing(controller, db_factory, workspace, run_id)
    assert await controller.reconcile_run(run_id)
    async with db_factory() as db:
        run = await db.get(DeliveryRun, run_id, populate_existing=True)
        assert (run.phase, run.activity) == ("monitoring", "waiting")
        return run.pr_monitor_run_id


@pytest.mark.asyncio
async def test_frontend_review_auto_policy_records_unavailable_skip(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(db_session, frontend_review="auto")
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    monkeypatch.setattr(settings, "auth_token", "")

    await _to_frontend_ready(controller, db_factory, workspace, run.id)
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        cycle = await db.get(DeliveryCycle, stored.current_cycle_id)
        assert (stored.phase, stored.activity) == ("publishing", "ready")
        assert cycle.frontend_review_run_id is None
        assert cycle.frontend_review_verdict is None
        assert cycle.frontend_review_skip_reason == (
            "Frontend review unavailable: AUTH_TOKEN is not configured"
        )


@pytest.mark.asyncio
async def test_frontend_review_recovers_unbound_idempotent_harness(
    db_session,
    db_factory,
    monkeypatch,
):
    from backend.services.test_harness_contracts import TestHarnessSpec
    from backend.services.test_harness_owner_fence import (
        test_harness_owner_identity,
    )

    run, _repo = await _scope(db_session, frontend_review="auto")
    workspace = FakeWorkspace()
    harness = FakeFrontendHarness(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
        test_harness_service=harness,
    )
    await _to_frontend_ready(controller, db_factory, workspace, run.id)
    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id)
        task = await db.get(Task, stored.developer_task_id)
        cycle_id = stored.current_cycle_id
        head_sha = stored.head_sha
        owner_identity = test_harness_owner_identity(task)
    await harness.start_task_run(
        task_id=task.id,
        spec=TestHarnessSpec(
            target_kind="current_workspace",
            target={},
            goal="Recover the Browser gate",
            idempotency_key=(f"delivery:{run.id}:cycle:{cycle_id}:frontend:{head_sha}"),
        ),
        owner_identity=owner_identity,
    )
    # Model a restart after Harness admission but before the Delivery cycle
    # bound the handle. Recovery must use frozen durable identity even if the
    # mutable runtime configuration is no longer available.
    monkeypatch.setattr(settings, "auth_token", "")

    assert await controller.reconcile_run(run.id)
    assert harness.start_calls == 1
    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        cycle = await db.get(DeliveryCycle, stored.current_cycle_id)
        assert (stored.phase, stored.activity) == ("frontend_review", "waiting")
        assert cycle.frontend_review_run_id == harness.run_id


@pytest.mark.asyncio
async def test_frontend_review_pass_requires_archived_report_and_screenshot(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(db_session, frontend_review="auto")
    workspace = FakeWorkspace()
    harness = FakeFrontendHarness(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
        test_harness_service=harness,
    )
    monkeypatch.setattr(settings, "auth_token", "test-token")
    monkeypatch.setattr(
        "backend.services.workspace_review.workspace_review_capability",
        lambda task, project: {
            "available": True,
            "configured": True,
            "reason": None,
            "repo_path": task.last_cwd,
        },
    )

    await _to_frontend_ready(controller, db_factory, workspace, run.id)
    assert await controller.reconcile_run(run.id)  # bind Harness
    assert harness.start_calls == 1
    assert harness.last_spec.target_kind == "current_workspace"
    assert harness.last_spec.profile == "standard"
    assert harness.last_spec.allow_actions is True
    assert "Implement the change and prove it with tests." in (harness.last_spec.goal)
    await harness.finish(verdict="passed", report="All scenarios passed.")
    assert await controller.reconcile_run(run.id)  # accept exact evidence

    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        cycle = await db.get(DeliveryCycle, stored.current_cycle_id)
        assert (stored.phase, stored.activity) == ("publishing", "ready")
        assert cycle.frontend_review_run_id == harness.run_id
        assert cycle.frontend_review_verdict == "passed"
        assert cycle.frontend_review_summary == "All scenarios passed."


@pytest.mark.asyncio
async def test_frontend_review_runs_every_matching_preview_profile_in_order(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(db_session, frontend_review="auto")
    project = await db_session.get(Project, run.project_id)
    project.preview_config = {"version": 2, "profiles": []}
    await db_session.commit()
    workspace = FakeWorkspace()
    workspace.changed_paths = ["web/src/App.tsx", "admin/src/Users.tsx"]
    harness = FakeFrontendHarness(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
        test_harness_service=harness,
    )
    profiles = [
        {"id": "web", "enabled": True, "match_paths": ["web/**"]},
        {"id": "admin", "enabled": True, "match_paths": ["admin/**"]},
    ]
    collection = {
        "version": 2,
        "default_profile": None,
        "profiles": profiles,
        "legacy": False,
    }
    monkeypatch.setattr(settings, "auth_token", "test-token")
    monkeypatch.setattr(
        "backend.services.workspace_review.workspace_review_capability",
        lambda task, project: {
            "available": True,
            "configured": True,
            "reason": None,
            "repo_path": task.last_cwd,
        },
    )
    monkeypatch.setattr(
        "backend.services.workspace_review.validate_preview_profiles",
        lambda config, workspace_path: collection,
    )
    monkeypatch.setattr(
        "backend.services.workspace_review.resolve_preview_config",
        lambda config, workspace_path, changed_paths: profiles,
    )

    await _to_frontend_ready(controller, db_factory, workspace, run.id)
    assert await controller.reconcile_run(run.id)  # start web
    assert harness.last_spec.target == {"preview_profile_id": "web"}
    await harness.finish(verdict="passed", report="Web passed.")
    assert await controller.reconcile_run(run.id)  # advance to admin

    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        cycle = await db.get(DeliveryCycle, stored.current_cycle_id)
        assert (stored.phase, stored.activity) == ("frontend_review", "ready")
        assert cycle.frontend_review_profile_ids == ["web", "admin"]
        assert cycle.frontend_review_profile_index == 1
        assert cycle.frontend_review_run_id is None
        assert [item["profile_id"] for item in cycle.frontend_review_results] == ["web"]

    assert await controller.reconcile_run(run.id)  # start admin
    assert harness.last_spec.target == {"preview_profile_id": "admin"}
    frozen_collection = {
        "version": 2,
        "default_profile": None,
        "profiles": profiles,
    }
    assert harness.preview_config_overrides == [
        frozen_collection,
        frozen_collection,
    ]
    await harness.finish(verdict="passed", report="Admin passed.")
    assert await controller.reconcile_run(run.id)  # all profiles passed

    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        cycle = await db.get(DeliveryCycle, stored.current_cycle_id)
        assert (stored.phase, stored.activity) == ("publishing", "ready")
        assert [item["profile_id"] for item in cycle.frontend_review_results] == [
            "web",
            "admin",
        ]


@pytest.mark.asyncio
async def test_frontend_review_rejects_pass_without_complete_evidence(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(db_session, frontend_review="auto")
    workspace = FakeWorkspace()
    harness = FakeFrontendHarness(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
        test_harness_service=harness,
    )
    monkeypatch.setattr(settings, "auth_token", "test-token")
    monkeypatch.setattr(
        "backend.services.workspace_review.workspace_review_capability",
        lambda task, project: {
            "available": True,
            "configured": True,
            "reason": None,
            "repo_path": task.last_cwd,
        },
    )

    await _to_frontend_ready(controller, db_factory, workspace, run.id)
    assert await controller.reconcile_run(run.id)
    await harness.finish(
        verdict="passed",
        report="Looks good, but evidence was not archived.",
        complete_evidence=False,
    )
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert (stored.phase, stored.activity, stored.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert stored.error_code == "frontend_review_evidence_incomplete"


@pytest.mark.asyncio
async def test_frontend_review_findings_start_a_new_planning_cycle(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(
        db_session,
        frontend_review="auto",
        max_cycles=3,
    )
    workspace = FakeWorkspace()
    harness = FakeFrontendHarness(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
        test_harness_service=harness,
    )
    monkeypatch.setattr(settings, "auth_token", "test-token")
    monkeypatch.setattr(
        "backend.services.workspace_review.workspace_review_capability",
        lambda task, project: {
            "available": True,
            "configured": True,
            "reason": None,
            "repo_path": task.last_cwd,
        },
    )

    await _to_frontend_ready(controller, db_factory, workspace, run.id)
    assert await controller.reconcile_run(run.id)
    await harness.finish(
        verdict="failed",
        report="The Save flow is broken.",
        finding=True,
    )
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        current = await db.get(DeliveryCycle, stored.current_cycle_id)
        previous = await db.scalar(
            select(DeliveryCycle).where(
                DeliveryCycle.run_id == stored.id,
                DeliveryCycle.cycle_number == 1,
            )
        )
        assert (stored.phase, stored.activity, stored.cycle_count) == (
            "planning",
            "ready",
            2,
        )
        assert previous.frontend_review_verdict == "failed"
        assert current.trigger_kind == "frontend_review_changes_requested"
        assert current.trigger_payload["source_git_head"] == HEAD_ONE
        assert current.trigger_payload["findings"][0]["title"] == (
            "Save action does not complete"
        )


async def _set_monitor_terminal(
    db_factory,
    *,
    run_id: int,
    monitor_id: int,
    auto_merge: bool,
) -> int:
    """Install the exact Review generation represented by a terminal Monitor."""

    async with db_factory() as db:
        run = await db.get(DeliveryRun, run_id, populate_existing=True)
        monitor = await db.get(
            PRMonitorRun,
            monitor_id,
            populate_existing=True,
        )
        review = PRReview(
            repo_id=monitor.repo_id,
            monitor_run_id=monitor.id,
            pr_number=monitor.pr_number,
            base_ref=run.base_branch,
            base_sha=run.base_sha,
            head_sha=run.head_sha,
            delivery_id=f"delivery:{run.id}:{run.head_sha}",
            pr_title=run.title,
            pr_author="delivery-bot",
            pr_url=run.pr_url,
            status="merged" if auto_merge else "approved",
            action_nonce="e" * 48,
            action_taken="approved_merged" if auto_merge else "lgtm_comment",
            publishing_actor="ccm-bot" if auto_merge else None,
            publishing_started_at=datetime.utcnow() if auto_merge else None,
            merge_method="fast-forward" if auto_merge else None,
            completed_at=datetime.utcnow(),
        )
        db.add(review)
        await db.flush()
        monitor.current_review_id = review.id
        monitor.status = "merged" if auto_merge else "ready_to_merge"
        monitor.completed_at = datetime.utcnow() if auto_merge else None
        monitor.state_version += 1
        await db.commit()
        return review.id


@pytest.mark.asyncio
async def test_code_dispatch_waits_for_terminal_task_harness_graph(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    assert await controller.reconcile_run(run.id)  # create Plan invocation
    assert await controller.reconcile_run(run.id)  # consume Plan
    async with db_factory() as db:
        task = await db.get(Task, run.developer_task_id)
        task.status = "completed"
        task.completed_at = datetime.utcnow()
        await db.commit()
    harness_run_id = "1" * 32
    await _add_active_harness_run(
        db_factory,
        run.developer_task_id,
        run_id=harness_run_id,
    )

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        waiting = await db.get(DeliveryRun, run.id, populate_existing=True)
        task = await db.get(Task, run.developer_task_id, populate_existing=True)
        turn = await db.scalar(
            select(DeliveryTurn.id).where(DeliveryTurn.run_id == run.id)
        )
        harness = await db.get(HarnessRun, harness_run_id)
        assert (waiting.phase, waiting.activity) == ("coding", "ready")
        assert task.status == "completed"
        assert turn is None
        assert harness.status == "running"

    await _complete_harness_run(db_factory, harness_run_id)
    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        dispatched = await db.get(DeliveryRun, run.id, populate_existing=True)
        task = await db.get(Task, run.developer_task_id, populate_existing=True)
        turn = await db.scalar(
            select(DeliveryTurn).where(DeliveryTurn.active_run_id == run.id)
        )
        assert (dispatched.phase, dispatched.activity) == ("coding", "running")
        assert task.status == "pending"
        assert turn is not None and turn.status == "queued"


@pytest.mark.asyncio
async def test_completed_turn_waits_for_terminal_task_harness_graph(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    await _to_coding_pending(controller, run.id)
    await _complete_code(db_factory, workspace, run.id, HEAD_ONE, TREE_ONE)
    harness_run_id = "2" * 32
    await _add_active_harness_run(
        db_factory,
        run.developer_task_id,
        run_id=harness_run_id,
    )

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        waiting = await db.get(DeliveryRun, run.id, populate_existing=True)
        task = await db.get(Task, run.developer_task_id, populate_existing=True)
        turn = await db.scalar(
            select(DeliveryTurn).where(DeliveryTurn.active_run_id == run.id)
        )
        harness = await db.get(HarnessRun, harness_run_id)
        assert (waiting.phase, waiting.activity) == ("coding", "running")
        assert task.status == "completed"
        assert turn is not None and turn.status in {
            "queued",
            "dispatching",
            "running",
        }
        assert harness.status == "running"

    await _complete_harness_run(db_factory, harness_run_id)
    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        finalized = await db.get(DeliveryRun, run.id, populate_existing=True)
        task = await db.get(Task, run.developer_task_id, populate_existing=True)
        turn = await db.scalar(
            select(DeliveryTurn).where(DeliveryTurn.run_id == run.id)
        )
        assert (finalized.phase, finalized.activity) == ("pre_review", "ready")
        assert task.status == "delivery_waiting"
        assert turn is not None and turn.status == "completed"
        assert turn.active_run_id is None


@pytest.mark.asyncio
async def test_fail_run_waits_for_terminal_task_harness_graph(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    controller = _controller(
        db_factory,
        FakeWorkspace(),
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    async with db_factory() as db:
        task = await db.get(Task, run.developer_task_id)
        task.status = "completed"
        task.completed_at = datetime.utcnow()
        await db.commit()
    harness_run_id = "3" * 32
    await _add_active_harness_run(
        db_factory,
        run.developer_task_id,
        run_id=harness_run_id,
    )
    lease = await controller._claim_run(run.id)
    assert lease is not None

    assert not await controller._fail_run(
        lease,
        code="delivery_test_failure",
        message="terminal failure must wait for frontend validation",
    )
    async with db_factory() as db:
        waiting = await db.get(DeliveryRun, run.id, populate_existing=True)
        task = await db.get(Task, run.developer_task_id, populate_existing=True)
        harness = await db.get(HarnessRun, harness_run_id)
        assert (waiting.phase, waiting.activity, waiting.outcome) == (
            "planning",
            "ready",
            None,
        )
        assert task.status == "completed"
        assert harness.status == "running"

    await _complete_harness_run(db_factory, harness_run_id)
    assert await controller._fail_run(
        lease,
        code="delivery_test_failure",
        message="terminal failure must wait for frontend validation",
    )
    async with db_factory() as db:
        failed = await db.get(DeliveryRun, run.id, populate_existing=True)
        task = await db.get(Task, run.developer_task_id, populate_existing=True)
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert task.status == "failed"
        assert task.error_message == (
            "terminal failure must wait for frontend validation"
        )


@pytest.mark.asyncio
async def test_repeated_controller_cancellation_settles_drive_before_lease_release(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(db_session)
    controller = _controller(
        db_factory,
        FakeWorkspace(),
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    drive_entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def blocked_drive(_lease):
        drive_entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError as first_cancel:
            cleanup_started.set()
            cleanup = asyncio.create_task(release_cleanup.wait())
            cancellation = first_cancel
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError as exc:
                    cancellation = exc
            cleanup.result()
            raise cancellation

    monkeypatch.setattr(controller, "_drive", blocked_drive)
    reconciliation = asyncio.create_task(controller.reconcile_run(run.id))
    await drive_entered.wait()
    reconciliation.cancel()
    await cleanup_started.wait()
    reconciliation.cancel()
    await asyncio.sleep(0)

    assert not reconciliation.done()
    async with db_factory() as db:
        leased = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert leased.lease_owner == controller.owner_id

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await reconciliation
    async with db_factory() as db:
        released = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert released.lease_owner is None
        assert released.lease_expires_at is None


@pytest.mark.asyncio
async def test_expired_lease_takeover_fences_stale_renew_and_release(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    publisher = FakePublisher(db_factory)
    first = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        owner="controller-first",
    )
    replacement = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        owner="controller-replacement",
    )

    stale_lease = await first._claim_run(run.id)
    assert stale_lease is not None
    async with db_factory() as db:
        leased = await db.get(DeliveryRun, run.id, populate_existing=True)
        leased.lease_expires_at = datetime.utcnow() - timedelta(seconds=1)
        await db.commit()
    current_lease = await replacement._claim_run(run.id)
    assert current_lease is not None
    assert current_lease.generation == stale_lease.generation + 1

    assert not await first._renew_run_lease(stale_lease)
    await first._release_run(stale_lease, delay=0)
    async with db_factory() as db:
        retained = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert retained.lease_owner == replacement.owner_id
        assert retained.controller_generation == current_lease.generation
        assert retained.lease_expires_at is not None

    await replacement._release_run(current_lease, delay=0)


@pytest.mark.asyncio
async def test_expired_run_lease_cannot_renew_or_mutate_without_takeover(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    controller = _controller(
        db_factory,
        FakeWorkspace(),
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    lease = await controller._claim_run(run.id)
    assert lease is not None
    expired_at = datetime.utcnow() - timedelta(seconds=1)
    async with db_factory() as db:
        leased = await db.get(DeliveryRun, run.id, populate_existing=True)
        leased.lease_expires_at = expired_at
        await db.commit()

    assert not await controller._renew_run_lease(lease)
    with pytest.raises(DeliveryConflictError, match="lease expired"):
        await controller._pause_run(
            lease,
            reason="stale owner must not write",
            code="stale_owner",
        )

    async with db_factory() as db:
        unchanged = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert (unchanged.phase, unchanged.activity) == ("planning", "ready")
        assert unchanged.lease_owner == controller.owner_id
        assert unchanged.controller_generation == lease.generation
        assert unchanged.lease_expires_at == expired_at


@pytest.mark.asyncio
async def test_expired_action_lease_cannot_renew_or_write_state(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    await _to_publishing(controller, db_factory, workspace, run.id)
    lease = await controller._claim_run(run.id)
    assert lease is not None
    context = await controller._context(lease)
    claimed = await controller._stage_or_claim_publish_action(lease, context)
    assert claimed is not None
    token, action_id, _idempotency_key, _receipt = claimed
    controller._active_action_fences[run.id] = (action_id, token)
    expired_at = datetime.utcnow() - timedelta(seconds=1)
    async with db_factory() as db:
        leased_run = await db.get(DeliveryRun, run.id, populate_existing=True)
        action = await db.get(DeliveryAction, action_id, populate_existing=True)
        run_expiry = leased_run.lease_expires_at
        action.lease_expires_at = expired_at
        await db.commit()

    try:
        assert not await controller._renew_run_lease(lease)
        with pytest.raises(DeliveryConflictError, match="action lease expired"):
            await controller._mark_action_unknown(
                lease,
                action_id,
                token,
                RuntimeError("stale owner must not transition the action"),
            )
    finally:
        controller._active_action_fences.pop(run.id, None)

    async with db_factory() as db:
        unchanged_run = await db.get(DeliveryRun, run.id, populate_existing=True)
        unchanged_action = await db.get(
            DeliveryAction,
            action_id,
            populate_existing=True,
        )
        assert unchanged_run.lease_expires_at == run_expiry
        assert unchanged_action.status == "leased"
        assert unchanged_action.lease_owner == token
        assert unchanged_action.lease_expires_at == expired_at


@pytest.mark.asyncio
async def test_expired_action_lease_cannot_start_remote_effect(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    publisher = FakePublisher(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
    )
    await _to_publishing(controller, db_factory, workspace, run.id)
    lease = await controller._claim_run(run.id)
    assert lease is not None
    context = await controller._context(lease)
    real_stage = controller._stage_or_claim_publish_action

    async def expire_after_claim(current_lease, current_context):
        claimed = await real_stage(current_lease, current_context)
        assert claimed is not None
        _token, action_id, _idempotency_key, _receipt = claimed
        async with db_factory() as db:
            action = await db.get(DeliveryAction, action_id)
            action.lease_expires_at = datetime.utcnow() - timedelta(seconds=1)
            await db.commit()
        return claimed

    monkeypatch.setattr(
        controller,
        "_stage_or_claim_publish_action",
        expire_after_claim,
    )

    with pytest.raises(DeliveryConflictError, match="action lease expired"):
        await controller._drive_publishing(lease, context)
    assert publisher.pr_calls == 0
    assert publisher.monitor_calls == 0


@pytest.mark.asyncio
async def test_commit_then_finalize_crash_recovers_same_exact_turn(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    await _to_coding_pending(controller, run.id)
    await _complete_code(db_factory, workspace, run.id, HEAD_ONE, TREE_ONE)
    real_finalize = controller._finalize_completed_turn
    finalize_calls = 0

    async def crash_once(*args, **kwargs):
        nonlocal finalize_calls
        finalize_calls += 1
        if finalize_calls == 1:
            raise RuntimeError("crash after commit before DB finalize")
        return await real_finalize(*args, **kwargs)

    monkeypatch.setattr(controller, "_finalize_completed_turn", crash_once)

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        unfinalized = await db.get(DeliveryRun, run.id, populate_existing=True)
        turn = await db.scalar(
            select(DeliveryTurn).where(DeliveryTurn.active_run_id == run.id)
        )
        assert (unfinalized.phase, unfinalized.activity) == ("coding", "running")
        assert unfinalized.head_sha == BASE_SHA
        assert turn is not None and turn.status in {"queued", "dispatching", "running"}
    assert workspace.head_sha == HEAD_ONE
    assert workspace.commit_calls == 1

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        recovered = await db.get(DeliveryRun, run.id, populate_existing=True)
        turn = await db.scalar(
            select(DeliveryTurn).where(DeliveryTurn.run_id == run.id)
        )
        assert (recovered.phase, recovered.activity) == ("pre_review", "ready")
        assert (recovered.head_sha, recovered.head_tree_sha) == (HEAD_ONE, TREE_ONE)
        assert turn is not None and turn.status == "completed"
        assert turn.active_run_id is None
    assert workspace.commit_calls == 2


@pytest.mark.asyncio
async def test_non_hex_review_patch_fence_pauses_before_publish(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    publisher = FakePublisher(db_factory)
    controller = _controller(db_factory, workspace, capabilities, publisher)
    await _to_coding_pending(controller, run.id)
    await _complete_code(db_factory, workspace, run.id, HEAD_ONE, TREE_ONE)
    assert await controller.reconcile_run(run.id)  # code -> pre-review
    assert await controller.reconcile_run(run.id)  # create Review invocation
    async with db_factory() as db:
        refreshed = await db.get(DeliveryRun, run.id, populate_existing=True)
        cycle = await db.get(DeliveryCycle, refreshed.current_cycle_id)
        invocation_id = cycle.review_invocation_id
    snapshot = capabilities.snapshots[invocation_id]
    capabilities.snapshots[invocation_id] = replace(
        snapshot,
        subject_ref={**(snapshot.subject_ref or {}), "patch_sha256": "z" * 64},
    )

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        paused = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert (paused.phase, paused.activity) == ("pre_review", "paused")
        assert "patch_sha256" in (paused.pause_reason or "")
    assert publisher.pr_calls == 0


@pytest.mark.asyncio
async def test_admitted_run_can_create_initial_plan_after_flags_are_disabled(
    db_session,
    db_factory,
    monkeypatch,
    registered_plan_capability,
):
    del registered_plan_capability
    run, _repo = await _scope(db_session)
    monkeypatch.setattr(settings, "delivery_loop_enabled", False)
    monkeypatch.setattr(settings, "capability_core_enabled", False)
    controller = _controller(
        db_factory,
        FakeWorkspace(),
        CoreDeliveryCapabilityGateway(db_factory),
        FakePublisher(db_factory),
        enabled=None,
    )

    await controller.start()
    try:
        async with db_factory() as db:
            refreshed = await db.get(DeliveryRun, run.id)
            cycle = await db.get(DeliveryCycle, refreshed.current_cycle_id)
            invocation = await db.get(
                CapabilityInvocation,
                cycle.plan_invocation_id,
            )
            assert (refreshed.phase, refreshed.activity) == ("planning", "waiting")
            assert invocation is not None
            assert invocation.task_id == refreshed.developer_task_id
            assert invocation.source == "delivery_controller"
            assert invocation.idempotency_key == f"delivery:{run.id}:cycle:1:plan"
    finally:
        await controller.shutdown()


@pytest.mark.asyncio
async def test_disabled_core_does_not_admit_controller_work_for_terminal_run(
    db_session,
    monkeypatch,
    registered_plan_capability,
):
    del registered_plan_capability
    run, _repo = await _scope(db_session)
    run.phase = "done"
    run.activity = "terminal"
    run.outcome = "cancelled"
    run.completed_at = datetime.utcnow()
    run.state_version += 1
    await db_session.commit()
    monkeypatch.setattr(settings, "capability_core_enabled", False)

    with pytest.raises(CapabilityDisabledError):
        await create_controller_invocation(
            db_session,
            task_id=run.developer_task_id,
            capability_key="plan",
            request_payload={"prompt": "must not revive a terminal run"},
            idempotency_key=f"delivery:{run.id}:terminal:plan",
        )


@pytest.mark.asyncio
async def test_happy_path_stops_at_exact_ready_to_merge(
    db_session,
    db_factory,
    client,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    publisher = FakePublisher(db_factory)
    dispatcher = FakeDispatcher()
    controller = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        dispatcher=dispatcher,
    )

    monitor_id = await _to_monitoring(controller, db_factory, workspace, run.id)
    await _set_monitor_terminal(
        db_factory,
        run_id=run.id,
        monitor_id=monitor_id,
        auto_merge=False,
    )

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        completed = await db.get(DeliveryRun, run.id)
        task = await db.get(Task, completed.developer_task_id)
        cycle = await db.get(DeliveryCycle, completed.current_cycle_id)
        plan_version = await db.get(PlanVersion, cycle.plan_version_id)
        assert (completed.phase, completed.activity, completed.outcome) == (
            "done",
            "terminal",
            "success",
        )
        assert completed.pr_number == 73
        assert completed.head_sha == HEAD_ONE
        assert task.status == "completed"
        monitor = await db.get(PRMonitorRun, monitor_id)
        wake = await db.scalar(
            select(PRRepairWake).where(PRRepairWake.monitor_run_id == monitor_id)
        )
        assert monitor.developer_task_id is None
        assert wake.status == "shadow"
        assert wake.developer_task_id is None
        task_id = task.id
        plan_id = plan_version.plan_id
    assert dispatcher.wake_count == 1
    assert publisher.pr_calls == 1
    assert publisher.monitor_calls == 1
    assert publisher.verify_calls == 1

    # The Delivery workspace is a projection over the native records. Prove
    # that every page-facing API resolves the same completed fake workflow,
    # rather than manufacturing a second Plan, Task, or PR Monitor record.
    delivery_response = await client.get(f"/api/delivery-runs/{run.id}")
    assert delivery_response.status_code == 200, delivery_response.text
    delivery = delivery_response.json()
    assert delivery["developer_task_id"] == task_id
    assert delivery["pr_monitor_run_id"] == monitor_id
    assert delivery["cycles"][0]["plan_version_id"] == plan_version.id
    assert delivery["turns"][0]["task_id"] == task_id

    plan_response = await client.get(f"/api/plans/{plan_id}")
    assert plan_response.status_code == 200, plan_response.text
    assert plan_response.json()["delivery_run_id"] == run.id

    task_response = await client.get(f"/api/tasks/{task_id}")
    assert task_response.status_code == 200, task_response.text
    task_projection = task_response.json()
    assert task_projection["delivery_run_id"] == run.id
    assert task_projection["delivery_terminal"] == "ready_to_merge"

    monitor_response = await client.get(f"/api/pr-monitor/runs/{monitor_id}")
    assert monitor_response.status_code == 200, monitor_response.text
    monitor_projection = monitor_response.json()
    assert monitor_projection["id"] == delivery["pr_monitor_run_id"]
    assert monitor_projection["pr_number"] == delivery["pr_number"]
    assert monitor_projection["current_head_sha"] == delivery["head_sha"]


@pytest.mark.asyncio
async def test_auto_merge_happy_path_requires_exact_merged_review(
    db_session,
    db_factory,
):
    run, repo = await _scope(db_session, auto_merge=True)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    publisher = FakePublisher(db_factory)
    dispatcher = FakeDispatcher()
    controller = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        dispatcher=dispatcher,
    )

    monitor_id = await _to_monitoring(controller, db_factory, workspace, run.id)
    async with db_factory() as db:
        stored_run = await db.get(DeliveryRun, run.id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        review = PRReview(
            repo_id=repo.id,
            monitor_run_id=monitor.id,
            pr_number=stored_run.pr_number,
            base_ref="main",
            base_sha=stored_run.base_sha,
            head_sha=stored_run.head_sha,
            delivery_id=(f"delivery:{stored_run.id}:{stored_run.head_sha}"),
            pr_title=stored_run.title,
            pr_author="delivery-bot",
            pr_url=stored_run.pr_url,
            status="merged",
            action_nonce="e" * 48,
            action_taken="approved_merged",
            publishing_actor="ccm-bot",
            publishing_started_at=datetime.utcnow(),
            merge_method="merge",
            completed_at=datetime.utcnow(),
        )
        db.add(review)
        await db.flush()
        monitor.current_review_id = review.id
        monitor.status = "merged"
        monitor.completed_at = datetime.utcnow()
        monitor.state_version += 1
        await db.commit()

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        completed = await db.get(DeliveryRun, run.id)
        task = await db.get(Task, completed.developer_task_id)
        assert (completed.phase, completed.activity, completed.outcome) == (
            "done",
            "terminal",
            "success",
        )
        assert completed.policy_snapshot["auto_merge"] is True
        assert completed.policy_snapshot["terminal"] == "merged"
        assert task.status == "completed"
    assert dispatcher.wake_count == 1
    assert publisher.verify_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("auto_merge", [False, True])
async def test_terminal_review_base_ref_drift_after_verifier_cannot_complete(
    db_session,
    db_factory,
    auto_merge,
):
    """The final locked CAS must recheck the Review's frozen target branch."""

    run, _repo = await _scope(db_session, auto_merge=auto_merge)
    workspace = FakeWorkspace()
    publisher = RacingReviewBaseRefPublisher(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
    )
    monitor_id = await _to_monitoring(
        controller,
        db_factory,
        workspace,
        run.id,
    )
    review_id = await _set_monitor_terminal(
        db_factory,
        run_id=run.id,
        monitor_id=monitor_id,
        auto_merge=auto_merge,
    )

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        paused = await db.get(DeliveryRun, run.id, populate_existing=True)
        review = await db.get(PRReview, review_id, populate_existing=True)
        assert (review.base_ref, review.base_sha, review.head_sha) == (
            "release/1.x",
            paused.base_sha,
            paused.head_sha,
        )
        assert (paused.phase, paused.activity, paused.outcome) == (
            "monitoring",
            "paused",
            None,
        )
        assert f"{paused.policy_snapshot['terminal']} subject changed" in (
            paused.pause_reason or ""
        )
    assert publisher.verify_calls == 1


@pytest.mark.asyncio
async def test_auto_merge_blocked_repair_clean_then_exact_merged(
    db_session,
    db_factory,
):
    run, repo = await _scope(db_session, auto_merge=True)
    run_id = run.id
    repo_id = repo.id
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(
        db_factory,
        review_verdicts=["approved", "approved"],
    )
    publisher = FakePublisher(db_factory)
    dispatcher = FakeDispatcher()
    controller = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        dispatcher=dispatcher,
    )

    monitor_id = await _to_monitoring(
        controller,
        db_factory,
        workspace,
        run_id,
    )
    async with db_factory() as db:
        blocked = await db.get(DeliveryRun, run_id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        assert (blocked.phase, blocked.activity, blocked.cycle_count) == (
            "monitoring",
            "waiting",
            1,
        )
        assert monitor.status == "waiting_for_fix"

    assert await controller.reconcile_run(run_id)
    async with db_factory() as db:
        repairing = await db.get(DeliveryRun, run_id)
        repair_cycle = await db.get(DeliveryCycle, repairing.current_cycle_id)
        assert (repairing.phase, repairing.activity, repairing.cycle_count) == (
            "planning",
            "ready",
            2,
        )
        assert repair_cycle.trigger_kind == "pr_monitor_blocked"

    await _to_publishing(
        controller,
        db_factory,
        workspace,
        run_id,
        head=HEAD_TWO,
        tree=TREE_TWO,
    )
    assert await controller.reconcile_run(run_id)

    async with db_factory() as db:
        repaired = await db.get(DeliveryRun, run_id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        assert (repaired.phase, repaired.activity, repaired.head_sha) == (
            "monitoring",
            "waiting",
            HEAD_TWO,
        )
        assert monitor.status == "reviewing"
        assert monitor.current_head_sha == HEAD_TWO
        review = PRReview(
            repo_id=repo_id,
            monitor_run_id=monitor.id,
            pr_number=repaired.pr_number,
            base_ref="main",
            base_sha=repaired.base_sha,
            head_sha=repaired.head_sha,
            delivery_id=f"delivery:{run_id}:{HEAD_TWO}",
            pr_title=repaired.title,
            pr_author="delivery-bot",
            pr_url=repaired.pr_url,
            status="merged",
            action_nonce="f" * 48,
            action_taken="approved_merged",
            publishing_actor="ccm-bot",
            publishing_started_at=datetime.utcnow(),
            merge_method="merge",
            completed_at=datetime.utcnow(),
        )
        db.add(review)
        await db.flush()
        monitor.current_review_id = review.id
        monitor.status = "merged"
        monitor.completed_at = datetime.utcnow()
        monitor.state_version += 1
        await db.commit()

    assert await controller.reconcile_run(run_id)
    async with db_factory() as db:
        completed = await db.get(DeliveryRun, run_id)
        developer = await db.get(Task, completed.developer_task_id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        review = await db.get(PRReview, monitor.current_review_id)
        assert (completed.phase, completed.activity, completed.outcome) == (
            "done",
            "terminal",
            "success",
        )
        assert completed.cycle_count == 2
        assert completed.head_sha == HEAD_TWO
        assert developer.status == "completed"
        assert monitor.status == "merged"
        assert review.status == "merged"
        assert review.action_taken == "approved_merged"
    assert publisher.pr_calls == 2
    assert publisher.monitor_calls == 2
    assert publisher.verify_calls == 1
    assert dispatcher.wake_count == 2


@pytest.mark.asyncio
async def test_dispatcher_success_history_advances_exact_developer_turn(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    dispatcher = _real_dispatcher(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
        dispatcher=dispatcher,
    )
    await _to_coding_pending(controller, run.id)

    task_id, historical_instance_id = await _finish_code_with_dispatcher(
        dispatcher,
        db_factory,
        workspace,
        run.id,
        success=True,
    )
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        refreshed = await db.get(DeliveryRun, run.id)
        task = await db.get(Task, task_id)
        turn = await db.scalar(
            select(DeliveryTurn).where(DeliveryTurn.run_id == run.id)
        )
        assert (refreshed.phase, refreshed.activity) == ("pre_review", "ready")
        assert task.status == "delivery_waiting"
        assert task.instance_id == historical_instance_id
        assert turn.status == "completed"
        assert turn.task_instance_id == historical_instance_id


@pytest.mark.asyncio
async def test_dispatcher_failure_history_fails_exact_developer_turn(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    dispatcher = _real_dispatcher(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
        dispatcher=dispatcher,
    )
    await _to_coding_pending(controller, run.id)

    task_id, historical_instance_id = await _finish_code_with_dispatcher(
        dispatcher,
        db_factory,
        workspace,
        run.id,
        success=False,
    )
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        refreshed = await db.get(DeliveryRun, run.id)
        task = await db.get(Task, task_id)
        turn = await db.scalar(
            select(DeliveryTurn).where(DeliveryTurn.run_id == run.id)
        )
        assert (refreshed.phase, refreshed.activity, refreshed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert refreshed.error_code == "developer_turn_failed"
        assert task.status == "failed"
        assert task.instance_id == historical_instance_id
        assert turn.status == "failed"


@pytest.mark.asyncio
async def test_terminal_waits_for_reverse_owner_then_allows_reused_slot(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    await _to_coding_pending(controller, run.id)
    workspace.advance(HEAD_ONE, TREE_ONE)
    now = datetime.utcnow()
    async with db_factory() as db:
        task = await db.get(Task, run.developer_task_id)
        instance = Instance(
            name="delivery-owner-not-released",
            status="running",
            current_task_id=task.id,
            pid=12345,
            started_at=now,
        )
        db.add(instance)
        await db.flush()
        task.status = "completed"
        task.instance_id = instance.id
        task.started_at = now
        task.completed_at = now + timedelta(seconds=1)
        task.session_id = "settlement-session"
        await db.commit()
        instance_id = instance.id

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        waiting = await db.get(DeliveryRun, run.id)
        assert (waiting.phase, waiting.activity) == ("coding", "running")

        replacement = Task(title="replacement slot owner", status="executing")
        db.add(replacement)
        await db.flush()
        instance = await db.get(Instance, instance_id)
        instance.current_task_id = replacement.id
        instance.pid = 54321
        await db.commit()

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        advanced = await db.get(DeliveryRun, run.id)
        task = await db.get(Task, advanced.developer_task_id)
        instance = await db.get(Instance, instance_id)
        assert (advanced.phase, advanced.activity) == ("pre_review", "ready")
        assert task.instance_id == instance_id
        assert instance.current_task_id != task.id


@pytest.mark.asyncio
async def test_review_changes_requested_starts_fresh_plan_cycle(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(
        db_factory,
        review_verdicts=["changes_requested", "approved"],
    )
    controller = _controller(
        db_factory,
        workspace,
        capabilities,
        FakePublisher(db_factory),
    )

    await _to_coding_pending(controller, run.id)
    await _complete_code(db_factory, workspace, run.id, HEAD_ONE, TREE_ONE)
    await controller.reconcile_run(run.id)
    await controller.reconcile_run(run.id)
    await controller.reconcile_run(run.id)

    async with db_factory() as db:
        refreshed = await db.get(DeliveryRun, run.id)
        cycle = await db.get(DeliveryCycle, refreshed.current_cycle_id)
        task = await db.get(Task, refreshed.developer_task_id)
        assert (refreshed.phase, refreshed.activity) == ("planning", "ready")
        assert refreshed.cycle_count == 2
        assert cycle.cycle_number == 2
        assert cycle.trigger_kind == "pre_review_changes_requested"
        assert cycle.trigger_payload["findings"][0]["severity"] == "high"
        assert task.status == "delivery_waiting"


@pytest.mark.asyncio
async def test_developer_no_progress_retries_development_then_fails_at_threshold(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session, max_cycles=4, max_no_progress=2)
    first_cycle_id = run.current_cycle_id
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        capabilities,
        FakePublisher(db_factory),
    )

    await _to_coding_pending(controller, run.id)
    await _complete_code(db_factory, workspace, run.id, BASE_SHA, workspace.tree_sha)
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        retrying = await db.get(DeliveryRun, run.id)
        first_cycle = await db.get(DeliveryCycle, first_cycle_id)
        current_cycle = await db.get(DeliveryCycle, retrying.current_cycle_id)
        assert (retrying.phase, retrying.activity) == ("coding", "ready")
        assert retrying.no_progress_count == 1
        assert retrying.cycle_count == 2
        assert first_cycle.status == "completed"
        assert current_cycle.status == "coding"
        assert current_cycle.plan_version_id == first_cycle.plan_version_id
        assert current_cycle.plan_invocation_id is None
        assert current_cycle.trigger_kind == "developer_no_progress"
        assert current_cycle.trigger_payload["no_progress_count"] == 1
    assert len(capabilities.created) == 1

    assert await controller.reconcile_run(run.id)  # dispatch next code turn
    await _complete_code(db_factory, workspace, run.id, BASE_SHA, workspace.tree_sha)
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        failed = await db.get(DeliveryRun, run.id)
        task = await db.get(Task, failed.developer_task_id)
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert failed.no_progress_count == 2
        assert failed.error_code == "delivery_no_progress"
        assert task.status == "failed"
    assert len(capabilities.created) == 1


@pytest.mark.asyncio
async def test_report_only_developer_completion_ends_successfully_without_commit(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session, max_cycles=4, max_no_progress=2)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        capabilities,
        FakePublisher(db_factory),
    )

    await _to_coding_pending(controller, run.id)
    await _complete_report_only(db_factory, workspace, run.id)
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        completed = await db.get(DeliveryRun, run.id)
        cycle = await db.get(DeliveryCycle, completed.current_cycle_id)
        task = await db.get(Task, completed.developer_task_id)
        assert (completed.phase, completed.activity, completed.outcome) == (
            "done",
            "terminal",
            "success",
        )
        assert completed.cycle_count == 1
        assert completed.no_progress_count == 0
        assert cycle.status == "completed"
        assert task.status == "completed"
        assert workspace.commit_calls == 1


@pytest.mark.asyncio
async def test_report_marker_from_an_older_turn_does_not_complete_current_turn(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session, max_cycles=4, max_no_progress=2)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )

    await _to_coding_pending(controller, run.id)
    await _complete_code(db_factory, workspace, run.id, BASE_SHA, workspace.tree_sha)
    async with db_factory() as db:
        current = await db.get(DeliveryRun, run.id)
        task = await db.get(Task, current.developer_task_id)
        db.add(
            LogEntry(
                instance_id=task.instance_id,
                task_id=task.id,
                task_retry_count=task.retry_count,
                task_turn_generation=current.turn_count - 1,
                event_type="message",
                role="assistant",
                content="DELIVERY_RESULT: REPORT_COMPLETE",
                is_error=False,
            )
        )
        await db.commit()

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        retrying = await db.get(DeliveryRun, run.id)
        assert (retrying.phase, retrying.activity, retrying.outcome) == (
            "coding",
            "ready",
            None,
        )
        assert retrying.no_progress_count == 1


@pytest.mark.asyncio
async def test_developer_no_progress_fails_at_cycle_budget(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session, max_cycles=1, max_no_progress=2)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )

    await _to_coding_pending(controller, run.id)
    await _complete_code(db_factory, workspace, run.id, BASE_SHA, workspace.tree_sha)
    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        failed = await db.get(DeliveryRun, run.id)
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert failed.no_progress_count == 1
        assert failed.error_code == "delivery_max_cycles"


@pytest.mark.asyncio
async def test_pr_monitor_blocking_evidence_starts_fresh_plan_cycle(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    monitor_id = await _to_monitoring(controller, db_factory, workspace, run.id)

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        refreshed = await db.get(DeliveryRun, run.id)
        cycle = await db.get(DeliveryCycle, refreshed.current_cycle_id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        assert (refreshed.phase, refreshed.activity) == ("planning", "ready")
        assert refreshed.cycle_count == 2
        assert cycle.trigger_kind == "pr_monitor_blocked"
        assert cycle.trigger_payload["monitor_status"] == "waiting_for_fix"
        assert cycle.trigger_payload["evidence"]["findings"]
        assert monitor.developer_task_id is None


@pytest.mark.asyncio
async def test_blocked_run_can_create_next_plan_after_flags_are_disabled(
    db_session,
    db_factory,
    monkeypatch,
    registered_plan_capability,
):
    del registered_plan_capability
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    publisher = FakePublisher(db_factory)
    first = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
    )
    await _to_monitoring(first, db_factory, workspace, run.id)
    assert await first.reconcile_run(run.id)

    monkeypatch.setattr(settings, "delivery_loop_enabled", False)
    monkeypatch.setattr(settings, "capability_core_enabled", False)
    recovered = _controller(
        db_factory,
        workspace,
        CoreDeliveryCapabilityGateway(db_factory),
        publisher,
        owner="controller-after-disable",
        enabled=None,
    )
    await recovered.start()
    try:
        async with db_factory() as db:
            refreshed = await db.get(DeliveryRun, run.id)
            cycle = await db.get(DeliveryCycle, refreshed.current_cycle_id)
            invocation = await db.get(
                CapabilityInvocation,
                cycle.plan_invocation_id,
            )
            assert refreshed.cycle_count == 2
            assert (refreshed.phase, refreshed.activity) == ("planning", "waiting")
            assert invocation is not None
            assert invocation.task_id == refreshed.developer_task_id
            assert invocation.source == "delivery_controller"
            assert invocation.idempotency_key == f"delivery:{run.id}:cycle:2:plan"
    finally:
        await recovered.shutdown()


@pytest.mark.asyncio
async def test_restart_recovers_waiting_plan_from_durable_cycle_handle(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    publisher = FakePublisher(db_factory)
    first = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        owner="old-process",
    )
    assert await first.reconcile_run(run.id)

    recovered = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        owner="new-process",
    )
    assert await recovered.reconcile_run(run.id)

    async with db_factory() as db:
        refreshed = await db.get(DeliveryRun, run.id)
        cycle = await db.get(DeliveryCycle, refreshed.current_cycle_id)
        assert (refreshed.phase, refreshed.activity) == ("coding", "ready")
        assert cycle.plan_version_id is not None
    assert [key for kind, key in capabilities.created if ":plan" in key] == [
        f"delivery:{run.id}:cycle:1:plan"
    ]


@pytest.mark.asyncio
async def test_two_controllers_cannot_publish_same_action_concurrently(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    gate = asyncio.Event()
    publisher = FakePublisher(db_factory, gate=gate)
    first = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        owner="first-controller",
    )
    second = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        owner="second-controller",
    )
    await _to_publishing(first, db_factory, workspace, run.id)

    publishing = asyncio.create_task(first.reconcile_run(run.id))
    await asyncio.wait_for(publisher.entered.wait(), timeout=2)
    assert await second.reconcile_run(run.id) is False
    assert publisher.pr_calls == 1
    gate.set()
    assert await publishing

    async with db_factory() as db:
        actions = list(
            (
                await db.execute(
                    select(DeliveryAction).where(DeliveryAction.run_id == run.id)
                )
            ).scalars()
        )
        assert len(actions) == 1
        assert actions[0].status == "succeeded"


@pytest.mark.asyncio
async def test_restart_recovers_indeterminate_publish_outbox_without_duplicate_row(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    interrupted_publisher = IndeterminatePublisher(db_factory)
    first = _controller(
        db_factory,
        workspace,
        capabilities,
        interrupted_publisher,
        owner="publisher-before-restart",
    )
    await _to_publishing(first, db_factory, workspace, run.id)
    assert await first.reconcile_run(run.id)

    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        assert action.status == "unknown"
        assert action.attempts == 1

    await asyncio.sleep(0.02)
    recovered_publisher = FakePublisher(db_factory)
    recovered = _controller(
        db_factory,
        workspace,
        capabilities,
        recovered_publisher,
        owner="publisher-after-restart",
    )
    assert await recovered.reconcile_run(run.id)

    async with db_factory() as db:
        actions = list(
            (
                await db.execute(
                    select(DeliveryAction).where(DeliveryAction.run_id == run.id)
                )
            ).scalars()
        )
        refreshed = await db.get(DeliveryRun, run.id)
        assert len(actions) == 1
        assert actions[0].status == "succeeded"
        assert actions[0].attempts == 2
        assert (refreshed.phase, refreshed.activity) == ("monitoring", "waiting")
    assert recovered_publisher.pr_calls == 1


@pytest.mark.asyncio
async def test_permanent_error_after_possible_write_stays_unknown_and_reconciles(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    publisher = PermanentAfterWritePublisher(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
    )
    await _to_publishing(controller, db_factory, workspace, run.id)

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        uncertain = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "unknown"
        assert action.active_run_id == run.id
        assert "identity conflict" in (action.last_error or "")
        assert (uncertain.phase, uncertain.activity, uncertain.outcome) == (
            "publishing",
            "running",
            None,
        )
        action.next_attempt_at = datetime.utcnow() - timedelta(seconds=1)
        await db.commit()

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        recovered = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "succeeded"
        assert action.attempts == 2
        assert (recovered.phase, recovered.activity, recovered.outcome) == (
            "monitoring",
            "waiting",
            None,
        )
    assert publisher.pr_calls == 2


@pytest.mark.asyncio
async def test_successful_create_intent_is_atomically_upgraded_to_pr_receipt(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    publisher = IntentBeforeSuccessPublisher(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
    )
    await _to_publishing(controller, db_factory, workspace, run.id)

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "succeeded"
        assert action.result["schema_version"] == 1
        assert action.result["pr_number"] == 73
        assert action.remote_id == "73"
        assert stored.pr_number == 73
        assert (stored.phase, stored.activity) == ("monitoring", "waiting")
    assert publisher.pr_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_state", ["closed", "merged"])
async def test_terminal_historical_receipt_fails_without_publisher_replay(
    db_session,
    db_factory,
    terminal_state,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    publisher = TerminalHistoryPublisher(db_factory, state=terminal_state)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
    )
    await _to_publishing(controller, db_factory, workspace, run.id)

    # First owner persists the remote terminal identity and leaves the action
    # unknown; a restarted owner projects that receipt without invoking the
    # publisher (and therefore without any chance to create a replacement).
    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        assert action.status == "unknown"
        assert action.result["kind"] == "pull_request_history_conflict"

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        failed = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "failed"
        assert action.active_run_id is None
        assert action.lease_owner is None
        assert action.lease_expires_at is None
        assert action.next_attempt_at is None
        assert action.completed_at is not None
        assert action.remote_id == "73"
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert f"already {terminal_state}" in (failed.error_message or "")
    assert publisher.pr_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_kind",
    ["pull_request_history_ambiguous", "pull_request_create_unresolved"],
)
async def test_unresolved_terminal_receipt_fails_without_publisher_replay(
    db_session,
    db_factory,
    receipt_kind,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    publisher = UnresolvedTerminalReceiptPublisher(
        db_factory,
        kind=receipt_kind,
    )
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
    )
    await _to_publishing(controller, db_factory, workspace, run.id)

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        assert action.status == "unknown"
        assert action.result["kind"] == receipt_kind
        assert action.remote_id is None
        assert action.remote_url is None

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        failed = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "failed"
        assert action.active_run_id is None
        assert action.lease_owner is None
        assert action.lease_expires_at is None
        assert action.next_attempt_at is None
        assert action.completed_at is not None
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert receipt_kind in (failed.error_message or "")
    assert publisher.pr_calls == 1


@pytest.mark.asyncio
async def test_bound_history_ambiguity_reconciles_without_replacement_pr(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    publisher = BoundHistoryAmbiguityThenSuccessPublisher(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
    )
    await _to_publishing(controller, db_factory, workspace, run.id)

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        stored = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "unknown"
        assert action.result["kind"] == "pull_request_history_ambiguous"
        assert action.remote_id is None
        assert stored.pr_number == 73
        action.next_attempt_at = datetime.utcnow() - timedelta(seconds=1)
        await db.commit()

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        recovered = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "succeeded"
        assert action.result["schema_version"] == 1
        assert action.remote_id == "73"
        assert (recovered.phase, recovered.activity, recovered.outcome) == (
            "monitoring",
            "waiting",
            None,
        )
    assert publisher.pr_calls == 2


@pytest.mark.asyncio
async def test_restart_consumes_terminal_receipt_left_under_old_action_lease(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    publisher = CrashAfterTerminalHistoryPublisher(db_factory)
    first = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
        owner="publisher-killed-after-receipt",
    )
    await _to_publishing(first, db_factory, workspace, run.id)

    with pytest.raises(asyncio.CancelledError):
        await first.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        old_token = action.lease_owner
        assert action.status == "leased"
        assert old_token
        assert action.lease_expires_at is not None
        assert action.result["kind"] == "pull_request_history_conflict"

    restarted = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
        owner="publisher-after-process-death",
    )
    assert await restarted.reconcile_run(run.id)

    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        failed = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "failed"
        assert action.active_run_id is None
        assert action.lease_owner is None
        assert action.lease_expires_at is None
        assert action.completed_at is not None
        assert old_token != action.lease_owner
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
    assert publisher.pr_calls == 1


@pytest.mark.asyncio
async def test_proven_no_effect_preflight_failure_terminalizes_action_and_run(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        NoEffectPreflightPublisher(db_factory),
    )
    await _to_publishing(controller, db_factory, workspace, run.id)

    assert await controller.reconcile_run(run.id)
    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        failed = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "failed"
        assert action.active_run_id is None
        assert "frozen publishing policy" in (action.last_error or "")
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )


@pytest.mark.asyncio
async def test_restart_binds_monitor_from_durable_pr_receipt_without_republishing(
    db_session,
    db_factory,
):
    run, repo = await _scope(db_session)
    workspace = FakeWorkspace()
    capabilities = FakeCapabilities(db_factory)
    publisher = MonitorFailsOncePublisher(db_factory)
    first = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        owner="publisher-before-monitor-failure",
    )
    await _to_publishing(first, db_factory, workspace, run.id)
    assert await first.reconcile_run(run.id)

    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        interrupted = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "unknown"
        assert action.remote_id == "73"
        assert action.remote_url == (
            "https://github.com/acme/delivery-controller/pull/73"
        )
        assert action.result == {
            "schema_version": 1,
            "repo_id": repo.id,
            "pr_number": 73,
            "url": "https://github.com/acme/delivery-controller/pull/73",
            "base_sha": BASE_SHA,
            "head_sha": HEAD_ONE,
            "head_branch": interrupted.delivery_branch,
            "head_repo_full_name": repo.repo_full_name,
        }
        assert (interrupted.phase, interrupted.activity) == (
            "publishing",
            "running",
        )
        assert (interrupted.pr_number, interrupted.pr_url) == (
            73,
            "https://github.com/acme/delivery-controller/pull/73",
        )
        action.next_attempt_at = datetime.utcnow() - timedelta(seconds=1)
        await db.commit()

    recovered = _controller(
        db_factory,
        workspace,
        capabilities,
        publisher,
        owner="publisher-after-monitor-failure",
    )
    assert await recovered.reconcile_run(run.id)

    async with db_factory() as db:
        action = await db.scalar(
            select(DeliveryAction).where(DeliveryAction.run_id == run.id)
        )
        completed = await db.get(DeliveryRun, run.id, populate_existing=True)
        assert action.status == "succeeded"
        assert action.remote_id == "73"
        assert action.result["monitor_run_id"] == completed.pr_monitor_run_id
        assert (completed.phase, completed.activity) == ("monitoring", "waiting")
    assert publisher.pr_calls == 1
    assert publisher.monitor_calls == 2


@pytest.mark.asyncio
async def test_stale_developer_generation_pauses_fail_closed(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    await _to_coding_pending(controller, run.id)
    now = datetime.utcnow()
    async with db_factory() as db:
        task = await db.get(Task, run.developer_task_id)
        task.retry_count += 1
        task.status = "completed"
        task.started_at = now
        task.completed_at = now + timedelta(seconds=1)
        await db.commit()

    await controller.reconcile_run(run.id)

    async with db_factory() as db:
        paused = await db.get(DeliveryRun, run.id)
        assert paused.activity == "paused"
        assert "retry generation" in (paused.pause_reason or "")


@pytest.mark.asyncio
async def test_resume_running_code_observes_existing_turn_without_redispatch(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    dispatcher = FakeDispatcher()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
        dispatcher=dispatcher,
    )
    await _to_coding_pending(controller, run.id)
    async with db_factory() as db:
        locked = await lock_run(db, run.id)
        await apply_run_event(
            db,
            run=locked,
            event=DeliveryReducerEvent("pause", {"reason": "maintenance"}),
            actor_kind="user",
        )
        await apply_run_event(
            db,
            run=locked,
            event=DeliveryReducerEvent("resume"),
            actor_kind="user",
        )
        await db.commit()

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        refreshed = await db.get(DeliveryRun, run.id)
        assert (refreshed.phase, refreshed.activity) == ("coding", "running")
        assert refreshed.turn_count == 1
    assert dispatcher.wake_count == 1


@pytest.mark.asyncio
async def test_stale_ready_to_merge_pr_head_never_completes_delivery(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    monitor_id = await _to_monitoring(controller, db_factory, workspace, run.id)
    async with db_factory() as db:
        monitor = await db.get(PRMonitorRun, monitor_id)
        monitor.current_head_sha = HEAD_TWO
        monitor.status = "ready_to_merge"
        monitor.state_version += 1
        await db.commit()

    await controller.reconcile_run(run.id)

    async with db_factory() as db:
        paused = await db.get(DeliveryRun, run.id)
        assert paused.activity == "paused"
        assert paused.outcome is None
        assert "unowned base/head" in (paused.pause_reason or "")


@pytest.mark.asyncio
async def test_current_review_error_terminalizes_delivery_instead_of_waiting(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    monitor_id = await _to_monitoring(
        controller,
        db_factory,
        workspace,
        run.id,
    )
    async with db_factory() as db:
        delivery = await db.get(DeliveryRun, run.id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        review = PRReview(
            monitor_run_id=monitor.id,
            repo_id=monitor.repo_id,
            pr_number=monitor.pr_number,
            base_ref="main",
            base_sha=monitor.current_base_sha,
            head_sha=monitor.current_head_sha,
            pr_title="failed delivery review",
            pr_author="reviewer",
            pr_url=delivery.pr_url,
            status="error",
            action_taken="error",
            review_summary="review output violated the structured contract",
        )
        db.add(review)
        await db.flush()
        monitor.current_review_id = review.id
        monitor.status = "reviewing"
        monitor.state_version += 1
        await db.commit()

    await controller.reconcile_run(run.id)

    async with db_factory() as db:
        failed = await db.get(DeliveryRun, run.id)
        developer = await db.get(Task, failed.developer_task_id)
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert failed.error_code == "pr_review_error"
        assert failed.error_message == (
            "review output violated the structured contract"
        )
        assert developer.status == "failed"
        assert developer.error_message == failed.error_message


@pytest.mark.asyncio
async def test_review_error_monitor_version_race_cannot_fail_new_generation(
    db_session,
    db_factory,
    monkeypatch,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )
    monitor_id = await _to_monitoring(
        controller,
        db_factory,
        workspace,
        run.id,
    )
    async with db_factory() as db:
        delivery = await db.get(DeliveryRun, run.id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        review = PRReview(
            monitor_run_id=monitor.id,
            repo_id=monitor.repo_id,
            pr_number=monitor.pr_number,
            base_ref="main",
            base_sha=monitor.current_base_sha,
            head_sha=monitor.current_head_sha,
            pr_title="racing failed review",
            pr_author="reviewer",
            pr_url=delivery.pr_url,
            status="error",
            action_taken="error",
            review_summary="stale reviewer transport failure",
        )
        db.add(review)
        await db.flush()
        monitor.current_review_id = review.id
        monitor.status = "reviewing"
        monitor.state_version += 1
        await db.commit()

    fail_current = controller._fail_current_review_error

    async def advance_monitor_generation(lease, **kwargs):
        async with db_factory() as db:
            monitor = await db.get(PRMonitorRun, monitor_id)
            monitor.status = "waiting_ci"
            monitor.state_version += 1
            await db.commit()
        await fail_current(lease, **kwargs)

    monkeypatch.setattr(
        controller,
        "_fail_current_review_error",
        advance_monitor_generation,
    )
    await controller.reconcile_run(run.id)

    async with db_factory() as db:
        waiting = await db.get(DeliveryRun, run.id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        assert (waiting.phase, waiting.activity, waiting.outcome) == (
            "monitoring",
            "waiting",
            None,
        )
        assert waiting.error_code is None
        assert monitor.status == "waiting_ci"


@pytest.mark.asyncio
async def test_ready_monitor_version_race_cannot_cross_terminal_cas(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    workspace = FakeWorkspace()
    publisher = RacingReadyPublisher(db_factory)
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        publisher,
    )
    monitor_id = await _to_monitoring(controller, db_factory, workspace, run.id)
    await _set_monitor_terminal(
        db_factory,
        run_id=run.id,
        monitor_id=monitor_id,
        auto_merge=False,
    )

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        paused = await db.get(DeliveryRun, run.id)
        monitor = await db.get(PRMonitorRun, monitor_id)
        assert paused.activity == "paused"
        assert paused.outcome is None
        assert monitor.status == "waiting_ci"
        assert "ready_to_merge subject changed" in (paused.pause_reason or "")
    assert publisher.verify_calls == 1


@pytest.mark.asyncio
async def test_permanent_capability_configuration_error_fails_run(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    controller = _controller(
        db_factory,
        FakeWorkspace(),
        FailingCapabilities(CapabilityValidationError("invalid plan route")),
        FakePublisher(db_factory),
    )

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        failed = await db.get(DeliveryRun, run.id)
        assert (failed.phase, failed.activity, failed.outcome) == (
            "done",
            "terminal",
            "failed",
        )
        assert failed.error_code == "delivery_capability_unavailable"
        assert failed.error_message == "invalid plan route"


@pytest.mark.asyncio
async def test_capability_active_slot_conflict_pauses_run(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    controller = _controller(
        db_factory,
        FakeWorkspace(),
        FailingCapabilities(CapabilityConflictError("active slot is occupied")),
        FakePublisher(db_factory),
    )

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        paused = await db.get(DeliveryRun, run.id)
        assert paused.activity == "paused"
        assert paused.outcome is None
        assert paused.pause_reason == "active slot is occupied"


@pytest.mark.asyncio
async def test_workspace_validation_error_pauses_run_fail_closed(
    db_session,
    db_factory,
):
    run, _repo = await _scope(db_session)
    controller = _controller(
        db_factory,
        FailingWorkspace(),
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )

    assert await controller.reconcile_run(run.id)

    async with db_factory() as db:
        paused = await db.get(DeliveryRun, run.id)
        assert paused.activity == "paused"
        assert paused.outcome is None
        assert "worktree ownership changed" in (paused.pause_reason or "")


@pytest.mark.asyncio
async def test_controller_start_wake_stop_lifecycle(db_factory):
    workspace = FakeWorkspace()
    controller = _controller(
        db_factory,
        workspace,
        FakeCapabilities(db_factory),
        FakePublisher(db_factory),
    )

    await controller.start()
    assert controller.is_running
    controller.wake()
    await asyncio.sleep(0.02)
    await controller.stop()

    assert not controller.is_running
