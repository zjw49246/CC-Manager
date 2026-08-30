import hashlib
import os
import re
import subprocess
import time
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError

from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRMonitorRun,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task
from backend.models.worker import Worker
from backend.tests.worker_termination_helpers import (
    persist_active_worker_receipt,
)


BASE_SHA = "1" * 40
HEAD_SHA = "a" * 40


def _patch_text() -> str:
    return (
        "diff --git a/backend/example.py b/backend/example.py\n"
        "--- a/backend/example.py\n"
        "+++ b/backend/example.py\n"
        "@@ -1 +1 @@\n"
        "-raise RuntimeError()\n"
        "+return default_value\n"
    )


def _patch_terminal() -> str:
    return f"PR_REVIEW_PATCH_BEGIN\n{_patch_text()}PR_REVIEW_PATCH_END"


async def _seed_finding(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="codex",
        review_mode="panel",
    )
    db_session.add(repo)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=7,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        pr_title="Fix issue",
        pr_author="alice",
        pr_url="https://github.com/owner/repo/pull/7",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    monitor_run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=7,
        current_base_sha=BASE_SHA,
        current_head_sha=HEAD_SHA,
        current_review_id=review.id,
        head_repo_full_name="fork-owner/repo",
        head_branch="feature/fix",
    )
    db_session.add(monitor_run)
    await db_session.flush()
    review.monitor_run_id = monitor_run.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior",
        provider="codex",
        status="completed",
        prompt_policy_hash="p" * 64,
        guide_pack_hash="g" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="f" * 64,
        role="senior",
        severity="high",
        category="correctness",
        path="backend/example.py",
        line=12,
        title="Unhandled empty value",
        evidence="Empty values raise unexpectedly.",
        impact="Valid requests fail.",
        required_fix="Return the documented default.",
        test="Cover the empty-value branch.",
        thread_nonce="n" * 48,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
    )
    db_session.add(finding)
    await db_session.commit()
    return repo, review, finding


async def _seed_confirmable_action(
    db_session,
    *,
    actor_user_id: int = 12,
    receipt: str = "download-receipt-7",
    downloaded_at: datetime | None = None,
    token_expires_at: int | None = None,
):
    from backend.services.pr_review_fix import _confirmation_token

    repo, review, finding = await _seed_finding(db_session)
    patch_text = _patch_text()
    patch_sha = hashlib.sha256(patch_text.encode()).hexdigest()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="awaiting_confirmation",
        idempotency_key=f"confirmable-{actor_user_id}-{receipt}",
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        patch_sha256=patch_sha,
        download_receipt_hash=hashlib.sha256(receipt.encode()).hexdigest(),
        downloaded_by_user_id=actor_user_id,
        downloaded_at=(
            downloaded_at or datetime.utcnow() - timedelta(seconds=1)
        ),
        result={
            "protocol_version": 1,
            "patch": patch_text,
            "action_nonce": "nonce-confirm-7",
            "allowed_files": [finding.path],
            "head_repo_full_name": "fork-owner/repo",
            "head_ref": "feature/fix",
        },
    )
    db_session.add(action)
    await db_session.flush()
    expires_at = token_expires_at or int(time.time()) + 3600
    token = _confirmation_token(
        secret=repo.webhook_secret,
        action_id=action.id,
        head_sha=HEAD_SHA,
        patch_sha256=patch_sha,
        expires_at=expires_at,
    )
    action.result = {
        **action.result,
        "confirmation_token": token,
        "confirmation_expires_at": expires_at,
    }
    await db_session.commit()
    return repo, review, finding, action, token, receipt, patch_sha


async def _seed_terminal_fix_action(
    db_session,
    *,
    task_status: str = "completed",
    retry_count: int = 2,
):
    repo, review, finding = await _seed_finding(db_session)
    task = Task(
        title="terminal fix",
        description="immutable",
        status=task_status,
        retry_count=retry_count,
        started_at=datetime.utcnow() - timedelta(seconds=5),
        completed_at=datetime.utcnow(),
        metadata_={},
    )
    db_session.add(task)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="running",
        idempotency_key=f"terminal-fix-{task_status}-{retry_count}",
        task_id=task.id,
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        result={
            "allowed_files": [finding.path],
            "action_nonce": "nonce-terminal",
            "head_repo_full_name": "fork-owner/repo",
            "head_ref": "feature/fix",
        },
    )
    db_session.add(action)
    await db_session.flush()
    task.metadata_ = {
        "pr_finding_action_id": action.id,
        "expected_head_sha": HEAD_SHA,
    }
    if task_status == "completed":
        db_session.add(LogEntry(
            task_id=task.id,
            task_retry_count=retry_count,
            event_type="result",
            content=_patch_terminal(),
            timestamp=task.started_at + timedelta(seconds=1),
        ))
    await db_session.commit()
    return repo, review, finding, action, task


def test_patch_protocol_accepts_only_the_exact_allowed_file():
    from backend.services.pr_review_fix import parse_patch_output

    assert parse_patch_output(
        _patch_terminal(), allowed_files={"backend/example.py"}
    ) == _patch_text()


def test_patch_protocol_rejects_unbounded_model_chatter():
    from backend.services.pr_review_fix import PatchProtocolError, parse_patch_output

    with pytest.raises(PatchProtocolError, match="exactly one"):
        parse_patch_output(
            f"explanation\n{_patch_terminal()}",
            allowed_files={"backend/example.py"},
        )


def test_patch_protocol_rejects_a_second_file_header_inside_one_diff_block():
    from backend.services.pr_review_fix import PatchProtocolError, parse_patch_output

    injected = (
        "diff --git a/backend/example.py b/backend/example.py\n"
        "--- a/backend/example.py\n"
        "+++ b/backend/example.py\n"
        "@@ -1 +1 @@\n"
        "-raise RuntimeError()\n"
        "+return default_value\n"
        "--- a/backend/secret.py\n"
        "+++ b/backend/secret.py\n"
        "@@ -1 +1 @@\n"
        "-secret = False\n"
        "+secret = True\n"
    )

    with pytest.raises(PatchProtocolError, match="outside a hunk"):
        parse_patch_output(
            f"PR_REVIEW_PATCH_BEGIN\n{injected}PR_REVIEW_PATCH_END",
            allowed_files={"backend/example.py"},
        )


def test_patch_protocol_rejects_unbounded_hunk_coordinates():
    from backend.services.pr_review_fix import PatchProtocolError, parse_patch_output

    oversized = _patch_text().replace("@@ -1 +1 @@", "@@ -12345678901 +1 @@")
    with pytest.raises(PatchProtocolError, match="outside a hunk"):
        parse_patch_output(
            f"PR_REVIEW_PATCH_BEGIN\n{oversized}PR_REVIEW_PATCH_END",
            allowed_files={"backend/example.py"},
        )


@pytest.mark.asyncio
async def test_immediate_action_is_idempotent_and_keeps_panel_gate_open(db_session):
    from backend.services.pr_review_actions import create_immediate_finding_action

    _, review, finding = await _seed_finding(db_session)
    first = await create_immediate_finding_action(
        db_session,
        finding_id=finding.id,
        review_id=review.id,
        action_type="human_advice",
        idempotency_key="advice-action-7",
        actor_user_id=12,
        human_advice="Preserve the documented fallback.",
    )
    second = await create_immediate_finding_action(
        db_session,
        finding_id=finding.id,
        review_id=review.id,
        action_type="human_advice",
        idempotency_key="advice-action-7",
        actor_user_id=12,
        human_advice="Preserve the documented fallback.",
    )

    assert first.id == second.id
    await db_session.refresh(finding)
    assert finding.status == "open"


@pytest.mark.asyncio
async def test_immediate_action_cannot_hide_an_active_ai_fix(db_session):
    from backend.services.pr_review_actions import (
        FindingActionConflict,
        create_immediate_finding_action,
    )

    _, review, finding = await _seed_finding(db_session)
    db_session.add(PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="awaiting_confirmation",
        idempotency_key="active-fix-7",
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
    ))
    await db_session.commit()

    with pytest.raises(FindingActionConflict, match="active AI repair"):
        await create_immediate_finding_action(
            db_session,
            finding_id=finding.id,
            review_id=review.id,
            action_type="human_advice",
            idempotency_key="advice-during-fix-7",
            actor_user_id=12,
            human_advice="Try another approach.",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action_type", "status", "slot_kind"),
    (
        ("ai_fix", "pending", "null"),
        ("ai_fix", "pending", "mismatched"),
        ("human_advice", "completed", "matching"),
        ("ai_fix", "completed", "matching"),
    ),
)
async def test_active_fix_slot_check_rejects_invalid_owners(
    db_session,
    action_type,
    status,
    slot_kind,
):
    """The CHECK must reject NULL's SQL three-valued-logic escape hatch."""

    _, _, finding = await _seed_finding(db_session)
    slot = {
        "null": None,
        "matching": finding.id,
        "mismatched": finding.id + 1000,
    }[slot_kind]
    db_session.add(PRFindingAction(
        finding_id=finding.id,
        action_type=action_type,
        status=status,
        idempotency_key=f"invalid-slot-{action_type}-{status}-{slot_kind}",
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=slot,
    ))

    with pytest.raises(IntegrityError, match="ck_pr_finding_actions_active_slot"):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_tokenless_pending_reservation_never_expires_from_app_clock(
    db_session,
):
    from backend.services.pr_review_fix import _expire_creation_reservation

    _, _, finding = await _seed_finding(db_session)
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="pending",
        idempotency_key="corrupt-tokenless-reservation",
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        operation_token=None,
        operation_expires_at=None,
        updated_at=datetime.utcnow() - timedelta(days=365),
    )
    db_session.add(action)
    await db_session.commit()

    assert await _expire_creation_reservation(db_session, action) is False
    refreshed = await db_session.get(
        PRFindingAction,
        action.id,
        populate_existing=True,
    )
    assert refreshed.status == "pending"
    assert refreshed.active_fix_finding_id == finding.id


@pytest.mark.asyncio
async def test_active_idempotent_fix_retry_returns_same_action(db_session):
    """A response-loss retry must survive the reservation rollback fence."""

    from backend.services import pr_review_fix

    repo, review, finding = await _seed_finding(db_session)
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="pending",
        idempotency_key="active-idempotent-retry",
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        operation_token="r" * 64,
        operation_expires_at=datetime.utcnow() + timedelta(minutes=5),
    )
    db_session.add(action)
    await db_session.commit()
    action_id = action.id

    repeated = await pr_review_fix.create_fix_task(
        db_session,
        finding_id=finding.id,
        review_id=review.id,
        repo_id=repo.id,
        idempotency_key="active-idempotent-retry",
        actor_user_id=12,
    )

    assert repeated.id == action_id
    assert repeated.status == "pending"
    assert repeated.operation_token == "r" * 64


@pytest.mark.asyncio
async def test_abandoned_fix_rechecks_review_after_releasing_expiry_fence(
    db_session,
):
    from backend.services import pr_review_fix
    from backend.services.pr_review_actions import FindingActionConflict

    repo, review, finding = await _seed_finding(db_session)
    abandoned = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="pending",
        idempotency_key="abandoned-old-generation",
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        operation_token="a" * 64,
        operation_expires_at=datetime.utcnow() - timedelta(hours=1),
    )
    db_session.add(abandoned)
    await db_session.commit()
    original_expire = pr_review_fix._expire_creation_reservation

    async def expire_then_begin_supersede(db, action):
        changed = await original_expire(db, action)
        stale_review = await db.get(PRReview, review.id)
        stale_review.status = "superseding"
        await db.commit()
        return changed

    with patch.object(
        pr_review_fix,
        "_expire_creation_reservation",
        side_effect=expire_then_begin_supersede,
    ):
        with pytest.raises(FindingActionConflict, match="no longer available"):
            await pr_review_fix.create_fix_task(
                db_session,
                finding_id=finding.id,
                review_id=review.id,
                repo_id=repo.id,
                idempotency_key="replacement-after-abandonment",
                actor_user_id=12,
            )

    expired = await db_session.get(
        PRFindingAction,
        abandoned.id,
        populate_existing=True,
    )
    assert expired.status == "failed"
    replacements = list((await db_session.execute(
        select(PRFindingAction).where(
            PRFindingAction.idempotency_key == "replacement-after-abandonment"
        )
    )).scalars())
    assert replacements == []


@pytest.mark.asyncio
async def test_capture_failure_cleans_reservation_after_concurrent_wal_writer(
    tmp_path,
):
    """A remote failure cannot leave the active slot stuck on BUSY_SNAPSHOT."""

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from backend.database import Base
    from backend.services import pr_review_fix
    from backend.services.pr_review_service import GhError

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'capture-race.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql(
                "PRAGMA journal_mode=WAL"
            )
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with sessions() as creator:
            repo, review, finding = await _seed_finding(creator)
            repo_id = repo.id
            review_id = review.id
            finding_id = finding.id

            async def writer_then_remote_failure(*_args, **_kwargs):
                # Reproduce the old failure precisely: the creator observes a
                # post-reservation WAL snapshot, then another connection wins
                # a write before capture cleanup tries to upgrade it.
                await creator.execute(text("BEGIN"))
                await creator.execute(
                    select(PRFindingAction.id).where(
                        PRFindingAction.idempotency_key
                        == "capture-concurrent-writer"
                    )
                )
                async with sessions() as writer:
                    await writer.execute(
                        update(MonitoredRepo)
                        .where(MonitoredRepo.id == repo_id)
                        .values(status="active")
                    )
                    await writer.commit()
                raise GhError("temporary GitHub 502")

            with (
                patch.object(
                    pr_review_fix,
                    "_verify_current_snapshot",
                    AsyncMock(),
                ),
                patch.object(
                    pr_review_fix,
                    "_load_current_head_route",
                    side_effect=writer_then_remote_failure,
                ),
            ):
                with pytest.raises(GhError, match="temporary GitHub 502"):
                    await pr_review_fix.create_fix_task(
                        creator,
                        finding_id=finding_id,
                        review_id=review_id,
                        repo_id=repo_id,
                        idempotency_key="capture-concurrent-writer",
                        actor_user_id=12,
                    )

        async with sessions() as verifier:
            action = (await verifier.execute(
                select(PRFindingAction).where(
                    PRFindingAction.idempotency_key
                    == "capture-concurrent-writer"
                )
            )).scalar_one()
            assert action.status == "failed"
            assert action.active_fix_finding_id is None
            assert action.operation_token is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_create_fix_task_captures_route_and_uses_tool_free_tag(db_session):
    from backend.services import pr_review_fix

    repo, review, finding = await _seed_finding(db_session)
    with (
        patch.object(pr_review_fix, "_verify_current_snapshot", AsyncMock()),
        patch.object(
            pr_review_fix,
            "_load_current_head_route",
            AsyncMock(return_value=("fork-owner/repo", "feature/fix", HEAD_SHA)),
        ),
        patch.object(
            pr_review_fix,
            "_fetch_exact_head_file",
            AsyncMock(return_value="raise RuntimeError()\n"),
        ),
        patch("backend.main.dispatcher", MagicMock(wake=MagicMock())),
    ):
        action = await pr_review_fix.create_fix_task(
            db_session,
            finding_id=finding.id,
            review_id=review.id,
            repo_id=repo.id,
            idempotency_key="fix-action-7",
            actor_user_id=12,
        )

    task = await db_session.get(Task, action.task_id)
    assert action.status == "running"
    assert action.result["head_repo_full_name"] == "fork-owner/repo"
    assert action.result["head_ref"] == "feature/fix"
    assert task.tags == ["pr-review-fix"]
    assert task.metadata_["pr_finding_action_id"] == action.id
    assert task.execution_user_id is None
    assert task.execution_user_role == "member"
    assert task.execution_mode == "sandbox"
    assert task.execution_principal_kind == "system"
    assert "backend/example.py" in task.description


@pytest.mark.asyncio
async def test_cancel_running_worker_fix_uses_exact_termination_protocol(
    db_session,
):
    """Manager cancellation reaps a Worker fix before releasing its slot."""

    import backend.main
    from backend.services import pr_review_fix
    from backend.services.worker_proxy import get_task_operation_lock
    from backend.services.worker_task_termination import (
        canonical_json_digest,
        receipt_not_found_payload,
    )

    _, _, finding = await _seed_finding(db_session)
    worker = Worker(
        name="fix-worker",
        status="ready",
        private_ip="10.0.0.77",
        auth_token="worker-token",
    )
    db_session.add(worker)
    await db_session.flush()
    task = Task(
        title="worker fix",
        description="immutable",
        status="executing",
        worker_id=worker.id,
        tags=["pr-review-fix"],
    )
    db_session.add(task)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="running",
        idempotency_key="cancel-worker-fix-7",
        task_id=task.id,
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
    )
    db_session.add(action)
    await db_session.flush()
    task.metadata_ = {"pr_finding_action_id": action.id}
    await db_session.commit()
    task_id = task.id
    worker_id = worker.id
    action_id = action.id

    calls = []
    remote_receipt = None
    receipt_path = None

    async def exact_worker_protocol(
        routing_task,
        method,
        path,
        body=None,
        **kwargs,
    ):
        nonlocal receipt_path, remote_receipt
        assert routing_task.id == task_id
        assert routing_task.worker_id == worker_id
        assert kwargs == {
            "require_json": True,
            "operation_lock_held": True,
        }
        operation_path = path.removesuffix("/ack")
        operation_id = operation_path.rsplit("/", 1)[-1]
        assert len(operation_id) == 32
        assert all(char in "0123456789abcdef" for char in operation_id)
        if receipt_path is None:
            receipt_path = operation_path
            assert receipt_path.startswith(
                f"/api/tasks/{task_id}/termination-receipts/"
            )
        else:
            assert operation_path == receipt_path
        calls.append((method, path, body))
        if method == "GET":
            assert body is None
            assert remote_receipt is None
            return receipt_not_found_payload(task_id, operation_id)
        if method == "PUT":
            assert path == receipt_path
            assert body["operation"] == "supersede"
            request_payload = body["request_payload"]
            request_digest = body["request_digest"]
            assert request_digest == canonical_json_digest(request_payload)
            assert request_payload["expected_remote"] == {
                "status": "executing",
                "retry_count": 0,
                "turn_generation": 0,
            }
            result_payload = {
                "version": 2,
                "operation_id": operation_id,
                "task_id": task_id,
                "operation": "supersede",
                "request_digest": request_digest,
                "task": {
                    "id": task_id,
                    "status": "completed",
                    "retry_count": 0,
                    "turn_generation": 0,
                    "instance_id": None,
                    "started_at": None,
                    "completed_at": "2026-01-02T03:04:06.000000",
                    "session_id": None,
                    "error_message": "Superseded by new PR push",
                    "background_active": False,
                },
                "response": {
                    "ok": True,
                    "stopped": True,
                    "cleared_messages": 0,
                },
            }
            remote_receipt = {
                "version": 2,
                "operation_id": operation_id,
                "task_id": task_id,
                "side": "worker",
                "worker_id": None,
                "operation": "supersede",
                "status": "succeeded",
                "state_version": 3,
                "source": {
                    "incarnation_id": "1" * 32,
                    "status": "executing",
                    "retry_count": 0,
                    "turn_generation": 0,
                    "source_log_id": None,
                    "instance_id": None,
                    "started_at": None,
                    "completed_at": None,
                    "session_id": None,
                    "pty_background_generation": None,
                },
                "request_payload": request_payload,
                "request_digest": request_digest,
                "result_payload": result_payload,
                "result_digest": canonical_json_digest(result_payload),
                "attempt_count": 1,
                "reconcile_count": 0,
                "last_error": None,
                "accepted_at": "2026-01-02T03:04:05.000000",
                "completed_at": "2026-01-02T03:04:06.000000",
                "ack_intent_at": None,
                "acknowledged_at": None,
                "created_at": "2026-01-02T03:04:05.000000",
                "updated_at": "2026-01-02T03:04:06.000000",
            }
            return remote_receipt
        assert method == "POST" and path == receipt_path + "/ack"
        assert remote_receipt is not None
        assert body == {
            "request_digest": remote_receipt["request_digest"],
            "result_digest": remote_receipt["result_digest"],
        }
        acknowledged = deepcopy(remote_receipt)
        acknowledged["status"] = "acknowledged"
        acknowledged["state_version"] += 1
        acknowledged["acknowledged_at"] = (
            "2026-01-02T03:04:07.000000"
        )
        acknowledged["updated_at"] = "2026-01-02T03:04:07.000000"
        remote_receipt = acknowledged
        return acknowledged

    proxy = SimpleNamespace(
        task_operation_lock=get_task_operation_lock,
        proxy_to_worker=AsyncMock(side_effect=exact_worker_protocol),
    )
    with (
        patch.object(
            backend.main,
            "task_migrator",
            SimpleNamespace(_locks={}),
        ),
        patch.object(backend.main, "worker_proxy", proxy),
        patch.object(
            backend.main.broadcaster,
            "broadcast",
            new_callable=AsyncMock,
        ) as publish,
    ):
        cancelled = await pr_review_fix.cancel_fix_action(
            db_session,
            action_id=action_id,
            cancelled_by_user_id=None,
        )
        cancelled_again = await pr_review_fix.cancel_fix_action(
            db_session,
            action_id=action_id,
            cancelled_by_user_id=None,
        )

    assert [method for method, _path, _body in calls] == [
        "GET",
        "PUT",
        "POST",
    ]
    assert cancelled.status == "cancelled"
    assert cancelled.active_fix_finding_id is None
    assert cancelled_again.status == "cancelled"
    publish.assert_awaited_once_with(
        "tasks",
        {
            "event": "status_change",
            "task_id": task_id,
            "task_retry_count": 0,
            "task_turn_generation": 0,
            "new_status": "completed",
            "background_active": False,
        },
    )
    current_task = await db_session.get(Task, task_id, populate_existing=True)
    assert current_task.status == "completed"
    assert current_task.metadata_ == {
        "pr_finding_action_id": action_id,
        "pr_review_superseded": True,
        "ccm_worker_remote_materialized_v1": True,
    }


@pytest.mark.asyncio
async def test_fix_completion_stages_hash_bound_confirmation(db_session):
    from backend.services import pr_review_fix

    repo, review, finding = await _seed_finding(db_session)
    task = Task(
        title="fix",
        description="immutable",
        status="completed",
        retry_count=2,
        started_at=datetime.utcnow() - timedelta(seconds=5),
        completed_at=datetime.utcnow(),
        pty_background_generation="fix-background-generation",
        metadata_={
            "pr_finding_action_id": 1,
            "expected_head_sha": HEAD_SHA,
        },
    )
    db_session.add(task)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="running",
        idempotency_key="fix-completion-7",
        task_id=task.id,
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        result={
            "allowed_files": [finding.path],
            "action_nonce": "nonce-7",
            "head_repo_full_name": "fork-owner/repo",
            "head_ref": "feature/fix",
        },
    )
    db_session.add(action)
    await db_session.flush()
    task.metadata_ = {
        "pr_finding_action_id": action.id,
        "expected_head_sha": HEAD_SHA,
    }
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=2,
        event_type="result",
        content=_patch_terminal(),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()
    finding_id = finding.id

    with (
        patch.object(pr_review_fix, "_verify_current_snapshot", AsyncMock()),
        patch.object(pr_review_fix, "_validate_patch_applies", AsyncMock()),
    ):
        await pr_review_fix.handle_fix_task_completion(
            db_session,
            action_id=action.id,
            task_id=task.id,
            retry_count=2,
            expected_background_generation="fix-background-generation",
        )

    await db_session.refresh(action)
    assert action.status == "awaiting_confirmation"
    assert action.patch_sha256 == hashlib.sha256(_patch_text().encode()).hexdigest()
    assert action.result["confirmation_token"]
    await db_session.refresh(task)
    assert task.pty_background_generation == "fix-background-generation"
    assert (
        await db_session.get(PRFinding, finding_id, populate_existing=True)
    ).status == "open"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("snapshot_error", "expected_status"),
    (
        ("gh request timed out (HTTP 502)", "running"),
        ("GitHub PR snapshot changed before the backend action", "stale"),
    ),
)
async def test_fix_completion_distinguishes_snapshot_outage_from_drift(
    db_session,
    snapshot_error,
    expected_status,
):
    from backend.services import pr_review_fix
    from backend.services.pr_review_service import GhError

    _, _, finding = await _seed_finding(db_session)
    task = Task(
        title="fix snapshot classification",
        description="immutable",
        status="completed",
        retry_count=0,
        started_at=datetime.utcnow() - timedelta(seconds=5),
        completed_at=datetime.utcnow(),
        metadata_={},
    )
    db_session.add(task)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="running",
        idempotency_key=f"snapshot-classification-{expected_status}",
        task_id=task.id,
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        result={
            "allowed_files": [finding.path],
            "action_nonce": "nonce-snapshot",
            "head_repo_full_name": "fork-owner/repo",
            "head_ref": "feature/fix",
        },
    )
    db_session.add(action)
    await db_session.flush()
    task.metadata_ = {
        "pr_finding_action_id": action.id,
        "expected_head_sha": HEAD_SHA,
    }
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=0,
        event_type="result",
        content=_patch_terminal(),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()
    action_id = action.id
    finding_id = finding.id

    with (
        patch.object(
            pr_review_fix,
            "verify_pr_review_snapshot_current",
            AsyncMock(side_effect=GhError(snapshot_error)),
        ),
        patch.object(pr_review_fix, "_validate_patch_applies", AsyncMock()),
    ):
        await pr_review_fix.handle_fix_task_completion(
            db_session,
            action_id=action.id,
            task_id=task.id,
            retry_count=0,
        )

    current = await db_session.get(
        PRFindingAction,
        action_id,
        populate_existing=True,
    )
    assert current.status == expected_status
    if expected_status == "running":
        assert current.active_fix_finding_id == finding_id
        assert "recovery will retry" in current.error_message
    else:
        assert current.active_fix_finding_id is None
    assert (
        await db_session.get(PRFinding, finding_id, populate_existing=True)
    ).status == "open"


@pytest.mark.asyncio
@pytest.mark.parametrize("task_status", ["completed", "failed"])
async def test_fix_terminal_consumers_yield_to_active_termination_receipt(
    db_session,
    db_factory,
    task_status,
):
    from backend.services import pr_review_fix

    _, _, finding, action, task = await _seed_terminal_fix_action(
        db_session,
        task_status=task_status,
    )
    finding_id = finding.id
    action_id = action.id
    task_id = task.id
    retry_count = task.retry_count
    await persist_active_worker_receipt(db_factory, task_id)

    with (
        patch.object(pr_review_fix, "_verify_current_snapshot", AsyncMock()),
        patch.object(pr_review_fix, "_validate_patch_applies", AsyncMock()),
    ):
        if task_status == "completed":
            await pr_review_fix.handle_fix_task_completion(
                db_session,
                action_id=action_id,
                task_id=task_id,
                retry_count=retry_count,
            )
        else:
            await pr_review_fix.handle_fix_task_failure(
                db_session,
                action_id=action_id,
                task_id=task_id,
                retry_count=retry_count,
                error="receipt owns failure arbitration",
            )

    current = await db_session.get(
        PRFindingAction,
        action_id,
        populate_existing=True,
    )
    assert current.status == "running"
    assert current.active_fix_finding_id == finding_id


@pytest.mark.asyncio
async def test_fix_completion_final_cas_yields_to_receipt_race(
    tmp_path,
):
    from backend.services import pr_review_fix
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'fix-receipt-race.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql(
                "PRAGMA journal_mode=WAL"
            )
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with sessions() as consumer:
            _, _, finding, action, task = await _seed_terminal_fix_action(
                consumer,
            )
            finding_id = finding.id
            action_id = action.id
            task_id = task.id
            retry_count = task.retry_count
            original_transition = pr_review_fix._commit_task_transition
            raced = False

            async def receipt_wins_before_final_cas(*args, **kwargs):
                nonlocal raced
                if not raced:
                    raced = True
                    await persist_active_worker_receipt(sessions, task_id)
                return await original_transition(*args, **kwargs)

            with (
                patch.object(
                    pr_review_fix,
                    "_verify_current_snapshot",
                    AsyncMock(),
                ),
                patch.object(
                    pr_review_fix,
                    "_validate_patch_applies",
                    AsyncMock(),
                ),
                patch.object(
                    pr_review_fix,
                    "_commit_task_transition",
                    side_effect=receipt_wins_before_final_cas,
                ),
            ):
                await pr_review_fix.handle_fix_task_completion(
                    consumer,
                    action_id=action_id,
                    task_id=task_id,
                    retry_count=retry_count,
                )

            assert raced is True
        async with sessions() as verifier:
            current = await verifier.get(PRFindingAction, action_id)
            assert current.status == "running"
            assert current.active_fix_finding_id == finding_id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("supplied_receipt", "confirmed_by", "download_age"),
    (
        ("", 12, timedelta()),
        ("wrong-receipt", 12, timedelta()),
        ("download-receipt-7", 99, timedelta()),
        ("download-receipt-7", 12, timedelta(hours=1)),
    ),
)
async def test_confirmation_requires_fresh_actor_and_hash_bound_receipt(
    db_session,
    supplied_receipt,
    confirmed_by,
    download_age,
):
    from backend.services import pr_review_fix

    _, _, _, action, token, _, patch_sha = await _seed_confirmable_action(
        db_session,
        downloaded_at=datetime.utcnow() - download_age,
    )
    with pytest.raises(
        pr_review_fix.FixConfirmationError,
        match="Download the current validated diff",
    ):
        await pr_review_fix.confirm_fix(
            db_session,
            action_id=action.id,
            confirmation_token=token,
            patch_sha256=patch_sha,
            download_receipt=supplied_receipt,
            confirmed_by_user_id=confirmed_by,
        )

    current = await db_session.get(
        PRFindingAction,
        action.id,
        populate_existing=True,
    )
    assert current.status == "awaiting_confirmation"
    assert current.confirmed_at is None


@pytest.mark.asyncio
async def test_confirmation_rejects_expired_browser_token_before_push(db_session):
    from backend.services import pr_review_fix

    _, _, _, action, token, receipt, patch_sha = await _seed_confirmable_action(
        db_session,
        token_expires_at=int(time.time()) - 1,
    )
    with pytest.raises(
        pr_review_fix.FixConfirmationError,
        match="token has expired",
    ):
        await pr_review_fix.confirm_fix(
            db_session,
            action_id=action.id,
            confirmation_token=token,
            patch_sha256=patch_sha,
            download_receipt=receipt,
            confirmed_by_user_id=12,
        )

    current = await db_session.get(
        PRFindingAction,
        action.id,
        populate_existing=True,
    )
    assert current.status == "awaiting_confirmation"
    assert current.confirmed_at is None


@pytest.mark.asyncio
async def test_confirmation_reloads_completed_action_after_fence_race(
    db_session,
):
    """A concurrent completion must return a serializable, live ORM row."""

    from backend.schemas.pr_monitor import PRFindingActionResponse
    from backend.services import pr_review_fix

    _, _, _, action, token, receipt, patch_sha = (
        await _seed_confirmable_action(db_session)
    )
    action_id = action.id
    original_lock = pr_review_fix.lock_pr_repo_action_boundary

    async def complete_before_confirmation_fence(db, repo_id):
        changed = await db.execute(
            update(PRFindingAction)
            .where(
                PRFindingAction.id == action_id,
                PRFindingAction.status == "awaiting_confirmation",
            )
            .values(
                status="completed",
                active_fix_finding_id=None,
                completed_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )
        assert changed.rowcount == 1
        await db.commit()
        return await original_lock(db, repo_id)

    with patch.object(
        pr_review_fix,
        "lock_pr_repo_action_boundary",
        side_effect=complete_before_confirmation_fence,
    ):
        completed = await pr_review_fix.confirm_fix(
            db_session,
            action_id=action_id,
            confirmation_token=token,
            patch_sha256=patch_sha,
            download_receipt=receipt,
            confirmed_by_user_id=12,
        )

    payload = PRFindingActionResponse.model_validate(completed)
    assert payload.id == action_id
    assert payload.status == "completed"


@pytest.mark.asyncio
async def test_first_confirmation_persists_candidate_before_external_push(
    db_session,
):
    from backend.services import pr_review_fix

    _, _, _, action, token, receipt, patch_sha = await _seed_confirmable_action(
        db_session
    )
    action_id = action.id
    candidate_sha = "c" * 40
    events = []

    async def reconcile(**kwargs):
        events.append(("reconcile", kwargs["push_attempted"]))
        return candidate_sha if kwargs["push_attempted"] else None

    async def push(*_args, **_kwargs):
        current = await db_session.get(
            PRFindingAction,
            action_id,
            populate_existing=True,
        )
        assert current.candidate_commit_sha == candidate_sha
        assert current.candidate_created_at is not None
        assert current.push_attempted_at is not None
        events.append(("push", True))

    with (
        patch.object(
            pr_review_fix,
            "_verify_current_head_route",
            AsyncMock(return_value=HEAD_SHA),
        ),
        patch.object(
            pr_review_fix,
            "_prepare_candidate_checkout",
            AsyncMock(return_value=(candidate_sha, {})),
        ),
        patch.object(
            pr_review_fix,
            "_reconcile_candidate_head",
            side_effect=reconcile,
        ),
        patch.object(
            pr_review_fix,
            "_push_candidate_checkout",
            side_effect=push,
        ),
    ):
        completed = await pr_review_fix.confirm_fix(
            db_session,
            action_id=action_id,
            confirmation_token=token,
            patch_sha256=patch_sha,
            download_receipt=receipt,
            confirmed_by_user_id=12,
        )

    assert events == [
        ("reconcile", False),
        ("push", True),
        ("reconcile", True),
    ]
    assert completed.status == "completed"
    assert completed.candidate_commit_sha == candidate_sha
    assert completed.result["pushed_commit_sha"] == candidate_sha
    assert (
        await db_session.get(PRFinding, completed.finding_id, populate_existing=True)
    ).status == "open"


@pytest.mark.asyncio
async def test_current_head_route_rejects_retargeted_base_with_same_head(
    db_session,
):
    from backend.services import pr_review_fix

    repo, review, _ = await _seed_finding(db_session)
    with patch.object(
        pr_review_fix,
        "_gh_api_json",
        AsyncMock(return_value={
            "state": "open",
            "draft": False,
            "base": {"ref": "main", "sha": "2" * 40},
            "head": {
                "repo": {"full_name": "fork-owner/repo"},
                "ref": "feature/fix",
                "sha": HEAD_SHA,
            },
        }),
    ):
        with pytest.raises(
            pr_review_fix.PRHeadDriftError,
            match="base snapshot changed",
        ):
            await pr_review_fix._load_current_head_route(repo, review)


@pytest.mark.asyncio
async def test_base_retarget_after_candidate_preparation_prevents_push(
    db_session,
):
    """The final snapshot check must run after candidate preparation."""

    from backend.services import pr_review_fix

    _, _, _, action, token, receipt, patch_sha = (
        await _seed_confirmable_action(db_session)
    )
    action_id = action.id
    candidate_sha = "d" * 40
    verify = AsyncMock(side_effect=(
        HEAD_SHA,
        pr_review_fix.PRHeadDriftError("PR base snapshot changed"),
    ))
    push = AsyncMock()
    with (
        patch.object(
            pr_review_fix,
            "_verify_current_head_route",
            verify,
        ),
        patch.object(
            pr_review_fix,
            "_prepare_candidate_checkout",
            AsyncMock(return_value=(candidate_sha, {})),
        ),
        patch.object(
            pr_review_fix,
            "_reconcile_candidate_head",
            AsyncMock(return_value=None),
        ),
        patch.object(pr_review_fix, "_push_candidate_checkout", push),
    ):
        with pytest.raises(
            pr_review_fix.FixConfirmationError,
            match="base snapshot changed",
        ):
            await pr_review_fix.confirm_fix(
                db_session,
                action_id=action_id,
                confirmation_token=token,
                patch_sha256=patch_sha,
                download_receipt=receipt,
                confirmed_by_user_id=12,
            )

    assert verify.await_count == 2
    push.assert_not_awaited()
    current = await db_session.get(
        PRFindingAction,
        action_id,
        populate_existing=True,
    )
    assert current.status == "stale"
    assert current.active_fix_finding_id is None
    assert current.candidate_commit_sha == candidate_sha
    assert current.push_attempted_at is None


@pytest.mark.asyncio
async def test_exact_old_lease_rejects_deleted_source_branch(
    tmp_path,
):
    """A deleted remote ref must not be recreated by the repair push."""

    from backend.services import pr_review_fix

    remote = tmp_path / "remote.git"
    checkout = tmp_path / "checkout"

    def git(*args, cwd=None):
        return subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "--bare", str(remote))
    git("init", str(checkout))
    git("config", "user.name", "CCM test", cwd=checkout)
    git("config", "user.email", "ccm-test@example.invalid", cwd=checkout)
    (checkout / "example.py").write_text("old\n", encoding="utf-8")
    git("add", "example.py", cwd=checkout)
    git("commit", "-m", "old head", cwd=checkout)
    git("branch", "-M", "feature/fix", cwd=checkout)
    git("remote", "add", "origin", str(remote), cwd=checkout)
    git("push", "origin", "feature/fix", cwd=checkout)
    old_head = git("rev-parse", "HEAD", cwd=checkout).lower()
    (checkout / "example.py").write_text("new\n", encoding="utf-8")
    git("add", "example.py", cwd=checkout)
    git("commit", "-m", "candidate", cwd=checkout)
    candidate_sha = git("rev-parse", "HEAD", cwd=checkout).lower()
    git(
        f"--git-dir={remote}",
        "update-ref",
        "-d",
        "refs/heads/feature/fix",
    )

    git_env = dict(os.environ)
    git_env.update({
        "GIT_ALLOW_PROTOCOL": "file",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": f"url.file://{remote}.insteadOf",
        "GIT_CONFIG_VALUE_0": (
            "https://github.com/fork-owner/repo.git"
        ),
    })
    with pytest.raises(pr_review_fix.PushOutcomeUnknown):
        await pr_review_fix._push_candidate_checkout(
            str(checkout),
            head_repo_full_name="fork-owner/repo",
            head_ref="feature/fix",
            expected_head_sha=old_head,
            candidate_sha=candidate_sha,
            git_env=git_env,
        )

    missing = subprocess.run(
        [
            "git",
            f"--git-dir={remote}",
            "show-ref",
            "--verify",
            "--quiet",
            "refs/heads/feature/fix",
        ],
        check=False,
    )
    assert missing.returncode != 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observed_head",
    ("c" * 40, "e" * 40),
    ids=("candidate-head", "candidate-descendant"),
)
async def test_confirmed_outbox_recovers_without_browser_credentials(
    db_session,
    observed_head,
):
    """A lost push response is reconciled from durable candidate evidence."""

    from backend.services import pr_review_fix

    _, _, _, action, _, _, patch_sha = await _seed_confirmable_action(
        db_session
    )
    action_id = action.id
    candidate_sha = "c" * 40
    action.status = "running"
    action.confirmed_by_user_id = 12
    action.confirmed_at = datetime.utcnow() - timedelta(minutes=20)
    action.candidate_created_at = action.confirmed_at.replace(microsecond=0)
    action.candidate_commit_sha = candidate_sha
    action.push_attempted_at = datetime.utcnow() - timedelta(minutes=19)
    action.operation_token = "expired-owner"
    action.operation_expires_at = datetime.utcnow() - timedelta(minutes=1)
    await db_session.commit()
    prepare = AsyncMock()
    push = AsyncMock()

    with (
        patch.object(
            pr_review_fix,
            "_verify_current_head_route",
            AsyncMock(return_value=observed_head),
        ),
        patch.object(
            pr_review_fix,
            "_reconcile_candidate_head",
            AsyncMock(return_value=observed_head),
        ) as reconcile,
        patch.object(pr_review_fix, "_prepare_candidate_checkout", prepare),
        patch.object(pr_review_fix, "_push_candidate_checkout", push),
    ):
        completed = await pr_review_fix.confirm_fix(
            db_session,
            action_id=action_id,
            confirmation_token="expired-and-ignored",
            patch_sha256=patch_sha,
            download_receipt="expired-and-ignored",
            confirmed_by_user_id=12,
        )

    prepare.assert_not_awaited()
    push.assert_not_awaited()
    reconcile.assert_awaited_once()
    assert completed.status == "completed"
    assert completed.result["pushed_commit_sha"] == candidate_sha
    assert completed.result["observed_head_sha"] == observed_head


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "detail"),
    (
        ("closed", "closed or draft"),
        ("open", "source repository no longer exists"),
    ),
)
async def test_confirmed_outbox_terminalizes_missing_closed_source_route(
    db_session,
    state,
    detail,
):
    from backend.services import pr_review_fix

    _, _, _, action, _, _, patch_sha = await _seed_confirmable_action(
        db_session
    )
    action_id = action.id
    action.status = "running"
    action.confirmed_by_user_id = 12
    action.confirmed_at = datetime.utcnow() - timedelta(minutes=20)
    action.candidate_created_at = action.confirmed_at.replace(microsecond=0)
    action.operation_token = "expired-owner"
    action.operation_expires_at = datetime.utcnow() - timedelta(minutes=1)
    await db_session.commit()

    with patch.object(
        pr_review_fix,
        "_gh_api_json",
        AsyncMock(return_value={
            "state": state,
            "draft": False,
            "head": {
                "repo": None,
                "ref": "feature/fix",
                "sha": HEAD_SHA,
            },
        }),
    ):
        with pytest.raises(
            pr_review_fix.FixConfirmationError,
            match=detail,
        ):
            await pr_review_fix.confirm_fix(
                db_session,
                action_id=action_id,
                confirmation_token="expired-and-ignored",
                patch_sha256=patch_sha,
                download_receipt="expired-and-ignored",
                confirmed_by_user_id=12,
            )

    current = await db_session.get(
        PRFindingAction,
        action_id,
        populate_existing=True,
    )
    assert current.status == "stale"
    assert current.active_fix_finding_id is None


@pytest.mark.asyncio
async def test_external_head_advance_before_push_marks_candidate_stale(
    db_session,
):
    from backend.services import pr_review_fix

    _, _, _, action, token, receipt, patch_sha = await _seed_confirmable_action(
        db_session
    )
    action_id = action.id
    candidate_sha = "d" * 40
    push = AsyncMock()
    with (
        patch.object(
            pr_review_fix,
            "_verify_current_head_route",
            AsyncMock(return_value=HEAD_SHA),
        ),
        patch.object(
            pr_review_fix,
            "_prepare_candidate_checkout",
            AsyncMock(return_value=(candidate_sha, {})),
        ),
        patch.object(
            pr_review_fix,
            "_reconcile_candidate_head",
            AsyncMock(side_effect=pr_review_fix.PRHeadDriftError(
                "PR head advanced before the confirmed candidate was pushed"
            )),
        ),
        patch.object(pr_review_fix, "_push_candidate_checkout", push),
    ):
        with pytest.raises(
            pr_review_fix.FixConfirmationError,
            match="head advanced",
        ):
            await pr_review_fix.confirm_fix(
                db_session,
                action_id=action_id,
                confirmation_token=token,
                patch_sha256=patch_sha,
                download_receipt=receipt,
                confirmed_by_user_id=12,
            )

    push.assert_not_awaited()
    current = await db_session.get(
        PRFindingAction,
        action_id,
        populate_existing=True,
    )
    assert current.status == "stale"
    assert current.active_fix_finding_id is None
    assert current.candidate_commit_sha == candidate_sha
    assert current.push_attempted_at is None


@pytest.mark.asyncio
async def test_recovery_treats_github_422_missing_candidate_as_head_drift(
    db_session,
):
    from backend.services import pr_review_fix
    from backend.services.pr_review_service import GhError

    repo, review, _ = await _seed_finding(db_session)
    with (
        patch.object(
            pr_review_fix,
            "_load_current_head_route",
            AsyncMock(return_value=(
                "fork-owner/repo",
                "feature/fix",
                "e" * 40,
            )),
        ),
        patch.object(
            pr_review_fix,
            "_verify_candidate_commit",
            AsyncMock(side_effect=GhError(
                f"No commit found for SHA: {'d' * 40} (HTTP 422)"
            )),
        ),
    ):
        with pytest.raises(
            pr_review_fix.PRHeadDriftError,
            match="absent from the advanced PR head",
        ):
            await pr_review_fix._reconcile_candidate_head(
                repo=repo,
                review=review,
                old_head_sha=HEAD_SHA,
                candidate_sha="d" * 40,
                nonce="nonce-confirm-7",
                source_repo="fork-owner/repo",
                source_ref="feature/fix",
                push_attempted=True,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    (
        "repository not found (HTTP 404)",
        f"No commit found for SHA: {'e' * 40} (HTTP 422)",
        "validation failed (HTTP 422)",
    ),
)
async def test_recovery_keeps_ambiguous_commit_lookup_errors_recoverable(
    db_session,
    message,
):
    from backend.services import pr_review_fix
    from backend.services.pr_review_service import GhError

    repo, review, _ = await _seed_finding(db_session)
    with (
        patch.object(
            pr_review_fix,
            "_load_current_head_route",
            AsyncMock(return_value=(
                "fork-owner/repo",
                "feature/fix",
                "e" * 40,
            )),
        ),
        patch.object(
            pr_review_fix,
            "_verify_candidate_commit",
            AsyncMock(side_effect=GhError(message)),
        ),
    ):
        with pytest.raises(GhError, match=re.escape(message)):
            await pr_review_fix._reconcile_candidate_head(
                repo=repo,
                review=review,
                old_head_sha=HEAD_SHA,
                candidate_sha="d" * 40,
                nonce="nonce-confirm-7",
                source_repo="fork-owner/repo",
                source_ref="feature/fix",
                push_attempted=True,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("comparison", "error_type"),
    (
        ({}, "infrastructure"),
        (
            {"status": "ahead", "merge_base_commit": {}},
            "infrastructure",
        ),
        (
            {
                "status": "diverged",
                "merge_base_commit": {"sha": "f" * 40},
            },
            "drift",
        ),
    ),
)
async def test_candidate_compare_requires_complete_evidence_before_drift(
    db_session,
    comparison,
    error_type,
):
    from backend.services import pr_review_fix
    from backend.services.pr_review_service import GhError

    repo, review, _ = await _seed_finding(db_session)
    expected_error = (
        GhError
        if error_type == "infrastructure"
        else pr_review_fix.PRHeadDriftError
    )
    with (
        patch.object(
            pr_review_fix,
            "_load_current_head_route",
            AsyncMock(return_value=(
                "fork-owner/repo",
                "feature/fix",
                "e" * 40,
            )),
        ),
        patch.object(
            pr_review_fix,
            "_verify_candidate_commit",
            AsyncMock(),
        ),
        patch.object(
            pr_review_fix,
            "_gh_api_json",
            AsyncMock(return_value=comparison),
        ),
    ):
        with pytest.raises(expected_error):
            await pr_review_fix._reconcile_candidate_head(
                repo=repo,
                review=review,
                old_head_sha=HEAD_SHA,
                candidate_sha="d" * 40,
                nonce="nonce-confirm-7",
                source_repo="fork-owner/repo",
                source_ref="feature/fix",
                push_attempted=True,
            )


@pytest.mark.asyncio
async def test_task_callback_cannot_overwrite_push_owner(db_session):
    from backend.services import pr_review_fix

    _, _, finding = await _seed_finding(db_session)
    task = Task(
        title="late",
        description="late",
        status="failed",
        retry_count=1,
    )
    db_session.add(task)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="running",
        idempotency_key="push-owner-7",
        task_id=task.id,
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        operation_token="push-owner",
        operation_expires_at=datetime.utcnow() + timedelta(minutes=5),
    )
    db_session.add(action)
    await db_session.commit()

    await pr_review_fix.handle_fix_task_failure(
        db_session,
        action_id=action.id,
        task_id=task.id,
        retry_count=1,
        error="late callback",
    )

    await db_session.refresh(action)
    assert action.status == "running"
    assert action.operation_token == "push-owner"


@pytest.mark.asyncio
async def test_reconciler_terminalizes_disabled_confirmation(
    db_session,
    db_factory,
):
    """Disabled monitors must release an unconfirmed active repair slot."""

    from backend.services import pr_review_fix

    repo, _, finding, action, _, _, _ = await _seed_confirmable_action(
        db_session
    )
    repo.enabled = False
    await db_session.commit()
    action_id = action.id

    changed = await pr_review_fix.reconcile_finding_action(
        db_factory,
        action_id,
    )

    assert changed is True
    current = await db_session.get(
        PRFindingAction,
        action_id,
        populate_existing=True,
    )
    assert current.status == "stale"
    assert current.active_fix_finding_id is None


@pytest.mark.asyncio
async def test_worker_recovery_uses_detached_worker_snapshot(
    db_session,
    db_factory,
):
    """Worker log recovery must not receive a rollback-expired ORM row."""

    from backend.services import pr_review_fix

    _, _, finding = await _seed_finding(db_session)
    worker = Worker(
        name="fix-recovery-worker",
        status="ready",
        private_ip="10.0.0.7",
        ccm_port=8123,
        auth_token="worker-secret",
    )
    db_session.add(worker)
    await db_session.flush()
    task = Task(
        title="recover remote fix",
        description="recover remote fix",
        status="completed",
        retry_count=1,
        worker_id=worker.id,
    )
    db_session.add(task)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="running",
        idempotency_key="worker-recovery-detached-snapshot",
        task_id=task.id,
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
    )
    db_session.add(action)
    await db_session.commit()
    action_id = action.id
    task_id = task.id
    observed = []

    class Relay:
        async def _backfill_missing_logs(
            self,
            loaded_worker,
            task_ids,
            *,
            sync_status,
        ):
            from backend.services.worker_proxy import get_task_operation_lock

            assert get_task_operation_lock(task_id).locked()
            observed.append((
                loaded_worker.id,
                loaded_worker.private_ip,
                loaded_worker.ccm_port,
                loaded_worker.auth_token,
                sync_status,
            ))
            return set(task_ids)

    with patch.object(
        pr_review_fix,
        "handle_fix_task_completion",
        new_callable=AsyncMock,
    ) as complete:
        async def assert_completion_lock(*args, **kwargs):
            from backend.services.worker_proxy import get_task_operation_lock

            assert get_task_operation_lock(task_id).locked()

        complete.side_effect = assert_completion_lock
        changed = await pr_review_fix.reconcile_finding_action(
            db_factory,
            action_id,
            worker_relay=Relay(),
        )

    assert changed is False
    assert observed == [(worker.id, "10.0.0.7", 8123, "worker-secret", False)]
    complete.assert_awaited_once_with(
        ANY,
        action_id=action_id,
        task_id=task_id,
        retry_count=1,
    )


@pytest.mark.asyncio
async def test_finding_recovery_yields_to_active_termination_receipt(
    db_session,
    db_factory,
):
    from backend.services import pr_review_fix

    _, _, finding, action, task = await _seed_terminal_fix_action(db_session)
    finding_id = finding.id
    action_id = action.id
    task_id = task.id
    await persist_active_worker_receipt(db_factory, task_id)

    with (
        patch.object(pr_review_fix, "_verify_current_snapshot", AsyncMock()),
        patch.object(pr_review_fix, "_validate_patch_applies", AsyncMock()),
    ):
        recovered = await pr_review_fix.recover_incomplete_finding_actions(
            db_factory,
        )

    assert recovered == 0
    async with db_factory() as db:
        current = await db.get(PRFindingAction, action_id)
        assert current.status == "running"
        assert current.active_fix_finding_id == finding_id
