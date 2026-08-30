"""Focused safety tests for durable Plan provider runtime receipts."""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError

from backend.models.plan_agent import PlanAgentRuntimeReceipt, PlanAgentStep
from backend.services import plan_runtime_receipt as runtime_receipts


def _boot_id(hex_digit: str) -> str:
    return str(uuid.UUID(hex_digit * 32))


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin libproc contract")
def test_darwin_runtime_identity_uses_boot_session_and_libproc():
    boot_id = runtime_receipts._read_boot_id()
    identity = runtime_receipts.read_process_identity(os.getpid())

    assert str(uuid.UUID(boot_id)) == boot_id
    assert identity is not None
    assert identity.pid == os.getpid()
    assert identity.uid == os.getuid()
    assert identity.process_group_id == os.getpgid(0)
    assert identity.start_ticks > 0
    assert identity.boot_id == boot_id
    assert identity.state != "Z"
    assert runtime_receipts._read_current_start_ticks() > 0


async def _create_step(
    db_factory,
    *,
    run_id: int = 101,
    generation: int = 7,
    provider: str = "claude",
) -> PlanAgentStep:
    async with db_factory() as db:
        step = PlanAgentStep(
            run_id=run_id,
            generation=generation,
            step_type="planner",
            round=1,
            provider=provider,
            status="running",
        )
        db.add(step)
        await db.commit()
        await db.refresh(step)
        db.expunge(step)
        return step


async def _create_receipt(
    db_factory,
    step: PlanAgentStep,
    *,
    status: str = "admitting",
    attempt_index: int = 1,
    run_id: int | None = None,
    run_generation: int | None = None,
    provider: str | None = None,
    runtime_token: str | None = None,
    prepared_boot_id: str | None = None,
    prepared_start_ticks: int = 10,
    prepared_uid: int | None = None,
    process_id: int | None = None,
    process_group_id: int | None = None,
    process_start_ticks: int | None = None,
    process_uid: int | None = None,
    boot_id: str | None = None,
    codex_home: str | None = None,
    codex_thread_id: str | None = None,
    cleanup_error: str | None = None,
    cleaned_at=None,
) -> PlanAgentRuntimeReceipt:
    async with db_factory() as db:
        receipt = PlanAgentRuntimeReceipt(
            run_id=step.run_id if run_id is None else run_id,
            step_id=step.id,
            run_generation=(
                step.generation if run_generation is None else run_generation
            ),
            attempt_index=attempt_index,
            provider=step.provider if provider is None else provider,
            runtime_token=(
                runtime_token
                or uuid.uuid5(
                    uuid.NAMESPACE_OID,
                    f"runtime-token-{step.id}-{attempt_index}",
                ).hex
            ),
            prepared_boot_id=(
                prepared_boot_id
                if prepared_boot_id is not None
                else runtime_receipts._read_boot_id()
            ),
            prepared_start_ticks=prepared_start_ticks,
            prepared_uid=os.getuid() if prepared_uid is None else prepared_uid,
            status=status,
            process_id=process_id,
            process_group_id=process_group_id,
            process_start_ticks=process_start_ticks,
            process_uid=process_uid,
            boot_id=boot_id,
            codex_home=codex_home,
            codex_thread_id=codex_thread_id,
            cleanup_error=(
                cleanup_error
                if cleanup_error is not None
                else (
                    "runtime cleanup remains uncertain"
                    if status == "cleanup_failed"
                    else None
                )
            ),
            cleaned_at=(
                cleaned_at
                if cleaned_at is not None
                else (datetime.utcnow() if status == "cleaned" else None)
            ),
        )
        db.add(receipt)
        await db.commit()
        await db.refresh(receipt)
        db.expunge(receipt)
        return receipt


async def _receipt_status(db_factory, receipt_id: int) -> tuple[str, str | None]:
    async with db_factory() as db:
        receipt = await db.get(PlanAgentRuntimeReceipt, receipt_id)
        assert receipt is not None
        return receipt.status, receipt.cleanup_error


class _CodexRegistry:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._servers: dict[str, object] = {}
        self._starting: dict[str, int] = {}
        self._thread_owners: dict[str, str] = {}
        self.delete_thread = AsyncMock()


class _CodexManager:
    def __init__(self, registry: _CodexRegistry) -> None:
        self.registry = registry
        self.guarded_homes: list[str] = []

    def _ensure_codex_app_server_registry(self) -> _CodexRegistry:
        return self.registry

    @asynccontextmanager
    async def codex_home_app_server_guard(self, home: str):
        self.guarded_homes.append(home)
        yield home


@pytest.mark.asyncio
async def test_prepare_runtime_attempt_atomically_claims_prepared_receipt(
    db_factory,
):
    step = await _create_step(db_factory)
    prepared = await _create_receipt(db_factory, step, status="prepared")

    claimed = await runtime_receipts.prepare_runtime_attempt(db_factory, step.id)

    assert claimed.id == prepared.id
    assert claimed.status == "admitting"
    assert claimed.run_id == step.run_id
    assert claimed.run_generation == step.generation
    assert claimed.provider == step.provider
    assert claimed.attempt_index == 1
    async with db_factory() as db:
        count = await db.scalar(
            select(func.count(PlanAgentRuntimeReceipt.id)).where(
                PlanAgentRuntimeReceipt.step_id == step.id
            )
        )
        persisted = await db.get(PlanAgentRuntimeReceipt, prepared.id)
    assert count == 1
    assert persisted is not None
    assert persisted.status == "admitting"


@pytest.mark.asyncio
async def test_prepare_runtime_attempt_rejects_duplicate_claim(db_factory):
    step = await _create_step(db_factory)
    prepared = await _create_receipt(db_factory, step, status="prepared")
    first = await runtime_receipts.prepare_runtime_attempt(db_factory, step.id)
    assert first.id == prepared.id

    with pytest.raises(
        runtime_receipts.PlanRuntimeReceiptError,
        match=r"retained unclean runtime attempt #1 \(admitting\)",
    ):
        await runtime_receipts.prepare_runtime_attempt(db_factory, step.id)

    async with db_factory() as db:
        count = await db.scalar(
            select(func.count(PlanAgentRuntimeReceipt.id)).where(
                PlanAgentRuntimeReceipt.step_id == step.id
            )
        )
    assert count == 1


@pytest.mark.asyncio
async def test_mark_runtime_cleaned_uses_exact_snapshot_cas(db_factory):
    step = await _create_step(db_factory, provider="codex")
    receipt = await _create_receipt(
        db_factory,
        step,
        status="launching",
        codex_home="/srv/codex/a",
        codex_thread_id="thread-original",
    )
    stale = runtime_receipts._snapshot(receipt)

    async with db_factory() as db:
        persisted = await db.get(PlanAgentRuntimeReceipt, receipt.id)
        assert persisted is not None
        persisted.codex_thread_id = "thread-rebound"
        await db.commit()

    with pytest.raises(
        runtime_receipts.PlanRuntimeReceiptError,
        match="changed during cleanup",
    ):
        await runtime_receipts.mark_runtime_cleaned(db_factory, stale)

    assert await _receipt_status(db_factory, receipt.id) == ("launching", None)


@pytest.mark.asyncio
async def test_mark_runtime_cleaned_is_idempotent_for_same_exact_snapshot(db_factory):
    step = await _create_step(db_factory)
    receipt = await _create_receipt(db_factory, step, status="admitting")
    exact = runtime_receipts._snapshot(receipt)

    await runtime_receipts.mark_runtime_cleaned(db_factory, exact)
    await runtime_receipts.mark_runtime_cleaned(db_factory, exact)

    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)


@pytest.mark.asyncio
async def test_mark_runtime_cleaned_wins_over_concurrent_cleanup_failure(db_factory):
    step = await _create_step(db_factory, provider="codex")
    receipt = await _create_receipt(
        db_factory,
        step,
        status="launching",
        codex_home="/srv/codex/racing-cleanup",
        codex_thread_id="thread-racing-cleanup",
    )
    exact = runtime_receipts._snapshot(receipt)

    async with db_factory() as db:
        persisted = await db.get(PlanAgentRuntimeReceipt, receipt.id)
        assert persisted is not None
        persisted.status = "cleanup_failed"
        persisted.cleanup_error = "concurrent reconciliation remained uncertain"
        persisted.cleaned_at = None
        await db.commit()

    await runtime_receipts.mark_runtime_cleaned(db_factory, exact)

    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)


@pytest.mark.asyncio
async def test_runtime_generation_accepts_contiguous_redundant_identities(db_factory):
    step = await _create_step(db_factory, run_id=201, generation=9)
    await _create_receipt(
        db_factory,
        step,
        status="cleaned",
        attempt_index=1,
    )
    await _create_receipt(
        db_factory,
        step,
        status="cleaned",
        attempt_index=2,
    )

    async with db_factory() as db:
        assert await runtime_receipts.runtime_generation_is_clean(
            db,
            run_id=step.run_id,
            generation=step.generation,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("forgery", ["missing_cleaned_at", "partial_process"])
async def test_database_rejects_forged_cleaned_receipt(db_factory, forgery):
    step = await _create_step(db_factory, run_id=203, generation=11)
    async with db_factory() as db:
        db.add(
            PlanAgentRuntimeReceipt(
                run_id=step.run_id,
                step_id=step.id,
                run_generation=step.generation,
                attempt_index=1,
                provider="claude",
                runtime_token=uuid.uuid4().hex,
                prepared_boot_id=runtime_receipts._read_boot_id(),
                prepared_start_ticks=1,
                prepared_uid=os.getuid(),
                status="cleaned",
                process_id=42 if forgery == "partial_process" else None,
                cleaned_at=(
                    datetime.utcnow()
                    if forgery == "partial_process"
                    else None
                ),
            )
        )
        with pytest.raises(IntegrityError):
            await db.commit()


@pytest.mark.asyncio
async def test_service_rejects_corrupted_cleaned_receipt_without_timestamp(db_factory):
    step = await _create_step(db_factory, run_id=204, generation=12)
    receipt = await _create_receipt(db_factory, step, status="cleaned")
    async with db_factory() as db:
        await db.execute(text("PRAGMA ignore_check_constraints = ON"))
        await db.execute(
            update(PlanAgentRuntimeReceipt)
            .where(PlanAgentRuntimeReceipt.id == receipt.id)
            .values(cleaned_at=None)
        )
        await db.commit()
        await db.execute(text("PRAGMA ignore_check_constraints = OFF"))

    async with db_factory() as db:
        assert not await runtime_receipts.runtime_generation_is_clean(
            db,
            run_id=step.run_id,
            generation=step.generation,
        )
    assert not await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        object(),
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )
    with pytest.raises(
        runtime_receipts.PlanRuntimeReceiptError,
        match="invalid runtime receipt identity",
    ):
        await runtime_receipts.prepare_runtime_attempt(db_factory, step.id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "corrupt_value"),
    [
        ("run_id", 999_001),
        ("run_generation", 999_002),
        ("provider", "codex"),
        ("attempt_index", 2),
    ],
)
async def test_runtime_generation_rejects_broken_redundant_identity_or_attempt_gap(
    db_factory,
    field: str,
    corrupt_value: object,
):
    step = await _create_step(db_factory, run_id=202, generation=10)
    kwargs = {field: corrupt_value}
    await _create_receipt(
        db_factory,
        step,
        status="cleaned",
        **kwargs,
    )

    async with db_factory() as db:
        assert not await runtime_receipts.runtime_generation_is_clean(
            db,
            run_id=step.run_id,
            generation=step.generation,
        )


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Linux /proc environment scan contract",
)
def test_proc_scan_skips_older_same_uid_process_before_unreadable_environ(
    monkeypatch,
):
    """Use our real /proc identity but make its environment audit fail if opened."""

    identity = runtime_receipts.read_process_identity(os.getpid())
    assert identity is not None
    snapshot = runtime_receipts.RuntimeReceiptSnapshot(
        id=301,
        run_id=302,
        step_id=303,
        run_generation=1,
        attempt_index=1,
        provider="claude",
        runtime_token=uuid.uuid4().hex,
        prepared_boot_id=identity.boot_id,
        prepared_start_ticks=identity.start_ticks + 1,
        prepared_uid=os.getuid(),
        status="admitting",
        process_id=None,
        process_group_id=None,
        process_start_ticks=None,
        process_uid=None,
        boot_id=None,
        codex_home=None,
        codex_thread_id=None,
    )
    opened: list[Path] = []

    monkeypatch.setattr(
        Path,
        "iterdir",
        lambda _path: [Path(f"/proc/{os.getpid()}")],
    )

    def unreadable_environ(path, _flags):
        opened.append(Path(path))
        raise PermissionError("same-UID supervisor intentionally hides environ")

    monkeypatch.setattr(runtime_receipts.os, "open", unreadable_environ)

    assert runtime_receipts._token_process_identities(snapshot) == []
    assert opened == []


@pytest.mark.asyncio
async def test_claude_prepared_receipt_cleans_without_process_scan(
    db_factory,
    monkeypatch,
):
    step = await _create_step(db_factory)
    receipt = await _create_receipt(
        db_factory,
        step,
        status="prepared",
    )
    token_scan = MagicMock(
        side_effect=AssertionError("prepared receipt must not scan processes")
    )
    monkeypatch.setattr(
        runtime_receipts,
        "_token_process_identities",
        token_scan,
    )

    assert await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        object(),
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )

    token_scan.assert_not_called()
    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)


@pytest.mark.asyncio
async def test_claude_reconcile_terminates_token_owner_before_cleaning(
    db_factory,
    monkeypatch,
):
    boot_id = _boot_id("b")
    step = await _create_step(db_factory)
    receipt = await _create_receipt(
        db_factory,
        step,
        status="launching",
        prepared_boot_id=boot_id,
        process_id=401,
        process_group_id=401,
        process_start_ticks=4001,
        process_uid=os.getuid(),
        boot_id=boot_id,
    )
    identity = runtime_receipts.ProcessIdentity(
        pid=401,
        process_group_id=401,
        start_ticks=4001,
        uid=os.getuid(),
        boot_id=boot_id,
        state="S",
    )
    monkeypatch.setattr(runtime_receipts, "_read_boot_id", lambda: boot_id)
    monkeypatch.setattr(
        runtime_receipts,
        "_token_process_identities",
        MagicMock(return_value=[identity]),
    )
    terminate = AsyncMock()
    monkeypatch.setattr(runtime_receipts, "_terminate_token_groups", terminate)

    assert await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        object(),
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )

    terminate.assert_awaited_once()
    cleaned_snapshot = terminate.await_args.args[0]
    assert cleaned_snapshot.runtime_token == receipt.runtime_token
    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)


@pytest.mark.asyncio
async def test_claude_reconcile_fails_closed_on_live_reused_pid_group(
    db_factory,
    monkeypatch,
):
    boot_id = _boot_id("c")
    step = await _create_step(db_factory)
    receipt = await _create_receipt(
        db_factory,
        step,
        status="launching",
        prepared_boot_id=boot_id,
        process_id=501,
        process_group_id=501,
        process_start_ticks=5001,
        process_uid=os.getuid(),
        boot_id=boot_id,
    )
    reused = runtime_receipts.ProcessIdentity(
        pid=501,
        process_group_id=501,
        start_ticks=9999,
        uid=os.getuid(),
        boot_id=boot_id,
        state="S",
    )
    monkeypatch.setattr(runtime_receipts, "_read_boot_id", lambda: boot_id)
    monkeypatch.setattr(
        runtime_receipts,
        "_token_process_identities",
        MagicMock(return_value=[]),
    )
    monkeypatch.setattr(
        runtime_receipts,
        "read_process_identity",
        MagicMock(return_value=reused),
    )
    monkeypatch.setattr(
        runtime_receipts,
        "_group_alive",
        MagicMock(return_value=True),
    )
    killpg = MagicMock()
    monkeypatch.setattr(runtime_receipts.os, "killpg", killpg)

    assert not await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        object(),
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )

    killpg.assert_not_called()
    status, error = await _receipt_status(db_factory, receipt.id)
    assert status == "cleanup_failed"
    assert error is not None and "ownership is ambiguous" in error


@pytest.mark.asyncio
async def test_claude_reconcile_treats_prior_boot_identity_as_gone(
    db_factory,
    monkeypatch,
):
    step = await _create_step(db_factory)
    receipt = await _create_receipt(
        db_factory,
        step,
        status="launching",
        prepared_boot_id=_boot_id("d"),
        process_id=601,
        process_group_id=601,
        process_start_ticks=6001,
        process_uid=os.getuid(),
        boot_id=_boot_id("d"),
    )
    monkeypatch.setattr(runtime_receipts, "_read_boot_id", lambda: _boot_id("e"))
    token_scan = MagicMock(side_effect=AssertionError("must not inspect old boot"))
    monkeypatch.setattr(runtime_receipts, "_token_process_identities", token_scan)

    assert await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        object(),
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )

    token_scan.assert_not_called()
    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)


@pytest.mark.asyncio
async def test_codex_pre_thread_attempt_cleans_without_registry_access(db_factory):
    step = await _create_step(db_factory, provider="codex")
    receipt = await _create_receipt(db_factory, step, status="admitting")

    class Manager:
        def _ensure_codex_app_server_registry(self):
            raise AssertionError("pre-thread attempt must not acquire a registry")

    assert await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        Manager(),
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )
    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)


@pytest.mark.asyncio
async def test_codex_thread_cleanup_deletes_exact_thread_and_marks_clean(db_factory):
    step = await _create_step(db_factory, provider="codex")
    receipt = await _create_receipt(
        db_factory,
        step,
        status="launching",
        codex_home="/srv/codex/thread-only",
        codex_thread_id="thread-701",
    )
    registry = _CodexRegistry()
    manager = _CodexManager(registry)

    assert await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        manager,
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )

    assert manager.guarded_homes == ["/srv/codex/thread-only"]
    registry.delete_thread.assert_awaited_once_with(
        "/srv/codex/thread-only",
        "thread-701",
    )
    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)


@pytest.mark.asyncio
async def test_codex_thread_cleanup_treats_missing_rollout_as_absent(db_factory):
    step = await _create_step(db_factory, provider="codex")
    receipt = await _create_receipt(
        db_factory,
        step,
        status="cleanup_failed",
        codex_home="/srv/codex/missing-rollout",
        codex_thread_id="thread-702",
    )
    registry = _CodexRegistry()
    registry.delete_thread.side_effect = RuntimeError(
        "thread/delete failed: no rollout found for thread id thread-702"
    )
    manager = _CodexManager(registry)

    assert await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        manager,
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )

    registry.delete_thread.assert_awaited_once_with(
        "/srv/codex/missing-rollout",
        "thread-702",
    )
    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)


@pytest.mark.asyncio
async def test_codex_cold_recovery_never_kills_live_unregistered_shared_transport(
    db_factory,
    monkeypatch,
):
    boot_id = _boot_id("f")
    step = await _create_step(db_factory, provider="codex")
    receipt = await _create_receipt(
        db_factory,
        step,
        status="launching",
        prepared_boot_id=boot_id,
        process_id=801,
        process_group_id=801,
        process_start_ticks=8001,
        process_uid=os.getuid(),
        boot_id=boot_id,
        codex_home="/srv/codex/cold",
        codex_thread_id="thread-801",
    )
    registry = _CodexRegistry()
    manager = _CodexManager(registry)
    is_live = MagicMock(return_value=True)
    killpg = MagicMock()
    monkeypatch.setattr(runtime_receipts, "_exact_codex_transport_is_live", is_live)
    monkeypatch.setattr(runtime_receipts.os, "killpg", killpg)

    assert not await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        manager,
        receipt_id=receipt.id,
        allow_transport_kill=True,
    )

    is_live.assert_called_once()
    killpg.assert_not_called()
    registry.delete_thread.assert_not_awaited()
    status, error = await _receipt_status(db_factory, receipt.id)
    assert status == "cleanup_failed"
    assert error is not None and "shared transport identity" in error


@pytest.mark.asyncio
async def test_codex_live_cleanup_never_probes_or_kills_shared_transport_when_disallowed(
    db_factory,
    monkeypatch,
):
    boot_id = _boot_id("1")
    step = await _create_step(db_factory, provider="codex")
    receipt = await _create_receipt(
        db_factory,
        step,
        status="launching",
        prepared_boot_id=boot_id,
        process_id=901,
        process_group_id=901,
        process_start_ticks=9001,
        process_uid=os.getuid(),
        boot_id=boot_id,
        codex_home="/srv/codex/live-shared",
        codex_thread_id="thread-901",
    )
    registry = _CodexRegistry()
    manager = _CodexManager(registry)
    is_live = MagicMock(return_value=True)
    killpg = MagicMock()
    monkeypatch.setattr(runtime_receipts, "_exact_codex_transport_is_live", is_live)
    monkeypatch.setattr(runtime_receipts.os, "killpg", killpg)

    assert await runtime_receipts.reconcile_runtime_receipt(
        db_factory,
        manager,
        receipt_id=receipt.id,
        allow_transport_kill=False,
    )

    is_live.assert_not_called()
    killpg.assert_not_called()
    registry.delete_thread.assert_awaited_once_with(
        "/srv/codex/live-shared",
        "thread-901",
    )
    assert await _receipt_status(db_factory, receipt.id) == ("cleaned", None)
