"""Crash/restart fences for remote-first Worker Task + Plan deletion."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from sqlalchemy import select

import backend.main as main_module
from backend.models.plan import Plan
from backend.models.task import Task
from backend.models.worker import Worker
from backend.models.worker_task_termination import WorkerTaskTerminationReceipt
from backend.services.task_queue import TaskQueue, task_delete_fence
from backend.services.worker_proxy import (
    WorkerProxy,
    WorkerTaskMutationOutcomeUncertainError,
    WorkerTaskPlanDeleteProtocolUnsupported,
)
from backend.services.plan_service import fence_plan_target_task
from backend.services import worker_task_termination as termination
from backend.services.worker_task_termination import (
    WorkerTaskTerminationCoordinator,
    active_worker_task_termination_receipt,
    mark_manager_task_delete_remote_possible,
    record_manager_task_delete_proof,
    stage_manager_task_delete_receipt,
)


async def _worker_graph(session_factory, *, with_plan: bool = True):
    async with session_factory() as db:
        worker = Worker(
            name="delete-worker",
            status="ready",
            private_ip="10.20.30.40",
            auth_token="secret",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="durable delete",
            description="delete exactly once",
            status="completed",
            worker_id=worker.id,
        )
        db.add(task)
        await db.flush()
        plan = None
        if with_plan:
            plan = Plan(
                title="delete with target",
                initial_request="plan",
                target_task_id=task.id,
                worker_id=worker.id,
                pipeline_config={},
            )
            db.add(plan)
            await db.flush()
        await db.commit()
        return worker.id, task.id, plan.id if plan is not None else None


async def _prepare_receipt(session_factory, task_id: int) -> str:
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        operation_id = None

        async def stage(preflight):
            nonlocal operation_id
            locked = await db.get(Task, task_id, populate_existing=True)
            receipt = await stage_manager_task_delete_receipt(
                db,
                locked,
                plan_ids=preflight.plan_ids,
            )
            operation_id = receipt.operation_id
            return True

        assert await TaskQueue(db).delete(
            task_id,
            expected_fence=task_delete_fence(task),
            prepare_remote_worker_delete=stage,
        )
        assert operation_id is not None
        return operation_id


def _delete_receipt(plan_ids: list[int]) -> dict:
    return {
        "ok": True,
        "plan_cascade_protocol": 1,
        "deleted_plan_ids": plan_ids,
        "remaining_target_plan_ids": [],
    }


def _deleted_audit() -> dict:
    return {
        "plan_cascade_protocol": 1,
        "task_exists": False,
        "remaining_target_plan_ids": [],
    }


async def test_pending_remote_restart_sends_one_delete_and_finalizes_graph(
    session_factory,
):
    _worker_id, task_id, plan_id = await _worker_graph(session_factory)
    operation_id = await _prepare_receipt(session_factory, task_id)
    proxy = Mock()
    proxy.proxy_to_worker = AsyncMock(return_value=_delete_receipt([plan_id]))
    proxy.require_task_plan_delete_protocol = AsyncMock(return_value=None)
    coordinator = WorkerTaskTerminationCoordinator(
        session_factory,
        worker_proxy=proxy,
    )

    await coordinator.recover_once()

    proxy.require_task_plan_delete_protocol.assert_awaited_once()
    proxy.proxy_to_worker.assert_awaited_once()
    assert proxy.proxy_to_worker.await_args.args[1] == "DELETE"
    assert (
        proxy.proxy_to_worker.await_args.kwargs[
            "require_task_incarnation_fence"
        ]
        is True
    )
    async with session_factory() as db:
        assert await db.get(Task, task_id) is None
        assert await db.get(Plan, plan_id) is None
        assert await db.get(WorkerTaskTerminationReceipt, operation_id) is None


async def test_remote_possible_restart_never_replays_delete_and_late_commit_converges(
    session_factory,
):
    _worker_id, task_id, plan_id = await _worker_graph(session_factory)
    operation_id = await _prepare_receipt(session_factory, task_id)
    async with session_factory() as db:
        await mark_manager_task_delete_remote_possible(db, operation_id)

    remote_committed = False

    async def audit_only(_task, method, path, **_kwargs):
        assert method == "GET"
        assert path == f"/api/tasks/{task_id}/plan-delete-audit"
        if remote_committed:
            return _deleted_audit()
        return {
            "plan_cascade_protocol": 1,
            "task_exists": True,
            "remaining_target_plan_ids": [plan_id],
        }

    proxy = Mock()
    proxy.proxy_to_worker = AsyncMock(side_effect=audit_only)
    proxy.require_task_plan_delete_protocol = AsyncMock()
    coordinator = WorkerTaskTerminationCoordinator(
        session_factory,
        worker_proxy=proxy,
    )

    await coordinator.recover_once()
    async with session_factory() as db:
        receipt = await active_worker_task_termination_receipt(db, task_id)
        receipt.next_reconcile_at = receipt.updated_at
        await db.commit()
    await coordinator.recover_once()

    assert proxy.proxy_to_worker.await_count == 2
    assert all(
        call.args[1] == "GET" for call in proxy.proxy_to_worker.await_args_list
    )
    assert all(
        call.kwargs["require_task_incarnation_fence"] is True
        for call in proxy.proxy_to_worker.await_args_list
    )
    proxy.require_task_plan_delete_protocol.assert_not_awaited()
    async with session_factory() as db:
        assert await db.get(Task, task_id) is not None
        receipt = await active_worker_task_termination_receipt(db, task_id)
        assert receipt is not None and receipt.status == "conflict"

    remote_committed = True
    async with session_factory() as db:
        receipt = await active_worker_task_termination_receipt(db, task_id)
        receipt.next_reconcile_at = None
        # Explicitly exercise a restart pass now instead of waiting for backoff.
        receipt.next_reconcile_at = receipt.updated_at
        await db.commit()
    await coordinator.recover_once()

    assert proxy.proxy_to_worker.await_args.args[1] == "GET"
    async with session_factory() as db:
        assert await db.get(Task, task_id) is None
        assert await db.get(Plan, plan_id) is None
        assert await db.get(WorkerTaskTerminationReceipt, operation_id) is None


async def test_awaiting_ack_restart_finalizes_locally_without_network(
    session_factory,
):
    _worker_id, task_id, plan_id = await _worker_graph(session_factory)
    operation_id = await _prepare_receipt(session_factory, task_id)
    async with session_factory() as db:
        await mark_manager_task_delete_remote_possible(db, operation_id)
        await record_manager_task_delete_proof(
            db,
            operation_id,
            _delete_receipt([plan_id]),
            proof_kind="delete_receipt",
        )

    proxy = Mock()
    proxy.proxy_to_worker = AsyncMock()
    proxy.require_task_plan_delete_protocol = AsyncMock()
    coordinator = WorkerTaskTerminationCoordinator(
        session_factory,
        worker_proxy=proxy,
    )

    await coordinator.recover_once()

    proxy.proxy_to_worker.assert_not_awaited()
    proxy.require_task_plan_delete_protocol.assert_not_awaited()
    async with session_factory() as db:
        assert await db.get(Task, task_id) is None
        assert await db.get(Plan, plan_id) is None
        assert await db.get(WorkerTaskTerminationReceipt, operation_id) is None


async def test_old_worker_fails_before_delete_and_leaves_pending_owner(
    client,
    session_factory,
    monkeypatch,
):
    _worker_id, task_id, plan_id = await _worker_graph(session_factory)
    proxy = AsyncMock()
    proxy.require_task_plan_delete_protocol.side_effect = (
        WorkerTaskPlanDeleteProtocolUnsupported("upgrade Worker")
    )
    proxy.proxy_to_worker = AsyncMock()
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task_id}")

    assert response.status_code == 503
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        assert await db.get(Task, task_id) is not None
        assert await db.get(Plan, plan_id) is not None
        receipt = await active_worker_task_termination_receipt(db, task_id)
        assert receipt is None
        historical = (
            await db.execute(
                select(WorkerTaskTerminationReceipt).where(
                    WorkerTaskTerminationReceipt.task_id == task_id,
                    WorkerTaskTerminationReceipt.operation == "delete",
                )
            )
        ).scalar_one()
        assert historical.status == "rejected"
        assert historical.active_task_id is None


async def test_lost_delete_ack_and_failed_audit_leave_remote_possible_owner(
    client,
    session_factory,
    monkeypatch,
):
    _worker_id, task_id, plan_id = await _worker_graph(session_factory)
    proxy = AsyncMock()
    proxy.require_task_plan_delete_protocol.return_value = None
    proxy.proxy_to_worker.side_effect = [
        WorkerTaskMutationOutcomeUncertainError(
            "lost DELETE response",
            status_code=502,
        ),
        HTTPException(502, "audit unavailable"),
    ]
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task_id}")

    assert response.status_code == 503
    assert [call.args[1] for call in proxy.proxy_to_worker.await_args_list] == [
        "DELETE",
        "GET",
    ]
    assert all(
        call.kwargs["require_task_incarnation_fence"] is True
        for call in proxy.proxy_to_worker.await_args_list
    )
    async with session_factory() as db:
        assert await db.get(Task, task_id) is not None
        assert await db.get(Plan, plan_id) is not None
        receipt = await active_worker_task_termination_receipt(db, task_id)
        assert receipt is not None
        assert receipt.status == "conflict"
        assert receipt.active_task_id == task_id


async def test_exact_plan_ids_are_frozen_before_remote_mutation(
    client,
    session_factory,
    monkeypatch,
):
    _worker_id, task_id, plan_id = await _worker_graph(session_factory)
    proxy = AsyncMock()
    proxy.require_task_plan_delete_protocol.return_value = None
    proxy.proxy_to_worker.side_effect = [
        _delete_receipt([plan_id + 1000]),
        {
            "plan_cascade_protocol": 1,
            "task_exists": True,
            "remaining_target_plan_ids": [plan_id],
        },
    ]
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task_id}")

    assert response.status_code == 503
    async with session_factory() as db:
        receipt = await active_worker_task_termination_receipt(db, task_id)
        assert receipt is not None and receipt.status == "conflict"
        assert receipt.request_payload["plan_ids"] == [plan_id]
        assert await db.get(Task, task_id) is not None
        assert await db.get(Plan, plan_id) is not None


async def test_remote_possible_owner_blocks_retry_plan_and_generic_proxy(
    client,
    session_factory,
    monkeypatch,
):
    worker_id, task_id, _plan_id = await _worker_graph(
        session_factory,
        with_plan=False,
    )
    operation_id = await _prepare_receipt(session_factory, task_id)
    async with session_factory() as db:
        await mark_manager_task_delete_remote_possible(db, operation_id)

    proxy = WorkerProxy(session_factory, AsyncMock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)
    async with session_factory() as db:
        route = await db.get(Task, task_id)
        with pytest.raises(HTTPException, match="active Worker termination"):
            await proxy.proxy_to_worker(
                route,
                "POST",
                f"/api/tasks/{task_id}/retry",
            )
        with pytest.raises(HTTPException, match="active Worker termination"):
            await fence_plan_target_task(
                db,
                target_task_id=task_id,
                expected_worker_id=worker_id,
            )

    retry = await client.post(f"/api/tasks/{task_id}/retry")
    assert retry.status_code == 409


@pytest.mark.parametrize("remote_possible", [False, True])
async def test_delete_reconcile_cancel_commits_exact_error_under_anyio(
    session_factory,
    remote_possible,
):
    from anyio import CancelScope

    _worker_id, task_id, _plan_id = await _worker_graph(
        session_factory,
        with_plan=False,
    )
    operation_id = await _prepare_receipt(session_factory, task_id)
    if remote_possible:
        async with session_factory() as db:
            await mark_manager_task_delete_remote_possible(db, operation_id)

    scope_holder: dict[str, CancelScope] = {}
    observed_method: str | None = None

    async def protocol_check(*_args, **_kwargs):
        return None

    async def cancel_proxy(_route, method, _path, **_kwargs):
        nonlocal observed_method
        observed_method = method
        scope_holder["scope"].cancel()
        await asyncio.sleep(0)

    async with session_factory() as db:
        with CancelScope() as scope:
            scope_holder["scope"] = scope
            with pytest.raises(asyncio.CancelledError):
                await termination.reconcile_manager_task_delete_receipt(
                    db,
                    operation_id,
                    proxy_request=cancel_proxy,
                    protocol_check=protocol_check,
                )

    expected_detail = (
        "Manager Task deletion audit was cancelled"
        if remote_possible
        else "Manager Task deletion was cancelled after remote_possible"
    )
    assert observed_method == ("GET" if remote_possible else "DELETE")
    async with session_factory() as db:
        receipt = await db.get(WorkerTaskTerminationReceipt, operation_id)
    assert receipt.status == "conflict"
    assert receipt.reconcile_count == 1
    assert receipt.last_error == expected_detail


async def test_api_cancellation_waits_for_exact_local_graph_commit(
    client,
    session_factory,
    monkeypatch,
):
    _worker_id, task_id, plan_id = await _worker_graph(session_factory)
    delete_started = asyncio.Event()
    release_delete = asyncio.Event()

    async def delayed_delete(_task, method, _path, _body=None, **_kwargs):
        assert method == "DELETE"
        delete_started.set()
        await release_delete.wait()
        return _delete_receipt([plan_id])

    proxy = AsyncMock()
    proxy.require_task_plan_delete_protocol.return_value = None
    proxy.proxy_to_worker.side_effect = delayed_delete
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    request_task = asyncio.create_task(client.delete(f"/api/tasks/{task_id}"))
    started_wait = asyncio.create_task(delete_started.wait())
    done, _pending = await asyncio.wait(
        {request_task, started_wait},
        timeout=5,
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert started_wait in done, (
        request_task.result().text if request_task in done else "DELETE did not start"
    )
    request_task.cancel()
    await asyncio.sleep(0)
    assert not request_task.done()
    release_delete.set()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    async with session_factory() as db:
        assert await db.get(Task, task_id) is None
        assert await db.get(Plan, plan_id) is None
        receipt_count = len(
            (
                await db.execute(
                    select(WorkerTaskTerminationReceipt).where(
                        WorkerTaskTerminationReceipt.task_id == task_id
                    )
                )
            ).scalars().all()
        )
        assert receipt_count == 0
