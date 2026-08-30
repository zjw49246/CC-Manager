"""Phase 2 测试：WorkerRelay 事件处理 / Dispatcher 双路径 / Chat 与操作代理。"""
import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock, patch

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

import backend.main as main_module
import backend.api.tasks as tasks_api_module
import backend.services.task_events as task_events_module
import backend.services.worker_proxy as worker_proxy_module
import backend.services.worker_relay as worker_relay_module
import backend.services.worker_task_termination as worker_termination_module
from backend.config import settings
from backend.models.log_entry import LogEntry
from backend.models.instance import Instance
from backend.models.monitor_session import MonitorCheck, MonitorSession
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMonitorTaskTombstone,
    PRReview,
    PRReviewerRun,
)
from backend.models.project import Project
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationReceipt,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentWorkerDispatchReceipt,
)
from backend.models.task import Task
from backend.models.user import User
from backend.models.test_harness import TestHarnessRun as HarnessRun
from backend.models.user_skill import UserSkill
from backend.models.worker import Worker
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
from backend.schemas.task import TaskCreate
from backend.services.worker_proxy import (
    WorkerEndpointNotFoundError,
    WorkerProxy,
    WorkerTaskMutationOutcomeUncertainError,
)
from backend.services.worker_relay import WorkerRelay
from backend.services.worker_routing_config import (
    WORKER_MIGRATION_IMPORT_RESERVATION_KEY,
    WORKER_ROUTING_PENDING_KEY,
)


pytestmark = pytest.mark.usefixtures("worker_control_plane_auth")


class FakeBroadcaster:
    def __init__(self):
        self.sent: list[tuple[str, dict]] = []

    async def broadcast(self, channel, data):
        self.sent.append((channel, data))


def _worker_namespace_capability() -> dict:
    return {
        "task_id_namespace_protocol": 1,
        "task_id_namespace_boundary": 1_000_000_000,
        "ccm_node_role": "worker",
    }


@pytest.fixture
def broadcaster():
    return FakeBroadcaster()


@pytest.fixture
def relay(db_factory, broadcaster):
    r = WorkerRelay(db_factory=db_factory, broadcaster=broadcaster)
    return r


def test_worker_proxy_network_boundaries_require_manager_auth(monkeypatch):
    worker = SimpleNamespace(
        auth_token="worker-token",
        private_ip="10.0.0.8",
        ssh_user="ubuntu",
        ssh_key_path="/unused/key",
        cloud_instance_id="i-auth-gate",
    )
    monkeypatch.setattr(settings, "auth_token", "")

    with pytest.raises(HTTPException) as headers_error:
        WorkerProxy._headers(worker)
    with pytest.raises(HTTPException) as ssh_error:
        WorkerProxy._ssh(worker)

    assert headers_error.value.status_code == 503
    assert ssh_error.value.status_code == 503
    assert "AUTH_TOKEN" in headers_error.value.detail


def test_worker_proxy_rejects_missing_worker_credential(monkeypatch):
    monkeypatch.setattr(settings, "auth_token", "manager-token")
    worker = SimpleNamespace(auth_token="   ")

    with pytest.raises(HTTPException) as exc_info:
        WorkerProxy._headers(worker)

    assert exc_info.value.status_code == 503
    assert "Worker authentication" in exc_info.value.detail


@pytest.mark.asyncio
async def test_worker_relay_refuses_network_without_manager_auth(
    relay,
    monkeypatch,
):
    worker = SimpleNamespace(
        id=81,
        private_ip="10.0.0.81",
        ccm_port=8002,
        auth_token="worker-token",
    )
    connect = AsyncMock()
    monkeypatch.setattr(settings, "auth_token", "")
    monkeypatch.setattr(worker_relay_module.websockets, "connect", connect)

    with pytest.raises(HTTPException) as exc_info:
        await relay.ensure_connection(worker)

    assert exc_info.value.status_code == 503
    connect.assert_not_awaited()


async def test_concurrent_worker_connection_admission_creates_one_transport(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    entered = asyncio.Event()
    release = asyncio.Event()
    sockets = []

    class FakeWebSocket:
        def __init__(self):
            self.sent = []
            self.closed = False

        async def send(self, payload):
            self.sent.append(json.loads(payload))

        async def close(self):
            self.closed = True

        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.Event().wait()

    async def connect(*_args, **_kwargs):
        socket = FakeWebSocket()
        sockets.append(socket)
        entered.set()
        await release.wait()
        return socket

    monkeypatch.setattr(
        worker_relay_module.websockets,
        "connect",
        connect,
    )
    first = asyncio.create_task(relay.ensure_connection(worker))
    await entered.wait()
    second = asyncio.create_task(relay.ensure_connection(worker))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    assert len(sockets) == 1
    assert relay._ws[worker.id] is sockets[0]
    assert len(relay._loops) == 1
    await relay.stop_worker(worker.id)


async def test_worker_relay_shutdown_closes_and_awaits_all_owned_tasks(relay):
    worker_id = 301
    socket = SimpleNamespace(close=AsyncMock())
    started = [asyncio.Event() for _ in range(3)]
    finalized = [asyncio.Event() for _ in range(3)]

    async def tracked_background(index):
        started[index].set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finalized[index].set()

    relay_task = asyncio.create_task(tracked_background(0))
    reconnect_task = asyncio.create_task(tracked_background(1))
    handoff_task = asyncio.create_task(tracked_background(2))
    relay._ws[worker_id] = socket
    relay._tasks[worker_id] = {11}
    relay._loops[worker_id] = relay_task
    relay._reconnect_tasks[worker_id] = {reconnect_task}
    relay._handoff_recovery_tasks[(worker_id, 11, "handoff-11")] = (
        handoff_task
    )
    await asyncio.gather(*(event.wait() for event in started))

    await relay.shutdown()
    await relay.shutdown()

    socket.close.assert_awaited_once_with()
    assert all(task.done() for task in (
        relay_task,
        reconnect_task,
        handoff_task,
    ))
    assert all(task.cancelled() for task in (
        relay_task,
        reconnect_task,
        handoff_task,
    ))
    assert all(event.is_set() for event in finalized)
    assert relay._ws == {}
    assert relay._tasks == {}
    assert relay._loops == {}
    assert relay._reconnect_tasks == {}
    assert relay._handoff_recovery_tasks == {}


async def test_worker_relay_shutdown_rejects_all_new_background_work(
    relay,
    monkeypatch,
):
    worker = SimpleNamespace(
        id=302,
        private_ip="10.0.0.10",
        ccm_port=8002,
        auth_token="worker-token",
    )
    observed = worker_relay_module.WorkerTaskGeneration(
        task_id=12,
        worker_id=worker.id,
        incarnation_id="1" * 32,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
        status="completed",
        retry_count=0,
        turn_generation=1,
        instance_id=None,
        started_at=None,
        completed_at=None,
        pty_background_generation=None,
        worker_turn_handoff_id="a" * 32,
        worker_turn_handoff_worker_id=worker.id,
        worker_turn_handoff_retry_count=0,
        worker_turn_handoff_from_generation=1,
        worker_turn_handoff_source_log_id=99,
        worker_turn_handoff_acknowledged=True,
    )
    connect = AsyncMock()
    monkeypatch.setattr(worker_relay_module.websockets, "connect", connect)

    await relay.shutdown()
    forbidden_db_factory = Mock(
        side_effect=AssertionError("shutdown path touched the database")
    )
    relay.db_factory = forbidden_db_factory
    relay._worker_turn_handoff_recovery_loop = AsyncMock()

    with pytest.raises(RuntimeError, match="shutting down"):
        await relay.ensure_connection(worker)
    await relay.recover(worker)
    await relay._reconnect(worker, {observed.task_id})
    relay._reconnect = AsyncMock()
    relay._schedule_reconnect(worker, {observed.task_id})
    relay.ensure_worker_turn_handoff_recovery(worker, observed)

    connect.assert_not_awaited()
    forbidden_db_factory.assert_not_called()
    relay._reconnect.assert_not_called()
    relay._worker_turn_handoff_recovery_loop.assert_not_called()
    assert relay._reconnect_tasks == {}
    assert relay._handoff_recovery_tasks == {}


async def test_worker_relay_shutdown_caller_cancellation_finishes_cleanup(
    relay,
):
    worker_id = 303
    close_started = asyncio.Event()
    release_close = asyncio.Event()
    close_finished = asyncio.Event()

    async def close():
        close_started.set()
        await release_close.wait()
        close_finished.set()

    relay._ws[worker_id] = SimpleNamespace(close=AsyncMock(side_effect=close))
    shutdown_call = asyncio.create_task(relay.shutdown())
    await close_started.wait()

    shutdown_call.cancel()
    await asyncio.sleep(0)
    assert not shutdown_call.done()
    release_close.set()

    with pytest.raises(asyncio.CancelledError):
        await shutdown_call
    assert close_finished.is_set()
    assert relay._shutdown_task is not None
    assert relay._shutdown_task.done()
    assert relay._ws == {}
    await relay.shutdown()


async def test_worker_relay_start_requires_completed_clean_shutdown(relay):
    worker_id = 304
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    async def close():
        close_started.set()
        await release_close.wait()

    await relay.start()
    relay._ws[worker_id] = SimpleNamespace(
        close=AsyncMock(side_effect=close)
    )
    shutdown_call = asyncio.create_task(relay.shutdown())
    await close_started.wait()

    with pytest.raises(RuntimeError, match="still in progress"):
        await relay.start()

    release_close.set()
    await shutdown_call
    relay._tasks[worker_id] = set()
    with pytest.raises(RuntimeError, match="owned resources"):
        await relay.start()

    relay._tasks.clear()
    await relay.start()
    assert relay._shutting_down is False
    assert relay._shutdown_task is None
    assert relay._closing == set()
    await relay.shutdown()


async def _mk_worker(session_factory, **fields) -> Worker:
    fields.setdefault("name", "w1")
    fields.setdefault("status", "ready")
    fields.setdefault("private_ip", "10.0.0.9")
    fields.setdefault("auth_token", "wtoken")
    async with session_factory() as db:
        w = Worker(**fields)
        db.add(w)
        await db.commit()
        await db.refresh(w)
        return w


async def _mk_task(session_factory, **fields) -> Task:
    fields.setdefault("status", "in_progress")
    fields.setdefault("description", "d")
    async with session_factory() as db:
        t = Task(title="t", **fields)
        db.add(t)
        await db.commit()
        await db.refresh(t)
        return t


async def _mk_worker_pr_reviewer_task(
    session_factory,
    *,
    review_status: str,
    reviewer_status: str | None = None,
    archived: bool = False,
) -> tuple[Worker, Task, int | None]:
    worker = await _mk_worker(
        session_factory,
        name=f"review-worker-{review_status}-{reviewer_status}-{archived}",
    )
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        archived=archived,
    )
    async with session_factory() as db:
        repo = MonitoredRepo(
            repo_full_name=f"example/worker-review-dispatch-{task.id}",
            webhook_secret="worker-review-secret",
        )
        db.add(repo)
        await db.flush()
        review = PRReview(
            repo_id=repo.id,
            pr_number=task.id,
            base_ref="main",
            base_sha="a" * 40,
            head_sha="b" * 40,
            pr_title="Worker reviewer dispatch",
            pr_author="alice",
            pr_url=(
                "https://github.com/example/worker-review-dispatch/"
                f"pull/{task.id}"
            ),
            # Panel review creation retains this legacy first-task link.  The
            # reviewer-run lifecycle must take precedence when present.
            task_id=task.id,
            status=review_status,
        )
        db.add(review)
        await db.flush()
        reviewer_id = None
        if reviewer_status is not None:
            reviewer = PRReviewerRun(
                pr_review_id=review.id,
                role="principal_engineer",
                task_id=task.id,
                provider="claude",
                status=reviewer_status,
                prompt_policy_hash="c" * 64,
                guide_pack_hash="d" * 64,
            )
            db.add(reviewer)
            await db.flush()
            reviewer_id = reviewer.id
        await db.commit()
    return worker, task, reviewer_id


@pytest.mark.parametrize("mapped", [False, True])
async def test_worker_project_reuse_rejects_stale_checkout_identity(
    db_factory,
    session_factory,
    monkeypatch,
    mapped,
):
    """A deleted/recreated Manager Project cannot inherit an old checkout."""

    async with session_factory() as db:
        project = Project(
            name="reused-project-name",
            git_url="https://example.test/new-owner/new-repository.git",
            has_remote=True,
            local_path="/workspace/reused-project-name",
            default_branch="main",
            status="ready",
        )
        db.add(project)
        await db.commit()
        await db.refresh(project)
        project_id = project.id
    worker = await _mk_worker(
        session_factory,
        project_mapping=({str(project_id): 77} if mapped else {}),
    )
    task = await _mk_task(
        session_factory,
        project_id=project_id,
    )
    stale_remote = {
        "id": 77,
        "name": "reused-project-name",
        "git_url": "https://example.test/former-owner/old-repository.git",
        "has_remote": True,
        "local_path": "/workspace/reused-project-name",
        "default_branch": "main",
        "status": "ready",
    }
    posted = []

    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, **kwargs):
            if mapped:
                assert url.endswith("/api/projects/77")
                return Response(stale_remote)
            assert url.endswith("/api/projects")
            return Response([stale_remote])

        async def post(self, *args, **kwargs):
            posted.append((args, kwargs))
            raise AssertionError("identity mismatch must fail before Project create")

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(db_factory, relay=AsyncMock())

    with pytest.raises(RuntimeError, match="源身份不一致"):
        await proxy.ensure_worker_project(worker, task)

    assert posted == []
    async with session_factory() as db:
        durable_worker = await db.get(Worker, worker.id)
    assert durable_worker.project_mapping == (
        {str(project_id): 77} if mapped else {}
    )


async def test_worker_project_reuse_accepts_exact_checkout_identity(
    db_factory,
    session_factory,
    monkeypatch,
):
    async with session_factory() as db:
        project = Project(
            name="exact-worker-project",
            git_url="https://example.test/owner/repository.git",
            has_remote=True,
            local_path="/workspace/exact-worker-project",
            default_branch="develop",
            status="ready",
        )
        db.add(project)
        await db.commit()
        await db.refresh(project)
        project_id = project.id
    worker = await _mk_worker(session_factory, project_mapping={})
    task = await _mk_task(session_factory, project_id=project_id)
    remote = {
        "id": 91,
        "name": "exact-worker-project",
        "git_url": "https://example.test/owner/repository.git",
        "has_remote": True,
        "local_path": "/workspace/exact-worker-project",
        "default_branch": "develop",
        "status": "ready",
    }

    class Response:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return [remote]

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, **kwargs):
            assert url.endswith("/api/projects")
            return Response()

        async def post(self, *args, **kwargs):
            raise AssertionError("an exact existing checkout must be reused")

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(db_factory, relay=AsyncMock())

    assert await proxy.ensure_worker_project(worker, task) == 91
    async with session_factory() as db:
        durable_worker = await db.get(Worker, worker.id)
    assert durable_worker.project_mapping == {str(project_id): 91}


def _remote_task(task: Task, **overrides) -> dict:
    task_turn_generation = getattr(task, "turn_generation", None)
    if type(task_turn_generation) is not int:
        task_turn_generation = 0
    payload = {
        "id": task.id,
        "incarnation_id": task.incarnation_id,
        "status": task.status,
        "retry_count": task.retry_count,
        "turn_generation": task_turn_generation,
        "session_id": task.session_id,
        "started_at": (
            task.started_at.isoformat() if task.started_at else None
        ),
        "completed_at": (
            task.completed_at.isoformat() if task.completed_at else None
        ),
        "error_message": task.error_message,
        "execution_user_id": task.execution_user_id,
        "execution_user_role": task.execution_user_role,
        "execution_mode": task.execution_mode,
        "execution_principal_kind": task.execution_principal_kind,
    }
    payload.update(overrides)
    return payload


def _terminal_worker_receipt(
    manager,
    *,
    rejected: bool,
    terminal_status: str = "cancelled",
    response: dict | None = None,
    task_overrides: dict | None = None,
) -> dict:
    if rejected:
        result = {
            "version": 2,
            "operation_id": manager.operation_id,
            "task_id": manager.task_id,
            "operation": manager.operation,
            "request_digest": manager.request_digest,
            "rejected": True,
            "error": "exact generation changed",
        }
        status = "rejected"
        last_error = result["error"]
    else:
        task_snapshot = {
            "id": manager.task_id,
            "status": terminal_status,
            "retry_count": manager.source_task_retry_count,
            "turn_generation": manager.source_task_turn_generation,
            "instance_id": None,
            "started_at": None,
            "completed_at": "2026-01-02T03:04:06.000000",
            "session_id": None,
            "error_message": None,
            "background_active": False,
        }
        task_snapshot.update(task_overrides or {})
        result = {
            "version": 2,
            "operation_id": manager.operation_id,
            "task_id": manager.task_id,
            "operation": manager.operation,
            "request_digest": manager.request_digest,
            "task": task_snapshot,
            "response": response or {"ok": True},
        }
        status = "succeeded"
        last_error = None
    return {
        "version": 2,
        "operation_id": manager.operation_id,
        "task_id": manager.task_id,
        "side": "worker",
        "worker_id": None,
        "operation": manager.operation,
        "status": status,
        "state_version": 3,
        "source": {
            "incarnation_id": "1" * 32,
            "status": manager.source_task_status,
            "retry_count": manager.source_task_retry_count,
            "turn_generation": manager.source_task_turn_generation,
            "source_log_id": None,
            "instance_id": None,
            "started_at": None,
            "completed_at": None,
            "session_id": None,
            "pty_background_generation": None,
        },
        "request_payload": manager.request_payload,
        "request_digest": manager.request_digest,
        "result_payload": result,
        "result_digest": worker_termination_module.canonical_json_digest(result),
        "attempt_count": 1,
        "reconcile_count": 0,
        "last_error": last_error,
        "accepted_at": "2026-01-02T03:04:05.000000",
        "completed_at": "2026-01-02T03:04:06.000000",
        "ack_intent_at": None,
        "acknowledged_at": None,
        "created_at": "2026-01-02T03:04:05.000000",
        "updated_at": "2026-01-02T03:04:06.000000",
    }


def _durable_terminal_protocol(
    task: Task,
    *,
    terminal_status: str,
    response: dict | None = None,
    task_overrides: dict | None = None,
):
    """Return a strict GET -> PUT -> ACK Worker receipt double."""

    state: dict[str, dict | None] = {"wire": None}

    async def protocol(_task, method, path, body=None, **_kwargs):
        operation_path = path.removesuffix("/ack")
        operation_id = operation_path.rsplit("/", 1)[-1]
        if method == "GET":
            if state["wire"] is None:
                return worker_termination_module.receipt_not_found_payload(
                    task.id,
                    operation_id,
                )
            return state["wire"]
        if method == "PUT":
            request_payload = body["request_payload"]
            expected = request_payload["expected_remote"]
            manager = SimpleNamespace(
                operation_id=operation_id,
                task_id=task.id,
                operation=body["operation"],
                request_payload=request_payload,
                request_digest=body["request_digest"],
                source_task_status=expected["status"],
                source_task_retry_count=expected["retry_count"],
                source_task_turn_generation=expected["turn_generation"],
            )
            state["wire"] = _terminal_worker_receipt(
                manager,
                rejected=False,
                terminal_status=terminal_status,
                response=response,
                task_overrides=task_overrides,
            )
            return state["wire"]
        assert method == "POST" and path.endswith("/ack")
        acknowledged = deepcopy(state["wire"])
        acknowledged["status"] = "acknowledged"
        acknowledged["state_version"] += 1
        acknowledged["acknowledged_at"] = "2026-01-02T03:04:07.000000"
        acknowledged["updated_at"] = "2026-01-02T03:04:07.000000"
        state["wire"] = acknowledged
        return acknowledged

    return protocol, state


def _relay_generation(task: Task) -> dict[str, int]:
    return {
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation,
    }


def _sub_agent_relay_payload(
    task: Task,
    *,
    remote_id: int,
    status: str,
    description: str = "worker child",
    checks_done: int = 0,
    last_summary: str | None = "do the work",
) -> dict:
    return {
        "sub_agent_session_id": remote_id,
        "description": description,
        "agent_type": "sub_agent",
        "source": "ccm",
        "monitor_context": "frozen context",
        "status": status,
        "checks_done": checks_done,
        "last_summary": last_summary,
        **_relay_generation(task),
    }


def _internal_worker_handoff_payload(
    task: Task,
    *,
    handoff_id: str,
    message: str,
) -> dict:
    principal = worker_relay_module.canonical_delegated_principal_payload(task)
    assert principal is not None
    return {
        "message": message,
        "worker_turn_handoff_id": handoff_id,
        "worker_turn_handoff_retry_count": task.retry_count,
        "worker_turn_handoff_from_generation": task.turn_generation,
        "worker_turn_handoff_incarnation_id": task.incarnation_id,
        **principal,
    }


def _worker_task_incarnation_headers(task: Task) -> dict[str, str]:
    assert task.incarnation_id is not None
    return {"X-CCM-Task-Incarnation": task.incarnation_id}


def _remote_worker_handoff_receipt(
    task: Task,
    request_body: dict,
    *,
    status: str,
    turn_generation: int | None = None,
) -> dict:
    principal = worker_relay_module.canonical_delegated_principal_payload(
        request_body
    )
    assert principal is not None
    admitted_routing = (
        (task.provider or "claude").lower(),
        request_body.get("model") or task.model,
        task.codex_service_tier or "default",
    )
    identity = worker_relay_module.worker_turn_handoff_request_identity(
        request_body,
        admitted_routing,
    )
    if turn_generation is None and status in {"claimed", "launching", "launched"}:
        turn_generation = task.turn_generation + 1
    return {
        "handoff_id": request_body["worker_turn_handoff_id"],
        "task_id": task.id,
        "incarnation_id": task.incarnation_id,
        "status": status,
        "retry_count": task.retry_count,
        "from_generation": task.turn_generation,
        "turn_generation": turn_generation,
        "source_log_id": 91,
        "request_digest": worker_relay_module._handoff_payload_digest(identity),
        "principal_digest": worker_relay_module._handoff_payload_digest(
            principal
        ),
        "response": {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
        },
    }


async def _reserve_worker_handoff(session_factory, task: Task):
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        observed = worker_relay_module.worker_task_generation(current)
        assert observed is not None
        log = LogEntry(
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="reserved follow-up",
        )
        db.add(log)
        await db.flush()
        handoff_id = f"{task.id:032x}"
        request_payload = {
            "message": "reserved follow-up",
            "worker_turn_handoff_id": handoff_id,
            "worker_turn_handoff_retry_count": task.retry_count,
            "worker_turn_handoff_from_generation": task.turn_generation,
            "worker_turn_handoff_incarnation_id": task.incarnation_id,
            **worker_relay_module.canonical_delegated_principal_payload(
                current
            ),
        }
        reserved = await worker_relay_module.reserve_worker_turn_handoff(
            db,
            observed,
            handoff_id=handoff_id,
            source_log_id=log.id,
            request_payload=request_payload,
            request_digest=worker_relay_module._handoff_payload_digest(
                request_payload
            ),
        )
        assert reserved is not None
        await db.commit()
        return reserved


async def _add_unsettled_harness_graph(
    session_factory,
    task: Task,
    *,
    run_key: str,
    run_status: str,
    cleanup_status: str,
) -> str:
    """Inject the durable legacy state an upgraded Manager must drain."""

    run_id = run_key * 32
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current is not None
        db.add(
            HarnessRun(
                id=run_id,
                task_id=current.id,
                owner_task_incarnation_id=current.incarnation_id,
                owner_task_retry_count=current.retry_count,
                owner_task_turn_generation=current.turn_generation,
                owner_task_status=current.status,
                target_kind="fixed_url",
                target_spec={"url": "https://example.com"},
                test_plan={"objective": "preserve pre-existing owner graph"},
                runtime_config={"provider": "codex"},
                request_fingerprint=run_key * 64,
                root_run_id=run_id,
                status=run_status,
                stage=(
                    "completed"
                    if run_status == "completed"
                    else "running"
                ),
                cleanup_status=cleanup_status,
            )
        )
        await db.commit()
    return run_id


@pytest.mark.parametrize(
    ("run_status", "cleanup_status"),
    [
        ("running", "pending"),
        ("completed", "failed"),
    ],
)
async def test_worker_handoff_reservation_rejects_unsettled_harness_graph(
    session_factory,
    run_status,
    cleanup_status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
    )
    run_id = ("a" if run_status == "running" else "b") * 32
    handoff_id = f"{task.id:032x}"
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        db.add(
            HarnessRun(
                id=run_id,
                task_id=current.id,
                owner_task_incarnation_id=current.incarnation_id,
                owner_task_retry_count=current.retry_count,
                owner_task_turn_generation=current.turn_generation,
                owner_task_status=current.status,
                target_kind="fixed_url",
                target_spec={"url": "https://example.com"},
                test_plan={"objective": "preserve exact owner graph"},
                runtime_config={"provider": "codex"},
                request_fingerprint="c" * 64,
                root_run_id=run_id,
                status=run_status,
                stage="completed" if run_status == "completed" else "running",
                cleanup_status=cleanup_status,
            )
        )
        source = LogEntry(
            task_id=current.id,
            event_type="user_message",
            role="user",
            content="must not cross harness cleanup",
        )
        db.add(source)
        await db.commit()
        await db.refresh(source)

        observed = worker_relay_module.worker_task_generation(current)
        assert observed is not None
        request_payload = {
            "message": source.content,
            "worker_turn_handoff_id": handoff_id,
            "worker_turn_handoff_retry_count": current.retry_count,
            "worker_turn_handoff_from_generation": current.turn_generation,
        }
        reserved = await worker_relay_module.reserve_worker_turn_handoff(
            db,
            observed,
            handoff_id=handoff_id,
            source_log_id=source.id,
            request_payload=request_payload,
            request_digest=worker_relay_module._handoff_payload_digest(
                request_payload
            ),
        )

        assert reserved is None
        persisted = await db.get(Task, task.id)
        receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
        assert persisted.worker_turn_handoff_id is None
        assert persisted.turn_generation == task.turn_generation
        assert receipt is None


async def test_preexisting_harness_graph_blocks_handoff_replay_effects(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    replay = await relay._manager_worker_turn_handoff_request(reserved)
    assert replay is not None
    await _add_unsettled_harness_graph(
        session_factory,
        task,
        run_key="d",
        run_status="completed",
        cleanup_status="failed",
    )
    post_client = SimpleNamespace(post=AsyncMock())
    client_factory = Mock()
    monkeypatch.setattr(
        worker_relay_module.httpx,
        "AsyncClient",
        client_factory,
    )

    posted = await relay._post_worker_turn_handoff_request(
        worker,
        reserved,
        replay,
        client=post_client,
    )
    resumed = await relay._resume_worker_turn_handoff(
        worker,
        reserved,
        replay,
    )

    assert posted is False
    assert resumed is False
    post_client.post.assert_not_awaited()
    client_factory.assert_not_called()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    assert current.worker_turn_handoff_acknowledged is False
    assert receipt.status == "prepared"


async def test_preexisting_harness_graph_blocks_handoff_event_adoption(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    await _add_unsettled_harness_graph(
        session_factory,
        task,
        run_key="e",
        run_status="running",
        cleanup_status="pending",
    )

    adopted = await relay._observe_or_adopt_event_generation(
        worker.id,
        task.id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation + 1,
        worker_turn_handoff_id=reserved.worker_turn_handoff_id,
    )

    assert adopted is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "completed"
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id


async def test_preexisting_harness_graph_blocks_handoff_snapshot_recovery(
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    await _add_unsettled_harness_graph(
        session_factory,
        task,
        run_key="f",
        run_status="completed",
        cleanup_status="failed",
    )

    async with session_factory() as db:
        resulting = await worker_relay_module.apply_authoritative_worker_task(
            db,
            reserved,
            _remote_task(
                task,
                status="completed",
                turn_generation=task.turn_generation + 1,
            ),
            worker_turn_handoff_id=reserved.worker_turn_handoff_id,
        )

    assert resulting is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "completed"
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id


async def _create_manager_termination_receipt(
    session_factory,
    task_id: int,
    *,
    operation: str = "cancel",
):
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        return await worker_termination_module.create_or_resume_manager_receipt(
            db,
            task,
            operation=operation,
        )


def _launched_handoff_receipt(reserved) -> dict:
    delegated_principal = (
        worker_relay_module.canonical_delegated_principal_payload(
            reserved
        )
    )
    assert delegated_principal is not None
    request_payload = {
        "message": "reserved follow-up",
        "worker_turn_handoff_id": reserved.worker_turn_handoff_id,
        "worker_turn_handoff_retry_count": (
            reserved.worker_turn_handoff_retry_count
        ),
        "worker_turn_handoff_from_generation": (
            reserved.worker_turn_handoff_from_generation
        ),
        "worker_turn_handoff_incarnation_id": reserved.incarnation_id,
        **delegated_principal,
    }
    return {
        "handoff_id": reserved.worker_turn_handoff_id,
        "task_id": reserved.task_id,
        "incarnation_id": reserved.incarnation_id,
        "status": "launched",
        "retry_count": reserved.worker_turn_handoff_retry_count,
        "from_generation": reserved.worker_turn_handoff_from_generation,
        "turn_generation": reserved.worker_turn_handoff_from_generation + 1,
        "source_log_id": 9001,
        "request_digest": worker_relay_module._handoff_payload_digest(
            request_payload
        ),
        "principal_digest": worker_relay_module._handoff_payload_digest(
            delegated_principal
        ),
        "response": {"ok": True, "queued": True},
    }


def _handoff_receipt_with_status(reserved, status: str) -> dict:
    receipt = _launched_handoff_receipt(reserved)
    receipt["status"] = status
    if status in {"accepted", "cancelled"}:
        receipt["turn_generation"] = None
    return receipt


def _mock_launched_handoff_receipts(relay, session_factory) -> None:
    async def fetch(
        _worker,
        task_id,
        handoff_id,
        incarnation_id,
        **_kwargs,
    ):
        async with session_factory() as db:
            task = await db.get(Task, task_id)
            receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
        if (
            task is None
            or task.worker_turn_handoff_id != handoff_id
            or task.incarnation_id != incarnation_id
        ):
            return None
        assert receipt is not None
        receipt_identity = receipt.request_payload
        if receipt_identity.get("version") == 2:
            receipt_identity = receipt_identity.get("identity")
        assert isinstance(receipt_identity, dict)
        delegated_principal = (
            worker_relay_module.canonical_delegated_principal_payload(
                receipt_identity
            )
        )
        assert delegated_principal is not None
        return {
            "handoff_id": handoff_id,
            "task_id": task_id,
            "incarnation_id": task.incarnation_id,
            "status": "launched",
            "retry_count": task.worker_turn_handoff_retry_count,
            "from_generation": task.worker_turn_handoff_from_generation,
            "turn_generation": task.worker_turn_handoff_from_generation + 1,
            "source_log_id": 9001,
            "request_digest": receipt.request_digest,
            "principal_digest": worker_relay_module._handoff_payload_digest(
                delegated_principal
            ),
            "response": {"ok": True, "queued": True},
        }

    relay._fetch_worker_turn_handoff_receipt = AsyncMock(side_effect=fetch)


def _routing_snapshot(
    task: Task,
    *,
    status: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    codex_service_tier: str | None = None,
    pending: dict | None = None,
) -> dict:
    return {
        "id": task.id,
        "status": status or task.status,
        "worker_id": None,
        "shared_from_id": None,
        "provider": provider or task.provider,
        "model": task.model if model is None else model,
        "codex_service_tier": (
            codex_service_tier or task.codex_service_tier
        ),
        "pending": pending,
    }


def _worker_manual_retry_response(
    task: Task,
    request: dict,
    *,
    task_overrides: dict | None = None,
) -> dict:
    """Build the exact durable Worker acknowledgement for one retry POST."""

    target_principal = {
        field: request[field]
        for field in (
            "execution_user_id",
            "execution_user_role",
            "execution_mode",
            "execution_principal_kind",
        )
    }
    source_generation = {
        "task_id": request["task_id"],
        "worker_id": request["worker_id"],
        "incarnation_id": request["source_incarnation_id"],
        "status": request["expected_status"],
        "retry_count": request["expected_retry_count"],
        "turn_generation": request["expected_turn_generation"],
        "principal_digest": request["source_principal_digest"],
    }
    result_generation = {
        "status": "pending",
        "retry_count": request["expected_retry_count"] + 1,
        "turn_generation": request["expected_turn_generation"],
    }
    remote_task = _remote_task(
        task,
        status="pending",
        retry_count=result_generation["retry_count"],
        turn_generation=result_generation["turn_generation"],
        completed_at=None,
        **target_principal,
    )
    if task_overrides:
        remote_task.update(task_overrides)
    return {
        "task": remote_task,
        "receipt": {
            "version": request["protocol_version"],
            "side": "worker",
            "state": "committed",
            "operation_id": request["operation_id"],
            "request_digest": request["request_digest"],
            "source_generation": source_generation,
            "result_generation": result_generation,
            "source_principal_digest": request["source_principal_digest"],
            "target_principal_digest": request["target_principal_digest"],
            "target_principal": target_principal,
            "committed_at": datetime.utcnow().isoformat(
                timespec="microseconds"
            ),
        },
    }


async def test_authoritative_worker_apply_preserves_supersede_marker(
    session_factory,
):
    """Normal relay/proxy convergence cannot drop a lost-response gate."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        metadata_={"pr_review_id": 37},
    )
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None

    async with session_factory() as db:
        resulting = await (
            worker_relay_module.apply_authoritative_worker_task(
                db,
                observed,
                _remote_task(
                    task,
                    status="completed",
                    metadata_={"pr_review_superseded": True},
                ),
            )
        )

    assert resulting is not None
    assert resulting.status == "completed"
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.metadata_ == {
            "pr_review_id": 37,
            "pr_review_superseded": True,
            "ccm_worker_remote_materialized_v1": True,
        }


async def test_worker_plan_mirror_preserves_manager_local_audit_ids(
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        mode="plan",
        plan_approved=True,
        plan_approved_by=42,
        plan_applied_log_id=314,
        plan_execution_task_id=2718,
    )
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None

    async with session_factory() as db:
        resulting = await (
            worker_relay_module.apply_authoritative_worker_task(
                db,
                observed,
                _remote_task(
                    task,
                    # These ids belong to the Worker's database/user namespace
                    # and must never overwrite Manager-local audit links.
                    plan_approved_by=9001,
                    plan_applied_log_id=9002,
                    plan_execution_task_id=9003,
                ),
            )
        )

    assert resulting is not None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.plan_approved_by == 42
    assert current.plan_applied_log_id == 314
    assert current.plan_execution_task_id == 2718


@pytest.mark.parametrize(
    ("remote_value", "should_update", "expected_marker"),
    [
        (True, True, worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL),
        (False, True, None),
        (None, False, "manager-owned"),
        (1, False, "manager-owned"),
        ("true", False, "manager-owned"),
    ],
)
async def test_authoritative_worker_background_uses_only_strict_boolean(
    session_factory,
    remote_value,
    should_update,
    expected_marker,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        pty_background_generation="manager-owned",
    )
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None
    remote = _remote_task(
        task,
        background_active=remote_value,
        # A remote token is never a mirror-safe field.
        pty_background_generation="remote-secret-generation",
    )

    async with session_factory() as db:
        resulting = await (
            worker_relay_module.apply_authoritative_worker_task(
                db,
                observed,
                remote,
            )
        )

    assert resulting is not None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.pty_background_generation == expected_marker
    if should_update:
        assert (
            current.pty_background_generation
            != "remote-secret-generation"
        )


async def test_authoritative_worker_background_missing_field_preserves_marker(
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        pty_background_generation="manager-owned",
    )
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None

    async with session_factory() as db:
        resulting = await (
            worker_relay_module.apply_authoritative_worker_task(
                db,
                observed,
                _remote_task(task),
            )
        )

    assert resulting is not None
    assert resulting.pty_background_generation == "manager-owned"


@pytest.mark.parametrize(
    "remote_turn_generation",
    [None, True, -1],
    ids=["missing", "boolean", "negative"],
)
async def test_authoritative_worker_snapshot_requires_valid_turn_generation(
    session_factory,
    remote_turn_generation,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        turn_generation=5,
    )
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None
    remote = _remote_task(task)
    if remote_turn_generation is None:
        remote.pop("turn_generation")
    else:
        remote["turn_generation"] = remote_turn_generation

    async with session_factory() as db:
        resulting = await (
            worker_relay_module.apply_authoritative_worker_task(
                db,
                observed,
                remote,
            )
        )

    assert resulting is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == task.status
    assert current.turn_generation == 5


async def test_authoritative_worker_apply_rejects_turn_generation_only_aba(
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        turn_generation=8,
    )
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(turn_generation=9)
        )
        await db.commit()
    remote = _remote_task(
        task,
        status="completed",
        turn_generation=8,
    )

    async with session_factory() as db:
        resulting = await (
            worker_relay_module.apply_authoritative_worker_task(
                db,
                observed,
                remote,
            )
        )

    assert resulting is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "in_progress"
    assert current.turn_generation == 9


async def test_authoritative_worker_apply_final_cas_rechecks_termination_owner(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        retry_count=2,
        turn_generation=8,
    )
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None
    receipt = await _create_manager_termination_receipt(
        session_factory,
        task.id,
    )
    # Simulate an earlier receipt lookup that raced and returned a stale
    # "allowed" answer.  The final correlated UPDATE must still reject the
    # ordinary relay writer after termination has claimed the active slot.
    stale_precheck = AsyncMock(return_value=True)
    monkeypatch.setattr(
        worker_termination_module,
        "manager_receipt_allows_authoritative_apply",
        stale_precheck,
    )

    async with session_factory() as db:
        resulting = await worker_relay_module.apply_authoritative_worker_task(
            db,
            observed,
            _remote_task(task, status="completed"),
        )

    assert resulting is None
    stale_precheck.assert_awaited_once_with(db, task.id, None)
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        active = await worker_termination_module.active_worker_task_termination_receipt(
            db,
            task.id,
        )
    assert current.status == "in_progress"
    assert current.turn_generation == task.turn_generation
    assert active.operation_id == receipt.operation_id
    assert active.status == "pending_remote"


async def test_authoritative_worker_generation_change_clears_only_stale_source(
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        retry_count=2,
        turn_generation=8,
    )
    async with session_factory() as db:
        source = LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            turn_scope="source",
            event_type="user_message",
            role="user",
            content="current exact source",
        )
        db.add(source)
        await db.flush()
        current = await db.get(Task, task.id)
        current.turn_source_log_id = source.id
        observed = worker_relay_module.worker_task_generation(current)
        source_id = source.id
        await db.commit()
    assert observed is not None

    # A same-generation status snapshot must preserve an already validated
    # source; this is not a blanket mirror cleanup.
    async with session_factory() as db:
        same = await worker_relay_module.apply_authoritative_worker_task(
            db,
            observed,
            _remote_task(task),
        )
    assert same is not None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.turn_source_log_id == source_id

    # retry_count is part of terminal identity even when the logical turn
    # counter is unchanged, so adoption must discard the old source pointer.
    async with session_factory() as db:
        retried = await worker_relay_module.apply_authoritative_worker_task(
            db,
            observed,
            _remote_task(task, retry_count=task.retry_count + 1),
        )
    assert retried is not None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.retry_count == task.retry_count + 1
    assert current.turn_source_log_id is None


def test_worker_proxy_ssh_is_scoped_to_cloud_instance(monkeypatch):
    ssh_factory = Mock()
    monkeypatch.setattr(worker_proxy_module, "SSHExecutor", ssh_factory)
    monkeypatch.setattr(
        worker_proxy_module,
        "worker_known_hosts_path",
        Mock(return_value="/tmp/known-hosts/i-worker-proxy"),
    )
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        name="scoped-worker",
        private_ip="10.0.0.9",
        ssh_user="ubuntu",
        ssh_key_path="/tmp/worker-key",
        cloud_instance_id="i-worker-proxy",
        auth_token="worker-token",
    )

    proxy._ssh(worker)

    worker_proxy_module.worker_known_hosts_path.assert_called_once_with(
        "i-worker-proxy"
    )
    ssh_factory.assert_called_once_with(
        host="10.0.0.9",
        user="ubuntu",
        key_path="/tmp/worker-key",
        known_hosts_path="/tmp/known-hosts/i-worker-proxy",
    )


async def test_worker_task_operation_lock_blocks_concurrent_reforward():
    proxy = WorkerProxy(None, relay=AsyncMock())
    task = Task(id=321, title="remote", worker_id=7)
    forwarded = asyncio.Event()

    async def record_forward(_task):
        forwarded.set()

    proxy._forward_task_to_worker_locked = AsyncMock(
        side_effect=record_forward
    )
    operation_lock = proxy.task_operation_lock(task.id)
    await operation_lock.acquire()
    forward_task = asyncio.create_task(proxy.forward_task_to_worker(task))
    await asyncio.sleep(0)
    assert not forwarded.is_set()

    operation_lock.release()
    await forward_task
    assert forwarded.is_set()


async def test_worker_forward_rejects_ready_destroy_recovery_worker(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(
        session_factory,
        status="ready",
        bootstrap_step="destroy",
    )
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        turn_generation=1,
    )
    relay = AsyncMock()
    proxy = WorkerProxy(db_factory, relay=relay)

    with pytest.raises(RuntimeError, match="destroy"):
        await proxy._forward_task_to_worker_locked(task)

    relay.subscribe_task.assert_not_awaited()


async def test_worker_forward_preserves_pr_review_tag_through_task_create(
    monkeypatch,
):
    """The Worker copy retains the internal endpoint's routing fallback tag."""

    captured_payload = {}

    class Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url, *, headers):
            return Response({
                "default_codex_model": "gpt-5.6-sol",
                "codex_model_service_tiers": {
                    "gpt-5.6-sol": ["default", "priority"],
                },
                "pr_review_snapshot_context_version": 3,
                "worker_delegated_principal_protocol": 1,
                "worker_delegated_launch_admission_protocol": 2,
                "worker_initial_generation_protocol": 1,
                "worker_task_incarnation_proxy_version": 1,
                **_worker_namespace_capability(),
            })

        async def post(self, _url, *, headers, json):
            captured_payload.update(json)
            return Response({
                **json,
                "incarnation_id": json["source_incarnation_id"],
                "metadata_": {
                    "ccm_source_task_incarnation_id": json[
                        "source_incarnation_id"
                    ]
                },
                "status": "pending",
                "retry_count": json["source_retry_count"],
                "turn_generation": json["source_turn_generation"] - 1,
            })

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    relay = AsyncMock()
    proxy = WorkerProxy(None, relay)
    worker = Worker(
        id=77,
        name="worker",
        status="ready",
        private_ip="10.0.0.77",
        auth_token="token",
    )
    task = Task(
        id=901,
        title="PR Review: owner/repo#1",
        description="review",
        worker_id=worker.id,
        project_id=12,
        priority=0,
        max_retries=2,
        mode="auto",
        max_iterations=50,
        must_complete=False,
        goal_max_turns=30,
        provider="codex",
        codex_service_tier="priority",
        enable_workflows=False,
        selected_user_skills=[5],
        tags=["pr-review"],
        attention_tag="等审核发布后再看",
        metadata_={"pr_review_id": 123},
        incarnation_id="1" * 32,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
        status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=34)
    proxy.require_worker_delegated_principal_support = AsyncMock()
    proxy._user_skill_snapshots = AsyncMock(return_value=[{
        "id": 5,
        "name": "Manager skill",
        "description": "copied",
        "content": "body",
    }])

    await proxy._forward_task_to_worker_locked(task)

    parsed_on_worker = TaskCreate.model_validate(captured_payload)
    assert captured_payload["tags"] == ["pr-review"]
    assert captured_payload["attention_tag"] == "等审核发布后再看"
    assert captured_payload["selected_user_skills"] == [5]
    assert captured_payload["user_skill_snapshots"] == [{
        "id": 5,
        "name": "Manager skill",
        "description": "copied",
        "content": "body",
    }]
    assert captured_payload["codex_service_tier"] == "priority"
    assert captured_payload["project_id"] is None
    assert parsed_on_worker.tags == ["pr-review"]
    assert parsed_on_worker.attention_tag == "等审核发布后再看"
    assert parsed_on_worker.project_id is None
    assert parsed_on_worker.codex_service_tier == "priority"
    proxy.ensure_worker_project.assert_not_awaited()
    # metadata_ is intentionally not a public TaskCreate field; the hidden
    # termination endpoint accepts the forwarded tag only for Worker copies.
    assert not hasattr(parsed_on_worker, "metadata_")


@pytest.mark.asyncio
async def test_initial_worker_forward_rejects_old_principal_protocol_before_post(
    monkeypatch,
):
    post = AsyncMock()

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"worker_delegated_principal_protocol": 0}

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return Response()

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        id=76,
        name="old-principal-worker",
        status="ready",
        private_ip="10.0.0.76",
        auth_token="token",
    )
    task = Task(
        id=900,
        title="must not reach old worker",
        description="delegated authority cannot be ignored",
        worker_id=worker.id,
        provider="claude",
        incarnation_id="0" * 32,
        status="in_progress",
        retry_count=0,
        turn_generation=1,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
    )
    proxy.get_worker = AsyncMock(return_value=worker)

    with pytest.raises(RuntimeError, match="delegated principal.*任务未转发"):
        await proxy._forward_task_to_worker_locked(task)

    post.assert_not_awaited()
    proxy.relay.subscribe_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_worker_forward_rejects_missing_launch_admission_before_post(
    monkeypatch,
):
    post = AsyncMock()

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "worker_delegated_principal_protocol": 1,
                # Intentionally omit the final provider-boundary protocol.
                **_worker_namespace_capability(),
            }

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return Response()

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        id=762,
        name="old-launch-admission-worker",
        status="ready",
        private_ip="10.0.0.762",
        auth_token="token",
    )
    task = Task(
        id=904,
        title="must not launch without final Manager permit",
        description="mixed versions fail before mutation",
        worker_id=worker.id,
        provider="claude",
        incarnation_id="6" * 32,
        status="in_progress",
        retry_count=0,
        turn_generation=1,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
    )
    proxy.get_worker = AsyncMock(return_value=worker)

    with pytest.raises(RuntimeError, match="provider launch admission"):
        await proxy._forward_task_to_worker_locked(task)

    post.assert_not_awaited()
    proxy.relay.subscribe_task.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "namespace_config",
    [
        {},
        {
            "task_id_namespace_protocol": 1,
            "task_id_namespace_boundary": 1_000_000_000,
            "ccm_node_role": "manager",
        },
    ],
    ids=["missing-protocol", "wrong-node-role"],
)
async def test_initial_worker_forward_rejects_invalid_id_namespace_before_effects(
    monkeypatch,
    namespace_config,
):
    post = AsyncMock()

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "worker_delegated_principal_protocol": 1,
                "worker_delegated_launch_admission_protocol": 2,
                **namespace_config,
            }

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return Response()

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        id=761,
        name="wrong-namespace-worker",
        status="ready",
        private_ip="10.0.0.61",
        auth_token="token",
    )
    task = Task(
        id=901,
        title="must not cross a mixed-version boundary",
        description="namespace is part of task identity",
        worker_id=worker.id,
        provider="claude",
        incarnation_id="a" * 32,
        status="in_progress",
        retry_count=0,
        turn_generation=1,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
    )
    proxy.get_worker = AsyncMock(return_value=worker)

    with pytest.raises(RuntimeError, match="Task ID namespace.*任务未转发"):
        await proxy._forward_task_to_worker_locked(task)

    post.assert_not_awaited()
    proxy.relay.subscribe_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_worker_forward_requires_exact_delegated_principal_ack(
    monkeypatch,
):
    from backend.services.worker_proxy import (
        WorkerTaskForwardOutcomeUncertainError,
    )

    class Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return Response({
                "worker_delegated_principal_protocol": 1,
                "worker_delegated_launch_admission_protocol": 2,
                "worker_initial_generation_protocol": 1,
                "worker_task_incarnation_proxy_version": 1,
                **_worker_namespace_capability(),
            })

        async def post(self, _url, *, headers, json):
            mismatched = dict(json)
            mismatched["incarnation_id"] = json["source_incarnation_id"]
            mismatched["metadata_"] = {
                "ccm_source_task_incarnation_id": json[
                    "source_incarnation_id"
                ]
            }
            mismatched["execution_user_role"] = "member"
            mismatched["execution_mode"] = "sandbox"
            return Response(mismatched)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        id=75,
        name="drifting-principal-worker",
        status="ready",
        private_ip="10.0.0.75",
        auth_token="token",
    )
    task = Task(
        id=899,
        title="admin worker task",
        description="retain delegated admin authority",
        worker_id=worker.id,
        provider="claude",
        execution_user_id=51,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
        incarnation_id="2" * 32,
        status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=None)
    proxy._user_skill_snapshots = AsyncMock(return_value=[])

    with pytest.raises(
        WorkerTaskForwardOutcomeUncertainError,
        match="exact delegated principal",
    ):
        await proxy._forward_task_to_worker_locked(task)


@pytest.mark.asyncio
async def test_initial_worker_forward_requires_exact_incarnation_ack(
    monkeypatch,
):
    from backend.services.worker_proxy import (
        WorkerTaskForwardOutcomeUncertainError,
    )

    class Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return Response({
                "worker_delegated_principal_protocol": 1,
                "worker_delegated_launch_admission_protocol": 2,
                "worker_initial_generation_protocol": 1,
                "worker_task_incarnation_proxy_version": 1,
                **_worker_namespace_capability(),
            })

        async def post(self, _url, *, headers, json):
            return Response({
                **json,
                "incarnation_id": "f" * 32,
                "metadata_": {
                    "ccm_source_task_incarnation_id": json[
                        "source_incarnation_id"
                    ]
                },
            })

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        id=74,
        name="wrong-incarnation-worker",
        status="ready",
        private_ip="10.0.0.74",
        auth_token="token",
    )
    task = Task(
        id=898,
        title="system worker task",
        description="retain exact logical Task identity",
        worker_id=worker.id,
        provider="claude",
        incarnation_id="e" * 32,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
        status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=None)
    proxy._user_skill_snapshots = AsyncMock(return_value=[])

    with pytest.raises(
        WorkerTaskForwardOutcomeUncertainError,
        match="exact Task incarnation",
    ):
        await proxy._forward_task_to_worker_locked(task)


@pytest.mark.parametrize(
    ("changed_field", "changed_value", "error"),
    [
        pytest.param("is_active", False, "no longer active", id="disabled"),
        pytest.param("role", "member", "role changed", id="demoted"),
    ],
)
@pytest.mark.asyncio
async def test_initial_worker_forward_revalidates_native_user_before_effects(
    session_factory,
    changed_field,
    changed_value,
    error,
):
    from backend.models.user import User

    async with session_factory() as db:
        user = User(
            email=f"worker-principal-{changed_field}@example.com",
            name="Worker principal",
            password_hash="unused",
            role="admin",
            is_active=True,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        user_id = user.id

    worker = await _mk_worker(
        session_factory,
        name=f"principal-{changed_field}-worker",
    )
    task = await _mk_task(
        session_factory,
        status="pending",
        worker_id=worker.id,
        execution_user_id=user_id,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
    )
    async with session_factory() as db:
        user = await db.get(User, user_id)
        setattr(user, changed_field, changed_value)
        await db.commit()

    proxy = WorkerProxy(session_factory, relay=AsyncMock())
    proxy.get_worker = AsyncMock()

    with pytest.raises(RuntimeError, match=error):
        await proxy.forward_task_to_worker(task)

    proxy.get_worker.assert_not_awaited()
    proxy.relay.subscribe_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_worker_forward_takes_exact_user_writer_fence(
    session_factory,
    monkeypatch,
):
    """Principal admission must lock active/role, not trust a plain read."""

    from backend.models.user import User

    async with session_factory() as db:
        user = User(
            email="worker-principal-writer-fence@example.com",
            name="Worker principal writer fence",
            password_hash="unused",
            role="admin",
            is_active=True,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        user_id = user.id

    worker = await _mk_worker(
        session_factory,
        name="principal-writer-fence-worker",
    )
    task = await _mk_task(
        session_factory,
        status="in_progress",
        turn_generation=1,
        worker_id=worker.id,
        execution_user_id=user_id,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
    )

    original_execute = AsyncSession.execute
    user_fences: list[str] = []

    async def execute_spy(session, statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if (
            getattr(statement, "is_update", False)
            and getattr(table, "name", None) == "users"
        ):
            user_fences.append(str(statement))
        return await original_execute(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", execute_spy)
    proxy = WorkerProxy(session_factory, relay=AsyncMock())

    current = await proxy._authoritative_forward_task(task)

    assert current.id == task.id
    assert len(user_fences) == 1
    normalized = " ".join(user_fences[0].lower().split())
    assert "update users" in normalized
    assert "users.is_active is true" in normalized
    assert "users.role =" in normalized


async def test_initial_worker_post_transport_failure_is_outcome_uncertain(
    monkeypatch,
):
    from backend.services.worker_proxy import (
        WorkerTaskForwardOutcomeUncertainError,
    )

    post_calls = 0

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, _url, *, headers, json):
            nonlocal post_calls
            post_calls += 1
            raise RuntimeError("response lost after commit")

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, AsyncMock())
    worker = Worker(
        id=79,
        name="worker",
        status="ready",
        private_ip="10.0.0.79",
        auth_token="token",
    )
    task = Task(
        id=903,
        title="uncertain initial forward",
        description="do work",
        worker_id=worker.id,
        priority=0,
        max_retries=2,
        mode="auto",
        max_iterations=50,
        must_complete=False,
        goal_max_turns=30,
        provider="codex",
        codex_service_tier="default",
        enable_workflows=False,
        incarnation_id="4" * 32,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
        status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=None)
    proxy._user_skill_snapshots = AsyncMock(return_value=[])
    proxy.require_worker_delegated_principal_support = AsyncMock()
    proxy.require_worker_initial_generation_support = AsyncMock()
    proxy.require_worker_task_incarnation_support = AsyncMock()

    with pytest.raises(
        WorkerTaskForwardOutcomeUncertainError,
        match="outcome is uncertain",
    ):
        await proxy._forward_task_to_worker_locked(task)

    assert post_calls == 1


async def test_worker_forward_syncs_related_plan_uploads(monkeypatch):
    captured_payload = {}
    monkeypatch.setattr(settings, "worker_remote_dir", "/srv/ccm-worker")
    image_name = "11111111-1111-4111-8111-111111111111.png"
    notes_name = "22222222-2222-4222-8222-222222222222.txt"
    local_paths = [
        f"/manager/checkout/uploads/{image_name}",
        f"/manager/checkout/uploads/{notes_name}",
    ]
    remote_paths = [
        f"/srv/ccm-worker/uploads/{image_name}",
        f"/srv/ccm-worker/uploads/{notes_name}",
    ]

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return captured_payload

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, _url, *, headers, json):
            captured_payload.update(json)
            captured_payload["incarnation_id"] = json[
                "source_incarnation_id"
            ]
            captured_payload["metadata_"] = {
                "ccm_source_task_incarnation_id": json[
                    "source_incarnation_id"
                ]
            }
            captured_payload["status"] = "pending"
            captured_payload["retry_count"] = json["source_retry_count"]
            captured_payload["turn_generation"] = (
                json["source_turn_generation"] - 1
            )
            return Response()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    relay = AsyncMock()
    proxy = WorkerProxy(None, relay)
    worker = Worker(
        id=78,
        name="worker",
        status="ready",
        private_ip="10.0.0.78",
        auth_token="token",
    )
    task = Task(
        id=902,
        title="Plan for #44",
        description="Use the attached references",
        worker_id=worker.id,
        project_id=12,
        mode="plan",
        provider="claude",
        plan_target_task_id=44,
        metadata_={
            "created_from_plan_target_task_id": 44,
            "file_paths": local_paths,
            "attachments": [
                {
                    "url": f"/api/uploads/{image_name}",
                    "name": "mockup.png",
                    "is_image": True,
                },
                {
                    "url": f"/api/uploads/{notes_name}",
                    "name": "notes.txt",
                    "is_image": False,
                },
            ],
        },
        incarnation_id="3" * 32,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
        status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=34)
    proxy.require_worker_delegated_principal_support = AsyncMock()
    proxy.require_worker_initial_generation_support = AsyncMock()
    proxy.require_worker_task_incarnation_support = AsyncMock()
    proxy.push_files = AsyncMock()
    proxy._user_skill_snapshots = AsyncMock(return_value=[])
    proxy.require_worker_delegated_principal_support = AsyncMock()

    await proxy._forward_task_to_worker_locked(task)

    assert captured_payload["file_paths"] == remote_paths
    assert captured_payload["image_paths"] == [remote_paths[0]]
    assert captured_payload["attachments"] == task.metadata_["attachments"]
    proxy.push_files.assert_awaited_once_with(
        worker,
        local_paths,
        remote_paths=remote_paths,
    )


async def test_worker_skill_selection_syncs_before_follow_up(monkeypatch):
    captured_payload = {}
    captured_headers = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "id": task.id,
                "incarnation_id": task.incarnation_id,
                "status": task.status,
                "retry_count": task.retry_count,
                "turn_generation": task.turn_generation,
                # Worker instance ids belong to a different database and are
                # intentionally not comparable with the Manager mirror.
                "instance_id": None,
                "enabled_skills": captured_payload["enabled_skills"],
                "selected_user_skills": captured_payload[
                    "selected_user_skills"
                ],
                "metadata_": {
                    "ccm_user_skill_snapshots": captured_payload[
                        "user_skill_snapshots"
                    ],
                },
            }

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return _ProxyResponse(
                200,
                {"worker_task_incarnation_proxy_version": 1},
            )

        async def put(self, _url, *, headers, json):
            captured_headers.update(headers)
            captured_payload.update(json)
            return Response()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    proxy._user_skill_snapshots = AsyncMock(return_value=[{
        "id": 6,
        "name": "Updated skill",
        "description": "latest selection",
        "content": "body",
    }])
    worker = Worker(
        id=78,
        name="worker",
        private_ip="10.0.0.78",
        auth_token="token",
    )
    task = Task(
        id=902,
        title="remote",
        description="continue",
        worker_id=worker.id,
        incarnation_id="1" * 32,
        status="completed",
        retry_count=0,
        turn_generation=1,
        enabled_skills={"code-review": True},
        selected_user_skills=[6],
    )

    await proxy.sync_task_skill_selection(worker, task)

    assert captured_payload == {
        "enabled_skills": {"code-review": True},
        "selected_user_skills": [6],
        "user_skill_snapshots": [{
            "id": 6,
            "name": "Updated skill",
            "description": "latest selection",
            "content": "body",
        }],
    }
    assert captured_headers == {
        "Authorization": "Bearer token",
        "X-CCM-Task-Incarnation": task.incarnation_id,
    }


@pytest.mark.parametrize(
    "mismatch",
    ["incarnation", "turn-generation", "skills"],
)
async def test_worker_skill_selection_sync_fails_closed_on_stale_confirmation(
    monkeypatch,
    mismatch,
):
    authoritative_snapshots = [{
        "id": 9,
        "name": "Authoritative",
        "description": "Manager copy",
        "content": "body",
    }]

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "id": task.id,
                "incarnation_id": (
                    "3" * 32
                    if mismatch == "incarnation"
                    else task.incarnation_id
                ),
                "status": task.status,
                "retry_count": task.retry_count,
                "turn_generation": (
                    task.turn_generation + 1
                    if mismatch == "turn-generation"
                    else task.turn_generation
                ),
                "instance_id": task.instance_id,
                "enabled_skills": (
                    {}
                    if mismatch == "skills"
                    else {"code-review": True}
                ),
                "selected_user_skills": [9],
                "metadata_": {
                    "ccm_user_skill_snapshots": authoritative_snapshots,
                },
            }

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return _ProxyResponse(
                200,
                {"worker_task_incarnation_proxy_version": 1},
            )

        async def put(self, _url, *, headers, json):
            return Response()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    proxy._user_skill_snapshots = AsyncMock(
        return_value=authoritative_snapshots,
    )
    worker = Worker(
        id=79,
        name="worker",
        private_ip="10.0.0.79",
        auth_token="token",
    )
    task = Task(
        id=903,
        title="remote",
        description="continue",
        worker_id=worker.id,
        incarnation_id="2" * 32,
        status="completed",
        retry_count=0,
        turn_generation=1,
        instance_id=444,
        enabled_skills={"code-review": True},
        selected_user_skills=[9],
    )

    with pytest.raises(HTTPException) as exc:
        await proxy.sync_task_skill_selection(worker, task)

    assert exc.value.status_code == 409
    assert "execution was blocked" in exc.value.detail


async def test_worker_skill_selection_rejects_old_worker_before_put(monkeypatch):
    put = AsyncMock()

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return _ProxyResponse(200, {})

        async def put(self, *args, **kwargs):
            return await put(*args, **kwargs)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        id=80,
        name="legacy-worker",
        private_ip="10.0.0.80",
        auth_token="token",
    )
    task = Task(
        id=904,
        title="remote",
        worker_id=worker.id,
        incarnation_id="f" * 32,
    )

    with pytest.raises(HTTPException) as exc:
        await proxy.sync_task_skill_selection(worker, task)

    assert exc.value.status_code == 409
    assert "upgraded" in exc.value.detail
    put.assert_not_awaited()


async def test_worker_skill_selection_rejects_legacy_null_incarnation(
    monkeypatch,
):
    class Client:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("legacy Task must fail before network access")

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        id=81,
        name="worker",
        private_ip="10.0.0.81",
        auth_token="token",
    )
    task = Task(
        id=905,
        title="legacy remote",
        worker_id=worker.id,
        incarnation_id=None,
    )

    with pytest.raises(HTTPException) as exc:
        await proxy.sync_task_skill_selection(worker, task)

    assert exc.value.status_code == 409
    assert "stable incarnation" in exc.value.detail


@pytest.mark.parametrize(
    "local_collision",
    [False, True],
    ids=["missing-local-row", "colliding-local-row"],
)
async def test_worker_proxy_uses_authoritative_user_skill_snapshots(
    session_factory,
    local_collision,
):
    from backend.models.user_skill import UserSkill

    if local_collision:
        async with session_factory() as db:
            db.add(UserSkill(
                id=86,
                name="Wrong Worker-local skill",
                description="must not replace Manager snapshot",
                content="wrong local body",
            ))
            await db.commit()

    authoritative = [{
        "id": 86,
        "name": "Manager snapshot",
        "description": "authoritative",
        "content": "correct Manager body",
    }]
    task = Task(
        id=986,
        title="snapshot forwarding",
        selected_user_skills=[86],
        metadata_={"ccm_user_skill_snapshots": authoritative},
    )
    proxy = WorkerProxy(session_factory, relay=AsyncMock())

    assert await proxy._user_skill_snapshots(task) == authoritative


async def test_worker_fast_fails_before_forward_when_capability_is_unproven(
    monkeypatch,
):
    post = AsyncMock()

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            # Shape returned by an older Worker that predates service tiers.
            return {
                "default_codex_model": "gpt-5.6-sol",
                "worker_delegated_principal_protocol": 1,
                "worker_delegated_launch_admission_protocol": 2,
                "worker_initial_generation_protocol": 1,
                "worker_task_incarnation_proxy_version": 1,
                **_worker_namespace_capability(),
            }

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url, *, headers):
            return Response()

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    relay = AsyncMock()
    proxy = WorkerProxy(None, relay)
    worker = Worker(
        id=78,
        name="old-worker",
        status="ready",
        private_ip="10.0.0.78",
        auth_token="token",
    )
    task = Task(
        id=902,
        title="Fast remote task",
        description="run fast",
        worker_id=worker.id,
        project_id=12,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
        incarnation_id="5" * 32,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
        status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=34)

    with pytest.raises(RuntimeError, match="未声明.*支持 Codex Fast"):
        await proxy._forward_task_to_worker_locked(task)

    proxy.ensure_worker_project.assert_not_awaited()
    relay.subscribe_task.assert_not_awaited()
    post.assert_not_awaited()


async def test_worker_pr_review_fails_before_forward_without_snapshot_isolation(
    monkeypatch,
):
    post = AsyncMock()

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            # Older Workers accept the tag but run from their CCM cwd.
            return {
                "default_provider": "claude",
                "worker_delegated_principal_protocol": 1,
                "worker_delegated_launch_admission_protocol": 2,
                "worker_initial_generation_protocol": 1,
                "worker_task_incarnation_proxy_version": 1,
                **_worker_namespace_capability(),
            }

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url, *, headers):
            return Response()

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    relay = AsyncMock()
    proxy = WorkerProxy(None, relay)
    worker = Worker(
        id=80,
        name="old-pr-worker",
        status="ready",
        private_ip="10.0.0.80",
        auth_token="token",
    )
    task = Task(
        id=904,
        title="PR Review: owner/repo#1",
        description="review captured snapshot",
        worker_id=worker.id,
        provider="claude",
        tags=["pr-review"],
        incarnation_id="6" * 32,
        execution_user_id=None,
        execution_user_role="member",
        execution_mode="sandbox",
        execution_principal_kind="system",
        status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=34)

    with pytest.raises(RuntimeError, match="PR 审核快照隔离能力"):
        await proxy._forward_task_to_worker_locked(task)

    proxy.ensure_worker_project.assert_not_awaited()
    relay.subscribe_task.assert_not_awaited()
    post.assert_not_awaited()


async def test_worker_fast_preflight_resolves_default_model_alias(
    monkeypatch,
):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "default_codex_model": "gpt-5.6-sol",
                "codex_model_service_tiers": {
                    "gpt-5.6-sol": ["default", "priority"],
                },
            }

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url, *, headers):
            return Response()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, AsyncMock())
    worker = Worker(
        id=79,
        name="worker",
        status="ready",
        private_ip="10.0.0.79",
        auth_token="token",
    )
    task = Task(
        id=903,
        title="Fast default-model task",
        description="run fast",
        worker_id=worker.id,
        provider="codex",
        model="default",
        codex_service_tier="priority",
    )

    await proxy.require_worker_fast_support(worker, task)


# === WorkerRelay._handle ===


async def test_relay_chat_event_stored_and_forwarded(relay, broadcaster, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}

    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {
            "id": 987654,
            "event_type": "message",
            "role": "assistant",
            "content": "hi",
            "instance_id": 7,
            "task_retry_count": t.retry_count,
            "task_turn_generation": t.turn_generation,
            "native_turn_id": "native-turn-live-1",
            "turn_scope": "foreground",
        },
    }, w)

    async with session_factory() as db:
        logs = (await db.execute(select(LogEntry).where(LogEntry.task_id == t.id))).scalars().all()
        task = await db.get(Task, t.id)
    assert len(logs) == 1
    assert logs[0].instance_id is None
    assert logs[0].content == "hi"
    assert logs[0].task_retry_count == t.retry_count
    assert logs[0].task_turn_generation == t.turn_generation
    assert logs[0].native_turn_id == "native-turn-live-1"
    assert logs[0].turn_scope == "foreground"
    assert logs[0].actual_transport is None
    assert task.has_unread is True
    # 镜像广播到同名 channel，剥掉 worker 的 instance_id，并以 Manager
    # 本地 LogEntry 身份覆盖远端数据库 id。
    assert len(broadcaster.sent) == 1
    channel, event = broadcaster.sent[0]
    assert channel == f"task:{t.id}"
    assert event["event_type"] == "message"
    assert event["role"] == "assistant"
    assert event["content"] == "hi"
    assert "instance_id" not in event
    assert event["id"] == logs[0].id
    assert event["id"] != 987654
    assert event["task_id"] == t.id
    assert event["task_retry_count"] == t.retry_count
    assert event["task_turn_generation"] == t.turn_generation
    assert event["native_turn_id"] == "native-turn-live-1"
    assert event["turn_scope"] == "foreground"
    assert event["actual_transport"] is None
    assert event["timestamp"].endswith("Z")


@pytest.mark.asyncio
async def test_relay_uses_local_codex_provider_to_clear_remote_anomaly_marker(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        provider="codex",
    )
    relay._tasks[worker.id] = {task.id}
    xml_example = (
        '<function_calls><invoke name="Bash">'
        '<parameter name="command">pwd</parameter>'
        '</invoke></function_calls>'
    )

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": xml_example,
                "protocol_anomaly": "legacy_tool_markup",
                **_relay_generation(task),
            },
        },
        worker,
    )

    assert len(broadcaster.sent) == 1
    _channel, event = broadcaster.sent[0]
    assert event["content"] == xml_example
    assert "protocol_anomaly" not in event


async def test_relay_rejects_non_string_native_turn_identity(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "invalid identity",
                "native_turn_id": 17,
                **_relay_generation(task),
            },
        },
        worker,
    )

    async with session_factory() as db:
        logs = list((await db.execute(
            select(LogEntry).where(LogEntry.task_id == task.id)
        )).scalars())
    assert logs == []
    assert broadcaster.sent == []


async def test_relay_rejects_unknown_turn_scope(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "must not become foreground evidence",
                "turn_scope": "background",
                **_relay_generation(task),
            },
        },
        worker,
    )

    async with session_factory() as db:
        logs = list((await db.execute(
            select(LogEntry).where(LogEntry.task_id == task.id)
        )).scalars())
    assert logs == []
    assert broadcaster.sent == []


async def test_relay_rejects_actual_transport_on_output(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "result",
                "role": "assistant",
                "content": "forged evidence",
                "turn_scope": "foreground",
                "actual_transport": "codex_exec",
                **_relay_generation(task),
            },
        },
        worker,
    )

    async with session_factory() as db:
        count = await db.scalar(
            select(func.count(LogEntry.id)).where(LogEntry.task_id == task.id)
        )
    assert count == 0
    assert broadcaster.sent == []


@pytest.mark.parametrize(
    ("event_type", "role", "event_retry_count"),
    [
        ("result", "assistant", 4),
        ("message", "assistant", None),
        ("result", "assistant", True),
    ],
)
async def test_relay_drops_pr_terminal_event_without_exact_retry_generation(
    relay,
    broadcaster,
    session_factory,
    event_type,
    role,
    event_retry_count,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=5,
        tags=["pr-review"],
        has_unread=False,
    )
    relay._tasks[worker.id] = {task.id}
    data = {
        "event_type": event_type,
        "role": role,
        "content": (
            "PR_REVIEW_BODY_BEGIN\nLGTM\nPR_REVIEW_BODY_END\n"
            "PR_REVIEW_RESULT: approved_merged"
        ),
    }
    if event_retry_count is not None:
        data["task_retry_count"] = event_retry_count

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": data,
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
    assert current.has_unread is False
    assert logs == []
    assert broadcaster.sent == []


async def test_relay_drops_chat_event_from_stale_turn_with_same_retry(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
        has_unread=False,
    )
    relay._tasks[worker.id] = {task.id}

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "result",
                "role": "assistant",
                "content": "stale logical turn",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation - 1,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
    assert current.has_unread is False
    assert logs == []
    assert broadcaster.sent == []


async def test_relay_rejects_unreserved_next_turn_generation(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
        has_unread=False,
    )
    relay._tasks[worker.id] = {task.id}

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "result",
                "role": "assistant",
                "content": "unreserved next turn",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation + 1,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = list((await db.execute(
            select(LogEntry).where(LogEntry.task_id == task.id)
        )).scalars())
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_id is None
    assert current.has_unread is False
    assert logs == []
    assert broadcaster.sent == []


async def test_worker_quarantine_blocks_reserved_next_turn_adoption_and_replay(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    async with session_factory() as db:
        quarantined = (
            await worker_relay_module.quarantine_uncertain_worker_termination(
                db,
                reserved,
                operation="cancel",
                error="cancel outcome uncertain",
            )
        )
    assert quarantined is not None

    adopted = await relay._observe_or_adopt_event_generation(
        worker.id,
        task.id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation + 1,
        worker_turn_handoff_id=reserved.worker_turn_handoff_id,
    )
    assert adopted is None

    relay.ensure_worker_turn_handoff_recovery(worker, quarantined)
    assert relay._handoff_recovery_tasks == {}
    relay._manager_worker_turn_handoff_request = AsyncMock()
    resumed = await relay._resume_accepted_worker_turn_handoff(
        worker,
        quarantined,
        operation_lock_held=True,
    )
    assert resumed is False
    relay._manager_worker_turn_handoff_request.assert_not_awaited()

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "conflict"
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id


async def test_active_termination_receipt_stops_handoff_recovery_before_io(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    await _create_manager_termination_receipt(session_factory, task.id)
    relay._manager_worker_turn_handoff_request = AsyncMock()
    relay._fetch_worker_turn_handoff_receipt = AsyncMock()
    relay._post_worker_turn_handoff_request = AsyncMock()
    relay._acknowledge_recovered_worker_turn_handoff = AsyncMock()
    relay._cancel_recovered_worker_turn_handoff = AsyncMock()
    relay._resume_worker_turn_handoff = AsyncMock()

    recovered = await relay._resume_accepted_worker_turn_handoff(
        worker,
        reserved,
        attempts=1,
        operation_lock_held=True,
    )

    assert recovered is False
    relay._manager_worker_turn_handoff_request.assert_not_awaited()
    relay._fetch_worker_turn_handoff_receipt.assert_not_awaited()
    relay._post_worker_turn_handoff_request.assert_not_awaited()
    relay._acknowledge_recovered_worker_turn_handoff.assert_not_awaited()
    relay._cancel_recovered_worker_turn_handoff.assert_not_awaited()
    relay._resume_worker_turn_handoff.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        handoff = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    assert current.worker_turn_handoff_acknowledged is False
    assert handoff.status == "prepared"


async def test_active_termination_receipt_fences_direct_handoff_effects(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    replay = await relay._manager_worker_turn_handoff_request(reserved)
    assert replay is not None
    await _create_manager_termination_receipt(session_factory, task.id)
    post_client = SimpleNamespace(post=AsyncMock())
    client_factory = Mock()
    monkeypatch.setattr(
        worker_relay_module.httpx,
        "AsyncClient",
        client_factory,
    )

    posted = await relay._post_worker_turn_handoff_request(
        worker,
        reserved,
        replay,
        client=post_client,
    )
    resumed = await relay._resume_worker_turn_handoff(
        worker,
        reserved,
        replay,
    )

    assert posted is False
    assert resumed is False
    post_client.post.assert_not_awaited()
    client_factory.assert_not_called()


async def test_active_termination_receipt_blocks_reserved_generation_adoption(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=6,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    await _create_manager_termination_receipt(session_factory, task.id)

    adopted = await relay._observe_or_adopt_event_generation(
        worker.id,
        task.id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation + 1,
        worker_turn_handoff_id=reserved.worker_turn_handoff_id,
    )

    assert adopted is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "completed"
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id


async def test_active_termination_receipt_blocks_system_init_log_and_session(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="executing",
        retry_count=1,
        turn_generation=4,
        session_id=None,
    )
    relay._tasks[worker.id] = {task.id}
    await _create_manager_termination_receipt(session_factory, task.id)
    relay._fetch_task_snapshot = AsyncMock()

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "system_init",
                "role": "system",
                "content": "remote session initialized",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation,
                "native_turn_id": "native-active-receipt",
                "turn_scope": "foreground",
            },
        },
        worker,
    )

    relay._fetch_task_snapshot.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = list(
            (
                await db.execute(
                    select(LogEntry).where(LogEntry.task_id == task.id)
                )
            ).scalars()
        )
    assert current.session_id is None
    assert logs == []
    assert broadcaster.sent == []


async def test_reserved_next_turn_rejects_accepted_but_unlaunched_receipt(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
        has_unread=False,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    relay._tasks[worker.id] = {task.id}
    receipt = _launched_handoff_receipt(reserved)
    receipt["status"] = "accepted"
    receipt["turn_generation"] = None
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=receipt
    )

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "an unrelated remote plus one",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation + 1,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assistant = await db.scalar(
            select(LogEntry.id).where(LogEntry.role == "assistant")
        )
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    assert assistant is None
    assert broadcaster.sent == []


async def test_launching_handoff_live_terminal_event_adopts_exact_next_turn(
    relay,
    broadcaster,
    session_factory,
):
    """A provider can emit G+1 before launch() returns and marks launched."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=10,
        has_unread=False,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    relay._tasks[worker.id] = {task.id}
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_handoff_receipt_with_status(reserved, "launching")
    )

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "result",
                "role": "assistant",
                "content": "terminal output raced launch settlement",
                "native_turn_id": "native-launching-turn-11",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation + 1,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager_receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
        terminal_log = await db.scalar(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.content == "terminal output raced launch settlement",
            )
        )
    assert current.turn_generation == task.turn_generation + 1
    assert current.worker_turn_handoff_id is None
    assert manager_receipt.status == "completed"
    assert terminal_log is not None
    assert terminal_log.task_turn_generation == task.turn_generation + 1
    assert terminal_log.native_turn_id == "native-launching-turn-11"
    assert any(
        payload.get("content") == "terminal output raced launch settlement"
        for _channel, payload in broadcaster.sent
    )


async def test_claimed_handoff_recovery_resumes_exact_generation(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_handoff_receipt_with_status(reserved, "claimed")
    )
    relay._resume_worker_turn_handoff = AsyncMock(return_value=True)
    replay = await relay._manager_worker_turn_handoff_request(reserved)
    assert replay is not None

    recovered = await relay._resume_accepted_worker_turn_handoff(
        worker,
        reserved,
        attempts=1,
    )

    assert recovered is True
    relay._resume_worker_turn_handoff.assert_awaited_once_with(
        worker,
        reserved,
        replay,
        client=None,
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager_receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
    assert current.turn_generation == task.turn_generation
    assert current.worker_turn_handoff_acknowledged is True
    assert manager_receipt.status == "acknowledged"


@pytest.mark.parametrize(
    ("field", "malformed"),
    [
        ("task_id", True),
        ("retry_count", True),
        ("from_generation", False),
        ("turn_generation", True),
    ],
)
async def test_remote_handoff_match_rejects_boolean_integer_fields(
    relay,
    session_factory,
    field,
    malformed,
):
    # Choose integer values that compare equal to their bool counterpart.  The
    # protocol must still reject them instead of relying on Python's True == 1
    # and False == 0 behavior.
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=1,
        turn_generation=0,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    replay = await relay._manager_worker_turn_handoff_request(reserved)
    assert replay is not None
    observed = replace(reserved, task_id=1)
    receipt = _handoff_receipt_with_status(observed, "launching")
    receipt["request_digest"] = replay["request_digest"]
    receipt["principal_digest"] = replay["principal_digest"]
    receipt[field] = malformed

    assert not relay._remote_handoff_matches(observed, receipt, replay)


@pytest.mark.parametrize(
    ("mismatched_field", "expected"),
    [
        (None, True),
        ("task_id", False),
        ("retry_count", False),
        ("from_generation", False),
        ("turn_generation", False),
    ],
)
async def test_resume_worker_handoff_validates_complete_remote_identity(
    relay,
    session_factory,
    monkeypatch,
    mismatched_field,
    expected,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    replay = await relay._manager_worker_turn_handoff_request(reserved)
    assert replay is not None
    receipt = _handoff_receipt_with_status(reserved, "claimed")
    receipt["request_digest"] = replay["request_digest"]
    receipt["principal_digest"] = replay["principal_digest"]
    if mismatched_field == "task_id":
        receipt[mismatched_field] = task.id + 1
    elif mismatched_field == "retry_count":
        receipt[mismatched_field] = task.retry_count + 1
    elif mismatched_field == "from_generation":
        receipt[mismatched_field] = task.turn_generation + 1
    elif mismatched_field == "turn_generation":
        receipt[mismatched_field] = task.turn_generation + 2

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return receipt

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **_kwargs):
            assert url.endswith("/api/system/config")
            return SimpleNamespace(
                status_code=200,
                json=lambda: {
                    "worker_delegated_principal_protocol": 1,
                    "worker_delegated_launch_admission_protocol": 2,
                    "worker_task_incarnation_proxy_version": 1,
                    **_worker_namespace_capability(),
                },
            )

        async def post(self, url, **kwargs):
            assert url.endswith(
                f"/api/tasks/{task.id}/worker-turn-handoffs/"
                f"{reserved.worker_turn_handoff_id}/resume"
            )
            assert kwargs["headers"]["X-CCM-Task-Incarnation"] == (
                task.incarnation_id
            )
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)

    resumed = await relay._resume_worker_turn_handoff(
        worker,
        reserved,
        replay,
    )

    assert resumed is expected


async def test_handoff_adoption_crash_state_keeps_marker_and_active_recovery(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=6,
    )
    async with session_factory() as db:
        stale_source = LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            turn_scope="source",
            event_type="user_message",
            role="user",
            content="source for G",
        )
        db.add(stale_source)
        await db.flush()
        current = await db.get(Task, task.id)
        current.turn_source_log_id = stale_source.id
        await db.commit()
    reserved = await _reserve_worker_handoff(session_factory, task)

    adopted = await relay._observe_or_adopt_event_generation(
        worker.id,
        task.id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation + 1,
        worker_turn_handoff_id=reserved.worker_turn_handoff_id,
    )

    assert adopted is not None
    assert adopted.status == "executing"
    assert adopted.turn_generation == task.turn_generation + 1
    assert adopted.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assistant = await db.scalar(
            select(LogEntry.id).where(LogEntry.role == "assistant")
        )
    assert current.status == "executing"
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    assert current.turn_source_log_id is None
    assert assistant is None


async def test_handoff_adoption_conditional_write_cannot_revive_cancelled_task(
    relay,
    session_factory,
):
    """SQLite SELECT FOR UPDATE must not let a stale ORM row undo cancel."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=6,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    injected = False

    class RacingSession:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        async def execute(self, statement, *args, **kwargs):
            nonlocal injected
            result = await self._inner.execute(statement, *args, **kwargs)
            if not injected:
                injected = True
                # Simulate a writer that commits after the adoption SELECT.
                # synchronize_session=False deliberately leaves the selected
                # ORM object stale, matching SQLite's lack of row locking.
                await self._inner.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(
                        status="cancelled",
                        completed_at=datetime.utcnow(),
                    )
                    .execution_options(synchronize_session=False)
                )
                await self._inner.commit()
            return result

    @asynccontextmanager
    async def racing_factory():
        async with session_factory() as db:
            yield RacingSession(db)

    relay.db_factory = racing_factory
    adopted = await relay._observe_or_adopt_event_generation(
        worker.id,
        task.id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation + 1,
        worker_turn_handoff_id=reserved.worker_turn_handoff_id,
    )

    assert injected
    assert adopted is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "cancelled"
    assert current.turn_generation == task.turn_generation


async def test_worker_startup_recover_arms_background_loop_for_accepted_handoff(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=1,
        turn_generation=4,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    accepted = _launched_handoff_receipt(reserved)
    accepted["status"] = "accepted"
    accepted["turn_generation"] = None
    relay.subscribe_task = AsyncMock()
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=accepted
    )
    relay._resume_worker_turn_handoff = AsyncMock(return_value=False)
    relay._fetch_task_snapshot = AsyncMock(return_value=_remote_task(task))
    relay.ensure_worker_turn_handoff_recovery = Mock()

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)

    await relay.recover(worker)

    relay.subscribe_task.assert_awaited_once_with(worker, task.id)
    relay.ensure_worker_turn_handoff_recovery.assert_called_once()
    recovery_worker, recovery_generation = (
        relay.ensure_worker_turn_handoff_recovery.call_args.args
    )
    assert recovery_worker.id == worker.id
    assert recovery_generation.task_id == task.id
    assert recovery_generation.worker_turn_handoff_id == (
        reserved.worker_turn_handoff_id
    )
    assert recovery_generation.worker_turn_handoff_acknowledged is True
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager_receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    assert current.worker_turn_handoff_acknowledged is True
    assert manager_receipt.status == "acknowledged"


async def test_worker_handoff_recovery_replays_missing_exact_post(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    accepted = _launched_handoff_receipt(reserved)
    accepted["status"] = "accepted"
    accepted["turn_generation"] = None
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        side_effect=[None, accepted]
    )
    relay._post_worker_turn_handoff_request = AsyncMock(return_value=True)
    relay._resume_worker_turn_handoff = AsyncMock(return_value=True)

    recovered = await relay._resume_accepted_worker_turn_handoff(
        worker,
        reserved,
        attempts=1,
    )

    assert recovered is True
    replay = relay._post_worker_turn_handoff_request.await_args.args[2]
    assert replay["payload"]["worker_turn_handoff_id"] == (
        reserved.worker_turn_handoff_id
    )
    assert replay["payload"]["message"] == "reserved follow-up"
    relay._resume_worker_turn_handoff.assert_awaited_once_with(
        worker,
        reserved,
        replay,
        client=None,
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager_receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
    assert current.worker_turn_handoff_acknowledged is True
    assert manager_receipt.status == "acknowledged"


async def test_worker_handoff_recovery_consumes_remote_cancellation(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=9,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    cancelled = _launched_handoff_receipt(reserved)
    cancelled.update({
        "status": "cancelled",
        "turn_generation": None,
        "cancel_reason": "explicit stop",
    })
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=cancelled
    )

    recovered = await relay._resume_accepted_worker_turn_handoff(
        worker,
        reserved,
        attempts=1,
    )

    assert recovered is True
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager_receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
    assert current.worker_turn_handoff_id is None
    assert current.worker_turn_handoff_worker_id is None
    assert manager_receipt.status == "cancelled"
    assert manager_receipt.cancel_reason == "explicit stop"


async def test_worker_handoff_recovery_timer_retries_without_reconnect(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=1,
        turn_generation=5,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    monkeypatch.setattr(
        worker_relay_module,
        "WORKER_HANDOFF_RECOVERY_BASE_DELAY",
        0.001,
    )
    monkeypatch.setattr(
        worker_relay_module,
        "WORKER_HANDOFF_RECOVERY_MAX_DELAY",
        0.002,
    )
    relay._resume_accepted_worker_turn_handoff = AsyncMock(
        side_effect=[False, True]
    )

    async def settle(_worker, task_ids, **_kwargs):
        async with session_factory() as db:
            current = await db.get(Task, task.id)
            for field in (
                "worker_turn_handoff_id",
                "worker_turn_handoff_worker_id",
                "worker_turn_handoff_retry_count",
                "worker_turn_handoff_from_generation",
                "worker_turn_handoff_source_log_id",
                "worker_turn_handoff_acknowledged",
            ):
                setattr(current, field, None)
            receipt = await db.get(
                WorkerTurnHandoffReceipt,
                reserved.worker_turn_handoff_id,
            )
            receipt.status = "completed"
            await db.commit()
        return set(task_ids)

    relay._backfill_missing_logs_with_operation_lock = AsyncMock(
        side_effect=settle
    )
    relay.ensure_worker_turn_handoff_recovery(worker, reserved)
    recovery_task = next(iter(relay._handoff_recovery_tasks.values()))
    await asyncio.wait_for(recovery_task, timeout=1)

    assert relay._resume_accepted_worker_turn_handoff.await_count == 2
    relay._backfill_missing_logs_with_operation_lock.assert_awaited_once()
    backfill = relay._backfill_missing_logs_with_operation_lock.await_args
    assert backfill.args[0].id == worker.id
    assert backfill.args[1] == {task.id}


@pytest.mark.parametrize("event_retry_offset", [0, 1])
async def test_reserved_next_turn_rejects_retry_aba(
    relay,
    broadcaster,
    session_factory,
    event_retry_offset,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=4,
        turn_generation=12,
        has_unread=False,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    relay._tasks[worker.id] = {task.id}
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_launched_handoff_receipt(reserved)
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(retry_count=Task.retry_count + 1)
        )
        await db.commit()

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "wrong retry cannot consume handoff",
                "task_retry_count": reserved.retry_count + event_retry_offset,
                "task_turn_generation": reserved.turn_generation + 1,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type != "user_message",
            )
        )).scalars())
    assert current.retry_count == reserved.retry_count + 1
    assert current.turn_generation == reserved.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    assert logs == []
    assert broadcaster.sent == []


async def test_reserved_next_turn_rejects_worker_migration_aba(
    relay,
    broadcaster,
    session_factory,
):
    source = await _mk_worker(session_factory)
    destination = await _mk_worker(
        session_factory,
        name="w2",
        private_ip="10.0.0.10",
    )
    task = await _mk_task(
        session_factory,
        worker_id=source.id,
        status="completed",
        retry_count=2,
        turn_generation=5,
        has_unread=False,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    relay._tasks[source.id] = {task.id}
    relay._tasks[destination.id] = {task.id}
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(worker_id=destination.id)
        )
        await db.commit()
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_launched_handoff_receipt(reserved)
    )

    event = {
        "channel": f"task:{task.id}",
        "data": {
            "event_type": "message_delta",
            "content": "wrong assignment",
            "task_retry_count": reserved.retry_count,
            "task_turn_generation": reserved.turn_generation + 1,
        },
    }
    await relay._handle(event, source)
    await relay._handle(event, destination)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id == destination.id
    assert current.turn_generation == reserved.turn_generation
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    assert broadcaster.sent == []


async def test_pending_worker_turn_handoff_blocks_migration_before_side_effects(
    session_factory,
):
    from backend.services.task_migrator import MigrationError, TaskMigrator

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=1,
        turn_generation=4,
    )
    await _reserve_worker_handoff(session_factory, task)
    migrator = TaskMigrator(session_factory, relay=AsyncMock())
    migrator._get_worker = AsyncMock()
    migrator._sync_workspace = AsyncMock()
    migrator._move_session = AsyncMock()

    with pytest.raises(MigrationError, match="handoff"):
        await migrator.migrate(task.id, None)

    migrator._get_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()


async def test_reserved_terminal_generation_drops_old_turn_then_adopts_next(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=6,
        has_unread=False,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    relay._tasks[worker.id] = {task.id}
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_launched_handoff_receipt(reserved)
    )

    for turn, content in (
        (reserved.turn_generation, "late old result"),
        (reserved.turn_generation + 1, "reserved next result"),
    ):
        await relay._handle(
            {
                "channel": f"task:{task.id}",
                "data": {
                    "event_type": "result",
                    "role": "assistant",
                    "content": content,
                    "task_retry_count": reserved.retry_count,
                    "task_turn_generation": turn,
                },
            },
            worker,
        )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assistant_logs = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.role == "assistant",
            )
        )).scalars())
    assert current.turn_generation == reserved.turn_generation + 1
    assert current.worker_turn_handoff_id is None
    assert [log.content for log in assistant_logs] == ["reserved next result"]
    assert [event[1]["content"] for event in broadcaster.sent] == [
        "reserved next result"
    ]


async def test_relay_skips_manager_canonical_events_and_unsubscribed(
    relay, broadcaster, session_factory
):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}

    await relay._handle({"channel": f"task:{t.id}",
                         "data": {"event_type": "user_message", "content": "x"}}, w)
    await relay._handle(
        {
            "channel": f"task:{t.id}",
            "data": {
                "event": "plan_version_applied",
                "plan_id": 901,
                "version_id": 902,
            },
        },
        w,
    )
    await relay._handle({"channel": "task:99999",
                         "data": {"event_type": "message", "content": "x"}}, w)

    async with session_factory() as db:
        count = len((await db.execute(select(LogEntry))).scalars().all())
    assert count == 0
    assert broadcaster.sent == []


async def test_relay_status_change_syncs_task(relay, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            t,
            status="completed",
            completed_at=None,
        )
    )

    await relay._handle({
        "channel": "tasks",
        "data": {"event": "status_change", "task_id": t.id,
                 "old_status": "in_progress", "new_status": "completed"},
    }, w)
    async with session_factory() as db:
        current = await db.get(Task, t.id)
        assert current.status == "completed"
        assert current.completed_at is not None
        assert current.error_message is None
    assert relay.broadcaster.sent == [
        (
            "tasks",
            {
                "event": "status_change",
                "task_id": t.id,
                "old_status": "in_progress",
                "new_status": "completed",
                "background_active": False,
            },
        )
    ]


async def test_relay_completed_generation_triggers_manager_pr_finalizer(
    relay,
    session_factory,
):
    import backend.main

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        tags=["pr-review"],
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="completed",
            completed_at=None,
            background_active=False,
        )
    )
    relay._backfill_missing_logs = AsyncMock(return_value={task.id})
    completion = AsyncMock()

    with patch.object(
        backend.main,
        "dispatcher",
        SimpleNamespace(
            _handle_pty_background_completion=completion,
        ),
    ):
        await relay._handle(
            {
                "channel": "tasks",
                "data": {
                    "event": "status_change",
                    "task_id": task.id,
                    "new_status": "completed",
                },
            },
            worker,
        )

    completion.assert_awaited_once_with(task.id)
    relay._backfill_missing_logs.assert_awaited_once()
    assert relay._backfill_missing_logs.await_args.args[1] == {task.id}
    assert relay._backfill_missing_logs.await_args.kwargs == {
        "sync_status": False,
    }


async def test_relay_completed_pr_fix_backfills_before_manager_finalizer(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        tags=["pr-review-fix"],
        metadata_={"pr_finding_action_id": 41},
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="completed",
            completed_at=None,
            background_active=False,
        )
    )
    calls = []

    async def backfill(*_args, **_kwargs):
        calls.append("backfill")
        return {task.id}

    async def complete(task_id):
        calls.append(("complete", task_id))

    relay._backfill_missing_logs = AsyncMock(side_effect=backfill)
    completion = AsyncMock(side_effect=complete)
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pty_background_completion=completion,
        ),
    )

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "completed",
            },
        },
        worker,
    )

    assert calls == ["backfill", ("complete", task.id)]
    assert relay._backfill_missing_logs.await_args.args[1] == {task.id}
    assert relay._backfill_missing_logs.await_args.kwargs == {
        "sync_status": False,
    }


async def test_relay_releases_operation_fence_before_pr_finalizer_reacquires_it(
    relay,
    session_factory,
    monkeypatch,
):
    from backend.services.worker_proxy import get_task_operation_lock

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        tags=["pr-review"],
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(return_value=_remote_task(
        task,
        status="completed",
        completed_at=None,
        background_active=False,
    ))
    relay._backfill_missing_logs = AsyncMock(return_value={task.id})
    finalizer_acquired = asyncio.Event()

    async def completion(task_id):
        async with get_task_operation_lock(task_id):
            finalizer_acquired.set()

    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pty_background_completion=completion,
        ),
    )

    await asyncio.wait_for(
        relay._handle(
            {
                "channel": "tasks",
                "data": {
                    "event": "status_change",
                    "task_id": task.id,
                    "new_status": "completed",
                },
            },
            worker,
        ),
        timeout=2,
    )

    assert finalizer_acquired.is_set()


@pytest.mark.parametrize("terminal_status", ["failed", "cancelled", "conflict"])
async def test_relay_unsuccessful_pr_fix_invokes_manager_failure_handler(
    relay,
    session_factory,
    monkeypatch,
    terminal_status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        # The metadata marker remains authoritative if an old Manager/client
        # has removed the presentation tag.
        tags=[],
        metadata_={"pr_finding_action_id": 42},
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status=terminal_status,
            completed_at=None,
            error_message="remote fix failure",
            background_active=False,
        )
    )
    relay._backfill_missing_logs = AsyncMock()
    failure = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pr_review_failure=failure,
        ),
    )

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": terminal_status,
            },
        },
        worker,
    )

    failure.assert_awaited_once()
    failed_task, error = failure.await_args.args
    assert failed_task.id == task.id
    assert failed_task.status == terminal_status
    assert failed_task.retry_count == task.retry_count
    assert error == "remote fix failure"
    relay._backfill_missing_logs.assert_not_awaited()


async def test_relay_failed_plain_pr_review_keeps_existing_failure_semantics(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        tags=["pr-review"],
        metadata_={"pr_review_id": 43},
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="failed",
            completed_at=None,
            error_message="review failed",
            background_active=False,
        )
    )
    failure = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pr_review_failure=failure,
        ),
    )

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "failed",
            },
        },
        worker,
    )

    failure.assert_not_awaited()


async def test_worker_pr_fix_failure_rechecks_exact_generation(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
        tags=["pr-review-fix"],
        metadata_={"pr_finding_action_id": 44},
        error_message="old failure",
    )
    async with session_factory() as db:
        generation = await worker_relay_module.read_worker_task_generation(
            db,
            task.id,
            worker.id,
        )
    relay._observe_task_generation = AsyncMock(return_value=None)
    failure = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pr_review_failure=failure,
        ),
    )

    await relay._notify_completed_pr_review(generation)

    failure.assert_not_awaited()


async def test_worker_pr_finalizer_defers_when_history_sync_fails(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        tags=["pr-review"],
    )
    async with session_factory() as db:
        generation = await worker_relay_module.read_worker_task_generation(
            db,
            task.id,
            worker.id,
        )
    relay._backfill_missing_logs = AsyncMock(return_value=set())
    completion = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pty_background_completion=completion,
        ),
    )

    await relay._notify_completed_pr_review(generation)

    relay._backfill_missing_logs.assert_awaited_once()
    completion.assert_not_awaited()


async def test_worker_pr_finalizer_rechecks_generation_after_history_sync(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        tags=["pr-review"],
    )
    async with session_factory() as db:
        generation = await worker_relay_module.read_worker_task_generation(
            db,
            task.id,
            worker.id,
        )

    async def retry_during_sync(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    status="executing",
                    retry_count=Task.retry_count + 1,
                    completed_at=None,
                )
            )
            await db.commit()
        return {task.id}

    relay._backfill_missing_logs = AsyncMock(
        side_effect=retry_during_sync
    )
    completion = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pty_background_completion=completion,
        ),
    )

    await relay._notify_completed_pr_review(generation)

    completion.assert_not_awaited()


async def test_relay_status_uses_authoritative_background_not_event_payload(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="completed",
            completed_at=None,
            background_active=True,
        )
    )

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "completed",
                "background_active": False,
                "pty_background_generation": "remote-secret",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert (
        current.pty_background_generation
        == worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL
    )
    assert broadcaster.sent == [
        (
            "tasks",
            {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "completed",
                "background_active": True,
            },
        )
    ]


async def test_relay_background_event_syncs_controlled_marker(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        tags=["pr-review"],
    )
    relay._tasks[worker.id] = {task.id}
    completion = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pty_background_completion=completion,
        ),
    )
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(task, background_active=True)
    )
    relay._backfill_missing_logs = AsyncMock(return_value={task.id})

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event": "background_activity",
                "event_type": "background_activity",
                "task_id": task.id,
                "background_active": True,
                "pty_background_generation": "remote-secret",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert (
        current.pty_background_generation
        == worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL
    )
    assert broadcaster.sent == [
        (
            f"task:{task.id}",
            {
                "event": "background_activity",
                "event_type": "background_activity",
                "task_id": task.id,
                "background_active": True,
            },
        )
    ]
    completion.assert_not_awaited()

    relay._fetch_task_snapshot.return_value = _remote_task(
        task,
        background_active=False,
    )
    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "background_activity",
                "task_id": task.id,
                "background_active": False,
            },
        },
        worker,
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.pty_background_generation is None
    assert broadcaster.sent[-1] == (
        "tasks",
        {
            "event": "background_activity",
            "event_type": "background_activity",
            "task_id": task.id,
            "background_active": False,
        },
    )
    completion.assert_awaited_once_with(task.id)
    relay._backfill_missing_logs.assert_awaited_once()


@pytest.mark.parametrize(
    ("event_value", "snapshot_value"),
    [
        ("true", True),
        (1, True),
        (True, False),
        (True, None),
        (True, "true"),
    ],
)
async def test_relay_background_event_rejects_unconfirmed_boolean(
    relay,
    broadcaster,
    session_factory,
    event_value,
    snapshot_value,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            background_active=snapshot_value,
        )
    )

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "background_activity",
                "task_id": task.id,
                "background_active": event_value,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.pty_background_generation is None
    assert broadcaster.sent == []
    if type(event_value) is bool:
        relay._fetch_task_snapshot.assert_awaited_once()
    else:
        relay._fetch_task_snapshot.assert_not_awaited()


async def test_delayed_background_event_cannot_publish_after_marker_changes(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    relay._tasks[worker.id] = {task.id}
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def delayed_snapshot(*_args, **_kwargs):
        fetch_started.set()
        await release_fetch.wait()
        return _remote_task(task, background_active=True)

    relay._fetch_task_snapshot = AsyncMock(side_effect=delayed_snapshot)
    handling = asyncio.create_task(
        relay._handle(
            {
                "channel": "tasks",
                "data": {
                    "event": "background_activity",
                    "task_id": task.id,
                    "background_active": True,
                },
            },
            worker,
        )
    )
    await fetch_started.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                pty_background_generation=(
                    worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL
                )
            )
        )
        await db.commit()
    release_fetch.set()
    await handling

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert (
        current.pty_background_generation
        == worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL
    )
    assert broadcaster.sent == []


async def test_background_publication_fence_drops_superseded_result(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(task, background_active=True)
    )
    real_publish = relay._publish_background_generation

    async def clear_before_publication(
        generation,
        *,
        channels,
        notify_completion=True,
    ):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(pty_background_generation=None)
            )
            await db.commit()
        return await real_publish(
            generation,
            channels=channels,
            notify_completion=notify_completion,
        )

    relay._publish_background_generation = AsyncMock(
        side_effect=clear_before_publication
    )

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "background_activity",
                "task_id": task.id,
                "background_active": True,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.pty_background_generation is None
    assert broadcaster.sent == []


async def test_relay_conflict_is_terminal_with_timestamp_and_error(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="conflict",
            completed_at=None,
            error_message=None,
        )
    )

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "conflict",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "conflict"
    assert current.completed_at is not None
    assert "conflict" in current.error_message


async def test_relay_plan_ready_fetches_content(relay, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id, mode="plan")
    relay._tasks[w.id] = {t.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            t,
            status="plan_review",
            plan_content="THE PLAN",
        )
    )

    await relay._handle({
        "channel": "tasks",
        "data": {"event": "plan_ready", "task_id": t.id},
    }, w)
    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.plan_content == "THE PLAN"
    assert task.status == "plan_review"


async def test_relay_monitor_events_with_remote_id(
    relay,
    broadcaster,
    session_factory,
):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}

    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {"event": "monitor_session_created", "monitor_session_id": 5,
                 "description": "watch", **_relay_generation(t)},
    }, w)
    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {"event": "monitor_check", "monitor_session_id": 5,
                 "check_number": 1, "status": "ok", "summary": "fine",
                 **_relay_generation(t)},
    }, w)
    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {"event": "monitor_session_status", "monitor_session_id": 5,
                 "status": "completed", **_relay_generation(t)},
    }, w)

    async with session_factory() as db:
        ms = (await db.execute(select(MonitorSession))).scalars().one()
        checks = (await db.execute(select(MonitorCheck))).scalars().all()
    assert ms.remote_id == 5
    assert ms.task_id == t.id
    assert ms.status == "completed"
    assert ms.last_summary == "fine"
    assert json.loads(ms.meta) == {
        "ccm_worker_mirror": {
            "worker_id": w.id,
            "task_incarnation_id": t.incarnation_id,
            "remote_id": 5,
        }
    }
    assert len(checks) == 1
    assert checks[0].monitor_session_id == ms.id  # 本地 id，不是 remote 的 5
    assert [payload["monitor_session_id"] for _, payload in broadcaster.sent] == [
        ms.id,
        ms.id,
        ms.id,
    ]


async def test_relay_monitor_remote_id_collision_selects_exact_worker_mirror(
    relay,
    broadcaster,
    session_factory,
):
    old_worker = await _mk_worker(session_factory, name="old-monitor-worker")
    worker = await _mk_worker(session_factory, name="current-monitor-worker")
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    remote_id = 51
    async with session_factory() as db:
        historical = MonitorSession(
            task_id=task.id,
            remote_id=remote_id,
            agent_type="monitor",
            source="ccm",
            description="historical monitor",
            status="completed",
            meta=json.dumps({
                "ccm_worker_mirror": {
                    "worker_id": old_worker.id,
                    "task_incarnation_id": task.incarnation_id,
                    "remote_id": remote_id,
                }
            }),
        )
        db.add(historical)
        await db.commit()
        historical_id = historical.id

    events = [
        {
            "event": "monitor_session_created",
            "monitor_session_id": remote_id,
            "description": "current monitor",
            **_relay_generation(task),
        },
        {
            "event": "monitor_check",
            "monitor_session_id": remote_id,
            "check_number": 1,
            "status": "ok",
            "summary": "current result",
            **_relay_generation(task),
        },
        {
            "event": "monitor_session_status",
            "monitor_session_id": remote_id,
            "status": "completed",
            **_relay_generation(task),
        },
    ]
    for data in events:
        await relay._handle(
            {"channel": f"task:{task.id}", "data": data},
            worker,
        )

    async with session_factory() as db:
        mirrors = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.remote_id == remote_id,
                    )
                )
            ).scalars()
        )
        checks = list((await db.execute(select(MonitorCheck))).scalars())
    assert len(mirrors) == 2
    historical = next(row for row in mirrors if row.id == historical_id)
    current = next(row for row in mirrors if row.id != historical_id)
    assert historical.status == "completed"
    assert historical.description == "historical monitor"
    assert historical.last_summary is None
    assert current.status == "completed"
    assert current.description == "current monitor"
    assert current.last_summary == "current result"
    assert json.loads(current.meta) == {
        "ccm_worker_mirror": {
            "worker_id": worker.id,
            "task_incarnation_id": task.incarnation_id,
            "remote_id": remote_id,
        }
    }
    assert [check.monitor_session_id for check in checks] == [current.id]
    assert [payload["monitor_session_id"] for _, payload in broadcaster.sent] == [
        current.id,
        current.id,
        current.id,
    ]


async def test_relay_rejects_stale_worker_child_events_after_reassignment(
    relay,
    broadcaster,
    session_factory,
):
    stale_worker = await _mk_worker(session_factory, name="stale-child-worker")
    current_worker = await _mk_worker(
        session_factory,
        name="current-child-worker",
    )
    task = await _mk_task(session_factory, worker_id=current_worker.id)
    # A disconnected/reconnecting Worker may retain an old in-memory
    # subscription.  The durable Task.worker_id writer fence must still win.
    relay._tasks[stale_worker.id] = {task.id}

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event": "monitor_session_created",
                "monitor_session_id": 61,
                "description": "stale monitor",
                **_relay_generation(task),
            },
        },
        stale_worker,
    )
    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event": "sub_agent_session_created",
                **_sub_agent_relay_payload(
                    task,
                    remote_id=9061,
                    status="running",
                    description="stale child",
                ),
            },
        },
        stale_worker,
    )

    async with session_factory() as db:
        mirrors = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.remote_id.in_([61, 9061]),
                    )
                )
            ).scalars()
        )
    assert mirrors == []
    assert broadcaster.sent == []


async def test_relay_sub_agent_created_is_idempotent_manager_mirror(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    event = {
        "channel": f"task:{task.id}",
        "data": {
            "event": "sub_agent_session_created",
            **_sub_agent_relay_payload(
                task,
                remote_id=9001,
                status="running",
            ),
        },
    }

    await relay._handle(event, worker)
    await relay._handle(event, worker)

    async with session_factory() as db:
        mirrors = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.agent_type == "sub_agent",
                        MonitorSession.source == "ccm",
                    )
                )
            ).scalars()
        )
    assert len(mirrors) == 1
    mirror = mirrors[0]
    assert mirror.remote_id == 9001
    assert mirror.id != mirror.remote_id
    assert mirror.status == "running"
    assert json.loads(mirror.meta) == {
        "ccm_worker_mirror": {
            "worker_id": worker.id,
            "task_incarnation_id": task.incarnation_id,
            "remote_id": 9001,
        }
    }
    assert [data["sub_agent_session_id"] for _, data in broadcaster.sent] == [
        mirror.id,
        mirror.id,
    ]


@pytest.mark.parametrize(
    ("event_type", "status"),
    [
        ("sub_agent_session_created", "running"),
        ("sub_agent_session_status", "completed"),
    ],
)
async def test_relay_sub_agent_remote_id_collision_materializes_exact_mirror(
    relay,
    broadcaster,
    session_factory,
    event_type,
    status,
):
    old_worker = await _mk_worker(session_factory, name=f"old-{event_type}")
    worker = await _mk_worker(session_factory, name=f"current-{event_type}")
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    remote_id = 9051 if status == "running" else 9052
    async with session_factory() as db:
        historical = MonitorSession(
            task_id=task.id,
            remote_id=remote_id,
            agent_type="sub_agent",
            source="ccm",
            description="historical child",
            interval=0,
            max_checks=0,
            status="completed",
            meta=json.dumps({
                "ccm_worker_mirror": {
                    "worker_id": old_worker.id,
                    "task_incarnation_id": task.incarnation_id,
                    "remote_id": remote_id,
                }
            }),
        )
        db.add(historical)
        await db.commit()
        historical_id = historical.id

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event": event_type,
                **_sub_agent_relay_payload(
                    task,
                    remote_id=remote_id,
                    status=status,
                    description="current child",
                    checks_done=2,
                    last_summary="current result",
                ),
            },
        },
        worker,
    )

    async with session_factory() as db:
        mirrors = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.remote_id == remote_id,
                    )
                )
            ).scalars()
        )
    assert len(mirrors) == 2
    historical = next(row for row in mirrors if row.id == historical_id)
    current = next(row for row in mirrors if row.id != historical_id)
    assert historical.status == "completed"
    assert historical.description == "historical child"
    assert current.status == status
    assert current.description == "current child"
    assert current.checks_done == 2
    assert current.last_summary == "current result"
    assert json.loads(current.meta) == {
        "ccm_worker_mirror": {
            "worker_id": worker.id,
            "task_incarnation_id": task.incarnation_id,
            "remote_id": remote_id,
        }
    }
    assert broadcaster.sent == [
        (
            f"task:{task.id}",
            {
                "event": event_type,
                **_sub_agent_relay_payload(
                    task,
                    remote_id=remote_id,
                    status=status,
                    description="current child",
                    checks_done=2,
                    last_summary="current result",
                ),
                "sub_agent_session_id": current.id,
            },
        )
    ]


async def test_relay_sub_agent_terminal_status_materializes_and_never_rewrites(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}

    terminal = {
        "channel": f"task:{task.id}",
        "data": {
            "event": "sub_agent_session_status",
            **_sub_agent_relay_payload(
                task,
                remote_id=9002,
                status="completed",
                description="canonical child",
                checks_done=3,
                last_summary="canonical result",
            ),
        },
    }
    await relay._handle(terminal, worker)

    duplicate = deepcopy(terminal)
    duplicate["data"]["description"] = "late conflicting description"
    duplicate["data"]["checks_done"] = 99
    duplicate["data"]["last_summary"] = "late conflicting result"
    await relay._handle(duplicate, worker)

    delayed_created = deepcopy(terminal)
    delayed_created["data"]["event"] = "sub_agent_session_created"
    delayed_created["data"]["status"] = "running"
    await relay._handle(delayed_created, worker)

    conflicting_terminal = deepcopy(terminal)
    conflicting_terminal["data"]["status"] = "failed"
    await relay._handle(conflicting_terminal, worker)

    async with session_factory() as db:
        mirrors = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.remote_id == 9002,
                    )
                )
            ).scalars()
        )
    assert len(mirrors) == 1
    mirror = mirrors[0]
    assert mirror.status == "completed"
    assert mirror.description == "canonical child"
    assert mirror.checks_done == 3
    assert mirror.last_summary == "canonical result"
    assert mirror.completed_at is not None
    # The same terminal status may be acknowledged again, but its forwarded
    # view is canonicalized from the durable mirror. Revival/conflict is mute.
    assert len(broadcaster.sent) == 2
    assert broadcaster.sent[-1][1]["sub_agent_session_id"] == mirror.id
    assert broadcaster.sent[-1][1]["description"] == "canonical child"
    assert broadcaster.sent[-1][1]["checks_done"] == 3


@pytest.mark.parametrize(
    "generation",
    [
        {},
        {"task_retry_count": True, "task_turn_generation": 12},
        {"task_retry_count": 4, "task_turn_generation": True},
        {"task_retry_count": 4, "task_turn_generation": 11},
    ],
    ids=["missing", "boolean-retry", "boolean-turn", "stale-turn"],
)
async def test_relay_drops_sub_agent_event_without_exact_generation(
    relay,
    broadcaster,
    session_factory,
    generation,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        retry_count=4,
        turn_generation=12,
    )
    relay._tasks[worker.id] = {task.id}
    payload = _sub_agent_relay_payload(
        task,
        remote_id=9003,
        status="running",
    )
    payload.pop("task_retry_count")
    payload.pop("task_turn_generation")
    payload.update(generation)

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event": "sub_agent_session_created",
                **payload,
            },
        },
        worker,
    )

    async with session_factory() as db:
        mirrors = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.remote_id == 9003,
                    )
                )
            ).scalars()
        )
    assert mirrors == []
    assert broadcaster.sent == []


async def test_relay_sub_agent_event_rejects_wrong_existing_category(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task.id,
            remote_id=9004,
            agent_type="monitor",
            source="ccm",
            description="must not alias",
        )
        db.add(monitor)
        await db.commit()

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event": "sub_agent_session_created",
                **_sub_agent_relay_payload(
                    task,
                    remote_id=9004,
                    status="running",
                ),
            },
        },
        worker,
    )

    async with session_factory() as db:
        rows = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.remote_id == 9004,
                    )
                )
            ).scalars()
        )
    assert len(rows) == 1
    assert rows[0].agent_type == "monitor"
    assert broadcaster.sent == []


@pytest.mark.parametrize(
    "event_type",
    [
        "monitor_session_created",
        "monitor_check",
        "monitor_session_status",
        "sub_agent_session_created",
        "sub_agent_session_status",
    ],
)
@pytest.mark.parametrize(
    "identity_kind",
    ["missing", "malformed", "extra-key"],
)
async def test_relay_child_event_fails_closed_on_ambiguous_mirror_identity(
    relay,
    broadcaster,
    session_factory,
    event_type,
    identity_kind,
):
    worker = await _mk_worker(
        session_factory,
        name=f"ambiguous-{event_type}-{identity_kind}",
    )
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    remote_id = 9071
    if identity_kind == "missing":
        meta = None
    elif identity_kind == "malformed":
        meta = "not-json"
    else:
        meta = json.dumps({
            "ccm_worker_mirror": {
                "worker_id": worker.id,
                "task_incarnation_id": task.incarnation_id,
                "remote_id": remote_id,
                "unexpected": True,
            }
        })
    agent_type = (
        "sub_agent" if event_type.startswith("sub_agent_") else "monitor"
    )
    async with session_factory() as db:
        ambiguous = MonitorSession(
            task_id=task.id,
            remote_id=remote_id,
            agent_type=agent_type,
            source="ccm",
            description="ambiguous existing mirror",
            interval=0 if agent_type == "sub_agent" else 120,
            max_checks=0 if agent_type == "sub_agent" else 50,
            status="running",
            meta=meta,
        )
        db.add(ambiguous)
        await db.commit()
        ambiguous_id = ambiguous.id

    if event_type == "monitor_session_created":
        data = {
            "event": event_type,
            "monitor_session_id": remote_id,
            "description": "new monitor",
            **_relay_generation(task),
        }
    elif event_type == "monitor_check":
        data = {
            "event": event_type,
            "monitor_session_id": remote_id,
            "check_number": 1,
            "status": "ok",
            "summary": "must not apply",
            **_relay_generation(task),
        }
    elif event_type == "monitor_session_status":
        data = {
            "event": event_type,
            "monitor_session_id": remote_id,
            "status": "completed",
            **_relay_generation(task),
        }
    else:
        data = {
            "event": event_type,
            **_sub_agent_relay_payload(
                task,
                remote_id=remote_id,
                status=(
                    "completed"
                    if event_type == "sub_agent_session_status"
                    else "running"
                ),
                description="new child",
                last_summary="must not apply",
            ),
        }

    await relay._handle(
        {"channel": f"task:{task.id}", "data": data},
        worker,
    )

    async with session_factory() as db:
        rows = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.remote_id == remote_id,
                    )
                )
            ).scalars()
        )
        checks = list((await db.execute(select(MonitorCheck))).scalars())
    assert [row.id for row in rows] == [ambiguous_id]
    assert rows[0].status == "running"
    assert rows[0].description == "ambiguous existing mirror"
    assert rows[0].last_summary is None
    assert checks == []
    assert broadcaster.sent == []


async def test_relay_preserves_native_sub_agent_notification_namespace(
    relay,
    broadcaster,
    session_factory,
):
    """Native watcher events share names but are not CCM Worker mirrors."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    native_event = {
        "event_type": "sub_agent_session_status",
        "sub_agent_session_id": 44,
        "agent_type": "native-agent",
        "source": "native",
        "status": "completed",
    }

    await relay._handle(
        {"channel": f"task:{task.id}", "data": native_event},
        worker,
    )

    async with session_factory() as db:
        mirrors = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task.id,
                        MonitorSession.remote_id == 44,
                    )
                )
            ).scalars()
        )
    assert mirrors == []
    assert broadcaster.sent == [(f"task:{task.id}", native_event)]


@pytest.mark.parametrize(
    "invalid_fields",
    [
        {"native_mirror_version": 2},
        {"native_mirror_version": 1, "source": "ccm"},
        {
            "native_mirror_version": 1,
            "native_sequence": 1,
            "status": [],
        },
    ],
)
async def test_relay_drops_invalid_declared_native_sub_agent_snapshot(
    relay,
    broadcaster,
    session_factory,
    invalid_fields,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    event = {
        "event_type": "sub_agent_session_status",
        "sub_agent_session_id": 45,
        "agent_type": "native-agent",
        "source": "native",
        "provider": "codex",
        "description": "invalid declared snapshot",
        "status": "completed",
        "checks_done": 0,
        "last_summary": None,
        "codex_thread_id": "thread-invalid",
        **_relay_generation(task),
        **invalid_fields,
    }

    await relay._handle(
        {"channel": f"task:{task.id}", "data": event},
        worker,
    )

    async with session_factory() as db:
        mirrors = list((
            await db.execute(
                select(MonitorSession).where(
                    MonitorSession.task_id == task.id,
                    MonitorSession.remote_id == 45,
                )
            )
        ).scalars())
    assert mirrors == []
    assert broadcaster.sent == []


async def test_relay_mirrors_versioned_native_sub_agent_lifecycle(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    base = {
        "sub_agent_session_id": 44,
        "agent_type": "native-agent",
        "source": "native",
        "native_mirror_version": 1,
        "provider": "codex",
        "description": "inspect scheduler",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "codex_thread_id": "thread-child",
        **_relay_generation(task),
    }

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                **base,
                "event_type": "sub_agent_session_created",
                "status": "running",
                "checks_done": 0,
                "last_summary": None,
                "native_sequence": 1,
            },
        },
        worker,
    )
    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                **base,
                "event_type": "sub_agent_report",
                "status": "running",
                "checks_done": 1,
                "check_number": 1,
                "last_summary": "scheduler inspected",
                "native_sequence": 2,
            },
        },
        worker,
    )
    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                **base,
                "event_type": "sub_agent_session_status",
                "status": "failed",
                "checks_done": 1,
                "last_summary": "scheduler inspected",
                "native_sequence": 3,
            },
        },
        worker,
    )

    async with session_factory() as db:
        mirror = (
            await db.execute(
                select(MonitorSession).where(
                    MonitorSession.task_id == task.id,
                    MonitorSession.remote_id == 44,
                )
            )
        ).scalar_one()
        reports = list((
            await db.execute(
                select(MonitorCheck).where(
                    MonitorCheck.monitor_session_id == mirror.id,
                )
            )
        ).scalars())
    assert mirror.source == "native"
    assert mirror.provider == "codex"
    assert mirror.codex_thread_id == "thread-child"
    assert mirror.status == "failed"
    assert mirror.checks_done == 1
    assert mirror.last_summary == "scheduler inspected"
    assert [(report.check_number, report.summary) for report in reports] == [
        (1, "scheduler inspected"),
    ]
    assert [
        event["event_type"] for _, event in broadcaster.sent
    ] == [
        "sub_agent_session_created",
        "sub_agent_report",
        "sub_agent_session_status",
    ]
    assert all(
        event["sub_agent_session_id"] == mirror.id
        for _, event in broadcaster.sent
    )


async def test_relay_reactivates_codex_child_only_with_newer_sequence(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}
    base = {
        "event_type": "sub_agent_session_status",
        "sub_agent_session_id": 46,
        "agent_type": "native-agent",
        "source": "native",
        "native_mirror_version": 1,
        "provider": "codex",
        "description": "reused child thread",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "codex_thread_id": "thread-reused",
        "checks_done": 0,
        "last_summary": None,
        **_relay_generation(task),
    }

    for status, sequence in (
        ("completed", 1),
        ("running", 2),
        # Delayed terminal replay must not close the newer activation.
        ("completed", 1),
    ):
        await relay._handle(
            {
                "channel": f"task:{task.id}",
                "data": {
                    **base,
                    "status": status,
                    "native_sequence": sequence,
                },
            },
            worker,
        )

    async with session_factory() as db:
        mirror = (
            await db.execute(
                select(MonitorSession).where(
                    MonitorSession.task_id == task.id,
                    MonitorSession.remote_id == 46,
                )
            )
        ).scalar_one()
    assert mirror.status == "running"
    assert mirror.completed_at is None
    assert [event["status"] for _, event in broadcaster.sent] == [
        "completed",
        "running",
    ]


async def test_relay_drops_stale_sub_agent_count_projection(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    relay._tasks[worker.id] = {task.id}

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "sub_agent_count",
                "event_type": "sub_agent_count",
                "task_id": task.id,
                "active_sub_agents": 2,
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation + 1,
            },
        },
        worker,
    )

    assert broadcaster.sent == []


async def test_relay_context_usage_syncs(relay, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}
    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {
            "event_type": "context_usage",
            "input_tokens": 100,
            "context_window": 200000,
            **_relay_generation(t),
        },
    }, w)
    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.context_window_usage == {"input_tokens": 100, "context_window": 200000}


@pytest.mark.parametrize(
    ("event_type", "event_key", "extra"),
    [
        ("context_usage", "event_type", {"input_tokens": 999}),
        ("message_delta", "event_type", {"content": "stale"}),
        ("thinking_delta", "event_type", {"content": "stale"}),
        ("loop_iteration_end", "event", {"progress": "9/9"}),
        ("goal_evaluation", "event_type", {"turn": 9, "reason": "stale"}),
        (
            "monitor_session_created",
            "event",
            {"monitor_session_id": 91, "description": "stale"},
        ),
    ],
)
async def test_relay_drops_direct_state_event_from_stale_turn(
    relay,
    broadcaster,
    session_factory,
    event_type,
    event_key,
    extra,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        retry_count=4,
        turn_generation=12,
        loop_progress="1/9",
        goal_turns_used=2,
        goal_last_reason="current",
    )
    relay._tasks[worker.id] = {task.id}
    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                event_key: event_type,
                **extra,
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation - 1,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        monitors = (
            await db.execute(
                select(MonitorSession).where(MonitorSession.task_id == task.id)
            )
        ).scalars().all()
    assert current.context_window_usage is None
    assert current.loop_progress == "1/9"
    assert current.goal_turns_used == 2
    assert current.goal_last_reason == "current"
    assert monitors == []
    assert broadcaster.sent == []


@pytest.mark.parametrize(
    "generation_fields",
    [
        {},
        {"task_retry_count": 4, "task_turn_generation": True},
        {"task_retry_count": True, "task_turn_generation": 12},
        {"task_retry_count": 3, "task_turn_generation": 12},
    ],
    ids=["missing", "boolean-turn", "boolean-retry", "stale-retry"],
)
async def test_relay_drops_direct_state_event_without_valid_generation(
    relay,
    broadcaster,
    session_factory,
    generation_fields,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        retry_count=4,
        turn_generation=12,
    )
    relay._tasks[worker.id] = {task.id}
    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "context_usage",
                "input_tokens": 999,
                **generation_fields,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.context_window_usage is None
    assert broadcaster.sent == []


async def test_delayed_worker_message_cannot_mark_reassigned_local_task_unread(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        has_unread=False,
    )
    relay._tasks[worker.id] = {task.id}
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                worker_id=None,
                status="executing",
                retry_count=Task.retry_count + 1,
                session_id="local-session",
            )
        )
        await db.commit()

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "late Worker output",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
    assert current.worker_id is None
    assert current.status == "executing"
    assert current.has_unread is False
    assert current.session_id == "local-session"
    assert logs == []
    assert broadcaster.sent == []


async def test_delayed_worker_status_cannot_overwrite_reassigned_local_generation(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def delayed_snapshot(*_args, **_kwargs):
        fetch_started.set()
        await release_fetch.wait()
        return _remote_task(
            task,
            status="completed",
            session_id="old-worker-session",
            completed_at=None,
        )

    relay._fetch_task_snapshot = AsyncMock(side_effect=delayed_snapshot)
    handling = asyncio.create_task(
        relay._handle(
            {
                "channel": "tasks",
                "data": {
                    "event": "status_change",
                    "task_id": task.id,
                    "new_status": "completed",
                },
            },
            worker,
        )
    )
    await fetch_started.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                worker_id=None,
                status="executing",
                retry_count=Task.retry_count + 1,
                session_id="local-session",
            )
        )
        await db.commit()
    release_fetch.set()
    await handling

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id is None
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "local-session"
    assert current.completed_at is None
    assert broadcaster.sent == []


async def test_delayed_worker_status_cannot_overwrite_same_worker_retry_aba(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def delayed_snapshot(*_args, **_kwargs):
        fetch_started.set()
        await release_fetch.wait()
        return _remote_task(
            task,
            status="completed",
            session_id="old-session",
            completed_at=None,
        )

    relay._fetch_task_snapshot = AsyncMock(side_effect=delayed_snapshot)
    handling = asyncio.create_task(
        relay._handle(
            {
                "channel": "tasks",
                "data": {
                    "event": "status_change",
                    "task_id": task.id,
                    "new_status": "completed",
                },
            },
            worker,
        )
    )
    await fetch_started.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                status="executing",
                retry_count=Task.retry_count + 1,
                session_id="new-session",
            )
        )
        await db.commit()
    release_fetch.set()
    await handling

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id == worker.id
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "new-session"
    assert current.completed_at is None
    assert broadcaster.sent == []


async def test_worker_status_publication_fence_drops_superseded_result(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="completed",
            completed_at=None,
        )
    )
    real_publish = relay._publish_status_generation

    async def retry_before_publication(
        generation,
        payload=None,
        *,
        notify_completion=True,
    ):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    status="executing",
                    retry_count=Task.retry_count + 1,
                    completed_at=None,
                )
            )
            await db.commit()
        return await real_publish(
            generation,
            payload,
            notify_completion=notify_completion,
        )

    relay._publish_status_generation = AsyncMock(
        side_effect=retry_before_publication
    )
    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "completed",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.completed_at is None
    assert broadcaster.sent == []


async def test_reconnect_backfill_cannot_write_after_task_moves_local(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    history_started = asyncio.Event()
    release_history = asyncio.Event()

    class Response:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload

        def json(self):
            return self._payload

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **_kwargs):
            if "/chat/history" in url:
                history_started.set()
                await release_history.wait()
                return Response(
                    [
                        {
                            "event_type": "message",
                            "role": "assistant",
                            "content": "late history",
                        }
                    ]
                )
            return Response(
                _remote_task(
                    task,
                    status="completed",
                    session_id="old-worker-session",
                    completed_at=None,
                )
            )

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)
    backfill = asyncio.create_task(
        relay._backfill_missing_logs(worker, {task.id})
    )
    await history_started.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                worker_id=None,
                status="executing",
                retry_count=Task.retry_count + 1,
                session_id="local-session",
            )
        )
        await db.commit()
    release_history.set()
    await backfill

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
    assert current.worker_id is None
    assert current.status == "executing"
    assert current.session_id == "local-session"
    assert logs == []
    assert broadcaster.sent == []


async def test_backfill_only_imports_exact_worker_retry_generation(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    async with session_factory() as db:
        db.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count - 1,
            task_turn_generation=task.turn_generation - 1,
            event_type="message",
            role="assistant",
            content="same content",
        ))
        await db.commit()

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "event_type": "message",
                    "role": "assistant",
                    "content": "same content",
                    "task_retry_count": task.retry_count,
                    "task_turn_generation": task.turn_generation,
                },
                {
                    "event_type": "message",
                    "role": "assistant",
                    "content": "stale",
                    "task_retry_count": task.retry_count - 1,
                    "task_turn_generation": task.turn_generation - 1,
                },
                {
                    "event_type": "message",
                    "role": "assistant",
                    "content": "stale turn, same retry",
                    "task_retry_count": task.retry_count,
                    "task_turn_generation": task.turn_generation - 1,
                },
            ]

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(task)
    )

    await relay._backfill_missing_logs(worker, {task.id})

    async with session_factory() as db:
        logs = (
            await db.execute(
                select(LogEntry)
                .where(LogEntry.task_id == task.id)
                .order_by(LogEntry.id)
            )
        ).scalars().all()
    assert [
        (
            row.content,
            row.task_retry_count,
            row.task_turn_generation,
            row.turn_scope,
        )
        for row in logs
    ] == [
        (
            "same content",
            task.retry_count - 1,
            task.turn_generation - 1,
            None,
        ),
        ("same content", task.retry_count, task.turn_generation, None),
    ]


@pytest.mark.parametrize(
    "history_kind",
    [
        "missing-turn",
        "non-current-only",
        "unknown-scope",
        "transport-on-output",
    ],
)
async def test_completion_backfill_does_not_confirm_unscoped_history(
    relay,
    session_factory,
    monkeypatch,
    history_kind,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=7,
        tags=["pr-review"],
    )
    message = {
        "event_type": "result",
        "role": "assistant",
        "content": "cannot prove this terminal turn",
        "task_retry_count": task.retry_count,
    }
    if history_kind == "non-current-only":
        message["task_turn_generation"] = task.turn_generation - 1
    elif history_kind == "unknown-scope":
        message["task_turn_generation"] = task.turn_generation
        message["turn_scope"] = "background"
    elif history_kind == "transport-on-output":
        message["task_turn_generation"] = task.turn_generation
        message["turn_scope"] = "foreground"
        message["actual_transport"] = "codex_exec"
    messages = [message]
    class Response:
        status_code = 200

        @staticmethod
        def json():
            return messages

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)

    synced = await relay._backfill_missing_logs(
        worker,
        {task.id},
        sync_status=False,
    )

    assert synced == set()
    async with session_factory() as db:
        logs = (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
    assert logs == []


async def test_completion_backfill_ignores_legacy_unscoped_with_exact_terminal(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=7,
        tags=["pr-review"],
    )
    messages = [
        {
            "event_type": "message",
            "role": "assistant",
            "content": "legacy row must not be imported",
            "task_retry_count": task.retry_count,
            "task_turn_generation": None,
            "native_turn_id": None,
        },
        {
            "event_type": "result",
            "role": "assistant",
            "content": "exact terminal evidence",
            "task_retry_count": task.retry_count,
            "task_turn_generation": task.turn_generation,
            "native_turn_id": "native-turn-backfill-7",
            "turn_scope": "foreground",
        },
    ]

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return messages

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)

    synced = await relay._backfill_missing_logs(
        worker,
        {task.id},
        sync_status=False,
    )

    assert synced == {task.id}
    async with session_factory() as db:
        logs = list((await db.execute(
            select(LogEntry).where(LogEntry.task_id == task.id)
        )).scalars())
    assert len(logs) == 1
    assert logs[0].content == "exact terminal evidence"
    assert logs[0].native_turn_id == "native-turn-backfill-7"
    assert logs[0].turn_scope == "foreground"
    assert logs[0].actual_transport is None


@pytest.mark.parametrize("with_terminal_log", [True, False])
async def test_recovery_backfill_replays_history_after_handoff_adopts_next_turn(
    relay,
    session_factory,
    monkeypatch,
    with_terminal_log,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=7,
        tags=["pr-review"],
    )
    async with session_factory() as db:
        stale_source = LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            turn_scope="source",
            event_type="user_message",
            role="user",
            content="source for recovered G",
        )
        db.add(stale_source)
        await db.flush()
        current = await db.get(Task, task.id)
        current.turn_source_log_id = stale_source.id
        await db.commit()
    reserved = await _reserve_worker_handoff(session_factory, task)
    async with session_factory() as db:
        acknowledged = await (
            worker_relay_module.acknowledge_worker_turn_handoff(
                db,
                reserved,
                session_id="worker-session-next",
            )
        )
        assert acknowledged is not None
        await db.commit()
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_launched_handoff_receipt(reserved)
    )

    messages = [{
        "event_type": "result",
        "role": "assistant",
        "content": "recovered exact G+1 terminal",
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation + 1,
        "native_turn_id": "native-recovered-turn-8",
        "turn_scope": "foreground",
    }] if with_terminal_log else []

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return messages

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)
    completion = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pty_background_completion=completion,
        ),
    )
    relay._fetch_task_snapshot = AsyncMock(return_value=_remote_task(
        task,
        status="completed",
        turn_generation=task.turn_generation + 1,
        session_id="worker-session-next",
    ))

    synced = await relay._backfill_missing_logs(worker, {task.id})

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = list((await db.execute(
            select(LogEntry).where(LogEntry.task_id == task.id)
        )).scalars())
    assert synced == {task.id}
    assert current.turn_generation == task.turn_generation + 1
    assert current.worker_turn_handoff_id is None
    assert current.turn_source_log_id is None
    assistant_logs = [log for log in logs if log.role == "assistant"]
    if with_terminal_log:
        assert [log.content for log in assistant_logs] == [
            "recovered exact G+1 terminal"
        ]
        assert assistant_logs[-1].native_turn_id == "native-recovered-turn-8"
        assert assistant_logs[-1].turn_scope == "foreground"
        assert assistant_logs[-1].actual_transport is None
    else:
        assert assistant_logs == []
    completion.assert_awaited_once_with(task.id)


async def test_active_termination_receipt_blocks_backfill_log_and_marker_clear(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=7,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    adopted = await relay._observe_or_adopt_event_generation(
        worker.id,
        task.id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation + 1,
        worker_turn_handoff_id=reserved.worker_turn_handoff_id,
    )
    assert adopted is not None
    await _create_manager_termination_receipt(session_factory, task.id)
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_launched_handoff_receipt(reserved)
    )
    messages = [
        {
            "event_type": "result",
            "role": "assistant",
            "content": "must remain remote while termination owns the task",
            "task_retry_count": task.retry_count,
            "task_turn_generation": task.turn_generation + 1,
            "native_turn_id": "native-receipt-owned-turn-8",
            "turn_scope": "foreground",
        }
    ]

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return messages

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)

    synced = await relay._backfill_missing_logs(
        worker,
        {task.id},
        sync_status=False,
    )

    assert synced == set()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        handoff = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
        assistant_logs = list(
            (
                await db.execute(
                    select(LogEntry).where(
                        LogEntry.task_id == task.id,
                        LogEntry.role == "assistant",
                    )
                )
            ).scalars()
        )
    assert current.status == "executing"
    assert current.turn_generation == task.turn_generation + 1
    assert current.worker_turn_handoff_id == reserved.worker_turn_handoff_id
    assert handoff.status == "prepared"
    assert assistant_logs == []


async def test_terminal_launched_handoff_settles_with_only_old_turn_history(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=7,
        tags=["pr-review"],
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    async with session_factory() as db:
        acknowledged = await (
            worker_relay_module.acknowledge_worker_turn_handoff(
                db,
                reserved,
                session_id="worker-session-next",
            )
        )
        assert acknowledged is not None
        await db.commit()
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_launched_handoff_receipt(reserved)
    )

    # The Worker has durably launched and terminally completed G+1, but its
    # compact history contains only a non-user row from G.  That old row must
    # not keep the exact G -> G+1 marker and Manager receipt alive forever.
    messages = [{
        "event_type": "result",
        "role": "assistant",
        "content": "old G terminal row",
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation,
        "native_turn_id": "native-old-turn-7",
    }]

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return messages

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)
    completion = AsyncMock()
    monkeypatch.setattr(
        main_module,
        "dispatcher",
        SimpleNamespace(
            _handle_pty_background_completion=completion,
        ),
    )
    relay._fetch_task_snapshot = AsyncMock(return_value=_remote_task(
        task,
        status="completed",
        turn_generation=task.turn_generation + 1,
        session_id="worker-session-next",
    ))

    synced = await relay._backfill_missing_logs(worker, {task.id})

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
        non_user_logs = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type != "user_message",
            )
        )).scalars())
    assert synced == {task.id}
    assert current.status == "completed"
    assert current.turn_generation == task.turn_generation + 1
    assert current.worker_turn_handoff_id is None
    assert receipt.status == "completed"
    assert [
        (log.content, log.task_turn_generation)
        for log in non_user_logs
    ] == [("old G terminal row", task.turn_generation)]


async def test_launching_handoff_startup_failure_snapshot_converges_manager(
    relay,
    session_factory,
    monkeypatch,
):
    """A Worker restart fail-closes G+1 without making it replayable."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=4,
        turn_generation=12,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_handoff_receipt_with_status(reserved, "launching")
    )
    relay._resume_worker_turn_handoff = AsyncMock(return_value=True)

    class Response:
        status_code = 200

        @staticmethod
        def json():
            # Startup fail-close may have no provider output at all.  The exact
            # terminal snapshot plus a complete empty G+1 history still settles
            # the Manager marker without replaying the prompt.
            return []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)
    relay._fetch_task_snapshot = AsyncMock(return_value=_remote_task(
        task,
        status="failed",
        turn_generation=task.turn_generation + 1,
        error_message=(
            "CCM restarted after the Worker handoff crossed its launch boundary"
        ),
    ))

    synced = await relay._backfill_missing_logs(worker, {task.id})

    assert synced == {task.id}
    # ``launching`` is post-boundary evidence: reconcile it, never resume it.
    relay._resume_worker_turn_handoff.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager_receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
    assert current.status == "failed"
    assert current.turn_generation == task.turn_generation + 1
    assert current.worker_turn_handoff_id is None
    assert "crossed its launch boundary" in current.error_message
    assert manager_receipt.status == "completed"


async def test_completion_backfill_syncs_logs_without_recursive_status(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        tags=["pr-review"],
    )

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return [{
                "event_type": "result",
                "role": "assistant",
                "content": "terminal",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation,
                "native_turn_id": "native-terminal-turn",
            }]

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)
    relay._fetch_task_snapshot = AsyncMock()
    relay._publish_status_generation = AsyncMock()
    relay._publish_background_generation = AsyncMock()

    synced = await relay._backfill_missing_logs(
        worker,
        {task.id},
        sync_status=False,
    )

    assert synced == {task.id}
    relay._fetch_task_snapshot.assert_not_awaited()
    relay._publish_status_generation.assert_not_awaited()
    relay._publish_background_generation.assert_not_awaited()
    async with session_factory() as db:
        stored = (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalar_one()
    assert stored.content == "terminal"
    assert stored.task_retry_count == task.retry_count
    assert stored.task_turn_generation == task.turn_generation
    assert stored.native_turn_id == "native-terminal-turn"


async def test_backfill_syncs_and_broadcasts_background_marker(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(task, background_active=True)
    )

    await relay._backfill_missing_logs(worker, {task.id})

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert (
        current.pty_background_generation
        == worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL
    )
    expected = {
        "event": "background_activity",
        "event_type": "background_activity",
        "task_id": task.id,
        "background_active": True,
    }
    assert broadcaster.sent == [
        ("tasks", expected),
        (f"task:{task.id}", expected),
    ]

    broadcaster.sent.clear()
    relay._fetch_task_snapshot.return_value = _remote_task(
        task,
        background_active=False,
    )
    await relay._backfill_missing_logs(worker, {task.id})

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.pty_background_generation is None
    expected["background_active"] = False
    assert broadcaster.sent == [
        ("tasks", expected),
        (f"task:{task.id}", expected),
    ]


async def test_reconnect_exhaustion_cannot_fail_same_worker_retry(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="executing",
    )
    relay._tasks[worker.id] = {task.id}
    retried = False

    async def fail_after_retry(_worker):
        nonlocal retried
        if not retried:
            retried = True
            async with session_factory() as db:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(
                        status="executing",
                        retry_count=Task.retry_count + 1,
                        session_id="new-session",
                    )
                )
                await db.commit()
        raise OSError("still disconnected")

    relay.ensure_connection = AsyncMock(side_effect=fail_after_retry)
    monkeypatch.setattr(
        worker_relay_module.asyncio,
        "sleep",
        AsyncMock(),
    )
    await relay._reconnect(worker)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert relay.ensure_connection.await_count == 10
    assert current.worker_id == worker.id
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "new-session"
    assert current.completed_at is None
    assert current.error_message is None
    assert broadcaster.sent == []


async def test_reconnect_exhaustion_quarantines_exact_generation_and_publishes(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="executing",
    )
    relay._tasks[worker.id] = {task.id}
    relay.ensure_connection = AsyncMock(
        side_effect=OSError("still disconnected")
    )
    monkeypatch.setattr(
        worker_relay_module.asyncio,
        "sleep",
        AsyncMock(),
    )

    await relay._reconnect(worker)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "executing"
    assert current.completed_at is None
    assert "outcome is uncertain" in current.error_message
    assert "retry/migration remains blocked" in current.error_message
    # Keeping the exact generation active is the quarantine: ordinary retry
    # cannot reclaim it as though the remote Worker had stopped.
    from backend.services.task_queue import TaskQueue

    async with session_factory() as db:
        assert await TaskQueue(db).retry(task.id) is None
    assert broadcaster.sent == [
        (
            "tasks",
            {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "executing",
                "relay_state": "uncertain",
                "error_message": (
                    f"Worker {worker.name} relay reconnect exhausted; "
                    "remote execution outcome is uncertain"
                ),
                "background_active": False,
            },
        )
    ]


async def test_reconnect_exhaustion_preserves_completed_background_quarantine(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        pty_background_generation=(
            worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL
        ),
    )
    relay._tasks[worker.id] = {task.id}
    relay.ensure_connection = AsyncMock(
        side_effect=OSError("still disconnected")
    )
    monkeypatch.setattr(
        worker_relay_module.asyncio,
        "sleep",
        AsyncMock(),
    )

    await relay._reconnect(worker)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "completed"
    assert (
        current.pty_background_generation
        == worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL
    )
    assert current.completed_at == task.completed_at
    assert "outcome is uncertain" in current.error_message
    assert "retry/migration remains blocked" in current.error_message
    assert broadcaster.sent == [
        (
            "tasks",
            {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "completed",
                "relay_state": "uncertain",
                "error_message": (
                    f"Worker {worker.name} relay reconnect exhausted; "
                    "remote execution outcome is uncertain"
                ),
                "background_active": True,
            },
        )
    ]


async def test_recover_includes_completed_task_with_background_marker(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    active = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="executing",
    )
    completed_background = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        pty_background_generation=(
            worker_relay_module._WORKER_BACKGROUND_MIRROR_SENTINEL
        ),
    )
    completed_plain = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    completed_handoff = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        turn_generation=7,
    )
    await _reserve_worker_handoff(session_factory, completed_handoff)
    relay.subscribe_task = AsyncMock()
    relay._backfill_missing_logs = AsyncMock()

    await relay.recover(worker)

    subscribed = {
        call.args[1]
        for call in relay.subscribe_task.await_args_list
    }
    assert subscribed == {
        active.id,
        completed_background.id,
        completed_handoff.id,
    }
    assert completed_plain.id not in subscribed
    relay._backfill_missing_logs.assert_awaited_once_with(
        worker,
        {active.id, completed_background.id, completed_handoff.id},
    )


async def test_recover_resubscribes_durable_termination_quarantine(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        observed = worker_relay_module.worker_task_generation(current)
        assert observed is not None
        quarantined = (
            await worker_relay_module.quarantine_uncertain_worker_termination(
                db,
                observed,
                operation="cancel",
                error="response lost",
            )
        )
    assert quarantined is not None
    relay.subscribe_task = AsyncMock()
    relay._backfill_missing_logs = AsyncMock()

    await relay.recover(worker)

    relay.subscribe_task.assert_awaited_once_with(worker, task.id)
    relay._backfill_missing_logs.assert_awaited_once_with(worker, {task.id})


async def test_reconnect_snapshot_does_not_pop_new_connection_tasks(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    old_task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="executing",
    )
    new_task_ids = {999_001}
    relay._tasks[worker.id] = new_task_ids
    relay._ws[worker.id] = object()
    relay.ensure_connection = AsyncMock()
    relay.subscribe_task = AsyncMock()
    relay._backfill_missing_logs = AsyncMock()
    monkeypatch.setattr(
        worker_relay_module.asyncio,
        "sleep",
        AsyncMock(),
    )

    await relay._reconnect(worker, {old_task.id})

    assert relay._tasks[worker.id] is new_task_ids
    relay.subscribe_task.assert_awaited_once()


# === Dispatcher 双路径 ===


async def test_dispatch_worker_tasks_forwards(db_factory, session_factory, broadcaster, monkeypatch):
    from backend.services.dispatcher import GlobalDispatcher
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id, status="pending")

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    disp = GlobalDispatcher.__new__(GlobalDispatcher)
    disp.db_factory = db_factory
    disp.broadcaster = broadcaster
    disp._running_tasks = {}

    await disp._dispatch_worker_tasks()
    # 等 fire-and-forget 的 forward 跑完
    for _ in range(10):
        await asyncio.sleep(0)

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.status == "in_progress"
    assert task.turn_generation == t.turn_generation + 1
    proxy.forward_task_to_worker.assert_called_once()
    forwarded = proxy.forward_task_to_worker.await_args.args[0]
    assert forwarded.turn_generation == task.turn_generation
    assert any(c == "tasks" and d.get("new_status") == "in_progress" for c, d in broadcaster.sent)

    # The Worker's pending row starts at N and its local dequeue advances it
    # to the same N+1.  That exact first-turn snapshot is accepted; a later
    # arbitrary remote value is still rejected by apply_authoritative_worker_task.
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None
    async with session_factory() as db:
        resulting = await worker_relay_module.apply_authoritative_worker_task(
            db,
            observed,
            _remote_task(task, status="executing"),
        )
    assert resulting is not None
    assert resulting.turn_generation == task.turn_generation
    assert resulting.status == "executing"


@pytest.mark.parametrize(
    (
        "review_status",
        "reviewer_status",
        "archived",
        "should_forward",
    ),
    [
        ("reviewing", None, False, True),
        ("error", None, False, False),
        ("reviewing", "pending", False, True),
        ("reviewing", "cancelled", False, False),
        ("reviewing", "completed", False, False),
        ("reviewing", "reviewing", True, True),
    ],
    ids=(
        "single-runnable",
        "single-terminal",
        "panel-runnable",
        "panel-cancelled",
        "panel-terminal",
        "archived-panel-runnable",
    ),
)
async def test_dispatch_worker_requires_runnable_pr_review_owner(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
    review_status,
    reviewer_status,
    archived,
    should_forward,
):
    from backend.services.dispatcher import GlobalDispatcher

    _, task, _ = await _mk_worker_pr_reviewer_task(
        session_factory,
        review_status=review_status,
        reviewer_status=reviewer_status,
        archived=archived,
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    forward = dispatcher._running_tasks.get(f"worker-{task.id}")
    if forward is not None:
        await forward

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    if should_forward:
        assert current.status == "in_progress"
        assert current.turn_generation == task.turn_generation + 1
        proxy.forward_task_to_worker.assert_awaited_once()
        assert any(
            channel == "tasks"
            and event.get("task_id") == task.id
            and event.get("new_status") == "in_progress"
            for channel, event in broadcaster.sent
        )
    else:
        assert current.status == "pending"
        assert current.turn_generation == task.turn_generation
        proxy.forward_task_to_worker.assert_not_awaited()
        assert dispatcher._running_tasks == {}
        assert broadcaster.sent == []


async def test_dispatch_worker_final_claim_rechecks_cancelled_panel_reviewer(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    _, task, reviewer_id = await _mk_worker_pr_reviewer_task(
        session_factory,
        review_status="reviewing",
        reviewer_status="pending",
    )
    assert reviewer_id is not None
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    original_execute = AsyncSession.execute
    task_updates = 0
    cancelled = False

    async def cancel_immediately_before_final_claim(
        session,
        statement,
        *args,
        **kwargs,
    ):
        nonlocal task_updates, cancelled
        table = getattr(statement, "table", None)
        if (
            getattr(statement, "is_update", False)
            and getattr(table, "name", None) == "tasks"
        ):
            task_updates += 1
            if task_updates == 2:
                cancelled = True
                await original_execute(
                    session,
                    update(PRReviewerRun)
                    .where(PRReviewerRun.id == reviewer_id)
                    .values(status="cancelled"),
                )
                await session.commit()
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(
        AsyncSession,
        "execute",
        new=cancel_immediately_before_final_claim,
    ):
        await dispatcher._dispatch_worker_tasks()

    assert cancelled
    assert task_updates == 2
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        reviewer = await db.get(PRReviewerRun, reviewer_id)
    assert current.status == "pending"
    assert current.turn_generation == task.turn_generation
    assert reviewer.status == "cancelled"
    proxy.forward_task_to_worker.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_never_forwards_deleted_owner_tombstone(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    async with session_factory() as db:
        db.add(PRMonitorTaskTombstone(task_id=task.id))
        await db.commit()
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.turn_generation == task.turn_generation
    proxy.forward_task_to_worker.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_fresh_claim_uses_task_worker_user_lock_order(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    """Fresh Manager claims must match final launch-permit lock order."""

    from backend.services.dispatcher import GlobalDispatcher

    async with session_factory() as db:
        user = User(
            email="manager-worker-lock-order@example.com",
            name="Manager Worker lock order",
            password_hash="unused",
            role="admin",
            is_active=True,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        user_id = user.id
    worker = await _mk_worker(session_factory)
    await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        execution_user_id=user_id,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
    )

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    original_execute = AsyncSession.execute
    write_tables: list[str] = []

    async def record_writes(session, statement, *args, **kwargs):
        table_name = getattr(
            getattr(statement, "table", None),
            "name",
            None,
        )
        if getattr(statement, "is_update", False) and table_name in {
            "tasks",
            "workers",
            "users",
        }:
            write_tables.append(table_name)
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", new=record_writes):
        await dispatcher._dispatch_worker_tasks()

    assert write_tables[:3] == ["tasks", "workers", "users"], write_tables


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    (("is_active", False), ("role", "member")),
    ids=("disabled", "demoted"),
)
async def test_dispatch_worker_fresh_claim_rejects_revoked_native_principal(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
    changed_field,
    changed_value,
):
    from backend.services.dispatcher import GlobalDispatcher

    async with session_factory() as db:
        user = User(
            email=f"manager-worker-claim-{changed_field}@example.com",
            name="Manager Worker claim principal",
            password_hash="unused",
            role="admin",
            is_active=True,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        user_id = user.id
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        execution_user_id=user_id,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
    )
    async with session_factory() as db:
        principal = await db.get(User, user_id)
        setattr(principal, changed_field, changed_value)
        await db.commit()

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.turn_generation == task.turn_generation
    proxy.forward_task_to_worker.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_tasks_rejects_ready_destroy_recovery_worker(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(
        session_factory,
        status="ready",
        bootstrap_step="destroy",
    )
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.turn_generation == task.turn_generation
    proxy.forward_task_to_worker.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_task_respects_running_plan_capacity(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory, max_tasks=1)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Running Worker Plan consumes capacity",
            initial_request="Plan first",
            worker_id=worker.id,
            pipeline_config=pipeline,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Plan first",
            pipeline_config=pipeline,
            status="running",
            generation=0,
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "pending"
        assert current.turn_generation == task.turn_generation
    proxy.forward_task_to_worker.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_task_respects_ack_unconfirmed_cancelling_plan_capacity(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory, max_tasks=1)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Cancelling Worker Plan still consumes capacity",
            initial_request="Plan first",
            worker_id=worker.id,
            pipeline_config=pipeline,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Plan first",
            pipeline_config=pipeline,
            status="cancelling",
            generation=1,
            cancellation_target_generation=0,
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        db.add(
            PlanAgentWorkerDispatchReceipt(
                plan_id=plan.id,
                run_id=run.id,
                worker_id=worker.id,
                run_generation=0,
                protocol=1,
                status="remote_possible",
                payload_digest="a" * 64,
            )
        )
        await db.commit()

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "pending"
        assert current.turn_generation == task.turn_generation
    proxy.forward_task_to_worker.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_plan_respects_running_task_capacity(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory, max_tasks=1)
    await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Running Worker Task consumes capacity",
            initial_request="Plan later",
            worker_id=worker.id,
            pipeline_config=pipeline,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Plan later",
            pipeline_config=pipeline,
            status="queued",
            generation=0,
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        run_id = run.id

    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_plan_runs()

    async with session_factory() as db:
        current = await db.get(PlanAgentRun, run_id)
        receipts = list(
            (
                await db.execute(
                    select(PlanAgentWorkerDispatchReceipt).where(
                        PlanAgentWorkerDispatchReceipt.run_id == run_id
                    )
                )
            ).scalars()
        )
        assert current.status == "queued"
        assert receipts == []
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_plan_rejects_ready_destroy_recovery_worker(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(
        session_factory,
        status="ready",
        bootstrap_step="destroy",
    )
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Blocked destroy Worker Plan",
            initial_request="Do not start",
            worker_id=worker.id,
            pipeline_config=pipeline,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Do not start",
            pipeline_config=pipeline,
            status="queued",
            generation=0,
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        run_id = run.id

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_plan_runs()

    async with session_factory() as db:
        current = await db.get(PlanAgentRun, run_id)
        receipts = list(
            (
                await db.execute(
                    select(PlanAgentWorkerDispatchReceipt).where(
                        PlanAgentWorkerDispatchReceipt.run_id == run_id
                    )
                )
            ).scalars()
        )
    assert current.status == "queued"
    assert receipts == []
    proxy.dispatch_plan_run.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_plan_respects_ack_unconfirmed_cancelling_plan_capacity(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory, max_tasks=1)
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        blocker_plan = Plan(
            title="Cancelling Worker Plan still consumes capacity",
            initial_request="Block the Worker until cancellation is confirmed",
            worker_id=worker.id,
            pipeline_config=pipeline,
        )
        db.add(blocker_plan)
        await db.flush()
        blocker_run = PlanAgentRun(
            plan_id=blocker_plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Block the Worker until cancellation is confirmed",
            pipeline_config=pipeline,
            status="cancelling",
            generation=1,
            cancellation_target_generation=0,
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(blocker_run)
        await db.flush()
        blocker_plan.active_run_id = blocker_run.id
        db.add(
            PlanAgentWorkerDispatchReceipt(
                plan_id=blocker_plan.id,
                run_id=blocker_run.id,
                worker_id=worker.id,
                run_generation=0,
                protocol=1,
                status="remote_possible",
                payload_digest="a" * 64,
            )
        )

        queued_plan = Plan(
            title="Queued Worker Plan waits for capacity",
            initial_request="Plan later",
            worker_id=worker.id,
            pipeline_config=pipeline,
        )
        db.add(queued_plan)
        await db.flush()
        queued_run = PlanAgentRun(
            plan_id=queued_plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Plan later",
            pipeline_config=pipeline,
            status="queued",
            generation=0,
        )
        db.add(queued_run)
        await db.flush()
        queued_plan.active_run_id = queued_run.id
        await db.commit()
        queued_run_id = queued_run.id

    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_plan_runs()

    async with session_factory() as db:
        current = await db.get(PlanAgentRun, queued_run_id)
        receipts = list(
            (
                await db.execute(
                    select(PlanAgentWorkerDispatchReceipt).where(
                        PlanAgentWorkerDispatchReceipt.run_id == queued_run_id
                    )
                )
            ).scalars()
        )
        assert current.status == "queued"
        assert receipts == []
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


@pytest.mark.asyncio
async def test_worker_task_and_plan_share_atomic_capacity_in_sqlite_wal(
    tmp_path,
    monkeypatch,
):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from backend.database import Base
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.dispatcher import GlobalDispatcher

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'worker-shared-capacity.db'}",
        connect_args={"timeout": 5},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        pipeline = default_plan_pipeline_config().model_dump(mode="json")
        async with sessions() as setup:
            worker = Worker(
                name="worker-shared-capacity",
                status="ready",
                max_tasks=1,
                auth_token="worker-token",
            )
            setup.add(worker)
            await setup.flush()
            task = Task(
                title="Ordinary Worker candidate",
                description="Only one candidate may win",
                status="pending",
                worker_id=worker.id,
            )
            setup.add(task)
            await setup.flush()
            plan = Plan(
                title="Worker Plan candidate",
                initial_request="Only one candidate may win",
                worker_id=worker.id,
                target_task_id=task.id,
                pipeline_config=pipeline,
            )
            setup.add(plan)
            await setup.flush()
            run = PlanAgentRun(
                plan_id=plan.id,
                worker_id=worker.id,
                run_type="initial",
                request_text="Only one candidate may win",
                pipeline_config=pipeline,
                status="queued",
                generation=0,
            )
            setup.add(run)
            await setup.flush()
            plan.active_run_id = run.id
            await setup.commit()
            task_id = task.id
            run_id = run.id

        proxy = AsyncMock()
        monkeypatch.setattr(main_module, "worker_proxy", proxy)

        async def preserve_plan_claim(*_args, **_kwargs):
            return None

        monkeypatch.setattr(
            GlobalDispatcher,
            "_run_worker_plan_lifecycle",
            preserve_plan_claim,
        )
        task_dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
        task_dispatcher.db_factory = sessions
        task_dispatcher.broadcaster = FakeBroadcaster()
        task_dispatcher._running_tasks = {}
        plan_dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
        plan_dispatcher.db_factory = sessions
        plan_dispatcher.broadcaster = FakeBroadcaster()
        plan_dispatcher._running_tasks = {}

        await asyncio.gather(
            task_dispatcher._dispatch_worker_tasks(),
            plan_dispatcher._dispatch_worker_plan_runs(),
        )
        await asyncio.sleep(0)

        async with sessions() as check:
            current_task = await check.get(Task, task_id)
            current_run = await check.get(PlanAgentRun, run_id)
            receipts = list(
                (
                    await check.execute(
                        select(PlanAgentWorkerDispatchReceipt).where(
                            PlanAgentWorkerDispatchReceipt.run_id == run_id
                        )
                    )
                ).scalars()
            )
        task_won = current_task.status == "in_progress"
        plan_won = current_run.status == "running"
        assert int(task_won) + int(plan_won) == 1
        assert current_task.status in {"pending", "in_progress"}
        assert current_run.status in {"queued", "running"}
        if plan_won:
            assert len(receipts) == 1
            assert receipts[0].status == "prepared"
            proxy.forward_task_to_worker.assert_not_awaited()
        else:
            assert receipts == []
            proxy.forward_task_to_worker.assert_awaited_once()

        pending_lifecycles = {
            lifecycle
            for dispatcher in (task_dispatcher, plan_dispatcher)
            for lifecycle in dispatcher._running_tasks.values()
        }
        if pending_lifecycles:
            await asyncio.gather(*pending_lifecycles)
    finally:
        await engine.dispose()


async def test_dispatch_worker_tasks_without_auth_keeps_pending_and_never_posts(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(settings, "auth_token", "")
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "pending"
        assert current.turn_generation == task.turn_generation
        assert current.started_at == task.started_at
    proxy.forward_task_to_worker.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_tasks_fail_closes_unproven_plan_before_post(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        mode="plan",
        plan_approved=True,
        plan_content="# Unproven Plan",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    proxy.forward_task_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "failed"
        assert current.turn_generation == task.turn_generation
        assert "first-class Plan execution protocol" in current.error_message
    assert any(
        channel == "tasks"
        and event.get("task_id") == task.id
        and event.get("new_status") == "failed"
        for channel, event in broadcaster.sent
    )


async def test_dispatch_worker_tasks_preserves_exact_legacy_carrier_without_post(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        mode="plan",
        plan_approved=True,
        plan_content="# Approved legacy Plan",
    )
    async with session_factory() as db:
        plan = Plan(
            title="Migrated Plan",
            initial_request="legacy request",
            worker_id=worker.id,
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Approved legacy Plan",
            human_decision="approved",
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        db.add_all(
            [
                PlanLegacyTaskLink(
                    legacy_task_id=task.id,
                    plan_id=plan.id,
                    plan_version_id=version.id,
                ),
                PlanApplication(
                    plan_id=plan.id,
                    plan_version_id=version.id,
                    application_type="execution_task",
                    execution_task_id=task.id,
                ),
            ]
        )
        await db.commit()

    proxy = AsyncMock()
    recovery = Mock()
    proxy.relay = SimpleNamespace(
        ensure_legacy_plan_execution_carrier_recovery=recovery,
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    proxy.forward_task_to_worker.assert_not_awaited()
    recovery.assert_called_once()
    recovered_worker, recovered_task_id, proof_digest, recovered_proxy = (
        recovery.call_args.args
    )
    assert recovered_worker.id == worker.id
    assert recovered_task_id == task.id
    assert isinstance(proof_digest, str) and len(proof_digest) == 64
    assert recovered_proxy is proxy
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "pending"
        assert current.turn_generation == task.turn_generation
        assert current.error_message is None
    assert broadcaster.sent == []


async def test_legacy_carrier_conflict_survives_resubscribe_snapshot(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        mode="plan",
        plan_approved=True,
        plan_content="# Approved legacy Plan",
    )
    async with session_factory() as db:
        plan = Plan(
            title="Migrated Plan",
            initial_request="legacy request",
            worker_id=worker.id,
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Approved legacy Plan",
            human_decision="approved",
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        db.add_all(
            [
                PlanLegacyTaskLink(
                    legacy_task_id=task.id,
                    plan_id=plan.id,
                    plan_version_id=version.id,
                ),
                PlanApplication(
                    plan_id=plan.id,
                    plan_version_id=version.id,
                    application_type="execution_task",
                    execution_task_id=task.id,
                ),
            ]
        )
        await db.commit()
        current = await db.get(Task, task.id)
        observed = worker_relay_module.worker_task_generation(current)
        proof = await worker_relay_module.legacy_approved_execution_carrier_proof(
            db,
            task.id,
        )
    assert observed is not None
    assert proof is not None

    conflicted = await relay._conflict_legacy_plan_execution_carrier(
        observed,
        expected_proof_digest=proof.proof_digest,
        error="semantic split",
    )
    assert conflicted is not None
    assert conflicted.status == "conflict"
    assert conflicted.legacy_carrier_conflict_present

    # A later generic WorkerProxy call can subscribe the task again. The
    # resulting ordinary status snapshot remains unable to erase quarantine.
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="pending",
            completed_at=None,
        )
    )
    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "pending",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "conflict"
    assert (
        worker_relay_module.LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY
        in current.metadata_
    )
    assert broadcaster.sent == []


async def test_dispatch_worker_tasks_skips_unready_worker(db_factory, session_factory, broadcaster, monkeypatch):
    from backend.services.dispatcher import GlobalDispatcher
    w = await _mk_worker(session_factory, status="stopped")
    t = await _mk_task(session_factory, worker_id=w.id, status="pending")
    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())

    disp = GlobalDispatcher.__new__(GlobalDispatcher)
    disp.db_factory = db_factory
    disp.broadcaster = broadcaster
    disp._running_tasks = {}
    await disp._dispatch_worker_tasks()

    async with session_factory() as db:
        assert (await db.get(Task, t.id)).status == "pending"  # 留队等 worker 就绪


async def test_dispatch_worker_plan_run_imports_terminal_outcome(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Worker-dispatched Plan",
            initial_request="Plan it",
            worker_id=worker.id,
            pipeline_config=pipeline,
            priority=0,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Plan it",
            pipeline_config=pipeline,
            status="queued",
            generation=0,
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        plan_id = plan.id
        run_id = run.id
        created_at = run.created_at.isoformat()
        updated_at = run.updated_at.isoformat()

    proxy = AsyncMock()
    remote_payload = {
        "protocol": 3,
        "run": {
            "id": run_id,
            "plan_id": plan_id,
            "run_type": "initial",
            "status": "failed",
            "current_stage": "failed",
            "base_version_id": None,
            "result_version_id": None,
            "request_text": "Plan it",
            "round": 1,
            "generation": 0,
            "instance_id": None,
            "worker_id": None,
            "open_input_request_id": None,
            "interaction_count": 0,
            "max_interactions": 3,
            "execution_seconds": 1.0,
            "last_execution_started_at": None,
            "review_verdict": None,
            "review_feedback": None,
            "review_exhausted": False,
            "error": "remote failure",
            "created_at": created_at,
            "updated_at": updated_at,
            "finished_at": updated_at,
            "steps": [],
            "input_requests": [],
        },
        "versions": [],
    }

    async def run_remote(_plan, _run, *, on_remote_possible):
        await on_remote_possible("a" * 64)
        return remote_payload

    proxy.run_versioned_plan_until_pause.side_effect = run_remote
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    original_get = AsyncSession.get
    original_execute = AsyncSession.execute
    locked_rows = []

    async def recording_get(self, entity, ident, *args, **kwargs):
        if kwargs.get("with_for_update"):
            locked_rows.append((entity, ident))
        return await original_get(self, entity, ident, *args, **kwargs)

    async def recording_execute(self, statement, *args, **kwargs):
        if getattr(statement, "_for_update_arg", None) is not None:
            descriptions = getattr(statement, "column_descriptions", ())
            entity = descriptions[0].get("entity") if descriptions else None
            if entity is PlanAgentWorkerDispatchReceipt:
                locked_rows.append((entity, "current-generation"))
        return await original_execute(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "get", recording_get)
    monkeypatch.setattr(AsyncSession, "execute", recording_execute)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_plan_runs()
    lifecycle = dispatcher._running_tasks[f"worker-plan-{run_id}"]
    await lifecycle

    assert locked_rows == [
        # Claim: Task/Worker writer fences are already held, then every Plan
        # aggregate writer takes Run -> Plan -> boundary receipt.
        (PlanAgentRun, run_id),
        (Plan, plan_id),
        (PlanAgentWorkerDispatchReceipt, "current-generation"),
        # Remote-boundary publication.
        (PlanAgentRun, run_id),
        (Plan, plan_id),
        (PlanAgentWorkerDispatchReceipt, 1),
        # Terminal outcome import.
        (PlanAgentRun, run_id),
        (Plan, plan_id),
        (PlanAgentWorkerDispatchReceipt, 1),
    ]

    async with session_factory() as db:
        current_plan = await db.get(Plan, plan_id)
        current_run = await db.get(PlanAgentRun, run_id)
        assert current_run.status == "failed"
        assert current_run.error == "remote failure"
        assert current_plan.active_run_id is None
    assert any(
        channel == "plans"
        and event.get("run_id") == run_id
        and event.get("status") == "failed"
        for channel, event in broadcaster.sent
    )


@pytest.mark.asyncio
async def test_worker_plan_claim_loses_cleanly_to_wal_cancel(
    tmp_path,
    monkeypatch,
):
    """A cross-process cancel between routing read and Task fence is exact."""

    from sqlalchemy.ext.asyncio import (
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services import plan_service
    from backend.services.dispatcher import GlobalDispatcher
    from backend.services.plan_service import cancel_worker_mirror_run_after_ack

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'worker-plan-claim-cancel.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        pipeline = default_plan_pipeline_config().model_dump(mode="json")
        async with sessions() as setup:
            worker = Worker(
                name="worker-plan-claim-cancel",
                status="ready",
                max_tasks=8,
                auth_token="worker-token",
            )
            setup.add(worker)
            await setup.flush()
            target = Task(
                title="Worker Plan claim target",
                description="target",
                status="pending",
                worker_id=worker.id,
            )
            setup.add(target)
            await setup.flush()
            plan = Plan(
                title="Worker Plan WAL claim",
                initial_request="claim exactly once",
                target_task_id=target.id,
                worker_id=worker.id,
                pipeline_config=pipeline,
            )
            setup.add(plan)
            await setup.flush()
            run = PlanAgentRun(
                plan_id=plan.id,
                worker_id=worker.id,
                run_type="initial",
                request_text="claim exactly once",
                pipeline_config=pipeline,
                status="queued",
                generation=0,
            )
            setup.add(run)
            await setup.flush()
            plan.active_run_id = run.id
            await setup.commit()
            plan_id = plan.id
            run_id = run.id

        fence_entered = asyncio.Event()
        allow_fence = asyncio.Event()
        real_fence = plan_service.fence_plan_target_task

        async def delayed_target_fence(
            db,
            *,
            target_task_id,
            expected_worker_id,
        ):
            fence_entered.set()
            await allow_fence.wait()
            return await real_fence(
                db,
                target_task_id=target_task_id,
                expected_worker_id=expected_worker_id,
            )

        monkeypatch.setattr(
            plan_service,
            "fence_plan_target_task",
            delayed_target_fence,
        )
        monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())
        dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
        dispatcher.db_factory = sessions
        dispatcher.broadcaster = FakeBroadcaster()
        dispatcher._running_tasks = {}

        claim = asyncio.create_task(dispatcher._dispatch_worker_plan_runs())
        await asyncio.wait_for(fence_entered.wait(), timeout=2)
        try:
            async with sessions() as cancellation_db:
                cancel_plan = await cancellation_db.get(Plan, plan_id)
                cancel_run = await cancellation_db.get(PlanAgentRun, run_id)
                assert cancel_plan is not None and cancel_run is not None
                cancelled = await cancel_worker_mirror_run_after_ack(
                    cancellation_db,
                    plan=cancel_plan,
                    run=cancel_run,
                )
                assert cancelled.status == "cancelled"
        finally:
            allow_fence.set()
        await asyncio.wait_for(claim, timeout=5)

        async with sessions() as check:
            persisted_plan = await check.get(Plan, plan_id)
            persisted_run = await check.get(PlanAgentRun, run_id)
            receipts = list(
                (
                    await check.execute(
                        select(PlanAgentWorkerDispatchReceipt).where(
                            PlanAgentWorkerDispatchReceipt.run_id == run_id
                        )
                    )
                ).scalars()
            )
            assert persisted_plan is not None
            assert persisted_plan.active_run_id is None
            assert persisted_run is not None and persisted_run.status == "cancelled"
            assert receipts == []
        assert dispatcher._running_tasks == {}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "with_target_task",
    (True, False),
    ids=("task-fence", "standalone-run-fence"),
)
async def test_worker_plan_remote_boundary_restarts_after_wal_receipt_read(
    tmp_path,
    monkeypatch,
    with_target_task,
):
    """A concurrent commit after receipt routing read cannot leak BUSY_SNAPSHOT."""

    from sqlalchemy.ext.asyncio import (
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.worker_plan_dispatch import (
        mark_worker_dispatch_remote_possible,
    )

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'worker-plan-boundary.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        pipeline = default_plan_pipeline_config().model_dump(mode="json")
        async with sessions() as setup:
            worker = Worker(
                name="worker-plan-boundary",
                status="ready",
                auth_token="worker-token",
            )
            setup.add(worker)
            await setup.flush()
            target = None
            if with_target_task:
                target = Task(
                    title="Worker Plan boundary target",
                    description="target",
                    status="pending",
                    worker_id=worker.id,
                )
                setup.add(target)
                await setup.flush()
            plan = Plan(
                title="Worker Plan boundary",
                initial_request="publish boundary exactly",
                target_task_id=target.id if target is not None else None,
                worker_id=worker.id,
                pipeline_config=pipeline,
            )
            setup.add(plan)
            await setup.flush()
            run = PlanAgentRun(
                plan_id=plan.id,
                worker_id=worker.id,
                run_type="initial",
                request_text="publish boundary exactly",
                pipeline_config=pipeline,
                status="running",
                generation=3,
                last_execution_started_at=datetime.utcnow(),
            )
            setup.add(run)
            await setup.flush()
            plan.active_run_id = run.id
            receipt = PlanAgentWorkerDispatchReceipt(
                plan_id=plan.id,
                run_id=run.id,
                target_task_id=target.id if target is not None else None,
                worker_id=worker.id,
                run_generation=run.generation,
                protocol=1,
                status="prepared",
            )
            setup.add(receipt)
            await setup.commit()
            worker_id = worker.id
            plan_id = plan.id
            run_id = run.id
            receipt_id = receipt.id
            generation = run.generation

        routing_read = asyncio.Event()
        allow_boundary = asyncio.Event()
        original_get = AsyncSession.get

        async def interleaved_get(self, entity, ident, *args, **kwargs):
            result = await original_get(self, entity, ident, *args, **kwargs)
            if (
                (
                    with_target_task
                    and entity is PlanAgentWorkerDispatchReceipt
                    and ident == receipt_id
                    and not kwargs.get("with_for_update")
                )
                or (
                    not with_target_task
                    and entity is PlanAgentRun
                    and ident == run_id
                    and kwargs.get("with_for_update")
                )
            ) and not routing_read.is_set():
                routing_read.set()
                await allow_boundary.wait()
            return result

        monkeypatch.setattr(AsyncSession, "get", interleaved_get)
        boundary = asyncio.create_task(
            mark_worker_dispatch_remote_possible(
                sessions,
                receipt_id=receipt_id,
                plan_id=plan_id,
                run_id=run_id,
                worker_id=worker_id,
                generation=generation,
                payload_digest="a" * 64,
            )
        )
        await asyncio.wait_for(routing_read.wait(), timeout=2)

        async def concurrent_commit():
            async with sessions() as concurrent_writer:
                await concurrent_writer.execute(
                    update(Worker)
                    .where(Worker.id == worker_id)
                    .values(name="worker-plan-boundary-concurrent-writer")
                )
                await concurrent_writer.commit()

        try:
            writer = asyncio.create_task(concurrent_commit())
            if with_target_task:
                # The commit lands after the routing SELECT but before the
                # helper discards that snapshot.
                await asyncio.wait_for(writer, timeout=2)
            else:
                # A standalone Plan has no Task row. Its portable no-op Run
                # writer fence must already exclude this second connection
                # before SELECT FOR UPDATE reloads the aggregate.
                await asyncio.sleep(0.05)
                assert not writer.done()
        finally:
            allow_boundary.set()
        snapshot = await asyncio.wait_for(boundary, timeout=5)
        await asyncio.wait_for(writer, timeout=2)

        assert snapshot.status == "remote_possible"
        assert snapshot.payload_digest == "a" * 64
        async with sessions() as check:
            persisted = await check.get(
                PlanAgentWorkerDispatchReceipt,
                receipt_id,
            )
            assert persisted is not None
            assert persisted.status == "remote_possible"
            assert persisted.payload_digest == "a" * 64
    finally:
        await engine.dispose()


async def test_dispatch_worker_plan_without_auth_keeps_queued_without_receipt(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.schemas.plan import default_plan_pipeline_config
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Unauthenticated Worker Plan",
            initial_request="Plan it",
            worker_id=worker.id,
            pipeline_config=pipeline,
            priority=0,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Plan it",
            pipeline_config=pipeline,
            status="queued",
            generation=0,
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        run_id = run.id

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(settings, "auth_token", "   ")
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_plan_runs()

    async with session_factory() as db:
        current = await db.get(PlanAgentRun, run_id)
        receipts = list(
            (
                await db.execute(
                    select(PlanAgentWorkerDispatchReceipt).where(
                        PlanAgentWorkerDispatchReceipt.run_id == run_id
                    )
                )
            ).scalars()
        )
        assert current.status == "queued"
        assert current.last_execution_started_at is None
        assert receipts == []
    proxy.run_versioned_plan_until_pause.assert_not_awaited()
    assert dispatcher._running_tasks == {}
    assert broadcaster.sent == []


async def test_dispatch_worker_claim_rejects_same_worker_pending_retry_aba(
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    factory_calls = 0

    @asynccontextmanager
    async def racing_db_factory():
        nonlocal factory_calls
        factory_calls += 1
        async with session_factory() as db:
            if factory_calls == 4:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(
                        retry_count=Task.retry_count + 1,
                        title="new retry generation",
                    )
                )
                await db.commit()
            yield db

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = racing_db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.retry_count == task.retry_count + 1
    assert current.title == "new retry generation"
    proxy.forward_task_to_worker.assert_not_awaited()
    assert broadcaster.sent == []


async def test_dispatch_worker_claim_rejects_new_shared_shadow(
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    factory_calls = 0

    @asynccontextmanager
    async def racing_db_factory():
        nonlocal factory_calls
        factory_calls += 1
        async with session_factory() as db:
            if factory_calls == 4:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(shared_from_id=987654)
                )
                await db.commit()
            yield db

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = racing_db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.shared_from_id == 987654
    proxy.forward_task_to_worker.assert_not_awaited()
    assert broadcaster.sent == []


async def test_dispatch_worker_forwards_refreshed_claimed_task(
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        previous_source = LogEntry(
            task_id=task.id,
            task_retry_count=current.retry_count,
            task_turn_generation=current.turn_generation,
            turn_scope="source",
            event_type="user_message",
            role="user",
            content="previous logical turn",
            is_error=False,
        )
        db.add(previous_source)
        await db.flush()
        current.turn_source_log_id = previous_source.id
        previous_generation = current.turn_generation
        await db.commit()
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    factory_calls = 0

    @asynccontextmanager
    async def racing_db_factory():
        nonlocal factory_calls
        factory_calls += 1
        async with session_factory() as db:
            if factory_calls == 4:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(title="current title")
                )
                await db.commit()
            yield db

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = racing_db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    forward = dispatcher._running_tasks.get(f"worker-{task.id}")
    assert forward is not None
    await forward

    forwarded_task = proxy.forward_task_to_worker.await_args.args[0]
    assert forwarded_task.title == "current title"
    assert forwarded_task.turn_generation == previous_generation + 1
    assert forwarded_task.turn_source_log_id is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.turn_generation == previous_generation + 1
    assert current.turn_source_log_id is None


def _capture_initial_worker_task_create(
    monkeypatch,
    captured_payloads,
    *,
    post_entered=None,
    release_post=None,
):
    class Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return Response({
                "worker_delegated_principal_protocol": 1,
                "worker_delegated_launch_admission_protocol": 2,
                "worker_initial_generation_protocol": 1,
                "worker_task_incarnation_proxy_version": 1,
                **_worker_namespace_capability(),
                "default_codex_model": "gpt-5.6-sol",
                "codex_model_service_tiers": {
                    "gpt-5.6-sol": ["default", "priority"],
                },
            })

        async def post(self, _url, *, headers, json):
            captured_payloads.append(dict(json))
            if post_entered is not None:
                post_entered.set()
            if release_post is not None:
                await release_post.wait()
            source_incarnation_id = json["source_incarnation_id"]
            return Response({
                **json,
                "incarnation_id": source_incarnation_id,
                "metadata_": {
                    "ccm_source_task_incarnation_id": source_incarnation_id,
                },
                "status": "pending",
                "retry_count": json["source_retry_count"],
                "turn_generation": json["source_turn_generation"] - 1,
            })

    monkeypatch.setattr(
        worker_proxy_module.httpx,
        "AsyncClient",
        Client,
    )


async def test_worker_forward_reloads_authoritative_skills_after_lock_wait(
    db_factory,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    async with session_factory() as db:
        user_skill = UserSkill(
            name="Reloaded forward skill",
            description="latest description",
            content="latest content",
        )
        db.add(user_skill)
        await db.commit()
        await db.refresh(user_skill)
        user_skill_id = user_skill.id
    stale_task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        turn_generation=1,
        enabled_skills={"code-review": False},
        selected_user_skills=[],
    )
    captured_payloads = []
    _capture_initial_worker_task_create(
        monkeypatch,
        captured_payloads,
    )
    proxy = WorkerProxy(db_factory, relay=AsyncMock())
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=None)
    lock = proxy.task_operation_lock(stale_task.id)

    await lock.acquire()
    forward = asyncio.create_task(
        proxy.forward_task_to_worker(stale_task)
    )
    for _ in range(10):
        await asyncio.sleep(0)
    assert captured_payloads == []

    async with session_factory() as db:
        current = await db.get(Task, stale_task.id)
        current.enabled_skills = {"code-review": True}
        current.selected_user_skills = [user_skill_id]
        await db.commit()
    lock.release()
    await forward

    assert captured_payloads[0]["enabled_skills"] == {
        "code-review": True,
    }
    assert captured_payloads[0]["selected_user_skills"] == [user_skill_id]
    assert captured_payloads[0]["user_skill_snapshots"] == [{
        "id": user_skill_id,
        "name": "Reloaded forward skill",
        "description": "latest description",
        "content": "latest content",
    }]


async def test_worker_forward_rejects_generation_change_after_lock_wait(
    db_factory,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    stale_task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        turn_generation=1,
    )
    captured_payloads = []
    _capture_initial_worker_task_create(
        monkeypatch,
        captured_payloads,
    )
    proxy = WorkerProxy(db_factory, relay=AsyncMock())
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=None)
    lock = proxy.task_operation_lock(stale_task.id)

    await lock.acquire()
    forward = asyncio.create_task(
        proxy.forward_task_to_worker(stale_task)
    )
    for _ in range(10):
        await asyncio.sleep(0)
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == stale_task.id)
            .values(retry_count=Task.retry_count + 1)
        )
        await db.commit()
    lock.release()

    with pytest.raises(
        RuntimeError,
        match="generation changed before initial forwarding",
    ):
        await forward
    assert captured_payloads == []


async def test_initial_worker_forward_uses_skill_update_that_wins_claim_lock(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    """A pending Skill save that wins the fence must reach task creation."""

    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    async with session_factory() as db:
        user_skill = UserSkill(
            name="Initial dispatch skill",
            description="fresh description",
            content="fresh content",
        )
        db.add(user_skill)
        await db.commit()
        await db.refresh(user_skill)
        user_skill_id = user_skill.id
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        enabled_skills={"code-review": False},
        selected_user_skills=[],
    )

    captured_payloads = []
    _capture_initial_worker_task_create(
        monkeypatch,
        captured_payloads,
    )
    proxy = WorkerProxy(db_factory, relay=AsyncMock())
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=None)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    original_get_lock = worker_proxy_module.get_task_operation_lock
    lock = original_get_lock(task.id)
    await lock.acquire()
    claim_lock_requested = asyncio.Event()

    def observed_get_lock(task_id):
        claim_lock_requested.set()
        return original_get_lock(task_id)

    monkeypatch.setattr(
        worker_proxy_module,
        "get_task_operation_lock",
        observed_get_lock,
    )
    dispatch_request = asyncio.create_task(
        dispatcher._dispatch_worker_tasks()
    )
    await claim_lock_requested.wait()

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        current.enabled_skills = {"code-review": True}
        current.selected_user_skills = [user_skill_id]
        await db.commit()
    lock.release()

    await dispatch_request
    forward = dispatcher._running_tasks.get(f"worker-{task.id}")
    assert forward is not None
    await forward

    assert len(captured_payloads) == 1
    assert captured_payloads[0]["enabled_skills"] == {
        "code-review": True,
    }
    assert captured_payloads[0]["selected_user_skills"] == [user_skill_id]
    assert captured_payloads[0]["user_skill_snapshots"] == [{
        "id": user_skill_id,
        "name": "Initial dispatch skill",
        "description": "fresh description",
        "content": "fresh content",
    }]


async def test_initial_worker_forward_rejects_skill_update_after_claim(
    client,
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    """A Skill edit cannot change the Manager mirror after remote creation wins."""

    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        enabled_skills={"code-review": False},
    )
    post_entered = asyncio.Event()
    release_post = asyncio.Event()
    captured_payloads = []
    _capture_initial_worker_task_create(
        monkeypatch,
        captured_payloads,
        post_entered=post_entered,
        release_post=release_post,
    )
    proxy = WorkerProxy(db_factory, relay=AsyncMock())
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=None)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    forward = dispatcher._running_tasks.get(f"worker-{task.id}")
    assert forward is not None
    await post_entered.wait()

    update_request = asyncio.create_task(client.put(
        f"/api/tasks/{task.id}",
        json={"enabled_skills": {"code-review": True}},
    ))
    for _ in range(10):
        await asyncio.sleep(0)
    assert not update_request.done()

    release_post.set()
    await forward
    response = await update_request

    assert response.status_code == 409
    assert "execution claim" in response.text
    assert captured_payloads[0]["enabled_skills"] == {
        "code-review": False,
    }
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "in_progress"
    assert current.enabled_skills == {"code-review": False}


async def test_dispatch_worker_target_repo_fill_preserves_concurrent_project_edit(
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    async with session_factory() as db:
        original_project = Project(
            name="worker-original-project",
            local_path="/workspace/original",
            status="ready",
        )
        replacement_project = Project(
            name="worker-replacement-project",
            local_path="/workspace/replacement",
            status="ready",
        )
        db.add_all([original_project, replacement_project])
        await db.commit()
        await db.refresh(original_project)
        await db.refresh(replacement_project)
        original_project_id = original_project.id
        replacement_project_id = replacement_project.id
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        project_id=original_project_id,
        target_repo="",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    factory_calls = 0

    @asynccontextmanager
    async def racing_db_factory():
        nonlocal factory_calls
        factory_calls += 1
        async with session_factory() as db:
            if factory_calls == 4:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(
                        project_id=replacement_project_id,
                        target_repo="/workspace/user-choice",
                    )
                )
                await db.commit()
            yield db

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = racing_db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.project_id == replacement_project_id
    assert current.target_repo == "/workspace/user-choice"
    proxy.forward_task_to_worker.assert_not_awaited()
    assert broadcaster.sent == []


async def test_dispatch_forward_failure_marks_failed(db_factory, session_factory, broadcaster, monkeypatch):
    from backend.services.dispatcher import GlobalDispatcher
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id, status="pending")

    proxy = AsyncMock()
    proxy.forward_task_to_worker.side_effect = RuntimeError("boom")
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    disp = GlobalDispatcher.__new__(GlobalDispatcher)
    disp.db_factory = db_factory
    disp.broadcaster = broadcaster
    disp._running_tasks = {}

    await disp._dispatch_worker_tasks()
    # _safe_forward_to_worker 带 3 次指数退避重试（1s+2s），直接 await 转发
    # 任务跑完全部重试（done_callback 会 pop，所以先取引用）
    fwd = disp._running_tasks.get(f"worker-{t.id}")
    assert fwd is not None
    await fwd

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.status == "failed"
    assert "转发到 Worker 失败" in task.error_message


async def test_dispatch_uncertain_initial_forward_is_not_replayed_or_failed(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher
    from backend.services.worker_proxy import (
        WorkerTaskForwardOutcomeUncertainError,
    )

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    proxy = AsyncMock()
    proxy.forward_task_to_worker.side_effect = (
        WorkerTaskForwardOutcomeUncertainError("response lost after commit")
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster

    async with db_factory() as db:
        current = await db.get(Task, task.id)
        generation = dispatcher._task_status_generation(current)

    await dispatcher._safe_forward_to_worker(task, generation)

    async with db_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "in_progress"
    assert current.completed_at is None
    assert "outcome is uncertain" in current.error_message
    assert "automatic replay was blocked" in current.error_message
    proxy.forward_task_to_worker.assert_awaited_once()


async def test_old_worker_forward_failure_cannot_fail_reclaimed_generation(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    """The async forwarder is fenced to the claim created for that request."""

    import backend.services.dispatcher as dispatcher_module
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    proxy = AsyncMock()
    proxy.forward_task_to_worker.side_effect = RuntimeError("boom")
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(
        dispatcher_module.asyncio,
        "sleep",
        AsyncMock(),
    )

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    async with db_factory() as db:
        claimed = await db.get(Task, task.id)
        old_generation = dispatcher._task_status_generation(claimed)
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(retry_count=Task.retry_count + 1)
        )
        await db.commit()

    await dispatcher._safe_forward_to_worker(task, old_generation)

    async with db_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "in_progress"
        assert current.retry_count == old_generation.retry_count + 1
    assert not any(
        channel == "tasks" and payload.get("new_status") == "failed"
        for channel, payload in broadcaster.sent
    )


# === API 代理 ===


class _ProxyResponse:
    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if not 200 <= self.status_code < 300:
            raise RuntimeError(f"upstream HTTP {self.status_code}")
        return self


class _InvalidJSONProxyResponse(_ProxyResponse):
    def json(self):
        raise ValueError("invalid JSON")


def _install_proxy_transport(monkeypatch, outcome):
    requests = []
    outcomes = list(outcome) if isinstance(outcome, list) else None

    def next_outcome():
        if outcomes is not None:
            if not outcomes:
                raise AssertionError("unexpected extra Worker HTTP request")
            current = outcomes.pop(0)
        else:
            current = outcome
        if isinstance(current, BaseException):
            raise current
        return current

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self.timeout = kwargs.get("timeout")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, **kwargs):
            requests.append(("GET", url, kwargs))
            return next_outcome()

        async def request(self, method, url, **kwargs):
            requests.append((method, url, kwargs))
            return next_outcome()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", FakeAsyncClient)
    return requests


@pytest.mark.parametrize("remote_status", [401, 403])
async def test_generic_worker_proxy_hides_internal_auth_failures(
    session_factory, monkeypatch, remote_status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    requests = _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(remote_status, {"detail": "secret Worker auth diagnostic"}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(task, "POST", f"/api/tasks/{task.id}/retry")

    assert caught.value.status_code == 502
    assert "内部 Worker 认证失败" in caught.value.detail
    assert str(remote_status) in caught.value.detail
    assert "secret Worker auth diagnostic" not in caught.value.detail
    assert requests[0][2]["headers"] == {"Authorization": "Bearer wtoken"}


async def test_generic_worker_proxy_can_require_json_confirmation(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(
        monkeypatch,
        _InvalidJSONProxyResponse(200, "not-json"),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(
            task,
            "DELETE",
            f"/api/tasks/{task.id}",
            require_json=True,
        )

    assert caught.value.status_code == 502
    assert "invalid confirmation" in caught.value.detail


@pytest.mark.parametrize(
    "outcome",
    [
        httpx.ReadTimeout("response lost"),
        _ProxyResponse(500, {"detail": "post-boundary failure"}),
        _InvalidJSONProxyResponse(200, "not-json"),
    ],
)
async def test_worker_terminal_proxy_surfaces_post_boundary_uncertainty(
    session_factory,
    monkeypatch,
    outcome,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(monkeypatch, outcome)
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(
        worker_proxy_module.WorkerTaskMutationOutcomeUncertainError
    ):
        await proxy.proxy_to_worker(
            task,
            "POST",
            f"/api/tasks/{task.id}/cancel",
            require_json=True,
            quarantine_on_transport_uncertainty=True,
        )


async def test_worker_proxy_marks_manager_confirmed_terminal_pr_chat(
    session_factory,
    monkeypatch,
):
    from backend.services.pr_review_runtime import (
        PR_REVIEW_TERMINAL_CHAT_HEADER,
        PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE,
    )

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        tags=["pr-review"],
    )
    requests = _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(200, {"ok": True}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    await proxy.proxy_to_worker(
        task,
        "POST",
        f"/api/tasks/{task.id}/chat",
        pr_review_terminal_chat=True,
    )

    assert requests[0][2]["headers"] == {
        "Authorization": "Bearer wtoken",
        PR_REVIEW_TERMINAL_CHAT_HEADER:
        PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE,
    }


@pytest.mark.parametrize(
    ("config", "expected_status"),
    [
        ({"pr_review_terminal_chat_version": 1}, None),
        ({}, 409),
    ],
)
async def test_worker_terminal_pr_chat_capability_preflight(
    session_factory,
    monkeypatch,
    config,
    expected_status,
):
    worker = await _mk_worker(session_factory)

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return config

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url, *, headers):
            return Response()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(session_factory, AsyncMock())

    if expected_status is None:
        await proxy.require_terminal_pr_review_chat_support(worker)
    else:
        with pytest.raises(HTTPException) as caught:
            await proxy.require_terminal_pr_review_chat_support(worker)
        assert caught.value.status_code == expected_status
        assert "升级" in caught.value.detail


async def test_generic_worker_proxy_can_confirm_task_already_absent(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(404, {"detail": "Task not found"}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    result = await proxy.proxy_to_worker(
        task,
        "DELETE",
        f"/api/tasks/{task.id}",
        require_json=True,
        allow_task_absent=True,
    )

    assert result == {"ok": True, "already_deleted": True}


async def test_generic_worker_proxy_can_surface_exact_endpoint_404(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(404, {"detail": "Not Found"}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(WorkerEndpointNotFoundError):
        await proxy.proxy_to_worker(
            task,
            "GET",
            f"/api/tasks/{task.id}/routing-config/status",
            require_json=True,
            surface_endpoint_not_found=True,
        )


@pytest.mark.parametrize(
    ("transport_error", "expected_status", "expected_detail"),
    [
        (httpx.ConnectError("private address unreachable"), 502, "Worker 网关连接失败"),
        (httpx.ReadTimeout("Worker stalled"), 503, "Worker w1 请求超时"),
    ],
)
async def test_generic_worker_proxy_maps_transport_failures(
    session_factory,
    monkeypatch,
    transport_error,
    expected_status,
    expected_detail,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(monkeypatch, transport_error)
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(task, "POST", f"/api/tasks/{task.id}/retry")

    assert caught.value.status_code == expected_status
    assert expected_detail in caught.value.detail
    assert str(transport_error) not in caught.value.detail


@pytest.mark.parametrize("remote_status", [302, 400, 404, 429, 500, 503])
async def test_generic_worker_proxy_hides_other_upstream_error_bodies(
    session_factory, monkeypatch, remote_status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(remote_status, {"detail": "sensitive Worker traceback"}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(task, "POST", f"/api/tasks/{task.id}/retry")

    assert caught.value.status_code == 502
    assert f"远端 HTTP {remote_status}" in caught.value.detail
    assert "sensitive Worker traceback" not in caught.value.detail


async def test_create_task_with_worker_id_and_explicit_id(
    client,
    session_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    resp = await client.post("/api/tasks", json={
        "id": 4242,
        "source_incarnation_id": "a" * 32,
        "source_retry_count": 0,
        "source_turn_generation": 1,
        "title": "x",
        "description": "remote",
        "execution_user_id": None,
        "execution_user_role": "member",
        "execution_mode": "sandbox",
        "execution_principal_kind": "system",
    })
    assert resp.status_code in (200, 201), resp.text
    data = resp.json()
    assert data["id"] == 4242
    assert data["worker_id"] is None
    assert data["status"] == "pending"
    assert data["turn_generation"] == 0


async def test_worker_routing_config_update_confirms_remote_before_manager(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()

    async def protocol(_task, method, path, body=None, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        if path.endswith("/stage"):
            return _routing_snapshot(task, pending=dict(body))
        assert path.endswith("/ack")
        return _routing_snapshot(
            task,
            codex_service_tier="priority",
        )

    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "priority"
    assert proxy.proxy_to_worker.await_count == 3
    stage = proxy.proxy_to_worker.await_args_list[1]
    assert stage.args[1:3] == (
        "POST",
        f"/api/tasks/{task.id}/routing-config/stage",
    )
    assert stage.args[3] | {"op_id": "<ignored>"} == {
        "op_id": "<ignored>",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }
    assert stage.kwargs["require_json"] is True
    assert stage.kwargs["operation_lock_held"] is True
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.codex_service_tier == "priority"


async def test_worker_routing_config_updates_model_and_disables_fast_atomically(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    proxy = AsyncMock()

    async def protocol(_task, method, path, body=None, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        if path.endswith("/stage"):
            return _routing_snapshot(task, pending=dict(body))
        assert path.endswith("/ack")
        return _routing_snapshot(
            task,
            model="gpt-5.4-mini",
            codex_service_tier="default",
        )

    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={
            "model": "gpt-5.4-mini",
            "codex_service_tier": "default",
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["model"] == "gpt-5.4-mini"
    assert response.json()["codex_service_tier"] == "default"
    assert proxy.proxy_to_worker.await_count == 3
    stage_payload = proxy.proxy_to_worker.await_args_list[1].args[3]
    assert {
        key: value
        for key, value in stage_payload.items()
        if key != "op_id"
    } == {
        "model": "gpt-5.4-mini",
        "codex_service_tier": "default",
        "provider": "codex",
    }


async def test_worker_routing_config_rejects_mixed_routing_and_other_fields(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={
            "title": "must-not-change",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    assert "save other fields separately" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.title == "t"
    assert current.codex_service_tier == "default"


@pytest.mark.parametrize(
    ("field", "remote_value"),
    (
        ("id", -1),
        ("provider", "claude"),
        ("model", "gpt-5.6-terra"),
        ("codex_service_tier", "default"),
    ),
)
async def test_worker_routing_config_rejects_unconfirmed_remote_snapshot(
    client,
    session_factory,
    monkeypatch,
    field,
    remote_value,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    remote = {
        "id": task.id,
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }
    remote[field] = remote_value
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = remote
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 502
    assert "invalid routing synchronization snapshot" in response.json()["detail"]
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.model == "gpt-5.6-sol"
    assert current.codex_service_tier == "default"


async def test_worker_routing_config_remote_failure_keeps_manager_standard(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = HTTPException(
        502,
        "Worker rejected config",
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 502
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.codex_service_tier == "default"


async def test_worker_routing_config_relay_active_change_keeps_worker_blocked(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )

    remote_pending = None

    async def stage_after_relay_event(
        _task,
        method,
        path,
        body=None,
        **_kwargs,
    ):
        nonlocal remote_pending
        if method == "GET":
            return _routing_snapshot(task)
        assert path.endswith("/stage")
        remote_pending = dict(body)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(status="executing")
            )
            await db.commit()
        return _routing_snapshot(task, pending=remote_pending)

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = stage_after_relay_event
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409, response.text
    assert "remains safely blocked" in response.json()["detail"]
    assert remote_pending is not None
    assert proxy.proxy_to_worker.await_count == 2
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "executing"
    assert current.codex_service_tier == "default"


@pytest.mark.parametrize("status", ("pending", "in_progress", "executing"))
async def test_worker_routing_config_rejects_forwarding_or_active_generation(
    client,
    session_factory,
    monkeypatch,
    status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status=status,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409
    assert "pending or active" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.codex_service_tier == "default"


async def test_worker_routing_config_rereads_assignment_after_operation_barrier(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    class MigrationGate:
        async def __aenter__(self):
            entered.set()
            await release.wait()

        async def __aexit__(self, *_args):
            return False

    monkeypatch.setattr(
        tasks_api_module,
        "get_task_operation_lock",
        lambda _task_id: MigrationGate(),
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    request = asyncio.create_task(
        client.put(
            f"/api/tasks/{task.id}",
            json={"codex_service_tier": "priority"},
        )
    )
    await entered.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(worker_id=None)
        )
        await db.commit()
    release.set()
    response = await request

    assert response.status_code == 409
    assert "assignment changed" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id is None
    assert current.codex_service_tier == "default"


async def test_worker_local_stage_readback_and_ack_are_atomic(
    client,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        worker_id=None,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
        metadata_={"keep": "yes"},
    )
    payload = {
        "op_id": "stage-standard-to-fast",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }

    staged = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json=payload,
    )

    assert staged.status_code == 200, staged.text
    assert staged.json()["codex_service_tier"] == "default"
    assert staged.json()["pending"] == payload
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"
        assert current.metadata_["keep"] == "yes"
        assert (
            current.metadata_[WORKER_ROUTING_PENDING_KEY]["op_id"]
            == payload["op_id"]
        )

    readback = await client.get(
        f"/api/tasks/{task.id}/routing-config/status"
    )
    assert readback.status_code == 200
    assert readback.json() == staged.json()

    acked = await client.post(
        f"/api/tasks/{task.id}/routing-config/ack",
        json=payload,
    )
    assert acked.status_code == 200, acked.text
    assert acked.json()["codex_service_tier"] == "priority"
    assert acked.json()["pending"] is None

    # A lost ack response may be retried exactly.
    duplicate = await client.post(
        f"/api/tasks/{task.id}/routing-config/ack",
        json=payload,
    )
    assert duplicate.status_code == 200
    assert duplicate.json() == acked.json()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "priority"
        assert current.metadata_ == {"keep": "yes"}


async def test_worker_stage_holds_codex_thread_guard_through_marker_commit(
    client,
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        worker_id=None,
        status="completed",
        session_id="native-thread-guarded",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    guard_exited = False

    @asynccontextmanager
    async def routing_guard(_home, thread_id):
        nonlocal guard_exited
        assert thread_id == "native-thread-guarded"
        async with session_factory() as db:
            before = await db.get(Task, task.id)
            assert WORKER_ROUTING_PENDING_KEY not in (before.metadata_ or {})
        yield {"thread": {"status": {"type": "idle"}}, "goal": None}
        async with session_factory() as db:
            staged = await db.get(Task, task.id)
            assert staged.codex_service_tier == "default"
            assert WORKER_ROUTING_PENDING_KEY in staged.metadata_
        guard_exited = True

    monkeypatch.setattr(
        main_module.instance_manager,
        "codex_thread_routing_guard",
        routing_guard,
    )
    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "guarded-standard-to-fast",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "default"
    assert response.json()["pending"]["codex_service_tier"] == "priority"
    assert guard_exited


@pytest.mark.parametrize("status", ("pending", "in_progress", "executing"))
async def test_worker_local_stage_atomically_rejects_active_status(
    client,
    session_factory,
    status,
):
    task = await _mk_task(
        session_factory,
        status=status,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": f"active-{status}",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"
        assert WORKER_ROUTING_PENDING_KEY not in (current.metadata_ or {})


async def test_worker_pending_marker_blocks_direct_retry_chat_and_plan_approve(
    client,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    payload = {
        "op_id": "block-direct-turns",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }
    staged = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json=payload,
    )
    assert staged.status_code == 200

    retry = await client.post(f"/api/tasks/{task.id}/retry")
    chat = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "must remain queued nowhere"},
    )
    assert retry.status_code == 409
    assert chat.status_code == 409
    async with session_factory() as db:
        logs = list(
            (
                await db.execute(
                    select(LogEntry).where(LogEntry.task_id == task.id)
                )
            ).scalars()
        )
    assert logs == []

    plan = await _mk_task(
        session_factory,
        status="plan_review",
        mode="plan",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    staged_plan = await client.post(
        f"/api/tasks/{plan.id}/routing-config/stage",
        json={
            **payload,
            "op_id": "block-plan-approve",
            "model": "gpt-5.5",
            "codex_service_tier": "default",
        },
    )
    assert staged_plan.status_code == 200
    approve = await client.post(f"/api/tasks/{plan.id}/plan/approve")
    assert approve.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, plan.id)
        assert current.status == "plan_review"
        assert not current.plan_approved


async def test_worker_stage_rejects_running_ccm_sub_agent(
    client,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    async with session_factory() as db:
        db.add(
            MonitorSession(
                task_id=task.id,
                agent_type="sub_agent",
                source="ccm",
                description="delayed child",
                status="running",
            )
        )
        await db.commit()

    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "child-running",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    assert "sub-agent is running" in response.json()["detail"]
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert WORKER_ROUTING_PENDING_KEY not in (current.metadata_ or {})


async def test_worker_stage_rejects_unsettled_main_instance_generation(
    client,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    async with session_factory() as db:
        instance = Instance(
            name="still-running-old-standard",
            status="running",
            pid=987654,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        current = await db.get(Task, task.id)
        current.instance_id = instance.id
        await db.commit()

    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "must-wait-for-old-turn",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    assert "Instance generation" in response.json()["detail"]
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"
        assert WORKER_ROUTING_PENDING_KEY not in (current.metadata_ or {})


async def test_worker_stage_rejects_unsettled_preowner_launch_reservation(
    client,
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    async with session_factory() as db:
        instance = Instance(name="preowner-reservation", status="idle")
        db.add(instance)
        await db.flush()
        current = await db.get(Task, task.id)
        current.instance_id = instance.id
        await db.commit()
        instance_id = instance.id

    barrier = AsyncMock(
        side_effect=HTTPException(
            409,
            "pre-owner process launch could not be proven stopped",
        )
    )
    monkeypatch.setattr(
        tasks_api_module,
        "_settle_task_launch_barrier",
        barrier,
    )

    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "hidden-preowner",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    barrier.assert_awaited_once_with(task.id, instance_id)
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"
        assert WORKER_ROUTING_PENDING_KEY not in (current.metadata_ or {})


async def test_worker_routing_stage_timeout_leaves_remote_blocked_and_manager_old(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    remote_pending = None

    async def lose_stage_response(
        _task,
        method,
        path,
        body=None,
        **_kwargs,
    ):
        nonlocal remote_pending
        if method == "GET":
            return _routing_snapshot(task)
        assert path.endswith("/stage")
        remote_pending = dict(body)
        raise HTTPException(503, "stage response lost")

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = lose_stage_response
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 503
    assert remote_pending is not None
    assert proxy.proxy_to_worker.await_count == 2
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"


async def test_worker_routing_lost_ack_response_converges_by_readback(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    state = {
        "tier": "default",
        "pending": None,
        "ack_lost": False,
    }

    async def protocol(_task, method, path, body=None, **_kwargs):
        if method == "GET":
            return _routing_snapshot(
                task,
                codex_service_tier=state["tier"],
                pending=state["pending"],
            )
        if path.endswith("/stage"):
            state["pending"] = dict(body)
            return _routing_snapshot(task, pending=state["pending"])
        assert path.endswith("/ack")
        state["tier"] = body["codex_service_tier"]
        state["pending"] = None
        state["ack_lost"] = True
        raise HTTPException(503, "ack response lost")

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "priority"
    assert state == {
        "tier": "priority",
        "pending": None,
        "ack_lost": True,
    }
    assert proxy.proxy_to_worker.await_count == 4


async def test_worker_fast_to_standard_returns_authoritative_after_unreadable_ack(
    client,
    session_factory,
    monkeypatch,
):
    """Post-commit uncertainty must not leave the UI showing stale Fast."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    remote_pending = None
    get_count = 0

    async def protocol(_task, method, path, body=None, **_kwargs):
        nonlocal remote_pending, get_count
        if method == "GET":
            get_count += 1
            if get_count == 1:
                return _routing_snapshot(
                    task,
                    codex_service_tier="priority",
                )
            raise HTTPException(503, "ack readback unavailable")
        if path.endswith("/stage"):
            remote_pending = dict(body)
            return _routing_snapshot(
                task,
                codex_service_tier="priority",
                pending=remote_pending,
            )
        assert path.endswith("/ack")
        raise HTTPException(503, "ack unavailable")

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "default"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "default"
    assert remote_pending is not None
    assert remote_pending["codex_service_tier"] == "default"
    assert proxy.proxy_to_worker.await_count == 4
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"


async def test_worker_execution_reconciles_orphan_stage_to_manager_tuple(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
        metadata_={"ccm_worker_remote_materialized_v1": True},
    )
    pending = {
        "op_id": "orphan-fast-stage",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }

    async def protocol(_task, method, path, body=None, **_kwargs):
        nonlocal pending
        if method == "GET":
            return _routing_snapshot(task, pending=pending)
        if path.endswith("/reconcile"):
            assert body["op_id"] == "orphan-fast-stage"
            assert body["codex_service_tier"] == "default"
            pending = None
            return _routing_snapshot(task)
        assert path.endswith("/internal/worker-retry")
        return _worker_manual_retry_response(task, body)

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/retry")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    assert pending is None
    assert proxy.proxy_to_worker.await_count == 3


async def test_worker_retry_revalidates_jwt_user_before_prepared_outbox(
    client,
    session_factory,
    monkeypatch,
):
    from backend import database
    from backend.api.auth import create_jwt
    from backend.models.user import User

    monkeypatch.setattr(database, "async_session", session_factory)

    async with session_factory() as db:
        admin = User(
            email="worker-retry-demotion@example.com",
            name="Worker retry admin",
            password_hash="unused",
            role="admin",
            is_active=True,
        )
        db.add(admin)
        await db.commit()
        await db.refresh(admin)
        admin_id = admin.id
        admin_token = create_jwt(admin)

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
        created_by=admin_id,
        metadata_={"ccm_worker_remote_materialized_v1": True},
    )
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.proxy_to_worker.return_value = _routing_snapshot(task)

    async def demote_after_protocol_preflight(_worker):
        async with session_factory() as db:
            current = await db.get(User, admin_id)
            current.role = "member"
            await db.commit()

    proxy.require_worker_manual_retry_support.side_effect = (
        demote_after_protocol_preflight
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(
        f"/api/tasks/{task.id}/retry",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert response.status_code == 409, response.text
    assert "changed role" in response.json()["detail"]
    assert proxy.proxy_to_worker.await_count == 1
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "failed"
    assert (
        worker_relay_module.worker_manual_retry_receipt(current.metadata_)
        is None
    )


@pytest.mark.parametrize(
    ("status", "mode", "action_path", "retry_delta"),
    [
        pytest.param(
            "completed",
            "auto",
            "retry",
            1,
            id="retry",
        ),
        pytest.param(
            "plan_review",
            "plan",
            "plan/approve",
            0,
            id="plan-approve",
        ),
    ],
)
async def test_worker_execution_admission_syncs_latest_manager_skills(
    client,
    session_factory,
    monkeypatch,
    status,
    mode,
    action_path,
    retry_delta,
):
    worker = await _mk_worker(session_factory)
    async with session_factory() as db:
        user_skill = UserSkill(
            name=f"Final {action_path} skill",
            description="Manager-authoritative description",
            content="Manager-authoritative content",
        )
        db.add(user_skill)
        await db.commit()
        await db.refresh(user_skill)
        user_skill_id = user_skill.id

    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status=status,
        mode=mode,
        plan_content="approved plan" if mode == "plan" else None,
        enabled_skills={"code-review": False},
        selected_user_skills=[],
        metadata_={"ccm_worker_remote_materialized_v1": True},
    )
    admission_order = []
    worker_skill_payloads = []

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "id": task.id,
                "incarnation_id": task.incarnation_id,
                "status": task.status,
                "retry_count": task.retry_count,
                "turn_generation": task.turn_generation,
                "instance_id": task.instance_id,
                "enabled_skills": self.payload["enabled_skills"],
                "selected_user_skills": self.payload[
                    "selected_user_skills"
                ],
                "metadata_": {
                    "ccm_user_skill_snapshots": self.payload[
                        "user_skill_snapshots"
                    ],
                },
            }

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, *, headers):
            return _ProxyResponse(
                200,
                {"worker_task_incarnation_proxy_version": 1},
            )

        async def put(self, _url, *, headers, json):
            assert headers["X-CCM-Task-Incarnation"] == task.incarnation_id
            admission_order.append("skills")
            worker_skill_payloads.append(json)
            return Response(json)

    async def protocol(current, method, path, body=None, **_kwargs):
        if method == "GET":
            if path.endswith("/routing-config/status"):
                return _routing_snapshot(current)
            assert mode == "plan"
            assert "/internal/worker-plan-decisions/" in path
            return {
                "protocol_version": 1,
                "state": "absent",
                "operation_id": body["operation_id"] if body else path.rsplit("/", 1)[-1],
                "task_id": current.id,
            }
        if mode == "plan":
            assert method == "PUT"
            assert "/internal/worker-plan-decisions/" in path
            assert admission_order == []
            admission_order.append(action_path)
            remote = _remote_task(
                current,
                status="completed",
                completed_at=datetime.utcnow().isoformat(),
                plan_approved=True,
            )
            receipt = {
                "protocol_version": 1,
                "side": "worker",
                "state": "applied",
                "action": "approve",
                "operation_id": body["operation_id"],
                "request_digest": body["request_digest"],
                "request": body,
                "result_generation": {
                    "task_id": current.id,
                    "incarnation_id": current.incarnation_id,
                    "status": "completed",
                    "retry_count": current.retry_count,
                    "turn_generation": current.turn_generation,
                },
                "applied_at": datetime.utcnow().isoformat(),
            }
            return {"task": remote, "receipt": receipt}
        assert method == "POST"
        assert path.endswith(
            "/internal/worker-retry"
            if action_path == "retry"
            else f"/{action_path}"
        )
        assert admission_order[-1] == "skills"
        admission_order.append(action_path)
        if action_path == "retry":
            return _worker_manual_retry_response(current, body)
        return _remote_task(
            current,
            status="completed",
            retry_count=current.retry_count + retry_delta,
            completed_at=datetime.utcnow().isoformat(),
            plan_approved=True,
        )

    proxy = WorkerProxy(session_factory, relay=AsyncMock())
    proxy.proxy_to_worker = AsyncMock(side_effect=protocol)
    proxy.require_worker_delegated_principal_support = AsyncMock()
    proxy.require_worker_manual_retry_support = AsyncMock()
    monkeypatch.setattr(
        worker_proxy_module.httpx,
        "AsyncClient",
        Client,
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    updated = await client.put(
        f"/api/tasks/{task.id}",
        json={
            "enabled_skills": {"code-review": True},
            "selected_user_skills": [user_skill_id],
        },
    )
    assert updated.status_code == 200, updated.text

    response = await client.post(f"/api/tasks/{task.id}/{action_path}")

    assert response.status_code == 200, response.text
    if mode == "plan":
        assert response.json()["status"] == "completed"
        assert admission_order == [action_path]
        assert worker_skill_payloads == []
    else:
        assert response.json()["status"] == "pending"
        assert admission_order == ["skills", action_path]
        assert len(worker_skill_payloads) == 1
        assert worker_skill_payloads[0]["enabled_skills"] == {
            "code-review": True,
        }
        assert worker_skill_payloads[0]["selected_user_skills"] == [
            user_skill_id,
        ]
        assert worker_skill_payloads[0]["user_skill_snapshots"] == [{
            "id": user_skill_id,
            "name": f"Final {action_path} skill",
            "description": "Manager-authoritative description",
            "content": "Manager-authoritative content",
        }]


@pytest.mark.parametrize(
    ("source_status", "action_path"),
    [
        pytest.param("completed", "retry", id="retry"),
        pytest.param("completed", "chat", id="chat"),
    ],
)
async def test_migrated_inert_task_can_start_its_next_worker_turn(
    client,
    db_factory,
    session_factory,
    monkeypatch,
    source_status,
    action_path,
):
    """Migration and the next admission agree on one remote generation."""

    from backend.services.task_migrator import TaskMigrator

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        status=source_status,
        mode="auto",
        instance_id=444,
        enabled_skills={"code-review": True},
    )
    relay = AsyncMock()
    remote: dict = {}
    imported_statuses = []
    skill_payloads = []

    class Response:
        text = ""

        def __init__(self, payload, status_code=200):
            self.payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            return None

        def json(self):
            return dict(self.payload)

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, url, *, headers, json):
            if url.endswith("/api/tasks/migration-import/commit"):
                reservation = remote["metadata_"][
                    WORKER_MIGRATION_IMPORT_RESERVATION_KEY
                ]
                assert json == {"task_id": task.id, **reservation}
                return Response({
                    "ok": True,
                    "committed": True,
                    "task_id": task.id,
                    "operation_id": reservation["operation_id"],
                    "operation_sequence": reservation["operation_sequence"],
                })
            if url.endswith("/api/tasks/migration-import/rollback"):
                reservation = remote["metadata_"][
                    WORKER_MIGRATION_IMPORT_RESERVATION_KEY
                ]
                assert json == {"task_id": task.id, **reservation}
                return Response({
                    "ok": True,
                    "removed": True,
                    "task_id": task.id,
                    "operation_id": reservation["operation_id"],
                    "operation_sequence": reservation["operation_sequence"],
                })
            assert url.endswith("/api/tasks/migration-import")
            imported_statuses.append(json["source_status"])
            reservation = {
                "operation_id": json["migration_operation_id"],
                "operation_sequence": json["migration_operation_sequence"],
                "incarnation_id": json["source_incarnation_id"],
                "retry_count": json["retry_count"],
                "turn_generation": json["turn_generation"],
                "source_status": json["source_status"],
            }
            remote.update({
                "id": json["id"],
                "incarnation_id": json["source_incarnation_id"],
                "status": json["source_status"],
                "retry_count": json["retry_count"],
                "turn_generation": json["turn_generation"],
                "instance_id": None,
                "enabled_skills": json["enabled_skills"],
                "selected_user_skills": json["selected_user_skills"],
                "metadata_": {
                    "ccm_user_skill_snapshots": json[
                        "user_skill_snapshots"
                    ],
                    WORKER_MIGRATION_IMPORT_RESERVATION_KEY: reservation,
                },
                    "provider": json["provider"],
                    "model": json["model"],
                    "codex_service_tier": json["codex_service_tier"],
                    "execution_user_id": json["execution_user_id"],
                    "execution_user_role": json["execution_user_role"],
                    "execution_mode": json["execution_mode"],
                    "execution_principal_kind": json[
                        "execution_principal_kind"
                    ],
            })
            return Response(remote, status_code=201)

        async def put(self, url, *, headers, json):
            assert url.endswith(f"/api/tasks/{task.id}")
            skill_payloads.append(json)
            remote["enabled_skills"] = json["enabled_skills"]
            remote["selected_user_skills"] = json["selected_user_skills"]
            remote["metadata_"]["ccm_user_skill_snapshots"] = json[
                "user_skill_snapshots"
            ]
            return Response(remote)

    async def worker_protocol(current, method, path, body=None, **_kwargs):
        if method == "GET":
            assert path.endswith("/routing-config/status")
            return _routing_snapshot(
                current,
                status=remote["status"],
            )
        assert method == "POST"
        if action_path == "retry":
            assert path.endswith("/internal/worker-retry")
            assert remote["status"] == "completed"
            remote.update(
                status="pending",
                retry_count=remote["retry_count"] + 1,
                instance_id=None,
            )
            return _worker_manual_retry_response(current, body)
        assert path.endswith("/chat")
        assert remote["status"] == "completed"
        return {
            "ok": True,
            "queued": True,
            "session_id": "migrated-session",
        }

    monkeypatch.setattr(
        worker_proxy_module.httpx,
        "AsyncClient",
        Client,
    )
    proxy = WorkerProxy(db_factory, relay=relay)
    proxy.ensure_worker_project = AsyncMock(return_value=17)
    proxy.proxy_to_worker = AsyncMock(side_effect=worker_protocol)
    proxy.require_worker_delegated_principal_support = AsyncMock()
    proxy.require_worker_task_incarnation_support = AsyncMock()
    proxy.require_worker_migration_import_support = AsyncMock()
    proxy.require_worker_manual_retry_support = AsyncMock()
    migrator = TaskMigrator(
        db_factory=db_factory,
        relay=relay,
        broadcaster=None,
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    migrated = await client.put(
        f"/api/tasks/{task.id}",
        json={"worker_id": worker.id},
    )

    assert migrated.status_code == 200, migrated.text
    assert migrated.json()["status"] == source_status
    assert migrated.json()["worker_id"] == worker.id
    assert imported_statuses == [source_status]
    assert remote["instance_id"] is None

    if action_path == "chat":
        response = await client.post(
            f"/api/tasks/{task.id}/chat",
            json={"message": "continue after migration"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["queued"] is True
    else:
        response = await client.post(
            f"/api/tasks/{task.id}/{action_path}",
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == (
            "completed" if action_path == "plan/approve" else "pending"
        )

    if action_path == "plan/approve":
        assert skill_payloads == []
    else:
        assert len(skill_payloads) == 1
        assert skill_payloads[0]["enabled_skills"] == {
            "code-review": True,
        }


@pytest.mark.parametrize("status", ["pending", "completed"])
async def test_worker_skill_update_shares_execution_admission_lock(
    client,
    session_factory,
    monkeypatch,
    status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status=status,
        enabled_skills={"code-review": False},
    )
    update_entered = asyncio.Event()
    original_update = tasks_api_module.TaskQueue.update_task

    async def observed_update(self, task_id, **updates):
        update_entered.set()
        return await original_update(self, task_id, **updates)

    monkeypatch.setattr(
        tasks_api_module.TaskQueue,
        "update_task",
        observed_update,
    )
    lock = worker_proxy_module.get_task_operation_lock(task.id)
    async with lock:
        pending = asyncio.create_task(client.put(
            f"/api/tasks/{task.id}",
            json={"enabled_skills": {"code-review": True}},
        ))
        for _ in range(10):
            await asyncio.sleep(0)
        assert not update_entered.is_set()

    response = await pending

    assert response.status_code == 200, response.text
    assert update_entered.is_set()
    assert response.json()["enabled_skills"]["code-review"] is True


async def test_worker_standard_execution_accepts_matching_legacy_routing(
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    paths = []

    async def legacy_protocol(_task, method, path, _body=None, **kwargs):
        paths.append(path)
        assert method == "GET"
        if path.endswith("/routing-config/status"):
            assert kwargs["surface_endpoint_not_found"] is True
            raise WorkerEndpointNotFoundError(path)
        assert path == f"/api/tasks/{task.id}"
        assert kwargs["require_task_incarnation_fence"] is True
        return {
            "id": task.id,
            "status": task.status,
            "provider": "codex",
            "model": "gpt-5.6-sol",
            # A pre-Fast Worker has no codex_service_tier response field.
        }

    monkeypatch.setattr(tasks_api_module, "_proxy", legacy_protocol)

    snapshot = await tasks_api_module._ensure_worker_routing_ready(
        task,
        operation_lock_held=True,
    )

    assert snapshot.provider == "codex"
    assert snapshot.model == "gpt-5.6-sol"
    assert snapshot.codex_service_tier == "default"
    assert snapshot.pending is None
    assert paths == [
        f"/api/tasks/{task.id}/routing-config/status",
        f"/api/tasks/{task.id}",
    ]


async def test_worker_fast_execution_rejects_legacy_routing(
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    paths = []

    async def legacy_protocol(_task, _method, path, _body=None, **_kwargs):
        paths.append(path)
        if path.endswith("/routing-config/status"):
            raise WorkerEndpointNotFoundError(path)
        return {
            "id": task.id,
            "status": task.status,
            "provider": "codex",
            "model": "gpt-5.6-sol",
        }

    monkeypatch.setattr(tasks_api_module, "_proxy", legacy_protocol)

    with pytest.raises(HTTPException) as caught:
        await tasks_api_module._ensure_worker_routing_ready(
            task,
            operation_lock_held=True,
        )

    assert caught.value.status_code == 409
    assert "cannot confirm Codex Fast" in caught.value.detail
    assert paths == [
        f"/api/tasks/{task.id}/routing-config/status",
        f"/api/tasks/{task.id}",
    ]


async def test_worker_standard_execution_rejects_mismatched_legacy_routing(
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )

    async def legacy_protocol(_task, _method, path, _body=None, **_kwargs):
        if path.endswith("/routing-config/status"):
            raise WorkerEndpointNotFoundError(path)
        return {
            "id": task.id,
            "status": task.status,
            "provider": "codex",
            "model": "gpt-5.6-terra",
        }

    monkeypatch.setattr(tasks_api_module, "_proxy", legacy_protocol)

    with pytest.raises(HTTPException) as caught:
        await tasks_api_module._ensure_worker_routing_ready(
            task,
            operation_lock_held=True,
        )

    assert caught.value.status_code == 409
    assert "does not exactly match" in caught.value.detail


async def test_worker_execution_does_not_downgrade_non_404_routing_failure(
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock(
        side_effect=HTTPException(
            502,
            "Worker 上游请求失败（远端 HTTP 500）",
        )
    )
    monkeypatch.setattr(tasks_api_module, "_proxy", proxy)

    with pytest.raises(HTTPException) as caught:
        await tasks_api_module._ensure_worker_routing_ready(
            task,
            operation_lock_held=True,
        )

    assert caught.value.status_code == 502
    assert "远端 HTTP 500" in caught.value.detail
    proxy.assert_awaited_once()


async def test_worker_routing_update_does_not_use_legacy_protocol(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    paths = []

    async def old_worker(_task, _method, path, _body=None, **kwargs):
        paths.append(path)
        assert kwargs.get("surface_endpoint_not_found", False) is False
        raise HTTPException(
            502,
            "Worker 上游请求失败（远端 HTTP 404）",
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = old_worker
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 502
    assert paths == [f"/api/tasks/{task.id}/routing-config/status"]


async def test_worker_routing_update_finishes_ack_before_propagating_cancel(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    ack_started = asyncio.Event()
    release_ack = asyncio.Event()
    state = {"tier": "default", "pending": None}

    async def protocol(_task, method, path, body=None, **_kwargs):
        if method == "GET":
            return _routing_snapshot(
                task,
                codex_service_tier=state["tier"],
                pending=state["pending"],
            )
        if path.endswith("/stage"):
            state["pending"] = dict(body)
            return _routing_snapshot(task, pending=state["pending"])
        assert path.endswith("/ack")
        ack_started.set()
        await release_ack.wait()
        state["tier"] = "priority"
        state["pending"] = None
        return _routing_snapshot(
            task,
            codex_service_tier="priority",
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    request = asyncio.create_task(
        client.put(
            f"/api/tasks/{task.id}",
            json={"codex_service_tier": "priority"},
        )
    )
    await asyncio.wait_for(ack_started.wait(), timeout=1)
    request.cancel()
    await asyncio.sleep(0)
    assert state["pending"] is not None
    release_ack.set()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert state == {"tier": "priority", "pending": None}
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "priority"


async def test_worker_execution_fails_closed_on_unmarked_tuple_divergence(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = _routing_snapshot(
        task,
        codex_service_tier="priority",
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/retry")

    assert response.status_code == 409
    assert "execution was blocked" in response.json()["detail"]
    proxy.proxy_to_worker.assert_awaited_once()


async def test_proxy_terminal_response_commits_normalized_generation_then_publishes(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        error_message="stale error",
    )
    protocol, _state = _durable_terminal_protocol(
        task,
        terminal_status="completed",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    local_broadcaster = FakeBroadcaster()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "completed"
    assert response.json()["completed_at"] is not None
    assert response.json()["error_message"] is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.completed_at is not None
    assert current.error_message is None
    assert local_broadcaster.sent == [
        (
            "tasks",
            {
                "event": "status_change",
                "task_id": task.id,
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation,
                "new_status": "completed",
                "background_active": False,
            },
        )
    ]


async def test_worker_terminal_receipt_preserves_manager_native_principal(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        execution_user_id=77,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
    )
    protocol, _state = _durable_terminal_protocol(
        task,
        terminal_status="cancelled",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "cancelled"
    assert {
        "execution_user_id": current.execution_user_id,
        "execution_user_role": current.execution_user_role,
        "execution_mode": current.execution_mode,
        "execution_principal_kind": current.execution_principal_kind,
    } == {
        "execution_user_id": 77,
        "execution_user_role": "admin",
        "execution_mode": "unrestricted",
        "execution_principal_kind": "user",
    }


@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    [
        ("cancel", "cancelled"),
        ("stop-session", "completed"),
    ],
)
async def test_worker_terminal_operation_fences_migration_through_mirror_apply(
    client,
    session_factory,
    monkeypatch,
    endpoint,
    terminal_status,
):
    """A stale pending migration cannot fit between remote stop and CAS."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    protocol, _state = _durable_terminal_protocol(
        task,
        terminal_status=terminal_status,
        response={"ok": True, "stopped": True, "cleared_messages": 0},
    )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    sync_entered = asyncio.Event()
    allow_sync = asyncio.Event()
    real_sync = worker_relay_module.apply_authoritative_worker_task

    async def delayed_sync(*args, **kwargs):
        sync_entered.set()
        await allow_sync.wait()
        return await real_sync(*args, **kwargs)

    monkeypatch.setattr(
        worker_relay_module,
        "apply_authoritative_worker_task",
        delayed_sync,
    )

    migration_attempting = asyncio.Event()
    migration_claimed = asyncio.Event()
    allow_restore = asyncio.Event()

    async def stale_migration():
        migration_attempting.set()
        async with worker_proxy_module.get_task_operation_lock(task.id):
            async with session_factory() as db:
                current = await db.get(Task, task.id)
                if current.status != "pending":
                    return current.status
                current.status = "migrating"
                await db.commit()
                migration_claimed.set()
                await allow_restore.wait()
                db.expire_all()
                current = await db.get(Task, task.id)
                current.status = "pending"
                await db.commit()
                return current.status

    request_task = asyncio.create_task(
        client.post(f"/api/tasks/{task.id}/{endpoint}")
    )
    await asyncio.wait_for(sync_entered.wait(), timeout=1)
    migration_task = asyncio.create_task(stale_migration())
    await migration_attempting.wait()
    # Give an unlocked stale migrator a deterministic chance to claim before
    # allowing Manager mirror apply. With the endpoint-level fence it blocks.
    for _ in range(3):
        await asyncio.sleep(0)
    allow_sync.set()
    response = await asyncio.wait_for(request_task, timeout=2)
    allow_restore.set()
    migration_observed = await asyncio.wait_for(migration_task, timeout=2)

    assert response.status_code == 200, response.text
    assert not migration_claimed.is_set()
    assert migration_observed == terminal_status
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == terminal_status
    assert [call.args[1] for call in proxy.proxy_to_worker.await_args_list] == [
        "GET",
        "PUT",
        "POST",
    ]
    assert all(
        call.kwargs["operation_lock_held"] is True
        for call in proxy.proxy_to_worker.await_args_list
    )


async def test_worker_cancel_finishes_authoritative_apply_before_propagating_cancellation(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    protocol, _state = _durable_terminal_protocol(
        task,
        terminal_status="cancelled",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    sync_entered = asyncio.Event()
    release_sync = asyncio.Event()
    real_sync = worker_relay_module.apply_authoritative_worker_task

    async def delayed_sync(*args, **kwargs):
        sync_entered.set()
        await release_sync.wait()
        return await real_sync(*args, **kwargs)

    monkeypatch.setattr(
        worker_relay_module,
        "apply_authoritative_worker_task",
        delayed_sync,
    )
    request_task = asyncio.create_task(
        client.post(f"/api/tasks/{task.id}/cancel")
    )
    await asyncio.wait_for(sync_entered.wait(), timeout=1)
    request_task.cancel()
    await asyncio.sleep(0)
    async with session_factory() as db:
        before_release = await db.get(Task, task.id)
    assert before_release.status == "pending"
    release_sync.set()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "cancelled"
    assert not worker_relay_module.has_worker_execution_quarantine(
        current.metadata_
    )


@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    [("cancel", "cancelled"), ("stop-session", "completed")],
)
async def test_uncertain_worker_termination_quarantines_until_terminal_readback(
    client,
    session_factory,
    monkeypatch,
    endpoint,
    terminal_status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        turn_generation=4,
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = (
        worker_proxy_module.WorkerTaskMutationOutcomeUncertainError(
            "connection lost",
            status_code=502,
        )
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/{endpoint}")

    assert response.status_code == 503, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        observed = worker_relay_module.worker_task_generation(current)
        from backend.services.worker_task_termination import (
            active_worker_task_termination_receipt,
        )

        receipt = await active_worker_task_termination_receipt(db, task.id)
    assert observed is not None
    assert current.status == "pending"
    assert not worker_relay_module.has_worker_execution_quarantine(
        current.metadata_
    )
    assert receipt is not None
    assert receipt.side == "manager"
    assert receipt.status == "pending_remote"
    assert receipt.operation == (
        "cancel" if endpoint == "cancel" else "stop_session"
    )

    proxy.reset_mock()
    retry = await client.post(f"/api/tasks/{task.id}/retry")
    chat = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "do not revive the uncertain generation"},
    )
    assert retry.status_code == 409
    assert chat.status_code == 409
    proxy.proxy_to_worker.assert_not_awaited()

    # Relay cannot apply any snapshot while the durable receipt owns this
    # exact Manager mirror; only query-before-write reconciliation may do so.
    async with session_factory() as db:
        unchanged = await worker_relay_module.apply_authoritative_worker_task(
            db,
            observed,
            _remote_task(
                task,
                status=terminal_status,
                completed_at=None,
            ),
        )
    assert unchanged is None
    async with session_factory() as db:
        still_pending = await db.get(Task, task.id)
    assert still_pending.status == "pending"


async def test_worker_termination_task_not_found_requires_durable_ack_intent(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        turn_generation=5,
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager = await worker_termination_module.create_or_resume_manager_receipt(
            db,
            current,
            operation="cancel",
        )
        operation_id = manager.operation_id
        await worker_termination_module.apply_manager_result(
            db,
            operation_id,
            _terminal_worker_receipt(manager, rejected=False),
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = (
        worker_termination_module.task_not_found_payload(
            task.id,
            operation_id,
        )
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    proxy.proxy_to_worker.assert_awaited_once()
    assert proxy.proxy_to_worker.await_args.args[1] == "GET"
    async with session_factory() as db:
        receipt = await db.get(
            worker_termination_module.WorkerTaskTerminationReceipt,
            operation_id,
        )
    assert receipt.status == "conflict"
    assert receipt.active_task_id == task.id
    assert receipt.ack_intent_at is None
    assert receipt.acknowledged_at is None


@pytest.mark.parametrize("rejected", [False, True], ids=["success", "rejection"])
async def test_worker_termination_task_not_found_settles_after_ack_intent(
    client,
    session_factory,
    monkeypatch,
    rejected,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        turn_generation=6,
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager = await worker_termination_module.create_or_resume_manager_receipt(
            db,
            current,
            operation="cancel",
        )
        operation_id = manager.operation_id
        remote = _terminal_worker_receipt(manager, rejected=rejected)
        if rejected:
            await worker_termination_module.reject_manager_receipt(
                db,
                operation_id,
                remote,
            )
        else:
            await worker_termination_module.apply_manager_result(
                db,
                operation_id,
                remote,
            )
        intent = await worker_termination_module.record_manager_ack_intent(
            db,
            operation_id,
        )
        assert intent.ack_intent_at is not None

    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = (
        worker_termination_module.task_not_found_payload(
            task.id,
            operation_id,
        )
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == (409 if rejected else 200), response.text
    proxy.proxy_to_worker.assert_awaited_once()
    assert proxy.proxy_to_worker.await_args.args[1] == "GET"
    async with session_factory() as db:
        receipt = await db.get(
            worker_termination_module.WorkerTaskTerminationReceipt,
            operation_id,
        )
        current = await db.get(Task, task.id)
    assert receipt.status == ("rejected" if rejected else "settled")
    assert receipt.active_task_id is None
    assert receipt.acknowledged_at >= receipt.ack_intent_at
    assert current.status == ("pending" if rejected else "cancelled")


async def test_proxy_response_cannot_overwrite_task_reassigned_during_request(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )

    async def move_local_before_response(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    worker_id=None,
                    status="executing",
                    retry_count=Task.retry_count + 1,
                    session_id="local-session",
                )
            )
            await db.commit()
        return _remote_task(
            task,
            status="cancelled",
            session_id="old-worker-session",
            completed_at=None,
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = move_local_before_response
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id is None
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "local-session"
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


async def test_proxy_response_cannot_overwrite_same_worker_retry_aba(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
        error_message="old failure",
        metadata_={"ccm_worker_remote_materialized_v1": True},
    )

    async def retry_completes_before_old_response(
        _task,
        method,
        path,
        body=None,
        **_kwargs,
    ):
        if method == "GET":
            return _routing_snapshot(task)
        assert path.endswith("/internal/worker-retry")
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    status="executing",
                    retry_count=Task.retry_count + 1,
                    session_id="new-session",
                    error_message=None,
                    completed_at=None,
                )
            )
            await db.commit()
        return _worker_manual_retry_response(task, body)

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = retry_completes_before_old_response
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/retry")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id == worker.id
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "new-session"
    assert current.error_message is None
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


async def test_proxy_status_publication_fence_miss_returns_conflict(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    base_protocol, _state = _durable_terminal_protocol(
        task,
        terminal_status="completed",
    )
    async def replace_before_result_apply(
        remote_task,
        method,
        path,
        body=None,
        **kwargs,
    ):
        result = await base_protocol(
            remote_task,
            method,
            path,
            body,
            **kwargs,
        )
        if method == "PUT":
            async with session_factory() as replacement_db:
                await replacement_db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(
                        status="executing",
                        retry_count=Task.retry_count + 1,
                        completed_at=None,
                    )
                )
                await replacement_db.commit()
        return result

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = replace_before_result_apply
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()

    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.completed_at is None
    async with session_factory() as db:
        receipt = (
            await worker_termination_module.active_worker_task_termination_receipt(
                db,
                task.id,
            )
        )
    assert receipt.status == "conflict"
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


async def test_proxy_response_without_remote_generation_fails_closed(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = {
        "id": task.id,
        "status": "cancelled",
        # retry_count intentionally absent: this cannot identify a remote
        # generation on the same Worker.
    }
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = (
            await worker_termination_module.active_worker_task_termination_receipt(
                db,
                task.id,
            )
        )
    assert current.status == "in_progress"
    assert current.completed_at is None
    assert not worker_relay_module.has_worker_execution_quarantine(
        current.metadata_
    )
    assert receipt.status == "conflict"
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


@pytest.mark.parametrize(
    "remote_overrides",
    [
        {"status": "not-a-task-status", "retry_count": 2},
        {"status": "cancelled", "retry_count": 1},
    ],
)
async def test_proxy_response_rejects_malformed_or_regressed_generation(
    client,
    session_factory,
    monkeypatch,
    remote_overrides,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        retry_count=2,
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = _remote_task(
        task,
        completed_at=None,
        **remote_overrides,
    )
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = (
            await worker_termination_module.active_worker_task_termination_receipt(
                db,
                task.id,
            )
        )
    assert current.status == "in_progress"
    assert current.retry_count == 2
    assert current.completed_at is None
    assert not worker_relay_module.has_worker_execution_quarantine(
        current.metadata_
    )
    assert receipt.status == "conflict"
    status_broadcast.assert_not_awaited()


async def test_proxy_response_cannot_overwrite_new_shared_shadow_authority(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )

    async def become_shared_before_response(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(shared_from_id=987654)
            )
            await db.commit()
        return _remote_task(
            task,
            status="cancelled",
            completed_at=None,
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = become_shared_before_response
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.shared_from_id == 987654
    assert current.status == "in_progress"
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


async def test_local_dequeue_skips_worker_tasks(session_factory):
    from backend.services.task_queue import TaskQueue
    await _mk_task(session_factory, status="pending", worker_id=1)
    local = await _mk_task(session_factory, status="pending")
    async with session_factory() as db:
        q = TaskQueue(db)
        got = await q.dequeue()
    assert got is not None and got.id == local.id


@pytest.mark.parametrize(
    ("message", "detail"),
    [
        ("$monitor watch the build", "does not support Skills: monitor"),
        ("$not-a-command explain this", "$not-a-command"),
    ],
)
async def test_codex_worker_chat_rejects_invalid_command_before_manager_side_effects(
    client,
    session_factory,
    monkeypatch,
    message,
    detail,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        provider="codex",
        status="completed",
    )
    proxy = AsyncMock()
    proxy.relay = AsyncMock()
    broadcaster = FakeBroadcaster()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", broadcaster)

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": message},
    )

    assert response.status_code == 400
    assert detail in response.text
    async with session_factory() as db:
        stored = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type == "user_message",
            )
        )).scalars().all())
    assert stored == []
    assert broadcaster.sent == []
    proxy.require_ready_worker.assert_not_awaited()
    proxy.push_files.assert_not_awaited()
    proxy.sync_task_skill_selection.assert_not_awaited()
    proxy.proxy_to_worker.assert_not_awaited()
    proxy.relay.subscribe_task.assert_not_awaited()


async def test_codex_worker_chat_allows_sub_agent_command(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        provider="codex",
        status="completed",
    )
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = AsyncMock()

    async def route_then_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        return {"ok": True, "queued": True}

    proxy.proxy_to_worker.side_effect = route_then_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    message = "$sub-agent review the change"
    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": message},
    )

    assert response.status_code == 200, response.text
    chat_call = next(
        call
        for call in proxy.proxy_to_worker.await_args_list
        if call.args[1] == "POST"
    )
    assert chat_call.kwargs["body"]["message"] == message
    proxy.sync_task_skill_selection.assert_awaited_once()


async def test_chat_proxy_for_worker_task(
    client,
    session_factory,
    monkeypatch,
    tmp_path,
):
    upload_name = "11111111-1111-4111-8111-111111111111.txt"
    upload_path = tmp_path / upload_name
    upload_path.write_text("worker attachment", encoding="utf-8")
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(settings, "worker_remote_dir", "/srv/ccm-worker")
    remote_path = f"/srv/ccm-worker/uploads/{upload_name}"
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = w
    proxy.relay = AsyncMock()

    async def route_then_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(t)
        return {"ok": True, "queued": True, "session_id": "sess-1"}

    proxy.proxy_to_worker.side_effect = route_then_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    resp = await client.post(
        f"/api/tasks/{t.id}/chat",
        json={
            "message": "hello worker",
            "file_paths": [f"/api/uploads/{upload_name}"],
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["has_session"] is True
    assert "session_id" not in body
    assert "instance_id" not in body
    chat_call = next(
        call
        for call in proxy.proxy_to_worker.await_args_list
        if call.args[1] == "POST"
    )
    assert chat_call.kwargs["body"]["file_paths"] == [remote_path]
    proxy.push_files.assert_awaited_once_with(
        w,
        [str(upload_path)],
        remote_paths=[remote_path],
    )

    async with session_factory() as db:
        logs = (await db.execute(
            select(LogEntry).where(LogEntry.task_id == t.id,
                                   LogEntry.event_type == "user_message")
        )).scalars().all()
        task = await db.get(Task, t.id)
    assert len(logs) == 1 and logs[0].instance_id is None
    assert task.session_id == "sess-1"
    assert proxy.proxy_to_worker.await_count == 2
    assert (
        proxy.proxy_to_worker.call_args.kwargs["operation_lock_held"]
        is True
    )


async def test_worker_chat_commit_cancellation_arms_handoff_recovery_first(
    session_factory,
    monkeypatch,
):
    """A durable Manager outbox must gain a recovery owner before cancel."""

    from backend.api.chat import ChatMessage, _send_worker_chat

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
        session_id="worker-session",
    )
    commit_durable = asyncio.Event()
    release_commit_return = asyncio.Event()
    recovery_armed = asyncio.Event()
    ordering: list[str] = []
    armed_generations = []

    def arm_recovery(observed_worker, reserved):
        assert observed_worker.id == worker.id
        ordering.append("recovery_armed")
        armed_generations.append(reserved)
        recovery_armed.set()

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = SimpleNamespace(
        subscribe_task=AsyncMock(),
        ensure_worker_turn_handoff_recovery=Mock(
            side_effect=arm_recovery,
        ),
    )

    async def routing_only(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        raise AssertionError("Cancellation must win before the initial POST")

    proxy.proxy_to_worker.side_effect = routing_only
    broadcaster = FakeBroadcaster()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", broadcaster)
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        original_commit = db.commit

        async def durable_commit_then_pause():
            await original_commit()
            ordering.append("commit_durable")
            commit_durable.set()
            await release_commit_return.wait()

        monkeypatch.setattr(db, "commit", durable_commit_then_pause)
        send = asyncio.create_task(
            _send_worker_chat(
                current,
                ChatMessage(message="recover after cancellation"),
                db,
                request,
            )
        )
        await asyncio.wait_for(commit_durable.wait(), timeout=2)
        send.cancel()
        await asyncio.sleep(0)
        assert not recovery_armed.is_set()

        release_commit_return.set()
        with pytest.raises(asyncio.CancelledError):
            await send

    assert recovery_armed.is_set()
    assert ordering == ["commit_durable", "recovery_armed"]
    assert len(armed_generations) == 1
    reserved = armed_generations[0]
    assert reserved.worker_turn_handoff_acknowledged is False
    assert broadcaster.sent == []
    assert not any(
        call.args[1] == "POST"
        for call in proxy.proxy_to_worker.await_args_list
    )

    async with session_factory() as db:
        persisted_task = await db.get(Task, task.id)
        receipt = await db.get(
            WorkerTurnHandoffReceipt,
            reserved.worker_turn_handoff_id,
        )
        user_log = await db.get(
            LogEntry,
            reserved.worker_turn_handoff_source_log_id,
        )
    assert persisted_task.worker_turn_handoff_id == (
        reserved.worker_turn_handoff_id
    )
    assert persisted_task.worker_turn_handoff_acknowledged is False
    assert receipt is not None
    assert receipt.side == "manager"
    assert receipt.status == "prepared"
    assert user_log is not None
    assert user_log.task_retry_count == task.retry_count
    assert user_log.task_turn_generation == task.turn_generation + 1
    assert json.loads(user_log.raw_json)["execution_principal"] == {
        "user_id": None,
        "role": "super_admin",
        "mode": "unrestricted",
        "kind": "deployment_token",
    }


@pytest.mark.parametrize(
    ("run_key", "run_status", "cleanup_status"),
    [
        ("7", "running", "pending"),
        ("8", "completed", "failed"),
    ],
)
async def test_worker_internal_handoff_rejects_unsettled_harness_graph(
    client,
    session_factory,
    monkeypatch,
    run_key,
    run_status,
    cleanup_status,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        retry_count=2,
        turn_generation=8,
        session_id="worker-local-session",
    )
    await _add_unsettled_harness_graph(
        session_factory,
        task,
        run_key=run_key,
        run_status=run_status,
        cleanup_status=cleanup_status,
    )
    dispatcher = SimpleNamespace(
        enqueue_worker_turn_handoff=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(main_module, "dispatcher", dispatcher)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    handoff_id = run_key * 32
    payload = {
        "message": "must wait for exact Harness cleanup",
        "worker_turn_handoff_id": handoff_id,
        "worker_turn_handoff_retry_count": task.retry_count,
        "worker_turn_handoff_from_generation": task.turn_generation,
        "worker_turn_handoff_incarnation_id": task.incarnation_id,
        **worker_relay_module.canonical_delegated_principal_payload(task),
    }

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json=payload,
        headers={"X-CCM-Task-Incarnation": task.incarnation_id},
    )

    assert response.status_code == 409, response.text
    assert "Test Harness" in response.text
    dispatcher.enqueue_worker_turn_handoff.assert_not_awaited()
    async with session_factory() as db:
        logs = list(
            (
                await db.execute(
                    select(LogEntry).where(
                        LogEntry.task_id == task.id,
                        LogEntry.event_type == "user_message",
                    )
                )
            ).scalars()
        )
        receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
        current = await db.get(Task, task.id)
    assert logs == []
    assert receipt is None
    assert current.turn_generation == task.turn_generation
    assert current.status == task.status


async def test_worker_internal_chat_handoff_post_is_durable_and_idempotent(
    client,
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        retry_count=2,
        turn_generation=8,
        session_id="worker-local-session",
    )
    dispatcher = SimpleNamespace(
        enqueue_worker_turn_handoff=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(main_module, "dispatcher", dispatcher)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    handoff_id = "a" * 32
    payload = _internal_worker_handoff_payload(
        task,
        handoff_id=handoff_id,
        message="run this exact follow-up",
    )
    headers = _worker_task_incarnation_headers(task)

    first = await client.post(
        f"/api/tasks/{task.id}/chat", json=payload, headers=headers
    )
    duplicate = await client.post(
        f"/api/tasks/{task.id}/chat", json=payload, headers=headers
    )

    assert first.status_code == 200, first.text
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json() == first.json()
    assert first.json()["workspace_review_expected"] is False
    assert first.json()["workspace_review_baseline_run_id"] is None
    async with session_factory() as db:
        logs = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type == "user_message",
            )
        )).scalars())
        durable_receipt = await db.get(
            WorkerTurnHandoffReceipt,
            handoff_id,
        )
    assert len(logs) == 1
    assert durable_receipt.response == first.json()
    metadata = json.loads(logs[0].raw_json)["worker_turn_handoff"]
    assert metadata["id"] == handoff_id
    assert metadata["queue_payload"]["prompt"] == payload["message"]
    assert metadata["queue_payload"]["initiating_user_id"] is None
    assert metadata["queue_payload"]["initiating_user_role"] == "member"
    assert metadata["queue_payload"]["execution_mode"] == "sandbox"
    assert (
        metadata["queue_payload"]["execution_principal_kind"] == "system"
    )
    assert dispatcher.enqueue_worker_turn_handoff.await_count == 2

    changed = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={**payload, "message": "different request"},
        headers=headers,
    )
    assert changed.status_code == 409

    receipt = await client.get(
        f"/api/tasks/{task.id}/worker-turn-handoffs/{handoff_id}",
        headers=headers,
    )
    assert receipt.status_code == 200, receipt.text
    assert receipt.json()["status"] == "accepted"
    assert "queue_payload" not in receipt.json()
    assert payload["message"] not in receipt.text
    resumed = await client.post(
        f"/api/tasks/{task.id}/worker-turn-handoffs/{handoff_id}/resume",
        headers=headers,
    )
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["resumed"] is True
    assert dispatcher.enqueue_worker_turn_handoff.await_count == 3

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        current.turn_generation = task.turn_generation + 1
        logs[0].task_retry_count = task.retry_count
        logs[0].task_turn_generation = task.turn_generation + 1
        await db.merge(logs[0])
        durable_receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
        durable_receipt.status = "launched"
        durable_receipt.claimed_turn_generation = task.turn_generation + 1
        await db.commit()
    launched = await client.get(
        f"/api/tasks/{task.id}/worker-turn-handoffs/{handoff_id}",
        headers=headers,
    )
    assert launched.status_code == 200
    assert launched.json()["status"] == "launched"
    assert launched.json()["turn_generation"] == task.turn_generation + 1


@pytest.mark.parametrize(
    ("handoff_status", "expected_status", "should_enqueue", "resumed"),
    [
        ("claimed", 200, True, True),
        ("launching", 200, False, False),
        ("launched", 200, False, False),
        ("cancelled", 409, False, None),
    ],
)
async def test_worker_internal_handoff_duplicate_and_resume_obey_boundary(
    client,
    session_factory,
    monkeypatch,
    handoff_status,
    expected_status,
    should_enqueue,
    resumed,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        retry_count=2,
        turn_generation=8,
        session_id="worker-local-session",
    )
    dispatcher = SimpleNamespace(
        enqueue_worker_turn_handoff=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(main_module, "dispatcher", dispatcher)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    handoff_id = "b" * 32
    payload = _internal_worker_handoff_payload(
        task,
        handoff_id=handoff_id,
        message="resume only before the provider boundary",
    )
    headers = _worker_task_incarnation_headers(task)

    first = await client.post(
        f"/api/tasks/{task.id}/chat", json=payload, headers=headers
    )
    assert first.status_code == 200, first.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
        source_log = await db.get(LogEntry, receipt.source_log_id)
        receipt.status = handoff_status
        if handoff_status in {"claimed", "launching", "launched"}:
            next_generation = task.turn_generation + 1
            receipt.claimed_turn_generation = next_generation
            source_log.task_retry_count = task.retry_count
            source_log.task_turn_generation = next_generation
            current.turn_generation = next_generation
        else:
            receipt.cancel_reason = "cancelled before provider launch"
        await db.commit()

    dispatcher.enqueue_worker_turn_handoff.reset_mock()
    duplicate = await client.post(
        f"/api/tasks/{task.id}/chat", json=payload, headers=headers
    )

    assert duplicate.status_code == expected_status, duplicate.text
    if expected_status == 200:
        assert duplicate.json() == first.json()
    if should_enqueue:
        dispatcher.enqueue_worker_turn_handoff.assert_awaited_once()
    else:
        dispatcher.enqueue_worker_turn_handoff.assert_not_awaited()

    dispatcher.enqueue_worker_turn_handoff.reset_mock()
    resume_response = await client.post(
        f"/api/tasks/{task.id}/worker-turn-handoffs/{handoff_id}/resume",
        headers=headers,
    )

    assert resume_response.status_code == expected_status, resume_response.text
    if resumed is not None:
        assert resume_response.json()["resumed"] is resumed
    if should_enqueue:
        dispatcher.enqueue_worker_turn_handoff.assert_awaited_once()
    else:
        dispatcher.enqueue_worker_turn_handoff.assert_not_awaited()

    async with session_factory() as db:
        user_logs = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type == "user_message",
            )
        )).scalars())
    assert len(user_logs) == 1


async def test_worker_handoff_rejects_tampered_structured_queue_payload(
    client,
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        retry_count=1,
        turn_generation=4,
        session_id="worker-local-session",
    )
    dispatcher = SimpleNamespace(
        enqueue_worker_turn_handoff=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(main_module, "dispatcher", dispatcher)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    handoff_id = "d" * 32
    payload = _internal_worker_handoff_payload(
        task,
        handoff_id=handoff_id,
        message="original exact prompt",
    )
    headers = _worker_task_incarnation_headers(task)
    first = await client.post(
        f"/api/tasks/{task.id}/chat", json=payload, headers=headers
    )
    assert first.status_code == 200

    async with session_factory() as db:
        receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
        changed = dict(receipt.queue_payload)
        changed["prompt"] = "tampered prompt"
        receipt.queue_payload = changed
        await db.commit()

    duplicate = await client.post(
        f"/api/tasks/{task.id}/chat", json=payload, headers=headers
    )
    stored = await client.get(
        f"/api/tasks/{task.id}/worker-turn-handoffs/{handoff_id}",
        headers=headers,
    )
    resumed = await client.post(
        f"/api/tasks/{task.id}/worker-turn-handoffs/{handoff_id}/resume",
        headers=headers,
    )
    assert duplicate.status_code == 409
    assert stored.status_code == 409
    assert resumed.status_code == 409
    assert dispatcher.enqueue_worker_turn_handoff.await_count == 1


@pytest.mark.parametrize(
    ("handoff_status", "should_resume"),
    [
        ("accepted", True),
        ("claimed", True),
        ("launching", False),
        ("launched", False),
    ],
)
async def test_worker_chat_lost_http_ack_reconciles_exact_handoff_receipt(
    client,
    session_factory,
    monkeypatch,
    handoff_status,
    should_resume,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
        session_id="worker-session",
    )
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = AsyncMock()
    captured_request = None

    async def route_then_lose_ack(_task, method, _path, *_args, **kwargs):
        nonlocal captured_request
        if method == "GET":
            return _routing_snapshot(task)
        captured_request = kwargs["body"]
        raise HTTPException(503, "lost Worker response")

    proxy.proxy_to_worker.side_effect = route_then_lose_ack

    async def get_receipt(_worker, task_id, handoff_id, incarnation_id):
        assert task_id == task.id
        assert captured_request is not None
        assert handoff_id == captured_request["worker_turn_handoff_id"]
        assert incarnation_id == task.incarnation_id
        return _remote_worker_handoff_receipt(
            task,
            captured_request,
            status=handoff_status,
        )

    proxy.get_worker_turn_handoff_receipt.side_effect = get_receipt
    async def resume_receipt(_worker, task_id, handoff_id, incarnation_id):
        return await get_receipt(
            _worker,
            task_id,
            handoff_id,
            incarnation_id,
        )

    proxy.resume_worker_turn_handoff.side_effect = resume_receipt
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "recover this exact request"},
    )

    assert response.status_code == 200, response.text
    assert captured_request is not None
    captured_handoff = captured_request["worker_turn_handoff_id"]
    proxy.get_worker_turn_handoff_receipt.assert_awaited_once()
    if should_resume:
        proxy.resume_worker_turn_handoff.assert_awaited_once()
    else:
        proxy.resume_worker_turn_handoff.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = list((await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type == "user_message",
            )
        )).scalars())
    assert current.worker_turn_handoff_id == captured_handoff
    assert current.worker_turn_handoff_acknowledged is True
    assert len(logs) == 1


async def test_worker_chat_lost_ack_mismatched_turn_fails_closed(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
        session_id="worker-session",
    )
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = AsyncMock()
    captured_request = None

    async def route_then_lose_ack(_task, method, _path, *_args, **kwargs):
        nonlocal captured_request
        if method == "GET":
            return _routing_snapshot(task)
        captured_request = kwargs["body"]
        raise HTTPException(503, "lost Worker response")

    proxy.proxy_to_worker.side_effect = route_then_lose_ack

    async def get_receipt(_worker, task_id, handoff_id, incarnation_id):
        assert task_id == task.id
        assert captured_request is not None
        assert handoff_id == captured_request["worker_turn_handoff_id"]
        assert incarnation_id == task.incarnation_id
        return _remote_worker_handoff_receipt(
            task,
            captured_request,
            status="launching",
            # Same handoff id cannot lend authority to a different turn.
            turn_generation=task.turn_generation + 2,
        )

    proxy.get_worker_turn_handoff_receipt.side_effect = get_receipt
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "reject mismatched remote turn"},
    )

    assert response.status_code == 503
    assert captured_request is not None
    captured_handoff = captured_request["worker_turn_handoff_id"]
    proxy.resume_worker_turn_handoff.assert_not_awaited()
    assert sum(
        call.args[1] == "POST"
        for call in proxy.proxy_to_worker.await_args_list
    ) == 1
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        manager_receipt = await db.get(
            WorkerTurnHandoffReceipt,
            captured_handoff,
        )
    assert current.worker_turn_handoff_id == captured_handoff
    assert current.worker_turn_handoff_acknowledged is False
    assert manager_receipt.status == "prepared"


async def test_worker_chat_ack_then_relay_consumes_exact_handoff(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
        session_id="worker-session",
    )
    broadcaster = FakeBroadcaster()
    actual_relay = WorkerRelay(session_factory, broadcaster)
    actual_relay._tasks[worker.id] = {task.id}
    _mock_launched_handoff_receipts(actual_relay, session_factory)
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = AsyncMock()

    async def route_then_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        return {"ok": True, "queued": True, "session_id": task.session_id}

    proxy.proxy_to_worker.side_effect = route_then_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", broadcaster)

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "next turn"},
    )
    assert response.status_code == 200, response.text
    async with session_factory() as db:
        acknowledged = await db.get(Task, task.id)
    assert acknowledged.turn_generation == task.turn_generation
    assert acknowledged.worker_turn_handoff_id is not None
    assert acknowledged.worker_turn_handoff_acknowledged is True

    await actual_relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "first delta",
                "item_id": "item-ack-first",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation + 1,
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.turn_generation == task.turn_generation + 1
    assert current.worker_turn_handoff_id is None
    assert any(
        payload.get("content") == "first delta"
        for _channel, payload in broadcaster.sent
    )


async def test_first_next_turn_relay_holds_migration_fence_through_broadcast(
    session_factory,
):
    from backend.services.task_migrator import TaskMigrator

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=2,
        turn_generation=8,
    )
    reserved = await _reserve_worker_handoff(session_factory, task)
    async with session_factory() as db:
        acknowledged = await (
            worker_relay_module.acknowledge_worker_turn_handoff(db, reserved)
        )
        assert acknowledged is not None
        await db.commit()

    broadcast_entered = asyncio.Event()
    release_broadcast = asyncio.Event()

    class BlockingBroadcaster:
        async def broadcast(self, _channel, _data):
            broadcast_entered.set()
            await release_broadcast.wait()

    relay = WorkerRelay(session_factory, BlockingBroadcaster())
    relay._tasks[worker.id] = {task.id}
    relay._fetch_worker_turn_handoff_receipt = AsyncMock(
        return_value=_launched_handoff_receipt(reserved)
    )
    event_task = asyncio.create_task(relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "first exact G+1 delta",
                "item_id": "first-exact-g-plus-one",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation + 1,
            },
        },
        worker,
    ))
    await asyncio.wait_for(broadcast_entered.wait(), timeout=2)

    # Adoption has already committed and cleared the marker, but migration
    # must still be unable to cross the event's persistence/publication fence.
    async with session_factory() as db:
        adopted = await db.get(Task, task.id)
    assert adopted.turn_generation == task.turn_generation + 1
    assert adopted.worker_turn_handoff_id is None

    migrator = TaskMigrator(session_factory, relay=AsyncMock())
    migrator._migrate_locked = AsyncMock()
    migration = asyncio.create_task(migrator.migrate(task.id, None))
    await asyncio.sleep(0)
    migrator._migrate_locked.assert_not_awaited()

    release_broadcast.set()
    await event_task
    await migration
    migrator._migrate_locked.assert_awaited_once_with(
        task.id,
        None,
        coordinated_updates={},
    )


async def test_worker_chat_relay_before_http_ack_accepts_exact_next_turn(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        retry_count=3,
        turn_generation=11,
        session_id="old-worker-session",
        has_unread=False,
    )
    broadcaster = FakeBroadcaster()
    actual_relay = WorkerRelay(session_factory, broadcaster)
    actual_relay._tasks[worker.id] = {task.id}
    _mock_launched_handoff_receipts(actual_relay, session_factory)
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = AsyncMock()
    pending_relay_events: list[asyncio.Task] = []

    async def relay_before_ack(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        async with session_factory() as db:
            pending = await db.get(Task, task.id)
        assert pending.turn_generation == task.turn_generation
        assert pending.worker_turn_handoff_id is not None
        assert pending.worker_turn_handoff_acknowledged is False

        # Terminal G is stale once this completed task reserved its follow-up.
        pending_relay_events.append(
            asyncio.create_task(actual_relay._handle(
                {
                    "channel": f"task:{task.id}",
                    "data": {
                        "event_type": "result",
                        "role": "assistant",
                        "content": "late terminal G",
                        "task_retry_count": task.retry_count,
                        "task_turn_generation": task.turn_generation,
                    },
                },
                worker,
            ))
        )
        # The exact reserved G+1 may arrive before this HTTP call returns.
        pending_relay_events.append(
            asyncio.create_task(actual_relay._handle(
                {
                    "channel": f"task:{task.id}",
                    "data": {
                        "event_type": "message_delta",
                        "content": "early G+1 delta",
                        "item_id": "item-relay-first",
                        "task_retry_count": task.retry_count,
                        "task_turn_generation": task.turn_generation + 1,
                    },
                },
                worker,
            ))
        )
        pending_relay_events.append(
            asyncio.create_task(actual_relay._handle(
                {
                    "channel": f"task:{task.id}",
                    "data": {
                        "event_type": "message",
                        "role": "assistant",
                        "content": "early G+1 message",
                        "native_turn_id": "native-worker-turn-12",
                        "task_retry_count": task.retry_count,
                        "task_turn_generation": task.turn_generation + 1,
                    },
                },
                worker,
            ))
        )
        await asyncio.sleep(0)
        assert all(not event.done() for event in pending_relay_events)
        return {
            "ok": True,
            "queued": True,
            "session_id": "new-worker-session",
        }

    proxy.proxy_to_worker.side_effect = relay_before_ack
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", broadcaster)

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "run exact next turn"},
    )
    assert response.status_code == 200, response.text
    await asyncio.gather(*pending_relay_events)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = list((await db.execute(
            select(LogEntry)
            .where(LogEntry.task_id == task.id)
            .order_by(LogEntry.id)
        )).scalars())
    assert current.turn_generation == task.turn_generation + 1
    assert current.session_id == "new-worker-session"
    assert current.worker_turn_handoff_id is None
    assert [row.content for row in logs if row.role == "assistant"] == [
        "early G+1 message"
    ]
    assert logs[0].role == "user"
    assert logs[0].task_retry_count == task.retry_count
    assert logs[0].task_turn_generation == task.turn_generation + 1
    assert logs[1].native_turn_id == "native-worker-turn-12"

    actual_relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="executing",
            turn_generation=task.turn_generation + 1,
            session_id="new-worker-session",
        )
    )
    await actual_relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "executing",
            },
        },
        worker,
    )
    await actual_relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "result",
                "role": "assistant",
                "content": "G+1 result",
                "native_turn_id": "native-worker-turn-12",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation + 1,
            },
        },
        worker,
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        result_log = await db.scalar(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.content == "G+1 result",
            )
        )
    assert current.status == "executing"
    assert result_log is not None
    assert result_log.task_turn_generation == task.turn_generation + 1


async def test_worker_chat_response_cannot_overwrite_retry_aba(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        session_id="old-session",
    )

    async def replace_generation_before_response(
        _task,
        method,
        _path,
        *_args,
        **_kwargs,
    ):
        if method == "GET":
            return _routing_snapshot(task)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    retry_count=Task.retry_count + 1,
                    session_id="new-session",
                )
            )
            await db.commit()
        return {
            "ok": True,
            "queued": True,
            "session_id": "stale-worker-session",
        }

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = AsyncMock()
    proxy.proxy_to_worker.side_effect = replace_generation_before_response
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "old generation chat"},
    )

    assert response.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "new-session"
    assert (
        proxy.proxy_to_worker.call_args.kwargs["operation_lock_held"]
        is True
    )


async def test_worker_chat_sender_prefix_is_display_only(session_factory, monkeypatch):
    """Manager displays the sender, while the Worker receives raw model text."""
    import json
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat
    from backend.models.user import User

    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    async with session_factory() as db:
        sender = User(
            email="worker-prefix@test.local",
            name="Worker Alice",
            password_hash="unused",
            role="super_admin",
        )
        db.add(sender)
        await db.commit()
        await db.refresh(sender)

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = w
    proxy.relay = AsyncMock()

    async def route_then_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(t)
        return {
            "ok": True,
            "queued": True,
            "session_id": "worker-prefix-session",
        }

    proxy.proxy_to_worker.side_effect = route_then_chat
    broadcaster = FakeBroadcaster()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", broadcaster)

    request = SimpleNamespace(
        state=SimpleNamespace(user_id=sender.id, user_role="super_admin")
    )
    async with session_factory() as db:
        task = await db.get(Task, t.id)
        await _send_worker_chat(
            task,
            ChatMessage(
                message="[FIX] preserve this tag",
                client_message_id="11111111-2222-4333-8444-777777777777",
            ),
            db,
            request,
        )

    forwarded = proxy.proxy_to_worker.call_args.kwargs["body"]
    assert forwarded["message"] == "[FIX] preserve this tag"
    assert "client_message_id" not in forwarded
    async with session_factory() as db:
        stored = (await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == t.id,
                LogEntry.event_type == "user_message",
            )
        )).scalar_one()
    assert stored.content == "[Worker Alice] [FIX] preserve this tag"
    metadata = json.loads(stored.raw_json)
    assert metadata["raw_content"] == "[FIX] preserve this tag"
    assert metadata["client_message_id"] == (
        "11111111-2222-4333-8444-777777777777"
    )
    event = broadcaster.sent[0][1]
    assert event["content"] == stored.content
    assert event["id"] == stored.id
    assert event["task_id"] == t.id
    assert event["timestamp"].endswith("Z")
    assert event["client_message_id"] == metadata["client_message_id"]


async def test_worker_terminal_pr_chat_separates_actor_from_sandbox_principal(
    session_factory,
    monkeypatch,
):
    """The Manager audit row must not encode admin/user + sandbox together."""

    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat
    from backend.models.user import User

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        session_id="terminal-worker-pr-session",
    )
    async with session_factory() as db:
        sender = User(
            email="worker-terminal-pr@test.local",
            name="Worker PR Admin",
            password_hash="unused",
            role="admin",
        )
        db.add(sender)
        await db.commit()
        await db.refresh(sender)
        sender_id = sender.id

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = AsyncMock()

    async def route_then_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        return {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
        }

    proxy.proxy_to_worker.side_effect = route_then_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    monkeypatch.setattr(
        "backend.api.tasks._require_pr_review_chat_allowed",
        AsyncMock(return_value=True),
    )
    request = SimpleNamespace(state=SimpleNamespace(
        user_id=sender_id,
        user_role="admin",
        auth_type="jwt",
    ))

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        await _send_worker_chat(
            current,
            ChatMessage(message="explain this terminal review"),
            db,
            request,
        )

    post = next(
        call
        for call in proxy.proxy_to_worker.await_args_list
        if call.args[1] == "POST"
    )
    forwarded = post.kwargs["body"]
    assert forwarded["execution_user_id"] is None
    assert forwarded["execution_user_role"] == "member"
    assert forwarded["execution_mode"] == "sandbox"
    assert forwarded["execution_principal_kind"] == "system"
    async with session_factory() as db:
        stored = await db.scalar(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type == "user_message",
            )
        )
    metadata = json.loads(stored.raw_json)
    assert metadata["execution_principal"] == {
        "user_id": None,
        "role": "member",
        "mode": "sandbox",
        "kind": "system",
    }
    assert metadata["actor_user_id"] == sender_id


async def test_worker_chat_applies_exact_mirrored_plan_version(
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat
    from backend.schemas.plan import default_plan_pipeline_config

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        session_id="worker-plan-session",
    )
    async with session_factory() as db:
        plan = Plan(
            title="Worker Plan",
            initial_request="Plan this",
            target_task_id=task.id,
            # The Task moved after this Version was produced. Application must
            # materialize immutable content on the Task's current Worker.
            worker_id=worker.id + 100,
            pipeline_config=default_plan_pipeline_config().model_dump(mode="json"),
            priority=0,
        )
        db.add(plan)
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            worker_id=worker.id + 100,
            worker_version_id=811,
            version_number=1,
            content="# Exact Worker Plan",
            context_session_id=task.session_id,
            context_log_id=None,
            review_verdict="approve",
            human_decision="approved",
            repo_revision={"available": False, "reason": "not_git"},
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        await db.commit()
        local_version_id = version.id

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.materialize_plan_version.return_value = 811
    proxy.relay = AsyncMock()

    async def route_chat(_task, method, path, *_args, **kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        assert kwargs["body"]["plan_version_ids"] == [811]
        assert kwargs["body"]["confirmed_stale_plan_version_ids"] == [811]
        return {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
            "applied_plan_version_ids": [811],
        }

    proxy.proxy_to_worker.side_effect = route_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    broadcaster = FakeBroadcaster()
    monkeypatch.setattr(main_module, "broadcaster", broadcaster)
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        result = await _send_worker_chat(
            current,
            ChatMessage(
                message="Implement the selected Version",
                plan_version_ids=[local_version_id],
            ),
            db,
            request,
        )

    assert result["applied_plan_version_ids"] == [local_version_id]
    applied_events = [
        (channel, data)
        for channel, data in broadcaster.sent
        if data.get("event") == "plan_version_applied"
    ]
    assert [channel for channel, _data in applied_events] == [
        "plans",
        f"plan:{plan.id}",
        f"task:{task.id}",
    ]
    assert all(
        data["plan_id"] == plan.id
        and data["version_id"] == local_version_id
        for _channel, data in applied_events
    )
    async with session_factory() as db:
        application = (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.plan_version_id == local_version_id
                )
            )
        ).scalar_one()
        log = await db.get(LogEntry, application.user_log_id)
        snapshot = json.loads(log.raw_json)["applied_plans"][0]
        assert snapshot["version_id"] == local_version_id
        assert snapshot["content"] == "# Exact Worker Plan"


async def _approved_worker_plan_version(session_factory):
    from backend.schemas.plan import default_plan_pipeline_config

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        session_id="worker-receipt-session",
    )
    async with session_factory() as db:
        plan = Plan(
            title="Receipt Plan",
            initial_request="Plan this",
            target_task_id=task.id,
            worker_id=worker.id,
            pipeline_config=default_plan_pipeline_config().model_dump(mode="json"),
            priority=0,
        )
        db.add(plan)
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            worker_id=worker.id,
            worker_version_id=912,
            version_number=1,
            content="# Receipt-safe Plan",
            context_session_id=task.session_id,
            review_verdict="approve",
            human_decision="approved",
            repo_revision={"available": False, "reason": "not_git"},
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        await db.commit()
        return worker, task, version.id


async def test_worker_chat_ack_survives_later_plan_confirmation_rollback(
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat

    worker, task, version_id = await _approved_worker_plan_version(
        session_factory
    )
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.materialize_plan_version.return_value = 912
    proxy.relay = AsyncMock()

    async def route_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        return {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
            # The remote request was accepted, but this malformed confirmation
            # must fail Manager Plan bookkeeping after the transport ACK.
            "applied_plan_version_ids": [999],
        }

    proxy.proxy_to_worker.side_effect = route_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        with pytest.raises(HTTPException) as caught:
            await _send_worker_chat(
                current,
                ChatMessage(
                    message="Implement once",
                    plan_version_ids=[version_id],
                ),
                db,
                request,
            )
    assert caught.value.status_code == 502

    async with session_factory() as db:
        acknowledged = await db.get(Task, task.id)
    assert acknowledged.turn_generation == task.turn_generation
    assert acknowledged.worker_turn_handoff_id is not None
    assert acknowledged.worker_turn_handoff_acknowledged is True

    relay = WorkerRelay(session_factory, FakeBroadcaster())
    relay._tasks[worker.id] = {task.id}
    _mock_launched_handoff_receipts(relay, session_factory)
    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "accepted turn started",
                "item_id": "accepted-after-plan-rollback",
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation + 1,
            },
        },
        worker,
    )
    async with session_factory() as db:
        converged = await db.get(Task, task.id)
    assert converged.turn_generation == task.turn_generation + 1
    assert converged.worker_turn_handoff_id is None


async def test_worker_plan_application_rechecks_target_writer_after_ack(
    session_factory,
    monkeypatch,
):
    """A remote ACK must not recreate Plan audit after target deletion wins."""

    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat
    import backend.services.plan_service as plan_service_module

    worker, task, version_id = await _approved_worker_plan_version(
        session_factory
    )
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.materialize_plan_version.return_value = 912
    proxy.relay = AsyncMock()

    async def route_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        return {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
            "applied_plan_version_ids": [912],
        }

    proxy.proxy_to_worker.side_effect = route_chat
    target_fence = AsyncMock(
        side_effect=HTTPException(
            409,
            "Plan target disappeared before Manager audit commit",
        )
    )
    monkeypatch.setattr(
        plan_service_module,
        "fence_plan_target_task",
        target_fence,
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        with pytest.raises(HTTPException, match="target disappeared"):
            await _send_worker_chat(
                current,
                ChatMessage(
                    message="Implement once",
                    plan_version_ids=[version_id],
                ),
                db,
                request,
            )

    target_fence.assert_awaited_once_with(
        ANY,
        target_task_id=task.id,
        expected_worker_id=worker.id,
    )
    async with session_factory() as db:
        assert await db.scalar(select(PlanApplication.id)) is None
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.target_task_id == task.id
            )
        )
        assert receipt is not None
        assert receipt.status == "prepared"


async def test_worker_plan_receipt_lock_order_starts_with_fresh_task_fence(
    session_factory,
    monkeypatch,
):
    """Worker delivery writers share Task -> Application -> Receipt order."""

    from backend.models.plan import PlanApplicationAttempt
    from backend.services import plan_service

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    receipt_key = "worker-plan-lock-order"
    async with session_factory() as db:
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement once",
        )
        db.add(log)
        await db.flush()
        db.add(PlanApplicationReceipt(
            receipt_key=receipt_key,
            target_task_id=task.id,
            worker_id=worker.id,
            manager_user_log_id=log.id,
            plan_version_ids=[version_id],
            status="committed",
            delivery_status="queued",
        ))
        await db.commit()

    events: list[str] = []
    original_end = plan_service._end_plan_routing_read
    original_fence = plan_service.fence_plan_target_task
    original_execute = AsyncSession.execute

    async def recording_end(db):
        events.append("fresh_transaction")
        return await original_end(db)

    async def recording_fence(db, *, target_task_id, expected_worker_id):
        events.append("task")
        return await original_fence(
            db,
            target_task_id=target_task_id,
            expected_worker_id=expected_worker_id,
        )

    async def recording_execute(self, statement, *args, **kwargs):
        if getattr(statement, "is_select", False):
            descriptions = getattr(statement, "column_descriptions", ())
            entity = descriptions[0].get("entity") if descriptions else None
            if entity is PlanApplication:
                events.append("application")
            elif entity is PlanApplicationAttempt:
                events.append("attempt")
            elif entity is PlanApplicationReceipt:
                events.append("receipt")
        return await original_execute(self, statement, *args, **kwargs)

    monkeypatch.setattr(plan_service, "_end_plan_routing_read", recording_end)
    monkeypatch.setattr(plan_service, "fence_plan_target_task", recording_fence)
    monkeypatch.setattr(AsyncSession, "execute", recording_execute)

    async with session_factory() as db:
        locked = await plan_service.fence_worker_plan_application_receipt(
            db,
            receipt_key=receipt_key,
            target_task_id=task.id,
            expected_worker_id=worker.id,
        )
        await db.rollback()

    assert locked is not None
    assert events == [
        "fresh_transaction",
        "task",
        "application",
        "attempt",
        "receipt",
    ]


async def test_worker_uncertain_relay_cannot_revive_application_after_task_delete(
    session_factory,
    broadcaster,
    monkeypatch,
):
    """A second Manager process deleting the Task wins before relay audit."""

    from backend.services import plan_service

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    receipt_key = "worker-uncertain-after-delete"
    async with session_factory() as db:
        version = await db.get(PlanVersion, version_id)
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement once",
        )
        db.add(log)
        await db.flush()
        db.add(PlanApplicationReceipt(
            receipt_key=receipt_key,
            target_task_id=task.id,
            worker_id=worker.id,
            manager_user_log_id=log.id,
            plan_version_ids=[version_id],
            status="committed",
            response={"ok": True, "queued": True},
            delivery_status="queued",
        ))
        await db.commit()
        plan_id = version.plan_id
        log_id = log.id

    fence_entered = asyncio.Event()
    release_fence = asyncio.Event()
    original_fence = plan_service.fence_plan_target_task

    async def fence_after_delete(db, *, target_task_id, expected_worker_id):
        fence_entered.set()
        await release_fence.wait()
        return await original_fence(
            db,
            target_task_id=target_task_id,
            expected_worker_id=expected_worker_id,
        )

    monkeypatch.setattr(plan_service, "fence_plan_target_task", fence_after_delete)
    relay = WorkerRelay(session_factory, broadcaster)
    relay._tasks[worker.id] = {task.id}
    relay_event = asyncio.create_task(relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "plan_application_delivery_uncertain",
                "task_id": task.id,
                "receipt_key": receipt_key,
                "delivery_status": "uncertain",
                "error": "late Worker evidence",
            },
        },
        worker,
    ))
    await asyncio.wait_for(fence_entered.wait(), timeout=5)

    # This represents another Manager process completing its already-fenced
    # aggregate delete before the relay transaction can acquire the Task row.
    async with session_factory() as db:
        await db.execute(
            delete(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == receipt_key
            )
        )
        await db.execute(delete(PlanVersion).where(PlanVersion.id == version_id))
        await db.execute(delete(Plan).where(Plan.id == plan_id))
        await db.execute(delete(LogEntry).where(LogEntry.id == log_id))
        await db.execute(delete(Task).where(Task.id == task.id))
        await db.commit()
    release_fence.set()
    await asyncio.wait_for(relay_event, timeout=5)

    async with session_factory() as db:
        assert await db.get(Task, task.id) is None
        assert await db.scalar(select(PlanApplication.id)) is None
        assert await db.scalar(
            select(PlanApplicationReceipt.id).where(
                PlanApplicationReceipt.receipt_key == receipt_key
            )
        ) is None
    assert not any(
        data.get("receipt_key") == receipt_key
        for _channel, data in broadcaster.sent
    )


async def test_worker_uncertain_relay_rejects_target_reassigned_before_fence(
    session_factory,
    broadcaster,
    monkeypatch,
):
    """A stale source Worker event cannot mutate the moved Task's Plan audit."""

    from backend.services import plan_service

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    destination = await _mk_worker(
        session_factory,
        name="worker-plan-destination",
        private_ip="10.0.0.88",
    )
    receipt_key = "worker-uncertain-after-move"
    async with session_factory() as db:
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement once",
        )
        db.add(log)
        await db.flush()
        db.add(PlanApplicationReceipt(
            receipt_key=receipt_key,
            target_task_id=task.id,
            worker_id=worker.id,
            manager_user_log_id=log.id,
            plan_version_ids=[version_id],
            status="committed",
            response={"ok": True, "queued": True},
            delivery_status="queued",
        ))
        await db.commit()

    fence_entered = asyncio.Event()
    release_fence = asyncio.Event()
    original_fence = plan_service.fence_plan_target_task

    async def fence_after_move(db, *, target_task_id, expected_worker_id):
        fence_entered.set()
        await release_fence.wait()
        return await original_fence(
            db,
            target_task_id=target_task_id,
            expected_worker_id=expected_worker_id,
        )

    monkeypatch.setattr(plan_service, "fence_plan_target_task", fence_after_move)
    relay = WorkerRelay(session_factory, broadcaster)
    relay._tasks[worker.id] = {task.id}
    relay_event = asyncio.create_task(relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "plan_application_delivery_uncertain",
                "task_id": task.id,
                "receipt_key": receipt_key,
                "delivery_status": "uncertain",
                "error": "stale source Worker evidence",
            },
        },
        worker,
    ))
    await asyncio.wait_for(fence_entered.wait(), timeout=5)
    async with session_factory() as db:
        moved = await db.execute(
            update(Task)
            .where(Task.id == task.id, Task.worker_id == worker.id)
            .values(worker_id=destination.id)
        )
        assert moved.rowcount == 1
        await db.commit()
    release_fence.set()
    await asyncio.wait_for(relay_event, timeout=5)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == receipt_key
            )
        )
        assert current.worker_id == destination.id
        assert receipt.delivery_status == "queued"
        assert await db.scalar(select(PlanApplication.id)) is None
    assert not any(
        data.get("receipt_key") == receipt_key
        for _channel, data in broadcaster.sent
    )


async def test_worker_delivery_failure_releases_manager_plan_application(
    session_factory,
    broadcaster,
):
    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    receipt_key = "worker-delivery-failed"
    async with session_factory() as db:
        version = await db.get(PlanVersion, version_id)
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement once",
            raw_json=json.dumps({
                "raw_content": "Implement once",
                "applied_plans": [{"plan_id": version.plan_id}],
            }),
        )
        db.add(log)
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key=receipt_key,
                target_task_id=task.id,
                worker_id=worker.id,
                manager_user_log_id=log.id,
                plan_version_ids=[version_id],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status="queued",
            )
        )
        db.add(
            PlanApplication(
                plan_id=version.plan_id,
                plan_version_id=version_id,
                application_type="chat_message",
                target_task_id=task.id,
                user_log_id=log.id,
                application_receipt_key=receipt_key,
            )
        )
        await db.commit()
        log_id = log.id

    relay = WorkerRelay(session_factory, broadcaster)
    relay._tasks[worker.id] = {task.id}
    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "plan_application_delivery_failed",
                "task_id": task.id,
                "receipt_key": receipt_key,
                "delivery_status": "failed",
                "error": "permanent route mismatch",
            },
        },
        worker,
    )

    async with session_factory() as db:
        assert await db.scalar(
            select(PlanApplication.id).where(
                PlanApplication.plan_version_id == version_id
            )
        ) is None
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == receipt_key
            )
        )
        log = await db.get(LogEntry, log_id)
        assert receipt.delivery_status == "failed"
        assert "applied_plans" not in json.loads(log.raw_json)
    assert any(
        channel == "plans"
        and data.get("event") == "plan_application_delivery_failed"
        for channel, data in broadcaster.sent
    )


async def test_worker_uncertain_delivery_and_resolution_reconcile_manager(
    session_factory,
    broadcaster,
):
    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    receipt_key = "worker-delivery-uncertain"
    async with session_factory() as db:
        version = await db.get(PlanVersion, version_id)
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement once",
            raw_json=json.dumps({
                "raw_content": "Implement once",
                "applied_plans": [{"plan_id": version.plan_id}],
            }),
        )
        db.add(log)
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key=receipt_key,
                target_task_id=task.id,
                worker_id=worker.id,
                manager_user_log_id=log.id,
                plan_version_ids=[version_id],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status="queued",
            )
        )
        db.add(
            PlanApplication(
                plan_id=version.plan_id,
                plan_version_id=version_id,
                application_type="chat_message",
                target_task_id=task.id,
                user_log_id=log.id,
                application_receipt_key=receipt_key,
            )
        )
        await db.commit()

    relay = WorkerRelay(session_factory, broadcaster)
    relay._tasks[worker.id] = {task.id}
    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "plan_application_delivery_uncertain",
                "task_id": task.id,
                "receipt_key": receipt_key,
                "delivery_status": "uncertain",
                "error": "Worker restarted after launch claim",
                "launch_evidence": {
                    "task_id": task.id,
                    "instance_id": 12,
                    "retry_count": 4,
                },
            },
        },
        worker,
    )

    async with session_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == receipt_key
            )
        )
        assert receipt.delivery_status == "uncertain"
        assert receipt.launch_evidence["retry_count"] == 4
        assert await db.scalar(
            select(PlanApplication.id).where(
                PlanApplication.plan_version_id == version_id
            )
        ) is not None

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "plan_application_delivery_resolved",
                "task_id": task.id,
                "receipt_key": receipt_key,
                "delivery_status": "cancelled",
                "action": "release_for_retry",
                "note": "No exact Worker turn exists",
            },
        },
        worker,
    )

    async with session_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == receipt_key
            )
        )
        assert receipt.delivery_status == "cancelled"
        assert receipt.delivery_resolution["action"] == "release_for_retry"
        assert await db.scalar(
            select(PlanApplication.id).where(
                PlanApplication.plan_version_id == version_id
            )
        ) is None


async def test_worker_plan_can_retry_after_failed_prepared_receipt(
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    async with session_factory() as db:
        old_log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement once",
            raw_json=json.dumps({
                "raw_content": "Implement once",
                "plan_delivery": {
                    "status": "failed",
                    "error": "permanent route mismatch",
                },
            }),
        )
        db.add(old_log)
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key="failed-prepared-receipt",
                target_task_id=task.id,
                worker_id=worker.id,
                manager_user_log_id=old_log.id,
                plan_version_ids=[version_id],
                status="prepared",
                delivery_status="failed",
                delivery_error="permanent route mismatch",
            )
        )
        await db.commit()

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.materialize_plan_version.return_value = 912
    proxy.relay = AsyncMock()

    async def route_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        return {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
            "applied_plan_version_ids": [912],
        }

    proxy.proxy_to_worker.side_effect = route_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        result = await _send_worker_chat(
            current,
            ChatMessage(
                message="Implement once",
                plan_version_ids=[version_id],
                confirmed_stale_plan_version_ids=[version_id],
            ),
            db,
            request,
        )

    assert result["applied_plan_version_ids"] == [version_id]
    assert sum(
        call.args[1] == "POST"
        for call in proxy.proxy_to_worker.await_args_list
    ) == 1
    async with session_factory() as db:
        receipts = list(
            (
                await db.execute(
                    select(PlanApplicationReceipt)
                    .where(
                        PlanApplicationReceipt.target_task_id == task.id
                    )
                    .order_by(PlanApplicationReceipt.id)
                )
            ).scalars()
        )
        assert [receipt.delivery_status for receipt in receipts] == [
            "failed",
            "queued",
        ]
        application = await db.scalar(
            select(PlanApplication).where(
                PlanApplication.plan_version_id == version_id
            )
        )
        assert application.application_receipt_key == receipts[1].receipt_key


async def test_worker_plan_application_recovers_lost_http_ack(
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.materialize_plan_version.return_value = 912
    proxy.relay = AsyncMock()
    proxy.get_worker_turn_handoff_receipt.return_value = None

    async def route_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        raise httpx.ReadTimeout("response was lost after Worker commit")

    proxy.proxy_to_worker.side_effect = route_chat
    proxy.get_plan_application_receipt.return_value = {
        "status": "committed",
        "response": {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
            "applied_plan_version_ids": [912],
        },
    }
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        result = await _send_worker_chat(
            current,
            ChatMessage(
                message="Implement once",
                plan_version_ids=[version_id],
            ),
            db,
            request,
        )

    assert result["applied_plan_version_ids"] == [version_id]
    assert sum(
        call.args[1] == "POST"
        for call in proxy.proxy_to_worker.await_args_list
    ) == 1
    assert proxy.get_plan_application_receipt.await_count == 1
    async with session_factory() as db:
        application = await db.scalar(
            select(PlanApplication).where(
                PlanApplication.plan_version_id == version_id
            )
        )
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.target_task_id == task.id
            )
        )
        assert application is not None
        assert receipt.status == "committed"


async def test_worker_plan_receipt_rebinds_ui_correlation_on_retry(
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    old_client_message_id = "11111111-2222-4333-8444-111111111111"
    new_client_message_id = "22222222-3333-4444-8555-222222222222"
    async with session_factory() as db:
        prior_log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement once",
            raw_json=json.dumps({
                "raw_content": "Implement once",
                "client_message_id": old_client_message_id,
            }),
            is_error=False,
        )
        db.add(prior_log)
        await db.flush()
        db.add(PlanApplicationReceipt(
            receipt_key="prepared-ui-correlation-retry",
            target_task_id=task.id,
            worker_id=worker.id,
            manager_user_log_id=prior_log.id,
            plan_version_ids=[version_id],
            status="prepared",
        ))
        await db.commit()
        prior_log_id = prior_log.id

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.relay = AsyncMock()
    proxy.get_plan_application_receipt.return_value = {
        "status": "committed",
        "response": {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
            "applied_plan_version_ids": [912],
        },
    }

    async def route_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        raise AssertionError("Prepared receipt recovery must not resend chat")

    proxy.proxy_to_worker.side_effect = route_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        result = await _send_worker_chat(
            current,
            ChatMessage(
                message="Implement once",
                plan_version_ids=[version_id],
                confirmed_stale_plan_version_ids=[version_id],
                client_message_id=new_client_message_id,
            ),
            db,
            request,
        )

    assert result["applied_plan_version_ids"] == [version_id]
    assert sum(
        call.args[1] == "POST"
        for call in proxy.proxy_to_worker.await_args_list
    ) == 0
    async with session_factory() as db:
        recovered_log = await db.get(LogEntry, prior_log_id)
        metadata = json.loads(recovered_log.raw_json)
        assert metadata["client_message_id"] == new_client_message_id
        assert metadata["client_message_id"] != old_client_message_id
        assert metadata["applied_plans"][0]["version_id"] == version_id


async def test_worker_uncertain_http_reconciliation_consumes_manager_version(
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat
    from backend.services import plan_service

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.materialize_plan_version.return_value = 912
    proxy.relay = AsyncMock()
    proxy.get_worker_turn_handoff_receipt.return_value = None

    async def route_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        raise httpx.ReadTimeout("response was lost after Worker launch claim")

    proxy.proxy_to_worker.side_effect = route_chat
    proxy.get_plan_application_receipt.return_value = {
        "status": "committed",
        "delivery_status": "uncertain",
        "delivery_error": "Worker restarted after launch claim",
        "launch_evidence": {
            "task_id": task.id,
            "instance_id": 17,
            "retry_count": 5,
        },
        "response": {
            "ok": True,
            "queued": True,
            "session_id": task.session_id,
            "applied_plan_version_ids": [912],
        },
    }
    target_fences: list[tuple[int | None, int | None]] = []
    original_fence = plan_service.fence_plan_target_task

    async def recording_fence(db, *, target_task_id, expected_worker_id):
        target_fences.append((target_task_id, expected_worker_id))
        return await original_fence(
            db,
            target_task_id=target_task_id,
            expected_worker_id=expected_worker_id,
        )

    monkeypatch.setattr(plan_service, "fence_plan_target_task", recording_fence)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        with pytest.raises(HTTPException, match="uncertain") as exc_info:
            await _send_worker_chat(
                current,
                ChatMessage(
                    message="Implement once",
                    plan_version_ids=[version_id],
                ),
                db,
                request,
            )
    assert exc_info.value.status_code == 409
    assert target_fences == [(task.id, worker.id)]

    async with session_factory() as db:
        application = await db.scalar(
            select(PlanApplication).where(
                PlanApplication.plan_version_id == version_id
            )
        )
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.target_task_id == task.id
            )
        )
        log = await db.get(LogEntry, receipt.manager_user_log_id)
        assert application.application_receipt_key == receipt.receipt_key
        assert receipt.status == "committed"
        assert receipt.delivery_status == "uncertain"
        assert receipt.launch_evidence["retry_count"] == 5
        assert "applied_plans" in json.loads(log.raw_json)


async def test_worker_plan_receipt_cannot_reconcile_a_different_message(
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    async with session_factory() as db:
        prior_log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement the original request",
            raw_json=json.dumps({"raw_content": "Implement the original request"}),
            is_error=False,
        )
        db.add(prior_log)
        await db.flush()
        db.add(PlanApplicationReceipt(
            receipt_key="receipt-for-original-message",
            target_task_id=task.id,
            worker_id=worker.id,
            manager_user_log_id=prior_log.id,
            plan_version_ids=[version_id],
            status="prepared",
        ))
        await db.commit()

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.relay = AsyncMock()

    async def route_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        raise AssertionError("A different message must not reach the Worker")

    proxy.proxy_to_worker.side_effect = route_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        with pytest.raises(HTTPException, match="different message") as exc_info:
            await _send_worker_chat(
                current,
                ChatMessage(
                    message="This is a new and different request",
                    plan_version_ids=[version_id],
                    confirmed_stale_plan_version_ids=[version_id],
                ),
                db,
                request,
            )

    assert exc_info.value.status_code == 409
    proxy.get_plan_application_receipt.assert_not_awaited()
    proxy.materialize_plan_version.assert_not_awaited()


async def test_worker_plan_preflight_failure_does_not_leave_blocking_receipt(
    session_factory,
    monkeypatch,
):
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat

    worker, task, version_id = await _approved_worker_plan_version(session_factory)
    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.get_plan_repo_revision.return_value = {
        "available": False,
        "reason": "not_git",
    }
    proxy.materialize_plan_version.return_value = 912
    proxy.proxy_to_worker.side_effect = (
        lambda _task, method, _path, *_args, **_kwargs:
        _routing_snapshot(task)
        if method == "GET"
        else None
    )
    proxy.relay.subscribe_task.side_effect = RuntimeError("relay unavailable")
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role="super_admin",
            auth_type="token",
        )
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        with pytest.raises(RuntimeError, match="relay unavailable"):
            await _send_worker_chat(
                current,
                ChatMessage(
                    message="Do not consume the Plan",
                    plan_version_ids=[version_id],
                ),
                db,
                request,
            )

    async with session_factory() as db:
        assert await db.scalar(select(PlanApplicationReceipt.id)) is None
        assert await db.scalar(select(PlanApplication.id)) is None


async def test_chat_proxy_rejects_secrets(client, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())
    resp = await client.post(f"/api/tasks/{t.id}/chat",
                             json={"message": "x", "secret_ids": [1]})
    assert resp.status_code == 400


async def test_worker_retry_rejects_migrating_without_remote_mutation(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="migrating",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/retry")

    assert response.status_code == 409
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "migrating"
    assert current.retry_count == task.retry_count


async def test_stop_session_proxies_for_worker_task(client, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    protocol, _state = _durable_terminal_protocol(
        t,
        terminal_status="completed",
        response={"ok": True, "stopped": True, "cleared_messages": 0},
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    resp = await client.post(f"/api/tasks/{t.id}/stop-session")
    assert resp.status_code == 200
    assert resp.json()["stopped"] is True
    assert proxy.proxy_to_worker.await_count == 3
    get_call, put_call, ack_call = proxy.proxy_to_worker.await_args_list
    assert [call.args[1] for call in (get_call, put_call, ack_call)] == [
        "GET",
        "PUT",
        "POST",
    ]
    receipt_path = get_call.args[2]
    assert receipt_path.startswith(
        f"/api/tasks/{t.id}/termination-receipts/"
    )
    assert put_call.args[2] == receipt_path
    assert ack_call.args[2] == receipt_path + "/ack"
    assert all(
        call.kwargs == {
            "require_json": True,
            "allow_task_absent": False,
            "operation_lock_held": True,
        }
        for call in (get_call, put_call, ack_call)
    )
    async with session_factory() as db:
        current = await db.get(Task, t.id)
    assert current.status == "completed"


async def test_stop_session_receipt_readback_failure_stays_durable_pending(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )

    async def protocol(_task, method, _path, *_args, **_kwargs):
        raise HTTPException(502, "readback lost")

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/stop-session")

    assert response.status_code == 503
    assert proxy.proxy_to_worker.await_count == 1
    assert proxy.proxy_to_worker.await_args.args[1] == "GET"
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = (
            await worker_termination_module.active_worker_task_termination_receipt(
                db,
                task.id,
            )
        )
    assert current.status == "pending"
    assert not worker_relay_module.has_worker_execution_quarantine(
        current.metadata_
    )
    assert receipt.status == "pending_remote"


async def test_delete_worker_task_remote_first_then_cleans_exact_manager_mirror(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                pty_background_generation=(
                    worker_relay_module
                    ._WORKER_BACKGROUND_MIRROR_SENTINEL
                )
            )
        )
        log = LogEntry(
            task_id=task.id,
            event_type="message",
            content="remote result",
        )
        monitor = MonitorSession(
            task_id=task.id,
            remote_id=17,
            description="stale mirror",
            status="running",
        )
        db.add_all([log, monitor])
        await db.flush()
        db.add(
            MonitorCheck(
                monitor_session_id=monitor.id,
                check_number=1,
                status="ok",
            )
        )
        await db.commit()

    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = {
        "ok": True,
        "plan_cascade_protocol": 1,
        "deleted_plan_ids": [],
        "remaining_target_plan_ids": [],
    }
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    proxy.proxy_to_worker.assert_awaited_once()
    method, path = proxy.proxy_to_worker.call_args.args[1:3]
    assert method == "DELETE"
    assert path == f"/api/tasks/{task.id}"
    assert proxy.proxy_to_worker.call_args.kwargs == {
        "require_json": True,
        "allow_task_absent": True,
        "operation_lock_held": True,
        "quarantine_on_transport_uncertainty": True,
        "require_task_incarnation_fence": True,
    }
    proxy.relay.unsubscribe_task.assert_called_once_with(worker.id, task.id)
    async with session_factory() as db:
        assert await db.get(Task, task.id) is None
        assert not (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
        assert not (
            await db.execute(
                select(MonitorSession).where(
                    MonitorSession.task_id == task.id
                )
            )
        ).scalars().all()
        assert not (await db.execute(select(MonitorCheck))).scalars().all()


async def test_delete_worker_task_requires_exact_plan_cascade_receipt(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    async with session_factory() as db:
        plan = Plan(
            title="Manager Plan mirror",
            initial_request="Plan before deleting",
            target_task_id=task.id,
            worker_id=worker.id,
            pipeline_config={},
        )
        db.add(plan)
        await db.commit()
        plan_id = plan.id

    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = {
        "ok": True,
        "plan_cascade_protocol": 1,
        "deleted_plan_ids": [plan_id],
        "remaining_target_plan_ids": [],
    }
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    assert response.json() == {
        "ok": True,
        "plan_cascade_protocol": 1,
        "deleted_plan_ids": [plan_id],
        "remaining_target_plan_ids": [],
    }
    async with session_factory() as db:
        assert await db.get(Task, task.id) is None
        assert await db.get(Plan, plan_id) is None


async def test_delete_worker_task_old_worker_cannot_strand_manager_plan(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    async with session_factory() as db:
        plan = Plan(
            title="Preserve without cascade proof",
            initial_request="Do not strand me",
            target_task_id=task.id,
            worker_id=worker.id,
            pipeline_config={},
        )
        db.add(plan)
        await db.commit()
        plan_id = plan.id

    requests = _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(200, {"plan_cascade_protocol": 0}),
    )
    relay = Mock()
    relay.subscribe_task = AsyncMock()
    proxy = WorkerProxy(session_factory, relay)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 503
    assert [request[0] for request in requests] == ["GET"]
    assert requests[0][1].endswith("/api/system/config")
    relay.unsubscribe_task.assert_not_called()
    async with session_factory() as db:
        assert await db.get(Task, task.id) is not None
        assert await db.get(Plan, plan_id) is not None
        receipt = await db.scalar(
            select(worker_termination_module.WorkerTaskTerminationReceipt)
            .where(
                worker_termination_module.WorkerTaskTerminationReceipt.task_id
                == task.id,
                worker_termination_module.WorkerTaskTerminationReceipt.operation
                == "delete",
            )
        )
        assert receipt.status == "rejected"
        assert receipt.active_task_id is None


async def test_delete_worker_task_lost_ack_converges_by_read_only_plan_audit(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    async with session_factory() as db:
        plan = Plan(
            title="Audit after lost ACK",
            initial_request="Delete exactly once",
            target_task_id=task.id,
            worker_id=worker.id,
            pipeline_config={},
        )
        db.add(plan)
        await db.commit()
        plan_id = plan.id

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = [
        WorkerTaskMutationOutcomeUncertainError(
            "DELETE response was lost",
            status_code=502,
        ),
        {
            "plan_cascade_protocol": 1,
            "task_exists": False,
            "remaining_target_plan_ids": [],
        },
    ]
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    assert proxy.proxy_to_worker.await_count == 2
    assert all(
        call.kwargs["require_task_incarnation_fence"] is True
        for call in proxy.proxy_to_worker.await_args_list
    )
    proxy.relay.unsubscribe_task.assert_called_once_with(worker.id, task.id)
    async with session_factory() as db:
        assert await db.get(Task, task.id) is None
        assert await db.get(Plan, plan_id) is None


async def test_delete_worker_task_retry_converges_when_remote_is_already_absent(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    requests = _install_proxy_transport(
        monkeypatch,
        [
            _ProxyResponse(200, {
                "plan_cascade_protocol": 1,
                "worker_task_incarnation_proxy_version": 1,
            }),
            _ProxyResponse(200, {
                "worker_task_incarnation_proxy_version": 1,
            }),
            _ProxyResponse(404, {"detail": "Task not found"}),
            _ProxyResponse(200, {
                "worker_task_incarnation_proxy_version": 1,
            }),
            _ProxyResponse(
                200,
                {
                    "plan_cascade_protocol": 1,
                    "task_exists": False,
                    "remaining_target_plan_ids": [],
                },
            ),
        ],
    )
    relay = Mock()
    relay.subscribe_task = AsyncMock()
    proxy = WorkerProxy(session_factory, relay)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    assert [request[0] for request in requests] == [
        "GET",
        "GET",
        "DELETE",
        "GET",
        "GET",
    ]
    assert requests[0][1].endswith("/api/system/config")
    assert requests[1][1].endswith("/api/system/config")
    assert requests[2][1].endswith(f"/api/tasks/{task.id}")
    assert requests[3][1].endswith("/api/system/config")
    assert requests[4][1].endswith(
        f"/api/tasks/{task.id}/plan-delete-audit"
    )
    assert requests[2][2]["headers"]["X-CCM-Task-Incarnation"] == (
        task.incarnation_id
    )
    assert requests[4][2]["headers"]["X-CCM-Task-Incarnation"] == (
        task.incarnation_id
    )
    relay.unsubscribe_task.assert_called_once_with(worker.id, task.id)
    async with session_factory() as db:
        assert await db.get(Task, task.id) is None


async def test_delete_worker_task_does_not_treat_unrelated_404_as_confirmation(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    requests = _install_proxy_transport(
        monkeypatch,
        [
            _ProxyResponse(200, {
                "plan_cascade_protocol": 1,
                "worker_task_incarnation_proxy_version": 1,
            }),
            _ProxyResponse(200, {
                "worker_task_incarnation_proxy_version": 1,
            }),
            _ProxyResponse(404, {"detail": "Route not found"}),
            _ProxyResponse(200, {
                "worker_task_incarnation_proxy_version": 1,
            }),
            _ProxyResponse(404, {"detail": "Route not found"}),
        ],
    )
    relay = Mock()
    relay.subscribe_task = AsyncMock()
    proxy = WorkerProxy(session_factory, relay)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 503
    assert [request[0] for request in requests] == [
        "GET",
        "GET",
        "DELETE",
        "GET",
        "GET",
    ]
    relay.unsubscribe_task.assert_not_called()
    async with session_factory() as db:
        assert await db.get(Task, task.id) is not None


@pytest.mark.parametrize(
    "remote_outcome",
    [
        HTTPException(502, "Worker unreachable"),
        {"ok": False},
        {"deleted": True},
    ],
)
async def test_delete_worker_task_preserves_manager_mirror_without_confirmation(
    client,
    session_factory,
    monkeypatch,
    remote_outcome,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
    )
    async with session_factory() as db:
        db.add(
            LogEntry(
                task_id=task.id,
                event_type="system_event",
                content="retain me",
            )
        )
        await db.commit()

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = [
        remote_outcome,
        WorkerEndpointNotFoundError("plan-delete-audit"),
    ]
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 503
    assert proxy.proxy_to_worker.await_count == 2
    assert [
        call.args[1:3] for call in proxy.proxy_to_worker.await_args_list
    ] == [
        ("DELETE", f"/api/tasks/{task.id}"),
        ("GET", f"/api/tasks/{task.id}/plan-delete-audit"),
    ]
    assert all(
        call.kwargs["require_task_incarnation_fence"] is True
        for call in proxy.proxy_to_worker.await_args_list
    )
    proxy.relay.unsubscribe_task.assert_not_called()
    async with session_factory() as db:
        assert await db.get(Task, task.id) is not None
        assert (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().one().content == "retain me"


async def test_delete_worker_task_rejects_delayed_relay_generation_update(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )

    release_relay = asyncio.Event()
    relay_task = None

    async def delayed_relay_generation():
        await release_relay.wait()
        async with session_factory() as db:
            changed = await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    status="in_progress",
                    retry_count=Task.retry_count + 1,
                )
            )
            await db.commit()
            return changed.rowcount

    async def remote_delete_then_queue_relay(*_args, **_kwargs):
        nonlocal relay_task
        relay_task = asyncio.create_task(delayed_relay_generation())
        return {
            "ok": True,
            "plan_cascade_protocol": 1,
            "deleted_plan_ids": [],
            "remaining_target_plan_ids": [],
        }

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = remote_delete_then_queue_relay
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    proxy.relay.unsubscribe_task.assert_called_once_with(worker.id, task.id)
    release_relay.set()
    assert await relay_task == 0
    async with session_factory() as db:
        assert await db.get(Task, task.id) is None


async def test_delete_worker_task_rejects_delayed_worker_move(
    client,
    session_factory,
    monkeypatch,
):
    source = await _mk_worker(session_factory)
    destination = await _mk_worker(
        session_factory,
        private_ip="10.0.0.10",
    )
    task = await _mk_task(
        session_factory,
        worker_id=source.id,
        status="completed",
    )

    release_move = asyncio.Event()
    move_task = None

    async def delayed_worker_move():
        await release_move.wait()
        async with session_factory() as db:
            changed = await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(worker_id=destination.id)
            )
            await db.commit()
            return changed.rowcount

    async def remote_delete_then_queue_move(*_args, **_kwargs):
        nonlocal move_task
        move_task = asyncio.create_task(delayed_worker_move())
        return {
            "ok": True,
            "plan_cascade_protocol": 1,
            "deleted_plan_ids": [],
            "remaining_target_plan_ids": [],
        }

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = remote_delete_then_queue_move
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    proxy.relay.unsubscribe_task.assert_called_once_with(source.id, task.id)
    release_move.set()
    assert await move_task == 0
    async with session_factory() as db:
        preserved = await db.get(Task, task.id)
        assert preserved is None


async def test_monitor_delete_translates_remote_id(client, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    async with session_factory() as db:
        ms = MonitorSession(
            task_id=t.id,
            remote_id=5,
            agent_type="monitor",
            source="ccm",
            description="m",
            status="running",
            meta=json.dumps({
                "ccm_worker_mirror": {
                    "worker_id": w.id,
                    "task_incarnation_id": t.incarnation_id,
                    "remote_id": 5,
                }
            }),
        )
        db.add(ms)
        await db.commit()
        await db.refresh(ms)

    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = {"ok": True}
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    resp = await client.delete(f"/api/tasks/{t.id}/monitor-sessions/{ms.id}")
    assert resp.status_code == 200
    path = proxy.proxy_to_worker.call_args.args[2]
    assert path.endswith("/monitor-sessions/5")  # 用 remote_id，不是本地 id
    async with session_factory() as db:
        assert (await db.get(MonitorSession, ms.id)).status == "cancelled"


# === WS 认证 ===


async def test_ws_token_check():
    from backend.api.ws import _ws_token_ok
    from backend.config import settings

    class FakeWS:
        def __init__(self, headers=None, qp=None):
            self.headers = headers or {}
            self.query_params = qp or {}

    old = settings.auth_token
    try:
        settings.auth_token = ""
        assert _ws_token_ok(FakeWS()) is True  # 未配置 token 放行
        settings.auth_token = "secret"
        assert _ws_token_ok(FakeWS()) is False
        assert _ws_token_ok(FakeWS(headers={"authorization": "Bearer secret"})) is True
        assert _ws_token_ok(FakeWS(qp={"token": "secret"})) is True
        assert _ws_token_ok(FakeWS(qp={"token": "wrong"})) is False
    finally:
        settings.auth_token = old


# ---------------------------------------------------------------------------
# Backfill dedup — duplicate-message-on-reconnect regression
# ---------------------------------------------------------------------------

def _entry(et="message", role="assistant", content=None, tool_name=None,
           tool_input=None, tool_output=None, loop_iteration=None,
           native_turn_id=None):
    return {
        "event_type": et, "role": role, "content": content,
        "tool_name": tool_name, "tool_input": tool_input,
        "tool_output": tool_output, "loop_iteration": loop_iteration,
        "native_turn_id": native_turn_id,
    }


class TestBackfillDedup:
    """`_missing_by_fingerprint` must not re-insert already-present entries —
    the count-based `remote[local_count:]` slicing did exactly that whenever a
    gap was mid-stream (not tail-only) or the live relay raced the backfill."""

    def test_tail_only_missing(self):
        from backend.services.worker_relay import _missing_by_fingerprint
        remote = [_entry(content=str(i)) for i in range(5)]
        local = remote[:3]
        missing = _missing_by_fingerprint(local, remote)
        assert [m["content"] for m in missing] == ["3", "4"]

    def test_mid_stream_gap_does_not_duplicate(self):
        # local missed entry "2" in the middle but has "3"; count-based slicing
        # (remote[local_count=3:]) would re-insert "3" AND drop "2".
        from backend.services.worker_relay import _missing_by_fingerprint
        remote = [_entry(content=str(i)) for i in range(5)]  # 0,1,2,3,4
        local = [remote[0], remote[1], remote[3]]            # missing "2"
        missing = _missing_by_fingerprint(local, remote)
        assert [m["content"] for m in missing] == ["2", "4"]  # "3" NOT re-inserted

    def test_fully_synced_inserts_nothing(self):
        from backend.services.worker_relay import _missing_by_fingerprint
        remote = [_entry(content=str(i)) for i in range(4)]
        assert _missing_by_fingerprint(list(remote), remote) == []

    def test_truncated_tool_output_still_matches(self):
        # remote tool_output is truncated by the history endpoint; the local copy
        # is full. Prefix-capped fingerprint must still treat them as identical.
        from backend.services.worker_relay import _missing_by_fingerprint
        full = "x" * 50_000
        truncated = ("x" * 20_000) + "\n…(truncated)"
        local = [_entry(et="tool_result", tool_name="bash", tool_output=full)]
        remote = [_entry(et="tool_result", tool_name="bash", tool_output=truncated)]
        assert _missing_by_fingerprint(local, remote) == []

    def test_distinct_long_observable_payloads_do_not_prefix_collide(self):
        from backend.services.worker_relay import _missing_by_fingerprint

        shared = "x" * 1_500
        local = [_entry(content=shared + "local")]
        remote = [local[0], _entry(content=shared + "remote")]
        assert _missing_by_fingerprint(local, remote) == [remote[1]]

        local_tool = [_entry(tool_input=shared + "local")]
        remote_tool = [local_tool[0], _entry(tool_input=shared + "remote")]
        assert _missing_by_fingerprint(local_tool, remote_tool) == [
            remote_tool[1]
        ]

    def test_duplicate_fingerprints_preserve_multiplicity(self):
        from backend.services.worker_relay import _missing_by_fingerprint
        remote = [_entry(content="same") for _ in range(3)]
        local = [_entry(content="same")]  # only one present
        missing = _missing_by_fingerprint(local, remote)
        assert len(missing) == 2  # insert the two still-missing copies

    def test_identical_content_from_distinct_native_turns_is_preserved(self):
        from backend.services.worker_relay import _missing_by_fingerprint

        local = [_entry(content="same", native_turn_id="turn-a")]
        remote = [
            _entry(content="same", native_turn_id="turn-a"),
            _entry(content="same", native_turn_id="turn-b"),
        ]

        assert _missing_by_fingerprint(local, remote) == [remote[1]]
