import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.websockets import WebSocketDisconnect

from backend.config import settings
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.user import User
from backend.models.worker import Worker
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
from backend.services.task_creation import (
    delegated_task_execution_principal_values,
    task_execution_principal_values,
)
from backend.services.worker_launch_admission import (
    WORKER_CONTEXT_RETRY_MARKER_METADATA_KEY,
    WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL,
    WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY,
    WORKER_LAUNCH_ADMISSION_EVENT,
    WorkerLaunchAdmissionError,
    accept_worker_launch_admission_response,
    build_worker_launch_admission_response,
    build_codex_context_preflight_relay_proof,
    canonical_payload_digest,
    parse_worker_launch_admission_request,
    request_worker_launch_admission,
)
from backend.services.worker_proxy import get_task_operation_lock
from backend.services.worker_relay import (
    WORKER_MANUAL_RETRY_PROTOCOL,
    WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY,
    WorkerRelay,
    canonical_delegated_principal_payload,
    worker_manual_retry_request_digest,
    worker_manual_retry_source_generation,
    worker_principal_digest,
    worker_task_generation,
)


def _delegated_user(user_id: int, role: str) -> dict[str, object]:
    return delegated_task_execution_principal_values(
        user_id=user_id,
        role=role,
        principal_kind="user",
    )


def _delegated_deployment_token() -> dict[str, object]:
    return delegated_task_execution_principal_values(
        user_id=None,
        role="super_admin",
        principal_kind="deployment_token",
    )


def _system_principal() -> dict[str, object]:
    return task_execution_principal_values(
        user_id=None,
        role="member",
        principal_kind="system",
    )


def test_worker_deployment_token_is_not_a_runtime_principal(monkeypatch):
    from backend.api.deps import task_execution_principal_from_request

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    request = SimpleNamespace(state=SimpleNamespace(
        user_id=None,
        user_role="super_admin",
        auth_type="token",
    ))

    assert task_execution_principal_from_request(request) == {
        "execution_user_id": None,
        "execution_user_role": "member",
        "execution_mode": "sandbox",
        "execution_principal_kind": "system",
    }


def _request(
    *,
    task_id: int,
    incarnation_id: str,
    retry_count: int,
    turn_generation: int,
    principal: dict[str, object],
    actual_transport: str = "claude_exec",
    context_retry: dict[str, object] | None = None,
) -> tuple[dict, object]:
    payload = {
        "event_type": WORKER_LAUNCH_ADMISSION_EVENT,
        "protocol_version": WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL,
        "request_id": "a" * 32,
        "task_id": task_id,
        "incarnation_id": incarnation_id,
        "retry_count": retry_count,
        "turn_generation": turn_generation,
        "actual_transport": actual_transport,
        "execution_principal": principal,
        "principal_digest": canonical_payload_digest(principal),
    }
    if context_retry is not None:
        payload["context_retry"] = context_retry
    payload["request_digest"] = canonical_payload_digest(payload)
    parsed = parse_worker_launch_admission_request(payload)
    assert parsed is not None
    return payload, parsed


async def _manager_rows(
    db_factory,
    *,
    task_status: str = "in_progress",
    retry_count: int = 0,
    turn_generation: int = 1,
    principal_user_id: int = 1,
    principal_role: str = "admin",
):
    async with db_factory() as db:
        worker = Worker(
            name="launch-admission-worker",
            status="ready",
            private_ip="10.0.0.50",
            auth_token="worker-control-token",
        )
        principal = User(
            id=principal_user_id,
            email=f"principal-{principal_user_id}@example.com",
            name="Principal",
            password_hash="hash",
            role=principal_role,
            is_active=True,
        )
        db.add_all([worker, principal])
        await db.flush()
        task = Task(
            title="delegated launch",
            status=task_status,
            worker_id=worker.id,
            retry_count=retry_count,
            turn_generation=turn_generation,
            **task_execution_principal_values(
                user_id=principal_user_id,
                role=principal_role,
                principal_kind="user",
            ),
        )
        db.add(task)
        await db.commit()
        return worker, task, principal


@pytest.mark.asyncio
async def test_worker_one_shot_launch_permit_is_signed_and_not_replayable(
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-control-token")
    observed_response = None

    class LoopbackBroadcaster:
        async def broadcast(self, channel, payload):
            nonlocal observed_response
            assert channel == "task:41"
            parsed = parse_worker_launch_admission_request(payload)
            assert parsed is not None
            observed_response = build_worker_launch_admission_response(
                parsed,
                worker_id=7,
                admitted=True,
                reason_code="admitted",
                control_token=settings.auth_token,
            )
            assert accept_worker_launch_admission_response(
                observed_response,
                control_token=settings.auth_token,
            )

    response = await request_worker_launch_admission(
        broadcaster=LoopbackBroadcaster(),
        task_id=41,
        incarnation_id="b" * 32,
        retry_count=2,
        turn_generation=9,
        actual_transport="claude_exec",
        execution_principal=_delegated_user(11, "admin"),
    )
    assert response["worker_id"] == 7
    assert response["admitted"] is True
    assert observed_response is not None
    assert not accept_worker_launch_admission_response(
        observed_response,
        control_token=settings.auth_token,
    )


@pytest.mark.asyncio
async def test_worker_launch_permit_bad_signature_or_timeout_fails_closed(
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-control-token")

    class ForgedBroadcaster:
        async def broadcast(self, _channel, payload):
            parsed = parse_worker_launch_admission_request(payload)
            assert parsed is not None
            forged = build_worker_launch_admission_response(
                parsed,
                worker_id=9,
                admitted=True,
                reason_code="admitted",
                control_token="wrong-token",
            )
            assert not accept_worker_launch_admission_response(
                forged,
                control_token=settings.auth_token,
            )

    with pytest.raises(WorkerLaunchAdmissionError, match="timed out"):
        await request_worker_launch_admission(
            broadcaster=ForgedBroadcaster(),
            task_id=42,
            incarnation_id="c" * 32,
            retry_count=0,
            turn_generation=1,
            actual_transport="codex_exec",
            execution_principal=_delegated_user(12, "member"),
            timeout_seconds=0.01,
        )


@pytest.mark.asyncio
async def test_manager_initial_launch_permit_revalidates_user_and_sends_after_lock(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, principal = await _manager_rows(db_factory)
    payload, _parsed = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        principal=_delegated_user(principal.id, "admin"),
    )
    relay = WorkerRelay(db_factory, MagicMock())

    class Socket:
        def __init__(self):
            self.sent = []

        async def send(self, raw):
            assert not get_task_operation_lock(task.id).locked()
            self.sent.append(json.loads(raw))

    socket = Socket()
    relay._ws[worker.id] = socket
    relay._tasks[worker.id] = {task.id}
    await relay._handle(
        {"channel": f"task:{task.id}", "data": payload},
        worker,
        ws=socket,
    )
    assert socket.sent[0]["admitted"] is True
    assert socket.sent[0]["worker_id"] == worker.id

    async with db_factory() as db:
        current_task = await db.get(Task, task.id)
        launch_marker = current_task.metadata_[
            WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY
        ]
        assert launch_marker == {
            "version": 1,
            "worker_id": worker.id,
            "incarnation_id": task.incarnation_id,
            "retry_count": task.retry_count,
            "turn_generation": task.turn_generation,
            "principal_digest": canonical_payload_digest(
                _delegated_user(principal.id, "admin")
            ),
            "actual_transport": "claude_exec",
        }
        current = await db.get(User, principal.id)
        current.role = "member"
        await db.commit()

    payload["request_id"] = "d" * 32
    payload["request_digest"] = canonical_payload_digest(
        {key: value for key, value in payload.items() if key != "request_digest"}
    )
    await relay._handle(
        {"channel": f"task:{task.id}", "data": payload},
        worker,
        ws=socket,
    )
    assert socket.sent[1]["admitted"] is False
    assert socket.sent[1]["reason_code"] == "principal_revoked"


@pytest.mark.asyncio
async def test_manager_launch_permit_revalidates_user_after_request_arrival(
    db_factory,
    monkeypatch,
):
    """A revocation while the relay request is queued vetoes provider launch."""

    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, principal = await _manager_rows(db_factory)
    payload, _parsed = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        principal=_delegated_user(principal.id, "admin"),
    )
    relay = WorkerRelay(db_factory, MagicMock())

    class Socket:
        def __init__(self):
            self.sent = []

        async def send(self, raw):
            self.sent.append(json.loads(raw))

    socket = Socket()
    relay._ws[worker.id] = socket
    relay._tasks[worker.id] = {task.id}
    operation_lock = get_task_operation_lock(task.id)
    await operation_lock.acquire()
    handling = asyncio.create_task(relay._handle(
        {"channel": f"task:{task.id}", "data": payload},
        worker,
        ws=socket,
    ))
    try:
        await asyncio.sleep(0)
        assert socket.sent == []
        async with db_factory() as db:
            current = await db.get(User, principal.id)
            current.is_active = False
            await db.commit()
    finally:
        operation_lock.release()
    await asyncio.wait_for(handling, timeout=2)

    assert socket.sent[0]["admitted"] is False
    assert socket.sent[0]["reason_code"] == "principal_revoked"


@pytest.mark.asyncio
async def test_manager_launch_permit_revalidates_worker_after_request_arrival(
    db_factory,
    monkeypatch,
):
    """A stop transition which wins before the final fence vetoes launch."""

    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, principal = await _manager_rows(db_factory)
    payload, _parsed = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        principal=_delegated_user(principal.id, "admin"),
    )
    relay = WorkerRelay(db_factory, MagicMock())

    class Socket:
        def __init__(self):
            self.sent = []

        async def send(self, raw):
            self.sent.append(json.loads(raw))

    socket = Socket()
    relay._ws[worker.id] = socket
    relay._tasks[worker.id] = {task.id}
    operation_lock = get_task_operation_lock(task.id)
    await operation_lock.acquire()
    handling = asyncio.create_task(relay._handle(
        {"channel": f"task:{task.id}", "data": payload},
        worker,
        ws=socket,
    ))
    try:
        await asyncio.sleep(0)
        assert socket.sent == []
        async with db_factory() as db:
            current = await db.get(Worker, worker.id)
            current.status = "stopping"
            await db.commit()
    finally:
        operation_lock.release()
    await asyncio.wait_for(handling, timeout=2)

    assert socket.sent[0]["admitted"] is False
    assert socket.sent[0]["reason_code"] == "worker_not_ready"


@pytest.mark.asyncio
async def test_manager_launch_permit_rejects_ready_destroy_recovery_worker(
    db_factory,
    monkeypatch,
):
    """A blocked destroy may relay reconciliation but cannot launch a model."""

    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, principal = await _manager_rows(db_factory)
    async with db_factory() as db:
        current = await db.get(Worker, worker.id)
        current.bootstrap_step = "destroy"
        await db.commit()
    _payload, request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        principal=_delegated_user(principal.id, "admin"),
    )
    relay = WorkerRelay(db_factory, MagicMock())

    admitted, reason = await relay._authorize_worker_launch_admission(
        request,
        worker,
    )

    assert (admitted, reason) == (False, "worker_not_ready")


@pytest.mark.asyncio
async def test_manager_initial_deployment_token_launch_permit(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, _principal = await _manager_rows(db_factory)
    native = task_execution_principal_values(
        user_id=None,
        role="super_admin",
        principal_kind="deployment_token",
    )
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        for field, value in native.items():
            setattr(current, field, value)
        await db.commit()

    _payload, request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        principal=_delegated_deployment_token(),
    )
    relay = WorkerRelay(db_factory, MagicMock())

    admitted, reason = await relay._authorize_worker_launch_admission(
        request,
        worker,
    )

    assert (admitted, reason) == (True, "admitted")


@pytest.mark.asyncio
async def test_manager_initial_system_launch_permit_revalidates_generation(
    db_factory,
    monkeypatch,
):
    """Sandboxed system mirrors still need Manager final-boundary approval."""

    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, _principal = await _manager_rows(db_factory)
    native = _system_principal()
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        for field, value in native.items():
            setattr(current, field, value)
        await db.commit()

    _payload, request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        principal=native,
    )
    relay = WorkerRelay(db_factory, MagicMock())

    admitted, reason = await relay._authorize_worker_launch_admission(
        request,
        worker,
    )

    assert (admitted, reason) == (True, "admitted")


async def _append_manager_context_preflight_tail(
    db_factory,
    *,
    task_id: int,
    retry_count: int,
    turn_generation: int,
    source_log_id: int,
    variant: str = "exact",
) -> None:
    transport = "codex_app_server"
    started_raw = {"type": "turn.started"}
    started_event = {
        "event_type": "system_event",
        "role": None,
        "content": "turn.started",
        "is_error": False,
    }
    started_proof = build_codex_context_preflight_relay_proof(
        started_raw,
        started_event,
        retry_count=retry_count,
        turn_generation=turn_generation,
        source_log_id=source_log_id,
        actual_transport=transport,
    )
    assert started_proof is not None
    failure_message = "The request could not be completed."
    failed_raw = {
        "type": "turn.failed",
        "error": {
            "message": failure_message,
            "codexErrorInfo": "contextWindowExceeded",
        },
    }
    failed_event = {
        "event_type": "system_event",
        "role": None,
        "content": failure_message,
        "is_error": True,
    }
    failed_proof = build_codex_context_preflight_relay_proof(
        failed_raw,
        failed_event,
        retry_count=retry_count,
        turn_generation=turn_generation,
        source_log_id=source_log_id,
        actual_transport=transport,
    )
    assert failed_proof is not None
    async with db_factory() as db:
        db.add(
            LogEntry(
                instance_id=None,
                task_id=task_id,
                task_retry_count=retry_count,
                task_turn_generation=turn_generation,
                turn_scope="foreground",
                event_type="system_event",
                role=None,
                content="turn.started",
                raw_json=json.dumps(started_proof),
                is_error=False,
            )
        )
        if variant == "activity":
            db.add(
                LogEntry(
                    instance_id=None,
                    task_id=task_id,
                    task_retry_count=retry_count,
                    task_turn_generation=turn_generation,
                    turn_scope="foreground",
                    event_type="message",
                    role="assistant",
                    content="agent activity happened",
                    raw_json=None,
                    is_error=False,
                )
            )
        db.add(
            LogEntry(
                instance_id=None,
                task_id=task_id,
                task_retry_count=retry_count,
                task_turn_generation=turn_generation,
                turn_scope="foreground",
                event_type="system_event",
                role=None,
                content=(
                    "ordinary provider failure"
                    if variant == "ordinary_failure"
                    else failure_message
                ),
                raw_json=(
                    None
                    if variant == "ordinary_failure"
                    else (
                        "{malformed"
                        if variant == "malformed"
                        else json.dumps(failed_proof)
                    )
                ),
                is_error=True,
            )
        )
        await db.commit()


async def _manager_context_retry_rows(
    db_factory,
    *,
    tail_variant: str = "exact",
):
    worker, task, principal = await _manager_rows(
        db_factory,
        task_status="failed",
        retry_count=0,
        turn_generation=4,
    )
    delegated = _delegated_user(principal.id, "admin")
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        current.provider = "codex"
        source = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="continue",
            is_error=False,
        )
        db.add(source)
        await db.flush()
        db.add(
            WorkerTurnHandoffReceipt(
                handoff_id="c" * 32,
                task_id=task.id,
                source_log_id=source.id,
                side="manager",
                worker_id=worker.id,
                retry_count=0,
                from_generation=3,
                status="completed",
                request_payload=delegated,
                request_digest=canonical_payload_digest(delegated),
            )
        )
        await db.commit()
    await _append_manager_context_preflight_tail(
        db_factory,
        task_id=task.id,
        retry_count=0,
        turn_generation=4,
        source_log_id=704,
        variant=tail_variant,
    )
    _payload, request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=5,
        principal=delegated,
        actual_transport="codex_app_server",
        context_retry={
            "authority_id": "d" * 32,
            "retry_count": 0,
            "from_generation": 4,
            "source_log_id": 704,
            "claimed_source_log_id": 705,
        },
    )
    return worker, task, request


@pytest.mark.asyncio
async def test_manager_context_retry_requires_exact_structured_preflight_and_chains(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, request = await _manager_context_retry_rows(db_factory)
    relay = WorkerRelay(db_factory, MagicMock())

    assert await relay._authorize_worker_launch_admission(
        request, worker
    ) == (True, "admitted")
    # A lost response may repeat the same request, but only against the exact
    # durable marker committed with G+1.
    assert await relay._authorize_worker_launch_admission(
        request, worker
    ) == (True, "admitted")
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "executing"
        assert current.turn_generation == 5
        marker = current.metadata_[WORKER_CONTEXT_RETRY_MARKER_METADATA_KEY]
        assert marker["claimed_source_log_id"] == 705
        launch_marker = current.metadata_[
            WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY
        ]
        assert launch_marker["turn_generation"] == 5
        assert launch_marker["actual_transport"] == "codex_app_server"
        receipt = await db.get(WorkerTurnHandoffReceipt, "c" * 32)
        assert receipt.status == "completed"

        current.status = "failed"
        await db.commit()
    await _append_manager_context_preflight_tail(
        db_factory,
        task_id=task.id,
        retry_count=0,
        turn_generation=5,
        source_log_id=705,
    )
    _payload, chained_request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=6,
        principal=_delegated_user(1, "admin"),
        actual_transport="codex_app_server",
        context_retry={
            "authority_id": "e" * 32,
            "retry_count": 0,
            "from_generation": 5,
            "source_log_id": 705,
            "claimed_source_log_id": 706,
        },
    )
    assert await relay._authorize_worker_launch_admission(
        chained_request, worker
    ) == (True, "admitted")
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        assert current.turn_generation == 6


@pytest.mark.asyncio
async def test_manager_initial_context_retry_uses_exact_launch_lineage(
    db_factory,
    monkeypatch,
):
    """Generation one has no ordinary handoff but retains launch authority."""

    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, principal = await _manager_rows(
        db_factory,
        task_status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    delegated = _delegated_user(principal.id, "admin")
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        current.provider = "codex"
        source = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="initial turn",
            is_error=False,
        )
        claimed = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="compact retry",
            is_error=False,
        )
        db.add_all([source, claimed])
        await db.flush()
        current.turn_source_log_id = source.id
        await db.commit()
        source_id = source.id
        claimed_id = claimed.id

    _payload, initial_request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=1,
        principal=delegated,
        actual_transport="codex_app_server",
    )
    relay = WorkerRelay(db_factory, MagicMock())
    assert await relay._authorize_worker_launch_admission(
        initial_request, worker
    ) == (True, "admitted")

    async with db_factory() as db:
        current = await db.get(Task, task.id)
        current.status = "failed"
        await db.commit()
    await _append_manager_context_preflight_tail(
        db_factory,
        task_id=task.id,
        retry_count=0,
        turn_generation=1,
        source_log_id=source_id,
    )
    _payload, retry_request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=2,
        principal=delegated,
        actual_transport="codex_app_server",
        context_retry={
            "authority_id": "f" * 32,
            "retry_count": 0,
            "from_generation": 1,
            "source_log_id": source_id,
            "claimed_source_log_id": claimed_id,
        },
    )
    assert await relay._authorize_worker_launch_admission(
        retry_request, worker
    ) == (True, "admitted")
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "executing"
        assert current.turn_generation == 2
        assert current.metadata_[
            WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY
        ]["turn_generation"] == 2
        receipts = list(
            (
                await db.execute(
                    WorkerTurnHandoffReceipt.__table__.select().where(
                        WorkerTurnHandoffReceipt.task_id == task.id
                    )
                )
            ).all()
        )
        assert receipts == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tail_variant",
    ["ordinary_failure", "activity", "malformed"],
)
async def test_manager_context_retry_rejects_unproven_preflight_tail(
    db_factory,
    monkeypatch,
    tail_variant,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, request = await _manager_context_retry_rows(
        db_factory,
        tail_variant=tail_variant,
    )
    relay = WorkerRelay(db_factory, MagicMock())

    assert await relay._authorize_worker_launch_admission(
        request, worker
    ) == (False, "context_preflight_unproven")
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "failed"
        assert current.turn_generation == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "service_tier", "actual_transport"),
    [
        ("claude", "default", "codex_exec"),
        ("codex", "priority", "codex_exec"),
    ],
)
async def test_manager_launch_permit_rejects_disallowed_transport(
    db_factory,
    monkeypatch,
    provider,
    service_tier,
    actual_transport,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, principal = await _manager_rows(db_factory)
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        current.provider = provider
        current.codex_service_tier = service_tier
        await db.commit()
    _payload, request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        principal=_delegated_user(principal.id, "admin"),
        actual_transport=actual_transport,
    )
    relay = WorkerRelay(db_factory, MagicMock())

    admitted, reason = await relay._authorize_worker_launch_admission(
        request,
        worker,
    )

    assert (admitted, reason) == (False, "transport_changed")


@pytest.mark.asyncio
async def test_manager_exact_launch_marker_advances_across_followups(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, principal = await _manager_rows(
        db_factory,
        task_status="in_progress",
        retry_count=0,
        turn_generation=1,
    )
    delegated = _delegated_user(principal.id, "admin")
    relay = WorkerRelay(db_factory, MagicMock())

    _payload, initial_request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=1,
        principal=delegated,
        actual_transport="claude_exec",
    )
    assert await relay._authorize_worker_launch_admission(
        initial_request, worker
    ) == (True, "admitted")
    _payload, changed_transport = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=1,
        principal=delegated,
        actual_transport="claude_pty",
    )
    assert await relay._authorize_worker_launch_admission(
        changed_transport, worker
    ) == (False, "generation_changed")

    async def prepare_handoff(*, from_generation: int, handoff_id: str) -> None:
        async with db_factory() as db:
            current = await db.get(Task, task.id)
            current.status = "completed"
            current.turn_generation = from_generation
            source = LogEntry(
                task_id=task.id,
                event_type="user_message",
                role="user",
                content=f"follow up {from_generation + 1}",
            )
            db.add(source)
            await db.flush()
            current.worker_turn_handoff_id = handoff_id
            current.worker_turn_handoff_worker_id = worker.id
            current.worker_turn_handoff_retry_count = 0
            current.worker_turn_handoff_from_generation = from_generation
            current.worker_turn_handoff_source_log_id = source.id
            current.worker_turn_handoff_acknowledged = False
            db.add(
                WorkerTurnHandoffReceipt(
                    handoff_id=handoff_id,
                    task_id=task.id,
                    source_log_id=source.id,
                    side="manager",
                    worker_id=worker.id,
                    retry_count=0,
                    from_generation=from_generation,
                    status="prepared",
                    request_payload=delegated,
                    request_digest=canonical_payload_digest(delegated),
                )
            )
            await db.commit()

    await prepare_handoff(from_generation=1, handoff_id="1" * 32)
    _payload, second_request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=2,
        principal=delegated,
        actual_transport="claude_pty",
    )
    assert await relay._authorize_worker_launch_admission(
        second_request, worker
    ) == (True, "admitted")

    await prepare_handoff(from_generation=2, handoff_id="2" * 32)
    _payload, third_request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=3,
        principal=delegated,
        actual_transport="claude_exec",
    )
    assert await relay._authorize_worker_launch_admission(
        third_request, worker
    ) == (True, "admitted")
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        marker = current.metadata_[WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY]
        assert marker["turn_generation"] == 3
        assert marker["actual_transport"] == "claude_exec"

    # A delayed G+2 replay cannot replace the newer durable G+3 marker.
    assert await relay._authorize_worker_launch_admission(
        second_request, worker
    ) == (False, "generation_changed")


@pytest.mark.asyncio
async def test_manager_handoff_permit_uses_target_receipt_principal(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, _source_user = await _manager_rows(
        db_factory,
        task_status="completed",
        retry_count=1,
        turn_generation=4,
        principal_user_id=21,
        principal_role="member",
    )
    target = _delegated_user(22, "admin")
    async with db_factory() as db:
        db.add(User(
            id=22,
            email="handoff-admin@example.com",
            name="Handoff Admin",
            password_hash="hash",
            role="admin",
            is_active=True,
        ))
        current = await db.get(Task, task.id)
        log = LogEntry(
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="follow up",
        )
        db.add(log)
        await db.flush()
        handoff_id = "e" * 32
        current.worker_turn_handoff_id = handoff_id
        current.worker_turn_handoff_worker_id = worker.id
        current.worker_turn_handoff_retry_count = 1
        current.worker_turn_handoff_from_generation = 4
        current.worker_turn_handoff_source_log_id = log.id
        current.worker_turn_handoff_acknowledged = False
        db.add(WorkerTurnHandoffReceipt(
            handoff_id=handoff_id,
            task_id=task.id,
            source_log_id=log.id,
            side="manager",
            worker_id=worker.id,
            retry_count=1,
            from_generation=4,
            status="prepared",
            request_payload=target,
            request_digest=canonical_payload_digest(target),
        ))
        await db.commit()

    _payload, request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=1,
        turn_generation=5,
        principal=target,
    )
    relay = WorkerRelay(db_factory, MagicMock())
    admitted, reason = await relay._authorize_worker_launch_admission(
        request,
        worker,
    )
    assert (admitted, reason) == (True, "admitted")

    mismatched = _delegated_user(21, "member")
    _payload, stale_request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=1,
        turn_generation=5,
        principal=mismatched,
    )
    denied, reason = await relay._authorize_worker_launch_admission(
        stale_request,
        worker,
    )
    assert (denied, reason) == (False, "generation_changed")


@pytest.mark.asyncio
async def test_manager_manual_retry_permit_accepts_pre_adoption_dequeue(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, _source_user = await _manager_rows(
        db_factory,
        task_status="failed",
        retry_count=2,
        turn_generation=6,
        principal_user_id=31,
        principal_role="member",
    )
    target = _delegated_user(32, "admin")
    target_native = task_execution_principal_values(
        user_id=32,
        role="admin",
        principal_kind="user",
    )
    async with db_factory() as db:
        db.add(User(
            id=32,
            email="retry-admin@example.com",
            name="Retry Admin",
            password_hash="hash",
            role="admin",
            is_active=True,
        ))
        current = await db.get(Task, task.id)
        observed = worker_task_generation(current, expected_worker_id=worker.id)
        source_generation = worker_manual_retry_source_generation(
            current,
            observed,
        )
        assert source_generation is not None
        operation_id = "f" * 32
        retry_request = {
            "protocol_version": WORKER_MANUAL_RETRY_PROTOCOL,
            "operation_id": operation_id,
            "task_id": task.id,
            "worker_id": worker.id,
            "source_incarnation_id": task.incarnation_id,
            "expected_status": "failed",
            "expected_retry_count": 2,
            "expected_turn_generation": 6,
            "source_principal_digest": source_generation["principal_digest"],
            "target_principal_digest": worker_principal_digest(target),
            **target,
        }
        retry_request["request_digest"] = worker_manual_retry_request_digest(
            retry_request
        )
        marker = {
            "version": WORKER_MANUAL_RETRY_PROTOCOL,
            "side": "manager",
            "state": "prepared",
            "operation_id": operation_id,
            "request_digest": retry_request["request_digest"],
            "worker_id": worker.id,
            "source_generation": source_generation,
            "target_principal": target,
            "target_manager_principal": target_native,
            "target_principal_digest": worker_principal_digest(target),
            "request": retry_request,
        }
        current.metadata_ = {
            **(current.metadata_ or {}),
            WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY: marker,
            WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY: {
                "version": 1,
                "worker_id": worker.id,
                "incarnation_id": task.incarnation_id,
                "retry_count": 2,
                "turn_generation": 6,
                "principal_digest": canonical_payload_digest(
                    _delegated_user(31, "member")
                ),
                "actual_transport": "claude_exec",
            },
        }
        await db.commit()

    _payload, request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=3,
        turn_generation=7,
        principal=target,
    )
    relay = WorkerRelay(db_factory, MagicMock())
    admitted, reason = await relay._authorize_worker_launch_admission(
        request,
        worker,
    )
    assert (admitted, reason) == (True, "admitted")
    # Response loss may repeat the exact N+1/G+1 launch identity.
    assert await relay._authorize_worker_launch_admission(
        request,
        worker,
    ) == (True, "admitted")
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        marker = current.metadata_[WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY]
        assert marker["retry_count"] == 3
        assert marker["turn_generation"] == 7
        assert marker["principal_digest"] == canonical_payload_digest(target)

    async with db_factory() as db:
        target_user = await db.get(User, 32)
        target_user.is_active = False
        await db.commit()
    denied, reason = await relay._authorize_worker_launch_admission(
        request,
        worker,
    )
    assert (denied, reason) == (False, "principal_revoked")


@pytest.mark.asyncio
async def test_manager_manual_retry_permit_accepts_acknowledged_pending_mirror(
    db_factory,
    monkeypatch,
):
    """The Worker may dequeue G+1 after Manager adopted only pending N+1/G."""

    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    worker, task, _source_user = await _manager_rows(
        db_factory,
        task_status="failed",
        retry_count=4,
        turn_generation=8,
        principal_user_id=41,
        principal_role="member",
    )
    target = _delegated_user(42, "admin")
    target_native = task_execution_principal_values(
        user_id=42,
        role="admin",
        principal_kind="user",
    )
    async with db_factory() as db:
        db.add(User(
            id=42,
            email="acknowledged-retry-admin@example.com",
            name="Acknowledged Retry Admin",
            password_hash="hash",
            role="admin",
            is_active=True,
        ))
        current = await db.get(Task, task.id)
        observed = worker_task_generation(current, expected_worker_id=worker.id)
        source_generation = worker_manual_retry_source_generation(
            current,
            observed,
        )
        assert source_generation is not None
        operation_id = "7" * 32
        retry_request = {
            "protocol_version": WORKER_MANUAL_RETRY_PROTOCOL,
            "operation_id": operation_id,
            "task_id": task.id,
            "worker_id": worker.id,
            "source_incarnation_id": task.incarnation_id,
            "expected_status": "failed",
            "expected_retry_count": 4,
            "expected_turn_generation": 8,
            "source_principal_digest": source_generation["principal_digest"],
            "target_principal_digest": worker_principal_digest(target),
            **target,
        }
        retry_request["request_digest"] = worker_manual_retry_request_digest(
            retry_request
        )
        result_generation = {
            "status": "pending",
            "retry_count": 5,
            "turn_generation": 8,
        }
        worker_receipt = {
            "version": WORKER_MANUAL_RETRY_PROTOCOL,
            "side": "worker",
            "state": "committed",
            "operation_id": operation_id,
            "request_digest": retry_request["request_digest"],
            "source_generation": source_generation,
            "result_generation": result_generation,
            "source_principal_digest": source_generation["principal_digest"],
            "target_principal_digest": worker_principal_digest(target),
            "target_principal": target,
        }
        marker = {
            "version": WORKER_MANUAL_RETRY_PROTOCOL,
            "side": "manager",
            "state": "acknowledged",
            "operation_id": operation_id,
            "request_digest": retry_request["request_digest"],
            "worker_id": worker.id,
            "source_generation": source_generation,
            "target_principal": target,
            "target_manager_principal": target_native,
            "target_principal_digest": worker_principal_digest(target),
            "request": retry_request,
            "worker_receipt": worker_receipt,
        }
        current.status = "pending"
        current.retry_count = 5
        current.turn_generation = 8
        for field, value in target_native.items():
            setattr(current, field, value)
        current.metadata_ = {
            **(current.metadata_ or {}),
            WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY: marker,
        }
        await db.commit()

    _payload, request = _request(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=5,
        turn_generation=9,
        principal=target,
    )
    relay = WorkerRelay(db_factory, MagicMock())

    admitted, reason = await relay._authorize_worker_launch_admission(
        request,
        worker,
    )

    assert (admitted, reason) == (True, "admitted")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("auth_type", "accepted"),
    [("jwt", False), ("token", False), ("worker_control_plane", True)],
)
async def test_worker_launch_response_ws_requires_deployment_token_identity(
    db_factory,
    monkeypatch,
    auth_type,
    accepted,
):
    from backend import database, main
    from backend.api import ws as ws_api
    from backend.services import worker_launch_admission

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-control-token")
    monkeypatch.setattr(database, "async_session", db_factory)
    monkeypatch.setattr(
        ws_api,
        "_current_ws_identity",
        AsyncMock(return_value={
                "auth_type": auth_type,
                "role": "super_admin",
                "user_id": 1 if auth_type == "jwt" else None,
        }),
    )
    accept_response = MagicMock(return_value=True)
    monkeypatch.setattr(
        worker_launch_admission,
        "accept_worker_launch_admission_response",
        accept_response,
    )
    broadcaster = MagicMock(
        subscribe=AsyncMock(),
        unsubscribe=AsyncMock(),
    )
    monkeypatch.setattr(main, "broadcaster", broadcaster)

    class Socket:
        headers = {}
        query_params = {}

        def __init__(self):
            self.received = False
            self.accepted = False

        async def accept(self):
            self.accepted = True

        async def receive_text(self):
            if not self.received:
                self.received = True
                return json.dumps({
                    "action": "worker_launch_admission_response",
                    "request_id": "8" * 32,
                })
            raise WebSocketDisconnect()

        async def send_text(self, _text):
            return None

        async def close(self, **_kwargs):
            return None

    socket = Socket()
    await ws_api.websocket_endpoint(socket)

    assert socket.accepted is True
    assert accept_response.called is accepted


def test_launch_request_rejects_non_delegated_or_mutated_identity():
    native = task_execution_principal_values(
        user_id=51,
        role="admin",
        principal_kind="user",
    )
    payload, _request_value = _request(
        task_id=88,
        incarnation_id="9" * 32,
        retry_count=0,
        turn_generation=1,
        principal=_delegated_user(51, "admin"),
    )
    payload["execution_principal"] = native
    payload["principal_digest"] = canonical_payload_digest(native)
    payload["request_digest"] = canonical_payload_digest(
        {key: value for key, value in payload.items() if key != "request_digest"}
    )
    assert parse_worker_launch_admission_request(payload) is None

    payload["execution_principal"] = _delegated_user(51, "admin")
    # Leave the digest bound to the native form.
    assert parse_worker_launch_admission_request(payload) is None
