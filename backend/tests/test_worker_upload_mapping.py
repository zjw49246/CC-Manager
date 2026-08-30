"""Cross-host upload path invariants for Manager-to-Worker effects."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

import backend.services.worker_plan_dispatch as worker_plan_dispatch_module
import backend.services.worker_proxy as worker_proxy_module
from backend.config import settings
from backend.models.plan import Plan, PlanInputRequest
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.models.worker import Worker
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.task_creation import SOURCE_TASK_INCARNATION_METADATA_KEY
from backend.services.worker_proxy import WorkerProxy, worker_managed_upload_paths


pytestmark = pytest.mark.usefixtures("worker_control_plane_auth")


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _AnswerResult:
    def __init__(self, answer):
        self.answer = answer

    def scalar_one_or_none(self):
        return self.answer


class _AnswerDB:
    def __init__(self, answer):
        self.answer = answer

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def execute(self, _statement):
        return _AnswerResult(self.answer)


class _AnswerDBFactory:
    def __init__(self, answer):
        self.answer = answer

    def __call__(self):
        return _AnswerDB(self.answer)


class _AnswerCaptured(RuntimeError):
    pass


def _attachment(path, *, image: bool) -> dict:
    return {
        "path": str(path),
        "url": f"/api/uploads/{path.name}",
        "name": "screen.png" if image else "notes.txt",
        "is_image": image,
    }


def _worker(worker_id: int = 7) -> Worker:
    return Worker(
        id=worker_id,
        name="mapped-worker",
        status="ready",
        private_ip="10.0.0.7",
        auth_token="worker-control-plane-test-token",
    )


def _plan_and_run(worker: Worker, attachment: dict) -> tuple[Plan, PlanAgentRun]:
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    plan = Plan(
        id=41,
        title="Remote Plan",
        initial_request="Inspect the attachment",
        worker_id=worker.id,
        pipeline_config=pipeline,
        initial_attachments=[attachment],
    )
    run = PlanAgentRun(
        id=42,
        plan_id=plan.id,
        worker_id=worker.id,
        run_type="initial",
        request_text=plan.initial_request,
        pipeline_config=pipeline,
        generation=3,
        max_interactions=3,
    )
    return plan, run


@pytest.mark.asyncio
async def test_push_files_confines_every_destination_to_worker_upload_root(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(settings, "worker_remote_dir", "/srv/worker-app")
    name = "11111111-1111-4111-8111-111111111111.txt"
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    local_file = upload_dir / name
    local_file.write_text("managed", encoding="utf-8")
    local_path = str(local_file)
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", upload_dir)
    remote_path = f"/srv/worker-app/uploads/{name}"
    worker = _worker()
    proxy = WorkerProxy(None, None)
    ssh = SimpleNamespace(copy_file=AsyncMock())
    monkeypatch.setattr(proxy, "_ssh", lambda _worker: ssh)

    assert worker_managed_upload_paths([local_path]) == [remote_path]
    await proxy.push_files(worker, [local_path])
    ssh.copy_file.assert_awaited_once_with(local_path, remote_path)

    with pytest.raises(ValueError, match="Worker upload root"):
        await proxy.push_files(
            worker,
            [local_path],
            remote_paths=[f"/manager/checkout/uploads/{name}"],
        )
    assert ssh.copy_file.await_count == 1

    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    outside_file = outside_dir / name
    outside_file.write_text("not an upload", encoding="utf-8")
    with pytest.raises(RuntimeError, match="no longer a managed upload"):
        await proxy.push_files(worker, [str(outside_file)])

    link_name = "66666666-6666-4666-8666-666666666666.txt"
    link_path = upload_dir / link_name
    link_path.symlink_to(local_file)
    with pytest.raises(RuntimeError, match="no longer a managed upload"):
        await proxy.push_files(worker, [str(link_path)])
    assert ssh.copy_file.await_count == 1


@pytest.mark.asyncio
async def test_versioned_plan_reproves_source_before_building_manifest(
    tmp_path,
    monkeypatch,
):
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    outside_file = (
        outside_dir / "77777777-7777-4777-8777-777777777777.txt"
    )
    outside_file.write_text("must not be hashed", encoding="utf-8")
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", upload_dir)
    monkeypatch.setattr(settings, "worker_remote_dir", "/srv/worker-app")

    worker = _worker()
    plan, run = _plan_and_run(
        worker,
        _attachment(outside_file, image=False),
    )
    proxy = WorkerProxy(None, None)
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    proxy._require_worker_plan_reconciliation_protocol = AsyncMock()

    def unexpected_manifest(*_args, **_kwargs):
        raise AssertionError("manifest opened an unproved Manager path")

    monkeypatch.setattr(proxy, "_attachment_manifest", unexpected_manifest)

    with pytest.raises(RuntimeError, match="no longer a managed upload"):
        await proxy.run_versioned_plan_until_pause(plan, run)


@pytest.mark.asyncio
async def test_versioned_plan_and_input_use_worker_paths_in_wire_and_manifest(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(settings, "worker_remote_dir", "/srv/worker-app")
    plan_file = tmp_path / "11111111-1111-4111-8111-111111111111.png"
    plan_file.write_bytes(b"plan-image")
    answer_file = tmp_path / "22222222-2222-4222-8222-222222222222.txt"
    answer_file.write_bytes(b"answer-notes")
    remote_plan = f"/srv/worker-app/uploads/{plan_file.name}"
    remote_answer = f"/srv/worker-app/uploads/{answer_file.name}"
    worker = _worker()
    plan, run = _plan_and_run(worker, _attachment(plan_file, image=True))
    answer = PlanInputRequest(
        id=55,
        plan_id=plan.id,
        run_id=run.id,
        worker_id=worker.id,
        worker_input_request_id=91,
        status="answered",
        answer_idempotency_key="answer-1",
        answers=[{"question_id": "scope", "value": "all"}],
        response_text="Attached details",
        attachments=[_attachment(answer_file, image=False)],
    )
    proxy = WorkerProxy(_AnswerDBFactory(answer), None)
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    proxy._require_worker_plan_reconciliation_protocol = AsyncMock()
    proxy.push_files = AsyncMock()
    imported_body = {}
    answer_body = {}

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, url, *, headers, json):
            assert headers["Authorization"].startswith("Bearer ")
            if url.endswith("/api/plans/worker-import"):
                imported_body.update(json)
                digest = proxy._versioned_plan_import_digest(
                    json,
                    json["attachment_manifest"],
                )
                return _Response({
                    "run": {
                        "id": run.id,
                        "plan_id": plan.id,
                        "status": "waiting_user",
                        "open_input_request_id": answer.worker_input_request_id,
                        "generation": 2,
                    },
                    "base_worker_version_id": None,
                    "import_payload_digest": digest,
                    "attachment_receipt": json["attachment_manifest"],
                })
            answer_body.update(json)
            raise _AnswerCaptured

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    monkeypatch.setattr(
        worker_plan_dispatch_module,
        "validate_worker_plan_outcome_graph",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(_AnswerCaptured):
        await proxy.run_versioned_plan_until_pause(plan, run)

    assert imported_body["file_paths"] == [remote_plan]
    assert imported_body["image_paths"] == [remote_plan]
    assert imported_body["attachment_manifest"] == [{
        "path": remote_plan,
        "size": len(b"plan-image"),
        "sha256": hashlib.sha256(b"plan-image").hexdigest(),
    }]
    assert answer_body["file_paths"] == [remote_answer]
    assert answer_body["image_paths"] is None
    assert answer_body["attachment_manifest"] == [{
        "path": remote_answer,
        "size": len(b"answer-notes"),
        "sha256": hashlib.sha256(b"answer-notes").hexdigest(),
    }]
    assert proxy.push_files.await_args_list == [
        call(worker, [str(plan_file)], remote_paths=[remote_plan]),
        call(worker, [str(answer_file)], remote_paths=[remote_answer]),
    ]


@pytest.mark.asyncio
async def test_plan_input_recovery_replays_only_worker_upload_paths(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(settings, "worker_remote_dir", "/opt/ccm-worker")
    answer_file = tmp_path / "33333333-3333-4333-8333-333333333333.png"
    answer_file.write_bytes(b"recovery-image")
    remote_answer = f"/opt/ccm-worker/uploads/{answer_file.name}"
    worker = _worker()
    plan, run = _plan_and_run(
        worker,
        _attachment(answer_file, image=True),
    )
    # The Plan's original import is already durable; only this answer is
    # replayed after exact readback proves the Worker is still waiting.
    plan.initial_attachments = []
    answer = PlanInputRequest(
        id=56,
        plan_id=plan.id,
        run_id=run.id,
        worker_id=worker.id,
        worker_input_request_id=92,
        status="answered",
        answer_idempotency_key="answer-recovery",
        answers=[],
        attachments=[_attachment(answer_file, image=True)],
    )
    proxy = WorkerProxy(_AnswerDBFactory(answer), None)
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    proxy._require_worker_plan_reconciliation_protocol = AsyncMock()
    proxy._read_worker_plan_import_audit = AsyncMock(return_value={
        "state": "matched",
        "base_worker_version_id": None,
        "run": {
            "id": run.id,
            "plan_id": plan.id,
            "status": "waiting_user",
            "open_input_request_id": answer.worker_input_request_id,
            "generation": 4,
        },
        "versions": [],
    })
    proxy.push_files = AsyncMock()
    answer_body = {}

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, _url, *, headers, json):
            assert headers["Authorization"].startswith("Bearer ")
            answer_body.update(json)
            raise _AnswerCaptured

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)

    with pytest.raises(_AnswerCaptured):
        await proxy.reconcile_versioned_plan_until_pause(
            plan,
            run,
            payload_digest="a" * 64,
        )

    assert answer_body["file_paths"] == [remote_answer]
    assert answer_body["image_paths"] == [remote_answer]
    assert answer_body["attachment_manifest"][0]["path"] == remote_answer
    proxy.push_files.assert_awaited_once_with(
        worker,
        [str(answer_file)],
        remote_paths=[remote_answer],
    )


@pytest.mark.asyncio
async def test_related_plan_task_materialization_uses_worker_upload_paths(
    monkeypatch,
):
    monkeypatch.setattr(settings, "worker_remote_dir", "/srv/worker-app")
    image_name = "44444444-4444-4444-8444-444444444444.png"
    notes_name = "55555555-5555-4555-8555-555555555555.txt"
    local_paths = [
        f"/manager/checkout/uploads/{image_name}",
        f"/manager/checkout/uploads/{notes_name}",
    ]
    remote_paths = [
        f"/srv/worker-app/uploads/{image_name}",
        f"/srv/worker-app/uploads/{notes_name}",
    ]
    worker = _worker(worker_id=78)
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
    captured_payload = {}

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, _url, *, headers, json):
            assert headers["Authorization"].startswith("Bearer ")
            captured_payload.update(json)
            created = dict(json)
            created.update({
                "incarnation_id": json["source_incarnation_id"],
                "metadata_": {
                    SOURCE_TASK_INCARNATION_METADATA_KEY:
                    json["source_incarnation_id"],
                },
                "status": "pending",
                "retry_count": json["source_retry_count"],
                "turn_generation": json["source_turn_generation"] - 1,
            })
            return _Response(created)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    relay = SimpleNamespace(subscribe_task=AsyncMock())
    proxy = WorkerProxy(None, relay)
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=34)
    proxy.require_worker_delegated_principal_support = AsyncMock()
    proxy.require_worker_initial_generation_support = AsyncMock()
    proxy.require_worker_task_incarnation_support = AsyncMock()
    proxy.require_worker_fast_support = AsyncMock()
    proxy.push_files = AsyncMock()
    proxy._user_skill_snapshots = AsyncMock(return_value=[])

    await proxy._forward_task_to_worker_locked(task)

    assert captured_payload["file_paths"] == remote_paths
    assert captured_payload["image_paths"] == [remote_paths[0]]
    assert captured_payload["attachments"] == task.metadata_["attachments"]
    proxy.push_files.assert_awaited_once_with(
        worker,
        local_paths,
        remote_paths=remote_paths,
    )
