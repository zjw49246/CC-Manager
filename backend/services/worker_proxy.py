"""Manager→Worker 任务转发与操作代理（elastic-worker 设计 §5.3/§6.3/§6.4/§8）。

- forward_task_to_worker：确保 worker 有项目 → 先订阅 relay → 用 Manager 分配的
  同一 task ID 在 worker 上创建 task（ID 全局统一，见设计 §2）
- proxy_to_worker：通用操作代理（stop/cancel/retry/plan/monitor），转发前确保
  relay 已订阅（幂等；retry 场景 Manager 重启后 relay 未订阅，不补订阅则全丢）
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
import stat
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from weakref import WeakKeyDictionary

import httpx
from fastapi import HTTPException
from sqlalchemy import select, update
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from backend.config import settings
from backend.models.project import Project
from backend.models.plan import Plan, PlanInputRequest, PlanVersion
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services.legacy_plan_execution import (
    LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION,
    LegacyPlanExecutionCarrierProof,
    parse_legacy_plan_execution_carrier_proof,
)
from backend.services.pr_review_runtime import (
    PR_REVIEW_SNAPSHOT_CONTEXT_VERSION,
    PR_REVIEW_TERMINAL_CHAT_HEADER,
    PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE,
    PR_REVIEW_TERMINAL_CHAT_VERSION,
    is_pr_review_task,
    is_pr_sandbox_task,
)
from backend.services.ssh_executor import SSHExecutor, worker_known_hosts_path
from backend.services.task_artifact_contract import (
    TASK_ARTIFACT_SCOPE_VERSION,
)
from backend.services.task_creation import (
    SOURCE_TASK_INCARNATION_METADATA_KEY,
    delegated_task_execution_principal_values,
)
from backend.services.task_id_namespace import (
    TASK_ID_NAMESPACE_PROTOCOL,
    TaskIdNamespaceProtocolError,
    validate_manager_allocated_task_id,
    validate_worker_task_id_namespace_config,
)
from backend.services.upload_references import is_managed_upload_basename
from backend.services.cloud_provider import canonical_cloud_termination_scope
from backend.services.worker_relay import (
    WORKER_MANUAL_RETRY_PROTOCOL,
    mark_worker_task_materialized,
    worker_manual_retry_is_prepared,
    worker_manual_retry_receipt,
    worker_task_generation,
)
from backend.services.worker_launch_admission import (
    WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL,
)
from backend.services.worker_routing_config import (
    WORKER_MIGRATION_IMPORT_PROTOCOL,
)
from backend.services.worker_drain_proof import (
    WORKER_NODE_DRAIN_PROOF_PROTOCOL,
    verify_worker_node_drain_proof_signature,
)
from backend.services.worker_task_termination import (
    WORKER_DESTROY_DRAIN_CLAIM_HEADER,
    WORKER_DESTROY_TASK_INCARCATION_HEADER,
    WORKER_DESTROY_TASK_RETRY_HEADER,
    WORKER_DESTROY_TASK_TURN_HEADER,
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)
from backend.services.worker_plan_decision import (
    worker_plan_decision_gate_receipt,
    worker_plan_decision_is_prepared,
    worker_plan_decision_request_matches,
)

logger = logging.getLogger(__name__)


_WORKER_DESTROY_CLAIM_SEAL = object()
WORKER_DESTROY_TERMINATION_RECEIPT_VERSION = 2
WORKER_DESTROY_TERMINATION_ACTION = "terminate_instance"


def _exact_task_incarnation(task: Task) -> str:
    """Return one canonical Task incarnation or reject legacy ambiguity."""

    incarnation_id = task.incarnation_id
    if (
        not isinstance(incarnation_id, str)
        or len(incarnation_id) != 32
        or any(char not in "0123456789abcdef" for char in incarnation_id)
    ):
        raise HTTPException(
            409,
            "Task has no stable incarnation identity",
        )
    return incarnation_id


def worker_managed_upload_paths(paths: list[str]) -> list[str]:
    """Map validated Manager uploads into the Worker's own upload root."""

    remote_root = os.path.normpath(
        os.path.join(settings.worker_remote_dir, "uploads")
    )
    if not os.path.isabs(remote_root):
        raise RuntimeError("Worker remote upload root must be absolute")
    mapped: list[str] = []
    for path in paths:
        basename = os.path.basename(path)
        if not is_managed_upload_basename(basename):
            raise RuntimeError("Worker attachment is not a CCM-managed upload")
        remote_path = os.path.normpath(os.path.join(remote_root, basename))
        if os.path.dirname(remote_path) != remote_root:
            raise RuntimeError("Worker attachment escaped its upload root")
        mapped.append(remote_path)
    return mapped


WORKER_PLAN_RECONCILIATION_PROTOCOL = 1
WORKER_PLAN_EXACT_CANCEL_PROTOCOL = 1
WORKER_DELEGATED_PRINCIPAL_PROTOCOL = 1
WORKER_INITIAL_GENERATION_PROTOCOL = 1


class WorkerPlanRemoteAbsent(RuntimeError):
    """Exact read-only audit proved that the Worker never imported the Run."""


class WorkerPlanRemoteIdentityConflict(RuntimeError):
    """The Worker Run id exists but belongs to another immutable payload."""


class WorkerPlanRemoteCancelled(RuntimeError):
    """The exact Worker import was tombstoned before it could be admitted."""


class WorkerPlanReconciliationUnsupported(RuntimeError):
    """The Worker cannot provide the required read-only identity proof."""


@dataclass(frozen=True)
class WorkerDestroyLifecycleClaim:
    """Opaque authority for one already-claimed Worker destroy lifecycle.

    The public Worker proxy remains ready-only.  This token is created only
    from the row returned by the ``ready|stopped|error -> destroying`` CAS and
    lets the destroy coordinator perform the narrow stop/readback handshake
    while that exact Worker endpoint still owns the Task.
    """

    _seal: object = field(repr=False, compare=False)
    worker_id: int
    created_at: datetime | None
    destroy_lifecycle_nonce: str
    cloud_instance_id: str | None
    private_ip: str | None
    ccm_port: int
    node_drain_claim: str
    auth_token: str | None = field(repr=False)


def _worker_node_drain_claim(worker: Worker) -> str:
    """Derive one stable claim for retries of this exact cloud Worker row."""

    identity = json.dumps(
        {
            "protocol": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
            "worker_id": worker.id,
            "created_at": (
                worker.created_at.isoformat() if worker.created_at else None
            ),
            "cloud_instance_id": worker.cloud_instance_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(b"ccm-worker-node-drain\0" + identity).hexdigest()


def _canonical_destroy_receipt_payload(payload: dict) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _valid_lower_hex(value: object, length: int) -> bool:
    return bool(
        type(value) is str
        and len(value) == length
        and all(char in "0123456789abcdef" for char in value)
    )


def _valid_nonempty_text(value: object) -> bool:
    return bool(
        type(value) is str
        and value
        and value == value.strip()
    )


def worker_destroy_provision_spec_digest(provision_spec: object) -> str:
    """Digest one exact JSON-safe RunInstances request journal."""

    if type(provision_spec) is not dict:
        raise ValueError("Worker destroy provision spec must be a JSON object")
    try:
        encoded = _canonical_destroy_receipt_payload(provision_spec)
        detached = json.loads(encoded)
    except (TypeError, ValueError, UnicodeError, OverflowError) as exc:
        raise ValueError(
            "Worker destroy provision spec is not canonical JSON"
        ) from exc
    if detached != provision_spec:
        raise ValueError(
            "Worker destroy provision spec changes during JSON serialization"
        )
    return hashlib.sha256(encoded).hexdigest()


def worker_destroy_client_token_digest(client_token: object) -> str:
    """Hash the EC2 idempotency token without persisting that token itself."""

    if not _valid_nonempty_text(client_token):
        raise ValueError("Worker destroy ClientToken is missing or invalid")
    return hashlib.sha256(client_token.encode("utf-8")).hexdigest()


def _validated_signed_destroy_proof_copy(
    proof: object,
    *,
    drain_claim: str,
    auth_token: str,
    require_clean: bool,
) -> tuple[dict, str]:
    """Return a detached payload/signature after exact HMAC validation."""

    payload_keys = {
        "protocol_version",
        "nonce",
        "node_role",
        "drain_claim",
        "runtime_sealed",
        "safe_to_destroy",
        "blockers",
        "blocker_count",
        "task_count",
    }
    expected_keys = payload_keys | {"signature"}
    if (
        type(proof) is not dict
        or set(proof) != expected_keys
        or not _valid_nonempty_text(auth_token)
        or not _valid_lower_hex(drain_claim, 64)
    ):
        raise ValueError(
            "Worker destroy termination proof envelope is invalid"
        )
    payload = dict(proof)
    signature = payload.pop("signature")
    blockers = payload.get("blockers")
    if (
        type(payload.get("protocol_version")) is not int
        or payload["protocol_version"] != WORKER_NODE_DRAIN_PROOF_PROTOCOL
        or not _valid_lower_hex(payload.get("nonce"), 32)
        or payload.get("node_role") != "worker"
        or payload.get("drain_claim") != drain_claim
        or payload.get("runtime_sealed") is not True
        or type(payload.get("safe_to_destroy")) is not bool
        or type(blockers) is not list
        or type(payload.get("blocker_count")) is not int
        or payload["blocker_count"] < 0
        # The Worker deliberately caps the serialized blocker details while
        # preserving the full count, so only the impossible inverse is bad.
        or payload["blocker_count"] < len(blockers)
        or type(payload.get("task_count")) is not int
        or payload["task_count"] < 0
        or not _valid_lower_hex(signature, 64)
        or not verify_worker_node_drain_proof_signature(
            payload,
            auth_token=auth_token,
            signature=signature,
        )
        or (
            require_clean
            and (
                payload["safe_to_destroy"] is not True
                or blockers != []
                or payload["blocker_count"] != 0
            )
        )
    ):
        raise ValueError(
            "Worker destroy termination proof is not a valid signed snapshot"
        )
    try:
        detached = json.loads(_canonical_destroy_receipt_payload(payload))
    except (TypeError, ValueError, UnicodeError, OverflowError) as exc:
        raise ValueError(
            "Worker destroy termination proof is not canonical JSON"
        ) from exc
    if detached != payload:
        raise ValueError(
            "Worker destroy termination proof changes during JSON serialization"
        )
    return detached, signature


def build_worker_destroy_termination_receipt(
    claim: WorkerDestroyLifecycleClaim,
    signed_proof: object,
    *,
    cloud_scope: object,
    provision_spec_digest: str,
    client_token_digest: str,
    authorized_at: datetime | None = None,
) -> dict:
    """Bind final signed drain evidence to one exact cloud destroy lifecycle."""

    if (
        not isinstance(claim, WorkerDestroyLifecycleClaim)
        or claim._seal is not _WORKER_DESTROY_CLAIM_SEAL
        or type(claim.worker_id) is not int
        or claim.worker_id <= 0
        or not _valid_lower_hex(claim.destroy_lifecycle_nonce, 32)
        or not _valid_lower_hex(claim.node_drain_claim, 64)
        or not _valid_nonempty_text(claim.cloud_instance_id)
        or not _valid_nonempty_text(claim.private_ip)
        or not _valid_nonempty_text(claim.auth_token)
        or type(claim.ccm_port) is not int
        or claim.ccm_port < 1
        or claim.ccm_port > 65535
    ):
        raise ValueError("Worker destroy lifecycle claim is invalid")
    try:
        clean_scope = canonical_cloud_termination_scope(cloud_scope)
    except (TypeError, ValueError) as exc:
        raise ValueError("Worker destroy cloud scope is invalid") from exc
    if (
        not _valid_lower_hex(provision_spec_digest, 64)
        or not _valid_lower_hex(client_token_digest, 64)
    ):
        raise ValueError("Worker destroy cloud identity digests are invalid")
    clean_proof, proof_signature = _validated_signed_destroy_proof_copy(
        signed_proof,
        drain_claim=claim.node_drain_claim,
        auth_token=claim.auth_token,
        require_clean=True,
    )
    stamp = authorized_at or datetime.utcnow()
    if (
        not isinstance(stamp, datetime)
        or stamp.tzinfo is not None
        or stamp.year < 2000
    ):
        raise ValueError("Worker destroy authorization timestamp is invalid")
    payload = {
        "version": WORKER_DESTROY_TERMINATION_RECEIPT_VERSION,
        "action": WORKER_DESTROY_TERMINATION_ACTION,
        "worker_id": claim.worker_id,
        "destroy_lifecycle_nonce": claim.destroy_lifecycle_nonce,
        "cloud_instance_id": claim.cloud_instance_id,
        "private_ip": claim.private_ip,
        "node_drain_claim": claim.node_drain_claim,
        "cloud_scope": clean_scope,
        "provision_spec_digest": provision_spec_digest,
        "client_token_digest": client_token_digest,
        "authorized_at": stamp.isoformat(timespec="microseconds"),
        "proof": clean_proof,
        "proof_signature": proof_signature,
    }
    payload["receipt_digest"] = hashlib.sha256(
        _canonical_destroy_receipt_payload(payload)
    ).hexdigest()
    return payload


def worker_destroy_termination_receipt_matches(
    worker: Worker,
    *,
    cloud_scope: object,
    client_token_digest: str,
) -> bool:
    """Validate restart authority without trusting a dead remote Worker."""

    receipt = worker.destroy_termination_receipt
    expected_keys = {
        "version",
        "action",
        "worker_id",
        "destroy_lifecycle_nonce",
        "cloud_instance_id",
        "private_ip",
        "node_drain_claim",
        "cloud_scope",
        "provision_spec_digest",
        "client_token_digest",
        "authorized_at",
        "proof",
        "proof_signature",
        "receipt_digest",
    }
    valid_status = worker.status == "destroying" or (
        worker.status in {"ready", "error"}
        and worker.bootstrap_step == "destroy"
    )
    try:
        expected_scope = canonical_cloud_termination_scope(cloud_scope)
        expected_provision_spec_digest = (
            worker_destroy_provision_spec_digest(worker.provision_spec)
        )
        provision_scope = canonical_cloud_termination_scope(
            worker.provision_spec.get("cloud_scope")
        )
        provision_client_token_digest = worker.provision_spec.get(
            "client_token_digest"
        )
    except (TypeError, ValueError, UnicodeError, OverflowError):
        return False
    if (
        not valid_status
        or type(worker.id) is not int
        or worker.id <= 0
        or not _valid_lower_hex(worker.destroy_lifecycle_nonce, 32)
        or not _valid_nonempty_text(worker.cloud_instance_id)
        or not _valid_nonempty_text(worker.private_ip)
        or not _valid_nonempty_text(worker.auth_token)
        or not _valid_lower_hex(client_token_digest, 64)
        or provision_scope != expected_scope
        or not _valid_lower_hex(provision_client_token_digest, 64)
        or not secrets.compare_digest(
            provision_client_token_digest,
            client_token_digest,
        )
        or type(receipt) is not dict
        or set(receipt) != expected_keys
        or type(receipt.get("version")) is not int
        or receipt["version"] != WORKER_DESTROY_TERMINATION_RECEIPT_VERSION
        or receipt.get("action") != WORKER_DESTROY_TERMINATION_ACTION
        or type(receipt.get("worker_id")) is not int
        or receipt["worker_id"] != worker.id
        or receipt.get("destroy_lifecycle_nonce")
        != worker.destroy_lifecycle_nonce
        or receipt.get("cloud_instance_id") != worker.cloud_instance_id
        or receipt.get("private_ip") != worker.private_ip
        or receipt.get("node_drain_claim") != _worker_node_drain_claim(worker)
        or receipt.get("cloud_scope") != expected_scope
        or receipt.get("provision_spec_digest")
        != expected_provision_spec_digest
        or receipt.get("client_token_digest") != client_token_digest
        or not _valid_lower_hex(receipt.get("provision_spec_digest"), 64)
        or not _valid_lower_hex(receipt.get("client_token_digest"), 64)
        or not _valid_lower_hex(receipt.get("proof_signature"), 64)
        or not _valid_lower_hex(receipt.get("receipt_digest"), 64)
    ):
        return False
    try:
        if canonical_cloud_termination_scope(receipt["cloud_scope"]) != receipt[
            "cloud_scope"
        ]:
            return False
        if type(receipt["authorized_at"]) is not str:
            return False
        authorized_at = datetime.fromisoformat(receipt["authorized_at"])
        if (
            authorized_at.tzinfo is not None
            or authorized_at.year < 2000
            or authorized_at.isoformat(timespec="microseconds")
            != receipt["authorized_at"]
        ):
            return False
        signed_proof = dict(receipt["proof"])
        signed_proof["signature"] = receipt["proof_signature"]
        proof, proof_signature = _validated_signed_destroy_proof_copy(
            signed_proof,
            drain_claim=receipt["node_drain_claim"],
            auth_token=worker.auth_token,
            require_clean=True,
        )
        if (
            proof != receipt["proof"]
            or proof_signature != receipt["proof_signature"]
        ):
            return False
        unsigned = dict(receipt)
        digest = unsigned.pop("receipt_digest")
        expected_digest = hashlib.sha256(
            _canonical_destroy_receipt_payload(unsigned)
        ).hexdigest()
    except (KeyError, TypeError, ValueError, UnicodeError, OverflowError):
        return False
    return secrets.compare_digest(digest, expected_digest)


def capture_worker_destroy_lifecycle_claim(
    worker: Worker,
) -> WorkerDestroyLifecycleClaim:
    """Freeze the stable identity behind one successful destroy CAS."""

    if worker.status != "destroying":
        raise ValueError("Worker destroy claim requires destroying status")
    nonce = worker.destroy_lifecycle_nonce
    if (
        not isinstance(nonce, str)
        or len(nonce) != 32
        or any(char not in "0123456789abcdef" for char in nonce)
    ):
        raise ValueError("Worker destroy lifecycle nonce is missing or invalid")
    return WorkerDestroyLifecycleClaim(
        _seal=_WORKER_DESTROY_CLAIM_SEAL,
        worker_id=worker.id,
        created_at=worker.created_at,
        destroy_lifecycle_nonce=nonce,
        cloud_instance_id=worker.cloud_instance_id,
        private_ip=worker.private_ip,
        ccm_port=worker.ccm_port,
        node_drain_claim=_worker_node_drain_claim(worker),
        auth_token=worker.auth_token,
    )


def _worker_destroy_lifecycle_predicates(
    claim: WorkerDestroyLifecycleClaim,
) -> tuple:
    """Return the durable CAS fence for one opaque in-process destroy claim."""

    if (
        not isinstance(claim, WorkerDestroyLifecycleClaim)
        or claim._seal is not _WORKER_DESTROY_CLAIM_SEAL
    ):
        raise ValueError("invalid Worker destroy lifecycle claim")
    return (
        Worker.id == claim.worker_id,
        Worker.status == "destroying",
        (
            Worker.created_at.is_(None)
            if claim.created_at is None
            else Worker.created_at == claim.created_at
        ),
        Worker.destroy_lifecycle_nonce == claim.destroy_lifecycle_nonce,
        (
            Worker.cloud_instance_id.is_(None)
            if claim.cloud_instance_id is None
            else Worker.cloud_instance_id == claim.cloud_instance_id
        ),
        (
            Worker.private_ip.is_(None)
            if claim.private_ip is None
            else Worker.private_ip == claim.private_ip
        ),
        Worker.ccm_port == claim.ccm_port,
    )

# (worker_id, manager_project_id) -> Lock，防并发 task 重复建项目
_project_locks: dict[tuple[int, int], asyncio.Lock] = {}
_task_operation_locks: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[int, asyncio.Lock],
] = WeakKeyDictionary()


class WorkerEndpointNotFoundError(Exception):
    """A caller-requested signal that the Worker returned an exact HTTP 404."""


class WorkerTaskForwardOutcomeUncertainError(RuntimeError):
    """The initial create request may already have committed on the Worker.

    Retrying that POST without an idempotent remote receipt can create a
    second execution or make the Manager declare failure while the Worker is
    still running.  ``cancellation`` preserves an outer shutdown request after
    the Manager has durably quarantined the ambiguous claim.
    """

    def __init__(
        self,
        message: str,
        *,
        cancellation: asyncio.CancelledError | None = None,
    ) -> None:
        super().__init__(message)
        self.cancellation = cancellation


class WorkerTaskForwardAdmissionBlockedError(RuntimeError):
    """A durable termination receipt won before initial Worker creation."""


class WorkerTaskMutationOutcomeUncertainError(RuntimeError):
    """A Worker mutation may have committed without a readable response.

    Callers which opt into this contract must durably quarantine the exact
    Manager-side generation before releasing the per-Task operation lock.  In
    particular, blindly replaying a cancel/stop POST is unsafe: the first
    request may already have terminated the only remote execution.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        cancellation: asyncio.CancelledError | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.cancellation = cancellation


class WorkerTaskPlanDeleteProtocolUnsupported(RuntimeError):
    """The Worker cannot prove atomic Task + first-class Plan deletion."""


def get_task_operation_lock(task_id: int) -> asyncio.Lock:
    """Return the process-wide operation lock for one Task on this event loop.

    Task migration and every Manager→Worker mutation must use the same lock.
    Keeping the registry at module scope avoids two independently constructed
    service objects accidentally creating different locks.  The event-loop key
    keeps async test loops isolated and lets completed loops be collected.
    """

    loop = asyncio.get_running_loop()
    locks = _task_operation_locks.setdefault(loop, {})
    return locks.setdefault(task_id, asyncio.Lock())


class WorkerProxy:
    def __init__(self, db_factory, relay):
        self.db_factory = db_factory
        self.relay = relay

    def task_operation_lock(self, task_id: int) -> asyncio.Lock:
        """Serialize remote operations that can create/mutate one Worker task."""

        return get_task_operation_lock(task_id)

    @staticmethod
    def _api(worker: Worker, path: str) -> str:
        return f"http://{worker.private_ip}:{worker.ccm_port}{path}"

    @staticmethod
    def _require_authenticated_control_plane(worker: Worker) -> None:
        """Refuse every Manager→Worker network effect in legacy open mode."""

        if (
            not isinstance(settings.auth_token, str)
            or not settings.auth_token.strip()
        ):
            raise HTTPException(
                503,
                "AUTH_TOKEN must be configured before Worker operations",
            )
        if (
            not isinstance(worker.auth_token, str)
            or not worker.auth_token.strip()
        ):
            raise HTTPException(
                503,
                "Worker authentication credential is unavailable",
            )

    @classmethod
    def _headers(cls, worker: Worker) -> dict:
        cls._require_authenticated_control_plane(worker)
        return {"Authorization": f"Bearer {worker.auth_token}"}

    @classmethod
    def _ssh(cls, worker: Worker) -> SSHExecutor:
        """Build every WorkerProxy SSH path with per-instance host trust."""
        cls._require_authenticated_control_plane(worker)
        return SSHExecutor(
            host=worker.private_ip,
            user=worker.ssh_user,
            key_path=worker.ssh_key_path or settings.worker_ssh_key_path,
            known_hosts_path=(
                worker_known_hosts_path(worker.cloud_instance_id)
                if worker.cloud_instance_id else None
            ),
        )

    async def get_worker(self, worker_id: int) -> Worker | None:
        async with self.db_factory() as db:
            return await db.get(Worker, worker_id)

    async def require_ready_worker(self, worker_id: int) -> Worker:
        worker = await self.get_worker(worker_id)
        if not worker:
            raise HTTPException(404, f"Worker {worker_id} 不存在")
        if worker.status != "ready" or worker.bootstrap_step is not None:
            raise HTTPException(
                503,
                f"Worker {worker.name} 当前状态 {worker.status}"
                f"/{worker.bootstrap_step or 'normal'}，无法执行操作。"
                "请等待 Worker 恢复或将 task 切回本机执行。",
            )
        return worker

    async def _require_destroy_lifecycle_claim(
        self,
        claim: WorkerDestroyLifecycleClaim,
    ) -> Worker:
        """Resolve one opaque destroy claim without widening ready admission."""

        async with self.db_factory() as db:
            worker = (
                await db.execute(
                    select(Worker).where(
                        *_worker_destroy_lifecycle_predicates(claim)
                    )
                )
            ).scalar_one_or_none()
        # Keep the internal credential out of SQL parameters: driver errors are
        # routinely logged and may render bound values. Endpoint identity plus
        # the dedicated destroy nonce fences lifecycle replacement; compare the
        # credential again in memory before it can authorize a request.
        if worker is None or worker.auth_token != claim.auth_token:
            raise HTTPException(
                409,
                "Worker destroy lifecycle or endpoint identity changed; "
                "remote Task mutation was refused",
            )
        return worker

    async def require_claimed_destroy_drain_proof(
        self,
        claim: WorkerDestroyLifecycleClaim,
    ) -> dict:
        """Require a fresh authenticated proof that the remote node is idle."""

        worker = await self._require_destroy_lifecycle_claim(claim)
        self._require_authenticated_control_plane(worker)
        nonce = secrets.token_hex(16)
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    self._api(worker, "/api/system/worker-drain-proof"),
                    headers=self._headers(worker),
                    json={
                        "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
                        "nonce": nonce,
                        "drain_claim": claim.node_drain_claim,
                    },
                )
                response.raise_for_status()
            remote = response.json()
        except Exception as exc:
            raise HTTPException(
                503,
                "Unable to obtain the authenticated Worker node drain proof",
            ) from exc
        try:
            proof, signature = _validated_signed_destroy_proof_copy(
                remote,
                drain_claim=claim.node_drain_claim,
                auth_token=claim.auth_token or "",
                require_clean=False,
            )
        except ValueError as exc:
            raise HTTPException(
                409,
                "Worker node drain proof failed identity/signature validation",
            ) from exc
        if proof["nonce"] != nonce:
            raise HTTPException(
                409,
                "Worker node drain proof failed nonce validation",
            )
        if not proof["safe_to_destroy"] or proof["blocker_count"] != 0:
            descriptions = []
            for blocker in proof["blockers"][:20]:
                if isinstance(blocker, dict):
                    descriptions.append(
                        f"{blocker.get('kind')}:{blocker.get('id')} "
                        f"({blocker.get('detail')})"
                    )
            detail = "; ".join(descriptions) or "unknown durable blocker"
            raise HTTPException(
                409,
                "Worker node drain proof refused cloud termination: " + detail,
            )
        return {**proof, "signature": signature}

    async def require_claimed_destroy_log_backfill(
        self,
        claim: WorkerDestroyLifecycleClaim,
        task_ids: set[int],
    ) -> None:
        """Prove every stopped Worker mirror's current log tail is local.

        Live relay delivery is not durable acknowledgement: the socket may
        disconnect after the Worker commits its final assistant/tool rows.
        The relay's history backfill validates the exact Manager generation,
        imports the complete current-generation non-user tail under the Task
        writer fence, and returns an id only after that transaction commits.
        Cloud destruction therefore requires exact set equality here.
        """

        expected: set[int] = set()
        for task_id in task_ids:
            try:
                expected.add(validate_manager_allocated_task_id(task_id))
            except Exception as exc:
                raise HTTPException(
                    409,
                    "Worker destroy log backfill received an invalid Task id",
                ) from exc
        if not expected:
            return
        worker = await self._require_destroy_lifecycle_claim(claim)
        if self.relay is None or not hasattr(
            self.relay,
            "_backfill_missing_logs",
        ):
            raise HTTPException(
                503,
                "Worker relay is unavailable for exact log backfill",
            )
        try:
            synced = await self.relay._backfill_missing_logs(
                worker,
                expected,
                sync_status=True,
            )
        except Exception as exc:
            raise HTTPException(
                503,
                "Worker exact-generation log backfill failed",
            ) from exc
        if not isinstance(synced, set) or synced != expected:
            observed = synced if isinstance(synced, set) else set()
            missing = sorted(expected - observed)
            raise HTTPException(
                409,
                "Worker exact-generation log backfill is incomplete for "
                "Task(s): " + ", ".join(str(task_id) for task_id in missing),
            )

    async def begin_claimed_destroy_drain(
        self,
        claim: WorkerDestroyLifecycleClaim,
    ) -> dict:
        """Install the Worker's irreversible mutation-admission fence."""

        worker = await self._require_destroy_lifecycle_claim(claim)
        self._require_authenticated_control_plane(worker)
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    self._api(worker, "/api/system/worker-drain/begin"),
                    headers=self._headers(worker),
                    json={
                        "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
                        "drain_claim": claim.node_drain_claim,
                    },
                )
                response.raise_for_status()
            remote = response.json()
        except Exception as exc:
            raise HTTPException(
                503,
                "Unable to install the authenticated Worker node drain fence",
            ) from exc
        if not isinstance(remote, dict):
            raise HTTPException(409, "Worker node drain response is malformed")
        signed = dict(remote)
        signature = signed.pop("signature", None)
        if (
            signed.get("protocol_version")
            != WORKER_NODE_DRAIN_PROOF_PROTOCOL
            or signed.get("node_role") != "worker"
            or signed.get("drain_claim") != claim.node_drain_claim
            or signed.get("draining") is not True
            or not verify_worker_node_drain_proof_signature(
                signed,
                auth_token=claim.auth_token or "",
                signature=signature,
            )
        ):
            raise HTTPException(
                409,
                "Worker node drain response failed identity/signature validation",
            )
        return remote

    async def seal_claimed_destroy_runtime(
        self,
        claim: WorkerDestroyLifecycleClaim,
    ) -> dict:
        """Drain callbacks and install the exact phase-two runtime seal."""

        worker = await self._require_destroy_lifecycle_claim(claim)
        self._require_authenticated_control_plane(worker)
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    self._api(worker, "/api/system/worker-drain/seal"),
                    headers=self._headers(worker),
                    json={
                        "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
                        "drain_claim": claim.node_drain_claim,
                    },
                )
                response.raise_for_status()
            remote = response.json()
        except Exception as exc:
            raise HTTPException(
                503,
                "Unable to install the authenticated Worker runtime seal",
            ) from exc
        if not isinstance(remote, dict):
            raise HTTPException(409, "Worker runtime seal response is malformed")
        signed = dict(remote)
        signature = signed.pop("signature", None)
        if (
            signed.get("protocol_version")
            != WORKER_NODE_DRAIN_PROOF_PROTOCOL
            or signed.get("node_role") != "worker"
            or signed.get("drain_claim") != claim.node_drain_claim
            or type(signed.get("runtime_sealed")) is not bool
            or type(signed.get("safe_to_seal")) is not bool
            or not isinstance(signed.get("blockers"), list)
            or type(signed.get("blocker_count")) is not int
            or not verify_worker_node_drain_proof_signature(
                signed,
                auth_token=claim.auth_token or "",
                signature=signature,
            )
        ):
            raise HTTPException(
                409,
                "Worker runtime seal response failed identity/signature validation",
            )
        if (
            signed["runtime_sealed"] is not True
            or signed["safe_to_seal"] is not True
            or signed["blocker_count"] != 0
        ):
            descriptions = []
            for blocker in signed["blockers"][:20]:
                if isinstance(blocker, dict):
                    descriptions.append(
                        f"{blocker.get('kind')}:{blocker.get('id')} "
                        f"({blocker.get('detail')})"
                    )
            detail = "; ".join(descriptions) or "unknown runtime blocker"
            raise HTTPException(
                409,
                "Worker runtime seal refused log backfill: " + detail,
            )
        return remote

    async def _require_versioned_plan_protocol(self, worker: Worker) -> None:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("versioned_plan_worker_protocol") != 3
        ):
            raise RuntimeError(
                f"Worker {worker.name} does not support versioned Plan protocol 3"
            )

    async def _require_worker_plan_reconciliation_protocol(
        self,
        worker: Worker,
    ) -> None:
        """Fail closed when a Worker cannot prove exact import identity."""

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
        try:
            payload = response.json()
        except Exception as exc:
            raise WorkerPlanReconciliationUnsupported(
                f"Worker {worker.name} returned invalid recovery capabilities"
            ) from exc
        if (
            not isinstance(payload, dict)
            or payload.get("versioned_plan_worker_protocol") != 3
            or payload.get("worker_plan_reconciliation_protocol")
            != WORKER_PLAN_RECONCILIATION_PROTOCOL
        ):
            raise WorkerPlanReconciliationUnsupported(
                f"Worker {worker.name} does not support read-only Worker Plan "
                f"reconciliation protocol {WORKER_PLAN_RECONCILIATION_PROTOCOL}"
            )

    async def _require_worker_plan_exact_cancel_protocol(
        self,
        worker: Worker,
    ) -> None:
        """Fail closed unless cancellation is bound to immutable import id."""

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError(
                f"Worker {worker.name} returned invalid cancellation capabilities"
            ) from exc
        if (
            not isinstance(payload, dict)
            or payload.get("versioned_plan_worker_protocol") != 3
            or payload.get("worker_plan_exact_cancel_protocol")
            != WORKER_PLAN_EXACT_CANCEL_PROTOCOL
        ):
            raise RuntimeError(
                f"Worker {worker.name} does not support exact Worker Plan "
                f"cancellation protocol {WORKER_PLAN_EXACT_CANCEL_PROTOCOL}"
            )

    async def require_task_plan_delete_protocol(
        self,
        task: Task,
        operation_id: str,
        *,
        operation_lock_held: bool = False,
    ) -> None:
        """Preflight an exact durable delete before its mutation boundary."""

        if not operation_lock_held:
            raise ValueError(
                "Task/Plan delete protocol check requires the Task operation lock"
            )
        if self.db_factory is None:
            raise WorkerTaskPlanDeleteProtocolUnsupported(
                "Worker Task deletion receipt storage is unavailable"
            )
        async with self.db_factory() as db:
            receipt = await active_worker_task_termination_receipt(db, task.id)
        if (
            receipt is None
            or receipt.operation_id != operation_id
            or receipt.side != "manager"
            or receipt.operation != "delete"
            or receipt.status != "pending_remote"
            or receipt.worker_id != task.worker_id
        ):
            raise WorkerTaskPlanDeleteProtocolUnsupported(
                "Task deletion protocol check lost its exact durable owner"
            )
        worker = await self.require_ready_worker(task.worker_id)
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
            response.raise_for_status()
            payload = response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise WorkerTaskPlanDeleteProtocolUnsupported(
                f"Worker {worker.name} deletion capabilities are unavailable"
            ) from exc
        if (
            not isinstance(payload, dict)
            or payload.get("plan_cascade_protocol") != 1
        ):
            raise WorkerTaskPlanDeleteProtocolUnsupported(
                f"Worker {worker.name} does not support atomic Task/Plan "
                "deletion protocol 1"
            )

    async def _require_legacy_plan_execution_carrier_protocol(
        self,
        worker: Worker,
    ) -> None:
        """Require exact readback before trusting an existing Plan carrier."""

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("legacy_plan_execution_carrier_protocol")
            != LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
        ):
            raise RuntimeError(
                f"Worker {worker.name} does not support legacy Plan execution "
                f"carrier protocol "
                f"{LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION}"
            )

    async def get_legacy_plan_execution_carrier_proof(
        self,
        worker: Worker,
        task_id: int,
    ) -> LegacyPlanExecutionCarrierProof | None:
        """Read one existing Worker's semantic carrier proof, never create it."""

        if type(task_id) is not int or task_id <= 0:
            raise ValueError("legacy Plan carrier task_id must be positive")
        await self._require_legacy_plan_execution_carrier_protocol(worker)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(
                    worker,
                    f"/api/tasks/{task_id}/legacy-plan-execution-carrier-proof",
                ),
                headers=self._headers(worker),
            )
        if response.status_code in {404, 409}:
            # Both outcomes prove that the assigned Worker cannot supply the
            # exact migrated carrier.  Recovery must durably quarantine the
            # Manager mirror instead of retrying a permanent 409 forever.
            return None
        response.raise_for_status()
        try:
            proof = parse_legacy_plan_execution_carrier_proof(response.json())
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Worker {worker.name} returned an invalid legacy Plan "
                "execution carrier proof"
            ) from exc
        if proof.task_id != task_id:
            raise RuntimeError(
                f"Worker {worker.name} returned a legacy Plan carrier proof "
                "for another Task"
            )
        return proof

    async def get_plan_repo_revision(
        self,
        *,
        worker: Worker,
        manager_project_id: int | None,
        target_task_id: int | None,
    ) -> dict | None:
        """Read the execution node's repository fingerprint for staleness."""

        await self._require_versioned_plan_protocol(worker)
        worker_project_id = None
        if manager_project_id is not None:
            async with self.db_factory() as db:
                current = await db.get(Worker, worker.id)
                mapping = dict(current.project_mapping or {}) if current else {}
            worker_project_id = mapping.get(str(manager_project_id))
            if worker_project_id is None:
                raise RuntimeError("Worker Project mapping is missing")
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(worker, "/api/plans/worker-repo-revision"),
                headers=self._headers(worker),
                json={
                    "project_id": worker_project_id,
                    "target_task_id": target_task_id,
                },
            )
            response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Worker returned an invalid repository receipt")
        revision = payload.get("repo_revision")
        if revision is not None and not isinstance(revision, dict):
            raise RuntimeError("Worker returned an invalid repository fingerprint")
        return revision

    async def get_plan_application_receipt(
        self, worker: Worker, receipt_key: str
    ) -> dict | None:
        await self._require_versioned_plan_protocol(worker)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(
                    worker,
                    f"/api/plans/worker-application-receipts/{receipt_key}",
                ),
                headers=self._headers(worker),
            )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("receipt_key") != receipt_key:
            raise RuntimeError("Worker returned an invalid Plan application receipt")
        return payload

    async def get_worker_turn_handoff_receipt(
        self,
        worker: Worker,
        task_id: int,
        handoff_id: str,
        incarnation_id: str,
    ) -> dict | None:
        headers = self._headers(worker)
        headers["X-CCM-Task-Incarnation"] = incarnation_id
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(
                    worker,
                    f"/api/tasks/{task_id}/worker-turn-handoffs/{handoff_id}",
                ),
                headers=headers,
            )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("handoff_id") != handoff_id
            or payload.get("task_id") != task_id
            or payload.get("incarnation_id") != incarnation_id
        ):
            raise RuntimeError(
                "Worker returned an invalid turn handoff receipt"
            )
        return payload

    async def resume_worker_turn_handoff(
        self,
        worker: Worker,
        task_id: int,
        handoff_id: str,
        incarnation_id: str,
    ) -> dict:
        headers = self._headers(worker)
        headers["X-CCM-Task-Incarnation"] = incarnation_id
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(
                    worker,
                    f"/api/tasks/{task_id}/worker-turn-handoffs/"
                    f"{handoff_id}/resume",
                ),
                headers=headers,
            )
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("handoff_id") != handoff_id
            or payload.get("task_id") != task_id
            or payload.get("incarnation_id") != incarnation_id
        ):
            raise RuntimeError(
                "Worker returned an invalid turn handoff resume receipt"
            )
        return payload

    async def resolve_plan_application_receipt(
        self,
        worker: Worker,
        receipt_key: str,
        *,
        action: str,
        note: str,
    ) -> dict:
        await self._require_versioned_plan_protocol(worker)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(
                    worker,
                    f"/api/plans/worker-application-receipts/{receipt_key}/resolve",
                ),
                headers=self._headers(worker),
                json={"action": action, "note": note},
            )
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("receipt_key") != receipt_key
            or payload.get("action") != action
        ):
            raise RuntimeError(
                "Worker returned an invalid Plan delivery resolution"
            )
        return payload

    @staticmethod
    def _attachment_manifest(
        paths: list[str],
        *,
        receipt_paths: list[str] | None = None,
    ) -> list[dict]:
        """Hash local files while naming their Worker-side upload paths."""

        reported_paths = paths if receipt_paths is None else receipt_paths
        if len(reported_paths) != len(paths):
            raise RuntimeError("Plan attachment receipt paths do not match uploads")
        manifest = []
        for path, receipt_path in zip(paths, reported_paths, strict=True):
            absolute = os.path.abspath(path)
            if path != absolute:
                raise RuntimeError("Plan attachment path must be absolute")
            absolute_receipt = os.path.abspath(receipt_path)
            if receipt_path != absolute_receipt:
                raise RuntimeError("Plan attachment receipt path must be absolute")
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(absolute, flags)
            digest = hashlib.sha256()
            size = 0
            with os.fdopen(fd, "rb") as handle:
                metadata = os.fstat(handle.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    raise RuntimeError("Plan attachment must be a regular file")
                if hasattr(os, "getuid") and metadata.st_uid != os.getuid():
                    raise RuntimeError("Plan attachment owner does not match CCM")
                while chunk := handle.read(1024 * 1024):
                    size += len(chunk)
                    digest.update(chunk)
            manifest.append({
                "path": absolute_receipt,
                "size": size,
                "sha256": digest.hexdigest(),
            })
        return manifest

    @staticmethod
    def _plan_attachment_payload(
        items: list[dict] | None,
    ) -> tuple[list[str], list[str], list[dict]]:
        rows = [item for item in (items or []) if isinstance(item, dict)]
        paths = [item["path"] for item in rows if isinstance(item.get("path"), str)]
        if len(paths) != len(rows):
            raise RuntimeError("Plan attachment mirror is missing a validated path")
        images = [
            item["path"]
            for item in rows
            if item.get("is_image") is True
        ]
        public = [
            {key: item[key] for key in ("url", "name", "is_image")}
            for item in rows
        ]
        return paths, images, public

    @staticmethod
    def _version_seed(version: PlanVersion) -> dict:
        return {
            "source_version_id": version.id,
            "version_number": version.version_number,
            "content": version.content,
            "context_session_id": version.context_session_id,
            "context_log_id": version.context_log_id,
            "context_snapshot": version.context_snapshot,
            "repo_revision": version.repo_revision,
            "reviewer_repo_revision": version.reviewer_repo_revision,
            "review_verdict": version.review_verdict,
            "review_feedback": version.review_feedback,
            "review_exhausted": version.review_exhausted,
            "reviewed_at": (
                version.reviewed_at.isoformat()
                if version.reviewed_at is not None
                else None
            ),
            "human_decision": version.human_decision,
        }

    @staticmethod
    def _versioned_plan_import_digest(
        payload: dict,
        attachment_receipt: list[dict],
    ) -> str:
        """Match the Worker's immutable import identity byte-for-byte."""

        identity = {
            key: value
            for key, value in payload.items()
            if key not in {"manager_claim_generation", "attachment_manifest"}
        }
        identity["attachment_receipt"] = attachment_receipt
        return hashlib.sha256(
            json.dumps(
                identity,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()

    async def run_versioned_plan_until_pause(
        self,
        plan: Plan,
        run: PlanAgentRun,
        *,
        on_remote_possible: Callable[[str], Awaitable[None]] | None = None,
    ) -> dict:
        """Mirror/resume one Manager PlanRun and return an authoritative pause."""

        if plan.worker_id is None or run.worker_id != plan.worker_id:
            raise RuntimeError("Plan Run Worker assignment changed before forwarding")
        worker = await self.require_ready_worker(plan.worker_id)
        # A new mutating import is admitted only when the same Worker can
        # later prove its exact identity after an ACK loss/restart.
        await self._require_worker_plan_reconciliation_protocol(worker)
        worker_project_id = (
            await self.ensure_worker_project(worker, plan)
            if plan.project_id is not None
            else None
        )
        plan_paths, plan_images, plan_attachments = self._plan_attachment_payload(
            plan.initial_attachments
        )
        run_paths, run_images, run_attachments = self._plan_attachment_payload(
            run.attachments
        )
        paths = list(dict.fromkeys([*plan_paths, *run_paths]))
        image_path_set = {*plan_images, *run_images}
        attachment_by_path = {
            path: attachment
            for path, attachment in [
                *zip(plan_paths, plan_attachments, strict=True),
                *zip(run_paths, run_attachments, strict=True),
            ]
        }
        attachments = [attachment_by_path[path] for path in paths]
        remote_paths = worker_managed_upload_paths(paths)
        remote_image_paths = [
            remote_path
            for path, remote_path in zip(paths, remote_paths, strict=True)
            if path in image_path_set
        ]
        if paths:
            await self.push_files(worker, paths, remote_paths=remote_paths)
        # ``push_files`` re-proves every persisted source against the current
        # Manager upload root before the first cross-host effect.  Hash only
        # after that proof so a corrupted Plan row cannot make the manifest
        # builder open an arbitrary Manager file even though the later copy
        # would have failed closed.
        attachment_manifest = self._attachment_manifest(
            paths,
            receipt_paths=remote_paths,
        )

        base_version = None
        if run.base_version_id is not None:
            async with self.db_factory() as db:
                base_version = await db.get(PlanVersion, run.base_version_id)
            if base_version is None:
                raise RuntimeError("Plan Run base Version disappeared before forwarding")
        request_text = run.request_text or plan.initial_request
        base_seed = self._version_seed(base_version) if base_version is not None else None
        if run.run_type == "fork" and base_version is not None:
            request_text = (
                f"{request_text}\n\n[Base Version selected for this fork]\n"
                f"{base_version.content}"
            )
            # A fork starts a fresh Version sequence; materializing its source
            # inside the new Plan would incorrectly make the first output vN+1.
            base_seed = None

        payload = {
            "protocol": 3,
            "plan_id": plan.id,
            "run_id": run.id,
            # This fences the Manager lifecycle only. The imported Worker Run
            # has an independent local generation used for its own retries and
            # input answers.
            "manager_claim_generation": run.generation,
            "title": plan.title,
            "initial_request": plan.initial_request,
            "target_task_id": plan.target_task_id,
            "project_id": worker_project_id,
            "target_branch": plan.target_branch,
            "priority": plan.priority,
            "timeout_hours": plan.timeout_hours,
            "pipeline_config": run.pipeline_config or plan.pipeline_config,
            "run_type": run.run_type,
            "source_run_id": run.source_run_id,
            "request_text": request_text,
            "context_session_id": run.context_session_id,
            "context_log_id": run.context_log_id,
            "context_snapshot": run.context_snapshot,
            "repo_revision": run.repo_revision,
            "max_interactions": run.max_interactions,
            "base_version": base_seed,
            "file_paths": remote_paths or None,
            "image_paths": remote_image_paths or None,
            "attachments": attachments or None,
            "attachment_manifest": attachment_manifest or None,
        }
        import_digest = self._versioned_plan_import_digest(
            payload,
            attachment_manifest,
        )
        # The callback commits Manager-side uncertainty before the first
        # mutating Plan import request. If it is cancelled or fails, the POST
        # must never be attempted.
        if on_remote_possible is not None:
            await on_remote_possible(import_digest)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(worker, "/api/plans/worker-import"),
                headers=self._headers(worker),
                json=payload,
            )
            response.raise_for_status()
            imported = response.json()
        remote_run = imported.get("run") if isinstance(imported, dict) else None
        base_worker_version_id = (
            imported.get("base_worker_version_id")
            if isinstance(imported, dict)
            else None
        )
        if not isinstance(remote_run, dict) or remote_run.get("id") != run.id:
            raise RuntimeError("Worker returned an invalid Plan Run import receipt")
        if imported.get("import_payload_digest") != import_digest:
            raise RuntimeError(
                "Worker Plan import receipt does not match the immutable payload"
            )
        if imported.get("attachment_receipt") != attachment_manifest:
            raise RuntimeError("Worker Plan attachment receipt does not match the manifest")
        from backend.services.worker_plan_dispatch import (
            WorkerPlanDispatchConflict,
            validate_worker_plan_outcome_graph,
        )

        try:
            validate_worker_plan_outcome_graph(
                {
                    "protocol": 3,
                    "base_worker_version_id": base_worker_version_id,
                    "run": remote_run,
                    "versions": [],
                },
                plan_id=plan.id,
                run_id=run.id,
                require_version_closure=False,
            )
        except WorkerPlanDispatchConflict as exc:
            raise RuntimeError(
                "Worker returned an invalid Plan Run import receipt"
            ) from exc

        if remote_run.get("status") == "waiting_user":
            remote_input_id = remote_run.get("open_input_request_id")
            async with self.db_factory() as db:
                answer = (
                    await db.execute(
                        select(PlanInputRequest)
                        .where(
                            PlanInputRequest.run_id == run.id,
                            PlanInputRequest.worker_id == worker.id,
                            PlanInputRequest.worker_input_request_id == remote_input_id,
                            PlanInputRequest.status == "answered",
                        )
                        .order_by(PlanInputRequest.id.desc())
                        .limit(1)
                    )
                ).scalar_one_or_none()
            if answer is not None:
                answer_paths, answer_images, answer_attachments = (
                    self._plan_attachment_payload(answer.attachments)
                )
                remote_answer_paths = worker_managed_upload_paths(answer_paths)
                answer_image_set = set(answer_images)
                remote_answer_images = [
                    remote_path
                    for path, remote_path in zip(
                        answer_paths,
                        remote_answer_paths,
                        strict=True,
                    )
                    if path in answer_image_set
                ]
                if answer_paths:
                    await self.push_files(
                        worker,
                        answer_paths,
                        remote_paths=remote_answer_paths,
                    )
                answer_manifest = self._attachment_manifest(
                    answer_paths,
                    receipt_paths=remote_answer_paths,
                )
                async with httpx.AsyncClient(timeout=30) as client:
                    response = await client.post(
                        self._api(
                            worker,
                            f"/api/plan-runs/{run.id}/input-requests/{remote_input_id}/answer",
                        ),
                        headers=self._headers(worker),
                        json={
                            "expected_run_generation": remote_run["generation"],
                            "idempotency_key": answer.answer_idempotency_key,
                            "answers": answer.answers or [],
                            "response_text": answer.response_text,
                            "file_paths": remote_answer_paths or None,
                            "image_paths": remote_answer_images or None,
                            "attachments": answer_attachments or None,
                            "attachment_manifest": answer_manifest or None,
                        },
                    )
                    response.raise_for_status()
                remote_run["status"] = "queued"

        timeout_seconds = (
            plan.timeout_hours * 3600
            if plan.timeout_hours is not None and plan.timeout_hours > 0
            else (
                None
                if plan.timeout_hours == 0
                else settings.task_timeout_seconds
            )
        )
        deadline = (
            asyncio.get_running_loop().time() + max(300.0, timeout_seconds + 300)
            if timeout_seconds is not None
            else None
        )
        while remote_run.get("status") in {"queued", "running"}:
            if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError("Worker Plan Run outcome polling timed out")
            await asyncio.sleep(1)
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, f"/api/plan-runs/{run.id}"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
                remote_run = response.json()
            if not isinstance(remote_run, dict) or remote_run.get("id") != run.id:
                raise RuntimeError("Worker returned an invalid Plan Run snapshot")
            try:
                validate_worker_plan_outcome_graph(
                    {
                        "protocol": 3,
                        "base_worker_version_id": base_worker_version_id,
                        "run": remote_run,
                        "versions": [],
                    },
                    plan_id=plan.id,
                    run_id=run.id,
                    require_version_closure=False,
                )
            except WorkerPlanDispatchConflict as exc:
                raise RuntimeError(
                    "Worker returned an invalid Plan Run snapshot"
                ) from exc

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(worker, f"/api/plans/{plan.id}/versions"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
            versions = response.json()
        if not isinstance(versions, list):
            raise RuntimeError("Worker returned an invalid Plan Version list")
        versions = [
            version
            for version in versions
            if isinstance(version, dict)
            and type(version.get("produced_by_run_id")) is int
            and version.get("produced_by_run_id") == run.id
        ]
        outcome = {
            "protocol": 3,
            "base_worker_version_id": base_worker_version_id,
            "run": remote_run,
            "versions": versions,
        }
        try:
            validate_worker_plan_outcome_graph(
                outcome,
                plan_id=plan.id,
                run_id=run.id,
            )
        except WorkerPlanDispatchConflict as exc:
            raise RuntimeError("Worker returned an invalid Plan outcome graph") from exc
        return outcome

    async def _read_worker_plan_import_audit(
        self,
        *,
        worker: Worker,
        plan_id: int,
        run_id: int,
        payload_digest: str,
    ) -> dict:
        """Read one exact Worker mirror without creating or resuming it."""

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(
                    worker,
                    f"/api/plan-runs/{run_id}/worker-import-audit",
                ),
                headers=self._headers(worker),
                params={
                    "plan_id": plan_id,
                    "payload_digest": payload_digest,
                },
            )
        if response.status_code == 409:
            raise WorkerPlanRemoteIdentityConflict(
                "Worker Plan Run identity conflicts with the durable Manager receipt"
            )
        response.raise_for_status()
        try:
            audit = response.json()
        except Exception as exc:
            raise RuntimeError("Worker returned an invalid Plan import audit") from exc
        if (
            not isinstance(audit, dict)
            or audit.get("protocol") != WORKER_PLAN_RECONCILIATION_PROTOCOL
            or type(audit.get("plan_id")) is not int
            or audit.get("plan_id") != plan_id
            or type(audit.get("run_id")) is not int
            or audit.get("run_id") != run_id
            or audit.get("payload_digest") != payload_digest
            or audit.get("state") not in {"absent", "cancelled", "matched"}
        ):
            raise RuntimeError("Worker returned an invalid Plan import audit")
        if audit["state"] in {"absent", "cancelled"} and (
            audit.get("run") is not None
            or audit.get("versions") != []
            or audit.get("base_worker_version_id") is not None
        ):
            raise RuntimeError("Worker returned an invalid absent Plan audit")
        if audit["state"] == "matched":
            remote = audit.get("run")
            versions = audit.get("versions")
            if (
                not isinstance(remote, dict)
                or type(remote.get("id")) is not int
                or remote.get("id") != run_id
                or type(remote.get("plan_id")) is not int
                or remote.get("plan_id") != plan_id
                or not isinstance(versions, list)
            ):
                raise RuntimeError("Worker returned an invalid matched Plan audit")
            from backend.services.worker_plan_dispatch import (
                WorkerPlanDispatchConflict,
                validate_worker_plan_outcome_graph,
            )

            try:
                validate_worker_plan_outcome_graph(
                    {
                        "protocol": 3,
                        "base_worker_version_id": audit.get(
                            "base_worker_version_id"
                        ),
                        "run": remote,
                        "versions": versions,
                    },
                    plan_id=plan_id,
                    run_id=run_id,
                )
            except WorkerPlanDispatchConflict as exc:
                raise RuntimeError(
                    "Worker returned an invalid matched Plan audit"
                ) from exc
        return audit

    async def reconcile_versioned_plan_until_pause(
        self,
        plan: Plan,
        run: PlanAgentRun,
        *,
        payload_digest: str,
    ) -> dict:
        """Recover a maybe-imported Run through exact readback, never import.

        The only mutation allowed here is replaying an already committed
        Manager input answer after an audit proves the exact Worker Run is
        still waiting on that exact request.  The answer endpoint has its own
        generation and idempotency fences.
        """

        if (
            plan.worker_id is None
            or run.worker_id != plan.worker_id
            or len(payload_digest) != 64
        ):
            raise WorkerPlanRemoteIdentityConflict(
                "Worker Plan recovery identity is inconsistent"
            )
        worker = await self.require_ready_worker(plan.worker_id)
        await self._require_worker_plan_reconciliation_protocol(worker)
        audit = await self._read_worker_plan_import_audit(
            worker=worker,
            plan_id=plan.id,
            run_id=run.id,
            payload_digest=payload_digest,
        )
        if audit["state"] == "absent":
            raise WorkerPlanRemoteAbsent(
                "Worker audit proved that the Plan Run was never imported"
            )
        if audit["state"] == "cancelled":
            raise WorkerPlanRemoteCancelled(
                "Worker audit proved that the exact Plan import was cancelled"
            )

        remote_run = audit["run"]
        base_worker_version_id = audit.get("base_worker_version_id")
        timeout_seconds = (
            plan.timeout_hours * 3600
            if plan.timeout_hours is not None and plan.timeout_hours > 0
            else (
                None
                if plan.timeout_hours == 0
                else settings.task_timeout_seconds
            )
        )
        deadline = (
            asyncio.get_running_loop().time() + max(300.0, timeout_seconds + 300)
            if timeout_seconds is not None
            else None
        )
        while True:
            if remote_run.get("status") == "waiting_user":
                remote_input_id = remote_run.get("open_input_request_id")
                async with self.db_factory() as db:
                    answer = (
                        await db.execute(
                            select(PlanInputRequest)
                            .where(
                                PlanInputRequest.run_id == run.id,
                                PlanInputRequest.worker_id == worker.id,
                                PlanInputRequest.worker_input_request_id
                                == remote_input_id,
                                PlanInputRequest.status == "answered",
                            )
                            .order_by(PlanInputRequest.id.desc())
                            .limit(1)
                        )
                    ).scalar_one_or_none()
                if answer is None:
                    break
                answer_paths, answer_images, answer_attachments = (
                    self._plan_attachment_payload(answer.attachments)
                )
                remote_answer_paths = worker_managed_upload_paths(answer_paths)
                answer_image_set = set(answer_images)
                remote_answer_images = [
                    remote_path
                    for path, remote_path in zip(
                        answer_paths,
                        remote_answer_paths,
                        strict=True,
                    )
                    if path in answer_image_set
                ]
                if answer_paths:
                    await self.push_files(
                        worker,
                        answer_paths,
                        remote_paths=remote_answer_paths,
                    )
                answer_manifest = self._attachment_manifest(
                    answer_paths,
                    receipt_paths=remote_answer_paths,
                )
                async with httpx.AsyncClient(timeout=30) as client:
                    response = await client.post(
                        self._api(
                            worker,
                            f"/api/plan-runs/{run.id}/input-requests/"
                            f"{remote_input_id}/answer",
                        ),
                        headers=self._headers(worker),
                        json={
                            "expected_run_generation": remote_run["generation"],
                            "idempotency_key": answer.answer_idempotency_key,
                            "answers": answer.answers or [],
                            "response_text": answer.response_text,
                            "file_paths": remote_answer_paths or None,
                            "image_paths": remote_answer_images or None,
                            "attachments": answer_attachments or None,
                            "attachment_manifest": answer_manifest or None,
                        },
                    )
                    response.raise_for_status()

            if remote_run.get("status") not in {"queued", "running", "waiting_user"}:
                break
            if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError("Worker Plan Run reconciliation timed out")
            await asyncio.sleep(1)
            audit = await self._read_worker_plan_import_audit(
                worker=worker,
                plan_id=plan.id,
                run_id=run.id,
                payload_digest=payload_digest,
            )
            if audit["state"] != "matched":
                raise WorkerPlanRemoteIdentityConflict(
                    "Worker Plan mirror disappeared during reconciliation"
                )
            remote_run = audit["run"]

        return {
            "protocol": 3,
            "base_worker_version_id": base_worker_version_id,
            "run": remote_run,
            "versions": audit["versions"],
        }

    async def materialize_plan_version(
        self,
        *,
        worker: Worker,
        plan: Plan,
        version: PlanVersion,
    ) -> int:
        """Ensure an exact immutable Version exists on the target Worker."""

        await self._require_versioned_plan_protocol(worker)
        worker_project_id = (
            await self.ensure_worker_project(worker, plan)
            if plan.project_id is not None
            else None
        )
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(worker, "/api/plans/worker-materialize-version"),
                headers=self._headers(worker),
                json={
                    "protocol": 3,
                    "plan_id": plan.id,
                    "title": plan.title,
                    "initial_request": plan.initial_request,
                    "target_task_id": plan.target_task_id,
                    "project_id": worker_project_id,
                    "target_branch": plan.target_branch,
                    "priority": plan.priority,
                    "timeout_hours": plan.timeout_hours,
                    "pipeline_config": plan.pipeline_config,
                    "version": self._version_seed(version),
                },
            )
            response.raise_for_status()
            receipt = response.json()
        remote_id = receipt.get("id") if isinstance(receipt, dict) else None
        if isinstance(remote_id, bool) or not isinstance(remote_id, int):
            raise RuntimeError("Worker returned an invalid Version materialization receipt")
        return remote_id

    async def cancel_versioned_plan_run(
        self,
        worker_id: int,
        run_id: int,
        *,
        plan_id: int,
        payload_digest: str,
    ) -> dict:
        if (
            type(plan_id) is not int
            or plan_id <= 0
            or type(run_id) is not int
            or run_id <= 0
            or not isinstance(payload_digest, str)
            or len(payload_digest) != 64
            or any(character not in "0123456789abcdef" for character in payload_digest)
        ):
            raise RuntimeError("Worker Plan cancellation identity is invalid")
        worker = await self.require_ready_worker(worker_id)
        await self._require_worker_plan_exact_cancel_protocol(worker)
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                self._api(
                    worker,
                    f"/api/plan-runs/{run_id}/worker-import-cancel",
                ),
                headers=self._headers(worker),
                json={
                    "protocol": WORKER_PLAN_EXACT_CANCEL_PROTOCOL,
                    "plan_id": plan_id,
                    "payload_digest": payload_digest,
                },
            )
        response.raise_for_status()
        try:
            receipt = response.json()
        except ValueError as exc:
            raise RuntimeError(
                "Worker returned an invalid Plan cancellation receipt"
            ) from exc
        if (
            not isinstance(receipt, dict)
            or receipt.get("protocol") != WORKER_PLAN_EXACT_CANCEL_PROTOCOL
            or receipt.get("state") not in {"absent", "terminal"}
            or type(receipt.get("plan_id")) is not int
            or receipt.get("plan_id") != plan_id
            or type(receipt.get("run_id")) is not int
            or receipt.get("run_id") != run_id
            or receipt.get("payload_digest") != payload_digest
            or (
                receipt.get("state") == "absent"
                and (
                    receipt.get("run") is not None
                    or "base_worker_version_id" not in receipt
                    or receipt.get("base_worker_version_id") is not None
                    or receipt.get("versions") != []
                )
            )
            or (
                receipt.get("state") == "terminal"
                and (
                    not isinstance(receipt.get("run"), dict)
                    or type(receipt["run"].get("id")) is not int
                    or receipt["run"].get("id") != run_id
                    or type(receipt["run"].get("plan_id")) is not int
                    or receipt["run"].get("plan_id") != plan_id
                    or receipt["run"].get("status")
                    not in {"completed", "failed", "cancelled"}
                    or not isinstance(receipt.get("versions"), list)
                    or isinstance(receipt.get("base_worker_version_id"), bool)
                    or (
                        receipt.get("base_worker_version_id") is not None
                        and not isinstance(
                            receipt.get("base_worker_version_id"),
                            int,
                        )
                    )
                )
            )
        ):
            raise RuntimeError(
                "Worker returned a non-terminal exact Plan cancellation receipt"
            )
        if receipt["state"] == "terminal":
            from backend.services.worker_plan_dispatch import (
                WorkerPlanDispatchConflict,
                validate_worker_terminal_outcome_graph,
            )

            try:
                validate_worker_terminal_outcome_graph(
                    {
                        "protocol": 3,
                        "base_worker_version_id": receipt.get(
                            "base_worker_version_id"
                        ),
                        "run": receipt["run"],
                        "versions": receipt["versions"],
                    },
                    plan_id=plan_id,
                    run_id=run_id,
                )
            except WorkerPlanDispatchConflict as exc:
                raise RuntimeError(
                    "Worker returned an invalid terminal Plan outcome graph"
                ) from exc
        return receipt

    # ------------------------------------------------------------------
    # 项目映射（设计 §8）
    # ------------------------------------------------------------------

    async def ensure_worker_project(self, worker: Worker, task: Task) -> int:
        """确保 worker 上有 task 对应的项目，返回 worker 侧 project_id。

        Phase 2 仅支持有 git remote 的项目（worker 自己 clone）；
        纯本地项目走 Phase 3 的播种方案，这里直接报错。
        """
        if not task.project_id:
            raise RuntimeError("worker task 必须关联项目（需要 git 信息）")

        key = (worker.id, task.project_id)
        lock = _project_locks.setdefault(key, asyncio.Lock())
        async with lock:
            async with self.db_factory() as db:
                w = await db.get(Worker, worker.id)
                mapping = dict(w.project_mapping or {})
                project = await db.get(Project, task.project_id)
            if not project:
                raise RuntimeError(f"项目 {task.project_id} 不存在")

            def require_exact_project_identity(remote: object) -> dict:
                """Reject name/id reuse that points at another checkout."""

                if not isinstance(remote, dict):
                    raise RuntimeError("Worker 返回了无效的 Project 记录")
                expected_git = (project.git_url or "").strip() or None
                remote_git = (remote.get("git_url") or "").strip() or None
                expected_branch = project.default_branch or "main"
                remote_branch = remote.get("default_branch") or "main"
                mismatch = (
                    remote.get("name") != project.name
                    or remote_git != expected_git
                    or remote_branch != expected_branch
                )
                if expected_git is None:
                    expected_path = os.path.normpath(
                        os.path.expanduser(project.local_path or "")
                    )
                    remote_path = os.path.normpath(
                        os.path.expanduser(str(remote.get("local_path") or ""))
                    )
                    mismatch = mismatch or remote_path != expected_path
                if mismatch:
                    # Do not silently bind a recreated Manager Project to a
                    # stale same-name/mapped Worker checkout.  That can execute
                    # a Task against an entirely different repository and can
                    # also reuse the former Project's credentials.
                    raise RuntimeError(
                        "Worker 上的同名或已映射 Project 与 Manager 的源身份不一致；"
                        "已拒绝复用旧 checkout"
                    )
                remote_id = remote.get("id")
                if isinstance(remote_id, bool) or not isinstance(remote_id, int):
                    raise RuntimeError("Worker 返回了无效的 Project id")
                return remote

            async with httpx.AsyncClient(timeout=30) as c:
                remote = None
                mapped_remote_id = mapping.get(str(task.project_id))
                if (
                    isinstance(mapped_remote_id, int)
                    and not isinstance(mapped_remote_id, bool)
                ):
                    # A Worker Project id is not immutable across deletion or
                    # replacement.  Validate the mapped row on every routing
                    # admission instead of treating the Manager-side JSON cache
                    # as repository identity.
                    r = await c.get(
                        self._api(worker, f"/api/projects/{mapped_remote_id}"),
                        headers=self._headers(worker),
                    )
                    if r.status_code != 404:
                        r.raise_for_status()
                        remote = require_exact_project_identity(r.json())

                if remote is None:
                    # 同名项目可能已存在（之前转发过/手工建过），但名字本身
                    # 从来不是源身份；只有完整 checkout 身份一致才允许复用。
                    r = await c.get(
                        self._api(worker, "/api/projects"),
                        headers=self._headers(worker),
                    )
                    r.raise_for_status()
                    items = r.json()
                    if isinstance(items, dict):
                        items = items.get("projects", [])
                    if not isinstance(items, list):
                        raise RuntimeError("Worker 返回了无效的 Project 列表")
                    candidate = next(
                        (
                            p
                            for p in items
                            if isinstance(p, dict)
                            and p.get("name") == project.name
                        ),
                        None,
                    )
                    if candidate is not None:
                        remote = require_exact_project_identity(candidate)

                if remote is None:
                    if not project.git_url:
                        # 纯本地项目：先把整个项目目录（含 .git 和未提交改动）
                        # rsync 到 Worker 同路径；Worker 见 .git 后跳过 init。
                        path = os.path.expanduser(project.local_path).rstrip("/")
                        if not os.path.isdir(path):
                            raise RuntimeError(f"项目目录不存在: {path}")
                        ssh = self._ssh(worker)
                        await ssh.run(f"mkdir -p {path}")
                        await ssh.rsync_to(
                            path + "/",
                            path + "/",
                            excludes=[],
                            timeout=1200,
                        )
                    r = await c.post(
                        self._api(worker, "/api/projects"),
                        headers=self._headers(worker),
                        json={
                            "name": project.name,
                            "git_url": project.git_url,
                            "default_branch": project.default_branch or "main",
                            "git_author_name": project.git_author_name,
                            "git_author_email": project.git_author_email,
                            "git_credential_type": project.git_credential_type,
                            "git_https_username": project.git_https_username,
                            "git_https_token": project.git_https_token,
                        },
                    )
                    r.raise_for_status()
                    remote = require_exact_project_identity(r.json())
                remote_id = remote["id"]

                # clone 是后台任务，等 status=ready（worker dispatch 需要 local_path 就绪）
                deadline = asyncio.get_event_loop().time() + 300
                while remote.get("status") != "ready":
                    if remote.get("status") == "error":
                        # Worker-side clone already failed; waiting out the
                        # full deadline only hides the real reason as a
                        # misleading timeout.
                        raise RuntimeError(
                            f"worker 项目 {project.name} clone 失败: "
                            f"{remote.get('error_message') or 'unknown error'}"
                        )
                    if asyncio.get_event_loop().time() > deadline:
                        raise RuntimeError(f"worker 项目 {project.name} clone 超时")
                    await asyncio.sleep(3)
                    r = await c.get(
                        self._api(worker, f"/api/projects/{remote_id}"),
                        headers=self._headers(worker),
                    )
                    r.raise_for_status()
                    remote = r.json()

            async with self.db_factory() as db:
                w = await db.get(Worker, worker.id)
                mapping = dict(w.project_mapping or {})
                mapping[str(task.project_id)] = remote_id
                w.project_mapping = mapping
                await db.commit()
            return remote_id

    # ------------------------------------------------------------------
    # 任务转发（设计 §5.3）
    # ------------------------------------------------------------------

    async def require_worker_fast_support(
        self,
        worker: Worker,
        task: Task,
    ) -> None:
        """Fail before creation when a Worker cannot prove required features.

        Older Workers ignore unknown Task fields, which would otherwise let a
        Manager display Fast while the remote turn runs as Standard, or run a
        PR review from the Worker's CCM checkout without snapshot isolation.
        """

        needs_fast = (
            (task.provider or "claude").lower() == "codex"
            and (task.codex_service_tier or "default") == "priority"
        )
        needs_pr_snapshot_context = is_pr_sandbox_task(task)
        if not needs_fast and not needs_pr_snapshot_context:
            return

        async with httpx.AsyncClient(timeout=30) as c:
            response = await c.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
        try:
            config = response.json()
        except Exception as exc:
            raise RuntimeError(
                f"Worker {worker.name} 无法确认任务所需能力，任务未转发"
            ) from exc
        if not isinstance(config, dict):
            raise RuntimeError(
                f"Worker {worker.name} 无法确认任务所需能力，任务未转发"
            )

        if (
            needs_pr_snapshot_context
            and config.get("pr_review_snapshot_context_version")
            != PR_REVIEW_SNAPSHOT_CONTEXT_VERSION
        ):
            raise RuntimeError(
                f"Worker {worker.name} 未声明 PR 审核快照隔离能力 v"
                f"{PR_REVIEW_SNAPSHOT_CONTEXT_VERSION}，任务未转发"
            )

        if not needs_fast:
            return

        model = task.model
        if not model or model == "default":
            model = config.get("default_codex_model")
        tiers_by_model = config.get("codex_model_service_tiers")
        supported = (
            tiers_by_model.get(model)
            if isinstance(tiers_by_model, dict) and isinstance(model, str)
            else None
        )
        if not isinstance(supported, list) or "priority" not in supported:
            raise RuntimeError(
                f"Worker {worker.name} 未声明模型 {model or 'default'} "
                "支持 Codex Fast，任务未转发"
            )

    async def require_worker_delegated_principal_support(
        self,
        worker: Worker,
    ) -> None:
        """Reject mixed-version Workers before the Task creation boundary.

        Pydantic versions used by older Workers may ignore unknown principal
        fields and immediately dispatch a Task under a different authority.
        A response-time ACK is therefore too late: capability negotiation must
        succeed before Manager sends the mutating create request.
        """

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"Worker {worker.name} 无法确认 delegated principal 协议，"
                "任务未转发"
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("worker_delegated_principal_protocol")
            != WORKER_DELEGATED_PRINCIPAL_PROTOCOL
            or config.get("worker_delegated_launch_admission_protocol")
            != WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL
        ):
            raise RuntimeError(
                f"Worker {worker.name} 未声明 delegated principal 协议 v"
                f"{WORKER_DELEGATED_PRINCIPAL_PROTOCOL} 与 provider launch "
                f"admission 协议 v"
                f"{WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL}，任务未转发"
            )
        try:
            validate_worker_task_id_namespace_config(config)
        except TaskIdNamespaceProtocolError as exc:
            raise RuntimeError(
                f"Worker {worker.name} 未声明 Task ID namespace 协议 v"
                f"{TASK_ID_NAMESPACE_PROTOCOL}，任务未转发"
            ) from exc

    async def require_worker_initial_generation_support(
        self,
        worker: Worker,
    ) -> None:
        """Reject Workers that cannot seed retry/turn before first dequeue."""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"Worker {worker.name} 无法确认 initial generation 协议，"
                "任务未转发"
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("worker_initial_generation_protocol")
            != WORKER_INITIAL_GENERATION_PROTOCOL
        ):
            raise RuntimeError(
                f"Worker {worker.name} 未声明 initial generation 协议 v"
                f"{WORKER_INITIAL_GENERATION_PROTOCOL}，任务未转发"
            )

    async def require_worker_task_incarnation_support(
        self,
        worker: Worker,
    ) -> None:
        """Preflight exact Task-id ABA fencing before any handoff effect."""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"Worker {worker.name} 无法确认 Task incarnation 协议，"
                "任务未转发"
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("worker_task_incarnation_proxy_version") != 1
        ):
            raise RuntimeError(
                f"Worker {worker.name} 未声明 Task incarnation 协议 v1，"
                "任务未转发"
            )

    async def require_worker_migration_import_support(
        self,
        worker: Worker,
    ) -> None:
        """Preflight exact prepare/commit/rollback before remote import."""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"Worker {worker.name} 无法确认 migration import 协议"
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("worker_migration_import_protocol")
            != WORKER_MIGRATION_IMPORT_PROTOCOL
        ):
            raise RuntimeError(
                f"Worker {worker.name} 未声明 migration import 协议 v"
                f"{WORKER_MIGRATION_IMPORT_PROTOCOL}，任务未迁移"
            )

    async def require_worker_manual_retry_support(
        self,
        worker: Worker,
    ) -> None:
        """Reject legacy Workers before a retry outbox or mutation is staged."""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"Worker {worker.name} 无法确认 manual retry 协议"
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("worker_manual_retry_protocol")
            != WORKER_MANUAL_RETRY_PROTOCOL
            or config.get("worker_delegated_principal_protocol")
            != WORKER_DELEGATED_PRINCIPAL_PROTOCOL
        ):
            raise RuntimeError(
                f"Worker {worker.name} 未声明 manual retry 协议 v"
                f"{WORKER_MANUAL_RETRY_PROTOCOL}"
            )

    async def require_terminal_pr_review_chat_support(
        self,
        worker: Worker,
    ) -> None:
        """Reject mixed-version PR follow-ups before Manager-side logging."""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except Exception as exc:
            raise HTTPException(
                503,
                f"无法确认 Worker {worker.name} 的 PR 审核续聊能力",
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("pr_review_terminal_chat_version")
            != PR_REVIEW_TERMINAL_CHAT_VERSION
        ):
            raise HTTPException(
                409,
                f"Worker {worker.name} 版本过旧，升级后才能继续 PR 审核对话",
            )

    async def forward_task_to_worker(
        self,
        task: Task,
        *,
        operation_lock_held: bool = False,
    ):
        if operation_lock_held:
            current = await self._authoritative_forward_task(task)
            return await self._forward_task_to_worker_locked(current)
        async with self.task_operation_lock(task.id):
            current = await self._authoritative_forward_task(task)
            return await self._forward_task_to_worker_locked(current)

    async def _authoritative_forward_task(self, task: Task) -> Task:
        """Fence one claimed generation and its native Manager principal.

        A Task may wait for Worker capacity after its HTTP admission.  Recheck
        the durable User row and exact role while holding the same writer fence
        that precedes the remote POST, so a disabled or demoted administrator
        cannot retain stale unrestricted authority on the Worker.
        """

        if self.db_factory is None:
            return task
        expected = worker_task_generation(task)
        if expected is None:
            raise RuntimeError(
                "Task is no longer assigned to a Worker before forwarding"
            )
        async with self.db_factory() as db:
            # This is the portable Task-side writer fence shared with
            # termination admission. ``forward_task_to_worker`` holds the
            # per-Task operation lock across this check and every following
            # Worker effect, so a receipt either wins first and blocks the
            # POST, or waits until this exact forwarding attempt settles.
            admitted = await db.execute(
                update(Task)
                .where(
                    Task.id == task.id,
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
            )
            if admitted.rowcount != 1:
                current = await db.get(Task, task.id, populate_existing=True)
                current_generation = (
                    worker_task_generation(current)
                    if current is not None
                    else None
                )
                await db.rollback()
                if current_generation == expected:
                    raise WorkerTaskForwardAdmissionBlockedError(
                        "Task termination owns the claimed Worker generation"
                    )
                raise RuntimeError(
                    "Task Worker generation changed before initial forwarding"
                )
            current = (
                await db.execute(
                    select(Task)
                    .where(
                        Task.id == task.id,
                        no_active_worker_task_termination_predicate(),
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if current is None or worker_task_generation(current) != expected:
                await db.rollback()
                raise RuntimeError(
                    "Task Worker generation changed before initial forwarding"
                )
            from backend.models.user import User
            from backend.services.task_creation import (
                TASK_EXECUTION_DELEGATED_PRINCIPAL_KINDS,
                task_execution_principal_values,
            )

            persisted_principal = {
                "execution_user_id": current.execution_user_id,
                "execution_user_role": current.execution_user_role,
                "execution_mode": current.execution_mode,
                "execution_principal_kind": current.execution_principal_kind,
            }
            try:
                canonical_principal = task_execution_principal_values(
                    user_id=current.execution_user_id,
                    role=current.execution_user_role,
                    principal_kind=current.execution_principal_kind,
                )
            except (TypeError, ValueError) as exc:
                await db.rollback()
                raise RuntimeError(
                    "Task has an invalid execution principal before Worker "
                    "forwarding"
                ) from exc
            if persisted_principal != canonical_principal:
                await db.rollback()
                raise RuntimeError(
                    "Task execution principal changed before Worker forwarding"
                )
            if (
                current.execution_principal_kind
                in TASK_EXECUTION_DELEGATED_PRINCIPAL_KINDS
            ):
                await db.rollback()
                raise RuntimeError(
                    "Manager Task cannot carry an already-delegated principal"
                )
            if current.execution_principal_kind == "user":
                # A plain SELECT leaves a TOCTOU window: an administrator can
                # disable/demote this User after the read but before this
                # transaction commits the forwarding admission.  The exact
                # no-op UPDATE is the portable User writer fence (including
                # SQLite), ordered after the Task fence like every other Task
                # effect.  Whichever transaction wins therefore determines
                # whether this native principal may be delegated.
                principal_fence = await db.execute(
                    update(User)
                    .where(
                        User.id == current.execution_user_id,
                        User.is_active.is_(True),
                        User.role == current.execution_user_role,
                    )
                    .values(role=User.role)
                    .execution_options(synchronize_session=False)
                )
                if principal_fence.rowcount != 1:
                    await db.rollback()
                    raise RuntimeError(
                        "Task initiator role changed or is no longer active "
                        "before Worker forwarding"
                    )
            await db.commit()
            return current

    async def _forward_task_to_worker_locked(self, task: Task):
        validate_manager_allocated_task_id(task.id)
        if (
            task.status != "in_progress"
            or type(task.retry_count) is not int
            or task.retry_count < 0
            or type(task.turn_generation) is not int
            or task.turn_generation < 1
        ):
            raise RuntimeError(
                "Initial Worker forwarding requires an exact claimed "
                "retry/turn generation"
            )
        worker = await self.get_worker(task.worker_id)
        if (
            not worker
            or worker.status != "ready"
            or worker.bootstrap_step is not None
        ):
            raise RuntimeError(
                f"Worker {worker.name if worker else task.worker_id} 不可用"
                "（"
                f"{worker.status if worker else 'not found'}/"
                f"{worker.bootstrap_step if worker and worker.bootstrap_step else 'normal'}"
                "）"
            )
        self._require_authenticated_control_plane(worker)

        await self.require_worker_delegated_principal_support(worker)
        await self.require_worker_initial_generation_support(worker)
        await self.require_worker_task_incarnation_support(worker)
        await self.require_worker_fast_support(worker, task)
        # PR reviews use only the remote GitHub snapshot named in their prompt.
        # Mapping the Manager's synthetic PR-Monitor project would either fail
        # (it has no repository) or make the Worker load unrelated local agent
        # docs.  Tags survive Manager→Worker forwarding, unlike metadata.
        worker_project_id = (
            None
            if is_pr_sandbox_task(task)
            else await self.ensure_worker_project(worker, task)
        )

        metadata = task.metadata_ or {}
        # Related-Plan uploads are validated and marked by the Manager API.
        # Do not copy arbitrary legacy metadata paths to another machine.
        has_related_plan_uploads = (
            task.mode == "plan"
            and task.plan_target_task_id is not None
            and metadata.get("created_from_plan_target_task_id")
            == task.plan_target_task_id
        )
        attachment_paths = (
            metadata.get("file_paths") or metadata.get("image_paths") or []
            if has_related_plan_uploads
            else []
        )
        attachment_records = (
            metadata.get("attachments") or []
            if has_related_plan_uploads
            else []
        )
        remote_attachment_paths = worker_managed_upload_paths(attachment_paths)
        if attachment_paths:
            await self.push_files(
                worker,
                attachment_paths,
                remote_paths=remote_attachment_paths,
            )
        image_paths = [
            remote_path
            for index, remote_path in enumerate(remote_attachment_paths)
            if (
                index < len(attachment_records)
                and isinstance(attachment_records[index], dict)
                and attachment_records[index].get("is_image") is True
            )
        ]

        # 先订阅 relay 再创建：worker Dispatcher 可能创建后立即执行，后订阅丢初始事件。
        # The authentication gate above must precede this WebSocket effect.
        await self.relay.subscribe_task(worker, task.id)
        user_skill_snapshots = await self._user_skill_snapshots(task)
        delegated_principal = delegated_task_execution_principal_values(
            user_id=task.execution_user_id,
            role=task.execution_user_role,
            principal_kind=task.execution_principal_kind,
        )

        payload = {
            "id": task.id,  # 关键：Manager 分配的全局 ID
            "source_incarnation_id": task.incarnation_id,
            "source_retry_count": task.retry_count,
            "source_turn_generation": task.turn_generation,
            "title": task.title,
            "description": task.description or "",
            "project_id": worker_project_id,
            "target_branch": task.target_branch or "main",
            "priority": task.priority,
            "max_retries": task.max_retries,
            "mode": task.mode,
            "todo_file_path": task.todo_file_path,
            "max_iterations": task.max_iterations,
            "must_complete": task.must_complete,
            "goal_condition": task.goal_condition,
            "goal_max_turns": task.goal_max_turns,
            "goal_evaluator_model": task.goal_evaluator_model,
            "plan_target_task_id": task.plan_target_task_id,
            "plan_context_session_id": task.plan_context_session_id,
            "plan_context_log_id": task.plan_context_log_id,
            "plan_context_snapshot": task.plan_context_snapshot,
            "plan_repo_revision": task.plan_repo_revision,
            "supersedes_plan_task_id": task.supersedes_plan_task_id,
            "plan_pipeline_config": task.plan_pipeline_config,
            "provider": task.provider,
            "model": task.model,
            "codex_service_tier": task.codex_service_tier,
            "effort_level": task.effort_level,
            "thinking_budget": task.thinking_budget,
            "timeout_hours": task.timeout_hours,
            "enable_workflows": task.enable_workflows,
            "enabled_skills": task.enabled_skills,
            "selected_user_skills": task.selected_user_skills,
            "user_skill_snapshots": user_skill_snapshots,
            "tags": list(task.tags) if task.tags else None,
            "file_paths": remote_attachment_paths or None,
            "image_paths": image_paths or None,
            "attachments": attachment_records or None,
            "attention_tag": task.attention_tag,
            **delegated_principal,
        }
        headers = self._headers(worker)
        post_started = False
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                post_started = True
                r = await c.post(
                    self._api(worker, "/api/tasks"),
                    headers=headers,
                    json=payload,
                )
                # Once POST has started, even an HTTP error or malformed ACK
                # cannot prove that the Worker did not commit and wake its
                # dispatcher.  Surface a distinct uncertainty contract so the
                # Manager never blindly resends the create request.
                r.raise_for_status()
                try:
                    created = r.json()
                except Exception as exc:
                    raise RuntimeError(
                        f"Worker {worker.name} 未返回有效 Task ACK"
                    ) from exc
                if not isinstance(created, dict) or created.get("id") != task.id:
                    raise RuntimeError(
                        f"Worker {worker.name} 未确认 exact Task identity"
                    )
                created_metadata = created.get("metadata_")
                if (
                    not isinstance(created_metadata, dict)
                    or created_metadata.get(SOURCE_TASK_INCARNATION_METADATA_KEY)
                    != task.incarnation_id
                ):
                    raise RuntimeError(
                        f"Worker {worker.name} 未确认 exact Task incarnation"
                    )
                if created.get("incarnation_id") != task.incarnation_id:
                    raise RuntimeError(
                        f"Worker {worker.name} 未确认 exact Task incarnation"
                    )
                acknowledged_principal = {
                    field: created.get(field)
                    for field in delegated_principal
                }
                if acknowledged_principal != delegated_principal:
                    raise RuntimeError(
                        f"Worker {worker.name} 未确认 exact delegated principal"
                    )
                if (
                    created.get("status") != "pending"
                    or created.get("retry_count") != task.retry_count
                    or created.get("turn_generation")
                    != task.turn_generation - 1
                ):
                    raise RuntimeError(
                        f"Worker {worker.name} 未确认 exact initial generation"
                    )
                if (
                    (task.codex_service_tier or "default") == "priority"
                    and created.get("codex_service_tier") != "priority"
                ):
                    raise RuntimeError(
                        f"Worker {worker.name} 未确认 Codex Fast 任务配置"
                    )
        except asyncio.CancelledError as exc:
            if not post_started:
                raise
            raise WorkerTaskForwardOutcomeUncertainError(
                f"Worker {worker.name} initial Task POST was cancelled after "
                "its outcome became uncertain",
                cancellation=exc,
            ) from exc
        except Exception as exc:
            if not post_started:
                raise
            raise WorkerTaskForwardOutcomeUncertainError(
                f"Worker {worker.name} initial Task POST outcome is uncertain: {exc}"
            ) from exc
        observed = worker_task_generation(task, expected_worker_id=worker.id)
        if observed is None:
            raise WorkerTaskForwardOutcomeUncertainError(
                "Manager Worker generation disappeared after initial Task ACK"
            )
        if self.db_factory is not None:
            async with self.db_factory() as db:
                materialized = await mark_worker_task_materialized(db, observed)
            if not materialized:
                raise WorkerTaskForwardOutcomeUncertainError(
                    "Worker created the Task but Manager could not persist the "
                    "remote-materialized fence"
                )
        logger.info("task %s forwarded to worker %s", task.id, worker.id)

    async def _user_skill_snapshots(self, task: Task) -> list[dict]:
        from backend.services.skill_context import (
            build_user_skill_snapshot_payload,
            normalize_user_skill_ids,
        )

        if not normalize_user_skill_ids(task.selected_user_skills):
            return []
        async with self.db_factory() as db:
            return await build_user_skill_snapshot_payload(
                db,
                task.selected_user_skills,
                metadata=task.metadata_,
            )

    async def sync_task_skill_selection(
        self,
        worker: Worker,
        task: Task,
    ) -> None:
        """Refresh and confirm a remote task's Skills before a new turn."""

        from backend.services.command_registry import ensure_default_skills
        from backend.services.skill_context import (
            USER_SKILL_SNAPSHOTS_METADATA_KEY,
            normalize_user_skill_ids,
        )

        incarnation_id = _exact_task_incarnation(task)
        try:
            await self.require_worker_task_incarnation_support(worker)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise HTTPException(
                409,
                "Worker must be upgraded before Task Skill synchronization",
            ) from exc

        user_skill_snapshots = await self._user_skill_snapshots(task)
        payload = {
            "enabled_skills": ensure_default_skills(task.enabled_skills),
            "selected_user_skills": normalize_user_skill_ids(
                task.selected_user_skills
            ),
            "user_skill_snapshots": user_skill_snapshots,
        }
        headers = self._headers(worker)
        headers["X-CCM-Task-Incarnation"] = incarnation_id
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.put(
                self._api(worker, f"/api/tasks/{task.id}"),
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            try:
                confirmed = response.json()
            except Exception as exc:
                raise HTTPException(
                    502,
                    "Worker Skill selection synchronization returned an "
                    "invalid confirmation",
                ) from exc

        confirmed_metadata = (
            confirmed.get("metadata_")
            if isinstance(confirmed, dict)
            else None
        )
        confirmed_snapshots = (
            confirmed_metadata.get(USER_SKILL_SNAPSHOTS_METADATA_KEY)
            if isinstance(confirmed_metadata, dict)
            else None
        )
        # ``instance_id`` is node-local execution ownership. A task migrated
        # from Manager to Worker deliberately keeps the old Manager instance
        # id while the imported Worker row has no corresponding local
        # instance. The globally assigned task id plus the monotonic retry
        # generation and coordinated inert status identify the remote copy;
        # comparing unrelated database ids would reject every such migration.
        if (
            not isinstance(confirmed, dict)
            or confirmed.get("id") != task.id
            or confirmed.get("incarnation_id") != incarnation_id
            or confirmed.get("status") != task.status
            or confirmed.get("retry_count") != task.retry_count
            or confirmed.get("turn_generation") != task.turn_generation
            or confirmed.get("enabled_skills") != payload["enabled_skills"]
            or confirmed.get("selected_user_skills")
            != payload["selected_user_skills"]
            or confirmed_snapshots != user_skill_snapshots
        ):
            raise HTTPException(
                409,
                "Worker Skill selection does not exactly match the Manager; "
                "execution was blocked",
            )

    async def push_files(
        self,
        worker: Worker,
        paths: list[str],
        *,
        remote_paths: list[str] | None = None,
    ):
        """Copy managed uploads into the Worker's own upload namespace."""

        expected_targets = worker_managed_upload_paths(paths)
        targets = expected_targets if remote_paths is None else remote_paths
        if len(targets) != len(paths):
            raise ValueError("Remote attachment paths must match local paths")
        if targets != expected_targets:
            raise ValueError("Remote attachments must stay in the Worker upload root")
        # Persistent Plan/Task metadata can outlive the request that first
        # validated it. Re-prove the source at the actual cross-host effect
        # boundary so a stale/tampered row cannot turn rsync into an arbitrary
        # Manager-file read. This also refreshes the upload cleanup TTL while
        # holding the upload store's lock.
        from backend.api.uploads import (
            UploadAttachmentValidationError,
            validate_upload_attachments,
        )

        try:
            validated = validate_upload_attachments(file_paths=paths)
        except UploadAttachmentValidationError as exc:
            raise RuntimeError(
                "Worker attachment source is no longer a managed upload"
            ) from exc
        if [upload.path for upload in validated] != paths:
            raise RuntimeError("Worker attachment source path is not canonical")
        ssh = self._ssh(worker)
        for path, remote_path in zip(paths, targets, strict=True):
            await ssh.copy_file(path, remote_path)

    async def require_task_artifact_scope_support(
        self,
        worker: Worker,
    ) -> None:
        """Fail closed when a Worker cannot enforce the managed namespace."""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except Exception as exc:
            raise HTTPException(
                503,
                f"无法确认 Worker {worker.name} 的 Task 产物隔离能力",
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("task_artifact_scope_version")
            != TASK_ARTIFACT_SCOPE_VERSION
        ):
            raise HTTPException(
                409,
                f"Worker {worker.name} 版本过旧，升级后才能下载 Task 产物",
            )

    async def stream_task_artifact(
        self,
        task: Task,
        artifact_path: str,
    ) -> StreamingResponse:
        """Stream a task-scoped file from its Worker without buffering it."""

        worker = await self.require_ready_worker(task.worker_id)
        await self.require_task_artifact_scope_support(worker)
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=None, write=30, pool=10),
        )
        try:
            request = client.build_request(
                "GET",
                self._api(
                    worker,
                    f"/api/tasks/{task.id}/artifacts/download",
                ),
                headers=self._headers(worker),
                params={"path": artifact_path},
            )
            response = await client.send(request, stream=True)
        except (httpx.TimeoutException, TimeoutError) as exc:
            await client.aclose()
            raise HTTPException(
                503,
                f"Worker {worker.name} artifact request timed out",
            ) from exc
        except (httpx.RequestError, OSError) as exc:
            await client.aclose()
            raise HTTPException(
                502,
                f"Unable to reach Worker {worker.name}",
            ) from exc

        if not 200 <= response.status_code < 300:
            try:
                payload = await response.aread()
            finally:
                await response.aclose()
                await client.aclose()
            if response.status_code == 401:
                raise HTTPException(
                    502,
                    f"Worker {worker.name} rejected its internal credential",
                )
            status_code = (
                response.status_code
                if response.status_code in {400, 403, 404, 413}
                else 502
            )
            detail = "Worker artifact download failed"
            try:
                decoded = response.json()
                if isinstance(decoded, dict) and isinstance(decoded.get("detail"), str):
                    detail = decoded["detail"]
            except Exception:
                if payload:
                    detail = payload[:300].decode(errors="replace")
            raise HTTPException(status_code, detail)

        forwarded_headers = {}
        for header in ("content-disposition", "content-length", "content-type"):
            value = response.headers.get(header)
            if value:
                forwarded_headers[header] = value

        async def close_upstream() -> None:
            await response.aclose()
            await client.aclose()

        async def body():
            try:
                async for chunk in response.aiter_bytes():
                    yield chunk
            finally:
                await close_upstream()

        return StreamingResponse(
            body(),
            status_code=response.status_code,
            headers=forwarded_headers,
            background=BackgroundTask(close_upstream),
        )

    # ------------------------------------------------------------------
    # 通用操作代理（设计 §6.4）
    # ------------------------------------------------------------------

    async def proxy_to_worker(
        self,
        task: Task,
        method: str,
        path: str,
        body=None,
        *,
        require_json: bool = False,
        allow_task_absent: bool = False,
        surface_endpoint_not_found: bool = False,
        operation_lock_held: bool = False,
        pr_review_terminal_chat: bool = False,
        quarantine_on_transport_uncertainty: bool = False,
        require_task_incarnation_fence: bool = False,
    ):
        if pr_review_terminal_chat and not is_pr_review_task(task):
            raise ValueError(
                "Terminal PR review chat authorization requires a PR review Task"
            )
        if operation_lock_held:
            return await self._proxy_to_worker_locked(
                task,
                method,
                path,
                body,
                require_json=require_json,
                allow_task_absent=allow_task_absent,
                surface_endpoint_not_found=surface_endpoint_not_found,
                pr_review_terminal_chat=pr_review_terminal_chat,
                quarantine_on_transport_uncertainty=(
                    quarantine_on_transport_uncertainty
                ),
                require_task_incarnation_fence=require_task_incarnation_fence,
            )
        async with self.task_operation_lock(task.id):
            return await self._proxy_to_worker_locked(
                task,
                method,
                path,
                body,
                require_json=require_json,
                allow_task_absent=allow_task_absent,
                surface_endpoint_not_found=surface_endpoint_not_found,
                pr_review_terminal_chat=pr_review_terminal_chat,
                quarantine_on_transport_uncertainty=(
                    quarantine_on_transport_uncertainty
                ),
                require_task_incarnation_fence=require_task_incarnation_fence,
            )

    async def _proxy_to_worker_locked(
        self,
        task: Task,
        method: str,
        path: str,
        body=None,
        *,
        require_json: bool,
        allow_task_absent: bool,
        surface_endpoint_not_found: bool,
        pr_review_terminal_chat: bool,
        quarantine_on_transport_uncertainty: bool,
        require_task_incarnation_fence: bool,
    ):
        if self.db_factory is not None:
            incarnation_id = _exact_task_incarnation(task)
            async with self.db_factory() as db:
                current = await db.scalar(
                    select(Task).where(
                        Task.id == task.id,
                        Task.incarnation_id == incarnation_id,
                        Task.worker_id == task.worker_id,
                    )
                )
            if current is None:
                raise HTTPException(
                    409,
                    "Manager Task incarnation or Worker assignment changed",
                )
            task = current
        if worker_plan_decision_is_prepared(task.metadata_):
            marker = worker_plan_decision_gate_receipt(task.metadata_)
            request = marker.get("request") if isinstance(marker, dict) else None
            operation_id = (
                marker.get("operation_id") if isinstance(marker, dict) else None
            )
            request_digest = (
                marker.get("request_digest") if isinstance(marker, dict) else None
            )
            receipt_path = (
                f"/api/tasks/{task.id}/internal/worker-plan-decisions/"
                f"{operation_id}"
            )
            routing = request.get("routing") if isinstance(request, dict) else None
            routing_status = f"/api/tasks/{task.id}/routing-config/status"
            routing_mutation = bool(
                method == "POST"
                and path
                in {
                    f"/api/tasks/{task.id}/routing-config/ack",
                    f"/api/tasks/{task.id}/routing-config/reconcile",
                }
                and isinstance(body, dict)
                and isinstance(routing, dict)
                and body.get("provider") == routing.get("provider")
                and body.get("model") == routing.get("model")
                and body.get("codex_service_tier")
                == routing.get("codex_service_tier")
                and isinstance(body.get("op_id"), str)
                and bool(body.get("op_id"))
            )
            exact_request = bool(
                isinstance(request, dict)
                and isinstance(operation_id, str)
                and isinstance(request_digest, str)
                and worker_plan_decision_request_matches(
                    request,
                    operation_id=operation_id,
                    request_digest=request_digest,
                )
            )
            allowed_plan_decision_request = bool(
                exact_request
                and (
                    (
                        method == "GET"
                        and body is None
                        and path
                        in {
                            receipt_path,
                            routing_status,
                            f"/api/tasks/{task.id}",
                        }
                    )
                    or (
                        method == "PUT"
                        and path == receipt_path
                        and body == request
                    )
                    or routing_mutation
                )
            )
            if not allowed_plan_decision_request:
                raise HTTPException(
                    409,
                    "Task has a prepared Worker Plan decision awaiting exact "
                    "receipt reconciliation",
                )
        if worker_manual_retry_is_prepared(task.metadata_):
            retry_receipt = worker_manual_retry_receipt(task.metadata_)
            operation_id = (
                retry_receipt.get("operation_id")
                if retry_receipt is not None
                else None
            )
            exact_retry_post = bool(
                method == "POST"
                and path == f"/api/tasks/{task.id}/internal/worker-retry"
                and isinstance(body, dict)
                and body.get("operation_id") == operation_id
                and body.get("request_digest")
                == retry_receipt.get("request_digest")
            )
            exact_retry_readback = bool(
                method == "GET"
                and path
                == (
                    f"/api/tasks/{task.id}/internal/worker-retry-receipts/"
                    f"{operation_id}"
                )
                and body is None
            )
            if not (exact_retry_post or exact_retry_readback):
                raise HTTPException(
                    409,
                    "Task has a prepared Worker retry awaiting exact receipt "
                    "reconciliation",
                )
        # A durable Manager receipt owns every remote mutation until its exact
        # result is ACKed. The reconciliation loop itself traverses this common
        # proxy, so admit only that receipt's identity-bound GET/PUT/ACK paths;
        # all ordinary Plan/Monitor/Sub-Agent/config requests must wait.
        if self.db_factory is not None:
            async with self.db_factory() as db:
                active_receipt = await active_worker_task_termination_receipt(
                    db,
                    task.id,
                )
            if active_receipt is not None:
                if active_receipt.operation == "delete":
                    exact_receipt_request = bool(
                        active_receipt.side == "manager"
                        and active_receipt.worker_id == task.worker_id
                        and active_receipt.status == "conflict"
                        and body is None
                        and (
                            (
                                method == "DELETE"
                                and path == f"/api/tasks/{task.id}"
                            )
                            or (
                                method == "GET"
                                and path
                                == (
                                    f"/api/tasks/{task.id}/"
                                    "plan-delete-audit"
                                )
                            )
                        )
                    )
                else:
                    receipt_path = (
                        f"/api/tasks/{task.id}/termination-receipts/"
                        f"{active_receipt.operation_id}"
                    )
                    exact_receipt_request = bool(
                        active_receipt.side == "manager"
                        and active_receipt.worker_id == task.worker_id
                        and (
                            (
                                method == "GET"
                                and path == receipt_path
                                and body is None
                            )
                            or (
                                method == "PUT"
                                and path == receipt_path
                                and isinstance(body, dict)
                            )
                            or (
                                method == "POST"
                                and path == f"{receipt_path}/ack"
                                and isinstance(body, dict)
                            )
                        )
                    )
                if not exact_receipt_request:
                    raise HTTPException(
                        409,
                        "Task has an active Worker termination receipt",
                    )
        worker = await self.require_ready_worker(task.worker_id)
        if require_task_incarnation_fence:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    response = await client.get(
                        self._api(worker, "/api/system/config"),
                        headers=self._headers(worker),
                    )
                    response.raise_for_status()
                config = response.json()
            except Exception as exc:
                raise HTTPException(
                    503,
                    "Unable to confirm Worker Task-incarnation fencing",
                ) from exc
            if (
                not isinstance(config, dict)
                or config.get("worker_task_incarnation_proxy_version") != 1
            ):
                raise HTTPException(
                    409,
                    "Worker must be upgraded before Monitor/Sub-Agent mutation",
                )
        return await self._proxy_to_authorized_worker_locked(
            worker,
            task,
            method,
            path,
            body,
            require_json=require_json,
            allow_task_absent=allow_task_absent,
            surface_endpoint_not_found=surface_endpoint_not_found,
            pr_review_terminal_chat=pr_review_terminal_chat,
            quarantine_on_transport_uncertainty=(
                quarantine_on_transport_uncertainty
            ),
            task_incarnation_fenced=require_task_incarnation_fence,
        )

    async def _proxy_to_claimed_destroying_worker(
        self,
        task: Task,
        method: str,
        path: str,
        body=None,
        *,
        destroy_claim: WorkerDestroyLifecycleClaim,
        require_json: bool = False,
        allow_task_absent: bool = False,
        surface_endpoint_not_found: bool = False,
        operation_lock_held: bool = False,
        quarantine_on_transport_uncertainty: bool = False,
    ):
        """Proxy only for exact terminal reconciliation during Worker destroy."""

        if not operation_lock_held:
            raise ValueError(
                "claimed Worker destroy proxy requires the Task operation lock"
            )
        receipt_prefix = f"/api/tasks/{task.id}/termination-receipts/"
        receipt_suffix = path[len(receipt_prefix):] if path.startswith(receipt_prefix) else ""
        receipt_operation_id = (
            receipt_suffix[:-4] if receipt_suffix.endswith("/ack") else receipt_suffix
        )
        valid_receipt_id = bool(
            len(receipt_operation_id) == 32
            and all(char in "0123456789abcdef" for char in receipt_operation_id)
        )
        receipt_get = method == "GET" and valid_receipt_id and not receipt_suffix.endswith("/ack")
        receipt_put = method == "PUT" and valid_receipt_id and not receipt_suffix.endswith("/ack")
        receipt_ack = method == "POST" and valid_receipt_id and receipt_suffix.endswith("/ack")
        allowed_request = bool(
            (receipt_get and body is None)
            or ((receipt_put or receipt_ack) and isinstance(body, dict))
        )
        if (
            not allowed_request
            or require_json is not True
            or allow_task_absent
            or surface_endpoint_not_found
            or quarantine_on_transport_uncertainty
        ):
            raise ValueError(
                "Worker destroy claim authorizes only exact termination receipt "
                "GET/PUT/ACK requests"
            )
        if task.worker_id != destroy_claim.worker_id:
            raise HTTPException(
                409,
                "Task moved away from the claimed destroying Worker",
            )
        worker = await self._require_destroy_lifecycle_claim(destroy_claim)
        destroy_cleanup_headers = None
        if receipt_put:
            if (
                not isinstance(task.incarnation_id, str)
                or not task.incarnation_id
                or type(task.retry_count) is not int
                or type(task.turn_generation) is not int
            ):
                raise HTTPException(
                    409,
                    "Task lacks the exact generation required for Worker "
                    "destroy cleanup",
                )
            destroy_cleanup_headers = {
                WORKER_DESTROY_DRAIN_CLAIM_HEADER:
                    destroy_claim.node_drain_claim,
                WORKER_DESTROY_TASK_INCARCATION_HEADER:
                    task.incarnation_id,
                WORKER_DESTROY_TASK_RETRY_HEADER: str(task.retry_count),
                WORKER_DESTROY_TASK_TURN_HEADER:
                    str(task.turn_generation),
            }
        return await self._proxy_to_authorized_worker_locked(
            worker,
            task,
            method,
            path,
            body,
            require_json=require_json,
            allow_task_absent=allow_task_absent,
            surface_endpoint_not_found=surface_endpoint_not_found,
            pr_review_terminal_chat=False,
            quarantine_on_transport_uncertainty=(
                quarantine_on_transport_uncertainty
            ),
            task_incarnation_fenced=False,
            destroy_cleanup_headers=destroy_cleanup_headers,
        )

    async def _proxy_to_authorized_worker_locked(
        self,
        worker: Worker,
        task: Task,
        method: str,
        path: str,
        body=None,
        *,
        require_json: bool,
        allow_task_absent: bool,
        surface_endpoint_not_found: bool,
        pr_review_terminal_chat: bool,
        quarantine_on_transport_uncertainty: bool,
        task_incarnation_fenced: bool,
        destroy_cleanup_headers: dict[str, str] | None = None,
    ):
        self._require_authenticated_control_plane(worker)
        await self.relay.subscribe_task(worker, task.id)
        headers = self._headers(worker)
        if destroy_cleanup_headers is not None:
            if method != "PUT":
                raise ValueError(
                    "Worker destroy cleanup headers are valid only for PUT"
                )
            expected_header_names = {
                WORKER_DESTROY_DRAIN_CLAIM_HEADER,
                WORKER_DESTROY_TASK_INCARCATION_HEADER,
                WORKER_DESTROY_TASK_RETRY_HEADER,
                WORKER_DESTROY_TASK_TURN_HEADER,
            }
            if set(destroy_cleanup_headers) != expected_header_names:
                raise ValueError("Worker destroy cleanup headers are incomplete")
            headers.update(destroy_cleanup_headers)
        if task_incarnation_fenced:
            headers["X-CCM-Task-Incarnation"] = _exact_task_incarnation(task)
        if pr_review_terminal_chat:
            headers[PR_REVIEW_TERMINAL_CHAT_HEADER] = (
                PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE
            )
        request_started = False
        try:
            async with httpx.AsyncClient(timeout=60) as c:
                request_started = True
                r = await c.request(
                    method, self._api(worker, path),
                    headers=headers, json=body,
                )
        except asyncio.CancelledError as exc:
            if quarantine_on_transport_uncertainty and request_started:
                raise WorkerTaskMutationOutcomeUncertainError(
                    f"Worker {worker.name} request was cancelled after "
                    "the mutation boundary",
                    status_code=503,
                    cancellation=exc,
                ) from exc
            raise
        except (httpx.TimeoutException, TimeoutError) as exc:
            if quarantine_on_transport_uncertainty and request_started:
                raise WorkerTaskMutationOutcomeUncertainError(
                    f"Worker {worker.name} request timed out after the "
                    "mutation boundary",
                    status_code=503,
                ) from exc
            raise HTTPException(
                503,
                f"Worker {worker.name} 请求超时，请稍后重试",
            ) from exc
        except (httpx.RequestError, OSError) as exc:
            if quarantine_on_transport_uncertainty and request_started:
                raise WorkerTaskMutationOutcomeUncertainError(
                    f"Worker {worker.name} connection was lost after the "
                    "mutation boundary",
                    status_code=502,
                ) from exc
            raise HTTPException(
                502,
                f"Worker 网关连接失败，无法连接到 Worker {worker.name}",
            ) from exc

        # Worker token is an internal Manager→Worker credential.  Never
        # propagate a remote 401/403: doing so makes the frontend treat the
        # Manager login as expired.  Other upstream failures are gateway
        # errors too, and their response bodies may contain Worker internals.
        if r.status_code in (401, 403):
            raise HTTPException(
                502,
                f"内部 Worker 认证失败（远端 HTTP {r.status_code}），"
                "请重试 Worker 引导以同步认证凭据",
            )
        if surface_endpoint_not_found and r.status_code == 404:
            raise WorkerEndpointNotFoundError(path)
        if allow_task_absent and r.status_code == 404:
            try:
                missing = r.json()
            except Exception:
                missing = None
            if (
                isinstance(missing, dict)
                and missing.get("detail") == "Task not found"
            ):
                return {"ok": True, "already_deleted": True}
        if task_incarnation_fenced and r.status_code == 409:
            raise HTTPException(
                409,
                "Worker Task incarnation or lifecycle changed",
            )
        if not 200 <= r.status_code < 300:
            if quarantine_on_transport_uncertainty:
                raise WorkerTaskMutationOutcomeUncertainError(
                    f"Worker {worker.name} returned HTTP {r.status_code} "
                    "after the mutation boundary",
                    status_code=502,
                )
            raise HTTPException(
                502,
                f"Worker 上游请求失败（远端 HTTP {r.status_code}）",
            )
        try:
            return r.json()
        except Exception as exc:
            if require_json:
                if quarantine_on_transport_uncertainty:
                    raise WorkerTaskMutationOutcomeUncertainError(
                        f"Worker {worker.name} returned an unreadable "
                        "confirmation after the mutation boundary",
                        status_code=502,
                    ) from exc
                raise HTTPException(
                    502,
                    f"Worker {worker.name} returned an invalid confirmation",
                ) from exc
            return {"ok": True}
