"""Durable Manager -> Worker receipts for legacy Plan decisions.

Legacy Plan approval/rejection is a terminal, non-repeatable effect.  The
Manager therefore publishes one immutable operation envelope before touching
the Worker, while the Worker commits the same envelope as a receipt in the
transaction that publishes the terminal Task state.  A lost HTTP response can
then be reconciled with a read-only lookup instead of blindly replaying the
public decision endpoint.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
)


WORKER_PLAN_DECISION_PROTOCOL = 1
WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD = (
    "worker_plan_decision_receipt_v1"
)
WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY = (
    "ccm_worker_plan_decision_receipt_v1"
)
WORKER_PLAN_DECISION_ACTIONS = frozenset({"approve", "reject"})


def _canonical_payload(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def worker_plan_decision_request_identity(payload: Mapping[str, Any]) -> dict:
    """Return the immutable request fields covered by ``request_digest``."""

    if not isinstance(payload, Mapping):
        raise TypeError("Worker Plan decision payload must be an object")
    identity = dict(payload)
    identity.pop("request_digest", None)
    return identity


def worker_plan_decision_request_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        _canonical_payload(worker_plan_decision_request_identity(payload))
    ).hexdigest()


def worker_plan_decision_receipt_digest(receipt: Mapping[str, Any]) -> str:
    """Hash a complete Worker receipt for the Manager settlement audit."""

    if not isinstance(receipt, Mapping):
        raise TypeError("Worker Plan decision receipt must be an object")
    return hashlib.sha256(_canonical_payload(receipt)).hexdigest()


def worker_plan_decision_gate_receipt(metadata: object) -> dict | None:
    if not isinstance(metadata, Mapping):
        return None
    gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    if not isinstance(gate, Mapping):
        return None
    receipt = gate.get(WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD)
    return dict(receipt) if isinstance(receipt, Mapping) else None


def worker_plan_decision_is_prepared(metadata: object) -> bool:
    receipt = worker_plan_decision_gate_receipt(metadata)
    return bool(
        receipt is not None
        and receipt.get("protocol_version") == WORKER_PLAN_DECISION_PROTOCOL
        and receipt.get("side") == "manager"
        and receipt.get("state") == "prepared"
        and receipt.get("action") in WORKER_PLAN_DECISION_ACTIONS
    )


def worker_plan_decision_worker_receipt(metadata: object) -> dict | None:
    if not isinstance(metadata, Mapping):
        return None
    receipt = metadata.get(WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY)
    return dict(receipt) if isinstance(receipt, Mapping) else None


def worker_plan_decision_request_matches(
    request: object,
    *,
    operation_id: str,
    request_digest: str,
) -> bool:
    return bool(
        isinstance(request, Mapping)
        and request.get("protocol_version") == WORKER_PLAN_DECISION_PROTOCOL
        and request.get("operation_id") == operation_id
        and request.get("request_digest") == request_digest
        and worker_plan_decision_request_digest(request) == request_digest
    )


def worker_plan_decision_worker_receipt_matches(
    receipt: object,
    request: Mapping[str, Any],
) -> bool:
    """Validate an applied Worker receipt against the exact Manager request."""

    if not isinstance(receipt, Mapping):
        return False
    operation_id = request.get("operation_id")
    request_digest = request.get("request_digest")
    result = receipt.get("result_generation")
    return bool(
        receipt.get("protocol_version") == WORKER_PLAN_DECISION_PROTOCOL
        and receipt.get("side") == "worker"
        and receipt.get("state") == "applied"
        and receipt.get("operation_id") == operation_id
        and receipt.get("request_digest") == request_digest
        and receipt.get("action") == request.get("action")
        and receipt.get("request") == dict(request)
        and worker_plan_decision_request_matches(
            request,
            operation_id=operation_id,
            request_digest=request_digest,
        )
        and isinstance(result, Mapping)
        and result.get("task_id") == request.get("task_id")
        and result.get("incarnation_id")
        == request.get("source_incarnation_id")
        and result.get("retry_count") == request.get("expected_retry_count")
        and result.get("turn_generation")
        == request.get("expected_turn_generation")
        and result.get("status")
        == ("completed" if request.get("action") == "approve" else "cancelled")
        and isinstance(receipt.get("applied_at"), str)
        and bool(receipt.get("applied_at"))
    )


def worker_plan_decision_response_matches(
    payload: object,
    request: Mapping[str, Any],
) -> bool:
    """Validate a complete applied readback, including its Task projection."""

    if not isinstance(payload, Mapping):
        return False
    task = payload.get("task")
    receipt = payload.get("receipt")
    result = receipt.get("result_generation") if isinstance(receipt, Mapping) else None
    return bool(
        worker_plan_decision_worker_receipt_matches(receipt, request)
        and isinstance(task, Mapping)
        and isinstance(result, Mapping)
        and task.get("id") == result.get("task_id")
        and task.get("incarnation_id") == result.get("incarnation_id")
        and task.get("status") == result.get("status")
        and task.get("retry_count") == result.get("retry_count")
        and task.get("turn_generation") == result.get("turn_generation")
        and task.get("plan_approved")
        is (True if request.get("action") == "approve" else False)
    )


def worker_plan_decision_absent_response_matches(
    payload: object,
    request: Mapping[str, Any],
) -> bool:
    return bool(
        isinstance(payload, Mapping)
        and set(payload) == {
            "protocol_version",
            "state",
            "operation_id",
            "task_id",
        }
        and payload.get("protocol_version") == WORKER_PLAN_DECISION_PROTOCOL
        and payload.get("state") == "absent"
        and payload.get("operation_id") == request.get("operation_id")
        and payload.get("task_id") == request.get("task_id")
    )
