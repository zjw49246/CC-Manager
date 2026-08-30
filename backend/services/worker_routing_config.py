"""Crash-safe Worker routing-config synchronization primitives.

The pending marker intentionally lives in ``Task.metadata_`` so it is durable
without coupling the protocol to a database migration.  While the marker is
present, no main-task turn may start.  Stage records only the candidate tuple;
ack atomically promotes that tuple to the live Task fields and clears the
marker.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

WORKER_MIGRATION_IMPORT_PROTOCOL = 3
WORKER_ROUTING_PENDING_KEY = "worker_routing_config_pending"
WORKER_MIGRATION_IMPORT_RESERVATION_KEY = (
    "worker_migration_import_reservation"
)
WORKER_MIGRATION_IMPORT_COMMIT_RECEIPT_KEY = (
    "worker_migration_import_commit_receipt"
)
WORKER_ROUTING_SAFE_STATUSES = frozenset(
    {"plan_review", "completed", "failed", "cancelled", "conflict"}
)
WORKER_ROUTING_TIERS = frozenset({"default", "priority"})


@dataclass(frozen=True)
class WorkerRoutingTuple:
    provider: str
    model: str | None
    codex_service_tier: str

    def as_dict(self) -> dict[str, str | None]:
        return {
            "provider": self.provider,
            "model": self.model,
            "codex_service_tier": self.codex_service_tier,
        }


@dataclass(frozen=True)
class WorkerRoutingPending:
    op_id: str
    routing: WorkerRoutingTuple

    def as_dict(self) -> dict[str, Any]:
        return {
            "op_id": self.op_id,
            **self.routing.as_dict(),
        }


class InvalidWorkerRoutingMarker(ValueError):
    """The durable marker exists but cannot be interpreted safely."""


@dataclass(frozen=True)
class WorkerMigrationImportReservation:
    """Exact destination mirror owned by one not-yet-committed migration."""

    operation_id: str
    operation_sequence: int
    incarnation_id: str
    retry_count: int
    turn_generation: int
    source_status: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "operation_sequence": self.operation_sequence,
            "incarnation_id": self.incarnation_id,
            "retry_count": self.retry_count,
            "turn_generation": self.turn_generation,
            "source_status": self.source_status,
        }


def read_worker_migration_import_reservation(
    task_or_metadata: Any,
) -> WorkerMigrationImportReservation | None:
    metadata = (
        task_or_metadata.metadata_
        if hasattr(task_or_metadata, "metadata_")
        else task_or_metadata
    )
    if not isinstance(metadata, dict):
        return None
    raw = metadata.get(WORKER_MIGRATION_IMPORT_RESERVATION_KEY)
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) != {
        "operation_id",
        "operation_sequence",
        "incarnation_id",
        "retry_count",
        "turn_generation",
        "source_status",
    }:
        raise InvalidWorkerRoutingMarker(
            "Worker migration import reservation has an invalid shape"
        )
    operation_id = raw.get("operation_id")
    operation_sequence = raw.get("operation_sequence")
    incarnation_id = raw.get("incarnation_id")
    retry_count = raw.get("retry_count")
    turn_generation = raw.get("turn_generation")
    source_status = raw.get("source_status")
    if (
        not isinstance(operation_id, str)
        or len(operation_id) != 32
        or any(char not in "0123456789abcdef" for char in operation_id)
        or type(operation_sequence) is not int
        or operation_sequence <= 0
        or not isinstance(incarnation_id, str)
        or len(incarnation_id) != 32
        or any(char not in "0123456789abcdef" for char in incarnation_id)
        or type(retry_count) is not int
        or retry_count < 0
        or type(turn_generation) is not int
        or turn_generation < 0
        or source_status not in WORKER_ROUTING_SAFE_STATUSES
    ):
        raise InvalidWorkerRoutingMarker(
            "Worker migration import reservation contains invalid values"
        )
    return WorkerMigrationImportReservation(
        operation_id=operation_id,
        operation_sequence=operation_sequence,
        incarnation_id=incarnation_id,
        retry_count=retry_count,
        turn_generation=turn_generation,
        source_status=source_status,
    )


def with_worker_migration_import_reservation(
    metadata: dict | None,
    reservation: WorkerMigrationImportReservation,
) -> dict:
    updated = dict(metadata or {})
    updated.pop(WORKER_MIGRATION_IMPORT_COMMIT_RECEIPT_KEY, None)
    updated[WORKER_MIGRATION_IMPORT_RESERVATION_KEY] = reservation.as_dict()
    return updated


def read_worker_migration_import_commit_receipt(
    task_or_metadata: Any,
) -> WorkerMigrationImportReservation | None:
    metadata = (
        task_or_metadata.metadata_
        if hasattr(task_or_metadata, "metadata_")
        else task_or_metadata
    )
    if not isinstance(metadata, dict):
        return None
    raw = metadata.get(WORKER_MIGRATION_IMPORT_COMMIT_RECEIPT_KEY)
    if raw is None:
        return None
    # Reuse the exact reservation parser so both sides bind the same complete
    # operation/generation tuple.  The temporary wrapper never escapes.
    return read_worker_migration_import_reservation({
        WORKER_MIGRATION_IMPORT_RESERVATION_KEY: raw,
    })


def with_worker_migration_import_commit_receipt(
    metadata: dict | None,
    reservation: WorkerMigrationImportReservation,
) -> dict:
    """Make one imported mirror permanently non-rollbackable, idempotently."""

    updated = dict(metadata or {})
    updated.pop(WORKER_MIGRATION_IMPORT_RESERVATION_KEY, None)
    updated[WORKER_MIGRATION_IMPORT_COMMIT_RECEIPT_KEY] = reservation.as_dict()
    return updated


def task_routing_tuple(task: Any) -> WorkerRoutingTuple:
    return WorkerRoutingTuple(
        provider=task.provider,
        model=task.model,
        codex_service_tier=task.codex_service_tier,
    )


def has_pending_worker_routing(task_or_metadata: Any) -> bool:
    metadata = (
        task_or_metadata.metadata_
        if hasattr(task_or_metadata, "metadata_")
        else task_or_metadata
    )
    return (
        isinstance(metadata, dict)
        and WORKER_ROUTING_PENDING_KEY in metadata
    )


def read_pending_worker_routing(
    task_or_metadata: Any,
) -> WorkerRoutingPending | None:
    metadata = (
        task_or_metadata.metadata_
        if hasattr(task_or_metadata, "metadata_")
        else task_or_metadata
    )
    if not isinstance(metadata, dict):
        return None
    if WORKER_ROUTING_PENDING_KEY not in metadata:
        return None
    raw = metadata[WORKER_ROUTING_PENDING_KEY]
    if not isinstance(raw, dict) or set(raw) != {
        "op_id",
        "provider",
        "model",
        "codex_service_tier",
    }:
        raise InvalidWorkerRoutingMarker(
            "Worker routing marker has an invalid shape"
        )
    op_id = raw.get("op_id")
    provider = raw.get("provider")
    model = raw.get("model")
    tier = raw.get("codex_service_tier")
    if (
        not isinstance(op_id, str)
        or not op_id
        or len(op_id) > 128
        or not isinstance(provider, str)
        or provider not in {"claude", "codex"}
        or (model is not None and not isinstance(model, str))
        or tier not in WORKER_ROUTING_TIERS
    ):
        raise InvalidWorkerRoutingMarker(
            "Worker routing marker contains invalid values"
        )
    return WorkerRoutingPending(
        op_id=op_id,
        routing=WorkerRoutingTuple(
            provider=provider,
            model=model,
            codex_service_tier=tier,
        ),
    )


def with_pending_worker_routing(
    metadata: dict | None,
    pending: WorkerRoutingPending,
) -> dict:
    updated = dict(metadata or {})
    updated[WORKER_ROUTING_PENDING_KEY] = pending.as_dict()
    return updated


def without_pending_worker_routing(metadata: dict | None) -> dict | None:
    if not isinstance(metadata, dict):
        return metadata
    updated = dict(metadata)
    updated.pop(WORKER_ROUTING_PENDING_KEY, None)
    return updated or None
