"""Public contracts for the autonomous Delivery Loop mode."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DeliveryRunCreate(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=128)
    project_id: int = Field(gt=0)
    monitored_repo_id: int = Field(gt=0)
    title: str = Field(min_length=1, max_length=200)
    requirements: str = Field(min_length=1, max_length=200_000)
    source_todo_id: int | None = Field(default=None, gt=0)
    base_branch: str | None = Field(default=None, min_length=1, max_length=200)
    # Both local coding providers use a fail-closed Delivery isolation
    # profile.  Keep Codex as the wire-compatible default for older clients.
    provider: Literal["claude", "codex"] = "codex"
    model: str | None = Field(default=None, max_length=100)
    codex_service_tier: Literal["default", "priority"] = "default"
    effort_level: str | None = Field(default=None, max_length=20)
    timeout_hours: float | None = Field(default=None, ge=0, le=168)
    max_cycles: int = Field(default=10, ge=1, le=100)
    max_no_progress: int = Field(default=3, ge=1, le=20)
    # Trusted mode is the default for new Runs; historical Runs remain strict.
    strict_branch_protection: bool = False

    model_config = ConfigDict(extra="forbid")

    @field_validator(
        "idempotency_key",
        "title",
        "requirements",
        "base_branch",
        "model",
        "effort_level",
    )
    @classmethod
    def strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be blank")
        return normalized


class DeliveryRunResponse(BaseModel):
    id: int
    created_by: int | None
    project_id: int
    monitored_repo_id: int | None
    source_todo_id: int | None
    developer_task_id: int | None
    pr_monitor_run_id: int | None
    worktree_id: int | None
    title: str
    requirements: str
    requirements_hash: str
    policy_hash: str
    base_branch: str
    delivery_branch: str
    workspace_path: str | None
    base_sha: str | None
    head_sha: str | None
    head_tree_sha: str | None
    patch_sha256: str | None
    head_generation: int
    pr_number: int | None
    pr_url: str | None
    phase: str
    activity: str
    outcome: str | None
    # Frozen completion policy selected from the PR Monitor at admission.
    terminal: Literal["ready_to_merge", "merged"] | None = None
    wait_reason: str | None
    pause_reason: str | None
    error_code: str | None
    error_message: str | None
    state_version: int
    current_cycle_id: int | None
    cycle_count: int
    turn_count: int
    max_cycles: int
    no_progress_count: int
    max_no_progress: int
    next_reconcile_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    allowed_actions: list[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class DeliveryCycleResponse(BaseModel):
    id: int
    run_id: int
    cycle_number: int
    status: str
    state_version: int
    trigger_kind: str
    trigger_payload: dict
    base_sha: str | None
    start_head_sha: str | None
    result_head_sha: str | None
    result_head_tree_sha: str | None
    result_patch_sha256: str | None
    plan_invocation_id: int | None
    plan_version_id: int | None
    review_invocation_id: int | None
    review_result_id: int | None
    review_verdict: str | None
    review_summary: str | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class DeliveryTurnResponse(BaseModel):
    id: int
    run_id: int
    cycle_id: int
    generation: int
    correlation_id: str
    purpose: str
    trigger_kind: str
    trigger_payload: dict
    status: str
    task_id: int | None
    task_retry_count: int | None
    task_instance_id: int | None
    task_started_at: datetime | None
    task_session_id: str | None
    checkpoint: dict | None
    checkpoint_status: str | None
    attempts: int
    last_error: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class DeliveryTransitionResponse(BaseModel):
    id: int
    run_id: int
    state_version: int
    cause: str
    actor_kind: str
    actor_id: str | None
    before_state: dict
    after_state: dict
    metadata: dict | None = Field(default=None, validation_alias="metadata_")
    created_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class DeliveryRunDetail(DeliveryRunResponse):
    policy_snapshot: dict
    cycles: list[DeliveryCycleResponse]
    turns: list[DeliveryTurnResponse]
    transitions: list[DeliveryTransitionResponse]


class DeliveryCommand(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)

    model_config = ConfigDict(extra="forbid")

    @field_validator("reason")
    @classmethod
    def normalize_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("reason must not be blank")
        return normalized


class DeliveryResumeCommand(BaseModel):
    reason: str | None = Field(default=None, max_length=2000)

    model_config = ConfigDict(extra="forbid")
