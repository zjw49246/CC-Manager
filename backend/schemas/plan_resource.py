"""Canonical API contracts for first-class versioned Plans."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.schemas.plan import PlanPipelineConfig


class PlanQuestionOption(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: str = Field(min_length=1, max_length=200)
    label: str = Field(min_length=1, max_length=500)


class PlanQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    header: str = Field(min_length=1, max_length=20)
    question: str = Field(min_length=1, max_length=2_000)
    response_type: Literal["text", "single_choice", "multi_choice"]
    options: list[PlanQuestionOption] = Field(default_factory=list)
    required: bool = True

    @model_validator(mode="after")
    def validate_options(self):
        if self.response_type == "text":
            if self.options:
                raise ValueError("text questions cannot define options")
            return self
        if not 2 <= len(self.options) <= 5:
            raise ValueError("choice questions require 2 to 5 options")
        values = [option.value for option in self.options]
        if len(values) != len(set(values)):
            raise ValueError("choice option values must be unique")
        return self


class PlanCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input: str = Field(min_length=1, max_length=200_000)
    title: str | None = Field(default=None, max_length=200)
    target_task_id: int | None = None
    project_id: int | None = None
    target_repo: str | None = Field(default=None, max_length=500)
    target_branch: str | None = Field(default=None, max_length=200)
    worker_id: int | None = None
    priority: int = 0
    timeout_hours: float | None = Field(default=None, ge=0)
    file_paths: list[str] | None = None
    image_paths: list[str] | None = None
    attachments: list[dict] | None = None

    @field_validator("input")
    @classmethod
    def require_nonblank_input(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Plan input cannot be blank")
        return value


class PlanPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(default=None, min_length=1, max_length=200)
    archived: bool | None = None
    expected_lock_version: int

    @field_validator("title")
    @classmethod
    def require_nonblank_title(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("Plan title cannot be blank")
        return value


class PlanRunCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_type: Literal["user_revision", "refresh_context", "retry"]
    request: str = Field(min_length=1, max_length=50_000)
    base_version_id: int | None = None
    expected_current_version_id: int | None = None
    source_run_id: int | None = Field(default=None, gt=0)
    file_paths: list[str] | None = None
    image_paths: list[str] | None = None
    attachments: list[dict] | None = None

    @field_validator("request")
    @classmethod
    def require_nonblank_request(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Plan Run request cannot be blank")
        return value


class PlanForkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_version_id: int
    title: str | None = Field(default=None, min_length=1, max_length=200)
    request: str | None = Field(default=None, max_length=50_000)

    @field_validator("request")
    @classmethod
    def normalize_optional_request(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value if value.strip() else None


class PlanDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_current_version_id: int
    confirm_stale: bool = False


class PlanExecutionCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_current_version_id: int
    confirm_stale: bool = False
    approve_if_pending: bool = False


class PlanInputAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question_id: str = Field(min_length=1, max_length=100)
    value: str | list[str] | None


class PlanInputAnswerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_run_generation: int
    idempotency_key: str = Field(min_length=1, max_length=200)
    answers: list[PlanInputAnswer]
    response_text: str | None = Field(default=None, max_length=50_000)
    file_paths: list[str] | None = None
    image_paths: list[str] | None = None
    attachments: list[dict] | None = None
    attachment_manifest: list[dict] | None = None


class WorkerPlanVersionSeed(BaseModel):
    """Immutable Version content needed to rehydrate a Worker mirror."""

    model_config = ConfigDict(extra="forbid")

    source_version_id: int = Field(gt=0)
    version_number: int = Field(gt=0)
    content: str = Field(min_length=1)
    context_session_id: str | None = Field(default=None, max_length=200)
    context_log_id: int | None = None
    context_snapshot: str | None = None
    repo_revision: dict | None = None
    reviewer_repo_revision: dict | None = None
    review_verdict: str | None = Field(default=None, max_length=20)
    review_feedback: str | None = None
    review_exhausted: bool = False
    reviewed_at: datetime | None = None
    human_decision: Literal["pending", "approved", "rejected"] = "pending"


class WorkerPlanRunImportRequest(BaseModel):
    """Internal Manager→Worker mirror protocol; never accepted from user JWTs."""

    model_config = ConfigDict(extra="forbid")

    protocol: Literal[3]
    plan_id: int = Field(gt=0)
    run_id: int = Field(gt=0)
    manager_claim_generation: int = Field(ge=0)
    title: str = Field(min_length=1, max_length=200)
    initial_request: str = Field(min_length=1, max_length=200_000)
    target_task_id: int | None = None
    project_id: int | None = None
    target_branch: str | None = Field(default=None, max_length=200)
    priority: int = 0
    timeout_hours: float | None = Field(default=None, ge=0)
    pipeline_config: PlanPipelineConfig
    run_type: str = Field(min_length=1, max_length=30)
    source_run_id: int | None = Field(default=None, gt=0)
    request_text: str = Field(min_length=1, max_length=200_000)
    context_session_id: str | None = Field(default=None, max_length=200)
    context_log_id: int | None = None
    context_snapshot: str | None = None
    repo_revision: dict | None = None
    max_interactions: int = Field(ge=0, le=5)
    base_version: WorkerPlanVersionSeed | None = None
    file_paths: list[str] | None = None
    image_paths: list[str] | None = None
    attachments: list[dict] | None = None
    attachment_manifest: list[dict] | None = None


class WorkerPlanRunCancelRequest(BaseModel):
    """Internal exact cancellation for one immutable Manager import."""

    model_config = ConfigDict(extra="forbid")

    protocol: Literal[1]
    plan_id: int = Field(gt=0)
    payload_digest: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )


class WorkerPlanVersionImportRequest(BaseModel):
    """Internal request to materialize an immutable Version near a Task."""

    model_config = ConfigDict(extra="forbid")

    protocol: Literal[3]
    plan_id: int = Field(gt=0)
    title: str = Field(min_length=1, max_length=200)
    initial_request: str = Field(min_length=1, max_length=200_000)
    target_task_id: int | None = None
    project_id: int | None = None
    target_branch: str | None = Field(default=None, max_length=200)
    priority: int = 0
    timeout_hours: float | None = Field(default=None, ge=0)
    pipeline_config: PlanPipelineConfig
    version: WorkerPlanVersionSeed


class PlanInputRequestResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    plan_id: int
    run_id: int
    source_step_id: int
    requested_by: str
    reason: str | None
    questions: list[PlanQuestion]
    status: str
    answers: list[dict] | None
    response_text: str | None
    attachments: list[dict] | None
    answered_by: int | None
    opened_at: datetime | None
    answered_at: datetime | None
    created_at: datetime


class PlanStepResource(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: int
    plan_id: int | None
    plan_version_id: int | None
    input_request_id: int | None
    step_type: str
    round: int
    generation: int
    provider: str
    model: str | None
    effort: str | None
    route_slot: str | None
    status: str
    output: str | None
    error: str | None
    last_delta_at: datetime | None = None
    streamed_output_chars: int = 0
    last_event_type: str | None = None
    started_at: datetime
    finished_at: datetime | None


class PlanRunResource(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    plan_id: int | None
    run_type: str
    source_run_id: int | None = None
    status: str
    current_stage: str
    base_version_id: int | None
    result_version_id: int | None
    draft_content: str | None = None
    draft_step_id: int | None = None
    draft_repo_revision: dict | None = None
    request_text: str | None
    round: int
    generation: int
    instance_id: int | None
    worker_id: int | None
    open_input_request_id: int | None
    interaction_count: int
    max_interactions: int
    execution_seconds: float
    last_execution_started_at: datetime | None
    review_verdict: str | None
    review_feedback: str | None
    review_exhausted: bool
    error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None
    steps: list[PlanStepResource] = Field(default_factory=list)
    input_requests: list[PlanInputRequestResponse] = Field(default_factory=list)


class PlanVersionResource(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    plan_id: int
    version_number: int
    parent_version_id: int | None
    produced_by_run_id: int | None
    produced_by_step_id: int | None
    content: str
    context_session_id: str | None
    context_log_id: int | None
    repo_revision: dict | None
    reviewer_repo_revision: dict | None = None
    review_verdict: str | None
    review_feedback: str | None
    reviewed_by_step_id: int | None
    review_exhausted: bool
    reviewed_at: datetime | None
    human_decision: str
    decided_at: datetime | None
    decided_by: int | None
    superseded_by_version_id: int | None
    applied: bool = False
    display_state: (
        Literal[
            "applied",
            "approved",
            "rejected",
            "superseded",
            "awaiting_review",
            "draft",
        ]
        | None
    ) = None
    created_at: datetime


class PlanResource(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    title: str
    initial_request: str
    initial_attachments: list[dict] | None
    target_task_id: int | None
    project_id: int | None
    target_repo: str | None
    target_branch: str | None
    worker_id: int | None
    priority: int
    timeout_hours: float | None
    created_by: int | None
    current_version_id: int | None
    active_run_id: int | None
    forked_from_version_id: int | None
    archived_at: datetime | None
    closed_at: datetime | None
    lock_version: int
    created_at: datetime
    updated_at: datetime
    display_state: str
    legacy: bool = False
    ownership: Literal["standard", "capability"] = "standard"
    read_only: bool = False
    # Read-only projection resolved from the Delivery Task or an applied cycle.
    # DeliveryRun remains the sole owner of orchestration state.
    delivery_run_id: int | None = None
    latest_run_status: str | None = None
    latest_run_error: str | None = None
    pipeline_config: PlanPipelineConfig
    application: "PlanApplicationResource | None" = None
    applications: list["PlanApplicationResource"] = Field(default_factory=list)
    application_attempts: list["PlanApplicationAttemptResource"] = Field(
        default_factory=list
    )
    current_version: PlanVersionResource | None = None
    active_run: PlanRunResource | None = None
    open_input_request: PlanInputRequestResponse | None = None


class PlanExecutionResource(BaseModel):
    plan: PlanResource
    version: PlanVersionResource
    execution_task_id: int


class PlanApplicationResource(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    plan_id: int
    plan_version_id: int
    application_type: str
    target_task_id: int | None
    target_session_id: str | None
    user_log_id: int | None
    execution_task_id: int | None
    execution_task_available: bool | None = None
    application_receipt_key: str | None = None
    delivery_status: str | None = None
    delivery_error: str | None = None
    launch_evidence: dict | None = None
    delivery_resolution: dict | None = None
    created_at: datetime


class PlanApplicationAttemptResource(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    plan_id: int
    plan_version_id: int
    application_receipt_key: str
    application_type: str
    target_task_id: int | None
    target_session_id: str | None
    user_log_id: int | None
    execution_task_id: int | None
    applied_by: int | None
    application_created_at: datetime
    released_at: datetime
    delivery_status: str = "missing"
    delivery_error: str | None = None
    launch_evidence: dict | None = None
    delivery_resolution: dict | None = None


PlanResource.model_rebuild()
