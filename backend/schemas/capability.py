"""Public and internal schema contracts for generic task capabilities."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


AUTO_CAPABILITY_KEYS = ("plan", "code_review")
MAX_AUTO_CAPABILITY_INVOCATIONS = 8
AutoCapabilityKey = Literal["plan", "code_review"]
AutoCapabilityLimit = Annotated[
    int,
    Field(strict=True, ge=1, le=MAX_AUTO_CAPABILITY_INVOCATIONS),
]


class AutoCapabilityPolicy(BaseModel):
    """Frozen allowlist and non-refundable budgets for Agent requests.

    Mapping keys are the allowlist, so authorization and per-capability budgets
    cannot drift into two contradictory representations.  SQL ``NULL`` on the
    Task remains the only disabled state.
    """

    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    max_invocations: AutoCapabilityLimit
    capabilities: dict[AutoCapabilityKey, AutoCapabilityLimit]

    @field_validator("version", mode="before")
    @classmethod
    def require_strict_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("version must be the integer 1")
        return value

    @field_validator("capabilities")
    @classmethod
    def normalize_capabilities(
        cls,
        value: dict[AutoCapabilityKey, int],
    ) -> dict[AutoCapabilityKey, int]:
        if not value:
            raise ValueError("at least one capability budget is required")
        return {
            key: value[key]
            for key in AUTO_CAPABILITY_KEYS
            if key in value
        }

    @model_validator(mode="after")
    def validate_budget_relationships(self):
        if any(
            limit > self.max_invocations
            for limit in self.capabilities.values()
        ):
            raise ValueError(
                "per-capability budgets cannot exceed max_invocations"
            )
        if self.max_invocations > sum(self.capabilities.values()):
            raise ValueError(
                "max_invocations cannot exceed the available capability budgets"
            )
        return self


class CapabilityInvocationCreate(BaseModel):
    """Public human request; source/policy/executor are server-owned."""

    model_config = ConfigDict(extra="forbid")

    capability: str = Field(min_length=1, max_length=64)
    request: dict = Field(default_factory=dict)
    idempotency_key: str = Field(min_length=1, max_length=128)

    @field_validator("capability", "idempotency_key")
    @classmethod
    def strip_nonempty(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized


class CapabilityInvocationCancel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_state_version: int = Field(ge=1)


class CapabilityInvocationConsume(BaseModel):
    """Acknowledge one exact ready result and release the Task slot."""

    model_config = ConfigDict(extra="forbid")

    expected_state_version: int = Field(ge=1)


class CapabilityExecutionResource(BaseModel):
    id: int
    invocation_id: int
    attempt: int
    status: str
    state_version: int
    executor_kind: str
    handle_kind: str | None
    handle_id: str | None
    handle_generation: int | None
    output_kind: str | None
    output_id: int | None
    output_hash: str | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class CapabilityInvocationResource(BaseModel):
    id: int
    task_id: int
    capability_key: str
    source: str
    purpose: str
    status: str
    state_version: int
    idempotency_key: str
    input_payload: dict
    input_hash: str
    subject_kind: str
    subject_ref: dict
    subject_hash: str
    executor_kind: str
    executor_config_hash: str
    policy_hash: str
    resume_policy: str
    max_attempts: int
    requested_by_user_id: int | None
    request_task_incarnation_id: str | None
    request_task_retry_count: int | None
    request_task_instance_id: int | None
    request_task_started_at: datetime | None
    request_task_session_id: str | None
    request_task_turn_generation: int | None
    request_source_log_id: int | None
    request_output_log_id: int | None
    request_terminal_log_id: int | None
    request_reason: str | None
    request_protocol_version: int | None
    request_output_hash: str | None
    request_native_turn_id: str | None
    result_kind: str | None
    result_id: int | None
    result_hash: str | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    ready_at: datetime | None
    completed_at: datetime | None
    active_execution: CapabilityExecutionResource | None = None

    model_config = ConfigDict(from_attributes=True)


class CapabilityInvocationCreateResource(BaseModel):
    invocation: CapabilityInvocationResource
    created: bool


class CodeReviewResultResource(BaseModel):
    id: int
    run_id: int
    capability_invocation_id: int
    capability_execution_id: int
    developer_task_id: int
    reviewer_task_id: int
    reviewer_task_retry_count: int
    reviewer_task_instance_id: int | None
    reviewer_task_started_at: datetime
    reviewer_task_completed_at: datetime
    output_log_id: int
    schema_version: int
    role: str
    verdict: str
    summary: str
    findings: list
    subject_ref: dict
    subject_hash: str
    result_hash: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CapabilityResultResource(BaseModel):
    """ACL-scoped materialization of an Invocation's exact output."""

    invocation_id: int
    invocation_status: str
    kind: str
    id: int
    hash: str
    resource_url: str
    data: dict
