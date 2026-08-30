from datetime import datetime, timezone
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    field_serializer,
    model_serializer,
    model_validator,
)

from backend.config import settings
from backend.models.task_id_allocator import TASK_ID_WORKER_NAMESPACE_START
from backend.schemas.capability import AutoCapabilityPolicy
from backend.schemas.plan import PlanPipelineConfig
from backend.schemas.task_ssh_grant import TaskSSHGrantInput
from backend.services.upload_references import (
    is_public_upload_reference_id,
    is_safe_upload_display_name,
    managed_upload_url_basename,
)


TaskMode = Literal["auto", "plan", "loop", "goal", "pr_loop"]


class UserSkillSnapshotPayload(BaseModel):
    """Internal Manager→Worker copy of one selected User Skill."""

    id: int = Field(gt=0)
    name: str = Field(min_length=1, max_length=100)
    description: str = ""
    content: str = ""


class FrontendReviewConfig(BaseModel):
    """Task-scoped frontend review workflow selected by the composer UI."""

    mode: Literal["goal"] = "goal"
    profile: Literal["standard", "exhaustive"] = "standard"
    max_iterations: int = Field(default=5, ge=1, le=10)


def _normalize_task_provider(value: object) -> str:
    """Canonicalize an explicit task provider without accepting empty input."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("provider must be 'claude' or 'codex'")
    provider = value.strip().lower()
    if provider not in {"claude", "codex"}:
        raise ValueError("provider must be 'claude' or 'codex'")
    return provider


def _normalize_attention_tag(value: object) -> object:
    """Trim a user attention tag and canonicalize blank input to NULL."""

    if not isinstance(value, str):
        return value
    normalized = value.strip()
    return normalized or None


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Internal Manager→Worker forwarding only: Manager allocates the global ID.
    id: int | None = Field(
        default=None,
        gt=0,
        lt=TASK_ID_WORKER_NAMESPACE_START,
    )
    # Internal Manager→Worker identity fence. Worker mirrors reuse the exact
    # logical incarnation so every later remote mutation can reject Task-id
    # ABA. Public callers cannot combine this with an explicit id.
    source_incarnation_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{32}$",
    )
    # Internal Manager→Worker initial-create generation.  The Manager has
    # already claimed logical turn G before forwarding; the Worker persists an
    # inert pending row at (retry=N, turn=G-1) so its own dequeue advances to
    # exactly the same N/G generation.  Public creation cannot set these.
    source_retry_count: int | None = Field(default=None, ge=0)
    source_turn_generation: int | None = Field(default=None, ge=1)
    # Internal Manager→Worker authority envelope. Public creation routes
    # reject these fields; explicit-id forwarding must carry all four and the
    # Worker accepts only delegated/system kinds.
    execution_user_id: int | None = Field(default=None, gt=0)
    execution_user_role: Literal["member", "admin", "super_admin"] | None = None
    execution_mode: Literal["sandbox", "unrestricted"] | None = None
    execution_principal_kind: Literal[
        "user",
        "deployment_token",
        "system",
        "delegated_user",
        "delegated_deployment_token",
    ] | None = None
    # None = 本机执行；有值 = 创建后由 Dispatcher 转发到该 Worker
    worker_id: int | None = None
    # TaskMigrator 在目标机重建 task 时带上（跨机 --resume 续聊）
    session_id: str | None = None
    last_cwd: str | None = None
    title: str = ""
    description: str = ""
    project_id: int | None = None
    target_repo: str | None = None
    target_branch: str = "main"
    priority: int = 0
    max_retries: int = 2
    mode: TaskMode = "auto"
    # Explicit opt-in for model-requested Plan/Review. ``None`` is the only
    # disabled state; a non-NULL policy is immutable for this Task incarnation.
    capability_policy: AutoCapabilityPolicy | None = None
    # Reserved for the Delivery Controller.  They are declared explicitly so
    # a public caller cannot smuggle ownership hints through Pydantic's legacy
    # extra-field compatibility.  ``None`` remains harmless for clients that
    # serialize the read model back into a create form.
    delivery_run_id: int | None = Field(default=None, exclude=True)
    delivery_role: str | None = Field(default=None, exclude=True)
    todo_file_path: str | None = None  # required when mode="loop"
    max_iterations: int = 50  # loop only: max iterations before auto-abort
    must_complete: bool = False  # loop only: reject done until all items finished
    goal_condition: str | None = None  # goal only: natural-language completion condition
    goal_max_turns: int = 30  # goal only: max turns before auto-fail
    goal_evaluator_model: str | None = None  # goal only: evaluator model (default haiku)
    pr_loop_max_turns: int = 10  # pr_loop only: max fix rounds
    pr_loop_poll_interval: int = 60  # pr_loop only: seconds between GitHub API polls
    frontend_review: FrontendReviewConfig | None = None
    # API callers that omit provider follow the deployment-wide default.
    provider: str = Field(
        default_factory=lambda: settings.default_provider,
        validate_default=True,
    )
    model: str | None = None
    codex_service_tier: Literal["default", "priority"] = "default"
    effort_level: str | None = None
    thinking_budget: int | None = None
    system_prompt_mode: str | None = None
    timeout_hours: float | None = None
    sort_order: float | None = None
    enable_workflows: bool = False
    enabled_skills: dict | None = None
    selected_user_skills: list[int] | None = None
    user_skill_snapshots: list[UserSkillSnapshotPayload] | None = None
    tags: list[str] | None = None
    attention_tag: str | None = Field(default=None, max_length=80)
    image_paths: list[str] | None = None  # kept for backwards compat
    file_paths: list[str] | None = None
    attachments: list[dict] | None = None  # [{url, name, is_image}, ...]
    secret_ids: list[int] | None = None
    ssh_grants: list[TaskSSHGrantInput] | None = Field(default=None, max_length=50)
    clone_from_task_id: int | None = None
    # Internal/Plan endpoints use these fields to preserve independent Plan
    # relationships across Manager→Worker copies. Public creation validates the
    # referenced Task before accepting them.
    plan_target_task_id: int | None = None
    plan_context_session_id: str | None = None
    plan_context_log_id: int | None = None
    plan_context_snapshot: str | None = Field(default=None, max_length=60_000)
    plan_repo_revision: dict | None = None
    supersedes_plan_task_id: int | None = None
    plan_pipeline_config: PlanPipelineConfig | None = None
    starred: bool = False

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> str:
        return _normalize_task_provider(value)

    @field_validator("attention_tag", mode="before")
    @classmethod
    def normalize_attention_tag(cls, value: object) -> object:
        return _normalize_attention_tag(value)

    @model_validator(mode='after')
    def validate_mode_fields(self):
        if self.frontend_review is not None:
            self.mode = 'goal'
            self.goal_max_turns = self.frontend_review.max_iterations
        if self.delivery_run_id is not None or self.delivery_role is not None:
            raise ValueError(
                "delivery_run_id and delivery_role are reserved for the "
                "Delivery Controller"
            )
        if self.capability_policy is not None:
            if self.mode != "auto":
                raise ValueError("capability_policy requires mode=auto")
            if self.worker_id is not None:
                raise ValueError("capability_policy is local-task only")
            if self.id is not None:
                raise ValueError(
                    "Manager-forwarded Worker Tasks cannot use capability_policy"
                )
        if self.mode not in ('loop',) and not self.description:
            raise ValueError('description is required for non-loop tasks')
        if self.mode == 'loop' and not self.todo_file_path:
            raise ValueError('todo_file_path is required for loop tasks')
        if self.mode == 'goal' and not self.goal_condition and self.frontend_review is None:
            raise ValueError('goal_condition is required for goal tasks')
        if self.mode == 'pr_loop' and not self.description:
            raise ValueError('description is required for pr_loop tasks')
        return self


class TaskMigrationImport(TaskCreate):
    """Manager -> Worker migration payload.

    Unlike the public create endpoint, this path requires the Manager-assigned
    global ID and is persisted as an inert task on the destination Worker.
    """

    id: int = Field(gt=0, lt=TASK_ID_WORKER_NAMESPACE_START)
    # One migration attempt owns the inert destination mirror until the
    # Manager commits its Worker pointer.  Rollback must echo this nonce and
    # the exact Task generation before the Worker may remove the mirror.
    migration_operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    migration_operation_sequence: int = Field(gt=0)
    # Auto capability history and resume state are Manager-local.  Inheriting
    # TaskCreate must never make the migration transport accept this field.
    capability_policy: None = None
    # Accept the reserved wire value so the internal endpoint can reject it
    # with a lifecycle conflict (409) instead of letting schema validation
    # disguise an attempted Delivery ownership migration as malformed input.
    mode: Literal["auto", "plan", "loop", "goal", "delivery_loop", "pr_loop"] = "auto"
    # Keep Manager and destination Worker retry generations monotonic. This is
    # intentionally internal-only; public task creation always starts at zero.
    retry_count: int = Field(default=0, ge=0)
    turn_generation: int = Field(default=0, ge=0)
    # Migration may preserve an already-inert source state without ever
    # exposing a dispatchable ``pending`` row on the destination Worker.
    source_status: Literal[
        "plan_review",
        "completed",
        "failed",
        "cancelled",
        "conflict",
    ] = "cancelled"

    @model_validator(mode="after")
    def require_incarnation_for_rollback_reservation(self):
        if self.source_incarnation_id is None:
            raise ValueError(
                "migration_operation_id requires source_incarnation_id"
            )
        return self


class TaskMigrationImportRollback(BaseModel):
    """Exact, idempotent cleanup for a failed destination import."""

    model_config = ConfigDict(extra="forbid")

    task_id: int = Field(gt=0, lt=TASK_ID_WORKER_NAMESPACE_START)
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    operation_sequence: int = Field(gt=0)
    incarnation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    retry_count: int = Field(ge=0)
    turn_generation: int = Field(ge=0)
    source_status: Literal[
        "plan_review",
        "completed",
        "failed",
        "cancelled",
        "conflict",
    ]


class TaskMigrationImportCommit(TaskMigrationImportRollback):
    """Exact destination acknowledgement after Manager pointer commit."""


class TaskTerminationRequest(BaseModel):
    """Manager→Worker fence for the hidden PR-review termination endpoint."""

    expected_status: str
    expected_retry_count: int = Field(ge=0)
    expected_turn_generation: int = Field(ge=0)
    expected_instance_id: int | None = None
    expected_started_at: datetime | None = None
    expected_completed_at: datetime | None = None
    # Required even when NULL: omission would let an old Manager snapshot
    # silently adopt the Worker's arrival-time PTY epoch.
    expected_pty_background_generation: str | None


class WorkerRoutingConfigRequest(BaseModel):
    """Internal Manager→Worker routing synchronization payload."""

    model_config = ConfigDict(extra="forbid")

    op_id: str = Field(min_length=1, max_length=128)
    provider: str = Field(min_length=1)
    model: str | None = None
    codex_service_tier: Literal["default", "priority"]

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> str:
        return _normalize_task_provider(value)


class WorkerRoutingConfigPending(WorkerRoutingConfigRequest):
    """Durable staged candidate returned by Worker readback."""


class WorkerRoutingConfigSnapshot(BaseModel):
    """Strict Worker-local live tuple plus its optional staged candidate."""

    model_config = ConfigDict(extra="forbid")

    id: int
    status: str
    worker_id: None = None
    shared_from_id: None = None
    provider: str
    model: str | None = None
    codex_service_tier: Literal["default", "priority"]
    pending: WorkerRoutingConfigPending | None = None


class TaskRoutingExpectation(BaseModel):
    """Client-visible routing tuple used to reject stale UI actions."""

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(min_length=1)
    model: str | None = None
    codex_service_tier: Literal["default", "priority"]

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> str:
        return _normalize_task_provider(value)


class TaskActionRequest(BaseModel):
    """Optional stale-view fence for actions that can start a model turn."""

    model_config = ConfigDict(extra="forbid")

    expected_routing: TaskRoutingExpectation | None = None


class WorkerManualRetryRequest(BaseModel):
    """Exact Manager -> Worker envelope for one idempotent manual retry.

    The Worker's bearer token authenticates the control plane, but it does not
    identify the user whose retry is being executed.  This internal-only
    request therefore carries both the immutable source generation and the
    complete delegated principal for the replacement generation.
    """

    model_config = ConfigDict(extra="forbid")

    protocol_version: Literal[1]
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    request_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    task_id: int = Field(gt=0)
    worker_id: int = Field(gt=0)
    source_incarnation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    expected_status: Literal["failed", "cancelled", "conflict", "completed"]
    expected_retry_count: int = Field(ge=0)
    expected_turn_generation: int = Field(ge=0)
    source_principal_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_principal_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution_user_id: int | None = Field(default=None, gt=0)
    execution_user_role: Literal["member", "admin", "super_admin"]
    execution_mode: Literal["sandbox", "unrestricted"]
    execution_principal_kind: Literal[
        "delegated_user",
        "delegated_deployment_token",
        "system",
    ]


class PlanApprovalRequest(TaskActionRequest):
    confirm_stale: bool = False


class WorkerPlanDecisionRequest(BaseModel):
    """Exact internal envelope for one legacy Plan terminal decision."""

    model_config = ConfigDict(extra="forbid")

    protocol_version: Literal[1]
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    request_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    action: Literal["approve", "reject"]
    task_id: int = Field(gt=0)
    manager_worker_id: int = Field(gt=0)
    source_incarnation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    expected_status: Literal["plan_review"]
    expected_retry_count: int = Field(ge=0)
    expected_turn_generation: int = Field(ge=0)
    decision_path: str = Field(min_length=1, max_length=500)
    routing: TaskRoutingExpectation
    decision_body: dict | None = None
    plan_target_task_id: int | None = Field(default=None, gt=0)
    plan_target_incarnation_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{32}$",
    )


class TaskUpdate(BaseModel):
    # 执行位置切换：传 worker_id 触发 TaskMigrator（-1 表示切回本机，
    # 因为 None 在 exclude_unset 语义下无法与「未传」区分）
    worker_id: int | None = None
    title: str | None = None
    model: str | None = None
    # A concrete default keeps PATCH-style exclude_unset semantics while
    # rejecting an explicit JSON null for this non-null Task setting.
    codex_service_tier: Literal["default", "priority"] = "default"
    effort_level: str | None = None
    thinking_budget: int | None = None
    system_prompt_mode: str | None = None
    timeout_hours: float | None = None
    sort_order: float | None = None
    description: str | None = None
    priority: int | None = None
    project_id: int | None = None
    target_repo: str | None = None
    target_branch: str | None = None
    max_retries: int | None = None
    max_iterations: int | None = None
    must_complete: bool | None = None
    mode: TaskMode | None = None
    # Reserve the wire name so old Pydantic extra-field behavior cannot turn a
    # policy mutation into a misleading 200 response. Policies are create-only.
    capability_policy: AutoCapabilityPolicy | None = Field(
        default=None,
        exclude=True,
    )
    goal_condition: str | None = None
    goal_max_turns: int | None = None
    goal_evaluator_model: str | None = None
    pr_loop_max_turns: int | None = None
    pr_loop_poll_interval: int | None = None
    enable_workflows: bool | None = None
    enabled_skills: dict | None = None
    selected_user_skills: list[int] | None = None
    user_skill_snapshots: list[UserSkillSnapshotPayload] | None = None
    provider: str | None = None
    starred: bool | None = None
    tags: list[str] | None = None
    attention_tag: str | None = Field(default=None, max_length=80)

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> str:
        # The field's None default represents "not supplied" under
        # exclude_unset. An explicit JSON null does run this validator and must
        # not become a database NULL or inherit a deployment default.
        return _normalize_task_provider(value)

    @field_validator("mode", mode="before")
    @classmethod
    def reject_mode_mutation(cls, value: object) -> object:
        # Mode selects a lifecycle state machine and is therefore part of the
        # Task's creation identity.  Changing it after dequeue can race a
        # detached dispatcher snapshot and turn a Plan into an ordinary coding
        # launch (or vice versa).  Keep the field on the wire so old clients
        # receive an explicit validation error instead of a misleading 200.
        raise ValueError("mode is immutable after Task creation")

    @field_validator("attention_tag", mode="before")
    @classmethod
    def normalize_attention_tag(cls, value: object) -> object:
        return _normalize_attention_tag(value)

    @model_validator(mode="after")
    def reject_capability_policy_mutation(self):
        if "capability_policy" in self.model_fields_set:
            raise ValueError("capability_policy is immutable after Task creation")
        return self


class InternalTaskSkillsUpdate(BaseModel):
    """Narrow payload accepted from the Task-scoped skills MCP server."""

    model_config = ConfigDict(extra="forbid")

    enabled_skills: dict


def _public_attachment(value: object) -> dict | None:
    """Return only display-safe fields from one persisted attachment."""

    if not isinstance(value, dict):
        return None
    url = value.get("url")
    name = value.get("name")
    is_image = value.get("is_image")
    if (
        managed_upload_url_basename(url) is None
        or not is_safe_upload_display_name(name)
        or type(is_image) is not bool
    ):
        return None
    return {"url": url, "name": name, "is_image": is_image}


def _public_fork_seed_upload(value: object) -> dict | None:
    """Project a fork upload without returning its absolute host path.

    The public composer may reuse the managed upload URL as an opaque path
    reference.  ``validate_upload_attachments`` resolves that reference back
    into the private upload root before any provider boundary.
    """

    if not isinstance(value, dict):
        return None
    upload_id = value.get("id")
    filename = value.get("filename")
    url = value.get("url")
    is_image = value.get("is_image")
    if (
        not is_public_upload_reference_id(upload_id)
        or filename is not None
        and not is_safe_upload_display_name(filename)
        or managed_upload_url_basename(url) is None
        or type(is_image) is not bool
    ):
        return None
    return {
        "id": upload_id,
        "filename": filename,
        "path": url,
        "url": url,
        "is_image": is_image,
    }


def public_task_metadata(value: object) -> dict | None:
    """Whitelist the small metadata surface used by the human Task UI.

    Task metadata is also the durable storage area for account bindings,
    principal/generation receipts, migration reservations, terminal gates and
    other control-plane state.  A blacklist would inevitably expose future
    protocol keys, so public responses copy only the explicitly supported UI
    fields below.
    """

    if not isinstance(value, dict):
        return None
    projected: dict[str, object] = {}

    def positive_int(key: str) -> None:
        item = value.get(key)
        if type(item) is int and item > 0:
            projected[key] = item

    def nullable_positive_int(key: str) -> None:
        item = value.get(key)
        if item is None and key in value:
            projected[key] = None
        elif type(item) is int and item > 0:
            projected[key] = item

    def bounded_string(key: str, *, maximum: int) -> None:
        item = value.get(key)
        if isinstance(item, str) and len(item) <= maximum:
            projected[key] = item

    for key in (
        "forked_from_task_id",
        "plan_agent_run_id",
        "revised_from_plan_task_id",
        "plan_superseded_by_task_id",
    ):
        positive_int(key)
    if value.get("pr_monitor_display") is True:
        projected["pr_monitor_display"] = True
        # These IDs are only useful for the durable display projection.  Keep
        # them behind the marker so an internal/malformed Task cannot expose
        # PR topology merely by placing similarly named metadata keys.
        positive_int("pr_monitor_run_id")
        positive_int("pr_monitor_review_id")
    for key in ("forked_from_log_id", "fork_seed_log_id"):
        nullable_positive_int(key)
    bounded_string("forked_from_turn_id", maximum=500)
    bounded_string("fork_seed_message", maximum=200_000)
    bounded_string("plan_review_feedback", maximum=200_000)
    if value.get("fork_mode") in {"branch", "full_copy"}:
        projected["fork_mode"] = value["fork_mode"]
    if value.get("plan_review_verdict") in {"approve", "revise"}:
        projected["plan_review_verdict"] = value["plan_review_verdict"]
    if type(value.get("plan_review_exhausted")) is bool:
        projected["plan_review_exhausted"] = value["plan_review_exhausted"]
    if "frontend_review" in value:
        try:
            projected["frontend_review"] = FrontendReviewConfig.model_validate(
                value["frontend_review"]
            ).model_dump(mode="json")
        except (TypeError, ValueError):
            pass
    attachments = [
        attachment
        for item in value.get("attachments") or []
        if (attachment := _public_attachment(item)) is not None
    ]
    if attachments:
        projected["attachments"] = attachments
    fork_uploads = [
        upload
        for item in value.get("fork_seed_uploads") or []
        if (upload := _public_fork_seed_upload(item)) is not None
    ]
    if fork_uploads:
        projected["fork_seed_uploads"] = fork_uploads
    return projected or None


def shared_task_metadata(value: object) -> dict | None:
    """Remove owner-only composer state from an already public projection."""

    projected = public_task_metadata(value)
    if projected is None:
        return None
    for key in ("fork_seed_message", "fork_seed_log_id", "fork_seed_uploads"):
        projected.pop(key, None)
    return projected or None


def _worker_managed_projection(*, worker_id: int | None, metadata: object) -> bool:
    return bool(
        worker_id is not None
        or isinstance(metadata, dict)
        and (
            metadata.get("ccm_worker_managed_task") is True
            or "ccm_user_skill_snapshots" in metadata
        )
    )


class _TaskResponseBase(BaseModel):
    id: int
    # Logical identity is returned so a Manager can prove that an explicit-id
    # Worker create persisted the exact incarnation it sent.  Internal writes
    # still require scoped service authentication; this value is not a secret.
    incarnation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    worker_id: int | None = None
    created_by: int | None = None
    execution_user_id: int | None = None
    execution_user_role: Literal[
        "member", "admin", "super_admin"
    ] = "member"
    execution_mode: Literal["sandbox", "unrestricted"] = "sandbox"
    execution_principal_kind: Literal[
        "user",
        "deployment_token",
        "system",
        "delegated_user",
        "delegated_deployment_token",
    ] = "system"
    title: str
    description: str | None
    status: str
    priority: int
    project_id: int | None
    target_repo: str | None
    target_branch: str
    result_branch: str | None
    merge_status: str
    instance_id: int | None
    retry_count: int
    turn_generation: int
    max_retries: int
    mode: str
    capability_policy: AutoCapabilityPolicy | None = None
    delivery_run_id: int | None = None
    delivery_role: str | None = None
    # Read-only DeliveryRun projection.  A Delivery-owned Task remains a
    # scheduler shell; the Run is the authority for its user-facing lifecycle.
    delivery_phase: str | None = None
    delivery_activity: str | None = None
    delivery_outcome: str | None = None
    delivery_terminal: str | None = None
    todo_file_path: str | None
    loop_progress: str | None
    max_iterations: int
    must_complete: bool
    goal_condition: str | None
    goal_evaluator_model: str | None
    goal_max_turns: int
    goal_turns_used: int
    goal_last_reason: str | None
    pr_loop_url: str | None
    pr_loop_number: int | None
    pr_loop_repo: str | None
    pr_loop_state: str | None
    pr_loop_max_turns: int
    pr_loop_turns_used: int
    pr_loop_poll_interval: int
    plan_content: str | None
    plan_approved: bool | None
    plan_target_task_id: int | None = None
    supersedes_plan_task_id: int | None = None
    plan_approved_at: datetime | None = None
    plan_approved_by: int | None = None
    plan_applied_at: datetime | None = None
    plan_applied_to_session_id: str | None = None
    plan_execution_task_id: int | None = None
    canonical_plan_id: int | None = None
    plan_pipeline_config: PlanPipelineConfig | None = None
    # Read-only projection of the latest PlanAgentRun. Task.status remains the
    # stable scheduler lifecycle (for example ``executing``).
    plan_stage: str | None = None
    plan_stage_round: int | None = None
    plan_stage_provider: str | None = None
    plan_stage_model: str | None = None
    plan_stage_effort: str | None = None
    plan_stage_route_slot: Literal["primary", "fallback"] | None = None
    session_id: str | None
    provider: str
    model: str | None
    codex_service_tier: Literal["default", "priority"]
    effort_level: str | None
    thinking_budget: int | None
    system_prompt_mode: str | None = None
    timeout_hours: float | None = None
    last_accessed_at: datetime | None = None
    sort_order: float | None = None
    enable_workflows: bool
    enabled_skills: dict | None
    selected_user_skills: list[int] | None = None
    starred: bool
    archived: bool
    has_unread: bool
    error_message: str | None
    tags: list[str] | None
    attention_tag: str | None = None
    metadata_: dict | None = None
    shared_from_id: int | None = None
    active_sub_agents: int = 0
    background_active: bool = False
    context_window_usage: dict | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = {"from_attributes": True}

    @field_serializer("created_at", "started_at", "completed_at")
    @classmethod
    def _serialize_utc(cls, v: datetime | None) -> str | None:
        if v is None:
            return None
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()

    @model_validator(mode="after")
    def _ensure_default_skills(self):
        from backend.services.command_registry import ensure_default_skills
        self.enabled_skills = ensure_default_skills(self.enabled_skills)
        return self


class TaskResponse(_TaskResponseBase):
    """Human-facing Task projection with explicitly public metadata only."""

    # Keep the complete ORM input shape so this projection can derive stable
    # booleans such as ``has_session``, while removing machine identities from
    # both serialized output *and* FastAPI's response schema.  Internal wire
    # endpoints use InternalTaskResponse and retain every field.
    incarnation_id: str = Field(pattern=r"^[0-9a-f]{32}$", exclude=True)
    execution_user_id: int | None = Field(default=None, exclude=True)
    execution_user_role: Literal[
        "member", "admin", "super_admin"
    ] = Field(default="member", exclude=True)
    execution_mode: Literal["sandbox", "unrestricted"] = Field(
        default="sandbox",
        exclude=True,
    )
    execution_principal_kind: Literal[
        "user",
        "deployment_token",
        "system",
        "delegated_user",
        "delegated_deployment_token",
    ] = Field(default="system", exclude=True)
    instance_id: int | None = Field(default=None, exclude=True)
    plan_approved_by: int | None = Field(default=None, exclude=True)
    plan_applied_to_session_id: str | None = Field(
        default=None,
        exclude=True,
    )
    session_id: str | None = Field(default=None, exclude=True)
    is_worker_managed: bool = False
    has_session: bool = False
    access_scope: Literal["control", "chat"] = "control"

    @model_validator(mode="after")
    def _project_public_metadata(self):
        raw_metadata = self.metadata_
        self.is_worker_managed = _worker_managed_projection(
            worker_id=self.worker_id,
            metadata=raw_metadata,
        )
        self.has_session = bool(self.session_id)
        self.metadata_ = public_task_metadata(raw_metadata)
        return self

    @model_serializer(mode="wrap")
    def _omit_machine_identity(self, handler):
        payload = handler(self)
        for key in (
            "incarnation_id",
            "execution_user_id",
            "execution_user_role",
            "execution_mode",
            "execution_principal_kind",
            "instance_id",
            "plan_approved_by",
            "plan_applied_to_session_id",
            "session_id",
        ):
            payload.pop(key, None)
        return payload


class SharedTaskResponse(TaskResponse):
    """Narrow view for users whose only authority is a chat Task share."""

    access_scope: Literal["chat"] = "chat"

    @model_validator(mode="after")
    def _remove_owner_only_metadata(self):
        self.metadata_ = shared_task_metadata(self.metadata_)
        return self

    @model_serializer(mode="wrap")
    def _project_chat_share_fields(self, handler):
        payload = handler(self)
        # This is deliberately an allowlist. A Task row also carries routing,
        # native-session and control configuration at top level; adding a new
        # internal field must never silently widen a chat-only share.
        allowed = {
            "id",
            "title",
            "description",
            "status",
            "priority",
            "project_id",
            "target_branch",
            "result_branch",
            "merge_status",
            "retry_count",
            "turn_generation",
            "mode",
            "delivery_run_id",
            "delivery_role",
            "delivery_phase",
            "delivery_activity",
            "delivery_outcome",
            "delivery_terminal",
            "loop_progress",
            "max_iterations",
            "must_complete",
            "goal_max_turns",
            "goal_turns_used",
            "goal_last_reason",
            "pr_loop_url",
            "pr_loop_number",
            "pr_loop_repo",
            "pr_loop_state",
            "pr_loop_max_turns",
            "pr_loop_turns_used",
            "pr_loop_poll_interval",
            "plan_content",
            "plan_approved",
            "plan_target_task_id",
            "supersedes_plan_task_id",
            "plan_approved_at",
            "plan_applied_at",
            "plan_execution_task_id",
            "canonical_plan_id",
            "plan_stage",
            "plan_stage_round",
            "plan_stage_provider",
            "plan_stage_model",
            "plan_stage_effort",
            "plan_stage_route_slot",
            "provider",
            "model",
            "codex_service_tier",
            "effort_level",
            "starred",
            "archived",
            "has_unread",
            "tags",
            "attention_tag",
            "metadata_",
            # Legacy cross-CCM shadows still use this nullable identity to
            # select their proxy chat path. Team Task shares do not fabricate
            # it; access_scope is their explicit UI capability signal.
            "shared_from_id",
            "active_sub_agents",
            "background_active",
            "context_window_usage",
            "created_at",
            "started_at",
            "completed_at",
            "is_worker_managed",
            "has_session",
            "access_scope",
        }
        return {key: value for key, value in payload.items() if key in allowed}


class InternalTaskResponse(_TaskResponseBase):
    """Complete Task wire model for scoped services and Manager→Worker RPC."""

    is_worker_managed: bool = False

    @model_validator(mode="after")
    def _project_worker_managed_state(self):
        self.is_worker_managed = _worker_managed_projection(
            worker_id=self.worker_id,
            metadata=self.metadata_,
        )
        return self


class TaskMigrationImportResponse(InternalTaskResponse):
    """Internal migration acknowledgement with the immutable identity fence."""

    incarnation_id: str = Field(pattern=r"^[0-9a-f]{32}$")


class TaskTerminationSnapshot(InternalTaskResponse):
    """Internal-only Worker snapshot with the opaque PTY generation fence."""

    pty_background_generation: str | None


class WorkerManualRetryResponse(BaseModel):
    """Internal acknowledgement containing the durable Worker receipt."""

    model_config = ConfigDict(extra="forbid")

    task: InternalTaskResponse
    receipt: dict


class WorkerPlanDecisionResponse(BaseModel):
    """Internal acknowledgement containing the applied decision receipt."""

    model_config = ConfigDict(extra="forbid")

    task: InternalTaskResponse
    receipt: dict


class WorkerPlanDecisionAbsentResponse(BaseModel):
    """Read-only proof that an exact Worker operation has no receipt yet."""

    model_config = ConfigDict(extra="forbid")

    protocol_version: Literal[1]
    state: Literal["absent"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    task_id: int = Field(gt=0)
