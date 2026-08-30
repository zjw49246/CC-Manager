from datetime import datetime
import secrets

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Float,
    Integer,
    String,
    Text,
    DateTime,
    JSON,
    UniqueConstraint,
    select,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, column_property

from backend.database import Base


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint(
            "codex_service_tier IN ('default', 'priority')",
            name="ck_tasks_codex_service_tier",
        ),
        CheckConstraint(
            "execution_user_role IN ('member', 'admin', 'super_admin')",
            name="ck_tasks_execution_user_role",
        ),
        CheckConstraint(
            "execution_mode IN ('sandbox', 'unrestricted')",
            name="ck_tasks_execution_mode",
        ),
        CheckConstraint(
            "execution_principal_kind IN "
            "('user', 'deployment_token', 'system', 'delegated_user', "
            "'delegated_deployment_token')",
            name="ck_tasks_execution_principal_kind",
        ),
        CheckConstraint(
            "(execution_principal_kind IN ('user', 'delegated_user') "
            "AND execution_user_id IS NOT NULL) OR "
            "(execution_principal_kind NOT IN ('user', 'delegated_user') "
            "AND execution_user_id IS NULL)",
            name="ck_tasks_execution_user_shape",
        ),
        CheckConstraint(
            "(execution_principal_kind = 'system' "
            "AND execution_user_role = 'member' "
            "AND execution_mode = 'sandbox') OR "
            "(execution_principal_kind IN "
            "('deployment_token', 'delegated_deployment_token') "
            "AND execution_user_role = 'super_admin' "
            "AND execution_mode = 'unrestricted') OR "
            "(execution_principal_kind IN ('user', 'delegated_user') AND "
            "((execution_user_role IN ('admin', 'super_admin') "
            "AND execution_mode = 'unrestricted') OR "
            "(execution_user_role = 'member' "
            "AND execution_mode = 'sandbox')))",
            name="ck_tasks_execution_principal_policy",
        ),
        CheckConstraint(
            "(mode = 'delivery_loop' AND delivery_run_id IS NOT NULL "
            "AND delivery_role = 'developer') OR "
            "(mode <> 'delivery_loop' AND delivery_run_id IS NULL "
            "AND delivery_role IS NULL)",
            name="ck_tasks_delivery_owner_shape",
        ),
        CheckConstraint(
            "(worker_turn_handoff_id IS NULL "
            "AND worker_turn_handoff_worker_id IS NULL "
            "AND worker_turn_handoff_retry_count IS NULL "
            "AND worker_turn_handoff_from_generation IS NULL "
            "AND worker_turn_handoff_source_log_id IS NULL "
            "AND worker_turn_handoff_acknowledged IS NULL) OR "
            "(worker_turn_handoff_id IS NOT NULL "
            "AND worker_turn_handoff_worker_id IS NOT NULL "
            "AND worker_turn_handoff_retry_count IS NOT NULL "
            "AND worker_turn_handoff_retry_count >= 0 "
            "AND worker_turn_handoff_from_generation IS NOT NULL "
            "AND worker_turn_handoff_from_generation >= 0 "
            "AND worker_turn_handoff_source_log_id IS NOT NULL "
            "AND worker_turn_handoff_acknowledged IS NOT NULL)",
            name="ck_tasks_worker_turn_handoff_shape",
        ),
        UniqueConstraint(
            "incarnation_id",
            name="uq_tasks_incarnation_id",
        ),
        # Task ids are external security identities (HTTP resources, WS
        # channels, share tokens, Worker mirrors).  SQLite's default ROWID may
        # reuse the deleted highest id; AUTOINCREMENT permanently forbids that
        # cross-incarnation aliasing.  Other dialects ignore this table option.
        {"sqlite_autoincrement": True},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Durable random incarnation used whenever an external integer Task id is
    # observed before a lock and acted on afterwards.  Nullable keeps legacy
    # rows readable; every newly constructed Task receives a token.
    incarnation_id: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        default=lambda: secrets.token_hex(16),
    )
    title: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    description: Mapped[str | None] = mapped_column(Text, nullable=True)  # nullable for loop tasks
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    target_repo: Mapped[str] = mapped_column(String(500), nullable=True, default="")
    target_branch: Mapped[str] = mapped_column(String(100), default="main")
    result_branch: Mapped[str | None] = mapped_column(String(100), nullable=True)
    merge_status: Mapped[str] = mapped_column(String(20), default="pending")
    instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # 分布式 Worker：None = 本机执行，有值 = 转发到该 Worker（workers.id）
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # Team CCM: 谁创建的（users.id）
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # Durable authority for the next/active logical turn. These fields are
    # internal execution state, not caller-controlled Task configuration.
    # Ordinary local user turns freeze their authenticated principal here;
    # system-derived and cross-boundary Tasks remain sandboxed.
    execution_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    execution_user_role: Mapped[str] = mapped_column(
        String(20), nullable=False, default="member", server_default="member"
    )
    execution_mode: Mapped[str] = mapped_column(
        String(20), nullable=False, default="sandbox", server_default="sandbox"
    )
    execution_principal_kind: Mapped[str] = mapped_column(
        String(32), nullable=False, default="system", server_default="system"
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=2)
    mode: Mapped[str] = mapped_column(String(20), default="auto")  # auto/plan/loop/goal/delivery_loop
    # Monotonic identity of a logical model turn. Retry/account failover within
    # one turn keeps the same generation; a newly admitted turn increments it.
    turn_generation: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default="0",
    )
    # Internal pointer to the exact user/system source row that admitted the
    # current logical turn.  It is deliberately not a foreign key: Worker
    # mirrors and migration boundaries cannot assume node-local LogEntry ids.
    turn_source_log_id: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    # Manager-side durable reservation for exactly one Worker follow-up turn.
    # The remote Worker increments ``turn_generation`` only when its queued
    # message is dequeued, which can happen before the proxy HTTP response.
    # These fields authorize only the recorded G -> G+1 transition; they are
    # cleared after both the HTTP ACK and exact Worker generation evidence are
    # observed.  They are intentionally Manager-only and absent from public
    # Task schemas / Worker migration payloads.
    worker_turn_handoff_id: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    worker_turn_handoff_worker_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    worker_turn_handoff_retry_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    worker_turn_handoff_from_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    worker_turn_handoff_source_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    worker_turn_handoff_acknowledged: Mapped[bool | None] = mapped_column(
        nullable=True
    )
    # Frozen per-Task allow/budget policy for model-requested capabilities.
    # NULL means the capability surface is unavailable for this Task.
    capability_policy: Mapped[dict | None] = mapped_column(
        JSON(none_as_null=True),
        nullable=True,
    )
    # DeliveryRun is Manager authority and is not mirrored as a foreign key on
    # remote Workers.  The controller is the only writer of these routing
    # fields; ordinary Task APIs must not synthesize a delivery-owned Task.
    delivery_run_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )
    delivery_role: Mapped[str | None] = mapped_column(String(24), nullable=True)
    todo_file_path: Mapped[str | None] = mapped_column(String(500), nullable=True)  # loop only: path relative to target_repo
    loop_progress: Mapped[str | None] = mapped_column(String(200), nullable=True)  # loop only: e.g. "3/5", written by Claude
    max_iterations: Mapped[int] = mapped_column(Integer, default=50)  # loop only: max iterations before auto-abort
    must_complete: Mapped[bool] = mapped_column(default=False, server_default="0")  # loop only: reject done until all items finished
    goal_condition: Mapped[str | None] = mapped_column(Text, nullable=True)  # goal mode: natural-language completion condition
    goal_evaluator_model: Mapped[str | None] = mapped_column(String(100), nullable=True)  # goal mode: model for evaluator (default haiku)
    goal_max_turns: Mapped[int] = mapped_column(Integer, default=30)  # goal mode: max turns before auto-fail
    goal_turns_used: Mapped[int] = mapped_column(Integer, default=0)  # goal mode: turns completed so far
    goal_last_reason: Mapped[str | None] = mapped_column(Text, nullable=True)  # goal mode: evaluator's latest judgment reason
    pr_loop_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    pr_loop_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pr_loop_repo: Mapped[str | None] = mapped_column(String(200), nullable=True)
    pr_loop_state: Mapped[str | None] = mapped_column(String(30), nullable=True)
    pr_loop_max_turns: Mapped[int] = mapped_column(Integer, default=10)
    pr_loop_turns_used: Mapped[int] = mapped_column(Integer, default=0)
    pr_loop_poll_interval: Mapped[int] = mapped_column(Integer, default=60)
    plan_content: Mapped[str | None] = mapped_column(Text, nullable=True)  # Claude's proposed plan
    plan_approved: Mapped[bool | None] = mapped_column(default=None)  # None=pending, True=approved, False=rejected
    # Independent Plan Task relationship and application audit.  Always relate
    # through Task.id: the target's native session_id may change after
    # compaction, recovery, account rotation, or Worker migration.
    plan_target_task_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )
    plan_context_session_id: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    plan_context_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    # Bounded immutable transcript captured at Plan creation. This keeps a
    # Worker-side Planner independent from node-local LogEntry ids.
    plan_context_snapshot: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )
    plan_repo_revision: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    supersedes_plan_task_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )
    plan_approved_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    plan_approved_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plan_applied_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    plan_applied_to_session_id: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    plan_applied_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    plan_execution_task_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    # Versioned Planner/Reviewer primary+fallback routing snapshot. Generic
    # provider/model/effort mirror the Planner primary route for compatibility
    # with existing Task lists and Worker routing.
    plan_pipeline_config: Mapped[dict | None] = mapped_column(
        JSON, nullable=True
    )
    session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    # Exact Claude PTY background epoch.  A persistent session can
    # finish foreground turn A, start turn B, and only then deliver A's late
    # autonomous sentinel.  The token prevents that old sentinel from
    # terminalizing B (task_id/session_id alone are vulnerable to ABA).
    pty_background_generation: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    last_cwd: Mapped[str | None] = mapped_column(String(500), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider: Mapped[str] = mapped_column(String(20), default="claude", server_default="claude")
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    codex_service_tier: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="default",
        server_default="default",
    )
    effort_level: Mapped[str | None] = mapped_column(String(20), nullable=True)
    thinking_budget: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # NULL = 不注入；"append" = 追加；"replace" = 替换
    system_prompt_mode: Mapped[str | None] = mapped_column(String(20), nullable=True)
    # NULL = 全局默认超时；0 = 不限时；>0 = 指定小时数
    timeout_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    # 最近访问时间（打开 chat 时更新）——默认排序：最近访问优先
    last_accessed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # 手动拖拽排序键（NULL = 跟随最近访问排序；越大越靠前）
    sort_order: Mapped[float | None] = mapped_column(Float, nullable=True)
    enable_workflows: Mapped[bool] = mapped_column(default=False, server_default="0")
    enabled_skills: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    selected_user_skills: Mapped[list | None] = mapped_column(JSON, nullable=True)
    tags: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # User-authored reminder shown in the Task list and Chat header. Keep this
    # separate from ``tags`` because that JSON contains machine-owned markers
    # such as ``pr-review``.
    attention_tag: Mapped[str | None] = mapped_column(
        String(80), nullable=True
    )
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    context_window_usage: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    starred: Mapped[bool] = mapped_column(default=False, server_default="0", index=True)
    archived: Mapped[bool] = mapped_column(default=False, server_default="0", index=True)
    has_unread: Mapped[bool] = mapped_column(default=False, server_default="0")
    # Non-NULL = shadow task from a shared remote task
    shared_from_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    @property
    def background_active(self) -> bool:
        """Public boolean view; never expose the internal generation token."""

        return self.pty_background_generation is not None


def _configure_task_properties():
    from backend.models.delivery import DeliveryRun
    from backend.models.monitor_session import MonitorSession
    from backend.models.plan import PlanLegacyTaskLink
    from backend.models.plan_agent import PlanAgentRun, PlanAgentStep

    ms = MonitorSession.__table__
    legacy_plan_links = PlanLegacyTaskLink.__table__
    plan_runs = PlanAgentRun.__table__
    plan_steps = PlanAgentStep.__table__
    delivery_runs = DeliveryRun.__table__
    # Always show real running sub-agent count — background agents can
    # outlive the main turn, so even completed tasks may have active sub-agents.
    Task.active_sub_agents = column_property(
        select(func.count(ms.c.id))
        .where(ms.c.task_id == Task.id, ms.c.status == "running")
        .correlate(Task.__table__)
        .scalar_subquery()
    )
    # A migrated Plan Task remains a real, visible Task history row. Expose its
    # canonical Plan destination so clients can navigate to the new aggregate
    # without treating the legacy Task as the writable Plan authority.
    Task.canonical_plan_id = column_property(
        select(legacy_plan_links.c.plan_id)
        .where(legacy_plan_links.c.legacy_task_id == Task.id)
        .limit(1)
        .correlate(Task.__table__)
        .scalar_subquery()
    )
    # PlanAgentRun is the durable authority for the currently executing
    # Planner/Reviewer phase. Keep this as a read-only projection rather than
    # duplicating mutable pipeline state on Task.
    Task.plan_stage = column_property(
        select(plan_runs.c.status)
        .where(plan_runs.c.plan_task_id == Task.id)
        .order_by(plan_runs.c.id.desc())
        .limit(1)
        .correlate(Task.__table__)
        .scalar_subquery()
    )
    Task.plan_stage_round = column_property(
        select(plan_runs.c.round)
        .where(plan_runs.c.plan_task_id == Task.id)
        .order_by(plan_runs.c.id.desc())
        .limit(1)
        .correlate(Task.__table__)
        .scalar_subquery()
    )

    latest_plan_run_id = (
        select(plan_runs.c.id)
        .where(plan_runs.c.plan_task_id == Task.id)
        .order_by(plan_runs.c.id.desc())
        .limit(1)
        .correlate(Task.__table__)
        .scalar_subquery()
    )

    def latest_plan_step_value(column):
        return (
            select(column)
            .where(plan_steps.c.run_id == latest_plan_run_id)
            .order_by(plan_steps.c.id.desc())
            .limit(1)
            .correlate(Task.__table__)
            .scalar_subquery()
        )

    # The latest step records the concrete route being attempted, including a
    # fallback route. These projections let task lists show what is actually
    # running without issuing one Plan-runs request per card.
    Task.plan_stage_provider = column_property(
        latest_plan_step_value(plan_steps.c.provider)
    )
    Task.plan_stage_model = column_property(
        latest_plan_step_value(plan_steps.c.model)
    )
    Task.plan_stage_effort = column_property(
        latest_plan_step_value(plan_steps.c.effort)
    )
    Task.plan_stage_route_slot = column_property(
        latest_plan_step_value(plan_steps.c.route_slot)
    )
    Task.delivery_phase = column_property(
        select(delivery_runs.c.phase)
        .where(delivery_runs.c.id == Task.delivery_run_id)
        .limit(1)
        .correlate(Task.__table__)
        .scalar_subquery()
    )
    Task.delivery_activity = column_property(
        select(delivery_runs.c.activity)
        .where(delivery_runs.c.id == Task.delivery_run_id)
        .limit(1)
        .correlate(Task.__table__)
        .scalar_subquery()
    )
    Task.delivery_outcome = column_property(
        select(delivery_runs.c.outcome)
        .where(delivery_runs.c.id == Task.delivery_run_id)
        .limit(1)
        .correlate(Task.__table__)
        .scalar_subquery()
    )
    Task.delivery_terminal = column_property(
        select(delivery_runs.c.policy_snapshot["terminal"].as_string())
        .where(delivery_runs.c.id == Task.delivery_run_id)
        .limit(1)
        .correlate(Task.__table__)
        .scalar_subquery()
    )

_configure_task_properties()
