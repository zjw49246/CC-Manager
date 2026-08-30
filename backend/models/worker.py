from datetime import datetime
from sqlalchemy import CheckConstraint, Integer, String, Text, DateTime, JSON, event
from sqlalchemy.orm import Mapped, mapped_column
from backend.database import Base


class Worker(Base):
    """分布式 Worker：一台跑完整 CCM 的 EC2，由 Manager 全生命周期管理。

    设计文档见 docs/plans/elastic-worker-design.md。
    status 状态机:
      creating → bootstrapping → ready ⇄ (stopping → stopped → starting)
      ready ⇄ error（健康检查自动降级/恢复）
      任意 → destroying → terminated
    """

    __tablename__ = "workers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)  # "{manager主机名}-worker-{id}"
    status: Mapped[str] = mapped_column(String(20), default="creating", server_default="creating")

    # Team CCM: Worker 分配给哪个用户（NULL = 公共池）
    owner_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    max_tasks: Mapped[int] = mapped_column(Integer, default=8, server_default="8")

    # 云实例信息
    cloud_instance_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    private_ip: Mapped[str | None] = mapped_column(String(45), nullable=True)  # 主通信地址（VPC 内网）
    public_ip: Mapped[str | None] = mapped_column(String(45), nullable=True)  # 仅记录，不用于通信
    # RunInstances request journal (name + non-secret overrides).  Persisted
    # before the call so a lost AWS response can be retried with an identical
    # ClientToken *and* identical parameters.
    provision_spec: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # Monotonic journal for the externally visible cloud ``Name`` tag.  The
    # database name and this outbox are committed together before create_tags
    # is called; ``rename_generation`` is never decremented, while the outbox
    # is cleared only after the exact generation has been acknowledged.
    rename_generation: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    rename_tag_outbox: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # 连接信息
    ssh_user: Mapped[str] = mapped_column(String(50), default="ubuntu", server_default="ubuntu")
    ssh_key_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    ccm_port: Mapped[int] = mapped_column(Integer, default=8000, server_default="8000")
    auth_token: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ccm_commit: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 版本锁定校验

    # 账号信息（在 Worker 本机登录；凭据留存供 bootstrap retry，API 响应严格脱敏）
    # provider/email/token/password/login_method/status；历史无 provider = claude。
    accounts: Mapped[list | None] = mapped_column(JSON, default=list)

    # Project ID 映射（manager_project_id → worker_project_id）
    project_mapping: Mapped[dict | None] = mapped_column(JSON, default=dict)

    # 健康监控 / bootstrap 进度
    last_heartbeat: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    bootstrap_step: Mapped[str | None] = mapped_column(String(100), nullable=True)
    bootstrap_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    bootstrap_log: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Stable identity for one Manager-side destroy lifecycle.  Unlike
    # ``updated_at`` this changes only when a fresh destroy is admitted, so
    # harmless metadata/log writers cannot revoke an in-flight coordinator.
    # ``ready|error`` + bootstrap_step="destroy" deliberately retains it for
    # restart/reconciliation retry of the same irreversible node drain.
    destroy_lifecycle_nonce: Mapped[str | None] = mapped_column(
        String(32), nullable=True,
    )
    # Short-lived durable authorization outbox installed only after the final
    # signed Worker drain proof and every Manager ownership fence succeed.
    # It survives a crash or ambiguous cloud response so restart recovery can
    # idempotently retry EC2 termination without contacting the sealed/dead
    # Worker again.  Successful terminalization clears it atomically with the
    # Worker credential scrub.
    destroy_termination_receipt: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


WORKER_NODE_CONTROL_SINGLETON_ID = 1


class WorkerNodeControl(Base):
    """Worker-local durable admission fence for node destruction.

    The row exists on Manager and Worker databases so one schema can be
    deployed everywhere, but it is consulted only when ``CCM_NODE_ROLE`` is
    ``worker``.  Once ``drain_claim`` is installed it is deliberately
    irreversible: a failed destroy/restarted Manager may retry the same claim,
    while no Task/runtime/login mutation can reopen the node before the cloud
    instance is finally terminated.
    """

    __tablename__ = "worker_node_controls"
    __table_args__ = (
        CheckConstraint(
            "id = 1",
            name="ck_worker_node_controls_singleton",
        ),
        CheckConstraint(
            "(drain_claim IS NULL AND drain_started_at IS NULL "
            "AND runtime_seal_claim IS NULL AND runtime_sealed_at IS NULL) "
            "OR (drain_claim IS NOT NULL AND drain_started_at IS NOT NULL "
            "AND ((runtime_seal_claim IS NULL AND runtime_sealed_at IS NULL) "
            "OR (runtime_seal_claim = drain_claim "
            "AND runtime_sealed_at IS NOT NULL)))",
            name="ck_worker_node_controls_drain_phase",
        ),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=False,
        default=WORKER_NODE_CONTROL_SINGLETON_ID,
    )
    drain_claim: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Phase one (drain_claim) rejects every new Task/runtime ownership while
    # callbacks from the already-admitted exact generation may still persist
    # their final output.  Phase two is installed only after those consumers
    # have stopped; it makes every later runtime callback fail closed before
    # Manager log backfill and the final cloud-termination proof.
    runtime_seal_claim: Mapped[str | None] = mapped_column(
        String(64), nullable=True,
    )
    # Codex login is a background process whose HTTP request returns before
    # credential mutation is complete.  Persist its exact attempt identity so
    # a node drain cannot cross that background effect or a crash-left journal.
    active_login_attempt_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True,
    )
    active_login_kind: Mapped[str | None] = mapped_column(
        String(32), nullable=True,
    )
    drain_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True,
    )
    runtime_sealed_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True,
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


@event.listens_for(WorkerNodeControl.__table__, "after_create")
def _seed_worker_node_control(target, connection, **_kwargs) -> None:
    """Seed metadata-created test/dev databases with the singleton row."""

    connection.execute(
        target.insert().values(
            id=WORKER_NODE_CONTROL_SINGLETON_ID,
            drain_claim=None,
            runtime_seal_claim=None,
            active_login_attempt_id=None,
            active_login_kind=None,
            drain_started_at=None,
            runtime_sealed_at=None,
            updated_at=None,
        )
    )
