"""add durable Worker Plan dispatch receipts

Revision ID: a6e4c2d9f810
Revises: 8d2f5b7a1c90
Create Date: 2026-08-08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a6e4c2d9f810"
down_revision: Union[str, None] = "8d2f5b7a1c90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _digest_sql(column: str) -> str:
    """Return a CHECK expression portable across all supported databases."""

    stripped = column
    for character in "0123456789abcdef":
        stripped = f"replace({stripped}, '{character}', '')"
    return f"(length({column}) = 64 AND {stripped} = '')"


_DIGEST_SQL = _digest_sql("payload_digest")
_SETTLED_SHAPE_SQL = (
    "status = 'settled' AND settled_at IS NOT NULL AND ("
    "(settlement_reason IN ('not_launched', 'preflight_failed') AND "
    "payload_digest IS NULL AND remote_status IS NULL) OR "
    "(settlement_reason = 'remote_cancelled' AND "
    f"(payload_digest IS NULL OR {_DIGEST_SQL}) AND "
    "remote_status = 'cancelled') OR "
    "(settlement_reason = 'remote_pause' AND "
    f"{_DIGEST_SQL} AND remote_status IN "
    "('waiting_user', 'completed', 'failed', 'cancelled')) OR "
    "(settlement_reason = 'remote_absent' AND "
    f"{_DIGEST_SQL} AND remote_status IS NULL) OR "
    "(settlement_reason = 'identity_conflict' AND "
    f"{_DIGEST_SQL} AND remote_status = 'conflict'))"
    " OR (status = 'settled' AND settled_at IS NOT NULL AND "
    "settlement_reason = 'legacy_terminal' AND payload_digest IS NULL AND "
    "remote_status IN ('completed', 'failed', 'cancelled'))"
)
_STATE_SHAPE_SQL = (
    "(status = 'prepared' AND payload_digest IS NULL AND "
    "remote_status IS NULL AND settlement_reason IS NULL AND "
    "settled_at IS NULL) OR "
    f"(status = 'remote_possible' AND {_DIGEST_SQL} AND "
    "remote_status IS NULL AND settlement_reason IS NULL AND "
    "settled_at IS NULL) OR "
    f"({_SETTLED_SHAPE_SQL})"
)


def upgrade() -> None:
    bind = op.get_bind()
    unsafe_runs = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM plan_agent_runs AS run "
            "LEFT JOIN plans AS plan ON plan.id = run.plan_id "
            "WHERE run.worker_id IS NOT NULL AND ("
            "run.id <= 0 OR run.plan_id IS NULL OR plan.id IS NULL OR "
            "plan.worker_id IS NULL OR plan.worker_id != run.worker_id OR "
            "run.generation < 0 OR "
            "run.status NOT IN ('completed', 'failed', 'cancelled') OR "
            "run.finished_at IS NULL OR run.instance_id IS NOT NULL OR "
            "run.last_execution_started_at IS NOT NULL OR "
            "plan.active_run_id = run.id)"
        )
    ).scalar_one()
    unsafe_steps = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM plan_agent_steps AS step "
            "JOIN plan_agent_runs AS run ON run.id = step.run_id "
            "WHERE run.worker_id IS NOT NULL AND ("
            "step.id <= 0 OR step.plan_id IS NULL OR "
            "step.plan_id != run.plan_id OR step.worker_id IS NULL OR "
            "step.worker_id != run.worker_id OR step.worker_step_id IS NULL OR "
            "step.worker_step_id <= 0 OR step.generation < 0 OR "
            "step.provider NOT IN ('claude', 'codex') OR "
            "step.status NOT IN ('completed', 'failed', 'cancelled') OR "
            "step.finished_at IS NULL)"
        )
    ).scalar_one()
    contradictory_runtime = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM plan_agent_runtime_receipts AS receipt "
            "JOIN plan_agent_runs AS run ON run.id = receipt.run_id "
            "WHERE run.worker_id IS NOT NULL"
        )
    ).scalar_one()
    if unsafe_runs or unsafe_steps or contradictory_runtime:
        raise RuntimeError(
            "Cannot upgrade while legacy Worker Plan mirrors are active, "
            "malformed, or carry contradictory Manager runtime evidence"
        )

    dispatch_table = op.create_table(
        "plan_agent_worker_dispatch_receipts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("target_task_id", sa.Integer(), nullable=True),
        sa.Column("worker_id", sa.Integer(), nullable=False),
        sa.Column("run_generation", sa.Integer(), nullable=False),
        sa.Column("protocol", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default="prepared",
        ),
        sa.Column("payload_digest", sa.String(length=64), nullable=True),
        sa.Column("remote_status", sa.String(length=30), nullable=True),
        sa.Column("settlement_reason", sa.String(length=50), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("settled_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('prepared', 'remote_possible', 'settled')",
            name="ck_plan_worker_dispatch_status",
        ),
        sa.CheckConstraint(
            "protocol = 1",
            name="ck_plan_worker_dispatch_protocol",
        ),
        sa.CheckConstraint(
            _STATE_SHAPE_SQL,
            name="ck_plan_worker_dispatch_state_shape",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "run_generation",
            name="uq_plan_worker_dispatch_run_generation",
        ),
    )
    op.create_index(
        "ix_plan_agent_worker_dispatch_receipts_plan_id",
        "plan_agent_worker_dispatch_receipts",
        ["plan_id"],
        unique=False,
    )
    op.create_index(
        "ix_plan_agent_worker_dispatch_receipts_run_id",
        "plan_agent_worker_dispatch_receipts",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ix_plan_agent_worker_dispatch_receipts_target_task_id",
        "plan_agent_worker_dispatch_receipts",
        ["target_task_id"],
        unique=False,
    )
    op.create_index(
        "ix_plan_agent_worker_dispatch_receipts_worker_id",
        "plan_agent_worker_dispatch_receipts",
        ["worker_id"],
        unique=False,
    )
    op.create_index(
        "ix_plan_worker_dispatch_worker_status",
        "plan_agent_worker_dispatch_receipts",
        ["worker_id", "status"],
        unique=False,
    )

    # Pre-receipt terminal Manager mirrors cannot supply the historical HTTP
    # payload digest which did not exist at the time. Preserve that fact as a
    # dedicated migration-only terminal proof instead of fabricating an exact
    # dispatch boundary. The authoritative Worker DELETE still performs its
    # own local runtime-receipt preflight before any Manager graph is removed.
    plans = sa.table(
        "plans",
        sa.column("id", sa.Integer()),
        sa.column("target_task_id", sa.Integer()),
        sa.column("worker_id", sa.Integer()),
    )
    runs = sa.table(
        "plan_agent_runs",
        sa.column("id", sa.Integer()),
        sa.column("plan_id", sa.Integer()),
        sa.column("worker_id", sa.Integer()),
        sa.column("generation", sa.Integer()),
        sa.column("status", sa.String(length=30)),
        sa.column("created_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
        sa.column("finished_at", sa.DateTime()),
    )
    legacy_rows = bind.execute(
        sa.select(
            plans.c.id.label("plan_id"),
            runs.c.id.label("run_id"),
            plans.c.target_task_id,
            runs.c.worker_id,
            runs.c.generation.label("run_generation"),
            runs.c.status.label("remote_status"),
            runs.c.created_at,
            runs.c.updated_at,
            runs.c.finished_at.label("settled_at"),
        )
        .select_from(runs.join(plans, plans.c.id == runs.c.plan_id))
        .where(runs.c.worker_id.is_not(None))
        .order_by(runs.c.id)
    ).mappings()
    batch_rows: list[dict] = []
    for row in legacy_rows:
        batch_rows.append(
            {
                "plan_id": row["plan_id"],
                "run_id": row["run_id"],
                "target_task_id": row["target_task_id"],
                "worker_id": row["worker_id"],
                "run_generation": row["run_generation"],
                "protocol": 1,
                "status": "settled",
                "payload_digest": None,
                "remote_status": row["remote_status"],
                "settlement_reason": "legacy_terminal",
                "last_error": None,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "settled_at": row["settled_at"],
            }
        )
        if len(batch_rows) == 1000:
            bind.execute(dispatch_table.insert(), batch_rows)
            batch_rows = []
    if batch_rows:
        bind.execute(dispatch_table.insert(), batch_rows)


def downgrade() -> None:
    bind = op.get_bind()
    # Service semantics make ``settled`` the only terminal/deletable state.
    # Match its full state shape here instead of checking the status label
    # alone, so a corrupted row cannot make a downgrade discard the only
    # durable proof of whether a remote import may have happened.
    unsafe_receipts = bind.execute(
        sa.text(
            "SELECT COALESCE(SUM(CASE WHEN ("
            f"{_SETTLED_SHAPE_SQL}"
            ") THEN 0 ELSE 1 END), 0) "
            "FROM plan_agent_worker_dispatch_receipts"
        )
    ).scalar_one()
    if unsafe_receipts:
        raise RuntimeError(
            "Cannot downgrade while non-settled or malformed durable Worker "
            "Plan dispatch receipts exist"
        )
    op.drop_index(
        "ix_plan_worker_dispatch_worker_status",
        table_name="plan_agent_worker_dispatch_receipts",
    )
    op.drop_index(
        "ix_plan_agent_worker_dispatch_receipts_worker_id",
        table_name="plan_agent_worker_dispatch_receipts",
    )
    op.drop_index(
        "ix_plan_agent_worker_dispatch_receipts_target_task_id",
        table_name="plan_agent_worker_dispatch_receipts",
    )
    op.drop_index(
        "ix_plan_agent_worker_dispatch_receipts_run_id",
        table_name="plan_agent_worker_dispatch_receipts",
    )
    op.drop_index(
        "ix_plan_agent_worker_dispatch_receipts_plan_id",
        table_name="plan_agent_worker_dispatch_receipts",
    )
    op.drop_table("plan_agent_worker_dispatch_receipts")
