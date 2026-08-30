"""add durable Plan Agent runtime receipts

Revision ID: 8d2f5b7a1c90
Revises: 7c1e4a9d2f60
Create Date: 2026-08-08
"""

import hashlib
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "8d2f5b7a1c90"
down_revision: Union[str, None] = "7c1e4a9d2f60"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _lower_hex_sql(column: str, length: int) -> str:
    stripped = column
    for character in "0123456789abcdef":
        stripped = f"replace({stripped}, '{character}', '')"
    return f"(length({column}) = {length} AND {stripped} = '')"


def _boot_id_sql(column: str) -> str:
    stripped = column
    for character in "0123456789abcdef":
        stripped = f"replace({stripped}, '{character}', '')"
    return (
        f"(length({column}) = 36 AND substr({column}, 9, 1) = '-' AND "
        f"substr({column}, 14, 1) = '-' AND substr({column}, 19, 1) = '-' AND "
        f"substr({column}, 24, 1) = '-' AND {stripped} = '----')"
    )


_TOKEN_SQL = _lower_hex_sql("runtime_token", 32)
_PREPARED_BOOT_SQL = _boot_id_sql("prepared_boot_id")
_PROCESS_BOOT_SQL = _boot_id_sql("boot_id")
_PROCESS_EMPTY_SQL = (
    "process_id IS NULL AND process_group_id IS NULL AND "
    "process_start_ticks IS NULL AND process_uid IS NULL AND boot_id IS NULL"
)
_PROCESS_COMPLETE_SQL = (
    "process_id IS NOT NULL AND process_group_id IS NOT NULL AND "
    "process_start_ticks IS NOT NULL AND process_uid IS NOT NULL AND "
    "boot_id IS NOT NULL AND process_id > 1 AND process_group_id > 1 AND "
    "process_start_ticks >= 0 AND process_uid >= 0 AND "
    "process_uid = prepared_uid AND boot_id = prepared_boot_id "
    f"AND {_PROCESS_BOOT_SQL}"
)
_PROCESS_SHAPE_SQL = f"(({_PROCESS_EMPTY_SQL}) OR ({_PROCESS_COMPLETE_SQL}))"
_CODEX_EMPTY_SQL = "codex_home IS NULL AND codex_thread_id IS NULL"
_CODEX_COMPLETE_SQL = (
    "codex_home IS NOT NULL AND length(trim(codex_home)) > 0 AND "
    "codex_thread_id IS NOT NULL AND length(trim(codex_thread_id)) > 0"
)
_PROVIDER_IDENTITY_SQL = (
    f"((provider = 'claude' AND ({_CODEX_EMPTY_SQL})) OR "
    f"(provider = 'codex' AND (({_CODEX_EMPTY_SQL}) OR "
    f"({_CODEX_COMPLETE_SQL})) AND "
    f"(({_PROCESS_EMPTY_SQL}) OR ({_CODEX_COMPLETE_SQL}))))"
)
_SCALAR_SHAPE_SQL = (
    "run_id > 0 AND step_id > 0 AND run_generation >= 0 AND "
    "attempt_index >= 1 AND provider IN ('claude', 'codex') AND "
    f"{_TOKEN_SQL} AND {_PREPARED_BOOT_SQL} AND "
    "prepared_start_ticks >= 0 AND prepared_uid >= 0"
)
_STATE_SHAPE_SQL = (
    "((status IN ('prepared', 'admitting') AND "
    f"({_PROCESS_EMPTY_SQL}) AND ({_CODEX_EMPTY_SQL}) AND "
    "cleanup_error IS NULL AND cleaned_at IS NULL) OR "
    "(status = 'launching' AND cleanup_error IS NULL AND cleaned_at IS NULL AND "
    f"((provider = 'claude' AND ({_PROCESS_COMPLETE_SQL}) AND "
    f"({_CODEX_EMPTY_SQL})) OR "
    f"(provider = 'codex' AND ({_CODEX_COMPLETE_SQL}) AND "
    f"({_PROCESS_SHAPE_SQL})))) OR "
    "(status = 'cleaned' AND cleaned_at IS NOT NULL AND cleanup_error IS NULL AND "
    f"({_PROVIDER_IDENTITY_SQL})) OR "
    "(status = 'cleanup_failed' AND cleaned_at IS NULL AND "
    "cleanup_error IS NOT NULL AND length(trim(cleanup_error)) > 0 AND "
    f"({_PROVIDER_IDENTITY_SQL})))"
)
_CLEANED_SHAPE_SQL = (
    f"({_SCALAR_SHAPE_SQL}) AND ({_PROCESS_SHAPE_SQL}) AND "
    "status = 'cleaned' AND cleaned_at IS NOT NULL AND cleanup_error IS NULL AND "
    f"({_PROVIDER_IDENTITY_SQL})"
)
_RUN_CANCELLATION_SHAPE_SQL = (
    "(cancellation_target_generation IS NULL AND status != 'cancelling') OR "
    "(cancellation_target_generation IS NOT NULL AND "
    "cancellation_target_generation >= 0 AND "
    "generation = cancellation_target_generation + 1 AND "
    "status IN ('cancelling', 'cancelled'))"
)
_LEGACY_CLEAN_BOOT_ID = "00000000-0000-0000-0000-000000000000"


def _legacy_runtime_token(step_id: int) -> str:
    return hashlib.sha256(
        f"ccm-plan-runtime-legacy:{step_id}".encode("utf-8")
    ).hexdigest()[:32]


def upgrade() -> None:
    bind = op.get_bind()
    active_cancellations = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM plan_agent_runs WHERE status = 'cancelling'"
        )
    ).scalar_one()
    if active_cancellations:
        raise RuntimeError(
            "Cannot upgrade while legacy Plan Runs are actively cancelling "
            "without an exact runtime generation"
        )
    unsafe_worker_runs = bind.execute(
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
    if unsafe_worker_runs:
        # A pre-dispatch-receipt Manager mirror cannot be resumed or cancelled
        # safely after restart: there is no durable proof whether its mutating
        # Worker import crossed the network boundary. Require operators to
        # converge such Runs before upgrading instead of inventing history.
        raise RuntimeError(
            "Cannot upgrade while active or malformed legacy Worker Plan "
            "Runs lack durable dispatch identity"
        )
    unsafe_legacy_steps = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM plan_agent_steps AS step "
            "LEFT JOIN plan_agent_runs AS run ON run.id = step.run_id "
            "WHERE run.id IS NULL OR step.id <= 0 OR step.run_id <= 0 OR "
            "step.generation < 0 OR step.provider NOT IN ('claude', 'codex') OR "
            "(run.worker_id IS NULL AND (step.worker_id IS NOT NULL OR "
            "step.worker_step_id IS NOT NULL)) OR "
            "(run.worker_id IS NOT NULL AND (step.worker_id IS NULL OR "
            "step.worker_step_id IS NULL OR step.worker_step_id <= 0 OR "
            "step.worker_id != run.worker_id)) OR "
            "step.status NOT IN ('completed', 'failed', 'cancelled') OR "
            "step.finished_at IS NULL"
        )
    ).scalar_one()
    if unsafe_legacy_steps:
        # A pre-receipt running Step may still have a live provider process.
        # Inventing a token after the fact would make restart reconciliation
        # falsely prove absence and could launch the same Step twice.
        raise RuntimeError(
            "Cannot upgrade while active or malformed legacy Plan Steps lack "
            "durable runtime identity"
        )

    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.add_column(
            sa.Column(
                "cancellation_target_generation",
                sa.Integer(),
                nullable=True,
            )
        )
        batch.create_check_constraint(
            "ck_plan_agent_run_cancellation_generation",
            _RUN_CANCELLATION_SHAPE_SQL,
        )

    op.create_table(
        "plan_agent_runtime_receipts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("step_id", sa.Integer(), nullable=False),
        sa.Column("run_generation", sa.Integer(), nullable=False),
        sa.Column("attempt_index", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("runtime_token", sa.String(length=32), nullable=False),
        sa.Column("prepared_boot_id", sa.String(length=64), nullable=False),
        sa.Column("prepared_start_ticks", sa.BigInteger(), nullable=False),
        sa.Column("prepared_uid", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default="prepared",
        ),
        sa.Column("process_id", sa.Integer(), nullable=True),
        sa.Column("process_group_id", sa.Integer(), nullable=True),
        sa.Column("process_start_ticks", sa.BigInteger(), nullable=True),
        sa.Column("process_uid", sa.Integer(), nullable=True),
        sa.Column("boot_id", sa.String(length=64), nullable=True),
        sa.Column("codex_home", sa.Text(), nullable=True),
        sa.Column("codex_thread_id", sa.String(length=200), nullable=True),
        sa.Column("cleanup_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("cleaned_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('prepared', 'admitting', 'launching', 'cleaned', "
            "'cleanup_failed')",
            name="ck_plan_runtime_receipt_status",
        ),
        sa.CheckConstraint(
            _SCALAR_SHAPE_SQL,
            name="ck_plan_runtime_receipt_scalar_shape",
        ),
        sa.CheckConstraint(
            _PROCESS_SHAPE_SQL,
            name="ck_plan_runtime_receipt_process_identity",
        ),
        sa.CheckConstraint(
            _STATE_SHAPE_SQL,
            name="ck_plan_runtime_receipt_state_shape",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "step_id",
            "attempt_index",
            name="uq_plan_runtime_receipt_step_attempt",
        ),
        sa.UniqueConstraint(
            "runtime_token",
            name="uq_plan_runtime_receipt_token",
        ),
    )
    op.create_index(
        "ix_plan_agent_runtime_receipts_run_id",
        "plan_agent_runtime_receipts",
        ["run_id"],
        unique=False,
    )

    steps = sa.table(
        "plan_agent_steps",
        sa.column("id", sa.Integer()),
        sa.column("run_id", sa.Integer()),
        sa.column("worker_id", sa.Integer()),
        sa.column("worker_step_id", sa.Integer()),
        sa.column("generation", sa.Integer()),
        sa.column("provider", sa.String(length=20)),
        sa.column("started_at", sa.DateTime()),
        sa.column("finished_at", sa.DateTime()),
    )
    receipts = sa.table(
        "plan_agent_runtime_receipts",
        sa.column("run_id", sa.Integer()),
        sa.column("step_id", sa.Integer()),
        sa.column("run_generation", sa.Integer()),
        sa.column("attempt_index", sa.Integer()),
        sa.column("provider", sa.String(length=20)),
        sa.column("runtime_token", sa.String(length=32)),
        sa.column("prepared_boot_id", sa.String(length=64)),
        sa.column("prepared_start_ticks", sa.BigInteger()),
        sa.column("prepared_uid", sa.Integer()),
        sa.column("status", sa.String(length=20)),
        sa.column("created_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
        sa.column("cleaned_at", sa.DateTime()),
    )
    legacy_steps = bind.execute(
        sa.select(
            steps.c.id,
            steps.c.run_id,
            steps.c.generation,
            steps.c.provider,
            steps.c.started_at,
            steps.c.finished_at,
        )
        .where(
            steps.c.worker_id.is_(None),
            steps.c.worker_step_id.is_(None),
        )
        .order_by(steps.c.id)
    ).mappings()
    batch_rows: list[dict] = []
    for step in legacy_steps:
        cleaned_at = step["finished_at"]
        batch_rows.append(
            {
                "run_id": step["run_id"],
                "step_id": step["id"],
                "run_generation": step["generation"],
                "attempt_index": 1,
                "provider": step["provider"],
                "runtime_token": _legacy_runtime_token(step["id"]),
                "prepared_boot_id": _LEGACY_CLEAN_BOOT_ID,
                "prepared_start_ticks": 0,
                "prepared_uid": 0,
                "status": "cleaned",
                "created_at": step["started_at"] or cleaned_at,
                "updated_at": cleaned_at,
                "cleaned_at": cleaned_at,
            }
        )
        if len(batch_rows) == 1000:
            bind.execute(receipts.insert(), batch_rows)
            batch_rows = []
    if batch_rows:
        bind.execute(receipts.insert(), batch_rows)
    op.create_index(
        "ix_plan_agent_runtime_receipts_step_id",
        "plan_agent_runtime_receipts",
        ["step_id"],
        unique=False,
    )
    op.create_index(
        "ix_plan_runtime_receipt_run_status",
        "plan_agent_runtime_receipts",
        ["run_id", "status"],
        unique=False,
    )


def downgrade() -> None:
    bind = op.get_bind()
    unsafe_receipts = bind.execute(
        sa.text(
            "SELECT COALESCE(SUM(CASE WHEN ("
            f"{_CLEANED_SHAPE_SQL}"
            ") THEN 0 ELSE 1 END), 0) "
            "FROM plan_agent_runtime_receipts"
        )
    ).scalar_one()
    malformed_graph = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM plan_agent_runtime_receipts AS receipt "
            "LEFT JOIN plan_agent_steps AS step ON step.id = receipt.step_id "
            "LEFT JOIN plan_agent_runs AS run ON run.id = receipt.run_id "
            "WHERE step.id IS NULL OR run.id IS NULL OR "
            "receipt.run_id != step.run_id OR "
            "receipt.run_generation != step.generation OR "
            "receipt.provider != step.provider"
        )
    ).scalar_one()
    malformed_attempts = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM ("
            "SELECT step_id FROM plan_agent_runtime_receipts GROUP BY step_id "
            "HAVING MIN(attempt_index) != 1 OR COUNT(*) != MAX(attempt_index)"
            ") AS malformed_runtime_attempts"
        )
    ).scalar_one()
    missing_receipts = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM plan_agent_steps AS step "
            "LEFT JOIN plan_agent_runtime_receipts AS receipt "
            "ON receipt.step_id = step.id WHERE receipt.id IS NULL AND "
            "step.worker_id IS NULL AND step.worker_step_id IS NULL"
        )
    ).scalar_one()
    active_cancellations = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM plan_agent_runs WHERE status = 'cancelling' "
            "OR cancellation_target_generation IS NOT NULL"
        )
    ).scalar_one()
    if (
        unsafe_receipts
        or malformed_graph
        or malformed_attempts
        or missing_receipts
        or active_cancellations
    ):
        raise RuntimeError(
            "Cannot downgrade while non-clean, malformed, or actively cancelling "
            "durable Plan runtime evidence exists"
        )
    op.drop_index(
        "ix_plan_runtime_receipt_run_status",
        table_name="plan_agent_runtime_receipts",
    )
    op.drop_index(
        "ix_plan_agent_runtime_receipts_step_id",
        table_name="plan_agent_runtime_receipts",
    )
    op.drop_index(
        "ix_plan_agent_runtime_receipts_run_id",
        table_name="plan_agent_runtime_receipts",
    )
    op.drop_table("plan_agent_runtime_receipts")
    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.drop_constraint(
            "ck_plan_agent_run_cancellation_generation",
            type_="check",
        )
        batch.drop_column("cancellation_target_generation")
