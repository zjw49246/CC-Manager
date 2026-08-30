"""add durable Test Harness sandbox leases

Revision ID: c8f1a2d4e6b9
Revises: 9f2c6b4d8a10
Create Date: 2026-08-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c8f1a2d4e6b9"
down_revision: Union[str, None] = "9f2c6b4d8a10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "test_harness_sandbox_leases",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("run_id", sa.String(length=32), nullable=False),
        sa.Column("backend", sa.String(length=24), nullable=False),
        sa.Column("lease_nonce", sa.String(length=48), nullable=False),
        sa.Column("image_ref", sa.String(length=500), nullable=False),
        sa.Column("image_digest", sa.String(length=128), nullable=True),
        sa.Column("resource_id", sa.String(length=128), nullable=True),
        sa.Column("resource_name", sa.String(length=128), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("phase", sa.String(length=48), nullable=False),
        sa.Column("runtime_metadata", sa.JSON(), nullable=False),
        sa.Column("cleanup_status", sa.String(length=24), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("cleanup_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lease_nonce"),
        sa.UniqueConstraint("run_id"),
        mysql_engine="InnoDB",
    )
    for column in ("run_id", "resource_id", "status"):
        op.create_index(
            f"ix_test_harness_sandbox_leases_{column}",
            "test_harness_sandbox_leases",
            [column],
        )


def downgrade() -> None:
    for column in ("status", "resource_id", "run_id"):
        op.drop_index(
            f"ix_test_harness_sandbox_leases_{column}",
            table_name="test_harness_sandbox_leases",
        )
    op.drop_table("test_harness_sandbox_leases")
