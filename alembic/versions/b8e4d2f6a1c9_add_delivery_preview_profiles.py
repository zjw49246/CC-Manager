"""add Delivery multi-Preview profile snapshots

Revision ID: b8e4d2f6a1c9
Revises: a7d4e9c2f610
Create Date: 2026-08-15
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b8e4d2f6a1c9"
down_revision: Union[str, None] = "a7d4e9c2f610"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("delivery_cycles", schema=None) as batch:
        batch.add_column(
            sa.Column("frontend_review_config_snapshot", sa.JSON(), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "frontend_review_profile_ids",
                sa.JSON(),
                nullable=False,
                server_default="[]",
            )
        )
        batch.add_column(
            sa.Column(
                "frontend_review_profile_index",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(
            sa.Column(
                "frontend_review_results",
                sa.JSON(),
                nullable=False,
                server_default="[]",
            )
        )
        batch.create_check_constraint(
            "ck_delivery_cycles_frontend_profile_index",
            "frontend_review_profile_index >= 0",
        )


def downgrade() -> None:
    with op.batch_alter_table("delivery_cycles", schema=None) as batch:
        batch.drop_constraint(
            "ck_delivery_cycles_frontend_profile_index",
            type_="check",
        )
        batch.drop_column("frontend_review_results")
        batch.drop_column("frontend_review_profile_index")
        batch.drop_column("frontend_review_profile_ids")
        batch.drop_column("frontend_review_config_snapshot")
