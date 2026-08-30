"""merge runtime capacity and PR Loop heads

Revision ID: c5e7a9d1f3b6
Revises: a3f7c2d8e1b4, a4d8e2f6b1c3
Create Date: 2026-08-11
"""

from typing import Sequence, Union


revision: str = "c5e7a9d1f3b6"
down_revision: Union[str, Sequence[str], None] = (
    "a3f7c2d8e1b4",
    "a4d8e2f6b1c3",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
