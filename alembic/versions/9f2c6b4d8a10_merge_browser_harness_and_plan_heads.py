"""merge browser test harness and first-class Plan migration heads

Revision ID: 9f2c6b4d8a10
Revises: 7d2f4b9a6c10, e5b8d1c4a7f2
Create Date: 2026-08-06
"""

from typing import Sequence, Union


revision: str = "9f2c6b4d8a10"
down_revision: Union[str, Sequence[str], None] = (
    "7d2f4b9a6c10",
    "e5b8d1c4a7f2",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Join both published histories without changing schema."""


def downgrade() -> None:
    """Split back to both published heads without changing schema."""
