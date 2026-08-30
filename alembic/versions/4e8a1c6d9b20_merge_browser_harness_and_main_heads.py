"""merge browser harness and managed SSH/discussion heads

Revision ID: 4e8a1c6d9b20
Revises: 2a6c8e0f4b1d, d1a9c4e7b260
Create Date: 2026-08-09
"""

from typing import Sequence, Union


revision: str = "4e8a1c6d9b20"
down_revision: Union[str, Sequence[str], None] = (
    "2a6c8e0f4b1d",
    "d1a9c4e7b260",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Join both published histories without changing schema."""


def downgrade() -> None:
    """Split back to both published heads without changing schema."""
