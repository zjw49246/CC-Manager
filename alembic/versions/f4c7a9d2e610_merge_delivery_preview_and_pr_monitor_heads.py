"""merge Delivery Preview Profile and PR Monitor tombstone heads

Revision ID: f4c7a9d2e610
Revises: e2a4c6f8b1d3, b8e4d2f6a1c9
Create Date: 2026-08-15
"""

from typing import Sequence, Union


revision: str = "f4c7a9d2e610"
down_revision: Union[str, Sequence[str], None] = (
    "e2a4c6f8b1d3",
    "b8e4d2f6a1c9",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Join both published histories without changing schema."""


def downgrade() -> None:
    """Split back to both feature heads without changing schema."""
