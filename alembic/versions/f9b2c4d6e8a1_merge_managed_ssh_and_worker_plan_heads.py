"""merge managed SSH and Worker Plan receipt heads

Revision ID: f9b2c4d6e8a1
Revises: a6d9f2c4e8b1, d3c8a7f1e620
Create Date: 2026-08-09
"""

from typing import Sequence, Union


revision: str = "f9b2c4d6e8a1"
down_revision: Union[str, Sequence[str], None] = (
    "a6d9f2c4e8b1",
    "d3c8a7f1e620",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Join both published histories without changing schema."""


def downgrade() -> None:
    """Split back to both feature heads without changing schema."""
