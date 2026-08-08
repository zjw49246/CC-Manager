"""add explicit Test Harness evidence archive state

Revision ID: f1c4e6a8b0d2
Revises: e0b3c5d7f9a1
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f1c4e6a8b0d2"
down_revision: Union[str, Sequence[str], None] = "e0b3c5d7f9a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "test_harness_attempts",
        sa.Column("artifact_staging_root", sa.String(length=1000), nullable=True),
    )
    op.add_column(
        "test_harness_attempts",
        sa.Column("artifact_archive_prefix", sa.String(length=1000), nullable=True),
    )
    op.add_column(
        "test_harness_attempts",
        sa.Column(
            "archive_state",
            sa.String(length=24),
            server_default="staging",
            nullable=False,
        ),
    )
    op.add_column(
        "test_harness_attempts",
        sa.Column(
            "archive_manifest",
            sa.JSON(),
            server_default="{}",
            nullable=False,
        ),
    )
    op.add_column(
        "test_harness_attempts",
        sa.Column("archive_error", sa.Text(), nullable=True),
    )
    op.add_column(
        "test_harness_attempts",
        sa.Column("archived_at", sa.DateTime(), nullable=True),
    )
    op.create_index(
        "ix_test_harness_attempts_archive_state",
        "test_harness_attempts",
        ["archive_state"],
        unique=False,
    )
    # The old column could contain either an absolute staging directory or a
    # relative archive prefix. Preserve it only as a recovery hint; the new
    # state starts conservatively at ``staging`` and is promoted to complete
    # only after application-level manifest/integrity verification.
    op.execute(
        "UPDATE test_harness_attempts "
        "SET artifact_staging_root = artifact_root"
    )


def downgrade() -> None:
    op.drop_index(
        "ix_test_harness_attempts_archive_state",
        table_name="test_harness_attempts",
    )
    op.drop_column("test_harness_attempts", "archived_at")
    op.drop_column("test_harness_attempts", "archive_error")
    op.drop_column("test_harness_attempts", "archive_manifest")
    op.drop_column("test_harness_attempts", "archive_state")
    op.drop_column("test_harness_attempts", "artifact_archive_prefix")
    op.drop_column("test_harness_attempts", "artifact_staging_root")
