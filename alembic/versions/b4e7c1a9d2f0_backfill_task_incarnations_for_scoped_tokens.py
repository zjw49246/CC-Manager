"""backfill Task incarnations for scoped internal credentials

Revision ID: b4e7c1a9d2f0
Revises: f9b2c4d6e8a1
"""

from alembic import op
import sqlalchemy as sa


revision = "b4e7c1a9d2f0"
down_revision = "f9b2c4d6e8a1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    dialect = op.get_bind().dialect.name
    if dialect == "sqlite":
        expression = "lower(hex(randomblob(16)))"
    elif dialect == "postgresql":
        expression = (
            "md5('ccm-task-incarnation:' || id::text || ':' || "
            "random()::text || ':' || clock_timestamp()::text)"
        )
    elif dialect in {"mysql", "mariadb"}:
        # UUID() is available on both MySQL and MariaDB. RANDOM_BYTES() is
        # absent from supported MariaDB releases and some older MySQL builds.
        expression = "lower(replace(uuid(), '-', ''))"
    else:
        raise RuntimeError(
            f"Task incarnation backfill does not support dialect {dialect!r}"
        )
    op.execute(sa.text(
        f"UPDATE tasks SET incarnation_id = {expression} "
        "WHERE incarnation_id IS NULL"
    ))


def downgrade() -> None:
    # Incarnations are durable identities, not feature data. Retaining them is
    # safe for the preceding nullable column and avoids reviving stale tokens.
    pass
