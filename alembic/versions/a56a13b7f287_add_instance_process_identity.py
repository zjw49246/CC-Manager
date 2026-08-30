"""add_instance_process_identity

Adds kernel process identity evidence beside ``instances.pid`` so a bare PID
number is never treated as proof that a managed generation is still running.

``process_identity`` holds an opaque ``v1:<pid>:<start_ticks>:<boot_id>``
value: the kernel-reported start time plus the boot session the PID was
observed in. Together they let recovery distinguish "this exact process is
still alive" from "this PID number was reused by an unrelated process", which
a bare ``kill(pid, 0)`` probe cannot do.

The PID is embedded in the value so a writer that updates ``pid`` without
refreshing this column yields a mismatch that reads as unusable evidence
rather than as proof of death.

The column is nullable: rows written by an older binary carry no identity
evidence and must keep the previous conservative fail-closed behaviour.

Revision ID: a56a13b7f287
Revises: a7d4e9c2f610
Create Date: 2026-08-15 11:59:57.462549

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a56a13b7f287'
down_revision: Union[str, None] = 'f4c7a9d2e610'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("instances", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("process_identity", sa.String(length=128), nullable=True)
        )
        batch_op.create_check_constraint(
            "ck_instances_process_identity_requires_pid",
            "process_identity IS NULL OR pid IS NOT NULL",
        )


def downgrade() -> None:
    with op.batch_alter_table("instances", schema=None) as batch_op:
        batch_op.drop_constraint(
            "ck_instances_process_identity_requires_pid",
            type_="check",
        )
        batch_op.drop_column("process_identity")
