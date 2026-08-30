"""add managed ssh profiles

Revision ID: 73c4a9e1b2d0
Revises: e5b8d1c4a7f2
"""

from alembic import op
import sqlalchemy as sa


revision = "73c4a9e1b2d0"
down_revision = "e5b8d1c4a7f2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ssh_profiles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("host", sa.String(length=253), nullable=False),
        sa.Column("port", sa.Integer(), server_default="22", nullable=False),
        sa.Column("username", sa.String(length=255), nullable=False),
        sa.Column("key_path", sa.String(length=1000), nullable=False),
        sa.Column("public_key_fingerprint", sa.String(length=100), nullable=False),
        sa.Column("host_key_type", sa.String(length=64), nullable=False),
        sa.Column("host_key_value", sa.Text(), nullable=False),
        sa.Column("host_key_fingerprint", sa.String(length=100), nullable=False),
        sa.Column("revision", sa.Integer(), server_default="1", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=True),
        sa.Column("last_tested_at", sa.DateTime(), nullable=True),
        sa.Column("last_test_ok", sa.Boolean(), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("last_error_detail", sa.String(length=500), nullable=True),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
        mysql_engine="InnoDB",
    )
    op.create_index(
        op.f("ix_ssh_profiles_created_by"),
        "ssh_profiles",
        ["created_by"],
        unique=False,
    )
    op.create_index(
        op.f("ix_ssh_profiles_deleted_at"),
        "ssh_profiles",
        ["deleted_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_ssh_profiles_deleted_at"), table_name="ssh_profiles")
    op.drop_index(op.f("ix_ssh_profiles_created_by"), table_name="ssh_profiles")
    op.drop_table("ssh_profiles")
